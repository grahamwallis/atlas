/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.openmetadata.adapters.repositoryconnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class provides the mapping between a number of high level types in OM and Atlas that
 * have the same name but different GUIDs and slightly different properties. These types are
 * all entity types and are all generally high up in any inheritance hierarchy, so are used
 * in superType chains. They will also be found in relationship end definitions and lists of
 * valid entity types in classification defs.
 *
 * The types involved are coded into a list below, so that if necessary other types can be added.
 * Initially the list only needs Referenceable, Asset, DataSet, Infrastructure and Process - these
 * are referred to collectively as the "Famous Five". In most of these types, the OM type and Atlas
 * type have identical properties, but there are a couple of notable differences - OM's Referenceable
 * has 'additionalProperties' and Atlas's Process has 'inputs' and 'outputs'.
 *
 * The LocalAtlasOMRSConnector spans the OM and Atlas domains, so it needs to accommodate the
 * differences between these types and convert between them appropriately. Whenever one of the OM
 * types is referenced it must be substituted by a type that can be stored into Atlas without
 * clashing (since the names are the same), and when one of the Atlas types is encountered it
 * needs to be converted to the appropriate OM type. For example, an OM 'Referenceable' is
 * converted and stored as 'OM_Referenceable' to avoid clashing with Atlas's 'Referenceable'.
 * Furthermore, the OM_Referenceable depends upon and extends the Atlas type.
 *
 * The way this is to be achieved is to introduce five new types that are stored in the Atlas
 * repository (during startup). When the connector stack is first started, the local connector
 * will call the Atlas connector's verifyTypeDef() method for each of the types defined in the
 * type archive. This set of around 350 types includes the types referred to as the Famous Five.
 * The parameters to verifyTypeDef() include the TypeDef, containing the type name and the type
 * GUID. In normal circumstances, when the connector is asked to verify a type, it tests whether
 * the type name is known and the type definition matches the existing type. If so, the type is
 * verified and the verifyTypeDef() methods returns true. If the type is not known the method
 * returns false. If a type is not supportable or clashes with an Atlas type an exception is
 * thrown.
 *
 * When the verifyTypeDef() method encounters any of the Famous Five types, it should return
 * that they are supportable but not known. The Local Connector will immediately issue an addTypeDef()
 * call to define the type. This provides the opportunity to define the additional type described
 * above. The addTypeDef() method needs to recognise that the type is a member of the Famous Five,
 * and create the modified type in Atlas, extending the original Atlas type.
 *
 * The Atlas Connector keeps track of the original OM GUID of the types that it has mapped in this way,
 * so that they can be converted back.
 *
 * Using Referenceable as an example, the result of these type additions during startup should result
 * in the following additional type being defined in Atlas:
 *
 * OM                          Connector                                         Atlas
 *
 * 'Referenceable'  ---------> addTypeDef (typeDef)
 *                             typeDef.name = 'Referenceable'
 *                             typeDef.GUID = <OM_GUID>
 *                             recognise type name E F5
 *                             create Atlas type
 *                             type.name = 'OM_Referenceable'
 *                             type.guid = <OM_GUID>
 *                             type.supertype = (Atlas) Referenceable
 *                             adds additionalProperties      ----------------> OM_Referenceable (OM_GUID)
 *                                                                              extends Atlas Referenceable
 *
 * Whenever a request is made (from an OMRS application/caller/connector) that refers to one of the
 * Famous Five types, the connector needs to switch the name to the OM-specific type name as stored
 * in the repository. For example:
 *
 * OM                           Connector                                         Atlas
 *
 * Simple references
 *
 * reference to type
 *  name = 'Referenceable'
 *  GUID = '12345'     ------>  detect name E F5
 *                              prepend name with 'OM_'
 *                              name = 'OM_Referenceable'
 *                              GUID = '12345'   --------------------------->  Look up type
 *                                                                             name = 'OM_Referenceable'
 *                                                                             GUID = '12345'
 *                                               <---------------------------
 *                              remove name prefix so name
 *                              reverts to 'Referenceable'
 *          <-------------------
 *
 *
 *
 * Supertype references
 *
 *  Type 'A' refers to
 *  supertype with:
 *  name = 'Referenceable'
 *  GUID = '12345'     ------>  detect name E F5
 *                              OM supertype remains as Referenceable
 *                              For Atlas persistence/lookup, name is
 *                              prefixed with 'OM_' so supertype changed
 *                              to OM_Referenceable (which in Atlas extends
 *                              from Atlas Referenceable so chain is
 *                              complete.                               ---------->  Type 'A' supertype = OM_Referenceable
 *                                                                                   OM_Referenceable supertype = Referenceable
 *                              Atlas-side, update supertype
 *                              reference to:
 *                              name = 'OM_Referenceable'
 *                              GUID = '12345'
 *                              with supertype = null
 *
 *                              OM-side, although in Atlas OM_Referenceable
 *                              extends Atlas Referenceable, this is not
 *                              surfaced to OM
 */

 // package private
 class FamousFive {

    private static final Logger LOG = LoggerFactory.getLogger(FamousFive.class);

    // In map keys are OM type names; values are corresponding Atlas type names

    private static Map<String,String> nameSubstitution = new HashMap<>();

    private static final String OM_PREFIX = "OM_";

    static {
        // Map of common name to extended name
        nameSubstitution.put("Referenceable",  OM_PREFIX+"Referenceable");
        nameSubstitution.put("Asset",          OM_PREFIX+"Asset");
        nameSubstitution.put("DataSet",        OM_PREFIX+"DataSet");
        nameSubstitution.put("Process",        OM_PREFIX+"Process");
        nameSubstitution.put("Infrastructure", OM_PREFIX+"Infrastructure");
    }

    // Second map is to keep track of OM GUIDs associated with common names. These are needed for reverse mappings.
    private static Map<String,String> omNameToGUID = new HashMap<>();

    private FamousFive() {
        // do nothing
    }

    // package private
    static boolean omTypeRequiresSubstitution(String typeName) {
        final String methodName = "omTypeRequiresSubstitution";
        // Look in the map keys for the specified name
        String val = nameSubstitution.get(typeName);
        if (val != null) {
            LOG.debug("{}: OM type name {} requires substitution", methodName, typeName);
            return true;
        }
        return false;
    }


    // package private
    static boolean atlasTypeRequiresSubstitution(String typeName) {
        final String methodName = "atlasTypeRequiresSubstitution";
        // Look in the map values for the specified name
        boolean found = false;
        Collection<String> atlasTypeNames = nameSubstitution.values();
        if (atlasTypeNames != null) {
            for (String s : atlasTypeNames) {
                if (typeName.equals(s)) {
                    LOG.debug("{}: Atlas type name {} requires substitution", methodName, typeName);
                    found = true;
                    break;
                }
            }
        }
        return found;
    }

    // package private
    static String getAtlasTypeName(String omTypeName, String guid) {
        final String methodName = "getAtlasTypeName";
        String atlasTypeName = nameSubstitution.get(omTypeName);
        if (atlasTypeName != null) {
            LOG.debug("{}: Atlas type name {} ", methodName, atlasTypeName);
            if (guid != null) {
                String recordedGUID = omNameToGUID.get(omTypeName);
                if (recordedGUID == null) {
                    // record the new guid
                    LOG.debug("{}: OM type name {} recorded GUID {} ", methodName, omTypeName, guid);
                    omNameToGUID.put(omTypeName, guid);
                } else if (!recordedGUID.equals(guid)) {
                    // update the recorded GUID - log this
                    LOG.debug("{}: OM type name {} update recorded GUID {} ", methodName, omTypeName, guid);
                    omNameToGUID.put(omTypeName, guid);
                }
            }
        }
        else {
            LOG.error("getAtlasTypeName: Atlas type name was null");
        }
        return atlasTypeName;
    }

    // package private
    static String getOMTypeName(String atlasTypeName) {
        final String methodName = "getOMTypeName";
        Set<String> omTypeNames = nameSubstitution.keySet();
        if (omTypeNames != null) {
            for (String s : omTypeNames) {
                String atlasName = nameSubstitution.get(s);
                if (atlasName.equals(atlasTypeName)) {
                    LOG.debug("{}: OM type name {} ", methodName, s);
                    return s;
                }
            }
        }
        return null;
    }

    // package private
    static String getRecordedGUID(String omTypeName) {
        String recordedGUID = omNameToGUID.get(omTypeName);
        if (recordedGUID == null)
            LOG.error("getAtlasTypeName: OM type name {} has no recorded GUID", omTypeName);
        return recordedGUID;
    }





}
