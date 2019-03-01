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

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EntityDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefLink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EntityDefMapper {

    private static final Logger LOG = LoggerFactory.getLogger(EntityDefMapper.class);

    private LocalAtlasOMRSMetadataCollection metadataCollection;
    private String                           userId;
    private EntityDef                        entityDef;


    public EntityDefMapper(LocalAtlasOMRSMetadataCollection metadataCollection, String userId, EntityDef entityDef) {
        this.metadataCollection = metadataCollection;
        this.userId             = userId;
        this.entityDef          = entityDef;
    }


    public ArrayList<String> getValidPropertyNames() {
        // Convenience method to iterate over superType hierarchy and collate properties
       return getValidPropertyNames(entityDef);
    }

    private ArrayList<String> getValidPropertyNames(TypeDef typeDef) {

        final String methodName = "getValidPropertyNames";

        ArrayList<String> validPropNames = null;
        List<TypeDefAttribute> typeDefAttributes = typeDef.getPropertiesDefinition();
        if (typeDefAttributes != null) {
            validPropNames = new ArrayList<>();
            for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                String attrName = typeDefAttribute.getAttributeName();
                validPropNames.add(attrName);
            }
        }

        // Visit the next type in the supertype hierarchy, if any
        TypeDefLink superTypeDefLink = typeDef.getSuperType();
        if (superTypeDefLink != null) {
            // Retrieve the supertype - the TDL gives us its GUID and name
            if (superTypeDefLink.getName() != null) {
                TypeDef superTypeDef;
                try {
                    superTypeDef = metadataCollection._getTypeDefByName(userId, superTypeDefLink.getName());
                } catch (Exception e) {
                    LOG.error("{}: could not get supertype {} using _getTypeDefByName {}", methodName, superTypeDefLink.getName(), e);
                    return null;
                }
                if (superTypeDef != null) {
                    ArrayList<String> additionalPropNames = getValidPropertyNames(superTypeDef);

                    if (additionalPropNames != null) {
                        // Add the additional properties to any we already found at this level...
                        if (validPropNames == null) {
                            // We did not already find any properties (at the original instance level) so need
                            // to allocate the InstanceProperties now.
                            validPropNames = new ArrayList<>();
                        }
                        for (String propName : additionalPropNames) {
                            validPropNames.add(propName);
                        }
                    }
                }
            }
        }

        return validPropNames;
    }


}
