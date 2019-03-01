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

import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import static org.apache.atlas.model.TypeCategory.*;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.OMRSLogicErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotKnownException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.AttributeCardinality.*;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.AttributeTypeDefCategory.COLLECTION;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.CollectionDefCategory.OM_COLLECTION_ARRAY;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.CollectionDefCategory.OM_COLLECTION_MAP;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory.OM_PRIMITIVE_TYPE_UNKNOWN;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;



public class AtlasAttributeDefMapper {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAttributeDefMapper.class);

    private final LocalAtlasOMRSMetadataCollection       metadataCollection;
    private       String                                 metadataCollectionId = null;
    private final String                                 userId;
    private final AtlasTypeDefStore                      typeDefStore;
    private final AtlasTypeRegistry                      typeRegistry;
    private final OMRSRepositoryHelper                   repositoryHelper;
    private final TypeDefsByCategory                     typeDefsForAPI;
    private final List<AtlasStructDef.AtlasAttributeDef> aads;


    public AtlasAttributeDefMapper(LocalAtlasOMRSMetadataCollection        metadataCollection,
                                   String                                  userId,
                                   AtlasTypeDefStore                       typeDefStore,
                                   AtlasTypeRegistry                       typeRegistry,
                                   OMRSRepositoryHelper                    repositoryHelper,
                                   TypeDefsByCategory                      typeDefsForAPI,
                                   List<AtlasStructDef.AtlasAttributeDef>  aads)

            throws
            RepositoryErrorException
    {

        final String methodName = "AtlasAttributeDefMapper";

        this.metadataCollection = metadataCollection;
        this.userId             = userId;
        this.typeDefStore       = typeDefStore;
        this.typeRegistry       = typeRegistry;
        this.repositoryHelper   = repositoryHelper;
        this.aads               = aads;
        this.typeDefsForAPI     = typeDefsForAPI;

        try {

            this.metadataCollectionId = metadataCollection.getMetadataCollectionId();

        } catch (Exception e) {
            LOG.error("{}: metadataCollectionId not available", methodName, e);
            OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


    }

    // Convert a List of AtlasAttributeDef to a List of OMRS TypeDefAttribute
    // package private
    List<TypeDefAttribute> convertAtlasAttributeDefs()
            throws
            TypeErrorException

    {
        final String methodName = "convertAtlasAttributeDefs";

        ArrayList<TypeDefAttribute> omrsTypeDefAttributes;
        if (aads == null || aads.isEmpty()) {

            LOG.debug("{}: Atlas attributes are missing {}", methodName, aads);
            return null;

        } else {
            LOG.debug("{}: convert Atlas attributes {}", methodName, aads);
            omrsTypeDefAttributes = new ArrayList<>();

            for (AtlasStructDef.AtlasAttributeDef aad : aads) {
                LOG.debug("{}: convert Atlas attribute {}", methodName, aad);
                // Map from AtlasAttributeDef to OMRS TypeDefAttribute
                try {
                    TypeDefAttribute tda = convertAtlasAttributeDef(aad);
                    if (tda != null)
                        omrsTypeDefAttributes.add(tda);
                }
                catch (TypeDefNotKnownException e) {
                    // Failed to process one of the attribute types; give up by explicitly throwing a ConversionException
                    LOG.error("{}: Failed to convert Atlas attribute def {}", methodName, aad.getName(), e);

                    // Throw an error here - which will cause the whole type to be skipped.
                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_ATTRIBUTE_TYPE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aad.getName(), aad.getTypeName());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());


                }
            }
        }
        return omrsTypeDefAttributes;
    }


    // Convert an individual attribute def
    private TypeDefAttribute convertAtlasAttributeDef(AtlasStructDef.AtlasAttributeDef aad)
        throws
            TypeErrorException,
            TypeDefNotKnownException
    {

        final String methodName = "convertAtlasAttributeDef";


        // Convert AtlasAttributeDef to OMRS TypeDefAttribute
        //
        //
        // AtlasAttributeDef has:                             OMRS TypeDefAttribute has:
        //
        // String name             ------------------>        String attributeName
        // String typeName         -- USED (MULTI) -->        AttributeTypeDef attributeType:
        //                                                       ---------------------------------------------------------------------------------
        //                                                       AttributeTypeDefCategory category:
        //                                                          OM defines the following values { UNKNOWN_DEF | PRIMITIVE | COLLECTION | ENUM_DEF }
        //                                                          Figure out the Atlas TypeCategory (see below) then map as follows:
        //                                                          [atlas]            [omrs]
        //                                                          PRIMITIVE       -> PRIMITIVE
        //                                                          OBJECT_ID_TYPE  -> UNKNOWN_DEF / AUDIT LOG!!
        //                                                          ENUM            -> ENUM_DEF
        //                                                          STRUCT          -> UNKNOWN_DEF / AUDIT LOG!!
        //                                                          CLASSIFICATION  -> UNKNOWN_DEF / AUDIT LOG!!
        //                                                          ENTITY          -> UNKNOWN_DEF / AUDIT LOG!!
        //                                                          ARRAY           -> COLLECTION
        //                                                          MAP             -> COLLECTION
        //                                                          RELATIONSHIP    -> UNKNOWN_DEF / AUDIT LOG!!
        //                                                       String guid  = will have to fabricate guid or leave null
        //                                                       String name  = AtlasAttributeDef.typeName
        //                                                       ---------------------------------------------------------------------------------
        //                                                     String attributeDescription = AtlasAttributeDef.description (see below)
        // boolean isOptional       ----- USED -------->       in OMRS AttributeCardinality (see below)
        // Cardinality cardinality  ----> USED -------->       AttributeCardinality cardinality:
        //                                                         atlas           -> omrs
        //                                                         any other combination  -> UNKNOWN
        //                                                         isOptional && SINGLE   -> AT_MOST_ONE
        //                                                         !isOptional && SINGLE  -> ONE_ONLY
        //                                                         !isOptional && LIST    -> AT_LEAST_ONE_ORDERED
        //                                                         !isOptional && SET     -> AT_LEAST_ONE_UNORDERED
        //                                                         isOptional && LIST     -> ANY_NUMBER_ORDERED
        //                                                         isOptional && SET      -> ANY_NUMBER_UNORDERED
        // int valuesMinCount       -------------------->      int valuesMinCount
        // int valuesMaxCount       -------------------->      int valuesMaxCount
        // boolean isIndexable      -------------------->      boolean isIndexable
        // boolean isUnique         -------------------->      boolean isUnique
        // String defaultValue      -------------------->      String defaultValue
        // String description       ----- USED --------->      used to set attributeDescription (see above)
        // List<AtlasConstraintDef> ---- IGNORED x             There are no constraints in OM so Atlas constraints are deliberately ignored
        //         n/a              ----- FABRICATE ---->      ArrayList<ExternalStandardMapping> externalStandardMappings = null (always)
        //

        LOG.debug("{}: AtlasAttributeDef is {}", methodName, aad);

        if (aad == null) {
            return null;
        }

        // Validate AtlasAttributeDef - make sure the named type exists in the type registry (or is primitive) and that we can find/set category
        String atlasTypeName = aad.getTypeName();
        TypeCategory atlasCategory = null;

        // We need to establish the Atlas type category.
        // If this is a PRIMITIVE, MAP or ARRAY then it will not have a TypeDef so we need to use other means to determine
        // what category it has. If, after exhausting these means, we still do not have a valid category then we can assume that
        // there is a TypeDef so we query the Atlas TypeDefStore.
        //

        // If the type is a PRIMITIVE it will not have a valid category for us to use - so we must identify that the
        // type name represents a primitive and set the category explicitly to PRIMITIVE.
        for (String atlasPrimitive : AtlasBaseTypeDef.ATLAS_PRIMITIVE_TYPES) {
            if (atlasTypeName.equals(atlasPrimitive)) {
                // This attribute is of a primitive type
                atlasCategory = PRIMITIVE;
                break;
            }
        }

        // If the type is a DATE it will not have a valid category for us to use - so we must identify that the
        // type name represents a primitive and set the category explicitly. In the specific case of 'date' we
        // have to provide special handling - 'date' is not a primitive in Atlas, but it is a a primitive in OM.
        if (atlasTypeName.equals(ATLAS_TYPE_DATE)) {
            // This attribute is handled in OM as a primitive type - although for Atlas it is not considered a primitive
            // It will be handled below along with the Atlas types that are primitive
            LOG.debug("{}: AtlasAttributeDef is a date", methodName);
            atlasCategory = PRIMITIVE;
        }

        // Similarly if the Atlas typeName uses the array prefix then we should set the category to array
        if (atlasCategory == null && AtlasTypeUtil.isArrayType(atlasTypeName)) {
            LOG.debug("{}: AtlasAttributeDef is an array", methodName);
            atlasCategory = ARRAY;
        }

        // Similarly if the Atlas typeName uses the map prefix then we should set the category to map
        if (atlasCategory == null && AtlasTypeUtil.isMapType(atlasTypeName)) {
            LOG.debug("{}: AtlasAttributeDef is a map", methodName);
            atlasCategory = MAP;
        }

        // Finally, if none of the above succeeded then we may be looking at an Enum (or anything else, but they are
        // not supported in OM and will result in error). Need to look in the TypeDefStore to find the category.
        // If none of the above were true then we need to find out what category of Atlas type we are dealing with....
        // Perform a typedefstore.getByName to get an AtlasBaseTypeDef which will have a category, i.e. one of
        // PRIMITIVE, OBJECT_ID_TYPE, ENUM, STRUCT, CLASSIFICATION, ENTITY, ARRAY, MAP, RELATIONSHIP
        // If PRIMITIVE, MAP or ARRAY then we already have a category anyway - will process below
        // If ENUM - then we will process as an Enum attribute - see below
        // All of PRIMITIVE, ARRAY, MAP and ENUM are handled in the switch below.
        // If CLASSIFICATION, ENTITY, RELATIONSHIP then this is not used as an attribute - log as an error
        // If OBJECT_ID_TYPE, STRUCT we do not support these in OM so log as an error
        if (atlasCategory == null) {

            // Using registry - if type is not found this will return null.
            AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByName(atlasTypeName);

            if (abtd == null) {

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasTypeName, methodName, metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // abtd is known to be good

            atlasCategory = abtd.getCategory();
            LOG.debug("{}: Retrieved typedef from store - attribute category is {}", methodName, atlasCategory);
            if (atlasCategory != ENUM) {
                // This is not a type of attribute that we can support in OM, we need to fail the conversion of the enclosing def...
                // This is the end of the road for attributes of type category OBJECT_ID_TYPE, STRUCT, CLASSIFICATION, ENTITY, RELATIONSHIP
                LOG.error("{}: The Atlas attribute category {} is not represented in OM - giving up on typedef {}", methodName, atlasCategory, atlasTypeName);

                // Throw an error here - which will cause the whole type to be skipped.
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.UNSUPPORTED_ATTRIBUTE_TYPE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(aad.getName(), aad.getTypeName(), methodName,  metadataCollectionId );

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

        }
        LOG.debug("{}: AtlasAttributeDef {} handled as {}", methodName, atlasTypeName, atlasCategory);


        // Create OMRS TypeDefAttribute
        TypeDefAttribute omrsTypeDefAttribute = new TypeDefAttribute();

        // Set attributeName
        omrsTypeDefAttribute.setAttributeName(aad.getName());

        // For OMRS we need to construct an AttributeTypeDef with an AttributeTypeDefCategory, plus guid and name.
        // This is based on the (already known) atlasCategory.
        // Iff category is PRIMITIVE, ARRAY, MAP or ENUM create AttributeTypeDef else throw exception.
        // AttributeTypeDef is abstract so we must wait till we know the category before instantiating relevant implementation subclass.
        // AttributeTypeDef requires String name, String guid and AttributeTypeDefCategory category

        AttributeTypeDef existingAttributeTypeDef;

        switch (atlasCategory) {

            case PRIMITIVE:
                // Convert Atlas primitive or date to OM PrimitiveDef
                // The TypeDefCategory (AttributeTypeDefCategory.PRIMITIVE) is set in the CTOR of PrimitiveDef
                // The Atlas primitive type is given by the atlasTypeName (which is a String).
                // Create a PrimitiveDefCategory to pass to the constructor of PrimitiveDef and set the
                // guid and name to the values found in the PrimitiveDefCategory.
                PrimitiveDefCategory omrs_primitive_category = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(atlasTypeName);
                // Speculatively create an ATD and then compare it to our own list and to the ATDs known by the RC<...if we find a match we drop it
                PrimitiveDef primitiveDef = new PrimitiveDef(omrs_primitive_category);
                primitiveDef.setName(atlasTypeName);
                primitiveDef.setGUID(omrs_primitive_category.getGUID());
                // Using the typeDescription from the AttributeTypeDefCategory
                primitiveDef.setDescription(primitiveDef.getCategory().getDescription());
                // descriptionGuid is always set to null - until it is implemented
                primitiveDef.setDescriptionGUID(null);

                // Look for an ATD with the required category
                LOG.debug("{}: Look for a matching ATD in the RCM for primitive type {}", methodName, atlasTypeName);

                // Ask RepositoryContentManager whether there is an AttributeTypeDef with the name we resolved above
                String source = metadataCollectionId;
                try {
                    existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, atlasTypeName);

                } catch (OMRSLogicErrorException e) {
                    LOG.error("{}: caught exception from RepositoryHelper", methodName, e.getMessage());
                    OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(metadataCollectionId, methodName, e.getMessage());

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                if (existingAttributeTypeDef == null) {
                    // No existing ATD was found in RH so use the candidate attribute type def
                    LOG.debug("convertAtlasAttributeDef: repository content manager returned name not found - generate GUIDs and publish");
                    // Check (by name) whether we have already added one to current TDBC - e.g. if there are
                    // multiple attributes of the same type - they should refer to the same ATD in TDBC.
                    // So if it does not already exist add the new ATD (plus GUIDs) to the TDBC and use it in TDA.
                    // If there is already a TDBC copy of the ATD then reuse that as-is in the TDA.
                    PrimitiveDef tdbcCopy = typeDefsForAPI.getPrimitiveDef(primitiveDef.getName());
                    if (tdbcCopy != null) {
                        // Found - so reuse it
                        LOG.debug("convertAtlasAttributeDef: found existing ATD in typeDefsForAPI");
                        primitiveDef = tdbcCopy;
                    } else {
                        // Not found - so add it
                        LOG.debug("convertAtlasAttributeDef: did not find existing ATD in typeDefsForAPI");
                        // Set the GUID to a new generated value and descriptionGUID to null
                        String newGuid = UUID.randomUUID().toString();
                        primitiveDef.setGUID(newGuid);
                        primitiveDef.setDescriptionGUID(null);
                        long initialVersion = 1;
                        primitiveDef.setVersion(initialVersion);
                        primitiveDef.setVersionName(Long.toString(initialVersion));
                        // Add to TDBC
                        typeDefsForAPI.addPrimitiveDef(primitiveDef);
                    }
                    omrsTypeDefAttribute.setAttributeType(primitiveDef);

                } else {
                    // name matched
                    LOG.debug("convertAtlasAttributeDef: there is an AttributeTypeDef with name {} : {}", atlasTypeName, existingAttributeTypeDef);
                    if (existingAttributeTypeDef.getCategory() == AttributeTypeDefCategory.PRIMITIVE) {
                        LOG.debug("convertAtlasAttributeDef: existing AttributeTypeDef has category {} ", existingAttributeTypeDef.getCategory());
                        // There is an existing primitive with this name - perform deep compare and only publish if exact match
                        // Perform a deep compare of the known type and new type
                        Comparator comp = new Comparator();
                        PrimitiveDef existingPrimitiveDef = (PrimitiveDef) existingAttributeTypeDef;
                        LOG.debug("{}: check equivalence of existing and new AttributeTypeDef {} vs {} ", methodName, existingAttributeTypeDef, primitiveDef);
                        boolean typematch = comp.equivalent(existingPrimitiveDef, primitiveDef);
                        // If compare matches then we can proceed to publish the def
                        if (typematch) {
                            // There is exact match in the ReposHelper - we will add that to our TDG and use it in our TDA
                            LOG.debug("{}: using the existing ATD for type name {}", methodName, atlasTypeName);
                            typeDefsForAPI.addPrimitiveDef(existingAttributeTypeDef);
                            omrsTypeDefAttribute.setAttributeType(existingAttributeTypeDef);
                        } else {
                            // If compare failed abandon processing of this EnumDef
                            LOG.error("{}: existing AttributeTypeDef did not match", methodName);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());

                        }
                    } else {
                        // There is a type of this name but it is not an EnumDef - fail!
                        LOG.error("{}: existing AttributeTypeDef not a Primitive - has category {}", methodName, existingAttributeTypeDef.getCategory());
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                }

                break;

            case ARRAY:
                // Attribute is an array - of primitive elements.
                // If we are dealing with an ArrayDef then we can use the structure of the typename to derive the element type
                if (!(atlasTypeName.startsWith(ATLAS_TYPE_ARRAY_PREFIX) && atlasTypeName.endsWith(ATLAS_TYPE_ARRAY_SUFFIX))) {
                    // sanity check failed - there is something more subtle about this name, log it and give up
                    LOG.error("{}: could not parse atlas type name {}", methodName, atlasTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                } else {

                    int startIdx = ATLAS_TYPE_ARRAY_PREFIX.length();
                    int endIdx = atlasTypeName.length() - ATLAS_TYPE_ARRAY_SUFFIX.length();
                    String elementTypeName = atlasTypeName.substring(startIdx, endIdx);
                    LOG.debug("{} : handling an Atlas array in which the elements are of type {}", methodName, elementTypeName);
                    // Convert this into a CollectionDef of the appropriate element type...
                    CollectionDef omrsCollectionDef = new CollectionDef(OM_COLLECTION_ARRAY);
                    // CollectionDefCategory and argumentCount are set by constructor.
                    // We need to set up the argument types...
                    ArrayList<PrimitiveDefCategory> argTypes = new ArrayList<>();
                    PrimitiveDefCategory pdc = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(elementTypeName);
                    argTypes.add(pdc);
                    omrsCollectionDef.setArgumentTypes(argTypes);

                    // The collection name can be generated from the template name in the collection def cat with parameter substitution.
                    String nameTemplate = omrsCollectionDef.getCollectionDefCategory().getName();
                    // I'm not sure how this was intended to be used so this might be rather odd but here goes...
                    LOG.debug("{} Generate Array Attributes: name template is {}", methodName, nameTemplate);
                    String[] parts = nameTemplate.split("[{}]");
                    String omrsArrayTypeName = parts[0] + elementTypeName + parts[2];
                    LOG.debug("{}: name resolved to {}", methodName, omrsArrayTypeName);
                    omrsCollectionDef.setName(omrsArrayTypeName);

                    // If elementTypeName is for anything other than a primitive then OM will not support it because OM only supports collections of primitives
                    PrimitiveDefCategory om_primitive_category = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(elementTypeName);
                    if (om_primitive_category == OM_PRIMITIVE_TYPE_UNKNOWN) {
                        // The element type is not a primitive - so skip this collection type....
                        LOG.debug("{}: array has non-primitive element type {} so it will be skipped", methodName, elementTypeName);
                        // Skip processing this attribute type definition
                        return null;
                    }

                    // For now we do not need the version, versionName and description. They will be needed if the type is new (below).

                    // Look for an ATD with the required category
                    LOG.debug("{}: Look for a matching ATD in the RCM for array type {}", methodName, atlasTypeName);

                    // Look in the RCM

                    // Ask RepositoryContentManager whether there is an AttributeTypeDef with the name we resolved above
                    try {
                        existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(metadataCollectionId, omrsArrayTypeName);
                    } catch (OMRSLogicErrorException e) {
                        LOG.error("{}: caught exception from RepositoryHelper, giving up trying to convert attribute {}", methodName, omrsArrayTypeName, e);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }
                    if (existingAttributeTypeDef == null) {
                        // No existing ATD was found in RH so use the candidate attribute type def
                        LOG.debug("{}: repository content manager returned name not found - generate GUIDs and publish", methodName);
                        // Check (by name) whether we have already added one to current TDBC - e.g. if there are
                        // multiple attributes of the same type - they should refer to the same ATD in TDBC.
                        // So if it does not already exist add the new ATD (plus GUIDs) to the TDBC and use it in TDA.
                        // If there is already a TDBC copy of the ATD then reuse that as-is in the TDA.
                        CollectionDef tdbcCopy = typeDefsForAPI.getCollectionDef(omrsCollectionDef.getName());
                        if (tdbcCopy != null) {
                            // Found - so reuse it
                            LOG.debug("{}: found existing ATD in typeDefsForAPI", methodName);
                            omrsCollectionDef = tdbcCopy;
                        } else {
                            // Not found - so add it
                            LOG.debug("{}: did not find existing ATD in typeDefsForAPI", methodName);
                            // Set the GUID to a new generated value and descriptionGUID to null
                            String newGuid = UUID.randomUUID().toString();
                            omrsCollectionDef.setGUID(newGuid);
                            long initialVersion = 1;
                            omrsCollectionDef.setVersion(initialVersion);
                            omrsCollectionDef.setVersionName(Long.toString(initialVersion));
                            omrsCollectionDef.setDescription("An array of " + elementTypeName);
                            omrsCollectionDef.setDescriptionGUID(null);
                            // Add to TDBC
                            typeDefsForAPI.addCollectionDef(omrsCollectionDef);
                        }
                        omrsTypeDefAttribute.setAttributeType(omrsCollectionDef);

                    } else {

                        // name matched
                        LOG.debug("{}: there is an AttributeTypeDef with name {} : {}", methodName, omrsArrayTypeName, existingAttributeTypeDef);
                        if (existingAttributeTypeDef.getCategory() == COLLECTION) {
                            LOG.debug("{}: existing AttributeTypeDef has category {} ", methodName, existingAttributeTypeDef.getCategory());
                            // There is an existing collection (array) with this name - perform deep compare and only publish if exact match
                            // Perform a deep compare of the known type and new type
                            Comparator comp = new Comparator();
                            CollectionDef existingCollectionDef = (CollectionDef) existingAttributeTypeDef;

                            // Before performing the compare - we need to assist things a little
                            // A CollectionDef needs a guid but cannot get it from Atlas.
                            // Since the RCM knows about this collection type we must have added it already and it must have a GUID
                            // So adopt the GUID found in the RCM. May as well adopt version and description as well...
                            omrsCollectionDef.setGUID(existingCollectionDef.getGUID());
                            omrsCollectionDef.setVersion(existingCollectionDef.getVersion());
                            omrsCollectionDef.setDescription(existingCollectionDef.getDescription());

                            // Compare existing versus new collection def
                            boolean typematch = comp.compare(true, existingCollectionDef, omrsCollectionDef);
                            // If compare matches then we can proceed to publish the def
                            if (typematch) {
                                // There is exact match in the ReposHelper - we will add that to our TDG and use it in our TDA
                                LOG.debug("{}: using the existing ATD for type name {}", methodName, omrsArrayTypeName);
                                typeDefsForAPI.addCollectionDef(existingAttributeTypeDef);
                                omrsTypeDefAttribute.setAttributeType(existingAttributeTypeDef);
                            } else {
                                // If compare failed generate AUDIT log entry and abandon processing of this EnumDef
                                LOG.error("{}: repository content manager found clashing def with name {}", methodName, omrsArrayTypeName);
                                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                       methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }
                        } else {
                            // There is a type of this name but it is not a CollectionDef - fail!
                            LOG.error("{}: repository content manager found type but not a Collection - has category {}", methodName, existingAttributeTypeDef.getCategory());
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                    }
                }
                break;

            case MAP:
                // Attribute is a map - from AtlasType to AtlasType - where these are assumed to be primitive.
                // If we are dealing with a MapDef then we can use the structure of the typename to derive the element type
                if (!(atlasTypeName.startsWith(ATLAS_TYPE_MAP_PREFIX) && atlasTypeName.endsWith(ATLAS_TYPE_MAP_SUFFIX))) {
                    // sanity check failed - there is something more subtle about this name, log it and give up
                    LOG.error("{}: could not parse atlas type name {}", methodName, atlasTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                } else {

                    int startIdx = ATLAS_TYPE_MAP_PREFIX.length();
                    int endIdx = atlasTypeName.length() - ATLAS_TYPE_MAP_SUFFIX.length();
                    String[] keyValueTypes = atlasTypeName.substring(startIdx, endIdx).split(ATLAS_TYPE_MAP_KEY_VAL_SEP, 2);
                    String keyTypeName = keyValueTypes.length > 0 ? keyValueTypes[0] : null;
                    String valueTypeName = keyValueTypes.length > 1 ? keyValueTypes[1] : null;

                    if (keyTypeName == null || valueTypeName == null) {
                        LOG.error("{}: Atlas map has key type name {} and value type name {}, cannot process nulls - skipping type", methodName, keyTypeName, valueTypeName );
                        return null;
                    }
                    LOG.debug("{}: handling Atlas map with elements of type {} {}", methodName, keyTypeName, valueTypeName);
                    // Convert this into a CollectionDef of the appropriate element type...
                    CollectionDef omrsCollectionDef = new CollectionDef(OM_COLLECTION_MAP);

                    // Set up the list of argument types...
                    ArrayList<PrimitiveDefCategory> argTypes = new ArrayList<>();
                    PrimitiveDefCategory pdck = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(keyTypeName);
                    PrimitiveDefCategory pdcv = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(valueTypeName);
                    argTypes.add(pdck);
                    argTypes.add(pdcv);
                    omrsCollectionDef.setArgumentTypes(argTypes);


                    // Set name
                    // The collection name can be generated from the name in the collection def cat with suitable parameter substitution...
                    String nameTemplate = omrsCollectionDef.getCollectionDefCategory().getName();
                    // I'm not sure how this was intended to be used so this might be rather odd but here goes...
                    LOG.debug("{}: Generate Map Attributes: name template is {}", methodName, nameTemplate);
                    String[] parts = nameTemplate.split("[{}]");
                    String omrsMapTypeName = parts[0] + keyTypeName + ',' + valueTypeName + parts[4];
                    LOG.debug("{}: name resolved to {}", methodName, omrsMapTypeName);
                    omrsCollectionDef.setName(omrsMapTypeName);


                    // If keyTypeName is for anything other than a primitive then OM will not support it because OM only supports collections of primitives
                    PrimitiveDefCategory om_primitive_category = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(keyTypeName);
                    if (om_primitive_category == OM_PRIMITIVE_TYPE_UNKNOWN) {
                        // The key type is not a primitive - so skip this collection type....
                        LOG.debug("{}: map has non-primitive key type {} so it will be skipped", methodName, keyTypeName);
                        // Skip processing this attribute type definition
                        return null;
                    }
                    // If valueTypeName is for anything other than a primitive then OM will not support it because OM only supports collections of primitives
                    om_primitive_category = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(valueTypeName);
                    if (om_primitive_category == OM_PRIMITIVE_TYPE_UNKNOWN) {
                        // The value type is not a primitive - so skip this collection type....
                        LOG.debug("{}: map has non-primitive value type {} so it will be skipped", methodName, valueTypeName);
                        // Skip processing this attribute type definition
                        return null;
                    }

                    // Need to find out if we ave seen this OMRS attribute type def before - look in local cache and ask RCM

                    // Look for an ATD with the required category
                    LOG.debug("{}: Look for a matching ATD in the RCM for map type {}", methodName, atlasTypeName);

                    // Look in the RCM
                    // Ask RepositoryContentManager whether there is an AttributeTypeDef with the name we resolved above
                    try {
                        existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(metadataCollectionId, omrsMapTypeName);
                    } catch (OMRSLogicErrorException e) {
                        LOG.error("{}: caught exception from RepositoryHelper, giving up trying to convert attribute {}", methodName, omrsMapTypeName, e);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    if (existingAttributeTypeDef == null) {
                        // No existing ATD was found in RH so use the candidate attribute type def
                        LOG.debug("convertAtlasAttributeDef: repository content manager returned name not found - generate GUIDs and publish");
                        // Check (by name) whether we have already added one to current TDBC - e.g. if there are
                        // multiple attributes of the same type - they should refer to the same ATD in TDBC.
                        // So if it does not already exist add the new ATD (plus GUIDs) to the TDBC and use it in TDA.
                        // If there is already a TDBC copy of the ATD then reuse that as-is in the TDA.
                        CollectionDef tdbcCopy = typeDefsForAPI.getCollectionDef(omrsCollectionDef.getName());
                        if (tdbcCopy != null) {
                            // Found - so reuse it
                            LOG.debug("{}: found existing ATD in typeDefsForAPI", methodName);
                            omrsCollectionDef = tdbcCopy;
                        } else {
                            // Not found - so add it
                            LOG.debug("{}: did not find existing ATD in typeDefsForAPI", methodName);
                            // Set the GUID to a new generated value and descriptionGUID to null
                            String newGuid = UUID.randomUUID().toString();
                            omrsCollectionDef.setGUID(newGuid);
                            long initialVersion = 1;
                            omrsCollectionDef.setVersion(initialVersion);
                            omrsCollectionDef.setVersionName(Long.toString(initialVersion));
                            omrsCollectionDef.setDescription("A map of " + keyTypeName + " to " + valueTypeName);
                            omrsCollectionDef.setDescriptionGUID(null);
                            // Add to TDBC
                            typeDefsForAPI.addCollectionDef(omrsCollectionDef);
                        }
                        omrsTypeDefAttribute.setAttributeType(omrsCollectionDef);

                    } else {
                        // name matched
                        LOG.debug("{}: there is an AttributeTypeDef with name {} : {}", methodName, omrsMapTypeName, existingAttributeTypeDef);
                        if (existingAttributeTypeDef.getCategory() == COLLECTION) {
                            LOG.debug("{}: existing AttributeTypeDef has category {} ", methodName, existingAttributeTypeDef.getCategory());
                            // There is an existing collection (map) with this name - perform deep compare and only publish if exact match
                            // Perform a deep compare of the known type and new type
                            Comparator comp = new Comparator();
                            CollectionDef existingCollectionDef = (CollectionDef) existingAttributeTypeDef;

                            // Before performing the compare - we need to assist things a little
                            // A CollectionDef needs a guid but cannot get it from Atlas.
                            // Since the RCM knows about this collection type we must have added it already and it must have a GUID
                            // So adopt the GUID found in the RCM. May as well adopt version and description as well...
                            omrsCollectionDef.setGUID(existingCollectionDef.getGUID());
                            omrsCollectionDef.setVersion(existingCollectionDef.getVersion());
                            omrsCollectionDef.setDescription(existingCollectionDef.getDescription());

                            // Compare existing versus new collection def
                            boolean typematch = comp.compare(true, existingCollectionDef, omrsCollectionDef);
                            // If compare matches then we can proceed to publish the def
                            if (typematch) {
                                // There is exact match in the ReposHelper - we will add that to our TDG and use it in our TDA
                                LOG.debug("{}: using the existing ATD for type name {}", methodName, omrsMapTypeName);
                                typeDefsForAPI.addCollectionDef(existingAttributeTypeDef);
                                omrsTypeDefAttribute.setAttributeType(existingAttributeTypeDef);
                            } else {
                                // If compare failed generate AUDIT log entry and abandon processing of this EnumDef
                                LOG.error("{}: repository content manager found clashing def with name {}", methodName, omrsMapTypeName);
                                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }
                        } else {
                            // There is a type of this name but it is not an EnumDef - fail!
                            LOG.error("{}: repository content manager found type but not a Collection - has category {}", methodName, existingAttributeTypeDef.getCategory());
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName", methodName, metadataCollectionId);

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                    }
                }
                break;

            case ENUM:
                // The attribute is an enum. The OM TypeDefAttribute needs to contain an ATD pointing to the OM EnumDef.
                // To create this we need to locate the atlas type def used for the enum - and convert that to an OM EnumDef.
                // The AtlasType.typeName is equal to the AtlasTypeDef.name - so take the typeName and look in the def store
                // Note that we already looked this enum up before - when refactored we save this this extra lookup
                AtlasEnumDef atlasEnumDef;

                // Using registry - if type not found returns null
                AtlasBaseTypeDef      atlasBaseTypeDef = typeRegistry.getTypeDefByName(atlasTypeName);

                if (atlasBaseTypeDef == null || atlasBaseTypeDef.getCategory() != ENUM) {
                    // Either no type def was found or the type def found is not a type of attribute that we can support in OM.
                    // We need to fail the conversion of the enclosing def...
                    // This is the end of the road for attributes of type category OBJECT_ID_TYPE, STRUCT, CLASSIFICATION, ENTITY,
                    // RELATIONSHIP
                    LOG.error("{}: could not find an EnumDef for name {}", methodName, atlasTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "AtlasEnumDef", methodName, metadataCollectionId);

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                // We have an AtlasBaseTypeDef with category ENUM.
                // We can call convertAtlasEnumDefToOMRSEnumDef(AtlasEnumDef atlasEnumDef) to ensure the EnumDef is on our TDBC ATD list.
                // We can then retrieve it from the TDBC list and refer to it in the TDA
                // If that retrieval fails then the Atlas enum was not converted into an OM EnumDef so we give up the Attribute conversion...
                atlasEnumDef = (AtlasEnumDef) atlasBaseTypeDef;
                LOG.debug("{}: Retrieved AtlasEnumDef from store {}", methodName, atlasEnumDef);

                // Attempt the conversion to OM

                try {

                    metadataCollection.processAtlasEnumDef(atlasEnumDef);

                }
                catch (TypeErrorException e) {

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "AtlasEnumDef", methodName, metadataCollectionId);

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                // Retrieve the result
                boolean enumdef_matched = false;
                AttributeTypeDef omrsEnumDef = null;
                if (typeDefsForAPI.getEnumDefs() != null) {
                    for (AttributeTypeDef atd : typeDefsForAPI.getEnumDefs()) {
                        if (atd.getName().equals(atlasTypeName)) {
                            enumdef_matched = true;
                            omrsEnumDef = atd;
                            break;
                        }
                    }
                }
                if (enumdef_matched) {
                    // We found the OM EnumDef - refer to it from the TDA; it will already have been added to TDB so will go in the TDG.
                    LOG.debug("{}: located OM EnumDef with name {}", methodName, omrsEnumDef.getName());
                    omrsTypeDefAttribute.setAttributeType(omrsEnumDef);
                } else {
                    LOG.error("{}: could not locate OM EnuMDef with name {}", methodName, atlasTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName",methodName, metadataCollectionId);

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                break;

            default:

                LOG.error("{}: Could not handle Atlas attribute category {}", methodName, atlasCategory);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasTypeName, "guid-unknown", "atlasTypeName",methodName, metadataCollectionId);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }


        // Copy description
        omrsTypeDefAttribute.setAttributeDescription(aad.getDescription());

        /* Cardinality
         * Map the Atlas value to an OMRS value
         *
         * Atlas has Cardinality values (as below) plus isOptional (boolean):
         *        SINGLE
         *        LIST
         *        SET
         * OMRS AttributeCardinality is:
         *        UNKNOWN                (0, "<Unknown>",                "Unknown or uninitialized cardinality"),
         *        AT_MOST_ONE            (1, "At Most One",              "0..1 - Zero or one instances. 0..1."),
         *        ONE_ONLY               (2, "One Only",                 "1 - One instance, no more and no less"),
         *        AT_LEAST_ONE_ORDERED   (3, "At Least One (Ordered)",   "1..* - One or more instances (stored in specific order)"),
         *        AT_LEAST_ONE_UNORDERED (4, "At Least One (Unordered)", "1..* - One or more instances (stored in any order)"),
         *        ANY_NUMBER_ORDERED     (5, "Any Number (Ordered)",     "0..* - Any number of instances (stored in a specific order)"),
         *        ANY_NUMBER_UNORDERED   (6, "Any Number (Unordered)",   "0..* - Any number of instances (stored in any order)");
         *
         * The Atlas values map to the OMRS values follows:
         *    atlas                ->   omrs
         *  isOptional && SINGLE   ->  AT_MOST_ONE
         *  !isOptional && SINGLE  ->  ONE_ONLY
         *  !isOptional && LIST    ->  AT_LEAST_ONE_ORDERED
         *  !isOptional && SET     ->  AT_LEAST_ONE_UNORDERED
         *  isOptional && LIST     ->  ANY_NUMBER_ORDERED
         *  isOptional && SET      ->  ANY_NUMBER_UNORDERED
         *  any other combination  ->  UNKNOWN
         */
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = aad.getCardinality();
        AttributeCardinality omrsCardinality;
        boolean isOptional = aad.getIsOptional();
        switch (atlasCardinality) {
            case SINGLE:
                omrsCardinality = isOptional ? AT_MOST_ONE : ONE_ONLY;
                break;
            case LIST:
                omrsCardinality = isOptional ? ANY_NUMBER_ORDERED : AT_LEAST_ONE_UNORDERED;
                break;
            case SET:
                omrsCardinality = isOptional ? ANY_NUMBER_UNORDERED : AT_LEAST_ONE_UNORDERED;
                break;
            default:
                omrsCardinality = AttributeCardinality.UNKNOWN;
                break;
        }
        omrsTypeDefAttribute.setAttributeCardinality(omrsCardinality);

        // Map other fields
        omrsTypeDefAttribute.setValuesMinCount(aad.getValuesMinCount());
        omrsTypeDefAttribute.setValuesMaxCount(aad.getValuesMaxCount());
        omrsTypeDefAttribute.setIndexable(aad.getIsIndexable());
        omrsTypeDefAttribute.setUnique(aad.getIsUnique());
        omrsTypeDefAttribute.setDefaultValue(aad.getDefaultValue());
        omrsTypeDefAttribute.setExternalStandardMappings(null);

        LOG.debug("{}: OMRS TypeDefAttribute is {}", methodName, omrsTypeDefAttribute);
        return omrsTypeDefAttribute;
    }





}
