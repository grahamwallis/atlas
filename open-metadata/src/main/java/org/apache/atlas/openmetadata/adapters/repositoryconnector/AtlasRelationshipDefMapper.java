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
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus.ACTIVE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class AtlasRelationshipDefMapper {


    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipDefMapper.class);

    private LocalAtlasOMRSMetadataCollection metadataCollection;
    private String                           userId;
    private AtlasRelationshipDef             atlasRelationshipDef;
    private String                           metadataCollectionId;


    public AtlasRelationshipDefMapper(LocalAtlasOMRSMetadataCollection metadataCollection,
                                      String                           userId,
                                      AtlasRelationshipDef             atlasRelationshipDef)
            throws
            RepositoryErrorException
    {

        final String methodName = "AtlasRelationshipDefMapper";

        this.metadataCollection = metadataCollection;
        this.userId = userId;
        this.atlasRelationshipDef = atlasRelationshipDef;
        try {
            this.metadataCollectionId = this.metadataCollection.getMetadataCollectionId();
        } catch (RepositoryErrorException e) {
            LOG.error("{}: could not initialize metadataCollectionId", methodName, e);
            throw e;
        }
    }


    /**
     * Method to convert AtlasRelationshipDef to corresponding OM RelationshipDef
     * @return RelationshipDef
     * @throws TypeErrorException - the relationship type cannot be mapped
     */
    public RelationshipDef toOMRelationshipDef()
        throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "toOMRelationshipDef";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(atlasRelationshipDef={}", methodName, atlasRelationshipDef);
        }

        // Convert the AtlasRelationshipDef into the corresponding OM RelationshipDef and (implicitly) validate the content of the OM RelationshipDef


        OMRSRepositoryHelper repositoryHelper = metadataCollection.getRepositoryHelper();


        if (atlasRelationshipDef == null) {
            // Give up now
            LOG.error("{}: Cannot convert null AtlasRelationshipDef to OM RelationshipDef", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasRelationshipDef",  methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (atlasRelationshipDef.getName() == null || atlasRelationshipDef.getGuid() == null ) {
            // Give up now
            LOG.error("{}: Cannot convert AtlasRelationshipDef to OM RelationshipDef, name or guid is null", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasRelationshipDef",  methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        String typeName = atlasRelationshipDef.getName();

        // Convert Atlas type into a valid OM type

        // Allocate OMRS RelationshipDef
        RelationshipDef omRelationshipDef = new RelationshipDef();

        // Set common fields
        omRelationshipDef.setGUID(atlasRelationshipDef.getGuid());
        omRelationshipDef.setName(atlasRelationshipDef.getName());

        // Care needed with version field - in case it is null - Atlas uses Long, OM uses long
        Long version = atlasRelationshipDef.getVersion();
        if (version == null) {
            // Long (the wrapper class) can have null value, but long (the primitive class) cannot
            LOG.error("{}: Cannot convert AtlasEntityDef to OM RelationshipDef - version is null", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("null", "version", metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        omRelationshipDef.setVersion(version);

        omRelationshipDef.setVersionName(atlasRelationshipDef.getTypeVersion());

        // Additional fields on an AtlasRelationshipDef and OM RelationshipDef are ...

        // Copy across the fields that can be simply assigned
        omRelationshipDef.setDescription(atlasRelationshipDef.getDescription());
        omRelationshipDef.setCreatedBy(atlasRelationshipDef.getCreatedBy());
        omRelationshipDef.setUpdatedBy(atlasRelationshipDef.getUpdatedBy());
        omRelationshipDef.setCreateTime(atlasRelationshipDef.getCreateTime());
        omRelationshipDef.setUpdateTime(atlasRelationshipDef.getUpdateTime());
        omRelationshipDef.setOptions(atlasRelationshipDef.getOptions());

        // ------------------
        /* For those fields we will need to check whether the TypeDef is known by the RCM:
         * For all fields where nothing is stored in Atlas, use the RCM if possible to determine how to set the OM RelationshipDef
         * apart from the origin field - which is set to this metadataCollectionId.
         * There are a couple of cases to consider:
         *
         * 1. The first is where this is a known type (by name) in the RCM. In this case the RCM will have an opinion about values
         *    for the fields we cannot derive from the Atlas object. SO we use the RCM values; these should pass any subsequent
         *    object comparison (obviously).
         * 2. The second case is where this is a new type and is not yet known by the RCM. This is the situation where a type is introduced
         *    into the repository but not via OMRS, e.g. via the Atlas REST/UI interfaces. In this situation, we will not get a type back
         *    from the RCM; but it's OK - we can just adopt the defaults, which we will have anyway by virtue of having called the
         *    appropriate def constructor.
         */
        TypeDef rcmTypeDef = repositoryHelper.getTypeDefByName(metadataCollectionId, typeName);
        if (rcmTypeDef != null) {
            // If we didn't get a typedef from the RCM then we would adopt the defaults, but we can use the RCM values...
            if (rcmTypeDef.getCategory() != TypeDefCategory.RELATIONSHIP_DEF) {
                LOG.error("{}: The RepositoryContentManager has a type of this name that is not a RelationshipDef!", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, atlasRelationshipDef.getGuid(), "atlasRelationshipDef", methodName, metadataCollectionId, atlasRelationshipDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            RelationshipDef rcmRelationshipDef = (RelationshipDef) rcmTypeDef;
            List<ExternalStandardMapping> rcmExternalStandardMappings = rcmRelationshipDef.getExternalStandardMappings();
            List<InstanceStatus> rcmInstanceStatusList = rcmRelationshipDef.getValidInstanceStatusList();
            InstanceStatus rcmInitialStatus = rcmRelationshipDef.getInitialStatus();
            LOG.debug("{}: Fields acquired from RCM: validInstanceStatusList = {}, initialStatus = {}, externalStandardMappings = {}",
                  methodName, rcmInstanceStatusList, rcmInitialStatus, rcmExternalStandardMappings);

            // The origin is the metadataCollectionId for this connector.
            omRelationshipDef.setOrigin(this.metadataCollectionId);

            // Atlas does not have a field for external standard mappings. Consult the RCM to set up the OM RelationshipDef
            // Atlas does not have external standard mappings. OMRS ExternalStandardMappings ArrayList<ExternalStandardMapping>
            omRelationshipDef.setExternalStandardMappings(rcmExternalStandardMappings);

            // Atlas does not have a field for valid instance statuses. Consult the RCM to set up the OM RelationshipDef
            // Atlas does not have valid instance statuses. OMRS ValidInstanceStatusList ArrayList<InstanceStatus>
            omRelationshipDef.setValidInstanceStatusList(rcmInstanceStatusList);

            // Atlas does not have a field for initial status. Consult the RCM to set up the OM RelationshipDef
            omRelationshipDef.setInitialStatus(rcmInitialStatus);
        }
        else {
            // The RCM does not have the type - either the type is new or we are doing a verify following a
            // restart - so the type has been retrieved from Atlas but the RCM does not yet have it cached.
            // In either case we should adopt sensible default values - which for the most part is done by
            // the object constructor, but explicitly set the fields here. We need to do enough to get past
            // the equivalence check.

            // The origin is the metadataCollectionId for this connector.
            omRelationshipDef.setOrigin(this.metadataCollectionId);

            // Atlas does not have a field for external standard mappings. We can only assume here that this
            // should be set to null in what we return.
            omRelationshipDef.setExternalStandardMappings(null);

            // Atlas does not have a field for valid instance statuses. Set a sensible default, which assumes
            // that either ACTIVE or DELETED states are OK.
            ArrayList<InstanceStatus> statusList = new ArrayList<>();
            statusList.add(InstanceStatus.ACTIVE);
            statusList.add(InstanceStatus.DELETED);
            omRelationshipDef.setValidInstanceStatusList(statusList);

            // Atlas does not have a field for initial status. Default to ACTIVE
            omRelationshipDef.setInitialStatus(ACTIVE);
        }

        // Handle fields that require conversion - i.e. supertypes, entityTypes, attributeDefs
        // subtypes are deliberately ignored

        // superType is always forced to null - Atlas does not have superTypes on AtlasRelationshipDef
        omRelationshipDef.setSuperType(null);

        // Atlas subtypes are explicitly ignored in OM

        // Convert Atlas relationshipCategory to OM

        /*
         * If in future OM RelationshipDef supports different categories of relationship, the following conversion will be needed:
         *
         *   RelationshipCategory relCat = convertAtlasRelationshipCategoryToOMRelationshipCategory(atlasRelationshipDef.getRelationshipCategory());
         *   omRelationshipDef.setRelationshipCategory(relCat);
         *
         * Until then, if the Atlas relationship def is for anything other than an association then do not convert it...
         */
        AtlasRelationshipDef.RelationshipCategory atlasRelCat = atlasRelationshipDef.getRelationshipCategory();
        if (atlasRelCat == null || atlasRelCat != AtlasRelationshipDef.RelationshipCategory.ASSOCIATION) {
            String atlasRelCatAsString = "null";
            // Try to improve the diagnostic is possible...
            if (atlasRelCat != null) {
                switch (atlasRelCat) {
                    case AGGREGATION:
                        atlasRelCatAsString = "AGGREGATION";
                        break;
                    case COMPOSITION:
                        atlasRelCatAsString = "COMPOSITION";
                        break;
                }
            }
            LOG.error("{}: Cannot convert AtlasRelationshipDef with category {}", methodName, atlasRelCatAsString);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(atlasRelCatAsString, "atlasRelCat", metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }



        /* Convert the end defs
         * Note that the types of attributes is transposed between OM and Atlas, so we transpose the types of the ends.
         *
         * If in future OM RelationshipDef supports COMPOSITION and/or AGGREGATION, then we will need to discover which end is
         * the container (if either) to use later
         */
         boolean end1Container = false;
         boolean end2Container = false;

        /*
         * Get both ends now so that we can transpose the names, descriptions and cardinality when needed
         */

        // END1
        RelationshipEndDef ored1 = new RelationshipEndDef();
        AtlasRelationshipEndDef ared1 = atlasRelationshipDef.getEndDef1();

        // END2
        RelationshipEndDef ored2 = new RelationshipEndDef();
        AtlasRelationshipEndDef ared2 = atlasRelationshipDef.getEndDef2();

        if (ared1 == null || ared2 == null) {
            LOG.error("{}: Could not resolve AtlasRelationshipDef with missing end defs; endDef1 = {}, endDef2 = {}", methodName, ared1, ared2);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(null, ared1 == null ? "relationship endDef1" : "relationship endDef2" ,
                    "RelationshipEndDef",  methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Process END1

        /*
         * If OM RelationshipDef supports COMPOSITION or AGGREGATION, will need to know which end is the container
         */
         if (ared1.getIsContainer())
             end1Container = true;


        /* Entity Type
         * First find out if the entity type is known - if not we cannot convert the rel def.
         * If it is a known entity type then produce a TDL for it....
         */

        TypeDefLink omrsTypeDefLink1 = null;
        String entityTypeName1 = ared1.getType();

        // Famous Five
        if (FamousFive.atlasTypeRequiresSubstitution(entityTypeName1)) {
            // entity type is in famous five
            LOG.debug("{}: famous five entity type {}", methodName, entityTypeName1);
            String omEntityTypeName1 = FamousFive.getOMTypeName(entityTypeName1);
            String omEntityTypeGUID1 = null;
            if (omEntityTypeName1 != null) {
                omEntityTypeGUID1 = FamousFive.getRecordedGUID(omEntityTypeName1);
                TypeDefLink omEntityType1 = new TypeDefLink();
                omEntityType1.setName(omEntityTypeName1);
                omEntityType1.setGUID(omEntityTypeGUID1);
                omrsTypeDefLink1 = omEntityType1;
            }
            if (omEntityTypeName1 == null || omEntityTypeGUID1 == null) {
                LOG.error("{}: Could not resolve entity type with name {} for AtlasEntityDef", methodName, entityTypeName1);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeName1, "guid unknown",
                        "relationshipEnd1",  methodName, metadataCollectionId, entityTypeName1);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        else {
            // not Famous Five
            omrsTypeDefLink1 = metadataCollection.constructTypeDefLink(entityTypeName1, TypeCategory.ENTITY);
            if (omrsTypeDefLink1 == null) {
                LOG.error("{}: Could not resolve type with name {} for AtlasEntityDef", methodName, entityTypeName1);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, atlasRelationshipDef.getGuid(),
                        "atlasRelationshipDef",  methodName, metadataCollectionId, atlasRelationshipDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        // We found an EntityDef with the desired name in our own cache
        ored1.setEntityType(omrsTypeDefLink1);


        // The attribute names, descriptions and cardinality must be swapped end to end when converting between OM and Atlas, or vice versa
        ored1.setAttributeName(ared2.getName());               // attribute names are deliberately transposed between OM and Atlas
        ored1.setAttributeDescription(ared2.getDescription()); // attribute descriptions are deliberately transposed between OM and Atlas

        /* Cardinality
         *
         * Cardinality is mapped from Atlas Cardinality { SINGLE, LIST, SET } to OM AttributeCardinality
         * { AT_MOST_ONE, ANY_NUMBER_UNORDERED } which correspond to 0..1 and 0..* respectively.
         * A relationship end def is always 'optional' and always UNORDERED. This allows the creation of a
         * relationship between a pair of entities as a one-to-one, one-to-many, many-to-one, many-to-many.
         */
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality1 = ared2.getCardinality();    // attribute cardinality deliberately transposed between OM and Atlas
        RelationshipEndCardinality omrsCardinality1;
        switch (atlasCardinality1) {
            case SINGLE:
                omrsCardinality1 = RelationshipEndCardinality.AT_MOST_ONE;
                break;

            case SET:
                omrsCardinality1 = RelationshipEndCardinality.ANY_NUMBER;
                break;

            default:
                /* Any other cardinality is unexpected - all OM RelationshipDefs are associations with optional, unordered ends.
                 * There is no sensible way to proceed here.
                 */
                LOG.error("{}: Atlas end cardinality {} for relationship end def 1 in relationship def {} is not supported by open metadata ", methodName, atlasCardinality1, atlasRelationshipDef.getName());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("atlasCardinality1", "Value not supported",
                        "atlasCardinality1",  methodName, metadataCollectionId, "end1 cardinality");

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        ored1.setAttributeCardinality(omrsCardinality1);

        // Add the OM relationship end def to the relationship def
        omRelationshipDef.setEndDef1(ored1);


        // Process END2

        /*
         * If OM RelationshipDef supports COMPOSITION or AGGREGATION, will need to know which end is the container
         */
         if (ared2.getIsContainer())
             end2Container = true;



        /* Entity Type
         * First find out if the entity type is known - if not we cannot convert the rel def.
         * If it is a known entity type then produce a TDL for it....
         */

        TypeDefLink omrsTypeDefLink2 = null;
        String entityTypeName2 = ared2.getType();

        // Famous Five
        if (FamousFive.atlasTypeRequiresSubstitution(entityTypeName2)) {
            // entity type is in famous five
            LOG.debug("{}: famous five entity type {}", methodName, entityTypeName2);
            String omEntityTypeName2 = FamousFive.getOMTypeName(entityTypeName2);
            String omEntityTypeGUID2 = null;
            if (omEntityTypeName2 != null) {
                omEntityTypeGUID2 = FamousFive.getRecordedGUID(omEntityTypeName2);
                TypeDefLink omEntityType2 = new TypeDefLink();
                omEntityType2.setName(omEntityTypeName2);
                omEntityType2.setGUID(omEntityTypeGUID2);
                omrsTypeDefLink2 = omEntityType2;
            }
            if (omEntityTypeName2 == null || omEntityTypeGUID2 == null) {
                LOG.error("{}: Could not resolve entity type with name {} for AtlasEntityDef", methodName, entityTypeName2);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeName2, "guid unknown",
                        "relationshipEnd2",  methodName, metadataCollectionId, entityTypeName2);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        else {
            // not Famous Five
            omrsTypeDefLink2 = metadataCollection.constructTypeDefLink(entityTypeName2, TypeCategory.ENTITY);
            if (omrsTypeDefLink2 == null) {
                LOG.error("{}: Could not resolve type with name {} for AtlasEntityDef", methodName, entityTypeName2);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, atlasRelationshipDef.getGuid(),
                        "atlasRelationshipDef",  methodName, metadataCollectionId, atlasRelationshipDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        // We found an EntityDef with the desired name in our own cache
        ored2.setEntityType(omrsTypeDefLink2);


        // Note that attribute names and cardinality must be swapped end to end when converting between OM and Atlas, or vice versa
        ored2.setAttributeName(ared1.getName());     // attribute names are deliberately transposed between OM and Atlas
        ored2.setAttributeDescription(ared1.getDescription()); // attribute descriptions are deliberately transposed between OM and Atlas

        /* Cardinality
         *
         * Cardinality is mapped from Atlas Cardinality { SINGLE, LIST, SET } to OM AttributeCardinality
         * { AT_MOST_ONE, ANY_NUMBER_UNORDERED } which correspond to 0..1 and 0..* respectively.
         * A relationship end def is always 'optional' and always UNORDERED. This allows the creation of a
         * relationship between a pair of entities as a one-to-one, one-to-many, many-to-one, many-to-many.
         */
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality2 = ared1.getCardinality();    // attribute cardinality deliberately transposed between OM and Atlas
        RelationshipEndCardinality omrsCardinality2;
        switch (atlasCardinality2) {
            case SINGLE:
                omrsCardinality2 = RelationshipEndCardinality.AT_MOST_ONE;
                break;

            case SET:
                omrsCardinality2 = RelationshipEndCardinality.ANY_NUMBER;
                break;

            default:
                /* Any other cardinality is unexpected - all OM RelationshipDefs are associations with optional, unordered ends.
                 * There is no sensible way to proceed here.
                 */
                LOG.error("{}: Atlas end cardinality {} for relationship end def 2 in relationship def {} is not supported by open metadata ", methodName, atlasCardinality2, atlasRelationshipDef.getName());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("atlasCardinality2", "Value not supported",
                        "atlasCardinality2",  methodName, metadataCollectionId, "end2 cardinality");

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        ored2.setAttributeCardinality(omrsCardinality2);

        // Add the OM relationship end def to the relationship def
        omRelationshipDef.setEndDef2(ored2);


        /*
         * If in future OM RelationshipDefs support COMPOSITION and/or AGGREGATION then a conversion
         * will be needed to set the container end information
         *
         * Until then, if either end is marked as a container then this is something that is not modelled in OM
         * so fail the conversion...
         */
        if (end1Container || end2Container) {
            LOG.error("{}: For an association, neither end should be a container", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, atlasRelationshipDef.getGuid(),
                    "atlasRelationshipDef",  methodName, metadataCollectionId, atlasRelationshipDef.toString());

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Convert propagateTags to propagationRule
        AtlasRelationshipDef.PropagateTags atlasPropTags = atlasRelationshipDef.getPropagateTags();
        boolean propTagsMissingOrInvalid = false;
        if (atlasPropTags == null) {
            LOG.error("{}: could not convert AtlasRelationshipDef, propagateTags is null", methodName);
            propTagsMissingOrInvalid = true; // handle below
        }
        else {
             switch (atlasPropTags) {
                 case NONE:
                     omRelationshipDef.setPropagationRule(ClassificationPropagationRule.NONE);
                     break;
                 case ONE_TO_TWO:
                     omRelationshipDef.setPropagationRule(ClassificationPropagationRule.ONE_TO_TWO);
                     break;
                 case TWO_TO_ONE:
                     omRelationshipDef.setPropagationRule(ClassificationPropagationRule.TWO_TO_ONE);
                     break;
                 case BOTH:
                     omRelationshipDef.setPropagationRule(ClassificationPropagationRule.BOTH);
                     break;
                 default:
                     LOG.error("{}: could not convert Atlas propagateTags value {}", methodName, atlasPropTags);
                     propTagsMissingOrInvalid = true; // handle below
             }
         }
         if (propTagsMissingOrInvalid) {
             OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
             String errorMessage = errorCode.getErrorMessageId()
                     + errorCode.getFormattedErrorMessage(typeName, "getPropagateTags",
                     "atlasRelationshipDef",  methodName, metadataCollectionId, atlasRelationshipDef.toString());

             throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                     this.getClass().getName(),
                     methodName,
                     errorMessage,
                     errorCode.getSystemAction(),
                     errorCode.getUserAction());
         }

        // Handle Attribute Defs
        LOG.debug("{}: attributeDefs {}", methodName, atlasRelationshipDef.getAttributeDefs());
        try {
            List<TypeDefAttribute> omrsTypeDefAttributes =
                    metadataCollection.convertAtlasAttributeDefs(userId, atlasRelationshipDef.getAttributeDefs());
            LOG.debug("{}: omrsTypeDefAttributes={}", methodName, omrsTypeDefAttributes);
            omRelationshipDef.setPropertiesDefinition(omrsTypeDefAttributes);
        }
        catch (RepositoryErrorException | TypeErrorException e) {
            // Failed to process the attribute types; give up trying to convert this entity def
            // Give up at this point by returning without having added the OM def to typeDefsForAPI
            LOG.debug("{}: Failed to convert attributes of AtlasRelationshipDef", methodName);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(omRelationshipDef={}", methodName, omRelationshipDef);
        }

        return omRelationshipDef;
    }


}