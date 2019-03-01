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

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.InvalidEntityException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotKnownException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefCategory.CLASSIFICATION_DEF;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefCategory.ENTITY_DEF;


// package private
class AtlasEntityMapper {

    private static final Logger LOG = LoggerFactory.getLogger(LocalAtlasOMRSMetadataCollection.class);

    /**
     *  Mapper with methods for different output objects EntityDetail, EntityProxy and EntitySummary
     *
     *  To use this class, construct a new AtlasEntityMapper and then invoke the
     *  method corresponding to the type of OM entity object you want as output.
     */

    /*
     * AtlasEntity has:
     *
     * String                    typeName
     * Map<String, Object>       attributes
     * String                    guid
     * Status                    status
     * String                    createdBy
     * String                    updatedBy
     * Date                      createTime
     * Date                      updateTime
     * Long                      version
     * Map<String, Object>       relationshipAttributes;
     * List<AtlasClassification> classifications;
     *
     * EntityDetail has:
     *
     * InstanceType              type
     * String                    createdBy
     * String                    updatedBy
     * Date                      createTime
     * Date                      updateTime
     * Long                      version
     * InstanceStatus            currentStatus
     * InstanceStatus            statusOnDelete
     * InstanceProvenanceType    instanceProvenanceType
     * String                    metadataCollectionId
     * String                    guid
     * String                    instanceURL
     * ArrayList<Classification> classifications
     *
     * We need to retrieve the AtlasEntity's type (by name) as a TypeDef from the repository.
     * This can be converted into an OM EntityDef - which can then be used to construct the
     * InstanceType.
     *
     */

    private LocalAtlasOMRSMetadataCollection metadataCollection;
    private String                           metadataCollectionId;
    private AtlasEntity                      atlasEntity;
    private String                           userId;
    private EntityDef                        entityDef;

    /**
     * AtlasEntityMapper converts an AtlasEntity to an OM Entity - choice of output formats
     * @param metadataCollection - the metadataCollection of the repository connector
     * @param userId             - the security context of the operation
     * @param atlasEntity        - the AtlasEntity to be converted
     * @throws TypeErrorException        - if conversion fails due to type errors
     * @throws RepositoryErrorException  - if conversion fails due to repository malfunction
     */

    // package private
    AtlasEntityMapper(LocalAtlasOMRSMetadataCollection metadataCollection,
                      String                           userId,
                      AtlasEntity                      atlasEntity)
        throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "AtlasEntityMapper";

        LOG.debug("{}: atlasEntity {}", methodName, atlasEntity);

        this.metadataCollection = metadataCollection;
        try {
            metadataCollectionId = metadataCollection.getMetadataCollectionId();
        }
        catch (RepositoryErrorException e) {
            LOG.error("{}: caught repository exception - could not get metadataCollectionId", methodName, e);
            // Re-throw the error
            throw e;
        }

        LOG.debug("{}: metadataCollectionId {}", methodName, metadataCollectionId);

        if (atlasEntity == null) {

            LOG.error("{}: atlasEntity is null", methodName);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.NULL_INSTANCE;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasEntity");

            LOG.error("{}: {}", methodName, errorMessage);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        this.atlasEntity = atlasEntity;
        this.userId = userId;

        try {
            this.entityDef = getEntityDef(atlasEntity);
        }
        catch (TypeErrorException e) {
            LOG.error("{}: caught type exception - could not retrieve entity def for typename {}", methodName, atlasEntity.getTypeName(),e);
            // Re-throw - exception will be caught in the MDC
            throw e;
        }

    }

    // package private
    EntitySummary toEntitySummary() throws TypeErrorException, InvalidEntityException {
        if (this.atlasEntity == null) {
            return null;
        }
        EntitySummary entitySummary = new EntitySummary();
        completeEntitySummary(entitySummary);
        return entitySummary;
    }

    // package private
    EntityProxy toEntityProxy() throws TypeErrorException, InvalidEntityException {
        if (this.atlasEntity == null) {
            return null;
        }
        EntityProxy entityProxy = new EntityProxy();
        completeEntityProxy(entityProxy);
        return entityProxy;
    }

    // package private
    EntityDetail toEntityDetail() throws TypeErrorException, InvalidEntityException {
        if (this.atlasEntity == null) {
            return null;
        }
        EntityDetail entityDetail = new EntityDetail();
        completeEntityDetail(entityDetail);
        return entityDetail;
    }


    private void completeEntitySummary(EntitySummary entitySummary)
        throws
            TypeErrorException, InvalidEntityException
    {

        final String methodName = "completeEntitySummary";

        InstanceType instanceType = createInstanceType(entityDef);

        // Construct an EntitySummary object
        // Set fields from InstanceAuditHeader
        entitySummary.setType(instanceType);
        entitySummary.setCreatedBy(atlasEntity.getCreatedBy());
        entitySummary.setCreateTime(atlasEntity.getCreateTime());
        entitySummary.setUpdatedBy(atlasEntity.getUpdatedBy());
        entitySummary.setUpdateTime(atlasEntity.getUpdateTime());

        // Care needed with version field - in case it is null - Atlas uses Long, OM uses long
        Long version = atlasEntity.getVersion();
        if (version == null) {
            // Long (the wrapper class) can have null value, but long (the primitive class) cannot
            LOG.error("{}: Cannot convert AtlasEntity to OM EntitySummary - version is null", methodName);
            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.NULL_VERSION;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage( "atlasEntity version", metadataCollectionId);

            throw new InvalidEntityException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("{}: set version in EntitySummary to {}", methodName, version);
        entitySummary.setVersion(version);


        if (atlasEntity.getStatus() == AtlasEntity.Status.ACTIVE) {
            entitySummary.setStatus(InstanceStatus.ACTIVE);
        } else {
            entitySummary.setStatus(InstanceStatus.DELETED);
        }

        // Set fields from InstanceHeader
        entitySummary.setMetadataCollectionId(atlasEntity.getHomeId());

        InstanceProvenanceType instanceProvenanceType = mapProvenanceOrdinalToEnum(atlasEntity.getProvenanceType());
        entitySummary.setInstanceProvenanceType(instanceProvenanceType);

        entitySummary.setGUID(atlasEntity.getGuid());
        entitySummary.setInstanceURL(null);

        // Set fields from EntitySummary

        // Set classifications
        // AtlasEntity provides a List<AtlasClassification>
        List<AtlasClassification> atlasClassifications = atlasEntity.getClassifications();
        ArrayList<Classification> omClassifications = null;
        if (atlasClassifications != null) {
            omClassifications = new ArrayList<>();
            for (AtlasClassification atlasClassification : atlasClassifications) {
                LOG.debug("{}: processing classification {}", methodName, atlasClassification);
                Classification omClassification = convertAtlasClassificationToOMClassification(atlasClassification);
                omClassifications.add(omClassification);
            }
        }
        entitySummary.setClassifications(omClassifications);

    }

    private void completeEntityProxy(EntityProxy entityProxy)
        throws
            TypeErrorException, InvalidEntityException

    {
        completeEntitySummary(entityProxy);

        /*
         * Add the EntityProxy portion...
         * Take only the unique attributes from AtlasEntity and set the entityProperties for OM EntityProxy
         */

        Map<String, Object> atlasAttrs = atlasEntity.getAttributes();
        AtlasAttributeMapper atlasAttributeMapper = new AtlasAttributeMapper(metadataCollection, userId);
        InstanceProperties instanceProperties = atlasAttributeMapper.convertAtlasAttributesToOMProperties(entityDef, atlasAttrs, true); // uniqueOnly == true => unique attributes only
        entityProxy.setUniqueProperties(instanceProperties);

    }

    private void completeEntityDetail(EntityDetail entityDetail)
        throws
            TypeErrorException, InvalidEntityException
    {
        final String methodName = "completeEntityDetail";

        LOG.debug("{}: atlasEntity = {}", methodName, atlasEntity);
        LOG.debug("{}: isProxy = {}", methodName, atlasEntity.isProxy());

        if (atlasEntity.isProxy() != null && atlasEntity.isProxy()) {
            // This is only a proxy entity - you cannot create an EntityDetail (or any subtype of EntityDetail) from it.
            LOG.error("{}: the AtlasEntity with GUID {} is a proxy - it cannot be used as EntityDetail ", methodName, atlasEntity.getGuid());
            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_DELETED;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(atlasEntity.getGuid(), methodName, metadataCollectionId);

            throw new InvalidEntityException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        completeEntitySummary(entityDetail);

        /*
         * Add the EntityDetail portion...
         * Take all the attributes from AtlasEntity and set the entityProperties for OM EntityDetail
         */
        Map<String, Object> atlasAttrs = atlasEntity.getAttributes();
        AtlasAttributeMapper atlasAttributeMapper = new AtlasAttributeMapper(metadataCollection, userId);
        InstanceProperties instanceProperties = atlasAttributeMapper.convertAtlasAttributesToOMProperties(entityDef, atlasAttrs, false); // uniqueOnly == false => all attributes

        entityDetail.setProperties(instanceProperties);
    }

    public List<Relationship> getEntityRelationships()
            throws
            TypeErrorException
    {

        final String methodName = "getEntityRelationships";

        ArrayList<Relationship> relationships = null;

        Map<String, Object> atlasEntRelAttrs = atlasEntity.getRelationshipAttributes();
        if (atlasEntRelAttrs != null && !(atlasEntRelAttrs.isEmpty())) {
            relationships = new ArrayList<>();
            for (String key : atlasEntity.getRelationshipAttributes().keySet()) {
                Object obj = atlasEntRelAttrs.get(key);
                // If the obj is null then it represents an absence of relationships of this type, so tolerate/ignore it
                if (obj != null) {

                    if (obj instanceof AtlasRelatedObjectId) {
                        // The relationshipAttribute is for a single relationship to another entity...
                        LOG.debug("{}: relationship attribute is AtlasRelatedObjectId {}", methodName, obj);
                        AtlasRelatedObjectId aroId = (AtlasRelatedObjectId) obj;
                        Relationship relationship = convertAtlasRelatedObjectIdToOMRelationship(aroId);
                        if (relationship != null) {
                            relationships.add(relationship);
                        }

                    } else if (obj instanceof ArrayList) {
                        /*
                         * The relationshipAttribute is for an array of relationships to other entities,
                         * each defined by an AtlasRelatedObjectId.
                         */
                        ArrayList list = (ArrayList) obj;
                        if (!list.isEmpty()) {
                            for (Object element : list) {
                                // Make no assumptions about element type...
                                if (element instanceof AtlasRelatedObjectId) {
                                    AtlasRelatedObjectId aroId = (AtlasRelatedObjectId) element;
                                    LOG.debug("{}: relationship contains AtlasRelatedObjectId {}", methodName, aroId);
                                    Relationship relationship = convertAtlasRelatedObjectIdToOMRelationship(aroId);
                                    if (relationship != null) {
                                        relationships.add(relationship);
                                    }
                                }
                            }
                        }

                    } else {
                        LOG.error("{}: relationship attribute with key {} is of unsupported type {} ", methodName, key, obj.getClass());
                        OMRSErrorCode errorCode = OMRSErrorCode.ATTRIBUTE_TYPEDEF_NOT_KNOWN;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(key, "attribute key", methodName, metadataCollectionId);

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                }
                else {
                    // Record that we have seen a null object, but do not treat it as an error
                    LOG.debug("{}: relationship attribute with key {} has null value", methodName, key);
                }
            }

        }
        //entityUniverse.setEntityRelationships(relationships);
        return relationships;

    }

    private Relationship convertAtlasRelatedObjectIdToOMRelationship(AtlasRelatedObjectId aroId)
    {

        final String methodName = "convertAtlasRelatedObjectIdToOMRelationship";
        String relationshipGuid = aroId.getRelationshipGuid();
        Relationship relationship;
        try {
            relationship = metadataCollection.getRelationship(userId, relationshipGuid);
        } catch (Exception e) {
            LOG.debug("{}: Caught exception from getRelationship {}", methodName, e);
            relationship = null;
        }
        return relationship;
    }

    /**
     * Helper method to create InstanceType from an EntityDef
     * @param entityDef - the type definition from which the instance type is generated
     * @return instanceType
     */
    private InstanceType createInstanceType(EntityDef entityDef)
    {
        final String methodName = "createInstanceType";

        /*
         * Create an instance type - this uses a combination of things from the entity type and the atlas entity.
         * An OM EntityDef only has one superType - so retrieve it and wrap into a list of length one...
         */
        ArrayList<TypeDefLink> listSuperTypes = new ArrayList<>();
        if (entityDef.getSuperType() != null) {
            listSuperTypes.add(entityDef.getSuperType());
        }

        // Collate the valid instance properties
        EntityDefMapper entityDefMapper = new EntityDefMapper(metadataCollection, userId, entityDef);
        ArrayList<String> validInstanceProperties = entityDefMapper.getValidPropertyNames();

        InstanceType instanceType = new InstanceType(
                entityDef.getCategory(),
                entityDef.getGUID(),
                entityDef.getName(),
                entityDef.getVersion(),
                entityDef.getDescription(),
                entityDef.getDescriptionGUID(),
                listSuperTypes,
                entityDef.getValidInstanceStatusList(),
                validInstanceProperties);

        LOG.debug("{}: InstanceType is {}", methodName, instanceType);
        return instanceType;
    }

    /**
     * Utility method to parse AtlasClassification into an OM Classification.
     *
     * @param atlasClassification - the AtlasClassification to convert
     * @return Classification     - converted from AtlasClassification
     */
    private Classification convertAtlasClassificationToOMClassification(AtlasClassification atlasClassification)
        throws
            TypeErrorException
    {

        String methodName = "convertAtlasClassificationToOMClassification";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(atlasClassification={})", methodName, atlasClassification);
        }

        if (atlasClassification == null) {
            return null;
        }

        TypeDef typeDef;
        String typeName = atlasClassification.getTypeName();
        try {
            typeDef = metadataCollection._getTypeDefByName(userId, typeName);
        }
        catch (Exception e) {
            // Catch this and handle below
            typeDef = null;
        }
        if (typeDef == null || typeDef.getCategory() != CLASSIFICATION_DEF) {
            LOG.error("{}: could not retrieve typedef from Classification Def from store by name {} ", methodName, typeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, "ClassificationDef", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        ClassificationDef classificationDef;
        try {
            classificationDef = (ClassificationDef) typeDef;
        }
        catch (ClassCastException e) {
            LOG.error("{}: TypeDef with name {} is not a ClassificationDef", methodName, typeName);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, "unknown", "classificationTypeName", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        Classification omClassification = new Classification();
        /* To convert from Atlas to OM:
         * Atlas:
         * entityGuid      ignored
         * propagate       ignored - in OM this is set in ClassificationDef.propagatable rather than on the instance
         * validityPeriod  ignored - for now - there is no support for this in OM
         * typeName        use this to look up ClassificationDef
         *                 this in turn will let you construct the InstanceType for the OM Classification
         * attributes      translate these across to Classification.classificationProperties
         * xxx             origin - this indicates whether the classification was assigned or propagated, but this is not known from AC
         * xxx             originGUID - this is the GUID of the entity from where the classification propagated, but this is not known from AC
         */

        // The OM classificationName is set to the name of the AtlasClassificationDef - e.g. "Confidentiality"
        omClassification.setName(typeName);

        // Copy the classification attributes across from Atlas to OM
        Map<String, Object> atlasClassAttrs = atlasClassification.getAttributes();
        AtlasAttributeMapper atlasAttributeMapper = new AtlasAttributeMapper(metadataCollection, userId);
        InstanceProperties omClassProps = atlasAttributeMapper.convertAtlasAttributesToOMProperties(classificationDef, atlasClassAttrs, false); // uniqueOnly == false => all attributes
        omClassification.setProperties(omClassProps);


        // Clear the classification origin info. Cannot tell from Atlas classification if it was assigned or propagated.
        omClassification.setClassificationOrigin(null);
        omClassification.setClassificationOriginGUID(null);

        /*
         * InstanceType
         * Create an InstanceType for the classification
         */

        /* Supertypes - an OM ClassificationDef only has one superType - if there is one,
         * retrieve it and wrap into a list of length one...
         */
        TypeDefLink superType = classificationDef.getSuperType();
        ArrayList<TypeDefLink> listSuperTypes = null;
        if (superType != null) {
            listSuperTypes = new ArrayList<>();
            listSuperTypes.add(superType);
        }

        /* ValidInstanceProperties
         * Walk the supertype hierarchy and find all possible instance properties.
         */
        List<TypeDefAttribute> allClassificationProperties;
        try {
            allClassificationProperties = metadataCollection.getAllDefinedProperties(userId, classificationDef);
        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("ClassificationDef", "ClassificationDef", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        ArrayList<String> validInstProps = null;
        if (allClassificationProperties != null) {
            // Convert allClassificationProperties into a list of string property keys...
            validInstProps = new ArrayList<>();
            for (TypeDefAttribute classificationProperty : allClassificationProperties) {
                String classificationPropertyName = classificationProperty.getAttributeName();
                validInstProps.add(classificationPropertyName);
            }
        }

        InstanceType instanceType = new InstanceType(
                classificationDef.getCategory(),
                classificationDef.getGUID(),
                classificationDef.getName(),
                classificationDef.getVersion(),
                classificationDef.getDescription(),
                classificationDef.getDescriptionGUID(),
                listSuperTypes,
                classificationDef.getValidInstanceStatusList(),
                validInstProps);

        omClassification.setType(instanceType);

        // Set the other fields from InstanceAuditHeader
        omClassification.setCreatedBy(classificationDef.getCreatedBy());
        omClassification.setUpdatedBy(classificationDef.getUpdatedBy());
        omClassification.setCreateTime(classificationDef.getCreateTime());
        omClassification.setUpdateTime(classificationDef.getUpdateTime());
        omClassification.setStatus(InstanceStatus.ACTIVE);


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(omClassification={})", methodName, omClassification);
        }
        return omClassification;
    }

    private EntityDef getEntityDef(AtlasEntity atlasEntity)
            throws
            TypeErrorException
    {

        final String methodName = "getEntityDef";

        // Find the entityDef for the specified entity
        String entityTypeName = atlasEntity.getTypeName();
        TypeDef typeDef;
        try {

            typeDef = metadataCollection._getTypeDefByName(userId, entityTypeName);

        } catch (TypeDefNotKnownException | RepositoryErrorException e) {
            LOG.error("{}: caught exception from attempt to locate typedef for type {}", methodName, entityTypeName, e);
            // handle below
            typeDef = null;
        }
        if (typeDef == null || typeDef.getCategory() != ENTITY_DEF) {
            LOG.error("{}: could not find typedef for type {}", methodName, entityTypeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeName, "EntityDef", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        EntityDef entityDef;
        try {
            entityDef = (EntityDef) typeDef;
        }
        catch (ClassCastException e) {
            LOG.error("{}: TypeDef with name {} is not an EntityDef", methodName, entityTypeName, e);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeName, "unknown", "entityTypeName", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("{}: EntityDef found {}", methodName, entityDef);
        return entityDef;
    }


    private InstanceProvenanceType mapProvenanceOrdinalToEnum(Integer provenanceOrdinal){
        if (provenanceOrdinal == null) {
            return InstanceProvenanceType.UNKNOWN;
        }
        InstanceProvenanceType instanceProvenanceType;
        switch (provenanceOrdinal) {
            case 0:
                instanceProvenanceType = InstanceProvenanceType.UNKNOWN;
                break;
            case 1:
                instanceProvenanceType = InstanceProvenanceType.LOCAL_COHORT;
                break;
            case 2:
                instanceProvenanceType = InstanceProvenanceType.EXPORT_ARCHIVE;
                break;
            case 3:
                instanceProvenanceType = InstanceProvenanceType.CONTENT_PACK;
                break;
            case 4:
                instanceProvenanceType = InstanceProvenanceType.DEREGISTERED_REPOSITORY;
                break;
            case 5:
                instanceProvenanceType = InstanceProvenanceType.DATA_PLATFORM;
                break;
            case 6:
                instanceProvenanceType = InstanceProvenanceType.EXTERNAL_ENGINE;
                break;
            case 7:
                instanceProvenanceType = InstanceProvenanceType.EXTERNAL_TOOL;
                break;
            default:
                instanceProvenanceType = InstanceProvenanceType.UNKNOWN;
                break;
        }
        return instanceProvenanceType;
    }

}
