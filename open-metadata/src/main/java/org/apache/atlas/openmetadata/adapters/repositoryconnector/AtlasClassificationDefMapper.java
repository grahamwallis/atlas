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
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus.ACTIVE;


public class AtlasClassificationDefMapper {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasClassificationDefMapper.class);

    private LocalAtlasOMRSMetadataCollection metadataCollection;
    private String                           userId;
    private AtlasClassificationDef           atlasClassificationDef;
    private String                           metadataCollectionId;


    public AtlasClassificationDefMapper(LocalAtlasOMRSMetadataCollection metadataCollection,
                                        String                           userId,
                                        AtlasClassificationDef           atlasClassificationDef)
            throws
            RepositoryErrorException
    {
        final String methodName     = "AtlasClassificationDefMapper";
        this.metadataCollection     = metadataCollection;
        this.userId                 = userId;
        this.atlasClassificationDef = atlasClassificationDef;

        try {
            this.metadataCollectionId = this.metadataCollection.getMetadataCollectionId();
        } catch (RepositoryErrorException e) {
            LOG.error("{}: could not initialize metadataCollectionId", methodName, e);
            throw e;
        }
    }


    public ClassificationDef toOMClassificationDef()
        throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "toOMClassificationDef";

        // Convert an AtlasClassificationDef to an OM ClassificationDef
        //
        // Convert the AtlasClassificationDef into the corresponding OM ClassificationDef and (implicitly)
        // validate the content of the OM ClassificationDef
        //

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(atlasClassificationDef={}", methodName, atlasClassificationDef);
        }


        OMRSRepositoryHelper repositoryHelper = metadataCollection.getRepositoryHelper();

        if (atlasClassificationDef == null) {
            // Give up now
            LOG.error("{}: Cannot convert null AtlasClassificationDef to OM ClassificationDef", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasClassificationDef", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (atlasClassificationDef.getName() == null || atlasClassificationDef.getGuid() == null ) {
            // Give up now
            LOG.error("{}: Cannot convert AtlasRelationshipDef to OM ClassificationDef, name or guid is null", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasClassificationDef", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        String typeName = atlasClassificationDef.getName();

        // Convert Atlas type into a valid OM type

        // Allocate OMRS ClassificationDef, which will set AttributeTypeDefCategory automatically
        ClassificationDef omClassificationDef = new ClassificationDef();

        // Set common fields
        omClassificationDef.setGUID(atlasClassificationDef.getGuid());
        omClassificationDef.setName(atlasClassificationDef.getName());

        // Care needed with version field - in case it is null - Atlas uses Long, OM uses long
        Long version = atlasClassificationDef.getVersion();
        if (version == null) {
            // Long (the wrapper class) can have null value, but long (the primitive class) cannot
            LOG.error("{}: Cannot convert AtlasEntityDef to OM ClassificationDef - version is null", methodName);
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
        omClassificationDef.setVersion(version);

        omClassificationDef.setVersionName(atlasClassificationDef.getTypeVersion());

        // Additional fields on an AtlasClassificationDef and OM ClassificationDef are ...

        // Copy across the fields that can be simply assigned
        omClassificationDef.setDescription(atlasClassificationDef.getDescription());
        omClassificationDef.setCreatedBy(atlasClassificationDef.getCreatedBy());
        omClassificationDef.setUpdatedBy(atlasClassificationDef.getUpdatedBy());
        omClassificationDef.setCreateTime(atlasClassificationDef.getCreateTime());
        omClassificationDef.setUpdateTime(atlasClassificationDef.getUpdateTime());
        omClassificationDef.setOptions(atlasClassificationDef.getOptions());


        /* For those fields we will need to check whether the TypeDef is known by the RCM:
         * For all fields where nothing is stored in Atlas, use the RCM if possible to determine how to set the OM ClassificationDef
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
            if (rcmTypeDef.getCategory() != TypeDefCategory.CLASSIFICATION_DEF) {
                LOG.error("{}: The RepositoryContentManager has a type of this name that is not a ClassificationDef!", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, atlasClassificationDef.getGuid(), "atlasClassificationDef", methodName, metadataCollectionId, atlasClassificationDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            ClassificationDef rcmClassificationDef = (ClassificationDef) rcmTypeDef;
            boolean rcmPropagatable = rcmClassificationDef.isPropagatable();
            List<ExternalStandardMapping> rcmExternalStandardMappings = rcmClassificationDef.getExternalStandardMappings();
            List<InstanceStatus> rcmInstanceStatusList = rcmClassificationDef.getValidInstanceStatusList();
            InstanceStatus rcmInitialStatus = rcmClassificationDef.getInitialStatus();
            LOG.debug("{}: Fields acquired from RCM: propagatable = {}, validInstanceStatusList = {}, initialStatus = {}, externalStandardMappings = {}",
                    methodName, rcmPropagatable, rcmInstanceStatusList, rcmInitialStatus, rcmExternalStandardMappings);

            // The origin is the metadataCollectionId for this connector.
            omClassificationDef.setOrigin(this.metadataCollectionId);

            // Atlas does not have a field for whether the classification is propagatable. Consult the RCM to set up the OM ClassificationDef
            omClassificationDef.setPropagatable(rcmPropagatable);

            // Atlas does not have a field for external standard mappings. Consult the RCM to set up the OM ClassificationDef
            // Atlas does not have external standard mappings. OMRS ExternalStandardMappings ArrayList<ExternalStandardMapping>
            omClassificationDef.setExternalStandardMappings(rcmExternalStandardMappings);

            // Atlas does not have a field for valid instance statuses. Consult the RCM to set up the OM ClassificationDef
            // Atlas does not have valid instance statuses. OMRS ValidInstanceStatusList ArrayList<InstanceStatus>
            omClassificationDef.setValidInstanceStatusList(rcmInstanceStatusList);

            // Atlas does not have a field for initial status. Consult the RCM to set up the OM ClassificationDef
            omClassificationDef.setInitialStatus(rcmInitialStatus);
        }
        else {
            // The RCM does not have the type - either the type is new or we are doing a verify following a
            // restart - so the type has been retrieved from Atlas but the RCM does not yet have it cached.
            // In either case we should adopt sensible default values - which for the most part is done by
            // the object constructor, but explicitly set the fields here. We need to do enough to get past
            // the equivalence check.

            // The origin is the metadataCollectionId for this connector.
            omClassificationDef.setOrigin(this.metadataCollectionId);

            // Atlas does not have a field for external standard mappings. We can only assume here that this
            // should be set to null in what we return.
            omClassificationDef.setExternalStandardMappings(null);

            // Atlas does not have a field for valid instance statuses. Set a sensible default, which assumes
            // that either ACTIVE or DELETED states are OK.
            ArrayList<InstanceStatus> statusList = new ArrayList<>();
            statusList.add(InstanceStatus.ACTIVE);
            statusList.add(InstanceStatus.DELETED);
            omClassificationDef.setValidInstanceStatusList(statusList);

            // Atlas does not have a field for initial status. Default to ACTIVE
            omClassificationDef.setInitialStatus(ACTIVE);
        }

        // Handle fields that require conversion - i.e. supertypes, entityTypes, attributeDefs
        // subtypes are deliberately ignored

        // Atlas supertypes are Set<String>. OMRS supertypes are ArrayList<TypeDefLink>
        // Convert a Set<String> of Atlas type names to an OMRS List<TypeDefLink>
        // use separate utility method for conversion of Atlas type name to TypeDefLink with name and GUID and vice versa
        Set<String> atlasSuperTypes = atlasClassificationDef.getSuperTypes();
        TypeDefLink omrsSuperType = null;
        // If atlas typedef has no supertypes we can just leave OM superType as null
        if (atlasSuperTypes != null && !(atlasSuperTypes.isEmpty())) {
            // Atlas has at least one supertype.
            LOG.debug("{}: Atlas def has supertypes {}", methodName, atlasSuperTypes);
            if (atlasSuperTypes.size() > 1) {
                // Atlas has more than one supertype - we cannot model this typedef in OM
                // Failed to process the Atlas superType list; throw TypeErrorException
                LOG.error("{}: Failed to convert Atlas type {} because it has multiple supertypes", methodName, typeName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, atlasClassificationDef.getGuid(),
                        "atlasClassificationDef", methodName, metadataCollectionId,atlasClassificationDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            else {
                // The Atlas def has exactly one supertype - we can attempt to model this typedef in OM
                // Validate the supertype in the Atlas typedef store. There is no point looking in our own
                // cache or in the RCM as we may not have processed the Atlas type yet, so explicitly ask
                // the typedef store for the name and guid. If this fails, fail the conversion.
                // Go back to Atlas to get the GUID...
                String atlasSuperTypeName = atlasSuperTypes.iterator().next();

                omrsSuperType = metadataCollection.constructTypeDefLink(atlasSuperTypeName,  TypeCategory.CLASSIFICATION);
                if (omrsSuperType == null) {
                    LOG.error("{}: Could not resolve supertype with name {} for AtlasClassificationDef", methodName, atlasSuperTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(typeName, atlasClassificationDef.getGuid(),
                            "atlasClassificationDef", methodName, metadataCollectionId, atlasClassificationDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
        }
        omClassificationDef.setSuperType(omrsSuperType);
        LOG.debug("{}: has supertypes {}", methodName, omClassificationDef.getSuperType());

        // Atlas subtypes are explicitly ignored in OM

        // Atlas entityTypes are Set<String>. OMRS validEntityDefs are ArrayList<TypeDefLink>
        // Convert a Set<String> of Atlas type names to an OMRS List<TypeDefLink>
        // ALL the listed entityDefs must exist for the overall type def to match; so if it is found that ANY of the
        // entity types is unknown then the overall def will not be converted.

        // Note that if any of the validEntityDefs are in famous five then they must be switched.
        // The references to VEDs in famous five will be in Atlas format (prefixed), whereas the
        // OM references should be non-prefixed.

        ArrayList<TypeDefLink> validEntityDefs = null;
        Set<String> entityTypes = atlasClassificationDef.getEntityTypes();
        LOG.debug("{}: Atlas def has entity types {}", methodName, entityTypes);
        if (entityTypes != null && !entityTypes.isEmpty()) {
            // Iterate over the entity type names to ensure they are all known and fail if any is unknown
            validEntityDefs = new ArrayList<>();

            for (String s : entityTypes) {

                if (FamousFive.atlasTypeRequiresSubstitution(s)) {
                    String omEntityTypeName = FamousFive.getOMTypeName(s);
                    String omEntityTypeGUID = null;
                    if (omEntityTypeName != null) {
                        omEntityTypeGUID = FamousFive.getRecordedGUID(omEntityTypeName);
                        if (omEntityTypeGUID != null) {
                            TypeDefLink omEntityType = new TypeDefLink();
                            omEntityType.setName(omEntityTypeName);
                            omEntityType.setGUID(omEntityTypeGUID);
                            validEntityDefs.add(omEntityType);
                        }
                    }
                    if (omEntityTypeName == null || omEntityTypeGUID == null ) {
                        LOG.error("{}: Could not get omTypeName for Atlas type {}", methodName, s);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(typeName, atlasClassificationDef.getGuid(),
                                "atlasClassificationDef", methodName, metadataCollectionId,atlasClassificationDef.toString());

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }

                }
                else {
                    // entity type is not a member of famous five
                    // Map the Atlas entity type name (String) to an OM TypeDefLink (which has name and guid)
                    LOG.debug("{}: try to find entity type {}", methodName, s);
                    // Create a TypeDefLink for the type with the name given by s
                    TypeDefLink omTypeDefLink;
                    omTypeDefLink = metadataCollection.constructTypeDefLink(s, TypeCategory.ENTITY);
                    if (omTypeDefLink == null) {
                        // Error case - if there is no type def for the listed entity type then the classification cannot be processed...
                        LOG.error("{}: Could not resolve type with name {} for AtlasClassificationDef", s);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(typeName, atlasClassificationDef.getGuid(),
                                "atlasClassificationDef", methodName, metadataCollectionId,atlasClassificationDef.toString());

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    } else {
                        LOG.debug("{}: TypeDefLink allocated for entity type {} {}", methodName, s, omTypeDefLink);
                        validEntityDefs.add(omTypeDefLink);
                    }
                }
            }
        }
        // If we reached this point then the valid entity defs are OK to add to the classification def
        omClassificationDef.setValidEntityDefs(validEntityDefs);


        // Atlas Attribute defs

        try {
            List<TypeDefAttribute> omrsTypeDefAttributes = metadataCollection.convertAtlasAttributeDefs(userId, atlasClassificationDef.getAttributeDefs());
            omClassificationDef.setPropertiesDefinition(omrsTypeDefAttributes);
        }
        catch (RepositoryErrorException | TypeErrorException e) {
            // Failed to process the attribute types; give up trying to convert this entity def
            // Give up at this point by returning without having added the OM def to typeDefsForAPI
            LOG.debug("{}: Failed to convert the attributes of an AtlasClassificationDef", methodName);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(omClassificationDef={}", methodName, omClassificationDef);
        }
        return omClassificationDef;

    }
}