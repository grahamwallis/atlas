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
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus.ACTIVE;


public class AtlasEntityDefMapper {


    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityDefMapper.class);

    private LocalAtlasOMRSMetadataCollection metadataCollection;
    private String                           userId;
    private AtlasEntityDef                   atlasEntityDef;
    private String                           metadataCollectionId;


    // package private
    AtlasEntityDefMapper(LocalAtlasOMRSMetadataCollection metadataCollection,
                         String                           userId,
                         AtlasEntityDef                   atlasEntityDef)
        throws
            RepositoryErrorException
    {

        final String methodName = "AtlasEntityDefMapper";

        this.metadataCollection = metadataCollection;
        this.userId = userId;
        this.atlasEntityDef = atlasEntityDef;
        try {
            this.metadataCollectionId = this.metadataCollection.getMetadataCollectionId();
        }
        catch (RepositoryErrorException e) {
            LOG.error("{}: could not initialize metadataCollectionId", methodName, e);
            throw e;
        }
    }


    /**
     * Convert Atlas type into a valid OM type
     * Convert the AtlasEntityDef into the corresponding OM EntityDef and (implicitly)
     * validate the content of the OM EntityDef
     */

    // package private
    EntityDef toOMEntityDef()
        throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "toOMEntityDef";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(atlasEntityDef={}", methodName, atlasEntityDef);
        }


        OMRSRepositoryHelper repositoryHelper = metadataCollection.getRepositoryHelper();

        String atlasTypeName = null;
        String atlasTypeGUID = null;
        String omTypeName = null;
        String omTypeGUID = null;

        boolean famousFive = false;

        if (atlasEntityDef != null) {

            atlasTypeName = atlasEntityDef.getName();
            atlasTypeGUID = atlasEntityDef.getGuid();
            omTypeName = atlasTypeName;
            omTypeGUID = atlasTypeGUID;

            // If the type has been extended by Famous Five then do reverse name and GUID substitution
            if (atlasTypeName != null && FamousFive.atlasTypeRequiresSubstitution(atlasTypeName)) {
                LOG.debug("{}: famous five type {}", methodName, atlasTypeName);
                famousFive = true;
                omTypeName = FamousFive.getOMTypeName(atlasTypeName);
                omTypeGUID = FamousFive.getRecordedGUID(omTypeName);
            }
        }

        if (atlasEntityDef == null || atlasTypeName == null || atlasTypeGUID == null || omTypeName == null ) {
            // Give up now
            LOG.error("{}: Cannot convert AtlasEntityDef to OM EntityDef", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasEntityDef", "toOMEntityDef",metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "toOMEntityDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Allocate OM EntityDef, which will set AttributeTypeDefCategory automatically
        EntityDef omEntityDef = new EntityDef();

        // Set common fields
        omEntityDef.setGUID(omTypeGUID);
        omEntityDef.setName(omTypeName);

        // Care needed with version field - in case it is null - Atlas uses Long, OM uses long
        Long version = atlasEntityDef.getVersion();
        if (version == null) {
            // Long (the wrapper class) can have null value, but long (the primitive class) cannot
            LOG.error("{}: Cannot convert AtlasEntityDef to OM EntityDef - version is null", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("null", "version", metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "toOMEntityDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        omEntityDef.setVersion(version);

        omEntityDef.setVersionName(atlasEntityDef.getTypeVersion());

        // Copy across the fields that can be simply assigned
        omEntityDef.setDescription(atlasEntityDef.getDescription());
        omEntityDef.setCreatedBy(atlasEntityDef.getCreatedBy());
        omEntityDef.setUpdatedBy(atlasEntityDef.getUpdatedBy());
        omEntityDef.setCreateTime(atlasEntityDef.getCreateTime());
        omEntityDef.setUpdateTime(atlasEntityDef.getUpdateTime());
        omEntityDef.setOptions(atlasEntityDef.getOptions());



        /* For those fields we will need to check whether the TypeDef is known by the RCM:
         * For all fields where nothing is stored in Atlas, use the RCM if possible to determine how to set the OM EntityDef
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
        TypeDef rcmTypeDef = repositoryHelper.getTypeDefByName(metadataCollectionId, omTypeName);
        if (rcmTypeDef != null) {
            // If we didn't get a typedef from the RCM then we would adopt the defaults, but we can use the RCM values...
            if (rcmTypeDef.getCategory() != TypeDefCategory.ENTITY_DEF) {
                LOG.error("{}: The RepositoryContentManager has a type of this name that is not a EntityDef!", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omTypeName, atlasEntityDef.getGuid(), "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            EntityDef rcmEntityDef = (EntityDef) rcmTypeDef;
            List<ExternalStandardMapping> rcmExternalStandardMappings = rcmEntityDef.getExternalStandardMappings();
            List<InstanceStatus> rcmInstanceStatusList = rcmEntityDef.getValidInstanceStatusList();
            InstanceStatus rcmInitialStatus = rcmEntityDef.getInitialStatus();
            LOG.debug("{}: Fields acquired from RCM: validInstanceStatusList = {}, initialStatus = {}, externalStandardMappings = {}",
                   methodName, rcmInstanceStatusList, rcmInitialStatus, rcmExternalStandardMappings);

            // The origin is the metadataCollectionId for this connector.
            omEntityDef.setOrigin(this.metadataCollectionId);

            // Atlas does not have a field for external standard mappings. Consult the RCM to set up the OM EntityDef
            // Atlas does not have external standard mappings. OMRS ExternalStandardMappings ArrayList<ExternalStandardMapping>
            omEntityDef.setExternalStandardMappings(rcmExternalStandardMappings);

            // Atlas does not have a field for valid instance statuses. Consult the RCM to set up the OM EntityDef
            // Atlas does not have valid instance statuses. OMRS ValidInstanceStatusList ArrayList<InstanceStatus>
            omEntityDef.setValidInstanceStatusList(rcmInstanceStatusList);

            // Atlas does not have a field for initial status. Consult the RCM to set up the OM EntityDef
            omEntityDef.setInitialStatus(rcmInitialStatus);
        }
        else {
            // The RCM does not have the type - either the type is new or we are doing a verify following a
            // restart - so the type has been retrieved from Atlas but the RCM does not yet have it cached.
            // In either case we should adopt sensible default values - which for the most part is done by
            // the object constructor, but explicitly set the fields here. We need to do enough to get past
            // the equivalence check.

            // The origin is the metadataCollectionId for this connector.
            omEntityDef.setOrigin(this.metadataCollectionId);

            // Atlas does not have a field for external standard mappings. We can only assume here that this
            // should be set to null in what we return.
            omEntityDef.setExternalStandardMappings(null);

            // Atlas does not have a field for valid instance statuses. Set a sensible default, which assumes
            // that either ACTIVE or DELETED states are OK.
            ArrayList<InstanceStatus> statusList = new ArrayList<>();
            statusList.add(InstanceStatus.ACTIVE);
            statusList.add(InstanceStatus.DELETED);
            omEntityDef.setValidInstanceStatusList(statusList);

            // Atlas does not have a field for initial status. Default to ACTIVE
            omEntityDef.setInitialStatus(ACTIVE);
        }


        // Handle fields that require conversion - i.e. supertypes, attributeDefs
        // Atlas subtypes are deliberately ignored

        // Convert Atlas Set<String> supertypes containing Atlas type names to an OM List<TypeDefLink>
        // OM enforces single inheritance, so fail conversion if Atlas has multiple supertypes but
        // Famous Five types must be recognised before the supertype count is checked.
        if (!famousFive) {
            Set<String> atlasSuperTypes = atlasEntityDef.getSuperTypes();
            TypeDefLink omSuperType;
            // If atlas typedef has no supertypes we can just leave omrs superType as null
            if (atlasSuperTypes != null && !(atlasSuperTypes.isEmpty())) {
                // Atlas has at least one supertype.
                LOG.debug("{}: Atlas def has supertypes {}", methodName, atlasSuperTypes);
                if (atlasSuperTypes.size() > 1) {
                    // Atlas has more than one supertype - we cannot model this typedef in OM
                    // Failed to process the Atlas superType list; give up without adding the entity def to the TDG
                    LOG.error("{}: Failed to convert Atlas type {} because it has multiple supertypes", methodName, atlasTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                                "atlasEntityDef", methodName, metadataCollectionId,atlasEntityDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                } else {
                    // The Atlas def has exactly one supertype - we can attempt to model this typedef in OM
                    // Validate the supertype in the Atlas typedef store. There is no point looking in our own
                    // cache or in the RCM as we may not have processed the Atlas type yet, so explicitly ask
                    // the typedef store for the name and guid. If this fails, fail the conversion.
                    // Go back to Atlas to get the GUID...
                    String atlasSuperTypeName = atlasSuperTypes.iterator().next();
                    String omSuperTypeName = atlasSuperTypeName;
                    // To accommodate types from either side of the type hierarchy - i.e. types with an Atlas F5 supertype
                    // and types with an OM F5 supertype (OM_xx), detect whether the supertype name is known on either side
                    // of the F5 mapping. In either case, treat as F5; converting to external (non-prefixed) name.
                    boolean f5Supertype = false;
                    if (FamousFive.omTypeRequiresSubstitution(atlasSuperTypeName)) {
                        LOG.debug("{}: famous five supertype {}", methodName, atlasSuperTypeName);
                        f5Supertype = true;
                        // Leave the superTypeName as is (e.g. DataSet), i.e keep omSuperTypeName = atlasSuperTypeName;
                    }
                    else if (FamousFive.atlasTypeRequiresSubstitution(atlasSuperTypeName)) {
                        // supertype is in famous five
                        // Strip off the OM_ prefix
                        LOG.debug("{}: famous five supertype {}", methodName, atlasSuperTypeName);
                        f5Supertype = true;
                        omSuperTypeName = FamousFive.getOMTypeName(atlasSuperTypeName);
                    }
                    if (f5Supertype) {
                        String omSuperTypeGUID = null;
                        if (omSuperTypeName != null) {
                            omSuperTypeGUID = FamousFive.getRecordedGUID(omSuperTypeName);
                            if (omSuperTypeGUID != null) {
                                omSuperType = new TypeDefLink();
                                omSuperType.setName(omSuperTypeName);
                                omSuperType.setGUID(omSuperTypeGUID);
                                omEntityDef.setSuperType(omSuperType);
                            }
                        }
                        if (omSuperTypeName == null || omSuperTypeGUID == null) {
                            LOG.error("{}: Could not resolve supertype with name {} for AtlasEntityDef", methodName, atlasSuperTypeName);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                                    "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                    }
                    else {
                        // supertype is not famous five
                        omSuperType = metadataCollection.constructTypeDefLink(atlasSuperTypeName, TypeCategory.ENTITY);
                        if (omSuperType != null) {
                            omEntityDef.setSuperType(omSuperType);
                        }
                        else {
                            LOG.error("{}: Could not resolve supertype with name {} for AtlasEntityDef", methodName, atlasSuperTypeName);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                                    "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }

                    }
                }
            }
        }
        else {
            // This is a famousFive type so it will have artificially high number of supertypes
            // Remove the Atlas-specific supertype (with the stripped self-name, then check supertypes etc)

            Set<String> atlasSuperTypes = atlasEntityDef.getSuperTypes();

            // We would expect there to be one or more supertype (since this is famous five).
            // Spin through the supertypes and look for the artificially added supertype.

            if (atlasSuperTypes == null || atlasSuperTypes.isEmpty()) {
                // This is not a valid state for the supertypes of a famous five type
                LOG.error("{}: Famous five type {} has no supertypes", methodName, atlasTypeName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                        "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /* We know we have at least one supertype, look for (and remove) the artificial one
             * which will be the one with the original OM type name.
             */
            boolean found = false;
            Set<String> realSuperTypes = new HashSet<>();
            for (String atlasSuperTypeName : atlasSuperTypes ) {
                if (atlasSuperTypeName.equals(omTypeName)) {
                    found = true;
                    // skip this supertype
                }
                else {
                    realSuperTypes.add(atlasSuperTypeName);
                }
            }
            if (!found) {
                LOG.error("{}: Famous five type {} is missing supertype {}", methodName, atlasTypeName, omTypeName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                        "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // The supertypes have been compacted

            TypeDefLink omSuperType = null;
            // If atlas typedef has no real supertypes we can just leave omrs superType as null
            if (!realSuperTypes.isEmpty()) {
                // Atlas has at least one real supertype.
                LOG.debug("{}: Atlas def has supertypes {}", methodName, realSuperTypes);
                if (realSuperTypes.size() > 1) {
                    // Atlas has more than one real supertype - we cannot model this typedef in OM
                    // Failed to process the Atlas superType list; give up without adding the entity def to the TDG
                    LOG.error("{}: Failed to convert Atlas type {} because it has multiple supertypes", methodName, atlasTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                            "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                } else {

                    // The Atlas def has exactly one supertype - we can attempt to model this typedef in OM
                    // Validate the supertype in the Atlas typedef store. There is no point looking in our own
                    // cache or in the RCM as we may not have processed the Atlas type yet, so explicitly ask
                    // the typedef store for the name and guid. If this fails, fail the conversion.
                    // Go back to Atlas to get the GUID...
                    String atlasSuperTypeName = realSuperTypes.iterator().next();
                    String omSuperTypeName;
                    if (FamousFive.atlasTypeRequiresSubstitution(atlasSuperTypeName)) {
                        // supertype is in famous five
                        LOG.debug("{}: famous five supertype {}", methodName, atlasSuperTypeName);
                        omSuperTypeName = FamousFive.getOMTypeName(atlasSuperTypeName);
                        String omSuperTypeGUID = null;
                        if (omSuperTypeName != null) {
                            omSuperTypeGUID = FamousFive.getRecordedGUID(omSuperTypeName);
                            if (omSuperTypeGUID != null) {
                                omSuperType = new TypeDefLink();
                                omSuperType.setName(omSuperTypeName);
                                omSuperType.setGUID(omSuperTypeGUID);
                                omEntityDef.setSuperType(omSuperType);
                            }
                        }
                        if (omSuperTypeName == null || omSuperTypeGUID == null) {
                            LOG.error("{}: Could not resolve supertype with name {} for AtlasEntityDef", methodName, atlasSuperTypeName);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                                    "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }

                    }
                    else {
                        // supertype is not famous five
                        omSuperType = metadataCollection.constructTypeDefLink(atlasSuperTypeName, TypeCategory.ENTITY);
                        if (omSuperType == null) {
                            LOG.error("{}: Could not resolve supertype with name {} for AtlasEntityDef", methodName, atlasSuperTypeName);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(atlasTypeName, atlasEntityDef.getGuid(),
                                    "atlasEntityDef", methodName, metadataCollectionId, atlasEntityDef.toString());

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                        else {
                            omEntityDef.setSuperType(omSuperType);
                        }
                    }
                }
            }
            omEntityDef.setSuperType(omSuperType);
        }

        // Atlas subtypes are explicitly ignored in OM

        // Atlas Attribute defs
        try {
            List<TypeDefAttribute> omrsTypeDefAttributes = metadataCollection.convertAtlasAttributeDefs(userId, atlasEntityDef.getAttributeDefs());
            omEntityDef.setPropertiesDefinition(omrsTypeDefAttributes);
        }
        catch (RepositoryErrorException | TypeErrorException e) {
            // Failed to process the attribute types; give up trying to convert this entity def
            // Give up at this point by returning without having added the OM def to typeDefsForAPI
            LOG.error("{}: caught exception during conversion of Atlas attributes", methodName, e);
            throw e;
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(omEntityDef={}", methodName, omEntityDef);
        }

        return omEntityDef;

    }

}
