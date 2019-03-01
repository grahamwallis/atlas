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


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.openmetadata.adapters.eventmapper.AtlasOMRSRepositoryEventMapper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStreamForImport;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.store.DeleteType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;

import static org.apache.atlas.AtlasErrorCode.CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY;
import static org.apache.atlas.model.discovery.SearchParameters.Operator.EQ;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.DELETED;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.*;

import org.apache.commons.collections.CollectionUtils;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSMetadataCollection;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.MatchCriteria;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.SequencingOrder;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.AttributeTypeDefCategory.ENUM_DEF;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.AttributeTypeDefCategory.PRIMITIVE;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory.OM_PRIMITIVE_TYPE_UNKNOWN;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefCategory.*;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;


/**
 *  The LocalAtlasOMRSMetadataCollection represents a local metadata repository.
 *  Requests to this metadata collection are mapped to requests to the local repository.
 *
 *  The metadata collection reads typedefs from Atlas and will attempt to convert them to OM typedefs - and
 *  vice versa. During these conversions it will check the validity of the content of the type defs as far as
 *  possible, giving up on a typedef that it cannot verify or convert.
 *
 *  This implementation of the metadata collection does not use the java implementations of the REST APIs because
 *  the REST interface is servlet and http oriented. This implementation uses lower level classes in Atlas.
 *  The behaviour still respects transactions because they are implemented at levels below the REST implementation.
 *  The transactions are implemented by annotation in the underlying implementations of the AtlasTypeDefStore
 *  interface.
 */


public class LocalAtlasOMRSMetadataCollection extends OMRSMetadataCollection {

    private static final Logger LOG = LoggerFactory.getLogger(LocalAtlasOMRSMetadataCollection.class);

    /*
     * The MetadataCollection makes use of a pair of TypeDefsByCategory objects.
     *
     * A TypeDefsByCategory is a more fully explicated rendering of the TypeDefGallery - it makes
     * it easier to store and search for a type def of a particular category. The TDBC objects are only
     * used internally - they do not form part of the API; being rendered as a TypeDefGallery just before
     * return from getTypeDefs. The MDC uses two TDBC objects - one is transient and the other is longer lived.
     *
     * The typeDefsCache is a long-lived TypeDefsByCategory used to remember the TypeDefs from Atlas (that can be
     * modeled in OM). It is allocated at the start of loadTypeDefs so can be refreshed by a fresh call to loadTypeDefs.
     * The typeDefsCache is therefore effectively a cache of type defs. To refresh it call loadTypeDefs again - this
     * will clear it and reload it.
     * The typeDefsCache is retained across API calls; unlike typeDefsForAPI which is not.
     */


    /*
     * typeDefsForAPI is a transient TDBC object - it is reallocated at the start of each external API call.
     * It's purpose is to marshall the results for the current API (only), which can then be extracted or
     * turned into a TDG, depending on return type of the API.
     */
    private TypeDefsByCategory typeDefsForAPI = null;

    /*
     * Declare the Atlas stores the connector will use - these are injected via AtlasStoresProxy.
     */
    private AtlasTypeRegistry typeRegistry;
    private AtlasTypeDefStore typeDefStore;
    private AtlasEntityStore entityStore;
    private AtlasRelationshipStore relationshipStore;
    private EntityDiscoveryService entityDiscoveryService;
    private AtlasGraphProvider atlasGraphProvider;


    // EventMapper will be set by the event mapper itself calling the metadataCollection once the mapper is started.
    private AtlasOMRSRepositoryEventMapper eventMapper = null;


    // package private
    LocalAtlasOMRSMetadataCollection(LocalAtlasOMRSRepositoryConnector parentConnector,
                                     String                            repositoryName,
                                     OMRSRepositoryHelper              repositoryHelper,
                                     OMRSRepositoryValidator           repositoryValidator,
                                     String                            metadataCollectionId)
    {

        /*
         * The metadata collection Id is the unique Id for the metadata collection.  It is managed by the super class.
         */
        super(parentConnector, repositoryName, metadataCollectionId, repositoryHelper, repositoryValidator);

        /*
         *  Initialize the Atlas stores and services
         */
        this.typeRegistry           = SpringBridge.services().getTypeRegistry();
        this.typeDefStore           = SpringBridge.services().getTypeDefStore();
        this.entityStore            = SpringBridge.services().getEntityStore();
        this.relationshipStore      = SpringBridge.services().getRelationshipStore();
        this.entityDiscoveryService = SpringBridge.services().getEntityDiscoveryService();
        this.atlasGraphProvider     = SpringBridge.services().getAtlasGraphProvider();


    }





    /* ======================================================================
     * Group 1: Confirm the identity of the metadata repository being called.
     */

    /**
     * Returns the identifier of the metadata repository.  This is the identifier used to register the
     * metadata repository with the metadata repository cohort.  It is also the identifier used to
     * identify the home repository of a metadata instance.
     *
     * @return String - metadata collection id.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     */
    public String getMetadataCollectionId() throws RepositoryErrorException
    {

        final String methodName = "getMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        /*
         * Perform operation
         */
        return super.metadataCollectionId;
    }



    /* ==============================
     * Group 2: Working with typedefs
     */

    /**
     * Returns the list of different types of TypeDefs organized by TypeDef Category.
     *
     * @param userId - unique identifier for requesting user.
     * @return TypeDefs - List of different categories of TypeDefs.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery getAllTypes(String userId)
            throws
            RepositoryErrorException,
            UserNotAuthorizedException
    {


        final String methodName = "getAllTypes";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {} (userId={})", methodName, userId);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);

            /*
             * Perform operation
             */

            /*
             * The metadataCollection requests the TypeDefs from Atlas. It then parses each of the lists in the TypesDef.
             *
             * For entities, relationships and classifications it will parse each TypeDef and will produce a corresponding OM
             * type def - which will be of concrete type (e.g. EntityDef). It can only do this for Atlas defs that can be modelled in
             * OM - some things are not possible, in which case no OM def is produced for that Atlas def.
             * It will validate that the generated OM type is valid - i.e. that all mandatory fields are present, that no constraints are
             * violated, etc - Note that this validation step is not very explicit yet - it will be refined/extended later.
             * The MDC must then check whether the generated OM type is known in the RCM. It uses the RepoHelper and Validator
             * classes for this - it can new up a RepoHelper and this will have a static link to the RCM. It should then issue
             * a get by name of the typedef against the RCM. I think this might use isKnownType().
             * If the type is found within the RCM, then we know that it already exists and we must then perform a deep
             * compare of the known type and the newly generated type. If they are the same then we can safely use the
             * known type; we adopt it's GUID and add the typedef to the TDG. If they do not match exactly then we issue
             * an audit log entry and ignore the generated type (it will not be added to the TDG). If the type is NOT found in the
             * RCM then we are dealing with a newly discovered type and we can generate a GUID (using the usual means). The generated
             * typedef is added to the TDG.
             * In addition to looking in the RCM we need to look at any types we already added to the TDG (in this pass) because each
             * type only appears once in the TDG. This can be a compare by name only and does not need a compare of content (which is needed
             * vs a type found in the RCM by name) because the type namespace in Atlas means that two references by the same name must by
             * definition be references to the same type.
             * The above processing is similar for EntityDef, RelationshipDef and ClassificationDef typedefs. The MDC is either referring to
             * known types or generating newly discovered types for those types not found in the RCM.
             *
             * The above types can (and probably will) contain attribute defs. For each attribute the MDC needs to decide what type
             * the attribute has. It will either be an Enum, a Prim or a Coll of Prim, or it may be a type reference or coll of type reference.
             * In the last two cases (involving type refs) the enclosing typedef will be skipped (not added to the TypeDefGallery). This is
             * because type references are specific to older Atlas types and in OM the types should use first-class relationships.
             * The older Atlas types are therefore not pulled into OM.
             *
             * Whenever an enclosing typedef (EntityDef, RelationshipDef or ClassificationDef) is added to the TypeDefGallery it will have a
             * propertiesDef which is a list of TDA, each having the attribute name and an AttributeTypeDef which is on the TypeDefGallery's
             * attributeTypeDefs list.
             * This is similar to how one TypDef refers to another TypeDef (as opposed to a property/attribute): when a TypeDef refers to another
             * TypeDef a similar linkage is established through a TypeDefLink that refers to a TypeDef on the TypeDefGallery's typeDefs list.
             * For attribute types that are either consistent and known or valid and new we add them to the TypeDefGallery's list of attributeTypeDefs.
             * Where multiple 'top-level' typedefs contain attributes of the same type - e.g. EntityDef1 has an int called 'retention' and
             * EntityDef2 has an int called 'count' - then they will both refer to the same single entry for the int primitive attr type in the
             * TypeDefGallery. References to types in the TypeDefGallery are by TypeDefLink (which needs a cat, name and guid).
             *
             * So the attribute types to be processed are:
             *
             * Primitive - for each of these the MDC will match the Atlas typename (a string) against the known PrimDefCats and construct a
             * PrimitiveDef with the appropriate PrimDefCat. Look in the RCM and if a Prim with the same string typename exists then adopt its GUID.
             * [There is a shortcut here I think as the RCM will have used the same GUID as the PrimDefCat so you can find the GUID directly.]
             * I think it is very unlikely that there will be new Primitives discovered. In fact I think that is impossible.
             *
             * Collection - a Collection can be either an ARRAY<Primitive> or a MAP<Primitive,Primitive>. In both cases we can create a consistent
             * name for the collection type by using the pattern found in CollectionDef[Cat]. The MDC uses that collection type name to
             * check whether the collection type is already known in the RCM (again this is a get by name, since we do not have a GUID).
             * If the collection is a known type then we will use the GUID from the RCM and add it. It is likely that the RCM
             * will already know about a small number of collections - eg. Map<String,String> - so for these the MDC will reuse the
             * existing attributeDef - it will still add the collection def and will refer to it using a TDL with the cat, name and guid.
             * Any further coll defs will be generated and given a new GUID.
             *
             * Enum - An Enum Def is easier to convert than a Primitive or a Collection because in Atlas it is a real type and has a guid. Processing for
             * an enum will be more like that for EntityDef, etc. and will require validation (of possible values, default value, etc) and an existence
             * check in the RCM. If known we will do a deep compare and on match will use the existing def, on mismatch generate audit log entry and
             * skip not just the enum but also any type that contains it. If it does not exist in the RCM we will generate a new EnumDef with a new GUID.
             *
             * Unit Testing
             * ------------
             * 3 phases of elaboration.
             * 1. Just assume (assert?) that each type does not exist in the RCM and hence we will project it into the generated √. This is a good way
             * to see and verify the generation of types by the MDC. The generator and comparator should be able to generate/compare √.
             * 2. Mock up the RCM so that we can control what it 'already knows'. This will allow us to test the existence match behaviour of MDC and
             * the apparent-match-but-different-content logic and audit log entry generation.
             * 3. Like UT1 (assume that everything is new) but have all the OM types loaded into Atlas. This will allow us to test whether the integration
             * with Atlas is working and whether the types are coming through OK. This is actually an integration test I think.
             *
             */


            // Clear the per-API records of TypeDefs
            typeDefsForAPI = new TypeDefsByCategory();

            /*
             * This method is a thin wrapper around loadAtlasTypeDefs which will clear and reload the cache
             * and will construct it's result in typeDefsForAPI and save it into the typeDefsCache.
             * The loadAtlasTypeDefs method can be used at other times when we need to cache the known TypeDefs
             * - e.g. for verify calls.
             */


            try {
                loadAtlasTypeDefs(userId);
            } catch (RepositoryErrorException e) {
                LOG.error("{}: caught exception from Atlas {}", methodName, e);
                throw e;
            }

            /*
             * typeDefsForAPI will have been loaded with all discovered TypeDefs. It can be converted into a TypeDefGallery
             * and can be reset on the next API to be called.
             */

            TypeDefGallery typeDefGallery = typeDefsForAPI.convertTypeDefsToGallery();
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {} typeDefGallery={}", methodName, typeDefGallery);
            }
            return typeDefGallery;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }

    /**
     * Returns a list of type definitions that have the specified name.  Type names should be unique.  This
     * method allows wildcard character to be included in the name.  These are * (asterisk) for an
     * arbitrary string of characters and ampersand for an arbitrary character.
     *
     * @param userId - unique identifier for requesting user.
     * @param name   - name of the TypeDefs to return (including wildcard characters).
     * @return TypeDefGallery - List of different categories of type definitions.
     * @throws InvalidParameterException  - the name of the TypeDef is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery findTypesByName(String userId,
                                          String name)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        /*
         * Retrieve the typedefs from Atlas, and return a TypeDefGallery that contains two lists, each sorted by category
         * as follows:
         * TypeDefGallery.attributeTypeDefs contains:
         * 1. PrimitiveDefs
         * 2. CollectionDefs
         * 3. EnumDefs
         * TypeDefGallery.newTypeDefs contains:
         * 1. EntityDefs
         * 2. RelationshipDefs
         * 3. ClassificationDefs
         */


        final String methodName = "findTypesByName";
        final String nameParameterName = "name";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, name={})", methodName, userId, name);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

            /*
             * Perform operation
             */

            return repositoryHelper.getActiveTypesByWildCardName(repositoryName, name);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Returns all of the TypeDefs for a specific category.
     *
     * @param userId   - unique identifier for requesting user.
     * @param category - enum value for the category of TypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException  - the TypeDefCategory is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypeDefsByCategory(String          userId,
                                                TypeDefCategory category)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "findTypeDefsByCategory";
        final String categoryParameterName = "category";

        // Use Atlas typedefstore search API and then post-filter by type category
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, category={})", methodName, userId, category);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

            /*
             * Perform operation
             */

            // Clear the per-API records of TypeDefs
            typeDefsForAPI = new TypeDefsByCategory();

            List<TypeDef> retList;
            try {
                retList = _findTypeDefsByCategory(userId, category);
            } catch (Exception e) {
                LOG.debug("{}: re-throwing exception from _findTypeDefsByCategory ", methodName);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {} : retList={}", methodName, retList);
            }
            return retList;
        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Returns all of the AttributeTypeDefs for a specific category.
     *
     * @param userId   - unique identifier for requesting user.
     * @param category - enum value for the category of an AttributeTypeDef to return.
     * @return AttributeTypeDefs list.
     * @throws InvalidParameterException  - the TypeDefCategory is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<AttributeTypeDef> findAttributeTypeDefsByCategory(String                   userId,
                                                                  AttributeTypeDefCategory category)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "findAttributeTypeDefsByCategory";
        final String categoryParameterName = "category";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, category={})", methodName, userId, category);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAttributeTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

            /*
             * Perform operation
             */

            // Clear the per-API records of TypeDefs
            typeDefsForAPI = new TypeDefsByCategory();

            List<AttributeTypeDef> retList;
            try {
                retList = _findAttributeTypeDefsByCategory(userId, category);
            } catch (Exception e) {
                LOG.debug("{}: re-throwing exception from _findAttributeTypeDefsByCategory ", methodName);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {} : retList={}", methodName, retList);
            }
            return retList;
        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return the TypeDefs that have the properties matching the supplied match criteria.
     *
     * @param userId        - unique identifier for requesting user.
     * @param matchCriteria - TypeDefProperties - a list of property names.
     * @return TypeDefs list.
     * @throws InvalidParameterException  - the matchCriteria is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypeDefsByProperty(String            userId,
                                                TypeDefProperties matchCriteria)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "findTypeDefsByProperty";
        final String matchCriteriaParameterName = "matchCriteria";
        final String sourceName = metadataCollectionId;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, matchCriteria={})", methodName, userId, matchCriteria);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateMatchCriteria(repositoryName, matchCriteriaParameterName, matchCriteria, methodName);

            /*
             * Perform operation
             */

            /*
             * Implementation: perform a search of the Atlas type defs and convert each into
             * corresponding OM TypeDef, filtering the results by properties in the matchCriteria
             * Return is a List<TypeDef>
             * AttributeTypeDefs are not included, so the list just contains
             *  1. EntityDefs
             *  2. RelationshipDefs
             *  3. ClassificationDefs
             */


            // The result of the load is constructed in typeDefsForAPI, so clear that first.
            typeDefsForAPI = new TypeDefsByCategory();

            // Strategy: use searchTypesDef with a null (default) SearchFilter.
            SearchFilter emptySearchFilter = new SearchFilter();
            AtlasTypesDef atd;
            try {

                atd = typeDefStore.searchTypesDef(emptySearchFilter);

            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from Atlas type def store {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("category", matchCriteriaParameterName, methodName, sourceName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Parse the Atlas TypesDef
             * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
             * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
             * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
             * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
             * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
             * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
             * contains anything other than primitives).
             */


            /*
             * This method will populate the typeDefsForAPI object.
             * This is also converting the ATDs which appears wasteful but does allow any problems
             * to be handled, resulting in the skipping of the problematic TypeDef.
             */
            if (atd != null) {
                convertAtlasTypeDefs(userId, atd);
            }


            // For the Entity, Relationship and Classification type defs, filter them through matchCriteria
            //List<String> matchPropertyNames = matchCriteria.getTypeDefProperties();
            Map<String, Object> matchProperties = matchCriteria.getTypeDefProperties();
            Set<String> keys = matchProperties.keySet();
            List<String> matchPropertyNames = new ArrayList<>(keys);

            List<TypeDef> returnableTypeDefs = new ArrayList<>();

            // Filter EntityDefs
            List<TypeDef> entityDefs = typeDefsForAPI.getEntityDefs();
            if (entityDefs != null) {
                for (TypeDef typeDef : entityDefs) {
                    // Get the property names for this TypeDef
                    List<String> currentTypePropNames = getPropertyNames(userId, typeDef);
                    if (propertiesContainAllMatchNames(currentTypePropNames, matchPropertyNames)) {
                        // Amalgamate each survivor into the returnable list...
                        returnableTypeDefs.add(typeDef);
                    }
                }
            }

            // Filter RelationshipDefs
            List<TypeDef> relationshipDefs = typeDefsForAPI.getRelationshipDefs();
            if (relationshipDefs != null) {
                for (TypeDef typeDef : relationshipDefs) {
                    // Get the property names for this TypeDef
                    List<String> currentTypePropNames = getPropertyNames(userId, typeDef);
                    if (propertiesContainAllMatchNames(currentTypePropNames, matchPropertyNames)) {
                        // Amalgamate each survivor into the returnable list...
                        returnableTypeDefs.add(typeDef);
                    }
                }
            }

            // Filter ClassificationDefs
            List<TypeDef> classificationDefs = typeDefsForAPI.getClassificationDefs();
            if (classificationDefs != null) {
                for (TypeDef typeDef : classificationDefs) {
                    // Get the property names for this TypeDef
                    List<String> currentTypePropNames = getPropertyNames(userId, typeDef);
                    if (propertiesContainAllMatchNames(currentTypePropNames, matchPropertyNames)) {
                        // Amalgamate each survivor into the returnable list...
                        returnableTypeDefs.add(typeDef);
                    }
                }
            }


            List<TypeDef> returnValue = returnableTypeDefs.isEmpty() ? null : returnableTypeDefs;
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}: return={})", methodName, returnValue);
            }
            return returnValue;


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return the types that are linked to the elements from the specified standard.
     *
     * @param userId       - unique identifier for requesting user.
     * @param standard     - name of the standard - null means any, but either standard or organization must be specified
     * @param organization - name of the organization - null means any, but either standard or organization must be specified.
     * @param identifier   - identifier of the element in the standard - must be specified (cannot be null)
     * @return TypeDefs list - each entry in the list contains a TypeDef.  This is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException  - all attributes of the external id are null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypesByExternalID(String userId,
                                               String standard,
                                               String organization,
                                               String identifier)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "findTypesByExternalID";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={},standard={},organization={},identifier={})",
                    methodName, userId, standard, organization, identifier);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateExternalId(repositoryName, standard, organization, identifier, methodName);

            /*
             * Perform operation
             */

            List<TypeDef> returnValue = null;

            // Pack the filter criteria into an ExternalStandardMapping
            ExternalStandardMapping filterCriteria = new ExternalStandardMapping();
            filterCriteria.setStandardName(standard);
            filterCriteria.setStandardOrganization(organization);
            filterCriteria.setStandardTypeName(identifier);

            /*
             * There is no point asking Atlas for type defs and then filtering on External Standards information,
             * because that information only exists in the RepositoryContentManager. So instead, ask the Repository
             * Helper, and then filter.
             */
            TypeDefGallery knownTypes = repositoryHelper.getKnownTypeDefGallery();
            List<TypeDef> knownTypeDefs = knownTypes.getTypeDefs();
            if (knownTypeDefs != null) {

                List<TypeDef> returnableTypeDefs = new ArrayList<>();

                /*
                 * Look through the knownTypeDefs checking for the three strings we need to match...
                 * According to the validator we are expecting at least one of standard OR organization to be non-null AND
                 * for identifier to be non-null. So identifier must be present AND either standard OR organization must
                 * be present.
                 * This has been asserted by calling the validator's validateExternalId method.
                 * For a TypeDef to match it must possess the specified standard mapping within its list of ESMs.
                 */
                for (TypeDef typeDef : knownTypeDefs) {
                    // Get the external standards fields for this TypeDef
                    List<ExternalStandardMapping> typeDefExternalMappings = typeDef.getExternalStandardMappings();

                    if (externalStandardsSatisfyCriteria(typeDefExternalMappings, filterCriteria)) {
                        // Amalgamate each survivor into the returnable list...
                        returnableTypeDefs.add(typeDef);
                    }
                }

                if (!(returnableTypeDefs.isEmpty())) {
                    returnValue = returnableTypeDefs;
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}: return={})", methodName, returnValue);
            }
            return returnValue;


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return the TypeDefs that match the search criteria.
     *
     * @param userId         - unique identifier for requesting user.
     * @param searchCriteria - String - search criteria.
     * @return TypeDefs list - each entry in the list contains a TypeDef.  This is is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException  - the searchCriteria is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> searchForTypeDefs(String userId,
                                           String searchCriteria)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "searchForTypeDefs";
        final String searchCriteriaParameterName = "searchCriteria";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={},searchCriteria={})", methodName, userId, searchCriteria);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);

            /*
             * Perform operation
             */

            /*
             * The searchCriteria is interpreted as a String to be used in a wildcard comparison with the name of the TypeDef.
             * It is only the name of the TypeDef that is compared.
             *
             */

            /*
             * Implementation: perform a search of the Atlas type defs and convert each into
             * corresponding OM TypeDef, filtering the results by testing whether the searchCriteria
             * string is contained in (or equal to) the name of each TypeDef.
             * Return is a List<TypeDef>
             * AttributeTypeDefs are not included, so the list just contains
             *  1. EntityDefs
             *  2. RelationshipDefs
             *  3. ClassificationDefs
             */


            // The result of the load is constructed in typeDefsForAPI, so clear that first.
            typeDefsForAPI = new TypeDefsByCategory();

            // Strategy: use searchTypesDef with a null (default) SearchFilter.
            SearchFilter emptySearchFilter = new SearchFilter();
            AtlasTypesDef atd;
            try {

                atd = typeDefStore.searchTypesDef(emptySearchFilter);

            } catch (AtlasBaseException e) {
                LOG.error("{}:caught exception from attempt to retrieve all TypeDefs from Atlas repository", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(searchCriteria, methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            /*
             * Parse the Atlas TypesDef
             * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
             * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
             *  is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
             *  supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
             *  things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
             *  anything that contains something that it does not understand (e.g. a reference attribute or a collection that
             *  contains anything other than primitives).
             *
             * This method will populate the typeDefsForAPI object.
             * This is also converting the ATDs which appears wasteful but does allow any problems
             * to be handled, resulting in the skipping of the problematic TypeDef.
             */
            if (atd != null) {
                convertAtlasTypeDefs(userId, atd);
            }


            // For the Entity, Relationship and Classification type defs, filter them through matchCriteria

            List<TypeDef> returnableTypeDefs = new ArrayList<>();

            // Filter EntityDefs
            List<TypeDef> entityDefs = typeDefsForAPI.getEntityDefs();
            if (entityDefs != null) {
                for (TypeDef typeDef : entityDefs) {
                    if (typeDef.getName().matches(searchCriteria)) {
                        returnableTypeDefs.add(typeDef);
                    }
                }
            }

            // Filter RelationshipDefs
            List<TypeDef> relationshipDefs = typeDefsForAPI.getEntityDefs();
            if (relationshipDefs != null) {
                for (TypeDef typeDef : relationshipDefs) {
                    if (typeDef.getName().matches(searchCriteria)) {
                        returnableTypeDefs.add(typeDef);
                    }
                }
            }

            // Filter ClassificationDefs
            List<TypeDef> classificationDefs = typeDefsForAPI.getEntityDefs();
            if (classificationDefs != null) {
                for (TypeDef typeDef : classificationDefs) {
                    if (typeDef.getName().matches(searchCriteria)) {
                        returnableTypeDefs.add(typeDef);
                    }
                }
            }


            List<TypeDef> returnValue = null;
            if (!(returnableTypeDefs.isEmpty())) {
                returnValue = returnableTypeDefs;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}: return={})", methodName, returnValue);
            }
            return returnValue;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return the TypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique id of the TypeDef
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - The requested TypeDef is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef getTypeDefByGUID(String userId,
                                    String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName = "getTypeDefByGUID";
        final String guidParameterName = "guid";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Perform operation
             */


            // Initialisation

            // Clear the transient type defs
            this.typeDefsForAPI = new TypeDefsByCategory();


            // Invoke internal helper method
            TypeDef ret;
            try {
                // The underlying method handles Famous Five conversions.
                ret = _getTypeDefByGUID(userId, guid);
            } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                LOG.error("{}: re-throwing exception from internal method", methodName, e);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}: ret={}", methodName, ret);
            }
            return ret;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return the AttributeTypeDef identified by the GUID
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String name of the AttributeTypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the name is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public AttributeTypeDef getAttributeTypeDefByGUID(String userId,
                                                      String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName = "getAttributeTypeDefByGUID";
        final String guidParameterName = "guid";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Perform operation
             */

            // Initialisation

            // Clear the transient type defs
            this.typeDefsForAPI = new TypeDefsByCategory();

            // Return object
            AttributeTypeDef ret;
            try {
                ret = _getAttributeTypeDefByGUID(userId, guid);
            } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                LOG.error("{}: re-throwing exception from internal method", methodName, e);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}): ret={}", methodName, userId, guid, ret);
            }
            return ret;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return the TypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name   - String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the name is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested TypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef getTypeDefByName(String userId,
                                    String name)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName = "getTypeDefByName";
        final String nameParameterName = "name";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, name={}", methodName, userId, name);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

            /*
             * Perform operation
             */

            // Clear the transient type defs
            this.typeDefsForAPI = new TypeDefsByCategory();

            TypeDef ret;
            try {
                ret = _getTypeDefByName(userId, name);
            } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                LOG.error("{}: re-throwing exception from internal method", methodName, e);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}: ret={}", methodName, ret);
            }
            return ret;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Internal implementation of getTypeDefByName()
     *
     * @param userId     - unique identifier for requesting user.
     * @param omTypeName - String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     */
    // package private
    TypeDef _getTypeDefByName(String userId,
                              String omTypeName)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        final String methodName = "_getTypeDefByName";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, omTypeName={})", methodName, userId, omTypeName);
        }


        // Strategy:
        // Check name is not null. null => throw
        // Use Atlas typedef store getByName()
        // If you get back a typedef of a category that can be converted to OM typedef then convert it and return the type def.
        // If Atlas type is not of a category that can be converted to an OM TypeDef then return throw TypeDefNotKnownException.


        // Look in the Atlas type def store

        // If the OM typeName is in famous five then you need to look for the corresponding Atlas name
        String atlasTypeName = omTypeName;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName)) {
            // The type to be looked up is in the famous five.
            // We do not have the OM GUID but all we need at the moment is the Atlas type name to look up so GUID can be null
            atlasTypeName = FamousFive.getAtlasTypeName(omTypeName, null);
            LOG.debug("{}: name {} substituted to {}", methodName, omTypeName, atlasTypeName);
        }


        // Look in the Atlas type registry
        AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByName(atlasTypeName);


        // Separate null check ensures we have covered both cases (registry and store)
        if (abtd == null) {

            LOG.debug("{}: received null return from Atlas getByName using name {}", methodName, atlasTypeName);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(atlasTypeName, "unknown", "name", "_getTypeDefByName", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // From here on we know that abtd is not null


        // Underlying method handles Famous Five conversions
        TypeDef ret;
        try {
            LOG.debug("{}: call convertAtlasTypeDefToOMTypeDef with abtd with name {}", methodName, abtd.getName());
            ret = convertAtlasTypeDefToOMTypeDef(userId, abtd);
        } catch (TypeErrorException e) {
            LOG.error("{}: Failed to convert the Atlas type {} to an OM TypeDef", methodName, abtd.getName(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasEntityDef", "getTypeDefByGuid", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: ret={}", methodName, ret);
        }

        return ret;

    }


    /**
     * Return the AttributeTypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name   - String name of the AttributeTypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the name is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public AttributeTypeDef getAttributeTypeDefByName(String userId,
                                                      String name)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName = "getAttributeTypeDefByName";
        final String nameParameterName = "name";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, name={})", methodName, userId, name);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

            /*
             * Perform operation
             */

            // Initialisation

            // Clear the transient type defs
            this.typeDefsForAPI = new TypeDefsByCategory();


            // Return object
            AttributeTypeDef ret;
            try {
                ret = _getAttributeTypeDefByName(userId, name);
            } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                LOG.error("{}: re-throwing exception from internal method", methodName, e);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}: ret={}", methodName, ret);
            }
            return ret;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Create a collection of related types.
     *
     * @param userId   - unique identifier for requesting user.
     * @param newTypes - TypeDefGalleryResponse structure describing the new AttributeTypeDefs and TypeDefs.
     * @throws InvalidParameterException    - the new TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException        - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public void addTypeDefGallery(String         userId,
                                  TypeDefGallery newTypes)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        final String methodName = "addTypeDefGallery";
        final String galleryParameterName = "newTypes";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, newTypes={})", methodName, userId, newTypes);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefGallery(repositoryName, galleryParameterName, newTypes, methodName);

            /*
             * Perform operation
             */

            /*
             * Parse the TypeDefGallery and for each category of TypeDef and AttributeTypeDef perform the
             * corresponding add operation.
             * AttributeTypeDefs and handled first, then TypeDefs.
             *
             * All exceptions thrown by the called methods match the signature of this method, so let them
             * throw through.
             */

            List<AttributeTypeDef> attributeTypeDefs = newTypes.getAttributeTypeDefs();
            if (attributeTypeDefs != null) {
                for (AttributeTypeDef attributeTypeDef : attributeTypeDefs) {
                    addAttributeTypeDef(userId, attributeTypeDef);
                }
            }

            List<TypeDef> typeDefs = newTypes.getTypeDefs();
            if (typeDefs != null) {
                for (TypeDef typeDef : typeDefs) {
                    addTypeDef(userId, typeDef);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}", methodName);
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Create a definition of a new TypeDef.
     *
     * @param userId     - unique identifier for requesting user.
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException    - the new TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException        - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public void addTypeDef(String  userId,
                           TypeDef newTypeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {


        final String methodName = "addTypeDef";
        final String typeDefParameterName = "newTypeDef";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, newTypeDef={})", methodName, userId, newTypeDef);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, newTypeDef, methodName);
            repositoryValidator.validateUnknownTypeDef(repositoryName, typeDefParameterName, newTypeDef, methodName);

            /*
             * Perform operation
             */

            /*
             * Validate the status fields in the passed TypeDef
             */
            boolean statusFieldsValid = validateStatusFields(newTypeDef);
            if (!statusFieldsValid) {
                // We cannot accept this typedef because it contains status fields that cannot be modelled by Atlas
                LOG.error("{}: The TypeDef with name {}, initialStatus {} and validInstanceStatusList {} could not be modelled in Atlas", methodName, newTypeDef, newTypeDef.getInitialStatus(), newTypeDef.getValidInstanceStatusList());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(newTypeDef.getName(), newTypeDef.getGUID(), typeDefParameterName, methodName, repositoryName, newTypeDef.toString());
                throw new TypeDefNotSupportedException(
                        errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            /*
             * Given an OM TypeDef - convert it to corresponding Atlas type def, store it, then read it back and convert
             * it back to OM. The write-through and read-back is so that we pick up any Atlas initializations, e.g. of
             * fields like createdBy, description (if originally null), etc.
             */


            /*
             *  To do this we need to put the Atlas type definition into an AtlasTypesDef
             *  Create an empty AtlasTypesDef container for type to be converted below
             */
            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

            /*
             *  Get category of OM TypeDef, then depending on category instantiate the corresponding Atlas typedef, which will
             * be one of AtlasEntityDef, AtlasRelationshipDef or AtlasClassificationDef
             */
            String newTypeName = newTypeDef.getName();
            TypeDefCategory category = newTypeDef.getCategory();
            LOG.debug("{}: was passed an OM TypeDef with name {} and category {}", methodName, newTypeName, category);
            switch (category) {

                case ENTITY_DEF:
                    // The following method will detect a Famous Five type and convert accordingly.
                    AtlasEntityDef atlasEntityDef = convertOMEntityDefToAtlasEntityDef((EntityDef) newTypeDef);
                    if (atlasEntityDef != null) {
                        atlasTypesDef.getEntityDefs().add(atlasEntityDef);
                    }
                    break;

                case RELATIONSHIP_DEF:
                    try {
                        AtlasRelationshipDef atlasRelationshipDef = convertOMRelationshipDefToAtlasRelationshipDef((RelationshipDef) newTypeDef);
                        if (atlasRelationshipDef != null) {
                            atlasTypesDef.getRelationshipDefs().add(atlasRelationshipDef);
                        }

                    } catch (RepositoryErrorException | TypeErrorException e) {
                        // Log the error and re-throw
                        LOG.debug("{}: caught exception from attempt to convert OM RelationshipDef to Atlas, name {}", methodName, newTypeDef.getName());
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("TypeDef", "retrieval", metadataCollectionId);

                        throw new InvalidTypeDefException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                "addTypeDef",
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    break;

                case CLASSIFICATION_DEF:
                    AtlasClassificationDef atlasClassificationDef = convertOMClassificationDefToAtlasClassificationDef((ClassificationDef) newTypeDef);
                    if (atlasClassificationDef != null) {
                        atlasTypesDef.getClassificationDefs().add(atlasClassificationDef);
                    }
                    break;

                case UNKNOWN_DEF:
                    LOG.debug("{}: cannot convert an OM TypeDef with category {}", methodName, category);
                    break;
            }


            AtlasBaseTypeDef retrievedAtlasTypeDef;
            String atlasTypeName = newTypeName;

            try {
                /*
                 * Add the AtlasTypesDef to the typeDefStore.
                 */
                LOG.debug("{}: add AtlasTypesDef {} to store", methodName, atlasTypesDef);
                typeDefStore.createTypesDef(atlasTypesDef);
                /*
                 * Read the AtlasTypeDef back and convert it back into an OM TypeDef.
                 */

                // Note that if the type is an EntityDef and is in the F5 set, the revised name is needed
                if (FamousFive.omTypeRequiresSubstitution(newTypeName)) {
                    atlasTypeName = FamousFive.getAtlasTypeName(newTypeName, null);
                }

                retrievedAtlasTypeDef = typeDefStore.getByName(atlasTypeName);

                LOG.debug("{}: retrieved created type from store {}", methodName, atlasTypesDef);

            } catch (AtlasBaseException e) {

                LOG.error("{}: exception from store and retrieve AtlasTypesDef {}", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasTypeName, "unknown", typeDefParameterName, methodName, metadataCollectionId);

                throw new InvalidTypeDefException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Formulate the return value - the retrieved Atlas Type is converted into an OM TypeDef
             */
            TypeDef returnableTypeDef = null;
            boolean fatalError = false;
            switch (retrievedAtlasTypeDef.getCategory()) {

                case ENTITY:
                    AtlasEntityDef retrievedAtlasEntityDef = (AtlasEntityDef) retrievedAtlasTypeDef;
                    EntityDef returnableEntityDef;
                    try {
                        AtlasEntityDefMapper atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, retrievedAtlasEntityDef);
                        returnableEntityDef = atlasEntityDefMapper.toOMEntityDef();
                        returnableTypeDef = returnableEntityDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                case RELATIONSHIP:
                    AtlasRelationshipDef retrievedAtlasRelationshipDef = (AtlasRelationshipDef) retrievedAtlasTypeDef;
                    RelationshipDef returnableRelationshipDef;
                    try {
                        AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, retrievedAtlasRelationshipDef);
                        returnableRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
                        returnableTypeDef = returnableRelationshipDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                case CLASSIFICATION:
                    AtlasClassificationDef retrievedAtlasClassificationDef = (AtlasClassificationDef) retrievedAtlasTypeDef;
                    ClassificationDef returnableClassificationDef;
                    try {
                        AtlasClassificationDefMapper atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, retrievedAtlasClassificationDef);
                        returnableClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();
                        returnableTypeDef = returnableClassificationDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                default:
                    LOG.debug("{}: cannot convert an OM TypeDef with category {}", methodName, category);
                    fatalError = true;
            }

            if (fatalError || returnableTypeDef == null) {

                LOG.error("{}: could not initialise mapper or convert retrieved AtlasBaseTypeDef {} to OM TypeDef", methodName, atlasTypeName);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("TypeDef", "addTypeDef", metadataCollectionId);
                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "addTypeDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, newTypeDef={})", methodName, userId, returnableTypeDef);
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Create a definition of a new AttributeTypeDef.
     *
     * @param userId              - unique identifier for requesting user.
     * @param newAttributeTypeDef - TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException  - the new TypeDef is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefKnownException      - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException   - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException    - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void addAttributeTypeDef(String           userId,
                                    AttributeTypeDef newAttributeTypeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        final String methodName = "addAttributeTypeDef";
        final String typeDefParameterName = "newAttributeTypeDef";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, newAttributeTypeDef={})", methodName, userId, newAttributeTypeDef);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, newAttributeTypeDef, methodName);
            repositoryValidator.validateUnknownAttributeTypeDef(repositoryName, typeDefParameterName, newAttributeTypeDef, methodName);

            /*
             * Perform operation
             */

            /*
             *  Given an OM AttributeTypeDef - convert it to corresponding AtlasAttributeDef
             *
             *  If asked to create a Primitive or Collection there is not really a corresponding Atlas type, so
             *  need to detect whether we are creating an Enum... otherwise no action.
             *
             * Approach:
             * Get the category of OM AttributeTypeDef, then depending on category instantiate the corresponding
             * AtlasAttributeDef
             *
             *  AttributeTypeDefCategory category        --> switch between Atlas cases and CTOR will set Atlas category
             *  The category is one of: UNKNOWN_DEF, PRIMITIVE, COLLECTION, ENUM_DEF
             *   String                   guid            --> IGNORED
             *   String                   name            --> Atlas name
             *   String                   description     --> Atlas description
             *   String                   descriptionGUID --> IGNORED (these are always set to null for now)
             *
             */


            // Create an empty AtlasTypesDef container for whatever we convert below..
            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

            AttributeTypeDefCategory category = newAttributeTypeDef.getCategory();

            if (category == ENUM_DEF) {
                EnumDef omEnumDef = (EnumDef) newAttributeTypeDef;
                // Building an AtlasEnumDef we need all the fields of the AtlasBaseTypeDef plus the
                // AtlasEnumDef specific extensions, i.e.
                // TypeCategory category; --> set by CTOR
                // String  guid           --> set from OM guid
                // String  createdBy      --> set to user
                // String  updatedBy      --> set to user
                // Date    createTime     --> set to NOW
                // Date    updateTime     --> set to NOW
                // Long    version        --> set to 1     - this is the initial create
                // String  name;          --> set from OM name
                // String  description;   --> set from OM description
                // String  typeVersion;   --> set to "1"
                // Map<String, String> options;  --> set to null
                // List<AtlasEnumElementDef> elementDefs;  --> set from OM elementDefs
                // String                    defaultValue;  --> set from OM defaultValue
                //
                // The OM EnumDef provides
                // AttributeTypeDefCategory category
                // String                   guid
                // String                   name
                // String                   description
                // String                   descriptionGUID --> IGNORED
                // ArrayList<EnumElementDef> elementDefs
                // EnumElementDef            defaultValue
                //

                AtlasEnumDef atlasEnumDef = new AtlasEnumDef();
                atlasEnumDef.setGuid(omEnumDef.getGUID());
                atlasEnumDef.setCreatedBy(userId);
                atlasEnumDef.setUpdatedBy(userId);
                Date now = new Date();
                atlasEnumDef.setCreateTime(now);
                atlasEnumDef.setUpdateTime(now);
                atlasEnumDef.setVersion(1L);
                atlasEnumDef.setName(omEnumDef.getName());
                atlasEnumDef.setDescription(omEnumDef.getDescription());
                atlasEnumDef.setTypeVersion("1");
                atlasEnumDef.setOptions(null);
                // EnumElements
                List<AtlasEnumDef.AtlasEnumElementDef> atlasElemDefs = null;
                List<EnumElementDef> omEnumElems = omEnumDef.getElementDefs();
                if (omEnumElems != null && !(omEnumElems.isEmpty())) {
                    atlasElemDefs = new ArrayList<>();
                    for (EnumElementDef omElemDef : omEnumDef.getElementDefs()) {
                        AtlasEnumDef.AtlasEnumElementDef atlasElemDef = new AtlasEnumDef.AtlasEnumElementDef();
                        atlasElemDef.setValue(omElemDef.getValue());
                        atlasElemDef.setDescription(omElemDef.getDescription());
                        atlasElemDef.setOrdinal(omElemDef.getOrdinal());
                        atlasElemDefs.add(atlasElemDef);
                    }
                }
                atlasEnumDef.setElementDefs(atlasElemDefs);
                // Default value
                EnumElementDef omDefaultValue = omEnumDef.getDefaultValue();
                LOG.debug("{}: omDefaultValue {}", methodName, omDefaultValue);
                if (omDefaultValue != null) {
                    atlasEnumDef.setDefaultValue(omDefaultValue.getValue());
                    LOG.debug("{}: Atlas default value set to {}", methodName, atlasEnumDef.getDefaultValue());
                }

                LOG.debug("{}: create AtlasEnumDef {}", methodName, atlasEnumDef);

                // Add the AtlasEnumDef to the AtlasTypesDef so it can be added to the TypeDefStore...
                atlasTypesDef.getEnumDefs().add(atlasEnumDef);

                // Add the AtlasTypesDef to the typeDefStore.
                // To do this we need to put the EntityDef into an AtlasTypesDef
                LOG.debug("{}: create Atlas types {}", methodName, atlasTypesDef);
                try {
                    typeDefStore.createTypesDef(atlasTypesDef);

                } catch (AtlasBaseException e) {

                    LOG.error("{}: caught exception from Atlas, error code {}", methodName, e);
                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_TYPEDEF_CREATE_FAILED;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEnumDef.toString(), methodName, repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            } else {
                LOG.debug("{}: category is {}, so no action needed", methodName, category);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, newAttributeTypeDef={})", methodName, newAttributeTypeDef);
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }

    /**
     * Verify whether a definition of a TypeDef is known. The method tests whether a type with the
     * specified name is known and whether that type matches the presented type, in which case the
     * it is verified and returns true.
     * If the type (name) is not known the method returns false.
     * If the type name is one of the Famous Five then convert the name to extended form. If the
     * extended type does not exist return false, to promote an addTypeDef(). During the add
     * the name is again converted so that the extended type is added.
     * If the type is not valid or conflicts with an existing type (of the same name) then the
     * method throws the relevant exception.
     *
     * @param userId  - unique identifier for requesting user.
     * @param typeDef - TypeDef structure describing the TypeDef to test.
     * @return boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known.
     * @throws InvalidParameterException    - the TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public boolean verifyTypeDef(String  userId,
                                 TypeDef typeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        final String methodName = "verifyTypeDef";
        final String typeDefParameterName = "typeDef";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, typeDef={})", methodName, userId, typeDef);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, typeDef, methodName);

            /*
             * Perform operation
             */

            String typeName = typeDef.getName();

            /*
             * Validate the status fields in the passed TypeDef
             */
            boolean statusFieldsValid = validateStatusFields(typeDef);
            if (!statusFieldsValid) {
                // We cannot accept this typedef because it contains status fields that cannot be modelled by Atlas
                LOG.error("{}: The TypeDef with name {}, initialStatus {} and validInstanceStatusList {} could not be modelled in Atlas", methodName, typeName, typeDef.getInitialStatus(), typeDef.getValidInstanceStatusList());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
                throw new TypeDefNotSupportedException(
                        errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Perform type dependency existence checks - for any TypeDef check that the immediate supertype (if any) has already been defined.
             * Further checks are necessary for relationship defs and classification defs - in the switch cases below.
             */
            TypeDefLink superType = typeDef.getSuperType();
            if (superType != null) {
                TypeDef existingSuperTypeDef;
                String superTypeName = superType.getName();
                String superTypeGUID = superType.getGUID();
                String typeNameToLookFor = superTypeName;
                if (FamousFive.omTypeRequiresSubstitution(superTypeName)) {
                    LOG.debug("{}: supertype name {} requires substitution guid {}", methodName, superTypeName, superTypeGUID);
                    typeNameToLookFor = FamousFive.getAtlasTypeName(superTypeName, superTypeGUID);
                }
                try {
                    existingSuperTypeDef = _getTypeDefByName(userId, typeNameToLookFor);
                } catch (TypeDefNotKnownException e) {
                    // Handle below
                    existingSuperTypeDef = null;
                }
                if (existingSuperTypeDef == null) {
                    // Log using both the original typeName and extended name where different
                    if (typeNameToLookFor.equals(typeName))
                        LOG.debug("{}: no existing TypeDef for supertype with name {}", methodName, superTypeName);
                    else
                        LOG.debug("{}: requested TypeDef for supertype name {} mapped to name {} which is not in the repo, ", methodName, superTypeName, typeNameToLookFor);
                    // We cannot accept this typedef because it refers to a supertype that does not have a TypeDef in Atlas
                    LOG.error("{}: The TypeDef with name {} and supertype name {} could not be modelled in Atlas", methodName, typeName, superTypeName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
                    throw new TypeDefNotSupportedException(
                            errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }


            // Clear the per-API records of TypeDefs
            typeDefsForAPI = new TypeDefsByCategory();


            boolean ret = false;
            boolean conflict = false;
            TypeDef existingTypeDef;

            // Verify whether the supplied TypeDef is known in the cache.

            TypeDefCategory tdCat = typeDef.getCategory();
            switch (tdCat) {

                case ENTITY_DEF:

                    /*
                     * With regard to type dependency existence checks - for an EntityDef it is only necessary to check that the
                     * immediate supertype (if any) has already been defined. This is true for relationship defs and
                     * classification defs too - so it was already processed above.
                     */


                    /*
                     * If the type name matches one of the Famous Five then convert the name to extended form.
                     * It is the extended type that we must look for in the repo. If the extended type does not
                     * exist then return false. This will provoke an addTypeDef (of the same type) which will
                     * allow us to create an extended type in Atlas.
                     * If the extended type does exist then convert it back prior to the compare.
                     *
                     */

                    String entityTypeNameToLookFor = typeName;
                    if (FamousFive.omTypeRequiresSubstitution(typeName)) {
                        LOG.debug("{}: type name {} requires substitution", methodName, typeName);
                        entityTypeNameToLookFor = FamousFive.getAtlasTypeName(typeName, typeDef.getGUID());
                    }

                    // Ask Atlas...
                    try {
                        existingTypeDef = _getTypeDefByName(userId, entityTypeNameToLookFor);
                    } catch (TypeDefNotKnownException e) {
                        // Handle below
                        existingTypeDef = null;
                    }
                    if (existingTypeDef == null) {
                        // Log using both the original typeName and extended name where different
                        if (entityTypeNameToLookFor.equals(typeName))
                            LOG.debug("{}: no existing TypeDef with name {}", methodName, typeName);
                        else
                            LOG.debug("{}: requested TypeDef name {} mapped to name {} which is not in the repo, ", methodName, typeName, entityTypeNameToLookFor);
                        ret = false;
                    } else if (existingTypeDef.getCategory() == ENTITY_DEF) {
                        EntityDef existingEntityDef = (EntityDef) existingTypeDef;
                        LOG.debug("{}: existing EntityDef: {}", methodName, existingEntityDef);

                        // The definition is not new - compare the existing def with the one passed
                        // A compare will be too strict - instead use an equivalence check...
                        Comparator comp = new Comparator();
                        EntityDef passedEntityDef = (EntityDef) typeDef;
                        LOG.debug("{}: new typedef: {}", methodName, passedEntityDef);
                        boolean match = comp.equivalent(existingEntityDef, passedEntityDef);
                        LOG.debug("{}: equivalence result {}", methodName, match);
                        if (match) {
                            // The definition is known and matches - return true
                            ret = true;
                        } else {
                            conflict = true;
                        }
                    } else {
                        conflict = true;
                    }
                    break;

                case RELATIONSHIP_DEF:

                    /*
                     * Perform type dependency existence checks - supertype was already checked above. For a RelationshipDef also check that the
                     * types for the ends have already been defined.
                     */
                    RelationshipDef relDef = (RelationshipDef) typeDef;
                    ArrayList<RelationshipEndDef> endDefs = new ArrayList<>();
                    endDefs.add(relDef.getEndDef1());
                    endDefs.add(relDef.getEndDef2());
                    for (RelationshipEndDef endDef : endDefs) {

                        if (endDef == null || endDef.getEntityType() == null) {
                            // Invalid end definition - throw exception
                            LOG.error("{}: The TypeDef with name {} has an invalid end definition {}", methodName, typeName, endDef);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
                            throw new TypeDefNotSupportedException(
                                    errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                        TypeDefLink entityType = endDef.getEntityType();

                        TypeDef existingEntityTypeDef;
                        String entityTypeName = entityType.getName();
                        String entityTypeGUID = entityType.getGUID();
                        String endTypeNameToLookFor = entityTypeName;
                        if (FamousFive.omTypeRequiresSubstitution(entityTypeName)) {
                            LOG.debug("{}: supertype name {} requires substitution", methodName, entityTypeName);
                            endTypeNameToLookFor = FamousFive.getAtlasTypeName(entityTypeName, entityTypeGUID);
                        }
                        try {
                            existingEntityTypeDef = _getTypeDefByName(userId, endTypeNameToLookFor);
                        } catch (TypeDefNotKnownException e) {
                            // Handle below
                            existingEntityTypeDef = null;
                        }
                        if (existingEntityTypeDef == null) {
                            // Log using both the original typeName and extended name where different
                            if (endTypeNameToLookFor.equals(entityTypeName))
                                LOG.debug("{}: no existing TypeDef for entity type with name {}", methodName, entityTypeName);
                            else
                                LOG.debug("{}: requested TypeDef for entity type name {} mapped to name {} which is not in the repo, ", methodName, entityTypeName, endTypeNameToLookFor);
                            // We cannot accept this typedef because it refers to an end entity type that does not have a TypeDef in Atlas
                            LOG.error("{}: The TypeDef with name {} and end entity type name {} could not be modelled in Atlas", methodName, typeName, entityTypeName);
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
                            throw new TypeDefNotSupportedException(
                                    errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }


                    }


                    // Ask Atlas...
                    try {
                        existingTypeDef = _getTypeDefByName(userId, typeName);
                    } catch (TypeDefNotKnownException e) {
                        // Handle below
                        existingTypeDef = null;
                    }
                    if (existingTypeDef == null) {
                        LOG.debug("{}: no existing TypeDef with name {}", methodName, typeName);
                        ret = false;
                    } else if (existingTypeDef.getCategory() == RELATIONSHIP_DEF) {
                        RelationshipDef existingRelationshipDef = (RelationshipDef) existingTypeDef;
                        LOG.debug("{}: existing RelationshipDef: {}", methodName, existingRelationshipDef);

                        // The definition is not new - compare the existing def with the one passed
                        // A compare will be too strict - instead use an equivalence check...
                        Comparator comp = new Comparator();
                        RelationshipDef passedRelationshipDef = (RelationshipDef) typeDef;
                        LOG.debug("{}: new typedef: {}", methodName, passedRelationshipDef);
                        boolean match = comp.equivalent(existingRelationshipDef, passedRelationshipDef);
                        LOG.debug("{}: equivalence result {}", methodName, match);
                        if (match) {
                            // The definition is known and matches - return true
                            ret = true;
                        } else {
                            conflict = true;
                        }
                    } else {
                        conflict = true;
                    }
                    break;

                case CLASSIFICATION_DEF:

                    /*
                     * Perform type dependency existence checks - supertype was already checked above. For a ClassificationDef also check that the
                     * types for the validEntityDefs have already been defined.
                     */
                    ClassificationDef classificationDef = (ClassificationDef) typeDef;
                    List<TypeDefLink> validEntityDefs = classificationDef.getValidEntityDefs();
                    if (validEntityDefs != null) {

                        for (TypeDefLink validEntityDef : validEntityDefs) {

                            TypeDef existingEntityTypeDef;
                            String validEntityTypeName = validEntityDef.getName();
                            String validEntityTypeGUID = validEntityDef.getGUID();
                            String validTypeNameToLookFor = validEntityTypeName;
                            if (FamousFive.omTypeRequiresSubstitution(validEntityTypeName)) {
                                LOG.debug("{}: supertype name {} requires substitution", methodName, validEntityTypeName);
                                validTypeNameToLookFor = FamousFive.getAtlasTypeName(validEntityTypeName, validEntityTypeGUID);
                            }
                            try {
                                existingEntityTypeDef = _getTypeDefByName(userId, validTypeNameToLookFor);
                            } catch (TypeDefNotKnownException e) {
                                // Handle below
                                existingEntityTypeDef = null;
                            }
                            if (existingEntityTypeDef == null) {
                                // Log using both the original typeName and extended name where different
                                if (validTypeNameToLookFor.equals(validEntityTypeName))
                                    LOG.debug("{}: no existing TypeDef for valid entity type with name {}", methodName, validEntityTypeName);
                                else
                                    LOG.debug("{}: requested TypeDef for valid entity type name {} mapped to name {} which is not in the repo, ", methodName, validEntityTypeName, validTypeNameToLookFor);
                                // We cannot accept this typedef because it refers to a valid entity type that does not have a TypeDef in Atlas
                                LOG.error("{}: The TypeDef with name {} and valid entity type name {} could not be modelled in Atlas", methodName, typeName, validEntityTypeName);
                                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
                                throw new TypeDefNotSupportedException(
                                        errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }
                        }
                    }

                    // Ask Atlas...
                    try {
                        existingTypeDef = _getTypeDefByName(userId, typeName);
                    } catch (TypeDefNotKnownException e) {
                        // Handle below
                        existingTypeDef = null;
                    }
                    if (existingTypeDef == null) {
                        LOG.debug("{}: no existing TypeDef with name {}", methodName, typeName);
                        ret = false;
                    } else if (existingTypeDef.getCategory() == CLASSIFICATION_DEF) {
                        ClassificationDef existingClassificationDef = (ClassificationDef) existingTypeDef;
                        LOG.debug("{}: existing ClassificationDef: {}", methodName, existingClassificationDef);

                        // The definition is not new - compare the existing def with the one passed
                        // A compare will be too strict - instead use an equivalence check...
                        Comparator comp = new Comparator();
                        ClassificationDef passedClassificationDef = (ClassificationDef) typeDef;
                        LOG.debug("{}: new typedef: {}", methodName, passedClassificationDef);
                        boolean match = comp.equivalent(existingClassificationDef, passedClassificationDef);
                        LOG.debug("{}: equivalence result {}", methodName, match);
                        if (match) {
                            // The definition is known and matches - return true
                            ret = true;
                        } else {
                            conflict = true;
                        }
                    } else {
                        conflict = true;
                    }
                    break;

                default:
                    // The typedef category is not supported - raise an exception
                    LOG.error("{}: The supplied TypeDef with name {} has an unsupported category {}", methodName, typeName, tdCat);
                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_CATEGORY;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(tdCat.toString(), typeDef.toString(), methodName, repositoryName);

                    throw new TypeDefNotSupportedException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
            }


            if (conflict) {
                // The definition is known but conflicts - raise an exception
                // The typeDef was known but conflicted in some way with the existing type - raise an exception
                LOG.error("{}: The TypeDef with name {} conflicts with an existing type {}", methodName, typeName, existingTypeDef);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
                throw new TypeDefConflictException(
                        errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, typeDef={}): ret={}", methodName, userId, typeDef, ret);
            }

            return ret;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }

    /**
     * Verify whether a definition of an AttributeTypeDef is known. The method tests whether a type with the
     * specified name is known and whether that type matches the presented type, in which case the
     * it is verified and returns true.
     * If the type (name) is not known the method returns false.
     * If the type name is one of the Famous Five then return false, to provoke an addTypeDef().
     * If the type is not valid or conflicts with an existing type (of the same name) then the
     * method throws the relevant exception.
     *
     * @param userId           - unique identifier for requesting user.
     * @param attributeTypeDef - TypeDef structure describing the TypeDef to test.
     * @return boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known.
     * @throws InvalidParameterException  - the TypeDef is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefConflictException   - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException    - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public boolean verifyAttributeTypeDef(String           userId,
                                          AttributeTypeDef attributeTypeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        final String methodName = "verifyAttributeTypeDef";
        final String typeDefParameterName = "attributeTypeDef";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, typeDef={})", methodName, userId, attributeTypeDef);
        }

        String source = metadataCollectionId;


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, attributeTypeDef, methodName);

            /*
             * Perform operation
             */

            // Clear the per-API records of TypeDefs
            typeDefsForAPI = new TypeDefsByCategory();

            /*
             * If a PRIMITIVE or COLLECTION (MAP or ARRAY) then check in the RCM and if not known return false to trigger an immediate
             * addAttributeTypeDef(). The ATD will then be added with the specified GUID. The benefit of this is that the LocalConnector
             * then populates the RCM with the OM types and correct GUIDs instead of the AtlasConnector fabricating them.
             * If we are dealing with an ENUM then we need to do more work. Note that in Atlas an ENUM is a Type whereas in OM it is an
             * Attribute Type.
             */
            boolean ret = false;
            String atdName = attributeTypeDef.getName();
            AttributeTypeDefCategory atdCat = attributeTypeDef.getCategory();
            switch (atdCat) {

                // The verifyATD method is called during startup at which time the ATD will not be known; but it
                // can also be called at any time. If called after startup is complete all types will have been
                // loaded and the RCM will have been primed. So check whether the RCM knows the type and if so
                // return TRUE. If the RCM does not know the ATD then return FALSE  - during startup this will
                // trigger a call to addAttributeTypeDef.

                case PRIMITIVE:
                    LOG.debug("{}: supplied AttributeTypeDef has category PRIMITIVE - consulting RCM", methodName);
                    AttributeTypeDef rcmPrimitiveATD;
                    try {
                        rcmPrimitiveATD = repositoryHelper.getAttributeTypeDefByName(source, atdName);
                    } catch (OMRSLogicErrorException e) {
                        LOG.error("{}: caught exception from repository helper for type name {}", methodName, atdName, e);
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    // If the type is not know by the RCM that's OK - just return false;
                    // If the type was known by the RCM - return true
                    ret = (rcmPrimitiveATD != null);
                    break;

                case COLLECTION:
                    LOG.debug("{}: supplied AttributeTypeDef has category COLLECTION - consulting RCM", methodName);
                    AttributeTypeDef rcmCollectionATD;
                    try {
                        rcmCollectionATD = repositoryHelper.getAttributeTypeDefByName(source, atdName);
                    } catch (OMRSLogicErrorException e) {
                        LOG.error("{}: caught exception from repository helper for type name {}", methodName, atdName, e);
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    // If the type is not know by the RCM that's OK - just return false;
                    // If the type was known by the RCM - return true
                    ret = (rcmCollectionATD != null);
                    break;

                case ENUM_DEF:
                    boolean conflict = false;
                    LOG.debug("{}: supplied AttributeTypeDef has category ENUM_DEF - checking for existence", methodName);
                    // Look in Atlas to see whether we have an ENUM of this name and if so
                    // perform a comparison.
                    AttributeTypeDef existingATD;
                    try {
                        existingATD = _getAttributeTypeDefByName(userId, atdName);
                    } catch (TypeDefNotKnownException e) {
                        // handle below
                        existingATD = null;
                    }
                    if (existingATD == null) {
                        LOG.debug("{}: no existing enum def with the name {}", methodName, atdName);
                        ret = false;
                    } else if (existingATD.getCategory() == ENUM_DEF) {
                        EnumDef existingEnumDef = (EnumDef) existingATD;
                        LOG.debug("{}: existing enum def: {}", methodName, existingEnumDef);

                        // The definition is not new - compare the existing def with the one passed
                        // A compare will be too strict - instead use an equivalence check...
                        Comparator comp = new Comparator();
                        EnumDef passedEnumDef = (EnumDef) attributeTypeDef;
                        LOG.debug("{}: new enum def: {}", methodName, passedEnumDef);
                        boolean match = comp.equivalent(existingEnumDef, passedEnumDef);
                        LOG.debug("{}: equivalence result: {}", methodName, match);
                        if (match) {
                            // The definition is known and matches - return true
                            ret = true;
                        } else {
                            conflict = true;
                        }
                    } else {
                        conflict = true;
                    }
                    if (conflict) {
                        // The typeDef was known but conflicted in some way with the existing type - raise an exception
                        LOG.error("{}: The supplied AttributeTypeDef conflicts with an existing type with name {}", methodName, atdName);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("typeDef", methodName, repositoryName);

                        throw new TypeDefConflictException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }

                    break;

                case UNKNOWN_DEF:
                    LOG.error("{}: The supplied AttributeTypeDef {} has unsupported category {}", methodName, atdName, atdCat);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("typeDef", methodName, repositoryName);

                    throw new TypeDefConflictException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, typeDef={}): ret={}", methodName, userId, attributeTypeDef, ret);
            }
            return ret;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }

    /**
     * Update one or more properties of the TypeDef.  The TypeDefPatch controls what types of updates
     * are safe to make to the TypeDef.
     *
     * @param userId       - unique identifier for requesting user.
     * @param typeDefPatch - TypeDef patch describing change to TypeDef.
     * @return updated TypeDef
     * @throws InvalidParameterException  - the TypeDefPatch is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested TypeDef is not found in the metadata collection.
     * @throws PatchErrorException        - the TypeDef can not be updated because the supplied patch is incompatible
     *                                    with the stored TypeDef.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef updateTypeDef(String       userId,
                                 TypeDefPatch typeDefPatch)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            PatchErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "updateTypeDef";
        final String typeDefParameterName = "typeDefPatch";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */

            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefPatch(repositoryName, typeDefPatch, methodName);

            /*
             * Perform operation
             */

            /*
             * A TypeDefPatch describes a change (patch) to a typeDef's properties, options, external
             * standards mappings or list of valid instance statuses.
             * A patch can be applied to an EntityDef, RelationshipDef or ClassificationDef.
             * Changes to a TypeDef's category or superclasses requires a new type definition.
             * In addition it is not possible to delete an attribute through a patch.
             *
             * The TypeDefPatch contains:
             *
             *   TypeDefPatchAction                 action
             *   String                             typeDefGUID
             *   String                             typeName
             *   long                               applyToVersion
             *   long                               updateToVersion
             *   String                             newVersionName
             *   String                             description
             *   String                             descriptionGUID
             *   ArrayList<TypeDefAttribute>        typeDefAttributes
             *   Map<String, String>                typeDefOptions
             *   ArrayList<ExternalStandardMapping> externalStandardMappings
             *   ArrayList<InstanceStatus>          validInstanceStatusList    - note that no action is defined for this
             *
             * The validateTypeDefPatch above merely checks the patch is not null. Within the patch itself
             * some fields are always mandatory and the presence of others depend on the action being performed.
             * Mandatory fields: action, typeDefGUID, typeName, applyToVersion, updateToVersion, newVersionName
             * Remaining fields are optional, subject to action (e.g. if add_options then options are needed)
             *
             * The TypeDefPatchAction can be one of:
             *  ADD_ATTRIBUTES                    ==> typeDefAttributes must be supplied
             *  ADD_OPTIONS                       ==> typeDefOptions must be supplied
             *  UPDATE_OPTIONS                    ==> typeDefOptions must be supplied
             *  DELETE_OPTIONS                    ==> typeDefOptions must be supplied
             *  ADD_EXTERNAL_STANDARDS            ==> externalStandardMappings must be supplied
             *  UPDATE_EXTERNAL_STANDARDS         ==> externalStandardMappings must be supplied
             *  DELETE_EXTERNAL_STANDARDS         ==> externalStandardMappings must be supplied
             *  UPDATE_DESCRIPTIONS               ==> description must be supplied; descriptionGUID is optional
             */

            /*
             * Versions
             *
             * The applyToVersion field in the TypeDefPatch is optional.
             * The Atlas Connector can only retrieve the TypeDef by GUID or name, not by version.
             * The returned TypeDef is assumed to be the current/active version and if applyToVersion
             * is specified (!= 0L) the connector will check that it matches the retrieved TypeDef
             * and throw an exception if it does not match.
             * If applyToVersion is not specified (== 0L) then the patch will be applied to the
             * current (retrieved) TypeDef.
             *
             * The updateToVersion and newVersionName fields in the TypeDefPatch are optional.
             * If updateToVersion and/or newVersionName are not supplied the connector can generate
             * values for them (in the updated TypeDef) because the updated TypeDef is returned to
             * the caller (i.e. LocalOMRSMetadataCollection) which then updates the RepositoryContentManager
             * (RCM). The new version information generated by the connector will therefore be reflected in
             * the RCM.
             */

            /*
             * Identification
             * This method can tolerate either typeDefGUID OR typeName being absent, but not both.
             */
            String typeDefGUID = typeDefPatch.getTypeDefGUID();
            String typeName = typeDefPatch.getTypeName();
            if (typeDefGUID == null && typeName == null) {

                LOG.error("{}: At least one of typeDefGUID and typeName must be supplied", methodName);

                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeDefParameterName, methodName, repositoryName);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            /*
             * Find the TypeDef - as described above this will only get the 'current' version.
             * Check the retrieved version matches the applyToVersion, if specified.
             * Perform the requested action.
             */
            AtlasBaseTypeDef atlasPreTypeDef;

            // Find the TypeDef
            if (typeDefGUID != null) {

                // Look in the Atlas registry
                atlasPreTypeDef = typeRegistry.getTypeDefByGuid(typeDefGUID);

            } else { // we know that typeName != null

                // Look in the Atlas registry
                atlasPreTypeDef = typeRegistry.getTypeDefByName(typeName);

            }

            // Separate null check ensures we have covered both cases (registry and store)
            if (atlasPreTypeDef == null) {

                LOG.error("{}: received null return from Atlas getByName using name {}", methodName, typeName);
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, "unknown", "name", methodName, metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // From here on we know that atlasPreTypeDef is not null

            // Check the version
            Long applyToVersion = typeDefPatch.getApplyToVersion();
            Long preTypeDefVersion = atlasPreTypeDef.getVersion();
            if (applyToVersion != 0L && !applyToVersion.equals(preTypeDefVersion)) {

                LOG.error("{}: Retrieved TypeDef version {} does not match version {} specified in patch", methodName, preTypeDefVersion, applyToVersion);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, typeDefGUID, typeDefParameterName, methodName, repositoryName, typeDefPatch.toString());

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            // Convert the Atlas TypeDef to an OM TypeDef
            // We are only concerned with EntityDef, RelationshipDef and ClassificationDef TypeDefs
            TypeDef omPreTypeDef = null;
            boolean fatalError = false;
            TypeCategory atlasCategory = atlasPreTypeDef.getCategory();
            switch (atlasCategory) {

                case ENTITY:
                    AtlasEntityDef retrievedAtlasEntityDef = (AtlasEntityDef) atlasPreTypeDef;
                    AtlasEntityDefMapper atlasEntityDefMapper;
                    EntityDef returnableEntityDef;
                    try {
                        atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, retrievedAtlasEntityDef);
                        returnableEntityDef = atlasEntityDefMapper.toOMEntityDef();
                        omPreTypeDef = returnableEntityDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                case RELATIONSHIP:
                    AtlasRelationshipDef retrievedAtlasRelationshipDef = (AtlasRelationshipDef) atlasPreTypeDef;
                    AtlasRelationshipDefMapper atlasRelationshipDefMapper;
                    RelationshipDef returnableRelationshipDef;
                    try {
                        atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, retrievedAtlasRelationshipDef);
                        returnableRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
                        omPreTypeDef = returnableRelationshipDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                case CLASSIFICATION:
                    AtlasClassificationDef retrievedAtlasClassificationDef = (AtlasClassificationDef) atlasPreTypeDef;
                    AtlasClassificationDefMapper atlasClassificationDefMapper;
                    ClassificationDef returnableClassificationDef;
                    try {
                        atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, retrievedAtlasClassificationDef);
                        returnableClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();
                        omPreTypeDef = returnableClassificationDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                default:
                    LOG.debug("{}: cannot convert an OM TypeDef with category {}", methodName, atlasCategory);
                    fatalError = true;
            }

            if (fatalError || omPreTypeDef == null) {

                LOG.error("{}: could not initialise mapper or convert retrieved AtlasBaseTypeDef {} to OM TypeDef", methodName, atlasPreTypeDef.getName());
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("TypeDef", "updateTypeDef", metadataCollectionId);
                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            // Now have established an omPreTypeDef to which action can be performed...
            LOG.debug("{}: AtlasEntityDef mapped to OM TypeDef {}", methodName, omPreTypeDef);


            // Perform the requested action
            TypeDef omPostTypeDef;
            switch (typeDefPatch.getAction()) {
                case ADD_OPTIONS:
                    omPostTypeDef = updateTypeDefAddOptions(omPreTypeDef, typeDefPatch.getTypeDefOptions());
                    break;
                case DELETE_OPTIONS:
                    omPostTypeDef = updateTypeDefDeleteOptions(omPreTypeDef, typeDefPatch.getTypeDefOptions());
                    break;
                case UPDATE_OPTIONS:
                    omPostTypeDef = updateTypeDefUpdateOptions(omPreTypeDef, typeDefPatch.getTypeDefOptions());
                    break;
                case ADD_ATTRIBUTES:
                    omPostTypeDef = updateTypeDefAddAttributes(omPreTypeDef, typeDefPatch.getTypeDefAttributes());
                    break;
                case UPDATE_DESCRIPTIONS:
                    omPostTypeDef = updateTypeDefUpdateDescriptions(omPreTypeDef, typeDefPatch.getDescription(), typeDefPatch.getDescriptionGUID());
                    break;
                case ADD_EXTERNAL_STANDARDS:
                    omPostTypeDef = updateTypeDefAddExternalStandards(omPreTypeDef, typeDefPatch.getExternalStandardMappings());
                    break;
                case DELETE_EXTERNAL_STANDARDS:
                    omPostTypeDef = updateTypeDefDeleteExternalStandards(omPreTypeDef, typeDefPatch.getExternalStandardMappings());
                    break;
                case UPDATE_EXTERNAL_STANDARDS:
                    omPostTypeDef = updateTypeDefUpdateExternalStandards(omPreTypeDef, typeDefPatch.getExternalStandardMappings());
                    break;
                // There maybe should be instance status actions too, but these are not yet defined (see TypeDefPatchAction)
                default:
                    LOG.error("{}: TypeDefPatch action {} is not recognised", methodName, typeDefPatch.getAction());
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(typeName, typeDefGUID, typeDefParameterName, methodName, repositoryName, typeDefPatch.toString());

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

            }

            LOG.debug("{}: omPostTypeDef {} ", methodName, omPostTypeDef);

            /*
             * Convert the updated OM TypeDef to Atlas TypeDef and update in Atlas, read it back and convert to OM.
             * The write-through and read-back is so that we pick up any Atlas initializations, e.g. of
             * fields like createdBy, description (if originally null), etc.
             */


            /*
             * Depending on OM TypeDef category, perform the corresponding Atlas update, which will be for one of
             * AtlasEntityDef, AtlasRelationshipDef or AtlasClassificationDef
             */

            AtlasBaseTypeDef retrievedAtlasTypeDef = null;
            try {

                TypeDefCategory category = omPostTypeDef.getCategory();
                LOG.debug("{}: convert and store updated OM TypeDef with guid {} and category {}", methodName, typeDefGUID, category);
                switch (category) {

                    case ENTITY_DEF:
                        // convertOMEntityDefToAtlasEntityDef will detect a Famous Five type and convert accordingly.
                        AtlasEntityDef atlasEntityDef = convertOMEntityDefToAtlasEntityDef((EntityDef) omPostTypeDef);
                        if (atlasEntityDef != null) {
                            LOG.debug("{}: Call Atlas updateEntityDefByGuid with guid {} entity {}", methodName, typeDefGUID, atlasEntityDef);
                            retrievedAtlasTypeDef = typeDefStore.updateEntityDefByGuid(typeDefGUID, atlasEntityDef);
                            if (retrievedAtlasTypeDef == null)
                                throw new AtlasBaseException();
                        }
                        break;

                    case RELATIONSHIP_DEF:
                        try {
                            AtlasRelationshipDef atlasRelationshipDef = convertOMRelationshipDefToAtlasRelationshipDef((RelationshipDef) omPostTypeDef);
                            if (atlasRelationshipDef != null) {
                                LOG.debug("{}: Call Atlas updateEntityDefByGuid with guid {} entity {}", methodName, typeDefGUID, atlasRelationshipDef);
                                retrievedAtlasTypeDef = typeDefStore.updateRelationshipDefByGuid(typeDefGUID, atlasRelationshipDef);
                                if (retrievedAtlasTypeDef == null)
                                    throw new AtlasBaseException();
                            }
                        } catch (RepositoryErrorException | TypeErrorException e) {
                            // Log the error and re-throw
                            LOG.debug("{}: caught exception from attempt to convert OM RelationshipDef to Atlas, name {}", methodName, omPostTypeDef.getName());
                            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage("TypeDef", methodName, metadataCollectionId);

                            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                        break;

                    case CLASSIFICATION_DEF:
                        AtlasClassificationDef atlasClassificationDef = convertOMClassificationDefToAtlasClassificationDef((ClassificationDef) omPostTypeDef);
                        if (atlasClassificationDef != null) {
                            LOG.debug("{}: Call Atlas updateClassificationDefByGuid with guid {} entity {}", methodName, typeDefGUID, atlasClassificationDef);
                            retrievedAtlasTypeDef = typeDefStore.updateClassificationDefByGuid(typeDefGUID, atlasClassificationDef);
                            if (retrievedAtlasTypeDef == null)
                                throw new AtlasBaseException();
                        }
                        break;

                    case UNKNOWN_DEF:
                        LOG.error("{}: cannot convert and update an OM TypeDef with category {}", methodName, category);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(typeName, typeDefGUID, typeDefParameterName, methodName, repositoryName, typeDefPatch.toString());

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }
            } catch (AtlasBaseException e) {

                LOG.error("{}: exception from store and retrieve AtlasTypesDef {}", methodName, e);

                // I'm not sure how to differentiate between the different causes of AtlasBaseException
                // e.g. if there were an authorization exception it is not clear how to tell that
                // in order to throw the correct type of exception. I think that for now if we get an
                // AtlasBaseException we will always throw RepositoryErrorException
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("TypeDef", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            /*
             * Convert the retrieved Atlas Type into an OM TypeDef to be returned
             */
            TypeDef returnableTypeDef = null;
            if (retrievedAtlasTypeDef != null) {
                fatalError = false;
                TypeCategory category = retrievedAtlasTypeDef.getCategory();
                switch (category) {

                    case ENTITY:
                        AtlasEntityDef retrievedAtlasEntityDef = (AtlasEntityDef) retrievedAtlasTypeDef;
                        EntityDef returnableEntityDef;
                        try {
                            AtlasEntityDefMapper atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, retrievedAtlasEntityDef);
                            returnableEntityDef = atlasEntityDefMapper.toOMEntityDef();
                            returnableTypeDef = returnableEntityDef;
                        } catch (Exception e) {
                            fatalError = true;
                        }
                        break;

                    case RELATIONSHIP:
                        AtlasRelationshipDef retrievedAtlasRelationshipDef = (AtlasRelationshipDef) retrievedAtlasTypeDef;
                        RelationshipDef returnableRelationshipDef;
                        try {
                            AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, retrievedAtlasRelationshipDef);
                            returnableRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
                            returnableTypeDef = returnableRelationshipDef;
                        } catch (Exception e) {
                            fatalError = true;
                        }
                        break;

                    case CLASSIFICATION:
                        AtlasClassificationDef retrievedAtlasClassificationDef = (AtlasClassificationDef) retrievedAtlasTypeDef;
                        ClassificationDef returnableClassificationDef;
                        try {
                            AtlasClassificationDefMapper atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, retrievedAtlasClassificationDef);
                            returnableClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();
                            returnableTypeDef = returnableClassificationDef;
                        } catch (Exception e) {
                            fatalError = true;
                        }
                        break;

                    default:
                        LOG.debug("{}: cannot convert an OM TypeDef with category {}", methodName, category);
                        fatalError = true;
                }
            }

            if (fatalError || returnableTypeDef == null) {

                LOG.error("{}: could not initialise mapper or convert retrieved AtlasBaseTypeDef {} to OM TypeDef", methodName, typeName);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("TypeDef", methodName, metadataCollectionId);
                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            // Finally return the retrieved, updated TypeDef
            LOG.debug("{}: Updated OM TypeDef {}", methodName, returnableTypeDef);
            return returnableTypeDef;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Delete the TypeDef.  This is only possible if the TypeDef has never been used to create instances or any
     * instances of this TypeDef have been purged from the metadata collection.
     *
     * @param userId              - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     * @throws InvalidParameterException  - the one of TypeDef identifiers is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested TypeDef is not found in the metadata collection.
     * @throws TypeDefInUseException      - the TypeDef can not be deleted because there are instances of this type in the
     *                                    the metadata collection.  These instances need to be purged before the
     *                                    TypeDef can be deleted.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteTypeDef(String userId,
                              String obsoleteTypeDefGUID,
                              String obsoleteTypeDefName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            TypeDefInUseException,
            UserNotAuthorizedException
    {

        final String methodName = "deleteTypeDef";
        final String guidParameterName = "obsoleteTypeDefGUID";
        final String nameParameterName = "obsoleteTypeDefName";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    obsoleteTypeDefGUID,
                    obsoleteTypeDefName,
                    methodName);

            /*
             * Perform operation
             */

            TypeDef omTypeDefToDelete = null;
            try {
                /*
                 * Find the obsolete type def so that we know its category
                 *
                 */
                omTypeDefToDelete = _getTypeDefByGUID(userId, obsoleteTypeDefGUID);

                if (omTypeDefToDelete == null) {
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(guidParameterName,
                            methodName,
                            repositoryName);

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                TypeDefCategory typeDefCategory = omTypeDefToDelete.getCategory();


                /*
                 * Construct an AtlasTypesDef containing an Atlas converted copy of the OM TypeDef to be deleted then call the TypeDefStore
                 *
                 */
                AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
                switch (typeDefCategory) {
                    case CLASSIFICATION_DEF:
                        ClassificationDef classificationDefToDelete = (ClassificationDef) omTypeDefToDelete;
                        AtlasClassificationDef atlasClassificationDef = convertOMClassificationDefToAtlasClassificationDef(classificationDefToDelete);
                        List<AtlasClassificationDef> atlasClassificationDefs = atlasTypesDef.getClassificationDefs();
                        atlasClassificationDefs.add(atlasClassificationDef);
                        break;

                    case RELATIONSHIP_DEF:
                        RelationshipDef relationshipDefToDelete = (RelationshipDef) omTypeDefToDelete;
                        AtlasRelationshipDef atlasRelationshipDef = convertOMRelationshipDefToAtlasRelationshipDef(relationshipDefToDelete);
                        List<AtlasRelationshipDef> atlasRelationshipDefs = atlasTypesDef.getRelationshipDefs();
                        atlasRelationshipDefs.add(atlasRelationshipDef);
                        break;

                    case ENTITY_DEF:
                        EntityDef entityDefToDelete = (EntityDef) omTypeDefToDelete;
                        AtlasEntityDef atlasEntityDef = convertOMEntityDefToAtlasEntityDef(entityDefToDelete);
                        List<AtlasEntityDef> atlasEntityDefs = atlasTypesDef.getEntityDefs();
                        atlasEntityDefs.add(atlasEntityDef);
                        break;

                    default:
                        LOG.error("{}: invalid typedef category {}", methodName, typeDefCategory);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, omTypeDefToDelete.toString());

                        throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                }

                /*
                 * Ask Atlas to perform the delete operation
                 */
                typeDefStore.deleteTypesDef(atlasTypesDef);

            } catch (TypeErrorException e) {
                LOG.error("{}: The retrieved TypeDef could not be converted to Atlas format", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, omTypeDefToDelete.toString());

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_HAS_REFERENCES) {

                    LOG.error("{}: The Atlas repository could not delete the TypeDef", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_IN_USE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, repositoryName);

                    throw new TypeDefInUseException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                } else {

                    LOG.error("{}: The Atlas repository could not delete the TypeDef", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, omTypeDefToDelete.toString());

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Delete an AttributeTypeDef.  This is only possible if the AttributeTypeDef has never been used to create
     * instances or any instances of this AttributeTypeDef have been purged from the metadata collection.
     *
     * @param userId              - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     * @throws InvalidParameterException  - the one of AttributeTypeDef identifiers is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws TypeDefInUseException      - the AttributeTypeDef can not be deleted because there are instances of this type in the
     *                                    the metadata collection.  These instances need to be purged before the
     *                                    AttributeTypeDef can be deleted.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteAttributeTypeDef(String userId,
                                       String obsoleteTypeDefGUID,
                                       String obsoleteTypeDefName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            TypeDefInUseException,
            UserNotAuthorizedException
    {


        final String methodName = "deleteAttributeTypeDef";
        final String guidParameterName = "obsoleteTypeDefGUID";
        final String nameParameterName = "obsoleteTypeDefName";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAttributeTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    obsoleteTypeDefGUID,
                    obsoleteTypeDefName,
                    methodName);

            /*
             * Perform operation
             */


            /*
             * Look in the Atlas type registry for the obsolete attribute type def so that we know its category
             */

            AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByGuid(obsoleteTypeDefGUID);

            if (abtd == null) {

                LOG.debug("{}: received null return from Atlas getByName using GUID {}", methodName, obsoleteTypeDefGUID);
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(obsoleteTypeDefGUID, "unknown", "obsoleteTypeDefGUID", "deleteAttributeTypeDef", metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "deleteAttributeTypeDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // From here on we know that atlasPreTypeDef is not null


            // We have an AtlasBaseTypeDef
            TypeCategory atlasTypeCategory = abtd.getCategory();

            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

            try {
                switch (atlasTypeCategory) {

                    case ENUM:
                        AtlasEnumDef atlasEnumDef = (AtlasEnumDef) abtd;
                        List<AtlasEnumDef> atlasEnumDefs = atlasTypesDef.getEnumDefs();
                        atlasEnumDefs.add(atlasEnumDef);
                        break;

                    case PRIMITIVE:
                    case ARRAY:
                    case MAP:
                        LOG.debug("{}: There is nothing to do when the AttributeTypeDef is a primitive or collection", methodName);
                        return;

                    default:
                        LOG.error("{}: unsupported attribute type def category {}", methodName, atlasTypeCategory);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, abtd.toString());

                        throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }

                /*
                 * Ask Atlas to perform the delete operation
                 */
                typeDefStore.deleteTypesDef(atlasTypesDef);

            } catch (AtlasBaseException e) {

                if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_HAS_REFERENCES) {

                    LOG.error("{}: The Atlas repository could not delete the TypeDef", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.ATTRIBUTE_TYPEDEF_IN_USE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, repositoryName);

                    throw new TypeDefInUseException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                } else {

                    LOG.error("{}: The Atlas repository could not delete the AttributeTypeDef", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, abtd.toString());

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }


    }


    /**
     * Change the guid or name of an existing TypeDef to a new value.  This is used if two different
     * TypeDefs are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId              - unique identifier for requesting user.
     * @param originalTypeDefGUID - the original guid of the TypeDef.
     * @param originalTypeDefName - the original name of the TypeDef.
     * @param newTypeDefGUID      - the new identifier for the TypeDef.
     * @param newTypeDefName      - new name for this TypeDef.
     * @return typeDef - new values for this TypeDef, including the new guid/name.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws FunctionNotSupportedException - the metadata repository does not support this function.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public TypeDef reIdentifyTypeDef(String userId,
                                     String originalTypeDefGUID,
                                     String originalTypeDefName,
                                     String newTypeDefGUID,
                                     String newTypeDefName)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName = "reIdentifyTypeDef";
        final String originalGUIDParameterName = "originalTypeDefGUID";
        final String originalNameParameterName = "originalTypeDefName";
        final String newGUIDParameterName = "newTypeDefGUID";
        final String newNameParameterName = "newTypeDefName";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    originalGUIDParameterName,
                    originalNameParameterName,
                    originalTypeDefGUID,
                    originalTypeDefName,
                    methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    newGUIDParameterName,
                    newNameParameterName,
                    newTypeDefGUID,
                    newTypeDefName,
                    methodName);

            /*
             * Perform operation
             */
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Change the guid or name of an existing TypeDef to a new value.  This is used if two different
     * TypeDefs are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param originalAttributeTypeDefGUID - the original guid of the AttributeTypeDef.
     * @param originalAttributeTypeDefName - the original name of the AttributeTypeDef.
     * @param newAttributeTypeDefGUID      - the new identifier for the AttributeTypeDef.
     * @param newAttributeTypeDefName      - new name for this AttributeTypeDef.
     * @return attributeTypeDef - new values for this AttributeTypeDef, including the new guid/name.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public AttributeTypeDef reIdentifyAttributeTypeDef(String userId,
                                                       String originalAttributeTypeDefGUID,
                                                       String originalAttributeTypeDefName,
                                                       String newAttributeTypeDefGUID,
                                                       String newAttributeTypeDefName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName = "reIdentifyAttributeTypeDef";
        final String originalGUIDParameterName = "originalAttributeTypeDefGUID";
        final String originalNameParameterName = "originalAttributeTypeDefName";
        final String newGUIDParameterName = "newAttributeTypeDefGUID";
        final String newNameParameterName = "newAttributeTypeDefName";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    originalGUIDParameterName,
                    originalNameParameterName,
                    originalAttributeTypeDefGUID,
                    originalAttributeTypeDefName,
                    methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    newGUIDParameterName,
                    newNameParameterName,
                    newAttributeTypeDefGUID,
                    newAttributeTypeDefName,
                    methodName);

            /*
             * Perform operation
             */
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    // ==========================================================================================================================

    // Group 3 methods

    /**
     * Returns a boolean indicating if the entity is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the entity.
     * @return entity details if the entity is found in the metadata collection; otherwise return null.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail isEntityKnown(String userId,
                                      String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {


        final String methodName = "isEntityKnown";
        final String guidParameterName = "guid";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Perform operation
             */

            EntityDetail entityDetail;
            try {
                entityDetail = getEntityDetail(userId, guid);

            } catch (EntityNotKnownException e) {
                LOG.debug("{}: caught EntityNotKnownException exception from getEntityDetail - exception swallowed, returning null", methodName);
                return null;
            } catch (RepositoryErrorException e) {
                LOG.error("{}: caught RepositoryErrorException exception from getEntityDetail - rethrowing", methodName, e);
                throw e;
            } catch (UserNotAuthorizedException e) {
                LOG.error("{}: caught UserNotAuthorizedException exception from getEntityDetail - rethrowing", methodName, e);
                throw e;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, guid={}: entityDetail={})", methodName, userId, guid, entityDetail);
            }
            return entityDetail;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }

    public boolean isEntityProxy(String guid)
            throws
            EntityNotKnownException
    {
        // Using the supplied guid look up the entity.
        final String methodName = "isEntityProxy";
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            // Important not to mask this call; must return the entity as the event mapper needs to know if hard/soft
            atlasEntityWithExt = entityStore.getById(guid);
        } catch (AtlasBaseException e) {
            LOG.error("{}: caught exception from to get entity from Atlas repository, guid {}", methodName, guid, e.getMessage());
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();
        LOG.debug("{}: AtlasEntity proxy {}", methodName, atlasEntity.isProxy());
        return (atlasEntity.isProxy() == null ? false : atlasEntity.isProxy());
    }

    public boolean isEntityLocal(String guid)
            throws
            EntityNotKnownException
    {
        // Using the supplied guid look up the entity.
        final String methodName = "isEntityLocal";
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            // Important not to mask this call; must return the entity as the event mapper needs to know if hard/soft
            atlasEntityWithExt = entityStore.getById(guid);
        } catch (AtlasBaseException e) {
            LOG.error("{}: caught exception from to get entity from Atlas repository, guid {}", methodName, guid, e.getMessage());
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();
        LOG.debug("{}: AtlasEntity local {}", methodName, atlasEntity.getHomeId() == null);
        String atlasEntityHomeId = atlasEntity.getHomeId();
        return (atlasEntityHomeId == null || atlasEntityHomeId.equals(metadataCollectionId));
    }

    /**
     * Return the header and classifications for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the entity.
     * @return EntitySummary structure
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntitySummary getEntitySummary(String userId,
                                          String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {


        final String methodName = "getEntitySummary";
        final String guidParameterName = "guid";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Perform operation
             */

            // Using the supplied guid look up the entity.
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
            try {
                atlasEntityWithExt = getAtlasEntityById(guid);
            } catch (AtlasBaseException e) {
                LOG.error("{}: caught exception from to get entity from Atlas repository, guid {}", methodName, guid, e.getMessage());
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


            // Project the AtlasEntity as an EntitySummary

            try {
                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                EntitySummary omEntitySummary = atlasEntityMapper.toEntitySummary();

                // Do not return a DELETED entity
                if (omEntitySummary.getStatus() == InstanceStatus.DELETED) {
                    LOG.debug("{}: entity in repository but has been deleted, will not be returned", methodName);

                    OMRSErrorCode errorCode = OMRSErrorCode.INSTANCE_ALREADY_DELETED;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(methodName, metadataCollectionId, guid);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== {}(userId={}, guid={}: entitySummary={})", methodName, userId, guid, omEntitySummary);
                }
                return omEntitySummary;

            } catch (Exception e) {
                LOG.error("{}: caught exception {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return the header, classifications and properties of a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the entity.
     * @return EntityDetail structure.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail getEntityDetail(String userId,
                                        String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {


        final String methodName = "getEntityDetail";
        final String guidParameterName = "guid";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Perform operation
             */

            return _getEntityDetail(userId, guid, false);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }

    // Deliberately public - needed by Event Mapper
    public EntityDetail _getEntityDetail(String  userId,
                                         String  guid,
                                         boolean includeDeletedEntities)
            throws
            RepositoryErrorException,
            EntityNotKnownException
    {

        final String methodName = "_getEntityDetail";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            // Using the supplied guid look up the entity.

            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
            try {
                atlasEntityWithExt = getAtlasEntityById(guid, includeDeletedEntities);

            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from get entity by guid {}, {}", methodName, guid, e.getMessage());

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            LOG.debug("{}: atlasEntityWithExt is {}", methodName, atlasEntityWithExt);
            AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


            // Project the AtlasEntity as an EntityDetail

            try {

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== {}(userId={}, guid={}: entityDetail={})", methodName, userId, guid, omEntityDetail);
                }
                return omEntityDetail;

            } catch (TypeErrorException | InvalidEntityException e) {
                LOG.error("{}: caught exception from attempt to convert Atlas entity to OM {}, {}", methodName, atlasEntity, e.getMessage());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return a historical versionName of an entity - includes the header, classifications and properties of the entity.
     *
     * @param userId   - unique identifier for requesting user.
     * @param guid     - String unique identifier for the entity.
     * @param asOfTime - the time used to determine which versionName of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException     - the guid or date is null or date is for future time
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws EntityNotKnownException       - the requested entity instance is not known in the metadata collection
     *                                       at the time requested.
     * @throws FunctionNotSupportedException - the repository does not support satOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public EntityDetail getEntityDetail(String userId,
                                        String guid,
                                        Date   asOfTime)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName = "getEntityDetail";
        final String guidParameterName = "guid";
        final String asOfTimeParameter = "asOfTime";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

            /*
             * Perform operation
             */

            if (asOfTime != null) {
                // This method requires a historic query which is not supported
                LOG.debug("{}: Does not support asOfTime historic retrieval", methodName);

                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            } else {
                return getEntityDetail(userId, guid);
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return the relationships for a specific entity.
     *
     * @param userId                  - unique identifier for requesting user.
     * @param entityGUID              - String unique identifier for the entity.
     * @param relationshipTypeGUID    - String GUID of the the type of relationship required (null for all).
     * @param fromRelationshipElement - the starting element number of the relationships to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus    - By default, relationships in all statuses are returned.  However, it is possible
     *                                to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                status values.
     * @param asOfTime                - Requests a historical query of the relationships for the entity.  Null means return the
     *                                present values.
     * @param sequencingProperty      - String name of the property that is to be used to sequence the results.
     *                                Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder         - Enum defining how the results should be ordered.
     * @param pageSize                -- the maximum number of result classifications that can be returned on this request.  Zero means
     *                                unrestricted return results size.
     * @return Relationships list.  Null means no relationships associated with the entity.
     * @throws InvalidParameterException     - a parameter is invalid or null.
     * @throws TypeErrorException            - the type guid passed on the request is not known by the metadata collection.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws EntityNotKnownException       - the requested entity instance is not known in the metadata collection.
     * @throws PagingErrorException          - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<Relationship> getRelationshipsForEntity(String               userId,
                                                        String               entityGUID,
                                                        String               relationshipTypeGUID,
                                                        int                  fromRelationshipElement,
                                                        List<InstanceStatus> limitResultsByStatus,
                                                        Date                 asOfTime,
                                                        String               sequencingProperty,
                                                        SequencingOrder      sequencingOrder,
                                                        int                  pageSize)
            throws
            InvalidParameterException,
            TypeErrorException,
            RepositoryErrorException,
            EntityNotKnownException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {


        final String methodName            = "getRelationshipsForEntity";
        final String guidParameterName     = "entityGUID";
        final String typeGUIDParameterName = "relationshipTypeGUID";
        final String asOfTimeParameter     = "asOfTime";
        final String pageSizeParameter     = "pageSize";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityGUID={}, relationshipTypeGUID={}, fromRelationshipElement={}, limitResultsByStatus={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})", methodName, userId, entityGUID, relationshipTypeGUID, fromRelationshipElement, limitResultsByStatus, asOfTime, sequencingProperty, sequencingOrder, pageSize);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, entityGUID, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

            try {
                this.validateTypeGUID(userId, repositoryName, typeGUIDParameterName, relationshipTypeGUID, methodName);
            } catch (TypeErrorException e) {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", relationshipTypeGUID, typeGUIDParameterName, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Perform operation
             */

            // Historical queries are not yet supported.
            if (asOfTime != null) {
                LOG.error("{}: Request to find relationships as they were at time {} is not supported", methodName, asOfTime);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
            try {

                atlasEntityWithExt = getAtlasEntityById(entityGUID);

            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from attempt to get Atlas entity by GUID {}", methodName, entityGUID, e.getMessage());

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

            List<Relationship> matchingRelationships = null;
            try {
                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                //EntityUniverse entityUniverse = atlasEntityMapper.toEntityUniverse();
                List<Relationship> allRelationships = atlasEntityMapper.getEntityRelationships();
                if (allRelationships != null && !allRelationships.isEmpty()) {
                    for (Relationship r : allRelationships) {
                        /*  If there is no type filtering, or if the relationship type matches then
                         *  consider including it in the result.
                         */
                        if (relationshipTypeGUID == null || r.getType().getTypeDefGUID().equals(relationshipTypeGUID)) {

                            /*  If there is no status filter specified, or if the relationship satisfies the status filter,
                             *  include the relationship in the result, otherwise skip the relationship
                             */
                            if (limitResultsByStatus != null) {
                                // Need to check that the relationship status is in the list of allowed status values
                                InstanceStatus relStatus = r.getStatus();
                                boolean match = false;
                                for (InstanceStatus allowedStatus : limitResultsByStatus) {
                                    if (relStatus == allowedStatus) {
                                        match = true;
                                        break;
                                    }
                                }
                                if (!match) {
                                    continue;  // skip this relationship and process the next, if any remain
                                }
                            }

                            // Need to add the relationship to the result
                            if (matchingRelationships == null)
                                matchingRelationships = new ArrayList<>();
                            matchingRelationships.add(r);
                        }
                    }
                }
            } catch (TypeErrorException | RepositoryErrorException e) {

                // For these exception types, log error and re-throw
                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);
                throw e;

            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, entityGUID={}, relationshipTypeGUID={}, fromRelationshipElement={}, limitResultsByStatus={}, ",
                        "asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}: matchingRelationships={})",
                        methodName, userId, entityGUID, relationshipTypeGUID, fromRelationshipElement, limitResultsByStatus,
                        asOfTime, sequencingProperty, sequencingOrder, pageSize, matchingRelationships);
            }


            return formatRelationshipResults(matchingRelationships, fromRelationshipElement, sequencingProperty, sequencingOrder, pageSize);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }

    /**
     * Return a list of entities that match the supplied properties according to the match criteria.  The results
     * can be returned over many pages.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param entityTypeGUID               - String unique identifier for the entity type of interest (null means any entity type).
     * @param matchProperties              - List of entity properties to match to (null means match on entityTypeGUID only).
     * @param matchCriteria                - Enum defining how the properties should be matched to the entities in the repository.
     * @param fromEntityElement            - the starting element number of the entities to return.
     *                                     This is used when retrieving elements
     *                                     beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus         - By default, entities in all statuses are returned.  However, it is possible
     *                                     to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                     status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime                     - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty           - String name of the entity property that is to be used to sequence the results.
     *                                     Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder              - Enum defining how the results should be ordered.
     * @param pageSize                     - the maximum number of result entities that can be returned on this request.  Zero means
     *                                     unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException     - a parameter is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - the type guid passed on the request is not known by the
     *                                       metadata collection.
     * @throws PropertyErrorException        - the properties specified are not valid for any of the requested types of
     *                                       entity.
     * @throws PagingErrorException          - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> findEntitiesByProperty(String               userId,
                                                     String               entityTypeGUID,
                                                     InstanceProperties   matchProperties,
                                                     MatchCriteria        matchCriteria,
                                                     int                  fromEntityElement,
                                                     List<InstanceStatus> limitResultsByStatus,
                                                     List<String>         limitResultsByClassification,
                                                     Date                 asOfTime,
                                                     String               sequencingProperty,
                                                     SequencingOrder      sequencingOrder,
                                                     int                  pageSize)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName                   = "findEntitiesByProperty";
        final String matchCriteriaParameterName   = "matchCriteria";
        final String matchPropertiesParameterName = "matchProperties";
        final String guidParameterName            = "entityTypeGUID";
        final String asOfTimeParameter            = "asOfTime";
        final String pageSizeParameter            = "pageSize";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})",
                    methodName, userId, entityTypeGUID, matchProperties, matchCriteria, fromEntityElement, limitResultsByStatus, limitResultsByClassification, asOfTime, sequencingProperty, sequencingOrder, pageSize);
        }


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);
            repositoryValidator.validateMatchCriteria(repositoryName,
                    matchCriteriaParameterName,
                    matchPropertiesParameterName,
                    matchCriteria,
                    matchProperties,
                    methodName);

            try {
                this.validateTypeGUID(userId, repositoryName, guidParameterName, entityTypeGUID, methodName);

            } catch (TypeErrorException e) {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, guidParameterName, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Perform operation
             */


            /*
             *  This method behaves as follows:
             *  * The optional entityTypeGUID restricts the find to entities of the given type, or its sub-types.
             *    A null entityTypeGUID means the search will span all types.
             *
             *  * The optional InstanceProperties object contains a list of property filters - each property filter specifies
             *    the type, name and value of a property that a matching entity must possess. Only the named properties are
             *    compared; properties that are not named in the InstanceProperties object are ignored. Where a named property
             *    is of type String, the property will match if the specified value is contained in the value of the named
             *    property - i.e. it does not have be an exact match. For all other types of property - i.e. non-String
             *    properties - the value comparison is an exact match.
             *
             *  * The property filters can be combined using the matchCriteria of ALL or ANY, but not NONE. If null, this will default to ALL
             *
             *  * The results can be narrowed by optionally specifying one or more instance status values - the results
             *    will only contain entities that have one of the specified status values
             *
             *  * The results can be narrowed by optionally specifying one or more classifications - the results will
             *    only contain entities that have all of the specified classifications
             *
             *  All of the above filters are optional - if none of them are supplied - i.e. no entityTypeGUID, matchProperties,
             *  status values or classifications - then ALL entities will be returned. This will perform very badly.
             *
             *
             *  IMPORTANT NOTE
             *  --------------
             *  It is difficult to see how to implement the combination of NONE and string searchCriteria - there is no
             *  NOT operator in DSL nor in SearchWithParameters, and the only solution I can currently think of is a
             *  negated regex which seems unduly complex. Short of performing two searches and subtracting one from the
             *  other - which would not perform well - I am currently stuck for a solution. This might require direct manipulation
             *  of the gremlin - which would make for an untidy solution alongside Atlas's other query methods; or we would
             *  need to introduce negation operators into Atlas, which will take longer to implement but is probably the nicest
             *  option. So for now this method is not supporting matchCriteria NONE.
             *
             */

            if (matchCriteria == MatchCriteria.NONE) {
                LOG.error("{}: Request to find entities using matchCriteria NONE is not supported", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(matchCriteriaParameterName, methodName, metadataCollectionId);

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Historical queries are not supported.
            if (asOfTime != null) {
                LOG.error("{}: Request to find entities as they were at time {} is not supported", methodName, asOfTime);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            List<EntityDetail> returnList;

            /*
             * Any paging/offset is performed in this method - not in the called method. This is so that we apply offset
             * and pageSize to the aggregated search result, including after any filtering for status or classifications.
             */

            returnList = findEntitiesByMatchProperties(userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}): returnList={}",
                        methodName, userId, entityTypeGUID, matchProperties, matchCriteria, fromEntityElement, limitResultsByStatus, limitResultsByClassification, asOfTime, sequencingProperty, sequencingOrder, pageSize, returnList);
            }

            // Apply offset and pageSize on the filtered and aggregated result
            return formatEntityResults(returnList, fromEntityElement, sequencingProperty, sequencingOrder, pageSize);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return a list of entities that have the requested type of classification attached.
     *
     * @param userId                        - unique identifier for requesting user.
     * @param entityTypeGUID                - unique identifier for the type of entity requested.  Null mans any type of entity.
     * @param classificationName            - name of the classification - a null is not valid.
     * @param matchClassificationProperties - list of classification properties used to narrow the search.
     * @param matchCriteria                 - Enum defining how the properties should be matched to the classifications in the repository.
     * @param fromEntityElement             - the starting element number of the entities to return.
     *                                      This is used when retrieving elements
     *                                      beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus          - By default, entities in all statuses are returned.  However, it is possible
     *                                      to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                      status values.
     * @param asOfTime                      - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty            - String name of the entity property that is to be used to sequence the results.
     *                                      Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder               - Enum defining how the results should be ordered.
     * @param pageSize                      - the maximum number of result entities that can be returned on this request.  Zero means
     *                                      unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException     - a parameter is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - the type guid passed on the request is not known by the
     *                                       metadata collection.
     * @throws ClassificationErrorException  - the classification request is not known to the metadata collection.
     * @throws PropertyErrorException        - the properties specified are not valid for the requested type of
     *                                       classification.
     * @throws PagingErrorException          - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> findEntitiesByClassification(String               userId,
                                                           String               entityTypeGUID,
                                                           String               classificationName,
                                                           InstanceProperties   matchClassificationProperties,
                                                           MatchCriteria        matchCriteria,
                                                           int                  fromEntityElement,
                                                           List<InstanceStatus> limitResultsByStatus,
                                                           Date                 asOfTime,
                                                           String               sequencingProperty,
                                                           SequencingOrder      sequencingOrder,
                                                           int                  pageSize)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            ClassificationErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {


        final String methodName                   = "findEntitiesByClassification";
        final String classificationParameterName  = "classificationName";
        final String entityTypeGUIDParameterName  = "entityTypeGUID";
        final String matchCriteriaParameterName   = "matchCriteria";
        final String matchPropertiesParameterName = "matchClassificationProperties";
        final String asOfTimeParameter            = "asOfTime";
        final String pageSizeParameter            = "pageSize";
        final String offsetParameter              = "fromEntityElement";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})",
                    methodName, userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria, fromEntityElement, limitResultsByStatus, asOfTime, sequencingProperty, sequencingOrder, pageSize);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

            try {
                this.validateTypeGUID(userId, repositoryName, entityTypeGUIDParameterName, entityTypeGUID, methodName);
            } catch (TypeErrorException e) {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, entityTypeGUIDParameterName, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validate TypeDef
             */
            if (entityTypeGUID != null) {
                TypeDef entityTypeDef;
                try {
                    entityTypeDef = _getTypeDefByGUID(userId, entityTypeGUID);
                } catch (TypeDefNotKnownException e) {
                    // handle below
                    LOG.error("{}: caught exception from _getTypeDefByGUID {}", methodName, e);
                    entityTypeDef = null;
                }
                if (entityTypeDef == null || entityTypeDef.getCategory() != ENTITY_DEF) {

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, entityTypeGUIDParameterName, methodName, repositoryName, "unknown");

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                repositoryValidator.validateTypeDefForInstance(repositoryName,
                        entityTypeGUIDParameterName,
                        entityTypeDef,
                        methodName);

                repositoryValidator.validateClassification(repositoryName,
                        classificationParameterName,
                        classificationName,
                        entityTypeDef.getName(),
                        methodName);
            } else {
                repositoryValidator.validateClassification(repositoryName,
                        classificationParameterName,
                        classificationName,
                        null,
                        methodName);
            }

            repositoryValidator.validateMatchCriteria(repositoryName,
                    matchCriteriaParameterName,
                    matchPropertiesParameterName,
                    matchCriteria,
                    matchClassificationProperties,
                    methodName);

            /*
             * Perform operation
             */

            // Historical queries are not yet supported.
            if (asOfTime != null) {
                LOG.error("{}: Request to find entities as they were at time {} is not supported", methodName, asOfTime);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            if (classificationName == null) {
                LOG.error("{}: Classification name must not be null; please specify a classification name", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_NAME;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(offsetParameter, methodName, metadataCollectionId);

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            List<EntityDetail> returnList;


            returnList = findEntitiesByClassificationName(userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus);


            // Offset and pageSize are handled in the queries above - more efficient at the server. So do not apply them again here.
            // Hence fromElement is set to 0; pageSize can be left as specified by caller.
            return formatEntityResults(returnList, fromEntityElement, sequencingProperty, sequencingOrder, pageSize);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return a list of entities whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param entityTypeGUID               - GUID of the type of entity to search for. Null means all types will
     *                                     be searched (could be slow so not recommended).
     * @param searchCriteria               - String expression contained in any of the property values within the entities
     *                                     of the supplied type.
     * @param fromEntityElement            - the starting element number of the entities to return.
     *                                     This is used when retrieving elements
     *                                     beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus         - By default, entities in all statuses are returned.  However, it is possible
     *                                     to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                     status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime                     - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty           - String name of the property that is to be used to sequence the results.
     *                                     Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder              - Enum defining how the results should be ordered.
     * @param pageSize                     - the maximum number of result entities that can be returned on this request.  Zero means
     *                                     unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException     - a parameter is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws PagingErrorException          - the paging/sequencing parameters are set up incorrectly.
     * @throws TypeErrorException            - the entityTypeGUID does not relate to a valid type
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> findEntitiesByPropertyValue(String               userId,
                                                          String               entityTypeGUID,
                                                          String               searchCriteria,
                                                          int                  fromEntityElement,
                                                          List<InstanceStatus> limitResultsByStatus,
                                                          List<String>         limitResultsByClassification,
                                                          Date                 asOfTime,
                                                          String               sequencingProperty,
                                                          SequencingOrder      sequencingOrder,
                                                          int                  pageSize)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            PagingErrorException,
            TypeErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {


        final String methodName                  = "findEntitiesByPropertyValue";
        final String searchCriteriaParameterName = "searchCriteria";
        final String asOfTimeParameter           = "asOfTime";
        final String typeGUIDParameter           = "entityTypeGUID";
        final String pageSizeParameter           = "pageSize";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, searchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})",
                    methodName, userId, entityTypeGUID, searchCriteria, fromEntityElement, limitResultsByStatus, limitResultsByClassification, asOfTime, sequencingProperty, sequencingOrder, pageSize);
        }

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

            try {
                this.validateTypeGUID(userId, repositoryName, typeGUIDParameter, entityTypeGUID, methodName);
            } catch (TypeErrorException e) {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, typeGUIDParameter, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            /*
             * Process operation
             */

            // The nature of this find/search is that it will match an entity that has ANY matching property
            // i.e. any string property containing the searchCriteria string. It only operates on string
            // properties (other property types are ignored) and it implicitly uses matchCriteria.ANY.

            // Historical queries are not yet supported.
            if (asOfTime != null) {
                LOG.error("{}: Request to find entities as they were at time {} is not supported", methodName, asOfTime);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /* Any resulting formatting is performed in this method - not the called method - to ensure offset and pageSize
             * are applied after status and classification filtering and type aggregation.
             */

            List<EntityDetail> returnList;

            returnList = findEntitiesBySearchCriteria(userId, entityTypeGUID, searchCriteria, limitResultsByStatus, limitResultsByClassification);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, entityTypeGUID={}, searchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}): returnList={}",
                        methodName, userId, entityTypeGUID, searchCriteria, fromEntityElement, limitResultsByStatus, limitResultsByClassification, asOfTime, sequencingProperty, sequencingOrder, pageSize, returnList);
            }

            // Apply offset and pageSize on filtered and aggregated result set
            return formatEntityResults(returnList, fromEntityElement, sequencingProperty, sequencingOrder, pageSize);


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Returns a boolean indicating if the relationship is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the relationship.
     * @return relationship details if the relationship is found in the metadata collection; otherwise return null.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship isRelationshipKnown(String userId,
                                            String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName        = "isRelationshipKnown";
        final String guidParameterName = "guid";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Process operation
             */

            try {
                return getRelationship(userId, guid);
            } catch (RelationshipNotKnownException e) {
                return null;
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    public boolean isRelationshipLocal(String guid)
            throws
            RelationshipNotKnownException
    {

        final String methodName = "isRelationshipLocal";

        // Using the supplied guid look up the relationship.
        AtlasRelationship atlasRelationship;
        try {
            // Important not to mask this call; must return the entity as the event mapper needs to know if hard/soft
            atlasRelationship = relationshipStore.getById(guid);
        } catch (AtlasBaseException e) {
            LOG.error("{}: caught exception from to get relationship from Atlas repository, guid {}", methodName, guid, e.getMessage());
            OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("{}: AtlasRelationship local {}", methodName, atlasRelationship.getHomeId() == null);
        String atlasRelationshipHomeId = atlasRelationship.getHomeId();
        return (atlasRelationshipHomeId == null || atlasRelationshipHomeId.equals(metadataCollectionId));
    }


    /**
     * Return a requested relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the relationship.
     * @return a relationship structure.
     * @throws InvalidParameterException     - the guid is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the metadata collection does not have a relationship with
     *                                       the requested GUID stored.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship getRelationship(String userId,
                                        String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {
        final String methodName        = "getRelationship";
        final String guidParameterName = "guid";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

            /*
             * Process operation
             */

            return _getRelationship(userId, guid, false);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return a historical versionName of a relationship.
     *
     * @param userId   - unique identifier for requesting user.
     * @param guid     - String unique identifier for the relationship.
     * @param asOfTime - the time used to determine which versionName of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException     - the guid or date is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the metadata collection does not have a relationship with
     *                                       the requested GUID stored.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship getRelationship(String userId,
                                        String guid,
                                        Date   asOfTime)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName        = "getRelationship";
        final String guidParameterName = "guid";
        final String asOfTimeParameter = "asOfTime";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

            /*
             * Perform operation
             */

            // This method requires a historic query which is not supported
            if (asOfTime != null) {
                LOG.error("{}: Does not support asOfTime historic retrieval", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            } else {
                return getRelationship(userId, guid);
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Return a list of relationships that match the requested properties by the matching criteria.   The results
     * can be broken into pages.
     *
     * @param userId                  - unique identifier for requesting user
     * @param relationshipTypeGUID    - unique identifier (guid) for the new relationship's type.
     * @param matchProperties         - list of  properties used to narrow the search.
     * @param matchCriteria           - Enum defining how the properties should be matched to the relationships in the repository.
     * @param fromRelationshipElement - the starting element number of the relationships to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus    - By default, relationships in all statuses are returned.  However, it is possible
     *                                to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                status values.
     * @param asOfTime                - Requests a historical query of the relationships for the entity.  Null means return the
     *                                present values.
     * @param sequencingProperty      - String name of the property that is to be used to sequence the results.
     *                                Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder         - Enum defining how the results should be ordered.
     * @param pageSize                - the maximum number of result relationships that can be returned on this request.  Zero means
     *                                unrestricted return results size.
     * @return a list of relationships.  Null means no matching relationships.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<Relationship> findRelationshipsByProperty(String                userId,
                                                          String                relationshipTypeGUID,
                                                          InstanceProperties    matchProperties,
                                                          MatchCriteria         matchCriteria,
                                                          int                   fromRelationshipElement,
                                                          List<InstanceStatus>  limitResultsByStatus,
                                                          Date                  asOfTime,
                                                          String                sequencingProperty,
                                                          SequencingOrder       sequencingOrder,
                                                          int                   pageSize)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException

    {
        final String methodName                   = "findRelationshipsByProperty";
        final String matchCriteriaParameterName   = "matchCriteria";
        final String matchPropertiesParameterName = "matchProperties";
        final String guidParameterName            = "relationshipTypeGUID";
        final String asOfTimeParameter            = "asOfTime";
        final String pageSizeParameter            = "pageSize";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);
            repositoryValidator.validateMatchCriteria(repositoryName,
                    matchCriteriaParameterName,
                    matchPropertiesParameterName,
                    matchCriteria,
                    matchProperties,
                    methodName);

            try {

                this.validateTypeGUID(userId, repositoryName, guidParameterName, relationshipTypeGUID, methodName);

            } catch (TypeErrorException e) {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", relationshipTypeGUID, guidParameterName, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Perform operation
             */

            // Historical queries are not supported.
            if (asOfTime != null) {
                LOG.error("{}: Request to find entities as they were at time {} is not supported", methodName, asOfTime);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            List<Relationship> returnList;

            /*
             * Any paging/offset is performed in this method - not in the called method. This ensures that we apply offset
             * and pageSize to the search result after after any filtering for status.
             */

            try {

                returnList = findRelationshipsByMatchProperties(userId, relationshipTypeGUID, matchProperties, matchCriteria, limitResultsByStatus);

            } catch (PropertyErrorException | TypeErrorException | RepositoryErrorException e) {

                LOG.error("{}: Exception from attempt to find relationships using match properties, {}", methodName, e.getMessage());

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.RELATIONSHIP_SEARCH_ERROR;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}): returnList={}",
                        methodName, userId, relationshipTypeGUID, matchProperties, matchCriteria, fromRelationshipElement, limitResultsByStatus, asOfTime, sequencingProperty, sequencingOrder, pageSize, returnList);
            }

            // Apply offset and pageSize on the filtered and aggregated result
            return formatRelationshipResults(returnList, fromRelationshipElement, sequencingProperty, sequencingOrder, pageSize);


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }

    /**
     * Return a list of relationships whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId                  - unique identifier for requesting user.
     * @param relationshipTypeGUID    - GUID of the type of entity to search for. Null means all types will
     *                                be searched (could be slow so not recommended).
     * @param searchCriteria          - String expression contained in any of the property values within the entities
     *                                of the supplied type.
     * @param fromRelationshipElement - Element number of the results to skip to when building the results list
     *                                to return.  Zero means begin at the start of the results.  This is used
     *                                to retrieve the results over a number of pages.
     * @param limitResultsByStatus    - By default, relationships in all statuses are returned.  However, it is possible
     *                                to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                status values.
     * @param asOfTime                - Requests a historical query of the relationships for the entity.  Null means return the
     *                                present values.
     * @param sequencingProperty      - String name of the property that is to be used to sequence the results.
     *                                Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder         - Enum defining how the results should be ordered.
     * @param pageSize                - the maximum number of result relationships that can be returned on this request.  Zero means
     *                                unrestricted return results size.
     * @return a list of relationships.  Null means no matching relationships.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws TypeErrorException            - the type guid passed on the request is not known by the metadata collection.
     * @throws PagingErrorException          - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<Relationship> findRelationshipsByPropertyValue(String               userId,
                                                               String               relationshipTypeGUID,
                                                               String               searchCriteria,
                                                               int                  fromRelationshipElement,
                                                               List<InstanceStatus> limitResultsByStatus,
                                                               Date                 asOfTime,
                                                               String               sequencingProperty,
                                                               SequencingOrder      sequencingOrder,
                                                               int                  pageSize)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName        = "findRelationshipsByPropertyValue";
        final String asOfTimeParameter = "asOfTime";
        final String pageSizeParameter = "pageSize";
        final String typeGUIDParameter = "relationshipTypeGUID";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);


            try {

                this.validateTypeGUID(userId, repositoryName, typeGUIDParameter, relationshipTypeGUID, methodName);

            } catch (TypeErrorException e) {

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", relationshipTypeGUID, typeGUIDParameter, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Perform operation
             */

            // Historical queries are not supported.
            if (asOfTime != null) {
                LOG.error("{}: Request to find entities as they were at time {} is not supported", methodName, asOfTime);
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /* Any result formatting is performed in this method - not the called method - to ensure offset and pageSize
             * are applied after any status filtering and type aggregation.
             */

            List<Relationship> returnList;

            returnList = findRelationshipsBySearchCriteria(userId, relationshipTypeGUID, searchCriteria, limitResultsByStatus);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== {}(userId={}, relationshipTypeGUID={}, searchCriteria={}, fromRelationshipElement={}, limitResultsByStatus={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}): returnList={}",
                        methodName, userId, relationshipTypeGUID, searchCriteria, fromRelationshipElement, limitResultsByStatus, asOfTime, sequencingProperty, sequencingOrder, pageSize, returnList);
            }

            // Apply offset and pageSize on filtered and aggregated result set
            return formatRelationshipResults(returnList, fromRelationshipElement, sequencingProperty, sequencingOrder, pageSize);


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return all of the relationships and intermediate entities that connect the startEntity with the endEntity.
     *
     * @param userId               - unique identifier for requesting user.
     * @param startEntityGUID      - The entity that is used to anchor the query.
     * @param endEntityGUID        - the other entity that defines the scope of the query.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime             - Requests a historical query of the relationships for the entity.  Null means return the
     *                             present values.
     * @return InstanceGraph - the sub-graph that represents the returned linked entities and their relationships.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public InstanceGraph getLinkingEntities(String                userId,
                                            String                startEntityGUID,
                                            String                endEntityGUID,
                                            List<InstanceStatus>  limitResultsByStatus,
                                            Date                  asOfTime)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName                   = "getLinkingEntities";
        final String startEntityGUIDParameterName = "startEntityGUID";
        final String endEntityGUIDParameterName   = "entityGUID";
        final String asOfTimeParameter            = "asOfTime";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, startEntityGUIDParameterName, startEntityGUID, methodName);
            repositoryValidator.validateGUID(repositoryName, endEntityGUIDParameterName, endEntityGUID, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

            /*
             * Perform operation
             */

            /*
             * To do this efficiently requires a gremlin traversal - it may be possible to do this in Atlas (internally).
             * To achieve this by other means would be inefficient.
             */

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return the entities and relationships that radiate out from the supplied entity GUID.
     * The results are scoped both the instance type guids and the level.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param entityGUID                   - the starting point of the query.
     * @param entityTypeGUIDs              - list of entity types to include in the query results.  Null means include
     *                                     all entities found, irrespective of their type.
     * @param relationshipTypeGUIDs        - list of relationship types to include in the query results.  Null means include
     *                                     all relationships found, irrespective of their type.
     * @param limitResultsByStatus         - By default, relationships in all statuses are returned.  However, it is possible
     *                                     to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                     status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime                     - Requests a historical query of the relationships for the entity.  Null means return the
     *                                     present values.
     * @param level                        - the number of the relationships out from the starting entity that the query will traverse to
     *                                     gather results.
     * @return InstanceGraph - the sub-graph that represents the returned linked entities and their relationships.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - one or more of the type guids passed on the request is not known by the
     *                                       metadata collection.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public InstanceGraph getEntityNeighborhood(String                userId,
                                               String                entityGUID,
                                               List<String>          entityTypeGUIDs,
                                               List<String>          relationshipTypeGUIDs,
                                               List<InstanceStatus>  limitResultsByStatus,
                                               List<String>          limitResultsByClassification,
                                               Date                  asOfTime,
                                               int                   level)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName                                  = "getEntityNeighborhood";
        final String entityGUIDParameterName                     = "entityGUID";
        final String entityTypeGUIDParameterName                 = "entityTypeGUIDs";
        final String relationshipTypeGUIDParameterName           = "relationshipTypeGUIDs";
        final String limitedResultsByClassificationParameterName = "limitResultsByClassification";
        final String asOfTimeParameter                           = "asOfTime";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

            if (entityTypeGUIDs != null) {
                for (String guid : entityTypeGUIDs) {
                    this.validateTypeGUID(userId, repositoryName, entityTypeGUIDParameterName, guid, methodName);
                }
            }

            if (relationshipTypeGUIDs != null) {
                for (String guid : relationshipTypeGUIDs) {
                    this.validateTypeGUID(userId, repositoryName, relationshipTypeGUIDParameterName, guid, methodName);
                }
            }

            if (limitResultsByClassification != null) {
                for (String classificationName : limitResultsByClassification) {
                    repositoryValidator.validateClassificationName(repositoryName,
                            limitedResultsByClassificationParameterName,
                            classificationName,
                            methodName);
                }
            }

            /*
             * Perform operation
             */

            // This should be implemented by a gremlin traversal internally within Atlas.

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Return the list of entities that are of the types listed in instanceTypes and are connected, either directly or
     * indirectly to the entity identified by startEntityGUID.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param startEntityGUID              - unique identifier of the starting entity.
     * @param instanceTypes                - list of types to search for.  Null means an type.
     * @param fromEntityElement            - starting element for results list.  Used in paging.  Zero means first element.
     * @param limitResultsByStatus         - By default, relationships in all statuses are returned.  However, it is possible
     *                                     to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                     status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime                     - Requests a historical query of the relationships for the entity.  Null means return the
     *                                     present values.
     * @param sequencingProperty           - String name of the property that is to be used to sequence the results.
     *                                     Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder              - Enum defining how the results should be ordered.
     * @param pageSize                     - the maximum number of result entities that can be returned on this request.  Zero means
     *                                     unrestricted return results size.
     * @return list of entities either directly or indirectly connected to the start entity
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - the requested type is not known, or not supported in the metadata repository
     *                                       hosting the metadata collection.
     * @throws PagingErrorException          - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> getRelatedEntities(String                userId,
                                                 String                startEntityGUID,
                                                 List<String>          instanceTypes,
                                                 int                   fromEntityElement,
                                                 List<InstanceStatus>  limitResultsByStatus,
                                                 List<String>          limitResultsByClassification,
                                                 Date                  asOfTime,
                                                 String                sequencingProperty,
                                                 SequencingOrder       sequencingOrder,
                                                 int                   pageSize)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName              = "getRelatedEntities";
        final String entityGUIDParameterName = "startEntityGUID";
        final String instanceTypesParameter  = "instanceTypes";
        final String asOfTimeParameter       = "asOfTime";
        final String pageSizeParameter       = "pageSize";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, startEntityGUID, methodName);
            repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
            repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

            if (instanceTypes != null) {
                for (String guid : instanceTypes) {
                    this.validateTypeGUID(userId, repositoryName, instanceTypesParameter, guid, methodName);
                }
            }

            /*
             * Perform operation
             */

            // This should be implemented by a gremlin traversal within Atlas

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    // ==========================================================================================================================

    // Group 4 methods

    /**
     * Create a new entity and put it in the requested state.  The new entity is returned.
     *
     * @param userId                 - unique identifier for requesting user.
     * @param entityTypeGUID         - unique identifier (guid) for the new entity's type.
     * @param initialProperties      - initial list of properties for the new entity - null means no properties.
     * @param initialClassifications - initial list of classifications for the new entity - null means no classifications.
     * @param initialStatus          - initial status - typically DRAFT, PREPARED or ACTIVE.
     * @return EntityDetail showing the new header plus the requested properties and classifications.  The entity will
     * not have any relationships at this stage.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeErrorException           - the requested type is not known, or not supported in the metadata repository
     *                                      hosting the metadata collection.
     * @throws PropertyErrorException       - one or more of the requested properties are not defined, or have different
     *                                      characteristics in the TypeDef for this entity's type.
     * @throws ClassificationErrorException - one or more of the requested classifications are either not known or
     *                                      not defined for this entity type.
     * @throws StatusNotSupportedException  - the metadata repository hosting the metadata collection does not support
     *                                      the requested status.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail addEntity(String               userId,
                                  String               entityTypeGUID,
                                  InstanceProperties   initialProperties,
                                  List<Classification> initialClassifications,
                                  InstanceStatus       initialStatus)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            ClassificationErrorException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName                   = "addEntity";
        final String entityGUIDParameterName      = "entityTypeGUID";
        final String propertiesParameterName      = "initialProperties";
        final String classificationsParameterName = "initialClassifications";
        final String initialStatusParameterName   = "initialStatus";

        if (LOG.isDebugEnabled())
            LOG.debug("==> {}: userId {} entityTypeGUID {} initialProperties {} initialClassifications {} initialStatus {} ",
                    methodName, userId, entityTypeGUID, initialProperties, initialClassifications, initialStatus);

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeGUID(repositoryName, entityGUIDParameterName, entityTypeGUID, methodName);

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
                if (typeDef != null) {
                    repositoryValidator.validateTypeDefForInstance(repositoryName, entityGUIDParameterName, typeDef, methodName);
                    repositoryValidator.validateClassificationList(repositoryName,
                            classificationsParameterName,
                            initialClassifications,
                            typeDef.getName(),
                            methodName);

                    repositoryValidator.validatePropertiesForType(repositoryName,
                            propertiesParameterName,
                            typeDef,
                            initialProperties,
                            methodName);

                    repositoryValidator.validateInstanceStatus(repositoryName,
                            initialStatusParameterName,
                            initialStatus,
                            typeDef,
                            methodName);
                }
            } catch (TypeDefNotKnownException e) {
                // Swallow and throw TypeErrorException below
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            LOG.debug("{}: typeGUID is {} maps to type with name {}", methodName, entityTypeGUID, typeDef.getName());

            /*
             * Validation complete - ok to create new instance
             */


            // Perform validation checks on type specified by guid.
            // Create an instance of the Entity, setting the createdBy to userId, createdTime to now, etc.
            // Then set the properties and initialStatus.
            // Create the entity in the Atlas repository.
            // Set the classifications

            // Note: There are two obvious ways to implement this method -
            // i) retrieve the typedef from Atlas into OM form, perform logic, construct Atlas objects and send to Atlas
            // ii) retrieve typedef from Atlas and use it directly to instantiate the entity.
            // This method uses the former approach, as it may result in common code with other methods.


            if (typeDef.getCategory() != ENTITY_DEF) {
                LOG.error("{}: Found typedef with guid {} but it is not an EntityDef", methodName, entityTypeGUID);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, entityGUIDParameterName, methodName, repositoryName, "unknown");

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            EntityDef entityDef = (EntityDef) typeDef;

            // Retrieve the classification def for each of the initial classifications, and check they list the entity type as valid.
            if (initialClassifications != null) {
                for (Classification classification : initialClassifications) {
                    // Retrieve the classification def and check it contains the entity type
                    ClassificationDef classificationDef;
                    try {
                        classificationDef = (ClassificationDef) _getTypeDefByName(userId, classification.getName());
                        classification.setType(repositoryHelper.getNewInstanceType(metadataCollectionId, classificationDef));
                    } catch (Exception e) {
                        LOG.error("{}: Could not find classification def with name {}", methodName, classification.getName(), e);

                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(classification.getName(), "unknown", classificationsParameterName, methodName, repositoryName, "unknown");

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }


                    // Check that the entity type or one of its supertypes is listed in the VEDs for the classificationDef

                    try {

                        boolean allowed = false;
                        List<TypeDefLink> validEntityDefs = classificationDef.getValidEntityDefs();
                        if (validEntityDefs == null) {
                            /*
                             * The classification has no restrictions on which entities it can be attached to.
                             */
                            allowed = true;

                        } else {

                            Set<String> entityTypes = new HashSet<>();
                            String entityTypeName = entityDef.getName();
                            entityTypes.add(entityTypeName);
                            TypeDef entityTypeDef = _getTypeDefByName(userId, entityTypeName);

                            while (entityTypeDef.getSuperType() != null) {
                                TypeDefLink superTypeLink = entityTypeDef.getSuperType();
                                String parentName = superTypeLink.getName();
                                entityTypes.add(parentName);
                                entityTypeDef = _getTypeDefByName(userId, parentName);
                            }

                            for (TypeDefLink allowedEntityDefLink : validEntityDefs) {
                                if (allowedEntityDefLink != null) {
                                    String allowedTypeName = allowedEntityDefLink.getName();
                                    if (entityTypes.contains(allowedTypeName)) {
                                        allowed = true;
                                    }
                                }
                            }
                        }

                        if (!allowed) {

                            // Entity is not of a type that is eligible for the classification...reject
                            LOG.error("{}: Classification cannot be applied to entity of type {}", methodName, entityDef.getName());
                            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(repositoryName, classification.getName(), entityDef.getName());

                            throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());

                        }
                    } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                        // Entity type could not be retrieved
                        LOG.error("{}: Classification cannot be applied to entity of type {}", methodName, entityDef.getName());
                        OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(entityDef.getName(), "category-unknown", methodName, repositoryName);

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                }
            }


            // Collate the valid instance properties
            EntityDefMapper entityDefMapper = new EntityDefMapper(this, userId, entityDef);
            ArrayList<String> validInstanceProperties = entityDefMapper.getValidPropertyNames();

            // Create an instance type
            // An OM EntityDef only has (at most) one superType - if it has one, retrieve it and wrap into a list of length one...
            ArrayList<TypeDefLink> listSuperTypes = null;
            if (entityDef.getSuperType() != null) {
                listSuperTypes = new ArrayList<>();
                listSuperTypes.add(entityDef.getSuperType());
            }
            InstanceType instanceType = new InstanceType(entityDef.getCategory(),
                    entityTypeGUID,
                    entityDef.getName(),
                    entityDef.getVersion(),
                    entityDef.getDescription(),
                    entityDef.getDescriptionGUID(),
                    listSuperTypes,
                    entityDef.getValidInstanceStatusList(),
                    validInstanceProperties);

            // Construct an EntityDetail object
            Date now = new Date();
            EntityDetail entityDetail = new EntityDetail();

            // Set fields from InstanceAuditHeader
            entityDetail.setType(instanceType);
            entityDetail.setCreatedBy(userId);
            entityDetail.setCreateTime(now);
            entityDetail.setUpdatedBy(userId);
            entityDetail.setUpdateTime(now);  // This is not the actual update time - it will be updated on retrieval of entity below
            entityDetail.setVersion(1L);
            entityDetail.setStatus(InstanceStatus.ACTIVE);

            // Set fields from InstanceHeader
            entityDetail.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            entityDetail.setMetadataCollectionId(metadataCollectionId);


            // GUID is set after the create by Atlas
            entityDetail.setGUID(null);
            entityDetail.setInstanceURL(null);
            // Set fields from EntitySummary
            // Set the classifications
            entityDetail.setClassifications(initialClassifications);
            // Set fields from EntityDetail
            // Set the properties
            entityDetail.setProperties(initialProperties);


            // Check whether the type name is to be converted for F5.
            String atlasTypeName;
            if (FamousFive.omTypeRequiresSubstitution(typeDef.getName())) {
                atlasTypeName = FamousFive.getAtlasTypeName(typeDef.getName(), null);
            } else {
                atlasTypeName = typeDef.getName();
            }


            // Add the Entity to the AtlasEntityStore...

            /*
             * Construct an AtlasEntity
             * Let the AtlasEntity constructor set the GUID - it will be initialized
             * to nextInternalId so that Atlas will generate a proper GUID
             */
            AtlasEntity atlasEntity = new AtlasEntity();

            atlasEntity.setTypeName(atlasTypeName);

            if (entityDetail.getInstanceProvenanceType() != null) {
                atlasEntity.setProvenanceType(entityDetail.getInstanceProvenanceType().getOrdinal());
            } else {
                atlasEntity.setProvenanceType(InstanceProvenanceType.UNKNOWN.getOrdinal());
            }

            atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
            atlasEntity.setCreatedBy(entityDetail.getCreatedBy());
            atlasEntity.setUpdatedBy(entityDetail.getUpdatedBy());
            atlasEntity.setCreateTime(entityDetail.getCreateTime());
            atlasEntity.setUpdateTime(entityDetail.getUpdateTime());
            atlasEntity.setVersion(entityDetail.getVersion());

            atlasEntity.setHomeId(metadataCollectionId);
            atlasEntity.setIsProxy(Boolean.FALSE);

            // Cannot set classifications yet - need to do that post-create to get the entity GUID

            // Map attributes from OM EntityDetail to AtlasEntity
            InstanceProperties instanceProperties = entityDetail.getProperties();
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);


            /* Properties in Egeria may be optional or mandatory. We would expect all mandatory Egeria properties to be supplied, as checked
             * by the validation above. However, where Egeria types are stored into Atlas and extend Atlas types - as they do in the case of
             * F5 types - we also need to check whether the Atlas types place any stricter constraints on the properties (i.e. Atlas attributes)
             * and if so we need to ensure that they are satisfied - by providing values.
             *
             * There may be mandatory attributes in the type hierarchy - including in the Atlas F5 types.
             * Detect any mandatory attributes and ensure that they are supplied. If an attribute is optional in the Egeria typedef, but inherits
             * a tighter constraint from the Atlas typesystem, synthesize a recognisable value for the attribute.
             * Within the Egeria and Atlas F5 types there is no need to traverse the cross-linked supertype references - proceed vertically and this
             * will ensure that every supertype is visited.
             */

            Map<String, AtlasStructDef.AtlasAttributeDef> allAttrDefs;
            try {
                allAttrDefs = getAllEntityAttributeDefs(atlasTypeName);
            } catch (Exception e) {
                LOG.error("{}: Could not get attribute defs for type {}", methodName, atlasTypeName, e);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_HIERARCHY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasTypeName, methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            // Ensure that the supplied properties satisfy the normalized attributes

            // Initial strategy is as follows:
            // If the properties passed to this method are not adequate to satisfy the mandatory
            InstanceProperties suppliedProperties = entityDetail.getProperties();
            Map<String, Object> supplementaryAttrs = new HashMap<>();
            Iterator<String> attributeNamesIter = allAttrDefs.keySet().iterator();
            while (attributeNamesIter.hasNext()) {
                String key = attributeNamesIter.next();
                AtlasStructDef.AtlasAttributeDef attrDef = allAttrDefs.get(key);
                if (attrDef != null && attrDef.getValuesMinCount() > 0) {
                    // This attribute is mandatory - ensure that a value is supplied
                    if (suppliedProperties == null || suppliedProperties.getPropertyValue(key) == null) {
                        // Synthesize value for property
                        switch (attrDef.getTypeName()) {
                            case "string":
                                supplementaryAttrs.put(key, "OM_UNASSIGNED_ATTRIBUTE:" + atlasTypeName + "." + key);
                                break;
                            default:
                                LOG.error("{}: need to synthesize attribute of type {}", methodName, attrDef.getTypeName());
                        }
                    }
                }
            }
            LOG.debug("{}: For Atlas type {} supplementary attributes contains {}", methodName, atlasTypeName, supplementaryAttrs);

            if (!supplementaryAttrs.isEmpty()) {
                if (atlasAttrs == null) {
                    atlasAttrs = new HashMap<>();
                }
                atlasAttrs.putAll(supplementaryAttrs);
            }
            atlasEntity.setAttributes(atlasAttrs);


            // AtlasEntity should have been fully constructed by this point
            LOG.debug("{}: atlasEntity to create is {}", methodName, atlasEntity);

            String newEntityGuid = null;

            boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
            RequestContext.get().setImportInProgress(true);
            try {

                // Construct an AtlasEntityStream and call the repository.
                // Because we want to retain the value of the isProxy flag and other system attributes, we need to
                // ask Atlas to treat this as an import.

                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWEI = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityWEI, null);
                EntityMutationResponse emr;
                boolean createSuccessful;
                try {
                    // Try to create the entity
                    emr = entityStore.createOrUpdateForImport(eis);

                    // Find the GUID of the new entity
                    if (emr != null) {
                        List<AtlasEntityHeader> atlasEntityHeaders = emr.getCreatedEntities();
                        if (atlasEntityHeaders != null) {
                            if (atlasEntityHeaders.size() == 1) {
                                AtlasEntityHeader aeh = atlasEntityHeaders.get(0);
                                if (aeh != null) {
                                    if (aeh.getStatus() == AtlasEntity.Status.ACTIVE) {
                                        if (aeh.getGuid() != null) {
                                            // Note the newly allocated entity GUID - we will need it for classification and retrieval
                                            newEntityGuid = aeh.getGuid();
                                            createSuccessful = true;
                                            LOG.debug("{}: Created entity with GUID {}", methodName, newEntityGuid);
                                        } else {
                                            LOG.error("{}: Created entity appears to have null GUID", methodName);
                                            createSuccessful = false;
                                        }

                                    } else {
                                        LOG.error("{}: Create entity does not have status ACTIVE", methodName);
                                        createSuccessful = false;
                                    }
                                } else {
                                    LOG.error("{}: First AtlasEntityHeader from Atlas was null", methodName);
                                    createSuccessful = false;
                                }
                            } else {
                                LOG.error("{}: EntityMutationResponse contained incorrect number {} of entity headers", methodName, atlasEntityHeaders.size());
                                createSuccessful = false;
                            }
                        } else {
                            LOG.error("{}: EntityMutationResponse contained no entity headers", methodName);
                            createSuccessful = false;
                        }
                    } else {
                        LOG.error("{}: EntityMutationResponse from Atlas was null", methodName);
                        createSuccessful = false;
                    }

                } catch (AtlasBaseException e) {
                    LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                    // Handle below
                    createSuccessful = false;
                }

                if (!createSuccessful) {

                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEntity.toString(), methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            } finally {
                // restore the previous setting
                RequestContext.get().setImportInProgress(importSetting);
            }



            /*
             * Call Atlas to set classifications..
             *
             *  You can't set the classifications on the AtlasEntity until after it has been
             *  created, as each AtlasClassification needs the entity GUID. So add the classifications post-create.
             *  AtlasEntity needs a List<AtlasClassification> so we need to do some translation
             *
             *  OM Classification has:
             *  String               classificationName
             *  InstanceProperties   classificationProperties
             *  ClassificationOrigin classificationOrigin
             *  String               classificationOriginGUID
             *
             *  AtlasClassification has:
             *  String              entityGuid
             *  boolean             propagate
             *  List<TimeBoundary>  validityPeriods
             *  String              typeName
             *  Map<String, Object> attributes
             */


            if (entityDetail.getClassifications() != null && entityDetail.getClassifications().size() > 0) {
                ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();
                for (Classification omClassification : entityDetail.getClassifications()) {
                    AtlasClassification atlasClassification = new AtlasClassification(omClassification.getName());
                    /*
                     * For this OM classification build an Atlas equivalent...
                     * For now we are always setting propagatable to true and AtlasClassification has propagate=true by default.
                     * Instead this could traverse to the Classification.InstanceType.typeDefGUID and retrieve the ClassificationDef
                     * to find the value of propagatable on the def.
                     */
                    InstanceType classificationType = omClassification.getType();
                    if (classificationType != null) {
                        atlasClassification.setTypeName(classificationType.getTypeDefName());
                    } else {
                        // Failed to add classifications to the entity
                        LOG.error("{}: Stored Atlas entity, guid {}, but could not process classification {} due to lack of InstanceType", methodName, newEntityGuid, omClassification.getName());

                        OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;

                        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                this.getClass().getName(),
                                repositoryName);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    atlasClassification.setEntityGuid(newEntityGuid);

                    /* OM Classification has InstanceProperties of form Map<String, InstancePropertyValue>  instanceProperties
                     */
                    InstanceProperties classificationProperties = omClassification.getProperties();
                    Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                    atlasClassification.setAttributes(atlasClassificationAttrs);

                    atlasClassifications.add(atlasClassification);
                }
                /* We do not need to augment the AtlasEntity we created earlier - we can just use the
                 * atlasClassifications directly with the following repository call...
                 */
                try {

                    entityStore.addClassifications(newEntityGuid, atlasClassifications);

                } catch (AtlasBaseException e) {

                    // Failed to add classifications to the entity
                    LOG.error("{}: Atlas created entity, but could not add classifications to it, guid {}", methodName, newEntityGuid);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;

                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                            this.getClass().getName(),
                            repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }


            // Retrieve the entity and convert it for return (this ensures all system attributes are updated
            // and that the entity properties etc accurately reflect the stored entity )
            AtlasEntity atlasEntityRetrieved;
            AtlasEntity.AtlasEntityWithExtInfo retrievedAtlasEntityWithExtInfo;
            boolean retrievalSuccessful;
            EntityDetail returnEntityDetail = null;
            try {

                retrievedAtlasEntityWithExtInfo = getAtlasEntityById(newEntityGuid);

                if (retrievedAtlasEntityWithExtInfo != null) {

                    // retrievedAtlasEntityWithExtInfo should contain an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                    // Extract the entity
                    atlasEntityRetrieved = retrievedAtlasEntityWithExtInfo.getEntity();

                    if (atlasEntityRetrieved != null) {

                        // Convert AtlasEntity to OM EntityDetail.

                        AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                        returnEntityDetail = atlasEntityMapper.toEntityDetail();

                        retrievalSuccessful = true;

                    } else {
                        LOG.error("{}: retrieval of created entity returned AtlasEntity null", methodName);
                        retrievalSuccessful = false;
                    }
                } else {
                    LOG.error("{}: retrieval of created entity returned AtlasEntityWithExtInfo null", methodName);
                    retrievalSuccessful = false;
                }

            } catch (Exception e) {

                LOG.error("{}: caught exception from AtlasEntityMapper {}", methodName, e.getMessage());
                retrievalSuccessful = false;
            }

            if (!retrievalSuccessful) {

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasEntity.toString(), methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } else {

                if (LOG.isDebugEnabled())
                    LOG.debug("<== {}: entityDetail {} ", methodName, entityDetail);

                return returnEntityDetail;
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Create an entity proxy in the metadata collection.  This is used to store relationships that span metadata
     * repositories.
     *
     * @param userId      - unique identifier for requesting user.
     * @param entityProxy - details of entity to add.
     * @throws InvalidParameterException  - the entity proxy is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void addEntityProxy(String      userId,
                               EntityProxy entityProxy)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {
        final String methodName         = "addEntityProxy";
        final String proxyParameterName = "entityProxy";


        if (LOG.isDebugEnabled())
            LOG.debug("==> {}: userId {} entityProxy {} ", methodName, userId, entityProxy);

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);

            repositoryValidator.validateEntityProxy(repositoryName,
                    metadataCollectionId,
                    proxyParameterName,
                    entityProxy,
                    methodName);

            /*
             * Also validate that the proxy contains a metadataCollectionId (i.e. it is not null)
             */
            if (entityProxy.getMetadataCollectionId() == null) {
                LOG.error("{}: Must contain a non-null metadataCollectionId identifying the home repository", methodName);

                OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_ENTITY_PROXY;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                        metadataCollectionId,
                        proxyParameterName,
                        methodName);

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validation complete
             */

            /*
             * An EntityProxy is stored in Atlas as a normal AtlasEntity but with a reduced
             * level of detail (the other fields of EntityDetail are not available). In order
             * to identify that the created AtlasEntity is a proxy, the isProxy system attribute
             * is set.
             */

            /*
             * Check that the EntityProxy contains everything we expect...
             */
            Date now = new Date();
            entityProxy.setCreatedBy(userId);
            entityProxy.setCreateTime(now);
            entityProxy.setUpdatedBy(userId);
            entityProxy.setUpdateTime(now);
            entityProxy.setVersion(1L);
            entityProxy.setStatus(InstanceStatus.ACTIVE);


            // Add the EntityProxy to the AtlasEntityStore...


            /*
             * Construct an AtlasEntity
             * The AtlasEntity constructor will set the GUID - but overwrite it with the GUID from the proxy
             * Also set the homeId and the isProxy flag.
             */
            AtlasEntity atlasEntity = new AtlasEntity();
            atlasEntity.setIsProxy(true);
            atlasEntity.setGuid(entityProxy.getGUID());
            atlasEntity.setHomeId(entityProxy.getMetadataCollectionId());

            if (entityProxy.getInstanceProvenanceType() != null) {
                atlasEntity.setProvenanceType(entityProxy.getInstanceProvenanceType().getOrdinal());
            } else {
                atlasEntity.setProvenanceType(InstanceProvenanceType.UNKNOWN.getOrdinal());
            }

            atlasEntity.setTypeName(entityProxy.getType().getTypeDefName());

            atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
            atlasEntity.setCreatedBy(entityProxy.getCreatedBy());
            atlasEntity.setUpdatedBy(entityProxy.getUpdatedBy());
            atlasEntity.setCreateTime(entityProxy.getCreateTime());
            atlasEntity.setUpdateTime(entityProxy.getUpdateTime());
            atlasEntity.setVersion(entityProxy.getVersion());

            // Cannot set classifications yet - need to do that post-create to get the entity GUID

            // Map attributes from OM EntityProxy to AtlasEntity
            InstanceProperties instanceProperties = entityProxy.getUniqueProperties();
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
            atlasEntity.setAttributes(atlasAttrs);

            // AtlasEntity should have been fully constructed by this point
            LOG.debug("{}: atlasEntity to create is {}", methodName, atlasEntity);

            // Store the entity proxy
            // Because we want Atlas to accept the pre-assigned GUID (e.g. RID) supplied in the EntityProxy, we
            // need to ask Atlas to do this as if it were an import. Therefore set the RequestContext
            // accordingly and reset it afterwards.

            boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
            RequestContext.get().setImportInProgress(true);
            try {

                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWEI = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityWEI, null);
                EntityMutationResponse emr;
                try {
                    emr = entityStore.createOrUpdateForImport(eis);

                } catch (AtlasBaseException e) {
                    LOG.error("{}: Caught exception trying to create entity proxy, {} ", methodName, e);
                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;
                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                            atlasEntity.toString(), methodName, repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
                LOG.debug("{}: response from entityStore {}", methodName, emr);
            } finally {
                // restore the previous setting
                RequestContext.get().setImportInProgress(importSetting);
            }

            /*
             * Call Atlas to set classifications..
             *
             *  You can't set the classifications on the AtlasEntity until after it has been
             *  created, as each AtlasClassification needs the entity GUID. So add the classifications post-create.
             *  AtlasEntity needs a List<AtlasClassification> so we need to do some translation
             *
             *  OM Classification has:
             *  String               classificationName
             *  InstanceProperties   classificationProperties
             *  ClassificationOrigin classificationOrigin
             *  String               classificationOriginGUID
             *
             *  AtlasClassification has:
             *  String              entityGuid
             *  boolean             propagate
             *  List<TimeBoundary>  validityPeriods
             *  String              typeName
             *  Map<String, Object> attributes
             */


            if (entityProxy.getClassifications() != null && entityProxy.getClassifications().size() > 0) {
                ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();
                for (Classification omClassification : entityProxy.getClassifications()) {
                    AtlasClassification atlasClassification = new AtlasClassification(omClassification.getName());
                    /*
                     * For this OM classification build an Atlas equivalent...
                     * For now we are always setting propagatable to true and AtlasClassification has propagate=true by default.
                     * Instead this could traverse to the Classification.InstanceType.typeDefGUID and retrieve the ClassificationDef
                     * to find the value of propagatable on the def.
                     */
                    atlasClassification.setTypeName(omClassification.getType().getTypeDefName());
                    atlasClassification.setEntityGuid(entityProxy.getGUID());

                    /* OM Classification has InstanceProperties of form Map<String, InstancePropertyValue>  instanceProperties
                     */
                    InstanceProperties classificationProperties = omClassification.getProperties();
                    Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                    atlasClassification.setAttributes(atlasClassificationAttrs);

                    atlasClassifications.add(atlasClassification);
                }
                /* We do not need to augment the AtlasEntity we created earlier - we can just use the
                 * atlasClassifications directly with the following repository call...
                 */
                try {

                    entityStore.addClassifications(entityProxy.getGUID(), atlasClassifications);

                } catch (AtlasBaseException e) {

                    // Failed to add classifications to the entity
                    LOG.error("{}: Atlas created entity, but we could not add classifications to it, guid {}", methodName, entityProxy.getGUID(), e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;

                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                            this.getClass().getName(),
                            repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }


            if (LOG.isDebugEnabled())
                LOG.debug("<== {}: entityProxy {} ", methodName, entityProxy);


        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Update the status for a specific entity.
     *
     * @param userId     - unique identifier for requesting user.
     * @param entityGUID - unique identifier (guid) for the requested entity.
     * @param newStatus  - new InstanceStatus for the entity.
     * @return EntityDetail showing the current entity header, properties and classifications.
     * @throws InvalidParameterException   - one of the parameters is invalid or null.
     * @throws RepositoryErrorException    - there is a problem communicating with the metadata repository where
     *                                     the metadata collection is stored.
     * @throws EntityNotKnownException     - the entity identified by the guid is not found in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException  - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityStatus(String         userId,
                                           String         entityGUID,
                                           InstanceStatus newStatus)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName              = "updateEntityStatus";
        final String entityGUIDParameterName = "entityGUID";
        final String statusParameterName     = "newStatus";

        if (LOG.isDebugEnabled())
            LOG.debug("==> {}: userId {} entityGUID {} newStatus {}", methodName, userId, entityGUID, newStatus);


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            repositoryValidator.validateInstanceType(repositoryName, entity);

            String entityTypeGUID = entity.getType().getTypeDefGUID();

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);

                if (typeDef != null) {
                    repositoryValidator.validateNewStatus(repositoryName, statusParameterName, newStatus, typeDef, methodName);
                }
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validation complete - ok to make changes
             */

            /*
             * The above validation has already retrieved the entity from Atlas, by GUID and
             * converted it to an EntityDetail.
             */

            /*
             * Set the status to the requested value.
             */
            entity.setStatus(newStatus);

            // Increment the version number...
            long currentVersion = entity.getVersion();
            entity.setVersion(currentVersion + 1);

            /*
             * Convert to Atlas and store, retrieve and convert to returnable EntityDetail
             */
            try {

                // Use an import so that system attributes are updated
                boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
                RequestContext.get().setImportInProgress(true);
                try {

                    AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
                    AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                    AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityToUpdate, null);

                    entityStore.createOrUpdate(eis, true);

                    // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                    AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

                    atlasEntWithExt = getAtlasEntityById(entityGUID);

                    if (atlasEntWithExt == null) {
                        LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                        OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                        throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }

                    // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                    // Extract the entity
                    AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

                    // Convert AtlasEntity to OM EntityDetail.
                    EntityDetail returnEntityDetail;

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                    returnEntityDetail = atlasEntityMapper.toEntityDetail();

                    if (LOG.isDebugEnabled())
                        LOG.debug("<== {}: complete entityDetail {}", returnEntityDetail);

                    return returnEntityDetail;
                } finally {
                    // restore the previous setting
                    RequestContext.get().setImportInProgress(importSetting);
                }

            } catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Update selected properties in an entity.
     *
     * @param userId     - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param properties - a list of properties to change.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection
     * @throws PropertyErrorException     - one or more of the requested properties are not defined, or have different
     *                                    characteristics in the TypeDef for this entity's type
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityProperties(String             userId,
                                               String             entityGUID,
                                               InstanceProperties properties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String methodName              = "updateEntityProperties";
        final String entityGUIDParameterName = "entityGUID";
        final String propertiesParameterName = "properties";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            repositoryValidator.validateInstanceType(repositoryName, entity);

            String entityTypeGUID = entity.getType().getTypeDefGUID();

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);

                if (typeDef != null) {
                    repositoryValidator.validateNewPropertiesForType(repositoryName,
                            propertiesParameterName,
                            typeDef,
                            properties,
                            methodName);
                }
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }



            /*
             * Validation complete - ok to make changes
             */

            /*
             * The above validation has already retrieved the entity from Atlas, by GUID and
             * converted it to an EntityDetail.
             */

            LOG.debug("{}: type {} has current entity properties {} ", methodName, typeDef.getName(), entity.getProperties());

            /*
             * Treat the passed properties as the target (resultant) property set.
             * Do not perform any value comparison, just include all the properties that have been passed.
             * If a property is not present it is (currently) ignored.
             * Always update the version.
             */

            entity.setProperties(properties);
            LOG.debug("{}: type {} has desired entity properties {} ", methodName, typeDef.getName(), entity.getProperties());

            // Increment the version number...
            long currentVersion = entity.getVersion();
            entity.setVersion(currentVersion + 1);

            /*
             * Convert to Atlas and store, retrieve and convert to returnable EntityDetail
             */
            try {

                AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
                LOG.debug("{}: atlasEntity to update is {}", methodName, atlasEntity);

                // The above provides an initial atlas entity based on the supplied set of properties - now check Atlas has everything it needs.

                /* Properties in Egeria may be optional or mandatory. We would expect all mandatory Egeria properties to be supplied, as checked
                 * by the validation above. However, where Egeria types are stored into Atlas and extend Atlas types - as they do in the case of
                 * F5 types - we also need to check whether the Atlas types place any stricter constraints on the properties (i.e. Atlas attributes)
                 * and if so we need to ensure that they are satisfied - by providing values.
                 *
                 * There may be mandatory attributes in the type hierarchy - including in the Atlas F5 types.
                 * Detect any mandatory attributes and ensure that they are supplied. If an attribute is optional in the Egeria typedef, but inherits
                 * a tighter constraint from the Atlas typesystem, synthesize a recognisable value for the attribute.
                 * Within the Egeria and Atlas F5 types there is no need to traverse the cross-linked supertype references - proceed vertically and this
                 * will ensure that every supertype is visited.
                 */


                String omTypeName = typeDef.getName();
                String atlasTypeName;
                if (FamousFive.omTypeRequiresSubstitution(typeDef.getName()))
                    atlasTypeName = FamousFive.getAtlasTypeName(omTypeName, null);
                else
                    atlasTypeName = omTypeName;

                Map<String, AtlasStructDef.AtlasAttributeDef> allAttrDefs = getAllEntityAttributeDefs(atlasTypeName);

                // Ensure that the supplied properties satisfy the normalized attributes

                // Initial strategy is as follows:
                // If the properties passed to this method are not adequate to satisfy the mandatory
                Map<String, Object> supplementaryAttrs = new HashMap<>();
                Iterator<String> attributeNamesIter = allAttrDefs.keySet().iterator();
                while (attributeNamesIter.hasNext()) {
                    String key = attributeNamesIter.next();
                    AtlasStructDef.AtlasAttributeDef attrDef = allAttrDefs.get(key);

                    if (properties.getPropertyValue(key) == null) {
                        // If property is null we either want to remove it - by passing the null value to Atlas
                        // or we may need to synthesize it - if the corresponding Atlas attribute is mandatory

                        if (attrDef != null && attrDef.getValuesMinCount() > 0) {
                            // This attribute is mandatory - and no value was supplied, so synthesize value if possible, or throw error
                            switch (attrDef.getTypeName()) {

                                case "string":
                                    supplementaryAttrs.put(key, "OM_UNASSIGNED_ATTRIBUTE:" + atlasTypeName + "." + key);
                                    break;

                                default:
                                    LOG.error("{}: Mandatory attribute {} of type {} not supplied and cannot be synthesized", methodName, key, attrDef.getTypeName());
                                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.MANDATORY_ATTRIBUTE_MISSING;
                                    String errorMessage = errorCode.getErrorMessageId()
                                            + errorCode.getFormattedErrorMessage(key, attrDef.getTypeName(), methodName, repositoryName);

                                    throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                            this.getClass().getName(),
                                            methodName,
                                            errorMessage,
                                            errorCode.getSystemAction(),
                                            errorCode.getUserAction());

                            }
                        }
                    }
                }

                LOG.debug("{}: type {} has supplementary attributes {}", methodName, atlasTypeName, supplementaryAttrs);


                // Map supplied properties to Atlas attributes
                Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(properties);
                // add supplementary attributes...
                if (!supplementaryAttrs.isEmpty()) {
                    if (atlasAttrs == null) {
                        atlasAttrs = new HashMap<>();
                    }
                    atlasAttrs.putAll(supplementaryAttrs);
                }
                atlasEntity.setAttributes(atlasAttrs);

                LOG.debug("{}: update type {} using atlasEntity with properties{} ", methodName, atlasTypeName, atlasEntity.getAttributes());

                // Use an import so that system attributes are updated
                boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
                RequestContext.get().setImportInProgress(true);
                try {

                    // Construct an AtlasEntityWithExtInfo and call the repository
                    AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                    AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityToUpdate, null);
                    // EMR is deliberately ignored - instead the whole entity is read back from Atlas.
                    entityStore.createOrUpdateForImport(eis);

                    // Retrieve the AtlasEntity. Rather than parsing the EMR returned by the store, just get the entity directly
                    AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

                    atlasEntWithExt = getAtlasEntityById(entityGUID);

                    if (atlasEntWithExt == null) {
                        LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                        OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                        throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }


                    // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                    // Extract the entity
                    AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();
                    LOG.debug("{}: returned atlasEntity with properties {}", methodName, atlasEntityRetrieved.getAttributes());

                    // Convert AtlasEntity to OM EntityDetail.
                    EntityDetail returnEntityDetail;

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                    returnEntityDetail = atlasEntityMapper.toEntityDetail();

                    LOG.debug("{}: returned entity mapped back to EntityDetail has properties {}", methodName, returnEntityDetail.getProperties());

                    return returnEntityDetail;
                } finally {
                    // restore the previous setting
                    RequestContext.get().setImportInProgress(importSetting);
                }

            } catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_UPDATE_FAILED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, typeDef.getName(), methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Undo the last update to an entity and return the previous content.
     *
     * @param userId     - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException     - the guid is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws FunctionNotSupportedException - the repository does not support undo.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public EntityDetail undoEntityUpdate(String userId,
                                         String entityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName              = "undoEntityUpdate";
        final String entityGUIDParameterName = "entityGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

            /*
             * Validation complete - ok to restore entity
             */

            // In the Atlas connector the previous update is not persisted.

            // I do not known of a way in Atlas to retrieve an earlier previous version.

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Delete an entity.  The entity is soft deleted.  This means it is still in the graph but it is no longer returned
     * on queries.  All relationships to the entity are also soft-deleted and will no longer be usable.
     * To completely eliminate the entity from the graph requires a call to the purgeEntity() method after the delete call.
     * The restoreEntity() method will switch an entity back to Active status to restore the entity to normal use.
     *
     * @param userId             - unique identifier for requesting user.
     * @param typeDefGUID        - unique identifier of the type of the entity to delete.
     * @param typeDefName        - unique name of the type of the entity to delete.
     * @param obsoleteEntityGUID - String unique identifier (guid) for the entity.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail deleteEntity(String userId,
                                     String typeDefGUID,
                                     String typeDefName,
                                     String obsoleteEntityGUID)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {


        final String methodName               = "deleteEntity";
        final String typeDefGUIDParameterName = "typeDefGUID";
        final String typeDefNameParameterName = "typeDefName";
        final String entityGUIDParameterName  = "obsoleteEntityGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    typeDefGUIDParameterName,
                    typeDefNameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, obsoleteEntityGUID, methodName);

            /*
             * Locate Entity
             */
            EntityDetail entity = getEntityDetail(userId, obsoleteEntityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, obsoleteEntityGUID, entity, methodName);

            repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                    typeDefGUID,
                    typeDefName,
                    entity,
                    methodName);

            repositoryValidator.validateInstanceStatusForDelete(repositoryName, entity, methodName);

            /*
             * Complete the request
             */

            try {

                /*
                 * Read the entity from the store - in preparation for version increment
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

                // Must get the entity - even though it's status should be DELETED.
                atlasEntWithExt = entityStore.getById(obsoleteEntityGUID);

                if (atlasEntWithExt == null) {
                    LOG.error("{}: Could not find entity with guid {} ", methodName, obsoleteEntityGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                // Extract the entity
                AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

                // Bump the version number and update the entity
                if (atlasEntityRetrieved != null) {
                    Long currentVersion = atlasEntityRetrieved.getVersion();
                    atlasEntityRetrieved.setVersion(currentVersion + 1L);

                    // Use an import so that system attributes are updated
                    boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
                    RequestContext.get().setImportInProgress(true);
                    try {

                        // Construct an AtlasEntityWithExtInfo and call the repository
                        AtlasEntity.AtlasEntityWithExtInfo atlasEntityVersionUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntityRetrieved);
                        AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityVersionUpdate, null);

                        entityStore.createOrUpdate(eis, true);
                    } finally {
                        // restore the previous setting
                        RequestContext.get().setImportInProgress(importSetting);
                    }
                }


                /*
                 * Now soft delete the entity so that we build the return object from the committed entity.
                 */
                RequestContext.get().setDeleteType(DeleteType.SOFT);


                entityStore.deleteById(obsoleteEntityGUID);

                // Re-read the entity - this should return the entity as the delete was soft.
                atlasEntWithExt = entityStore.getById(obsoleteEntityGUID);

                if (atlasEntWithExt == null) {
                    LOG.error("{}: Could not find entity with guid {} ", methodName, obsoleteEntityGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                // Extract the entity
                atlasEntityRetrieved = atlasEntWithExt.getEntity();

                // Convert AtlasEntity to OM EntityDetail.
                EntityDetail returnEntityDetail;

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                returnEntityDetail = atlasEntityMapper.toEntityDetail();

                return returnEntityDetail;

            } catch (TypeErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Permanently removes a deleted entity from the metadata collection.  This request can not be undone.
     *
     * @param userId            - unique identifier for requesting user.
     * @param typeDefGUID       - unique identifier of the type of the entity to purge.
     * @param typeDefName       - unique name of the type of the entity to purge.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection
     * @throws EntityNotDeletedException  - the entity is not in DELETED status and so can not be purged
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeEntity(String userId,
                            String typeDefGUID,
                            String typeDefName,
                            String deletedEntityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            EntityNotDeletedException,
            UserNotAuthorizedException
    {

        final String methodName               = "purgeEntity";
        final String typeDefGUIDParameterName = "typeDefGUID";
        final String typeDefNameParameterName = "typeDefName";
        final String entityGUIDParameterName  = "deletedEntityGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    typeDefGUIDParameterName,
                    typeDefNameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, deletedEntityGUID, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = _getEntityDetail(userId, deletedEntityGUID, true);

            repositoryValidator.validateEntityFromStore(repositoryName, deletedEntityGUID, entity, methodName);

            repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                    typeDefGUID,
                    typeDefName,
                    entity,
                    methodName);



            /*
             * Check that the entity has been deleted.
             */
            LOG.debug("{}: Check that entity to be purged is in DELETED state, guid {}", methodName, deletedEntityGUID);
            repositoryValidator.validateEntityIsDeleted(repositoryName, entity, methodName);


            /*
             * Validation is complete - ok to remove the entity and its relationships
             */

            /*
             * Perform a HARD delete.
             */

            LOG.debug("{}: Issue a hard delete request to Atlas, guid {}", methodName, deletedEntityGUID);
            RequestContext.get().setDeleteType(DeleteType.HARD);


            /*
             * Locate/purge any relationships for entity
             */

            try {
                List<Relationship> relationships = this.getRelationshipsForEntity(userId,
                        deletedEntityGUID,
                        null,
                        0,
                        null,
                        null,
                        null,
                        null,
                        0);


                if (relationships != null) {

                    for (Relationship relationship : relationships) {

                        if (relationship != null) {

                            String relationshipGUID = relationship.getGUID();

                            // Purge the relationship
                            try {

                                relationshipStore.deleteById(relationshipGUID);

                            } catch (AtlasBaseException e) {

                                LOG.error("{}: Caught exception from Atlas relationshipStore trying to purge relationship {}", methodName, relationshipGUID, e);

                                // Do not throw an exception here - there is nothing we can constructively do - better to try to continue
                                // with purges of other relationships, if any exist.

                            }
                        }
                    }
                }
            } catch (Throwable e) {

                LOG.error("{}: Caught exception trying to find relationships for entity with GUID {}", methodName, deletedEntityGUID);
                // Do not throw an exception here - there is nothing we can constructively do - better to try to continue
                // with the hard delete of the entity.

            }



            /*
             * Hard delete the entity
             */

            try {

                entityStore.deleteById(deletedEntityGUID);
                LOG.debug("{}: Hard delete request complete", methodName);

            } catch (AtlasBaseException e) {

                // Don't log the exception - an attempt to delete an entity is not necessarily exceptional; tests do it deliberately; users might too.
                LOG.error("{}: Atlas entityStore could not hard delete entity {}", methodName, deletedEntityGUID);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_DELETED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(deletedEntityGUID, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Restore the requested entity to the state it was before it was deleted.
     *
     * @param userId            - unique identifier for requesting user.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetail showing the restored entity header, properties and classifications.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection
     * @throws EntityNotDeletedException  - the entity is currently not in DELETED status and so it can not be restored
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail restoreEntity(String userId,
                                      String deletedEntityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            EntityNotDeletedException,
            UserNotAuthorizedException
    {

        final String methodName              = "restoreEntity";
        final String entityGUIDParameterName = "deletedEntityGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, deletedEntityGUID, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = _getEntityDetail(userId, deletedEntityGUID, true);

            repositoryValidator.validateEntityFromStore(repositoryName, deletedEntityGUID, entity, methodName);

            repositoryValidator.validateEntityIsDeleted(repositoryName, entity, methodName);


            if (entity == null) {

                LOG.error("{}: Could not find entity with guid {} ", methodName, deletedEntityGUID);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(deletedEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validate that the entity supports ACTIVE status
             */


            String entityTypeGUID = entity.getType().getTypeDefGUID();

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);

                if (typeDef != null) {

                    try {
                        repositoryValidator.validateNewStatus(repositoryName, "target status", InstanceStatus.ACTIVE, typeDef, methodName);
                    } catch (StatusNotSupportedException e) {

                        LOG.error("{}: Entity does not support ACTIVE status {}", methodName, e.getMessage());

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.STATUS_NOT_SUPPORTED;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(deletedEntityGUID, "ACTIVE", methodName, repositoryName);

                        throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                }
            } catch (TypeDefNotKnownException e) {
                // Handle below - same as if typeDef were returned null
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            /*
             * Validation complete - ok to make changes
             */

            /*
             * The above validation has already retrieved the entity from Atlas, by GUID and
             * converted it to an EntityDetail.
             */

            /*
             * Set the status to the requested value.
             */
            entity.setStatus(InstanceStatus.ACTIVE);

            // Increment the version number...
            long currentVersion = entity.getVersion();
            entity.setVersion(currentVersion + 1);

            /*
             * Convert to Atlas and store, retrieve and convert to returnable EntityDetail
             */
            try {
                AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);

                // Construct an AtlasEntityWithExtInfo and call the repository
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);

                // Use an import so that system attributes are updated
                boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
                RequestContext.get().setImportInProgress(true);
                try {

                    AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityToUpdate, null);

                    entityStore.createOrUpdate(eis, true);

                    // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                    AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

                    atlasEntWithExt = getAtlasEntityById(deletedEntityGUID);

                    if (atlasEntWithExt == null) {
                        LOG.error("{}: Could not find entity with guid {} ", methodName, deletedEntityGUID);
                        OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(deletedEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                        throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }


                    // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                    // Extract the entity
                    AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

                    // Convert AtlasEntity to OM EntityDetail.
                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                    EntityDetail returnEntityDetail = atlasEntityMapper.toEntityDetail();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{}: entity with GUID {} restored to ACTIVE status", methodName, deletedEntityGUID);
                    }

                    return returnEntityDetail;
                } finally {
                    // restore the previous setting
                    RequestContext.get().setImportInProgress(importSetting);
                }

            } catch (StatusNotSupportedException e) {

                LOG.error("{}: Entity does not support ACTIVE status {}", methodName, e.getMessage());

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.STATUS_NOT_SUPPORTED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(deletedEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(deletedEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(deletedEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Add the requested classification to a specific entity.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param entityGUID               - String unique identifier (guid) for the entity.
     * @param classificationName       - String name for the classification.
     * @param classificationProperties - list of properties to set in the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException      - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is either not known or not valid
     *                                      for the entity.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail classifyEntity(String             userId,
                                       String             entityGUID,
                                       String             classificationName,
                                       InstanceProperties classificationProperties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            ClassificationErrorException,
            UserNotAuthorizedException
    {

        final String methodName                  = "classifyEntity";
        final String entityGUIDParameterName     = "entityGUID";
        final String classificationParameterName = "classificationName";
        final String propertiesParameterName     = "classificationProperties";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            repositoryValidator.validateInstanceType(repositoryName, entity);

            InstanceType entityType = entity.getType();

            repositoryValidator.validateClassification(repositoryName,
                    classificationParameterName,
                    classificationName,
                    entityType.getTypeDefName(),
                    methodName);

            Classification newClassification;
            try {
                repositoryValidator.validateClassificationProperties(repositoryName,
                        classificationName,
                        propertiesParameterName,
                        classificationProperties,
                        methodName);

                /*
                 * Validation complete - build the new classification
                 */
                newClassification = repositoryHelper.getNewClassification(repositoryName,
                        userId,
                        classificationName,
                        entityType.getTypeDefName(),
                        ClassificationOrigin.ASSIGNED,
                        null,
                        classificationProperties);
            } catch (Throwable error) {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(metadataCollectionId, classificationName, entityType.getTypeDefName());

                throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validation complete - ok to update entity
             */


            /*
             * The classificationName parameter is actually the name of the ClassificationDef for the type of classification.
             * Locate the ClassificationDef (by name). Fail if not found.
             * Locate the AtlasEntity (by GUID).  Fail if not found.
             * Project the AtlasEntity to an OM Entity - classification eligibility is tested in terms of OM defs.
             * Check that the ClassificationDef lists the EntityDef as a validEntityDef. Fail if not.
             * Create an AtlasClassification.
             * Update the AtlasEntity with the AtlasClassification.
             * Store in the repo.
             * Retrieve the classified entity from the repo and convert to/return as an EntityDetail showing the new classification.
             */

            LOG.debug("{}: newClassification is {}", methodName, newClassification);

            // Use the classificationName to locate the ClassificationDef.
            // The parameter validation above performs some of this already, but we need to classify the Atlas entity
            // with an Atlas classification
            TypeDef typeDef;
            try {

                typeDef = _getTypeDefByName(userId, classificationName);

            } catch (TypeDefNotKnownException e) {
                // Handle below
                typeDef = null;
            }
            if (typeDef == null || typeDef.getCategory() != CLASSIFICATION_DEF) {
                LOG.error("{}: Could not find a classification def with name {}", methodName, classificationName);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(classificationName, "classificationName", methodName, repositoryName);

                throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            ClassificationDef classificationDef = (ClassificationDef) typeDef;


            // Retrieve the AtlasEntity
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
            try {

                atlasEntWithExt = getAtlasEntityById(entityGUID);

            } catch (AtlasBaseException e) {
                // handle below
                atlasEntWithExt = null;
            }
            if (atlasEntWithExt == null) {
                LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, "entityGUID", methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntity = atlasEntWithExt.getEntity();

            LOG.debug("{}: atlasEntity retrieved with classifications {}", methodName, atlasEntity.getClassifications());

            // Project AtlasEntity as an EntityDetail.
            EntityDetail entityDetail;
            try {
                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                entityDetail = atlasEntityMapper.toEntityDetail();
            } catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, "entityGUID", methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Check that the entity type or one of its supertypes is listed in the VEDs for the classificationDef

            try {

                boolean allowed = false;
                List<TypeDefLink> validEntityDefs = classificationDef.getValidEntityDefs();
                if (validEntityDefs == null) {
                    /*
                     * The classification has no restrictions on which entities it can be attached to.
                     */
                    allowed = true;

                } else {

                    Set<String> entityTypes = new HashSet<>();
                    String entityTypeName = entityType.getTypeDefName();
                    entityTypes.add(entityTypeName);
                    TypeDef entityTypeDef = _getTypeDefByName(userId, entityTypeName);

                    while (entityTypeDef.getSuperType() != null) {
                        TypeDefLink superTypeLink = entityTypeDef.getSuperType();
                        String parentName = superTypeLink.getName();
                        entityTypes.add(parentName);
                        entityTypeDef = _getTypeDefByName(userId, parentName);
                    }

                    for (TypeDefLink allowedEntityDefLink : validEntityDefs) {
                        if (allowedEntityDefLink != null) {
                            String allowedTypeName = allowedEntityDefLink.getName();
                            if (entityTypes.contains(allowedTypeName)) {
                                allowed = true;
                            }
                        }
                    }
                }

                if (!allowed) {

                    // Entity is not of a type that is eligible for the classification...reject
                    LOG.error("{}: Classification cannot be applied to entity of type {}", methodName, entityType.getTypeDefName());
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(repositoryName, classificationName, entityType.getTypeDefName());

                    throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                // Entity type could not be retrieved
                LOG.error("{}: Classification cannot be applied to entity of type {}", methodName, entityType.getTypeDefName());
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityType.getTypeDefName(), "category-unknown", methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Create an AtlasClassification.
            AtlasClassification atlasClassification = new AtlasClassification();
            atlasClassification.setEntityGuid(entityGUID);
            atlasClassification.setTypeName(classificationName);

            // Map classification properties to classification attributes
            if (classificationProperties != null) {
                Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                atlasClassification.setAttributes(atlasClassificationAttrs);
            }

            // Add the AtlasClassification to the AtlasEntity

            try {
                // method accepts a list of entity GUIDS
                List<String> entityGUIDList = new ArrayList<>();
                entityGUIDList.add(entityGUID);
                entityStore.addClassification(entityGUIDList, atlasClassification);

                /* Retrieve the AtlasEntity back from store to pick up any Atlas modifications.
                 * Rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityExtRetrieved;

                atlasEntityExtRetrieved = getAtlasEntityById(entityGUID);

                if (atlasEntityExtRetrieved == null) {
                    LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                // atlasEntityRetrieved contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                // Extract the entity
                AtlasEntity atlasEntityRetrieved = atlasEntityExtRetrieved.getEntity();
                LOG.error("{}: retrieved entity from Atlas {}", methodName, atlasEntityRetrieved);

                // Convert AtlasEntity to OM EntityDetail.
                EntityDetail returnEntityDetail;

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                returnEntityDetail = atlasEntityMapper.toEntityDetail();

                // Return OM EntityDetail which should now have the new classification.
                LOG.error("{}: retrieved entity from Atlas and converted it to OM EntityDetail {}", methodName, returnEntityDetail);
                return returnEntityDetail;

            } catch (Exception e) {
                LOG.error("{}: caught exception {}", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }


    }


    /**
     * Remove a specific classification from an entity.
     *
     * @param userId             - unique identifier for requesting user.
     * @param entityGUID         - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException      - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is not set on the entity.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail declassifyEntity(String userId,
                                         String entityGUID,
                                         String classificationName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            ClassificationErrorException,
            UserNotAuthorizedException
    {

        final String methodName                  = "declassifyEntity";
        final String entityGUIDParameterName     = "entityGUID";
        final String classificationParameterName = "classificationName";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
            repositoryValidator.validateClassificationName(repositoryName,
                    classificationParameterName,
                    classificationName,
                    methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            /*
             * Process the request
             */


            /*
             * The classificationName parameter is actually the name of the ClassificationDef for the type of classification.
             * Locate the ClassificationDef (by name). Fail if not found.
             * Retrieve the AtlasEntity (by GUID).  Fail if not found.
             * Update the AtlasEntity by removing the AtlasClassification.
             * Store in the repo.
             * Retrieve the de-classified entity from the repository.
             * Convert to/return an EntityDetail showing the revised classifications.
             */

            // Retrieve the AtlasEntity
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
            try {

                atlasEntWithExt = getAtlasEntityById(entityGUID);

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                // handle below
                atlasEntWithExt = null;
            }

            if (atlasEntWithExt == null) {

                LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity)
            // Extract the entity
            AtlasEntity atlasEntity = atlasEntWithExt.getEntity();

            LOG.debug("{}: atlasEntity retrieved with classifications {}", methodName, atlasEntity.getClassifications());

            // Remove the AtlasClassification from the AtlasEntity


            AtlasEntity.AtlasEntityWithExtInfo atlasEntityExtRetrieved;

            try {

                entityStore.deleteClassification(entityGUID, classificationName);

                /* Retrieve the AtlasEntity back from store to pick up any Atlas modifications.
                 * Rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                 */

                atlasEntityExtRetrieved = getAtlasEntityById(entityGUID);

            } catch (AtlasBaseException e) {
                if (e.getAtlasErrorCode() == CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY) {
                    LOG.error("{}: Entity {} does not have classification {} ", methodName, entityGUID, classificationName);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                    throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                } else {
                    LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                    // handle below
                    atlasEntityExtRetrieved = null;
                }

            }

            if (atlasEntityExtRetrieved == null) {

                LOG.error("{}: Could not update or find entity with guid {} ", methodName, entityGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntityRetrieved contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntityExtRetrieved.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            try {
                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                returnEntityDetail = atlasEntityMapper.toEntityDetail();
            } catch (TypeErrorException | InvalidEntityException e) {

                LOG.error("{}: Could not convert Atlas entity with guid {} ", methodName, entityGUID, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Return OM EntityDetail which should now have the new classification.
            LOG.debug("{}: om entity {}", methodName, returnEntityDetail);
            return returnEntityDetail;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Update one or more properties in one of an entity's classifications.
     *
     * @param userId             - unique identifier for requesting user.
     * @param entityGUID         - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @param properties         - list of properties for the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException      - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is not attached to the classification.
     * @throws PropertyErrorException       - one or more of the requested properties are not defined, or have different
     *                                      characteristics in the TypeDef for this classification type
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityClassification(String userId,
                                                   String entityGUID,
                                                   String classificationName,
                                                   InstanceProperties properties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            ClassificationErrorException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String methodName                  = "updateEntityClassification";
        final String sourceName                  = metadataCollectionId;
        final String entityGUIDParameterName     = "entityGUID";
        final String classificationParameterName = "classificationName";
        final String propertiesParameterName     = "properties";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
            repositoryValidator.validateClassificationName(repositoryName, classificationParameterName, classificationName, methodName);


            try {
                repositoryValidator.validateClassificationProperties(repositoryName,
                        classificationName,
                        propertiesParameterName,
                        properties,
                        methodName);
            } catch (PropertyErrorException error) {
                throw error;
            } catch (Throwable error) {
                OMRSErrorCode errorCode = OMRSErrorCode.UNKNOWN_CLASSIFICATION;

                throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        error.getMessage(),
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);


            /*
             * Process the request
             */

            /*
             * The classificationName parameter is the name of the ClassificationDef for the type of classification.
             * This has already been validated above.
             * Retrieve the AtlasEntity (by GUID).  Fail if not found.
             * Retrieve the classifications, locate the one to update and update its properties
             * Store in the repo.
             * Retrieve the de-classified entity from the repository.
             * Convert to/return an EntityDetail showing the revised classifications.
             */


            // Retrieve the AtlasEntity
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
            try {

                atlasEntWithExt = getAtlasEntityById(entityGUID);

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                // handle below
                atlasEntWithExt = null;
            }

            if (atlasEntWithExt == null) {

                LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity). Extract the entity
            AtlasEntity atlasEntity = atlasEntWithExt.getEntity();

            LOG.debug("{}: atlasEntity retrieved with classifications {}", methodName, atlasEntity.getClassifications());

            // Project AtlasEntity as an EntityDetail.
            EntityDetail entityDetail;
            try {
                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                entityDetail = atlasEntityMapper.toEntityDetail();
            } catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, sourceName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            // Update the entityDetail's classifications

            // Locate the Classification to update
            List<Classification> preClassifications = entityDetail.getClassifications();
            // Find the classification to remove...
            if (preClassifications == null) {

                LOG.error("{}: Entity with guid {} has no classifications", methodName, entityGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Allow loop to match multiple entries, just in case duplicates exist
            for (Classification preClassification : preClassifications) {
                if (preClassification.getName().equals(classificationName)) {
                    // Found the classification to update
                    LOG.debug("{}: classification {} previously had properties {}", methodName, classificationName, preClassification.getProperties());
                    // Replace all of the classification's properties with the new InstanceProperties
                    preClassification.setProperties(properties);
                    LOG.debug("{}: classification {} now has properties {}", methodName, classificationName, preClassification.getProperties());
                }
            }

            entityDetail.setClassifications(preClassifications);


            // Construct an atlasClassifications list - based on the revised OM classifications above - that we can pass to the Atlas EntityStore.
            // There is no public Atlas method to update the props of one classification, so this code replaces them all.
            ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();

            // For each classification in the entity detail create an Atlas classification and add it to the list...
            for (Classification c : entityDetail.getClassifications()) {
                // Create an AtlasClassification.
                AtlasClassification atlasClassification = new AtlasClassification();
                atlasClassification.setEntityGuid(entityGUID);
                atlasClassification.setTypeName(c.getName());
                // Map classification properties to classification attributes
                if (c.getProperties() != null) {
                    Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(c.getProperties());
                    atlasClassification.setAttributes(atlasClassificationAttrs);
                }
                atlasClassifications.add(atlasClassification);
            }


            LOG.debug("{}: atlasEntity to be updated with classifications {}", methodName, atlasEntity.getClassifications());


            AtlasEntity.AtlasEntityWithExtInfo atlasEntityExtRetrieved;
            try {

                entityStore.updateClassifications(entityGUID, atlasClassifications);

                /* Retrieve the AtlasEntity back from store to pick up any Atlas modifications.
                 * Rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                 */

                atlasEntityExtRetrieved = getAtlasEntityById(entityGUID);
            } catch (AtlasBaseException e) {
                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                // handle below
                atlasEntityExtRetrieved = null;
            }

            if (atlasEntityExtRetrieved == null) {

                LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntityRetrieved contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntityExtRetrieved.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            try {
                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                returnEntityDetail = atlasEntityMapper.toEntityDetail();
            } catch (TypeErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper, entity guid {}, {} ", methodName, entityGUID, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Return OM EntityDetail which should now have the new classification.
            LOG.debug("{}: om entity {}", methodName, returnEntityDetail);
            return returnEntityDetail;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Add a new relationship between two entities to the metadata collection.
     *
     * @param userId               - unique identifier for requesting user.
     * @param relationshipTypeGUID - unique identifier (guid) for the new relationship's type.
     * @param initialProperties    - initial list of properties for the new entity - null means no properties.
     * @param entityOneGUID        - the unique identifier of one of the entities that the relationship is connecting together.
     * @param entityTwoGUID        - the unique identifier of the other entity that the relationship is connecting together.
     * @param initialStatus        - initial status - typically DRAFT, PREPARED or ACTIVE.
     * @return Relationship structure with the new header, requested entities and properties.
     * @throws InvalidParameterException   - one of the parameters is invalid or null.
     * @throws RepositoryErrorException    - there is a problem communicating with the metadata repository where
     *                                     the metadata collection is stored.
     * @throws TypeErrorException          - the requested type is not known, or not supported in the metadata repository
     *                                     hosting the metadata collection.
     * @throws PropertyErrorException      - one or more of the requested properties are not defined, or have different
     *                                     characteristics in the TypeDef for this relationship's type.
     * @throws EntityNotKnownException     - one of the requested entities is not known in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException  - the userId is not permitted to perform this operation.
     */
    public Relationship addRelationship(String             userId,
                                        String             relationshipTypeGUID,
                                        InstanceProperties initialProperties,
                                        String             entityOneGUID,
                                        String             entityTwoGUID,
                                        InstanceStatus     initialStatus)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            EntityNotKnownException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName                 = "addRelationship";
        final String guidParameterName          = "relationshipTypeGUID";
        final String propertiesParameterName    = "initialProperties";
        final String initialStatusParameterName = "initialStatus";
        final String entityOneGUIDParameterName = "entityOneGUID";
        final String entityTwoGUIDParameterName = "entityTwoGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateTypeGUID(repositoryName, guidParameterName, relationshipTypeGUID, methodName);

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);
                if (typeDef != null) {
                    repositoryValidator.validateTypeDefForInstance(repositoryName, guidParameterName, typeDef, methodName);


                    repositoryValidator.validatePropertiesForType(repositoryName,
                            propertiesParameterName,
                            typeDef,
                            initialProperties,
                            methodName);

                    repositoryValidator.validateInstanceStatus(repositoryName,
                            initialStatusParameterName,
                            initialStatus,
                            typeDef,
                            methodName);

                }
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validation complete - ok to create new instance
             */

            //=================================================================================================================
            // Perform validation checks on type specified by guid.
            // Retrieve both entities by guid from the repository
            // Create an instance of the Relationship, setting the createdBy to userId, createdTime to now, etc.
            // Then set the properties and initialStatus.
            // Create the relationship in the Atlas repository.
            //


            RelationshipDef relationshipDef = (RelationshipDef) typeDef;

            // Collate the valid instance properties - no supertypes to traverse for a relationship def
            ArrayList<String> validInstanceProperties = null;
            List<TypeDefAttribute> typeDefAttributes = relationshipDef.getPropertiesDefinition();
            if (typeDefAttributes != null) {
                validInstanceProperties = new ArrayList<>();
                for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                    String attrName = typeDefAttribute.getAttributeName();
                    validInstanceProperties.add(attrName);
                }
            }


            /* Create a Relationship object
             * InstanceAuditHeader fields
             * InstanceType              type = null;
             * String                    createdBy
             * String                    updatedBy
             * Date                      createTime
             * Date                      updateTime
             * Long                      version
             * InstanceStatus            currentStatus
             * InstanceStatus            statusOnDelete
             * Instance Header fields
             * InstanceProvenanceType    instanceProvenanceType
             * String                    metadataCollectionId
             * String                    guid
             * String                    instanceURL
             * Relationship fields
             *   InstanceProperties    relationshipProperties
             *   String                entityOnePropertyName    // Retrieve this from the RelDef.RelEndDef for end1
             *   EntityProxy           entityOneProxy
             *   String                entityTwoPropertyName    // Retrieve this from the RelDef.RelEndDef for end2
             *   EntityProxy           entityTwoProxy
             */

            // An OM RelationshipDef has no superType - so we just set that to null
            InstanceType instanceType = new InstanceType(
                    relationshipDef.getCategory(),
                    relationshipTypeGUID,
                    relationshipDef.getName(),
                    relationshipDef.getVersion(),
                    relationshipDef.getDescription(),
                    relationshipDef.getDescriptionGUID(),
                    null,
                    relationshipDef.getValidInstanceStatusList(),
                    validInstanceProperties);

            // Construct a Relationship object
            Date now = new Date();
            Relationship omRelationship = new Relationship();
            // Set fields from InstanceAuditHeader
            omRelationship.setType(instanceType);
            omRelationship.setCreatedBy(userId);
            omRelationship.setCreateTime(now);  // This will be over-written once the relationship has been created in Atlas
            omRelationship.setUpdatedBy(userId);
            omRelationship.setUpdateTime(now);  // This will be over-written once the relationship has been created in Atlas
            omRelationship.setVersion(1L);
            omRelationship.setStatus(InstanceStatus.ACTIVE);
            // Set fields from InstanceHeader
            omRelationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
            omRelationship.setMetadataCollectionId(metadataCollectionId);
            omRelationship.setGUID(null);                                    // OM copy of GUID will not be set until after the create in Atlas
            omRelationship.setInstanceURL(null);
            // Set fields from Relationship
            omRelationship.setProperties(initialProperties);

            /*
             * Set the relationship ends to refer to the entity proxies
             * For each end, retrieve the entity from the repository and create an EntityProxy object
             * and set that in the relationship
             */


            // Entity 1

            // Retrieve the entity from Atlas
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt1;
            try {
                atlasEntWithExt1 = getAtlasEntityById(entityOneGUID);
            } catch (AtlasBaseException e) {
                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                // Handle below
                atlasEntWithExt1 = null;
            }
            if (atlasEntWithExt1 == null) {
                LOG.error("{}: Could not find entity with guid {} ", methodName, entityOneGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityOneGUID, entityOneGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // Extract the entity
            AtlasEntity atlasEnt1 = atlasEntWithExt1.getEntity();
            LOG.debug("{}: retrieved Atlas entity one {}", methodName, atlasEnt1);

            // Convert the AtlasEntity into an OM EntityProxy
            try {

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEnt1);
                EntityProxy end1Proxy = atlasEntityMapper.toEntityProxy();
                LOG.debug("{}: om entity {}", methodName, end1Proxy);
                omRelationship.setEntityOneProxy(end1Proxy);

            } catch (TypeErrorException | InvalidEntityException e) {

                LOG.error("{}: caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityOneGUID, entityOneGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // Entity 2

            // Retrieve the entity from Atlas
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt2;
            try {
                atlasEntWithExt2 = getAtlasEntityById(entityTwoGUID);
            } catch (AtlasBaseException e) {
                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                // Handle below
                atlasEntWithExt2 = null;
            }
            if (atlasEntWithExt2 == null) {
                LOG.error("{}: Could not find entity with guid {} ", methodName, entityTwoGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTwoGUID, entityTwoGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // Extract the entity
            AtlasEntity atlasEnt2 = atlasEntWithExt2.getEntity();
            LOG.debug("{}: retrieved Atlas entity two {}", methodName, atlasEnt2);

            // Convert the AtlasEntity into an OM EntityProxy
            try {

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEnt2);
                EntityProxy end2Proxy = atlasEntityMapper.toEntityProxy();
                LOG.debug("{}: om entity {}", methodName, end2Proxy);
                omRelationship.setEntityTwoProxy(end2Proxy);

            } catch (Exception e) {

                LOG.error("{}: caught exception {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTwoGUID, entityTwoGUIDParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            // omRelationship should be complete apart from GUID - which is to be assigned by repo.

            // Convert the OM Relationship to an AtlasRelationship. Because this is a new Relationship we want
            // Atlas to generate the GUID, so useExistingGUID must be set to false.

            AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(omRelationship, false, relationshipDef);
            LOG.debug("{}: converted to atlasRelationship {}", methodName, atlasRelationship);

            // Add the Relationship to the AtlasRelationshipStore...
            AtlasRelationship returnedAtlasRelationship;
            try {

                returnedAtlasRelationship = relationshipStore.create(atlasRelationship);

            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from relationship store create method {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            // Verify the returnedAtlasRelationship and fill in any extra detail in omRelationship - e.g. sys attrs

            if (returnedAtlasRelationship.getStatus() != ACTIVE) {

                LOG.error("{}: Atlas created relationship, but status set to {}", methodName, returnedAtlasRelationship.getStatus());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Note the newly allocated relationship GUID
            String newRelationshipGuid = returnedAtlasRelationship.getGuid();
            omRelationship.setGUID(newRelationshipGuid);


            // Set the actual created and updated times - they can differ from the created time because atlas will auto update the modification metadata.
            omRelationship.setCreateTime(returnedAtlasRelationship.getCreateTime());
            omRelationship.setUpdateTime(returnedAtlasRelationship.getUpdateTime());

            LOG.debug("{}: new relationship {}", methodName, omRelationship);

            return omRelationship;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Update the status of a specific relationship.
     *
     * @param userId           - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param newStatus        - new InstanceStatus for the relationship.
     * @return Resulting relationship structure with the new status set.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws StatusNotSupportedException   - the metadata repository hosting the metadata collection does not support
     *                                       the requested status.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship updateRelationshipStatus(String         userId,
                                                 String         relationshipGUID,
                                                 InstanceStatus newStatus)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String methodName          = "updateRelationshipStatus";
        final String guidParameterName   = "relationshipGUID";
        final String statusParameterName = "newStatus";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this.getRelationship(userId, relationshipGUID);

            repositoryValidator.validateInstanceType(repositoryName, relationship);

            String relationshipTypeGUID = relationship.getType().getTypeDefGUID();

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);
                if (typeDef != null) {
                    repositoryValidator.validateNewStatus(repositoryName,
                            statusParameterName,
                            newStatus,
                            typeDef,
                            methodName);
                }
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            /*
             * Validation complete - ok to make changes
             */

            Relationship updatedRelationship = new Relationship(relationship);

            updatedRelationship.setStatus(newStatus);

            updatedRelationship = repositoryHelper.incrementVersion(userId, relationship, updatedRelationship);

            /*
             * Store the updatedRelationship into Atlas
             *
             * To do this, retrieve the AtlasRelationship but do not map it (as we did above) to an OM Relationship.
             * Update the status of the Atlas relationship then write it back to the store
             */
            AtlasRelationship atlasRelationship;
            try {
                atlasRelationship = getAtlasRelationshipById(relationshipGUID);

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            LOG.debug("{}: Read from atlas relationship store; relationship {}", methodName, atlasRelationship);

            // Atlas status field has type Status
            AtlasRelationship.Status atlasStatus;
            switch (newStatus) {
                case ACTIVE:
                    atlasStatus = ACTIVE;
                    break;
                case DELETED:
                    atlasStatus = DELETED;
                    break;
                default:
                    // Atlas can only accept ACTIVE | DELETED
                    LOG.error("{}: Atlas cannot accept status value of {}", methodName, newStatus);

                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(relationshipGUID,
                            methodName,
                            repositoryName);

                    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

            }

            atlasRelationship.setStatus(atlasStatus);

            // Save the relationship to Atlas
            AtlasRelationship returnedAtlasRelationship;
            try {

                returnedAtlasRelationship = relationshipStore.update(atlasRelationship);

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas relationship store update method {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID,
                        methodName,
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Refresh the system attributes of the returned OM Relationship
            updatedRelationship.setUpdatedBy(returnedAtlasRelationship.getUpdatedBy());
            updatedRelationship.setUpdateTime(returnedAtlasRelationship.getUpdateTime());

            return updatedRelationship;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Update the properties of a specific relationship.
     *
     * @param userId           - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param properties       - list of the properties to update.
     * @return Resulting relationship structure with the new properties set.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws PropertyErrorException        - one or more of the requested properties are not defined, or have different
     *                                       characteristics in the TypeDef for this relationship's type.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship updateRelationshipProperties(String             userId,
                                                     String             relationshipGUID,
                                                     InstanceProperties properties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String methodName              = "updateRelationshipProperties";
        final String guidParameterName       = "relationshipGUID";
        final String propertiesParameterName = "properties";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this.getRelationship(userId, relationshipGUID);

            repositoryValidator.validateInstanceType(repositoryName, relationship);

            String relationshipTypeGUID = relationship.getType().getTypeDefGUID();

            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);
                if (typeDef != null) {
                    repositoryValidator.validateNewPropertiesForType(repositoryName,
                            propertiesParameterName,
                            typeDef,
                            properties,
                            methodName);
                }
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validation complete - ok to make changes
             */

            Relationship updatedRelationship = new Relationship(relationship);

            updatedRelationship.setProperties(repositoryHelper.mergeInstanceProperties(repositoryName,
                    relationship.getProperties(),
                    properties));
            updatedRelationship = repositoryHelper.incrementVersion(userId, relationship, updatedRelationship);

            /*
             * Store the updatedRelationship into Atlas
             *
             * To do this, retrieve the AtlasRelationship but do not map it (as we did above) to an OM Relationship.
             * Update the properties of the Atlas relationship then write it back to the store
             */
            AtlasRelationship atlasRelationship;
            try {

                atlasRelationship = getAtlasRelationshipById(relationshipGUID);
                LOG.debug("{}: Read from atlas relationship store; relationship {}", methodName, atlasRelationship);

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // The properties were merged above, retrieve the merged set from the updatedRelationship
            // and use it to replace the Atlas properties
            InstanceProperties mergedProperties = updatedRelationship.getProperties();
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(mergedProperties);
            atlasRelationship.setAttributes(atlasAttrs);

            // Save the relationship to Atlas
            AtlasRelationship returnedAtlasRelationship;
            try {

                returnedAtlasRelationship = relationshipStore.create(atlasRelationship);

            } catch (AtlasBaseException e) {
                LOG.error("{}: Caught exception from Atlas relationship store create method, {}", methodName, e);
                // handle below
                returnedAtlasRelationship = null;
            }

            if (returnedAtlasRelationship == null) {

                LOG.error("{}: Atlas relationship store create method did not return a relationship", methodName);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // Convert the returnedAtlasRelationship to OM for return, instead of updatedRelationship

            Relationship returnRelationship;

            try {
                AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                        this,
                        userId,
                        returnedAtlasRelationship,
                        entityStore);

                returnRelationship = atlasRelationshipMapper.toOMRelationship();
                LOG.debug("{}: om relationship {}", methodName, returnRelationship);

            } catch (Exception e) {
                LOG.debug("{}: caught exception from mapper ", methodName, e.getMessage());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guidParameterName,
                        methodName,
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            return returnRelationship;

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Undo the latest change to a relationship (either a change of properties or status).
     *
     * @param userId           - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @return Relationship structure with the new current header, requested entities and properties.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship undoRelationshipUpdate(String userId,
                                               String relationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName        = "undoRelationshipUpdate";
        final String guidParameterName = "relationshipGUID";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

            // I do not known of a way in Atlas to retrieve an earlier previous version.

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Delete a specific relationship.  This is a soft-delete which means the relationship's status is updated to
     * DELETED and it is no longer available for queries.  To remove the relationship permanently from the
     * metadata collection, use purgeRelationship().
     *
     * @param userId                   - unique identifier for requesting user.
     * @param typeDefGUID              - unique identifier of the type of the relationship to delete.
     * @param typeDefName              - unique name of the type of the relationship to delete.
     * @param obsoleteRelationshipGUID - String unique identifier (guid) for the relationship.
     * @throws InvalidParameterException     - one of the parameters is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship deleteRelationship(String userId,
                                           String typeDefGUID,
                                           String typeDefName,
                                           String obsoleteRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName                = "deleteRelationship";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "obsoleteRelationshipGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, obsoleteRelationshipGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this.getRelationship(userId, obsoleteRelationshipGUID);

            repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                    typeDefGUID,
                    typeDefName,
                    relationship,
                    methodName);


            /*
             * Process the request
             */

            /* Perform a SOFT delete.
             * The method returns the deleted Relationship, which should reflect the change in state and
             * system attributes.
             */

            RequestContext.get().setDeleteType(DeleteType.SOFT);

            try {

                /*
                 * Read the relationship back from the store - then update the version number
                 * and delete it. Finally read the relationship back to refresh the
                 * values of system attributes.
                 */
                AtlasRelationship atlasRelationship;

                /*
                 * Retrieve the AtlasRelationship
                 */

                atlasRelationship = relationshipStore.getById(obsoleteRelationshipGUID);

                if (atlasRelationship == null) {
                    LOG.error("{}: Could not find relationship with guid {} ", methodName, obsoleteRelationshipGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteRelationshipGUID, relationshipParameterName, methodName, repositoryName);

                    throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                /*
                 * Update the version number and delete the AtlasRelationship
                 */

                Long currentVersion = atlasRelationship.getVersion();
                atlasRelationship.setVersion(currentVersion + 1L);

                relationshipStore.update(atlasRelationship);

                relationshipStore.deleteById(obsoleteRelationshipGUID);

                /*
                 * Re-read the relationship
                 * This should get the relationship but it's status should be DELETED.
                 */

                atlasRelationship = relationshipStore.getById(obsoleteRelationshipGUID);

                if (atlasRelationship == null) {
                    LOG.error("{}: Could not find relationship with guid {} ", methodName, obsoleteRelationshipGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteRelationshipGUID, relationshipParameterName, methodName, repositoryName);

                    throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                /*
                 * Convert AtlasRelationship to OM Relationship
                 */
                AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(this, userId, atlasRelationship, entityStore);
                Relationship returnRelationship = atlasRelationshipMapper.toOMRelationship();

                LOG.debug("{}: deleted relationship {}", methodName, returnRelationship);

                return returnRelationship;


            } catch (TypeErrorException | AtlasBaseException | InvalidEntityException | InvalidRelationshipException | EntityNotKnownException e) {

                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Permanently delete the relationship from the repository.  There is no means to undo this request.
     *
     * @param userId                  - unique identifier for requesting user.
     * @param typeDefGUID             - unique identifier of the type of the relationship to purge.
     * @param typeDefName             - unique name of the type of the relationship to purge.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @throws InvalidParameterException       - one of the parameters is null.
     * @throws RepositoryErrorException        - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws RelationshipNotKnownException   - the requested relationship is not known in the metadata collection.
     * @throws RelationshipNotDeletedException - the requested relationship is not in DELETED status.
     * @throws UserNotAuthorizedException      - the userId is not permitted to perform this operation.
     */
    public void purgeRelationship(String userId,
                                  String typeDefGUID,
                                  String typeDefName,
                                  String deletedRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            RelationshipNotDeletedException,
            UserNotAuthorizedException
    {
        final String methodName                = "purgeRelationship";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "deletedRelationshipGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, deletedRelationshipGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName, guidParameterName, nameParameterName, typeDefGUID, typeDefName, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this._getRelationship(userId, deletedRelationshipGUID, true);

            repositoryValidator.validateTypeForInstanceDelete(repositoryName, typeDefGUID, typeDefName, relationship, methodName);


            /*
             * Check that the relationship has been deleted.
             */
            LOG.debug("{}: Check that relationship to be purged is in DELETED state", methodName);
            repositoryValidator.validateRelationshipIsDeleted(repositoryName, relationship, methodName);


            /*
             * Validation is complete - ok to remove the entity
             */

            /*
             * Perform a HARD delete.
             */

            RequestContext.get().setDeleteType(DeleteType.HARD);

            try {

                relationshipStore.deleteById(deletedRelationshipGUID);

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas relationshipStore trying to purge relationship {}", methodName, deletedRelationshipGUID, e);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.RELATIONSHIP_NOT_DELETED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(deletedRelationshipGUID, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Restore a deleted relationship into the metadata collection.  The new status will be ACTIVE and the
     * restored details of the relationship are returned to the caller.
     *
     * @param userId                  - unique identifier for requesting user.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @return Relationship structure with the restored header, requested entities and properties.
     * @throws InvalidParameterException       - the guid is null.
     * @throws RepositoryErrorException        - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws RelationshipNotKnownException   - the requested relationship is not known in the metadata collection.
     * @throws RelationshipNotDeletedException - the requested relationship is not in DELETED status.
     * @throws UserNotAuthorizedException      - the userId is not permitted to perform this operation.
     */
    public Relationship restoreRelationship(String userId,
                                            String deletedRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            RelationshipNotDeletedException,
            UserNotAuthorizedException
    {

        final String methodName        = "restoreRelationship";
        final String guidParameterName = "deletedRelationshipGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, guidParameterName, deletedRelationshipGUID, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this._getRelationship(userId, deletedRelationshipGUID, true);

            repositoryValidator.validateRelationshipIsDeleted(repositoryName, relationship, methodName);

            /*
             * Validation is complete.  It is ok to restore the relationship.
             */

            if (relationship == null) {
                LOG.error("{}: Could not find relationship with guid {}", methodName, deletedRelationshipGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(deletedRelationshipGUID,
                        methodName,
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /*
             * Validate that the relationship supports ACTIVE status
             */
            TypeDef typeDef;

            String relationshipTypeGUID = relationship.getType().getTypeDefGUID();

            try {
                typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);

                if (typeDef != null) {

                    try {
                        repositoryValidator.validateNewStatus(repositoryName, "target status", InstanceStatus.ACTIVE, typeDef, methodName);
                    } catch (StatusNotSupportedException e) {

                        LOG.error("{}: Relationship does not support ACTIVE status {}", methodName, e.getMessage());

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.STATUS_NOT_SUPPORTED;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(deletedRelationshipGUID, "ACTIVE", methodName, repositoryName);

                        throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                }
            } catch (TypeDefNotKnownException e) {
                // Handle below - typeDef is null
                typeDef = null;
            }

            if (typeDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            RelationshipDef relationshipDef = (RelationshipDef) typeDef;

            /*
             * Validation complete - ok to make changes
             */

            /*
             * The above validation has already retrieved the relationship from Atlas, by GUID and
             * converted it to a Relationship.
             */

            /*
             * Set the status to the requested value.
             */
            relationship.setStatus(InstanceStatus.ACTIVE);

            // Increment the version number...
            long currentVersion = relationship.getVersion();
            relationship.setVersion(currentVersion + 1);

            /*
             * Convert to AtlasRelationship, store, retrieve and convert to returnable EntityDetail
             */

            try {
                AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(relationship, true, relationshipDef);

                try {

                    // Ignore the returned relationship - separately get post-commit
                    relationshipStore.update(atlasRelationship);

                } catch (AtlasBaseException e) {

                    LOG.error("{}: Caught exception from Atlas relationship store update method {}", methodName, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(deletedRelationshipGUID,
                            methodName,
                            repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                // Retrieve the relationship from the store....

                AtlasRelationship retrievedAtlasRelationship;
                try {
                    retrievedAtlasRelationship = getAtlasRelationshipById(deletedRelationshipGUID);

                } catch (AtlasBaseException e) {
                    LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(deletedRelationshipGUID, methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                LOG.debug("{}: Read from atlas relationship store; relationship {}", methodName, retrievedAtlasRelationship);

                // Convert to OM Relationship and return to caller

                Relationship omRelationship;

                try {

                    AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(this, userId, retrievedAtlasRelationship, entityStore);
                    omRelationship = atlasRelationshipMapper.toOMRelationship();
                    LOG.debug("{}: om relationship {}", methodName, omRelationship);

                } catch (Exception e) {
                    LOG.error("{}: caught exception from mapper ", methodName, e.getMessage());
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(guidParameterName,
                            methodName,
                            repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("{}: returning relationship {}", methodName, omRelationship);
                }

                return omRelationship;

            } catch (StatusNotSupportedException | TypeErrorException e) {
                LOG.debug("{}: could not convert OM Relationship to Atlas ", methodName, e.getMessage());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guidParameterName,
                        methodName,
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    // =========================================================================================================

    // Group 5 methods

    /**
     * Change the guid of an existing entity to a new value.  This is used if two different
     * entities are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId        - unique identifier for requesting user.
     * @param typeDefGUID   - the guid of the TypeDef for the entity - used to verify the entity identity.
     * @param typeDefName   - the name of the TypeDef for the entity - used to verify the entity identity.
     * @param entityGUID    - the existing identifier for the entity.
     * @param newEntityGUID - new unique identifier for the entity.
     * @return entity - new values for this entity, including the new guid.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reIdentifyEntity(String userId,
                                         String typeDefGUID,
                                         String typeDefName,
                                         String entityGUID,
                                         String newEntityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName            = "reIdentifyEntity";
        final String guidParameterName     = "typeDefGUID";
        final String nameParameterName     = "typeDefName";
        final String instanceParameterName = "deletedRelationshipGUID";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            parentConnector.validateRepositoryIsActive(methodName);
            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, instanceParameterName, newEntityGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            /*
             * Validation complete - ok to make changes
             */

            /* In Atlas the GUID is the id of the vertex, so it is not possible to change it in place.
             * It may be possible to implement this method by deleting the existing Atlas entity
             * and creating a new one. If soft deletes are in force then the previous entity would
             * still exist, in soft-deleted state. This may lead to complexity compared to the
             * more desirable situation of having exactly one GUID that relates to the entity.
             */

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Change the type of an existing entity.  Typically this action is taken to move an entity's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type and the properties adjusted.
     *
     * @param userId                - unique identifier for requesting user.
     * @param entityGUID            - the unique identifier for the entity to change.
     * @param currentTypeDefSummary - the current details of the TypeDef for the entity - used to verify the entity identity
     * @param newTypeDefSummary     - details of this entity's new TypeDef.
     * @return entity - new values for this entity, including the new type information.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - the requested type is not known, or not supported in the metadata repository
     *                                    hosting the metadata collection.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.     *
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reTypeEntity(String         userId,
                                     String         entityGUID,
                                     TypeDefSummary currentTypeDefSummary,
                                     TypeDefSummary newTypeDefSummary)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            ClassificationErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException

    {
        final String methodName                  = "reTypeEntity";
        final String entityParameterName         = "entityGUID";
        final String currentTypeDefParameterName = "currentTypeDefSummary";
        final String newTypeDefParameterName     = "newTypeDefSummary";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            repositoryValidator.validateInstanceType(repositoryName,
                    entity,
                    currentTypeDefParameterName,
                    currentTypeDefParameterName,
                    currentTypeDefSummary.getGUID(),
                    currentTypeDefSummary.getName());

            repositoryValidator.validatePropertiesForType(repositoryName,
                    newTypeDefParameterName,
                    newTypeDefSummary,
                    entity.getProperties(),
                    methodName);

            repositoryValidator.validateClassificationList(repositoryName,
                    entityParameterName,
                    entity.getClassifications(),
                    newTypeDefSummary.getName(),
                    methodName);

            /*
             * Validation complete - ok to make changes
             */


            /* In Atlas the entity type is stored in the AtlasEntity typeName field.
             * If you want to update an entity then you essentially pass the modified AtlasEntity
             * to the entity store's updateEntity method - this accepts an AtlasEntity.
             * That in turn will call the entity store's createOrUpdate.
             * This calls the store's preCreateOrUpdate...
             * preCreateOrUpdate calls validateAndNormalizeForUpdate
             *  ... that gets the type using the typeName in the updated entity
             *  ... and then calls validateValueForUpdate
             *  ... and getNormalizedValueForUpdate
             *  ... which validates that the attributes are valid values
             * createOrUpdate then...
             * ... compares the attributes & classifications of the updated entity
             * ... and then asks the EntityGraphMapper to alter attributes and classifications
             * ... this will get the type by referring to the typename in the (updated) entity....
             *
             * All in all I think it is possible to change the entity type
             *
             */

            /*
             * Alter the retrieved EntityDetail so that it's InstanceType reflects the new type
             * Then convert it to an AtlasEntity and call updateEntity.
             */


            // Retrieve the TypeDef for the new type
            String newTypeGUID = newTypeDefSummary.getGUID();
            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, newTypeGUID);
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null || typeDef.getCategory() != ENTITY_DEF) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(newTypeGUID, newTypeDefParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            // Create a new InstanceType
            InstanceType newInstanceType = new InstanceType();
            newInstanceType.setTypeDefName(typeDef.getName());
            newInstanceType.setTypeDefCategory(typeDef.getCategory());
            List<TypeDefLink> superTypes = new ArrayList<>();

            // To set the typeDefSuperTypes, we need to traverse the supertype hierarchy and find all types
            List<TypeDefLink> discoveredSuperTypes = new ArrayList<>();
            String parentTypeName = null;
            TypeDef searchTypeDef = typeDef;
            try {
                while (searchTypeDef.getSuperType() != null) {
                    TypeDefLink superTypeLink = typeDef.getSuperType();
                    discoveredSuperTypes.add(superTypeLink);
                    parentTypeName = superTypeLink.getName();
                    searchTypeDef = _getTypeDefByName(userId, parentTypeName);
                }
            } catch (TypeDefNotKnownException | RepositoryErrorException e) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(parentTypeName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            newInstanceType.setTypeDefSuperTypes(discoveredSuperTypes);

            List<TypeDefAttribute> typeDefAttributes = typeDef.getPropertiesDefinition();
            List<String> validPropertyNames = null;
            if (typeDefAttributes != null) {
                validPropertyNames = new ArrayList<>();
                for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                    validPropertyNames.add(typeDefAttribute.getAttributeName());
                }
            }
            newInstanceType.setValidInstanceProperties(validPropertyNames);
            newInstanceType.setTypeDefGUID(newTypeGUID);
            newInstanceType.setTypeDefVersion(typeDef.getVersion());
            newInstanceType.setValidStatusList(typeDef.getValidInstanceStatusList());
            newInstanceType.setTypeDefDescription(typeDef.getDescription());
            newInstanceType.setTypeDefDescriptionGUID(typeDef.getDescriptionGUID());

            // Set the new instance type into the entity
            entity.setType(newInstanceType);

            // Convert and store the re-typed entity to Atlas
            try {
                AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
                LOG.debug("{}: atlasEntity to update is {}", methodName, atlasEntity);

                // Construct an AtlasEntityWithExtInfo and call the repository
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                AtlasObjectId atlasObjectId = AtlasTypeUtil.getAtlasObjectId(atlasEntity);

                entityStore.updateEntity(atlasObjectId, atlasEntityToUpdate, true);

                // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

                atlasEntWithExt = getAtlasEntityById(entityGUID);

                if (atlasEntWithExt == null) {
                    LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                // Extract the entity
                AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

                // Convert AtlasEntity to OM EntityDetail.
                EntityDetail returnEntityDetail;

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                returnEntityDetail = atlasEntityMapper.toEntityDetail();

                return returnEntityDetail;

            } catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught OMRS exception {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }


    }


    /**
     * Change the home of an existing entity.  This action is taken for example, if the original home repository
     * becomes permanently unavailable, or if the user community updating this entity move to working
     * from a different repository in the open metadata repository cohort.
     *
     * @param userId                      - unique identifier for requesting user.
     * @param entityGUID                  - the unique identifier for the entity to change.
     * @param typeDefGUID                 - the guid of the TypeDef for the entity - used to verify the entity identity.
     * @param typeDefName                 - the name of the TypeDef for the entity - used to verify the entity identity.
     * @param homeMetadataCollectionId    - the existing identifier for this entity's home.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return entity - new values for this entity, including the new home information.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reHomeEntity(String userId,
                                     String entityGUID,
                                     String typeDefGUID,
                                     String typeDefName,
                                     String homeMetadataCollectionId,
                                     String newHomeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName           = "reHomeEntity";
        final String guidParameterName    = "typeDefGUID";
        final String nameParameterName    = "typeDefName";
        final String entityParameterName  = "entityGUID";
        final String homeParameterName    = "homeMetadataCollectionId";
        final String newHomeParameterName = "newHomeMetadataCollectionId";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, newHomeParameterName, newHomeMetadataCollectionId, methodName);

            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);


            /*
             * Validation complete - ok to make changes
             */
            // Set the new instance type into the entity
            entity.setMetadataCollectionId(newHomeMetadataCollectionId);

            // Convert and store the re-typed entity to Atlas
            try {
                AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
                LOG.debug("{}: atlasEntity to update is {}", methodName, atlasEntity);

                // Construct an AtlasEntityWithExtInfo and call the repository
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                AtlasObjectId atlasObjectId = AtlasTypeUtil.getAtlasObjectId(atlasEntity);
                entityStore.updateEntity(atlasObjectId, atlasEntityToUpdate, true);

                // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
                AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

                atlasEntWithExt = getAtlasEntityById(entityGUID);

                if (atlasEntWithExt == null) {
                    LOG.error("{}: Could not find entity with guid {} ", methodName, entityGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
                // Extract the entity
                AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

                // Convert AtlasEntity to OM EntityDetail.
                EntityDetail returnEntityDetail;

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
                returnEntityDetail = atlasEntityMapper.toEntityDetail();

                return returnEntityDetail;

            } catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

                LOG.error("{}: Caught exception from AtlasEntityMapper {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas {}", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Change the guid of an existing relationship.  This is used if two different
     * relationships are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId              - unique identifier for requesting user.
     * @param typeDefGUID         - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName         - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param relationshipGUID    - the existing identifier for the relationship.
     * @param newRelationshipGUID - the new unique identifier for the relationship.
     * @return relationship - new values for this relationship, including the new guid.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                       metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship reIdentifyRelationship(String userId,
                                               String typeDefGUID,
                                               String typeDefName,
                                               String relationshipGUID,
                                               String newRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName                   = "reIdentifyRelationship";
        final String guidParameterName            = "typeDefGUID";
        final String nameParameterName            = "typeDefName";
        final String relationshipParameterName    = "relationshipGUID";
        final String newRelationshipParameterName = "newHomeMetadataCollectionId";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);
            repositoryValidator.validateGUID(repositoryName, newRelationshipParameterName, newRelationshipGUID, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this.getRelationship(userId, relationshipGUID);

            /*
             * Validation complete - ok to make changes
             */

            LOG.debug("{}: relationship is {}", methodName, relationship);

            /* In Atlas the GUID is the id of the vertex, so it is not possible to change it in place.
             * It may be possible to implement this method by deleting the existing Atlas entity
             * and creating a new one. If soft deletes are in force then the previous entity would
             * still exist, in soft-deleted state. This may lead to complexity compared to the
             * more desirable situation of having exactly one GUID that relates to the entity.
             */

            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * Change the type of an existing relationship.  Typically this action is taken to move a relationship's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param userId                - unique identifier for requesting user.
     * @param relationshipGUID      - the unique identifier for the relationship.
     * @param currentTypeDefSummary - the details of the TypeDef for the relationship - used to verify the relationship identity.
     * @param newTypeDefSummary     - details of this relationship's new TypeDef.
     * @return relationship - new values for this relationship, including the new type information.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - the requested type is not known, or not supported in the metadata repository
     *                                       hosting the metadata collection.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                       metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship reTypeRelationship(String         userId,
                                           String         relationshipGUID,
                                           TypeDefSummary currentTypeDefSummary,
                                           TypeDefSummary newTypeDefSummary)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException

    {

        final String methodName                  = "reTypeRelationship";
        final String relationshipParameterName   = "relationshipGUID";
        final String currentTypeDefParameterName = "currentTypeDefSummary";
        final String newTypeDefParameterName     = "newTypeDefSummary";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
            repositoryValidator.validateType(repositoryName, currentTypeDefParameterName, currentTypeDefSummary, TypeDefCategory.RELATIONSHIP_DEF, methodName);
            repositoryValidator.validateType(repositoryName, currentTypeDefParameterName, newTypeDefSummary, TypeDefCategory.RELATIONSHIP_DEF, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this.getRelationship(userId, relationshipGUID);

            repositoryValidator.validateInstanceType(repositoryName,
                    relationship,
                    currentTypeDefParameterName,
                    currentTypeDefParameterName,
                    currentTypeDefSummary.getGUID(),
                    currentTypeDefSummary.getName());


            repositoryValidator.validatePropertiesForType(repositoryName,
                    newTypeDefParameterName,
                    newTypeDefSummary,
                    relationship.getProperties(),
                    methodName);

            /*
             * Validation complete - ok to make changes
             */

            /* In Atlas the relationship type is stored in the AtlasRelationship typeName field.
             * If you want to update an relationship then you essentially pass the modified AtlasRelationship
             * to the relationship store's update() method - this accepts an AtlasRelationship.
             */

            /*
             * Alter the retrieved Relationship so that it's InstanceType reflects the new type
             * Then convert it to an AtlasRelationship and call the update method.
             */


            // Retrieve the TypeDef for the new type
            String newTypeGUID = newTypeDefSummary.getGUID();
            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, newTypeGUID);
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null || typeDef.getCategory() != RELATIONSHIP_DEF) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(newTypeGUID, newTypeDefParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            RelationshipDef relationshipDef = (RelationshipDef) typeDef;

            // Create a new InstanceType
            InstanceType newInstanceType = new InstanceType();
            newInstanceType.setTypeDefName(typeDef.getName());
            newInstanceType.setTypeDefCategory(typeDef.getCategory());
            // supertypes not relevant to Relationship
            newInstanceType.setTypeDefSuperTypes(null);
            List<TypeDefAttribute> typeDefAttributes = typeDef.getPropertiesDefinition();
            List<String> validPropertyNames = null;
            if (typeDefAttributes != null) {
                validPropertyNames = new ArrayList<>();
                for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                    validPropertyNames.add(typeDefAttribute.getAttributeName());
                }
            }
            newInstanceType.setValidInstanceProperties(validPropertyNames);
            newInstanceType.setTypeDefGUID(newTypeGUID);
            newInstanceType.setTypeDefVersion(typeDef.getVersion());
            newInstanceType.setValidStatusList(typeDef.getValidInstanceStatusList());
            newInstanceType.setTypeDefDescription(typeDef.getDescription());
            newInstanceType.setTypeDefDescriptionGUID(typeDef.getDescriptionGUID());

            // Set the new instance type into the relationship
            relationship.setType(newInstanceType);


            try {

                // Convert and store the re-typed relationship to Atlas. Because this is a retype of an existing
                // Relationship, reuse the same GUID, so useExistingGUID must be set to true.
                AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(relationship, true, relationshipDef);

                LOG.debug("{}: atlasRelationship to update is {}", methodName, atlasRelationship);

                // Save the retyped AtlasRelationship to the repository
                AtlasRelationship atlasRelationshipRetrieved = relationshipStore.update(atlasRelationship);

                // Convert and return the retrieved AtlasRelationship
                if (atlasRelationshipRetrieved == null) {
                    LOG.error("{}: Could not update relationship with guid {} ", methodName, relationshipGUID);
                    OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

                    throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }


                // Convert AtlasRelationship to OM Relationship.
                Relationship returnRelationship;

                AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                        this,
                        userId,
                        atlasRelationshipRetrieved,
                        entityStore);

                returnRelationship = atlasRelationshipMapper.toOMRelationship();
                LOG.debug("{}: retrieved Relationship {}", methodName, returnRelationship);

                return returnRelationship;

            } catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | EntityNotKnownException | InvalidRelationshipException | InvalidEntityException e) {
                LOG.error("{}: Caught OMRS exception", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

                throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Change the home of an existing relationship.  This action is taken for example, if the original home repository
     * becomes permanently unavailable, or if the user community updating this relationship move to working
     * from a different repository in the open metadata repository cohort.
     *
     * @param userId                      - unique identifier for requesting user.
     * @param relationshipGUID            - the unique identifier for the relationship.
     * @param typeDefGUID                 - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName                 - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId    - the existing identifier for this relationship's home.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return relationship - new values for this relationship, including the new home information.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                       metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship reHomeRelationship(String userId,
                                           String relationshipGUID,
                                           String typeDefGUID,
                                           String typeDefName,
                                           String homeMetadataCollectionId,
                                           String newHomeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName                = "reHomeRelationship";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "relationshipGUID";
        final String homeParameterName         = "homeMetadataCollectionId";
        final String newHomeParameterName      = "newHomeMetadataCollectionId";


        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, newHomeParameterName, newHomeMetadataCollectionId, methodName);

            /*
             * Locate relationship
             */
            Relationship relationship = this.getRelationship(userId, relationshipGUID);

            /*
             * Validation complete - ok to make changes
             */

            // Will need the RelationshipDef
            RelationshipDef relationshipDef;
            TypeDef relTypeDef;
            try {
                relTypeDef = _getTypeDefByName(userId, typeDefName);
            } catch (TypeDefNotKnownException e) {
                // Handle below
                relTypeDef = null;
            }
            if (relTypeDef == null || relTypeDef.getCategory() != RELATIONSHIP_DEF) {
                LOG.debug("{}: no existing relationship_def with name {}", methodName, typeDefName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeDefName, typeDefGUID, nameParameterName, methodName, repositoryName, "unknown");

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            relationshipDef = (RelationshipDef) relTypeDef;
            LOG.debug("{}: existing RelationshipDef: {}", methodName, relationshipDef);


            // Set the new metadataCollectionId in the relationship
            relationship.setMetadataCollectionId(newHomeMetadataCollectionId);

            // Convert and store the re-typed entity to Atlas
            try {
                // Construct an AtlasRelationship and call the repository
                AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(relationship, true, relationshipDef);
                LOG.debug("{}: atlasRelationship to update is {}", methodName, atlasRelationship);

                AtlasRelationship atlasReturnedRelationship = relationshipStore.update(atlasRelationship);

                // Convert AtlasRelationship to OM Relationship.
                Relationship returnRelationship;

                AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                        this,
                        userId,
                        atlasReturnedRelationship,
                        entityStore);

                returnRelationship = atlasRelationshipMapper.toOMRelationship();
                LOG.debug("{}: returning Relationship {}", methodName, returnRelationship);

                return returnRelationship;

            } catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidRelationshipException | EntityNotKnownException | InvalidEntityException e) {
                LOG.error("{}: Caught OMRS exception", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

                throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } catch (AtlasBaseException e) {

                LOG.error("{}: Caught exception from Atlas", methodName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    // =========================================================================================================

    // Group 6 methods

    /**
     * Save the entity as a reference copy.  The id of the home metadata collection is already set up in the
     * entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entity - details of the entity to save.
     * @throws InvalidParameterException  - the entity is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - the requested type is not known, or not supported in the metadata repository
     *                                    hosting the metadata collection.
     * @throws InvalidEntityException     - the new entity has invalid contents.
     * @throws PropertyErrorException     - one or more of the requested properties are not defined, or have different
     *                                    characteristics in the TypeDef for this entity's type.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void saveEntityReferenceCopy(String       userId,
                                        EntityDetail entity)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            InvalidEntityException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String methodName            = "saveEntityReferenceCopy";
        final String instanceParameterName = "entity";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);
            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateReferenceInstanceHeader(repositoryName,
                    metadataCollectionId,
                    instanceParameterName,
                    entity,
                    methodName);


            // Validate the system attributes - fields in the InstanceAuditHeader

            ArrayList<String> missingFieldNames = new ArrayList<>();
            if (entity.getInstanceProvenanceType() == null) {
                missingFieldNames.add("instanceProvenanceType");
            }
            if (entity.getMetadataCollectionId() == null) {
                missingFieldNames.add("metadataCollectionId");
            }
            if (entity.getCreatedBy() == null) {
                missingFieldNames.add("createdBy");
            }
            if (entity.getUpdatedBy() == null) {
                missingFieldNames.add("updatedBy");
            }
            if (entity.getCreateTime() == null) {
                missingFieldNames.add("createTime");
            }
            if (entity.getUpdateTime() == null) {
                missingFieldNames.add("updateTime");
            }
            if (entity.getStatus() == null) {
                missingFieldNames.add("status");
            }
            if (!missingFieldNames.isEmpty()) {

                StringBuilder fieldMessage = new StringBuilder();
                fieldMessage.append("Supplied entity is missing the following fields -");
                Iterator<String> itMissingFields = missingFieldNames.iterator();
                while (itMissingFields.hasNext()) {
                    fieldMessage.append(" ").append(itMissingFields.next());
                }
                LOG.error("{}: {} ", methodName, fieldMessage);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_INSTANCE_HEADER;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        methodName,
                        repositoryName,
                        fieldMessage.toString());

                throw new InvalidEntityException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            // Convert the EntityDetail to an AtlasEntity and store it.
            TypeDef typeDef;
            try {
                if (entity.getType() != null) {
                    typeDef = _getTypeDefByGUID(userId, entity.getType().getTypeDefGUID());
                } else {
                    typeDef = null;
                }
            } catch (TypeDefNotKnownException e) {
                typeDef = null;
            }

            if (typeDef == null) {
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_INSTANCE_TYPE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entity.getGUID(), methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            if (entity == null) {
                LOG.debug("{}: the EntityDetail entity is null", methodName);
                return;
            }

            AtlasEntity atlasEntity;
            try {
                atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
            } catch (StatusNotSupportedException e) {
                LOG.error("{}: Could not set entity status for entity with GUID {} ", methodName, entity.getGUID(), e);
                OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new InvalidEntityException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            LOG.debug("{}: atlasEntity to update is {}", methodName, atlasEntity);

            // The above provides an initial atlas entity based on the supplied set of properties - now check Atlas has everything it needs.

            /* Properties in Egeria may be optional or mandatory. We would expect all mandatory Egeria properties to be supplied, as checked
             * by the validation above. However, where Egeria types are stored into Atlas and extend Atlas types - as they do in the case of
             * F5 types - we also need to check whether the Atlas types place any stricter constraints on the properties (i.e. Atlas attributes)
             * and if so we need to ensure that they are satisfied - by providing values.
             *
             * There may be mandatory attributes in the type hierarchy - including in the Atlas F5 types.
             * Detect any mandatory attributes and ensure that they are supplied. If an attribute is optional in the Egeria typedef, but inherits
             * a tighter constraint from the Atlas typesystem, synthesize a recognisable value for the attribute.
             * Within the Egeria and Atlas F5 types there is no need to traverse the cross-linked supertype references - proceed vertically and this
             * will ensure that every supertype is visited.
             */


            InstanceProperties properties = entity.getProperties();
            String omTypeName = typeDef.getName();
            String atlasTypeName;
            if (FamousFive.omTypeRequiresSubstitution(typeDef.getName()))
                atlasTypeName = FamousFive.getAtlasTypeName(omTypeName, null);
            else
                atlasTypeName = omTypeName;

            Map<String, AtlasStructDef.AtlasAttributeDef> allAttrDefs = getAllEntityAttributeDefs(atlasTypeName);

            // Ensure that the supplied properties satisfy the normalized attributes

            // Initial strategy is as follows:
            // If the properties passed to this method are not adequate to satisfy the mandatory
            Map<String, Object> supplementaryAttrs = new HashMap<>();
            Iterator<String> attributeNamesIter = allAttrDefs.keySet().iterator();
            while (attributeNamesIter.hasNext()) {
                String key = attributeNamesIter.next();
                AtlasStructDef.AtlasAttributeDef attrDef = allAttrDefs.get(key);

                if (properties.getPropertyValue(key) == null) {
                    // If property is null we either want to remove it - by passing the null value to Atlas
                    // or we may need to synthesize it - if the corresponding Atlas attribute is mandatory

                    if (attrDef != null && attrDef.getValuesMinCount() > 0) {
                        // This attribute is mandatory - and no value was supplied, so synthesize value if possible, or throw error
                        switch (attrDef.getTypeName()) {

                            case "string":
                                supplementaryAttrs.put(key, "OM_UNASSIGNED_ATTRIBUTE:" + atlasTypeName + "." + key);
                                break;

                            default:
                                LOG.error("{}: Mandatory attribute {} of type {} not supplied and cannot be synthesized", methodName, key, attrDef.getTypeName());
                                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.MANDATORY_ATTRIBUTE_MISSING;
                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(key, attrDef.getTypeName(), methodName, repositoryName);

                                throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());

                        }
                    }
                }
            }

            LOG.debug("{}: type {} has supplementary attributes {}", methodName, atlasTypeName, supplementaryAttrs);


            // Map supplied properties to Atlas attributes
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(properties);
            // add supplementary attributes...
            if (!supplementaryAttrs.isEmpty()) {
                if (atlasAttrs == null) {
                    atlasAttrs = new HashMap<>();
                }
                atlasAttrs.putAll(supplementaryAttrs);
            }
            atlasEntity.setAttributes(atlasAttrs);


            LOG.debug("{}: atlasEntity (minus any classifications) to create is {}", methodName, atlasEntity);

            // Store the entity
            // Because we want Atlas to accept the pre-assigned GUID (e.g. RID) supplied in the EntityDetail, we
            // need to ask Atlas to do this as if it were an import. Therefore we need to set the RequestContext
            // accordingly and reset it afterwards.

            boolean importSetting = RequestContext.get().isImportInProgress(); // should be false but remember anyway
            RequestContext.get().setImportInProgress(true);
            try {

                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWEI = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
                AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityWEI, null);
                EntityMutationResponse emr;
                try {
                    emr = entityStore.createOrUpdateForImport(eis);
                    LOG.debug("{}: emr is {}", methodName, emr);

                } catch (AtlasBaseException e) {
                    LOG.error("{}: Caught exception trying to create entity", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                            this.getClass().getName(),
                            repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            } finally {
                // restore the previous setting
                RequestContext.get().setImportInProgress(importSetting);
            }


            /* If there were classifications on the supplied EntityDetail, call Atlas to set classifications
             *
             * AtlasEntity needs a List<AtlasClassification> so we need to do some translation
             * OM Classification has:
             * String                 classificationName
             * InstanceProperties     classificationProperties
             * ClassificationOrigin   classificationOrigin
             * String                 classificationOriginGUID
             *
             * AtlasClassification has:
             * String                 entityGuid
             * boolean                propagate
             * List<TimeBoundary>     validityPeriods
             * String                 typeName;
             * Map<String, Object>    attributes;
             */
            if (entity.getClassifications() != null && entity.getClassifications().size() > 0) {
                ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();
                for (Classification omClassification : entity.getClassifications()) {
                    AtlasClassification atlasClassification = new AtlasClassification(omClassification.getName());
                    // For this OM classification build an Atlas equivalent...

                    /* For now we are always setting propagatable to true and AtlasClassification has propagate=true by default.
                     * Instead this could traverse to the Classification.InstanceType.typeDefGUID and retrieve the ClassificationDef
                     * to find the value of propagatable on the def.
                     */
                    atlasClassification.setTypeName(omClassification.getType().getTypeDefName());
                    atlasClassification.setEntityGuid(entity.getGUID());

                    // Map attributes from OM Classification to AtlasEntity
                    InstanceProperties classificationProperties = omClassification.getProperties();
                    Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                    atlasClassification.setAttributes(atlasClassificationAttrs);

                    LOG.debug("{}: adding classification {}", methodName, atlasClassification);
                    atlasClassifications.add(atlasClassification);
                }
                // We do not need to augment the AtlasEntity we created earlier - we can just use the
                // atlasClassifications directly with the following repository call...
                try {
                    LOG.debug("{}: adding classifications {}", methodName, atlasClassifications);
                    entityStore.addClassifications(entity.getGUID(), atlasClassifications);

                } catch (AtlasBaseException e) {
                    // Could not add classifications to the entity
                    LOG.error("{}: Atlas saved entity, but could not add classifications to it, guid {}", methodName, entity.getGUID(), e);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                            this.getClass().getName(),
                            repositoryName);
                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
            LOG.debug("{}: completed", methodName);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Remove a reference copy of the the entity from the local repository.  This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or entities that have come from open metadata archives.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param entityGUID               - the unique identifier for the entity.
     * @param typeDefGUID              - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName              - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the new home repository.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeEntityReferenceCopy(String userId,
                                         String entityGUID,
                                         String typeDefGUID,
                                         String typeDefName,
                                         String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {
        final String methodName          = "purgeEntityReferenceCopy";
        final String guidParameterName   = "typeDefGUID";
        final String nameParameterName   = "typeDefName";
        final String entityParameterName = "entityGUID";
        final String homeParameterName   = "homeMetadataCollectionId";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName, guidParameterName, nameParameterName, typeDefGUID, typeDefName, methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);


            /*
             * Locate entity
             */
            EntityDetail entity = getEntityDetail(userId, entityGUID);

            repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

            repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                    typeDefGUID,
                    typeDefName,
                    entity,
                    methodName);


            // On a purge of a reference copy the local repository connector does NOT need to check if the status is DELETED.
            // This is because the local repository connector does not know whether the remote (home) repository supports
            // soft delete - so it must simply purge the entity.
            // Note the requirement to replace the entity with a proxy, for any relationships.\
            EntityProxy entityProxy = null;

            /*
             * Validation is complete - ok to remove the entity
             */


            // Before doing the hard delete get a copy of the entity - so we can construct a proxy if needed.
            EntityDetail entityDetail;
            try {

                entityDetail = _getEntityDetail(userId, entityGUID, true);
            } catch (RepositoryErrorException | EntityNotKnownException e) {

                // Could not get existing entityDetail

                LOG.error("{}: Could not retrieve entity with GUID {} from Atlas repository", methodName, entityGUID);
                throw e;
            }

            if (entityDetail != null) {

                try {

                    // Perform a hard delete.
                    RequestContext.get().setDeleteType(DeleteType.HARD);
                    entityStore.deleteById(entityGUID);
                } catch (AtlasBaseException e) {

                    // Could not hard delete existing entity

                    // Don't log the exception - an attempt to delete an entity is not necessarily exceptional; tests do it deliberately; users might too.
                    LOG.error("{}: Atlas entityStore could not hard delete entity {}", methodName, entityGUID);

                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_DELETED;

                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(entityGUID, methodName, repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }


    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified entity sends out the details of this entity so the local repository can create a reference copy.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param entityGUID               - unique identifier of requested entity.
     * @param typeDefGUID              - unique identifier of requested entity's TypeDef.
     * @param typeDefName              - unique name of requested entity's TypeDef.
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void refreshEntityReferenceCopy(String userId,
                                           String entityGUID,
                                           String typeDefGUID,
                                           String typeDefName,
                                           String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName          = "refreshEntityReferenceCopy";
        final String guidParameterName   = "typeDefGUID";
        final String nameParameterName   = "typeDefName";
        final String entityParameterName = "entityGUID";
        final String homeParameterName   = "homeMetadataCollectionId";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName, guidParameterName, nameParameterName, typeDefGUID, typeDefName, methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);

            /*
             * This method needs to check whether this is the metadataCollection indicated in the homeMetadataCollectionId
             * parameter. If it is not, it will ignore the request. If it is the specified metadataCollection, then it needs
             * to retrieve the entity (by GUID) from Atlas and pass it to the EventMapper which will handle it (formulate the
             * appropriate call to the repository event processor).
             */

            if (!homeMetadataCollectionId.equals(this.metadataCollectionId)) {
                LOG.debug("{}: ignoring request because not intended for this metadataCollection", methodName);
                return;
            }

            if (this.eventMapper == null) {

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.EVENT_MAPPER_NOT_INITIALIZED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /* This is the correct metadataCollection and it has a valid event mapper.
             * Retrieve the entity and call the event mapper
             */

            // Using the supplied guid look up the entity.

            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
            try {
                atlasEntityWithExt = getAtlasEntityById(entityGUID);
            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from get entity by guid {}, {}", methodName, entityGUID, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            LOG.debug("{}: atlasEntityWithExt is {}", methodName, atlasEntityWithExt);
            AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

            // Project the AtlasEntity as an EntityDetail via the mapper

            try {

                AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== {}(userId={}, guid={}: entityDetail={})", methodName, userId, entityGUID, omEntityDetail);
                }
                this.eventMapper.processEntityRefreshEvent(omEntityDetail);

            } catch (TypeErrorException | InvalidEntityException e) {

                LOG.error("{}: caught exception from attempt to convert Atlas entity to OM {}, {}", methodName, atlasEntity, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }

    /**
     * Save the relationship as a reference copy.  The id of the home metadata collection is already set up in the
     * relationship.
     *
     * @param userId       - unique identifier for requesting user.
     * @param relationship - relationship to save.
     * @throws InvalidParameterException    - the relationship is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeErrorException           - the requested type is not known, or not supported in the metadata repository
     *                                      hosting the metadata collection.
     * @throws EntityNotKnownException      - one of the entities identified by the relationship is not found in the
     *                                      metadata collection.
     * @throws InvalidRelationshipException the new relationship has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public void saveRelationshipReferenceCopy(String       userId,
                                              Relationship relationship)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            EntityNotKnownException,
            InvalidRelationshipException,
            UserNotAuthorizedException
    {
        final String methodName            = "saveRelationshipReferenceCopy";
        final String instanceParameterName = "relationship";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);
            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateReferenceInstanceHeader(repositoryName, metadataCollectionId, instanceParameterName, relationship, methodName);


            // Validate the system attributes - fields in the InstanceAuditHeader

            ArrayList<String> missingFieldNames = new ArrayList<>();
            if (relationship.getInstanceProvenanceType() == null) {
                missingFieldNames.add("instanceProvenanceType");
            }
            if (relationship.getMetadataCollectionId() == null) {
                missingFieldNames.add("metadataCollectionId");
            }
            if (relationship.getCreatedBy() == null) {
                missingFieldNames.add("createdBy");
            }
            if (relationship.getUpdatedBy() == null) {
                missingFieldNames.add("updatedBy");
            }
            if (relationship.getCreateTime() == null) {
                missingFieldNames.add("createTime");
            }
            if (relationship.getUpdateTime() == null) {
                missingFieldNames.add("updateTime");
            }
            if (relationship.getStatus() == null) {
                missingFieldNames.add("status");
            }
            if (!missingFieldNames.isEmpty()) {

                StringBuilder fieldMessage = new StringBuilder();
                fieldMessage.append("Supplied relationship is missing the following fields -");
                Iterator<String> itMissingFields = missingFieldNames.iterator();
                while (itMissingFields.hasNext()) {
                    fieldMessage.append(" ").append(itMissingFields.next());
                }
                LOG.error("{}: {} ", methodName, fieldMessage);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_INSTANCE_HEADER;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        methodName,
                        repositoryName,
                        fieldMessage.toString());

                throw new InvalidRelationshipException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            // Need to access the relationship def to find the OM propagation rule.
            // Use the typeName from the relationship to locate the RelationshipDef typedef for the relationship.

            // Note that relationship end def attributeNames are reversed between OM and Atlas.... see the converter methods.


            // Find the relationship type
            String relationshipTypeName = relationship.getType().getTypeDefName();
            TypeDef typeDef;
            try {

                typeDef = _getTypeDefByName(userId, relationshipTypeName);

            } catch (TypeDefNotKnownException e) {
                LOG.debug("{}: Caught exception attempting to get relationship type from _getTypeDefByName {}", methodName, e.getMessage());
                // Handle below
                typeDef = null;
            }
            // Validate it
            if (typeDef == null || typeDef.getCategory() != RELATIONSHIP_DEF) {
                LOG.error("{}: Could not find a RelationshipDef with name {} ", methodName, relationshipTypeName);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
            RelationshipDef relationshipDef = (RelationshipDef) typeDef;


            // Find the GUIDs and the type names of the entities
            String entityOneGUID = null;
            String entityOneTypeName = null;
            EntityProxy entityOneProxy = relationship.getEntityOneProxy();
            if (entityOneProxy != null) {
                entityOneGUID = entityOneProxy.getGUID();
                if (entityOneProxy.getType() != null) {
                    entityOneTypeName = entityOneProxy.getType().getTypeDefName();
                }
            }

            // Test whether we have the entity (by GUID). If so that's cool; if not we must create a proxy
            // Using the supplied guid look up the entity
            try {
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                atlasEntityWithExt = getAtlasEntityById(entityOneGUID);
                LOG.debug("{}: entity one exists in repository; {}", methodName, atlasEntityWithExt);

            } catch (AtlasBaseException e) {

                LOG.debug("{}: exception from get entity by guid {}, {}", methodName, entityOneGUID, e.getMessage());
                // The entity does not already exist, so create as proxy
                try {
                    addEntityProxy(userId, entityOneProxy);
                } catch (InvalidParameterException | RepositoryErrorException | UserNotAuthorizedException exc) {
                    LOG.error("{}: caught exception from addEntityProxy", methodName, exc);
                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;

                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                            entityOneProxy.toString(), methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

            }


            String entityTwoGUID = null;
            String entityTwoTypeName = null;
            EntityProxy entityTwoProxy = relationship.getEntityTwoProxy();
            if (entityTwoProxy != null) {
                entityTwoGUID = entityTwoProxy.getGUID();
                if (entityTwoProxy.getType() != null) {
                    entityTwoTypeName = entityTwoProxy.getType().getTypeDefName();
                }
            }

            // Test whether we have the entity (by GUID). If so that's cool; if not we must create a proxy
            // Using the supplied guid look up the entity
            try {
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                atlasEntityWithExt = getAtlasEntityById(entityTwoGUID);
                LOG.debug("{}: entity two exists in repository; {}", methodName, atlasEntityWithExt);

            } catch (AtlasBaseException e) {

                LOG.debug("saveRelationshipReferenceCopy: exception from get entity by guid {}, {}", entityTwoGUID, e.getMessage());
                // The entity does not already exist, so create as proxy
                try {
                    addEntityProxy(userId, entityTwoProxy);
                } catch (InvalidParameterException | RepositoryErrorException | UserNotAuthorizedException exc) {
                    LOG.error("{}: caught exception from addEntityProxy", methodName, exc);
                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;

                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                            entityTwoProxy.toString(), methodName, repositoryName);

                    throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

            }

            /* Create or Update the AtlasRelationship
             * If the relationship already exists - i.e. we have a relationship with the same GUID - then
             * perform an update. Otherwise we will create the relationship.
             */

            AtlasRelationship atlasRelationship = new AtlasRelationship();

            /* GUID is set by Atlas CTOR (called above) to nextInternalID. Because we are saving a ref copy we overwrite with
             * the provided GUID if one has been supplied. In some cases, the caller may not have a GUID to use, in which case
             * leave the GUID as generated by Atlas.
             */
            String suppliedGUID = relationship.getGUID();
            if (suppliedGUID != null) {
                atlasRelationship.setGuid(suppliedGUID);
            }
            // The atlasRelationship now has either the supplied GUID or has generated its own GUID


            /* The Atlas connector will need to later be able to identify the home repo that this object came from - as well as reconstitute that
             * repo's identifier for the object - which could be an IGC rid for example. The GUID is stored as supplied and the homeId is set to the
             * metadataCollectionId of the home repository.
             */
            atlasRelationship.setHomeId(relationship.getMetadataCollectionId());

            if (relationship.getInstanceProvenanceType() != null) {
                atlasRelationship.setProvenanceType(relationship.getInstanceProvenanceType().getOrdinal());
            } else {
                atlasRelationship.setProvenanceType(InstanceProvenanceType.UNKNOWN.getOrdinal());
            }

            atlasRelationship.setTypeName(relationshipTypeName);
            atlasRelationship.setStatus(ACTIVE);
            atlasRelationship.setCreatedBy(relationship.getCreatedBy());
            atlasRelationship.setUpdatedBy(relationship.getUpdatedBy());
            atlasRelationship.setCreateTime(relationship.getCreateTime());
            atlasRelationship.setUpdateTime(relationship.getUpdateTime());
            atlasRelationship.setVersion(relationship.getVersion());
            // Set end1
            AtlasObjectId end1 = new AtlasObjectId();
            end1.setGuid(entityOneGUID);
            end1.setTypeName(entityOneTypeName);
            atlasRelationship.setEnd1(end1);
            // Set end2
            AtlasObjectId end2 = new AtlasObjectId();
            end2.setGuid(entityTwoGUID);
            end2.setTypeName(entityTwoTypeName);
            atlasRelationship.setEnd2(end2);
            atlasRelationship.setLabel(relationshipTypeName);    // Set the label to the type name of the relationship def.
            // Set propagateTags
            ClassificationPropagationRule omPropRule = relationshipDef.getPropagationRule();
            AtlasRelationshipDef.PropagateTags atlasPropTags = convertOMPropagationRuleToAtlasPropagateTags(omPropRule);
            atlasRelationship.setPropagateTags(atlasPropTags);

            // Set attributes on AtlasRelationship
            // Get the relationshipProperties from the OM Relationship, and create Atlas attributes...

            // Map attributes from OM Relationship to AtlasRelationship
            InstanceProperties instanceProperties = relationship.getProperties();
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
            atlasRelationship.setAttributes(atlasAttrs);

            // AtlasRelationship should have been fully constructed by this point

            // Call the repository
            // If the relationship does not exist we will call create(). If it already exists then we must call update().
            // So we need to find out whether it exists or not... the getById will throw an exception if the id does
            // not yet exist.

            boolean relationshipExists = false;
            try {
                AtlasRelationship existingRelationship = getAtlasRelationshipById(atlasRelationship.getGuid());
                if (existingRelationship != null) {
                    // No exception thrown and relationship was returned
                    LOG.debug("{}: Relationship with GUID {} already exists - so will be updated", methodName, atlasRelationship.getGuid());
                    relationshipExists = true;
                }
            } catch (AtlasBaseException e) {
                if (e.getAtlasErrorCode() == AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND) {
                    LOG.debug("{}: Relationship with GUID {} does not exist - so will be created", methodName, atlasRelationship.getGuid());
                    relationshipExists = false;
                } else {
                    // Trouble at mill...
                    LOG.debug("{}: Caught exception from Atlas relationship store getById method {}", methodName, e.getMessage());

                    LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;

                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                            this.getClass().getName(),
                            methodName,
                            e.getMessage(),
                            repositoryName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }


            AtlasRelationship returnedAtlasRelationship;

            try {
                if (relationshipExists) {
                    LOG.debug("{}: Updating existing relationship wih GUID {}", methodName, atlasRelationship.getGuid());
                    returnedAtlasRelationship = relationshipStore.update(atlasRelationship);
                } else {
                    LOG.debug("{}: Creating new relationship wih GUID {}", methodName, atlasRelationship.getGuid());
                    returnedAtlasRelationship = relationshipStore.create(atlasRelationship);
                }
            } catch (AtlasBaseException e) {

                LOG.debug("{}: Caught exception from Atlas relationship store create/update method {}", methodName, e.getMessage());

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        methodName,
                        e.getMessage(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }


            // Verify the returnedAtlasRelationship and fill in any extra detail in relationship - e.g. sys attrs

            if (returnedAtlasRelationship.getStatus() != ACTIVE) {
                LOG.error("{}: Atlas created relationship, but status set to {}", methodName, returnedAtlasRelationship.getStatus());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            LOG.debug("{}: Save to Atlas returned relationship {}", methodName, returnedAtlasRelationship);

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }

    }


    /**
     * Remove the reference copy of the relationship from the local repository. This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or relationships that have come from open metadata archives.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param relationshipGUID         - the unique identifier for the relationship.
     * @param typeDefGUID              - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName              - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identifier is not recognized.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public void purgeRelationshipReferenceCopy(String userId,
                                               String relationshipGUID,
                                               String typeDefGUID,
                                               String typeDefName,
                                               String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {
        final String methodName                = "purgeRelationshipReferenceCopy";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "relationshipGUID";
        final String homeParameterName         = "homeMetadataCollectionId";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {


            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName,
                    guidParameterName,
                    nameParameterName,
                    typeDefGUID,
                    typeDefName,
                    methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);


            /*
             * Locate relationship
             */
            Relationship relationship = getRelationship(userId, relationshipGUID);

            repositoryValidator.validateRelationshipFromStore(repositoryName, relationshipGUID, relationship, methodName);

            repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                    typeDefGUID,
                    typeDefName,
                    relationship,
                    methodName);


            // On a purge of a reference copy the local repository connector does NOT need to check if the status is DELETED.
            // This is because the local repository connector does not know whether the remote (home) repository supports
            // soft delete - so it must simply purge the entity.


            /*
             * Validation is complete - ok to remove the relationship
             */


            // Perform a hard delete.

            RequestContext.get().setDeleteType(DeleteType.HARD);

            try {

                relationshipStore.deleteById(relationshipGUID);

            } catch (AtlasBaseException e) {

                // Don't log the exception - an attempt to delete an relationship is not necessarily exceptional; tests do it deliberately; users might too.
                LOG.error("{}: Atlas entityStore could not hard delete relationship {}", methodName, relationshipGUID);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.RELATIONSHIP_NOT_DELETED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(relationshipGUID, methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified relationship sends out the details of this relationship so the local repository can create a
     * reference copy.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param relationshipGUID         - unique identifier of the relationship.
     * @param typeDefGUID              - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName              - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identifier is not recognized.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public void refreshRelationshipReferenceCopy(String userId,
                                                 String relationshipGUID,
                                                 String typeDefGUID,
                                                 String typeDefName,
                                                 String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName                = "refreshRelationshipReferenceCopy";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "relationshipGUID";
        final String homeParameterName         = "homeMetadataCollectionId";

        // Remember the existing user in the RequestContext, and push API user into context
        String existingUserId = RequestContext.get().getUser();
        RequestContext.get().setUser(userId, null);
        try {

            /*
             * Validate parameters
             */
            this.validateRepositoryConnector(methodName);
            parentConnector.validateRepositoryIsActive(methodName);

            repositoryValidator.validateUserId(repositoryName, userId, methodName);
            repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
            repositoryValidator.validateTypeDefIds(repositoryName, guidParameterName, nameParameterName, typeDefGUID, typeDefName, methodName);
            repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);


            /*
             * This method needs to check whether this is the metadataCollection indicated in the homeMetadataCollectionId
             * parameter. If it is not, it will ignore the request. If it is the specified metadataCollection, then it needs
             * to retrieve the entity (by GUID) from Atlas and pass it to the EventMapper which will handle it (formulate the
             * appropriate call to the repository event processor).
             */

            if (!homeMetadataCollectionId.equals(this.metadataCollectionId)) {
                LOG.debug("{}: ignoring request because not intended for this metadataCollection", methodName);
                return;
            }

            if (this.eventMapper == null) {

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.EVENT_MAPPER_NOT_INITIALIZED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            /* This is the correct metadataCollection and it has a valid event mapper.
             * Retrieve the entity and call the event mapper
             */

            // Using the supplied guid look up the entity.

            AtlasRelationship atlasRelationship;
            try {
                atlasRelationship = getAtlasRelationshipById(relationshipGUID);
            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from get entity by guid {}, {}", methodName, relationshipGUID, e);

                OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, methodName, metadataCollectionId);

                throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            LOG.debug("{}: atlasRelationship is {}", methodName, atlasRelationship);

            // Project the AtlasRelationship as a Relationship via the mapper

            try {

                AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(this, userId, atlasRelationship, entityStore);
                Relationship omRelationship = atlasRelationshipMapper.toOMRelationship();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== {}(userId={}, guid={}: entityDetail={})", methodName, userId, relationshipGUID, omRelationship);
                }
                this.eventMapper.processRelationshipRefreshEvent(omRelationship);

            } catch (TypeErrorException | InvalidEntityException | InvalidRelationshipException | EntityNotKnownException e) {

                LOG.error("{}: caught exception from attempt to convert Atlas entity to OM {}, {}", methodName, atlasRelationship, e);

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } finally {
            // Restore the original userId in the RequestContext
            RequestContext.get().setUser(existingUserId, null);
        }


    }


    // ==============================================================================================================
    //
    // INTERNAL METHODS AND HELPER CLASSES
    //

    /*
     * Parse the OM EntityDef and create an AtlasEntityDef
     */
    private AtlasEntityDef convertOMEntityDefToAtlasEntityDef(EntityDef omEntityDef)
    {

        final String methodName = "convertOMEntityDefToAtlasEntityDef";


        LOG.debug("{}: OM EntityDef {}", methodName, omEntityDef);

        if (omEntityDef == null) {
            return null;
        }

        String omTypeName = omEntityDef.getName();
        String atlasTypeName = omTypeName;

        // Detect whether the OM Type is one of the Famous Five
        boolean famousFive = false;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName)) {
            famousFive = true;
            // Prefix the type name
            atlasTypeName = FamousFive.getAtlasTypeName(omTypeName, omEntityDef.getGUID());
        }

        // Convert OM type into a valid Atlas type

        // Allocate AtlasEntityDef, which will set TypeCategory automatically
        AtlasEntityDef atlasEntityDef = new AtlasEntityDef();

        // Set common fields
        atlasEntityDef.setGuid(omEntityDef.getGUID());
        atlasEntityDef.setName(atlasTypeName);
        atlasEntityDef.setDescription(omEntityDef.getDescription());
        atlasEntityDef.setVersion(omEntityDef.getVersion());
        atlasEntityDef.setTypeVersion(omEntityDef.getVersionName());
        atlasEntityDef.setCreatedBy(omEntityDef.getCreatedBy());
        atlasEntityDef.setUpdatedBy(omEntityDef.getUpdatedBy());
        atlasEntityDef.setCreateTime(omEntityDef.getCreateTime());
        atlasEntityDef.setUpdateTime(omEntityDef.getUpdateTime());
        atlasEntityDef.setOptions(omEntityDef.getOptions());

        // Handle fields that require conversion - i.e. supertypes, attributeDefs
        // subtypes are deliberately ignored

        // Convert OMRS List<TypeDefLink> to an Atlas Set<String> of Atlas type names
        // If famousFive then modify the superType hierarchy
        if (!famousFive) {
            TypeDefLink omSuperType = omEntityDef.getSuperType();
            if (omSuperType == null) {
                // If there is no superType defined in OM EntityDef then set atlasEntityDef.superTypes to null
                atlasEntityDef.setSuperTypes(null);
            } else {
                // OM type has supertype...add that plus original OM type as supertypes in Atlas.
                // If the OM supertype is itself in Famous Five, convert it...
                String omSuperTypeName = omSuperType.getName();
                String omSuperTypeGUID = omSuperType.getGUID();
                if (FamousFive.omTypeRequiresSubstitution(omSuperTypeName)) {
                    // Prefix the supertype name
                    String atlasSuperTypeName = FamousFive.getAtlasTypeName(omSuperTypeName, omSuperTypeGUID);
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(atlasSuperTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                } else {
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(omSuperTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                }
            }
        } else { // famousFive
            TypeDefLink omSuperType = omEntityDef.getSuperType();
            if (omSuperType == null) {
                // If there is no superType defined in OM set atlasEntityDef.superTypes to original OM type
                Set<String> atlasSuperTypes = new HashSet<>();
                atlasSuperTypes.add(omTypeName);
                atlasEntityDef.setSuperTypes(atlasSuperTypes);
            } else {
                // OM type has supertype...add that plus original OM type as supertypes in Atlas.
                // If the OM supertype is itself in Famous Five, convert it...
                String omSuperTypeName = omSuperType.getName();
                String omSuperTypeGUID = omSuperType.getGUID();
                if (FamousFive.omTypeRequiresSubstitution(omSuperTypeName)) {
                    // Prefix the supertype name
                    String atlasSuperTypeName = FamousFive.getAtlasTypeName(omSuperTypeName, omSuperTypeGUID);
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(atlasSuperTypeName);
                    atlasSuperTypes.add(omTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                } else {
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(omSuperTypeName);
                    atlasSuperTypes.add(omTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                }
            }
        }

        // Set Atlas Attribute defs
        // OMRS ArrayList<TypeDefAttribute> --> Atlas List<AtlasAttributeDef>
        // Retrieve the OM EntityDef attributes:
        List<TypeDefAttribute> omAttrs = omEntityDef.getPropertiesDefinition();
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttrs = convertOMAttributeDefs(omAttrs);
        atlasEntityDef.setAttributeDefs(atlasAttrs);

        // Return the AtlasEntityDef
        return atlasEntityDef;


    }


    /*
     * Parse the OM RelationshipDef and create an AtlasRelationshipDef
     */
    private AtlasRelationshipDef convertOMRelationshipDefToAtlasRelationshipDef(RelationshipDef omRelationshipDef)
            throws
            RepositoryErrorException,
            TypeErrorException
    {


        final String methodName = "convertOMRelationshipDefToAtlasRelationshipDef";

        LOG.debug("{}: OM RelationshipDef {}", methodName, omRelationshipDef);

        if (omRelationshipDef == null) {
            return null;
        }

        /*
         * Convert OM type into a valid Atlas type:
         *
         *  [OM RelationshipDef]                                         ->  [AtlasRelationshipDef]
         *  RelationshipCategory               relationshipCategory      ->  RelationshipCategory    relationshipCategory; REMOVED
         *  RelationshipContainerEnd           relationshipContainerEnd  ->  sets aspects of EndDef that is the ctr end    REMOVED
         *  ClassificationPropagationRule      propagationRule           ->  PropagateTags           propagateTags;
         *  RelationshipEndDef                 endDef1                   ->  AtlasRelationshipEndDef endDef1;
         *  RelationshipEndDef                 endDef2                   ->  AtlasRelationshipEndDef endDef2;
         *  TypeDefLink                        superType                 ->  IGNORED
         *  String                             description               ->  description
         *  String                             descriptionGUID           ->  IGNORED
         *  String                             origin                    ->  IGNORED
         *  String                             createdBy                 ->  createdBy
         *  String                             updatedBy                 ->  updatedBy
         *  Date                               createTime                ->  createTime
         *  Date                               updateTime                ->  updateTime
         *  Map<String, String>                options                   ->  options
         *  ArrayList<ExternalStandardMapping> externalStandardMappings  ->  IGNORED
         *  ArrayList<InstanceStatus>          validInstanceStatusList   ->  IGNORED
         *  InstanceStatus                     initialStatus             ->  IGNORED
         *  ArrayList<TypeDefAttribute>        propertiesDefinition      ->  attributeDefs
         *  Long                               version                   ->  atlas version
         *  String                             versionName               ->  typeVersion
         *  TypeDefCategory                    category                  ->  NOT NEEDED Atlas Cat set by CTOR
         *  String                             guid                      ->  atlas guid
         *  String                             name                      ->  atlas name
         */


        // Allocate AtlasRelationshipDef, which will set TypeCategory automatically
        AtlasRelationshipDef atlasRelationshipDef;
        try {
            atlasRelationshipDef = new AtlasRelationshipDef();

        } catch (AtlasBaseException e) {
            LOG.error("{}: could not create an AtlasRelationshipDef for type {}", methodName, omRelationshipDef.getName(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("RelationshipDef", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Set common fields
        atlasRelationshipDef.setGuid(omRelationshipDef.getGUID());
        atlasRelationshipDef.setName(omRelationshipDef.getName());
        atlasRelationshipDef.setDescription(omRelationshipDef.getDescription());
        atlasRelationshipDef.setVersion(omRelationshipDef.getVersion());
        atlasRelationshipDef.setTypeVersion(omRelationshipDef.getVersionName());
        atlasRelationshipDef.setCreatedBy(omRelationshipDef.getCreatedBy());
        atlasRelationshipDef.setUpdatedBy(omRelationshipDef.getUpdatedBy());
        atlasRelationshipDef.setCreateTime(omRelationshipDef.getCreateTime());
        atlasRelationshipDef.setUpdateTime(omRelationshipDef.getUpdateTime());
        atlasRelationshipDef.setOptions(omRelationshipDef.getOptions());

        /*
         * Remaining fields require conversion - supertypes and subtypes are deliberately ignored
         */

        /* If in future OM RelationshipDef supports different categories of Relationship, convert accordingly:
         *
         * RelationshipCategory omRelCat = omRelationshipDef.getRelationshipCategory();
         * AtlasRelationshipDef.RelationshipCategory atlasRelCat = convertOMRelationshipCategoryToAtlasRelationshipCategory(omRelCat);
         * atlasRelationshipDef.setRelationshipCategory(atlasRelCat);
         *
         * For now though:
         * OM relationship defs are always ASSOCIATIONS. Atlas supports COMPOSITION and AGGREGATION too, but OM only uses ASSOCIATION
         */
        atlasRelationshipDef.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);

        // Convert propagationRule to propagateTags
        ClassificationPropagationRule omPropRule = omRelationshipDef.getPropagationRule();

        AtlasRelationshipDef.PropagateTags atlasPropTags = convertOMPropagationRuleToAtlasPropagateTags(omPropRule);

        atlasRelationshipDef.setPropagateTags(atlasPropTags);

        /* RelationshipEndDef needs to be converted to AtlasRelationshipEndDef
         *
         * OM RelationshipEndDef contains:
         * TypeDefLink  entityType   which contains name and guid     -> set Atlas type
         * String       attributeName                                 -> set Atlas name
         * String       attributeDescription                          -> set Atlas description
         * String       attributeDescriptionGUID                      -> IGNORED
         * AttributeCardinality attributeCardinality                  -> set Atlas cardinality
         * In Atlas RED we need the following fields - which are set as above, unless noted differently:
         *
         * Note that the attribute naming is transposed between the OM and Atlas models, so we swap the
         * names - i.e. the attrName of OM end1 will be used for the attribute name of Atlas end2 and v.v.
         * This is because Atlas stores the relationship ends as literal objects - i.e. the object will contain
         * the attributeName that will be used to refer to the other (relationship) end. - e.g. the RelationshipEndDef
         * for a Glossary that has a TermAnchor relationship to a Term will be stored as type="Glossary";attributeName="terms".
         * This is the opposite to OM which defines the RelEndDef for Glossary as type="Glossary"; attributeName="anchor" - ie.
         * the name by which the Glossary will be 'known' (referred to) from a Term. Therefore when converting between
         * OM and Atlas we must switch the attributeNames between ends 1 and 2.
         *
         * String type;
         * String name;
         * boolean isContainer;        <--will be set from OM RCE after both ends have been established
         * Cardinality cardinality;
         * boolean isLegacyAttribute;  <-- will always be set to false.
         * String description;
         *
         * The connector take the various parts of the RelationshipDef in good faith and does not perform existence
         * checking or comparison - e.g. that the entity defs exist. That is delegated to Atlas since it has that
         * checking already.
         */

        // Get both the OM end defs and validate them. We also need both their types up front - so we can swap them over.


        // END1
        //RelationshipEndDef            endDef1                   ->  AtlasRelationshipEndDef endDef1;
        RelationshipEndDef omEndDef1 = omRelationshipDef.getEndDef1();
        // An OM end def must have a type and an attributeName, otherwise it will fail to register in Atlas
        TypeDefLink omTDL1 = omEndDef1.getEntityType();
        String attributeName1 = omEndDef1.getAttributeName();
        String attributeDescription1 = omEndDef1.getAttributeDescription();
        if (attributeName1 == null || omTDL1.getName() == null || omTDL1.getGUID() == null) {
            // There is not enough information to create the relationship end - and hence the relationship def
            LOG.error("{}: Failed to convert OM RelationshipDef {} - end1 partially defined; name {}, type {}",
                    methodName, omRelationshipDef.getName(), attributeName1, omTDL1.getName());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // END2
        RelationshipEndDef omEndDef2 = omRelationshipDef.getEndDef2();
        TypeDefLink omTDL2 = omEndDef2.getEntityType();
        String attributeName2 = omEndDef2.getAttributeName();
        String attributeDescription2 = omEndDef2.getAttributeDescription();
        if (attributeName2 == null || omTDL2.getName() == null || omTDL2.getGUID() == null) {
            // There is not enough information to create the relationship end - and hence the relationship def
            LOG.error("{}: Failed to convert OM RelationshipDef {} - end2 partially defined; name {}, type {}",
                    methodName, omRelationshipDef.getName(), attributeName2, omTDL2.getName());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /* Process END1
         * Note that hen converting from Atlas to OM or vice versa, the attribute name and cardinality needs to be switched
         * from one end to the other.
         */
        AtlasRelationshipEndDef atlasEndDef1 = new AtlasRelationshipEndDef();
        atlasEndDef1.setName(omEndDef2.getAttributeName());   // attribute names are deliberately transposed as commented above
        atlasEndDef1.setDescription(attributeDescription2);   // attribute descriptions are deliberately transposed as above
        // Ends with entity types in famous five need to be converted...
        String omTypeName1 = omTDL1.getName();
        String omTypeGUID1 = omTDL1.getGUID();
        String atlasTypeName1 = omTypeName1;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName1)) {
            atlasTypeName1 = FamousFive.getAtlasTypeName(omTypeName1, omTypeGUID1);
        }
        atlasEndDef1.setType(atlasTypeName1);
        LOG.debug("{}: Atlas end1 is {}", methodName, atlasEndDef1);


        atlasEndDef1.setIsLegacyAttribute(false);

        /* Cardinality
         * Cardinality is mapped from OM to Atlas Cardinality { SINGLE, LIST, SET }.
         * An OM relationship end def has cardinality always interpreted in the 'optional' sense, so that
         * the relationship ends are 0..1 or 0..* - allowing us to create a relationship instance between a pair of entity
         * instances as a one-to-many, many-to-one, many-to-many.
         */
        RelationshipEndCardinality omEndDef1Card = omEndDef2.getAttributeCardinality(); // attribute cardinality deliberately transposed as commented above
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasEndDef1Card;
        switch (omEndDef1Card) {

            case AT_MOST_ONE:
                atlasEndDef1Card = SINGLE;
                break;

            case ANY_NUMBER:
                atlasEndDef1Card = SET;
                break;

            default:
                /* Any other cardinality is unexpected - all OM RelationshipDefs are associations with optional, unordered ends.
                 * There is no sensible way to proceed here.
                 */
                LOG.error("{}: OM cardinality {} not valid for relationship end def 1 in relationship def {}", methodName, omEndDef1Card, omRelationshipDef.getName());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        atlasEndDef1.setCardinality(atlasEndDef1Card);
        atlasRelationshipDef.setEndDef1(atlasEndDef1);

        /* Process END2
         * Note that hen converting from Atlas to OM or vice versa, the attribute name and cardinality needs to be switched
         * from one end to the other.
         */

        AtlasRelationshipEndDef atlasEndDef2 = new AtlasRelationshipEndDef();
        atlasEndDef2.setName(omEndDef1.getAttributeName());     // attribute names are deliberately transposed as commented above
        atlasEndDef2.setDescription(attributeDescription1);     // attribute descriptions are deliberately transposed as above
        // Ends with entity types in famous five need to be converted...
        String omTypeName2 = omTDL2.getName();
        String omTypeGUID2 = omTDL2.getGUID();
        String atlasTypeName2 = omTypeName2;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName2)) {
            atlasTypeName2 = FamousFive.getAtlasTypeName(omTypeName2, omTypeGUID2);
        }
        atlasEndDef2.setType(atlasTypeName2);
        atlasEndDef2.setDescription(omEndDef2.getAttributeDescription());
        atlasEndDef2.setIsLegacyAttribute(false);

        /* Cardinality
         * Cardinality is mapped from OM to Atlas Cardinality { SINGLE, LIST, SET }.
         * An OM relationship end def has cardinality always interpreted in the 'optional' sense, so that
         * the relationship ends are 0..1 or 0..* - allowing us to create a relationship instance between a pair of entity
         * instances as a one-to-many, many-to-one, many-to-many.
         */
        RelationshipEndCardinality omEndDef2Card = omEndDef1.getAttributeCardinality();  // attribute cardinality deliberately transposed as commented a
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasEndDef2Card;
        switch (omEndDef2Card) {
            case AT_MOST_ONE:
                atlasEndDef2Card = SINGLE;
                break;

            case ANY_NUMBER:
                atlasEndDef2Card = SET;
                break;

            default:
                /* Any other cardinality is unexpected - all OM RelationshipDefs are associations with optional, unordered ends.
                 * There is no sensible way to proceed here.
                 */
                LOG.error("{}: OM cardinality {} not valid for relationship end def 2 in relationship def {}", methodName, omEndDef2Card, omRelationshipDef.getName());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        atlasEndDef2.setCardinality(atlasEndDef2Card);
        atlasRelationshipDef.setEndDef2(atlasEndDef2);

        /*
         * If in future OM RelationshipDef supports COMPOSITION and/or AGGREGATION, then set the container end.
         * For now a RelationshipDef is always an ASSOCIATION, so neither end is a container
         */
        atlasEndDef1.setIsContainer(false);
        atlasEndDef2.setIsContainer(false);

        /*
         * Set Atlas Attribute defs
         * OMRS ArrayList<TypeDefAttribute> --> Atlas List<AtlasAttributeDef>
         * Retrieve the OM RelationshipDef attributes:
         */
        List<TypeDefAttribute> omAttrs = omRelationshipDef.getPropertiesDefinition();
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttrs = convertOMAttributeDefs(omAttrs);
        atlasRelationshipDef.setAttributeDefs(atlasAttrs);

        /*
         * Return the AtlasRelationshipDef
         */
        LOG.debug("{}: AtlasRelationshipDef is {}", methodName, atlasRelationshipDef);
        return atlasRelationshipDef;
    }


    /*
     * Parse the OM ClassificationDef and create an AtlasClassificationDef
     * @param omClassificationDef - the OM classification def to convert
     * @return - the Atlas classification def
     */
    private AtlasClassificationDef convertOMClassificationDefToAtlasClassificationDef(ClassificationDef omClassificationDef)
    {

        final String methodName = "convertOMClassificationDefToAtlasClassificationDef";

        LOG.debug("{}: OM ClassificationDef {}", methodName, omClassificationDef);

        if (omClassificationDef == null) {
            return null;
        }

        /*
         * OM ClassificationDef                                             AtlasClassificationDef
         * --------------------                                             ----------------------
         * ArrayList<TypeDefLink>               validEntityDefs          -> entityTypes
         * . boolean                            propagatable             -> IGNORED
         * TypeDefLink                          superType                -> superTypes
         * . String                             description              -> description
         * . String                             descriptionGUID          -> IGNORED
         * . String                             origin                   -> IGNORED
         * . String                             createdBy                -> createdBy
         * . String                             updatedBy                -> updatedBy
         * . Date                               createTime               -> createTime
         * . Date                               updateTime               -> updateTime
         * . Map<String, String>                options                  -> options
         * . ArrayList<ExternalStandardMapping> externalStandardMappings -> IGNORED
         * . ArrayList<InstanceStatus>          validInstanceStatusList  -> IGNORED
         * . InstanceStatus                     initialStatus            -> IGNORED
         * . ArrayList<TypeDefAttribute>        propertiesDefinition     -> attributeDefs
         * . Long                               version                  -> version
         * . String                             versionName              -> typeVersion
         * . TypeDefCategory                    category                 -> NOT NEEDED Atlas category set by constructor
         * . String                             uid                      -> guid
         * . String                             name                     -> name
         *
         */

        // Convert OM type into a valid Atlas type

        // Allocate AtlasClassificationDef, which will set TypeCategory automatically
        AtlasClassificationDef atlasClassificationDef = new AtlasClassificationDef();

        // Set common fields
        atlasClassificationDef.setGuid(omClassificationDef.getGUID());
        atlasClassificationDef.setName(omClassificationDef.getName());
        atlasClassificationDef.setDescription(omClassificationDef.getDescription());
        atlasClassificationDef.setVersion(omClassificationDef.getVersion());
        atlasClassificationDef.setTypeVersion(omClassificationDef.getVersionName());
        atlasClassificationDef.setCreatedBy(omClassificationDef.getCreatedBy());
        atlasClassificationDef.setUpdatedBy(omClassificationDef.getUpdatedBy());
        atlasClassificationDef.setCreateTime(omClassificationDef.getCreateTime());
        atlasClassificationDef.setUpdateTime(omClassificationDef.getUpdateTime());
        atlasClassificationDef.setOptions(omClassificationDef.getOptions());

        // Handle fields that require conversion - i.e. supertypes and validEntityDefs

        // Set the (at most one) superType
        // Note that there is no error or existence checking on this
        TypeDefLink omSuperType = omClassificationDef.getSuperType();
        atlasClassificationDef.setSuperTypes(null);
        if (omSuperType != null) {
            String atlasSuperType = omSuperType.getName();
            Set<String> atlasSuperTypes = new HashSet<>();
            atlasSuperTypes.add(atlasSuperType);
            atlasClassificationDef.setSuperTypes(atlasSuperTypes);
        }

        // Set the validEntityDefs
        // For each TypeDefLink in the OM list of VEDs, extract the name into a Set<String> for Atlas.
        // Note that there is no error or existence checking on this
        List<TypeDefLink> omVEDs = omClassificationDef.getValidEntityDefs();
        Set<String> atlasEntityTypes = null;
        if (omVEDs != null) {
            atlasEntityTypes = new HashSet<>();
            for (TypeDefLink tdl : omVEDs) {
                LOG.debug("{}: process OM VED {}", methodName, tdl.getName());
                String omEntityTypeName = tdl.getName();
                String omEntityTypeGUID = tdl.getGUID();
                String atlasEntityTypeName = omEntityTypeName;
                if (FamousFive.omTypeRequiresSubstitution(omEntityTypeName)) {
                    atlasEntityTypeName = FamousFive.getAtlasTypeName(omEntityTypeName, omEntityTypeGUID);
                    LOG.debug("{}: substituted type name {}", methodName, atlasEntityTypeName);
                }
                atlasEntityTypes.add(atlasEntityTypeName);
            }
        }
        atlasClassificationDef.setEntityTypes(atlasEntityTypes);

        // Set Atlas Attribute defs
        List<TypeDefAttribute> omAttrs = omClassificationDef.getPropertiesDefinition();
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttrs = convertOMAttributeDefs(omAttrs);
        atlasClassificationDef.setAttributeDefs(atlasAttrs);

        // Return the AtlasClassificationDef
        return atlasClassificationDef;
    }


    // Accept an AtlasTypesDef and populate the typeDefsForAPI so it contains all OM defs corresponding to
    // defs encountered in the AtlasTypesDef
    //
    private void convertAtlasTypeDefs(String        userId,
                                      AtlasTypesDef atd)
    {

        final String methodName = "convertAtlasTypeDefs";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, atd={}", methodName, userId, atd);
        }

        if (atd == null) {
            return;
        }

        // This method could walk the AtlasTypesDef in any order because discovered defs are marshalled
        // by the typeDefsForAPI and converted into desired output order later. In order to allow the
        // connector to discover dependencies before they are required, use the following order:
        //
        // i.e. enums -> entities -> relationships -> classifications ( structs are ignored )
        //
        // For example a classificationDef may refer to an entityDef in its list of validEntityDefs.
        //
        // Each category method will handle all the typedefs it can - i.e. all those that result in valid
        // OM typedefs. Not all Atlas type defs can be modelled - e.g. an Atlas def could have multiple
        // superTypes. Any problems with conversion of the AtlasTypeDefs are handled internally by the individual
        // parsing methods, and each method returns a list of the OM defs it could
        // generate from the given AtlasTypeDefs.

        // Process enums
        List<AtlasEnumDef> enumDefs = atd.getEnumDefs();
        processAtlasEnumDefs(enumDefs);
        LOG.debug("{}: process enums returned {}", methodName, typeDefsForAPI.getEnumDefs());

        // Process entities
        List<AtlasEntityDef> entityDefs = atd.getEntityDefs();
        processAtlasEntityDefs(userId, entityDefs);
        LOG.debug("{}: process entities returned {}", methodName, typeDefsForAPI.getEntityDefs());

        // Process relationships
        List<AtlasRelationshipDef> relationshipDefs = atd.getRelationshipDefs();
        processAtlasRelationshipDefs(userId, relationshipDefs);
        LOG.debug("{}: process relationships returned {}", methodName, typeDefsForAPI.getRelationshipDefs());

        // Process classifications
        List<AtlasClassificationDef> classificationDefs = atd.getClassificationDefs();
        processAtlasClassificationDefs(userId, classificationDefs);
        LOG.debug("{}: process classifications returned {}", methodName, typeDefsForAPI.getClassificationDefs());

        // Structs are explicitly ignored

        //LOG.debug("convertAtlasTypeDefs: complete, typeDefsForAPI {}", typeDefsForAPI);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(atd={}", methodName, atd);
        }

    }


    // Convert a list of AtlasEnumDef to a typeDefsForAPI; each convertible typedef is added to the typeDefsForAPI member variable.
    //
    private void processAtlasEnumDefs(List<AtlasEnumDef> atlasEnumDefs)
    {

        final String methodName = "processAtlasEnumDefs";

        if (atlasEnumDefs != null) {
            for (AtlasEnumDef atlasEnumDef : atlasEnumDefs) {

                // On the load-all-types path we need to ensure that we only admit types that are known in the OM type system - ask the RCM

                String atlasTypeName = atlasEnumDef.getName();
                boolean typeKnown = false;
                AttributeTypeDef knownAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(metadataCollectionId, atlasTypeName);
                if (knownAttributeTypeDef != null) {
                    typeKnown = true;
                }
                if (typeKnown) {
                    try {
                        processAtlasEnumDef(atlasEnumDef);
                    } catch (TypeErrorException e) {
                        LOG.error("{}: gave up on AtlasEnumDef {}", methodName, atlasTypeName, e);
                        // swallow the exception to proceed with the next type...
                    }
                } else {
                    // this type is not known to the RCM - abandon it...
                    LOG.debug("{}: the type {} found in Atlas is not known in the ODPi type system", methodName, atlasTypeName);
                    // continue with the next type...
                }
            }
        }
    }


    // Convert a list of AtlasEntityDef saving each into the typeDefsForAPI
    //
    private void processAtlasEntityDefs(String               userId,
                                        List<AtlasEntityDef> atlasEntityDefs)
    {

        final String methodName = "processAtlasEntityDefs";

        if (atlasEntityDefs != null) {
            for (AtlasEntityDef atlasEntityDef : atlasEntityDefs) {

                // On the load-all-types path we need to ensure that we only admit types that are known in the OM type system - ask the RCM
                // Need to convert any OM_ F5 typenames to the corresponding raw F5 name - as these are types that we want to convert...

                boolean typeKnown = false;
                String atlasTypeName = atlasEntityDef.getName();
                if (FamousFive.atlasTypeRequiresSubstitution(atlasTypeName)) {
                    typeKnown = true; // the type is known in the RCM, but it knows it by the un-mapped name...
                } else { // Not a mapped (OM_) f5 name, so either not F5 or raw F5
                    TypeDef knownTypeDef = repositoryHelper.getTypeDefByName(metadataCollectionId, atlasTypeName);
                    if (knownTypeDef != null) {
                        typeKnown = true;
                    }
                }

                if (typeKnown) {

                    try {

                        processAtlasEntityDef(userId, atlasEntityDef);

                    } catch (TypeErrorException | RepositoryErrorException e) {
                        LOG.error("{}: gave up on AtlasEntityDef {}", methodName, atlasTypeName, e);
                        // swallow the exception to proceed with the next type...
                    }
                } else {
                    // this type is not known to the RCM - abandon it...
                    LOG.debug("{}: the type {} found in Atlas is not known in the ODPi type system", methodName, atlasTypeName);
                    // continue with the next type...
                }
            }
        }
    }

    // Convert a list of AtlasClassificationDef to a typeDefsForAPI
    //
    private void processAtlasClassificationDefs(String                       userId,
                                                List<AtlasClassificationDef> atlasClassificationDefs)
    {

        final String methodName = "processAtlasClassificationDefs";

        if (atlasClassificationDefs != null) {
            for (AtlasClassificationDef atlasClassificationDef : atlasClassificationDefs) {

                // On the load-all-types path we need to ensure that we only admit types that are known in the OM type system - ask the RCM

                String atlasTypeName = atlasClassificationDef.getName();
                boolean typeKnown = false;
                TypeDef knownTypeDef = repositoryHelper.getTypeDefByName(metadataCollectionId, atlasTypeName);
                if (knownTypeDef != null) {
                    typeKnown = true;
                }

                if (typeKnown) {

                    try {
                        processAtlasClassificationDef(userId, atlasClassificationDef);
                    } catch (TypeErrorException | RepositoryErrorException e) {
                        LOG.error("{}: gave up on AtlasClassificationDef {}", methodName, atlasTypeName, e);
                        // swallow the exception to proceed with the next type...
                    }
                } else {
                    // this type is not known to the RCM - abandon it...
                    LOG.debug("{}: the type {} found in Atlas is not known in the ODPi type system", methodName, atlasTypeName);
                    // continue with the next type...
                }

            }
        }
    }


    // Convert a list of AtlasRelationshipDef to a typeDefsForAPI
    //
    private void processAtlasRelationshipDefs(String                     userId,
                                              List<AtlasRelationshipDef> atlasRelationshipDefs)
    {

        final String methodName = "processAtlasRelationshipDefs";

        if (atlasRelationshipDefs != null) {
            for (AtlasRelationshipDef atlasRelationshipDef : atlasRelationshipDefs) {

                // On the load-all-types path we need to ensure that we only admit types that are known in the OM type system - ask the RCM

                String atlasTypeName = atlasRelationshipDef.getName();
                boolean typeKnown = false;
                TypeDef knownTypeDef = repositoryHelper.getTypeDefByName(metadataCollectionId, atlasTypeName);
                if (knownTypeDef != null) {
                    typeKnown = true;
                }

                if (typeKnown) {
                    try {
                        processAtlasRelationshipDef(userId, atlasRelationshipDef);
                    } catch (TypeErrorException | RepositoryErrorException e) {
                        LOG.error("{}: gave up on AtlasRelationshipDef {}", methodName, atlasTypeName, e);
                        // swallow the exception to proceed with the next type...
                    }
                } else {
                    // this type is not known to the RCM - abandon it...
                    LOG.debug("{}: the type {} found in Atlas is not known in the ODPi type system", methodName, atlasTypeName);
                    // continue with the next type...
                }

            }
        }
    }


    // Convert an AtlasEnumDef to an OM EnumDef
    //
    // 1. Convert the AtlasEnumDef into the corresponding OM EnumDef and (implicitly) validate the content of the OM EnumDef
    // 2. Query RCM to find whether it knows of an EnumDef with the same name:
    //    a. If exists, retrieve the known typedef from RCM and deep compare the known and new EnumDefs.
    //       i.  If same use the existing type's GUID; add def to TypeDefGallery;
    //       ii. If different fail (audit log)
    //    b. If not exists (in RCM), generate a GUID and add to TypeDefGallery.
    //
    // Error handling: If at any point we decide not to proceed with the conversion of this EnumDef, we must log
    // the details of the error condition and return to the caller without having added the OM EnumDef to typeDefsForAPI.
    //
    // package private
    void processAtlasEnumDef(AtlasEnumDef atlasEnumDef)
            throws
            TypeErrorException
    {

        final String methodName = "processAtlasEnumDef";

        LOG.debug("{}: convert AtlasEnumDef {}", methodName, atlasEnumDef);

        if (atlasEnumDef == null) {
            return;
        }

        String typeName = atlasEnumDef.getName();

        // 1. Convert Atlas type into a valid OM type

        // Allocate OMRS EnumDef, which will set AttributeTypeDefCategory automatically
        EnumDef omrsEnumDef = new EnumDef();

        // Set common fields
        omrsEnumDef.setGUID(atlasEnumDef.getGuid());
        omrsEnumDef.setName(atlasEnumDef.getName());
        omrsEnumDef.setDescription(atlasEnumDef.getDescription());
        long initialVersion = 1;
        omrsEnumDef.setVersion(initialVersion);
        omrsEnumDef.setVersionName(Long.toString(initialVersion));

        // Additional fields on an AtlasEnumDef and OM EnumDef are elementDefs and defaultValue
        // Initialize the Atlas and OMRS default values so we can test and set default value in the loop
        String atlasDefaultValue = atlasEnumDef.getDefaultValue();
        EnumElementDef omrsDefaultValue = null;
        ArrayList<EnumElementDef> omrsElementDefs = new ArrayList<>();
        List<AtlasEnumDef.AtlasEnumElementDef> atlasElemDefs = atlasEnumDef.getElementDefs();
        if (atlasElemDefs != null) {
            for (AtlasEnumDef.AtlasEnumElementDef atlasElementDef : atlasElemDefs) {
                EnumElementDef omrsEnumElementDef = new EnumElementDef();
                omrsEnumElementDef.setValue(atlasElementDef.getValue());
                omrsEnumElementDef.setDescription(atlasElementDef.getDescription());
                omrsEnumElementDef.setOrdinal(atlasElementDef.getOrdinal());
                omrsElementDefs.add(omrsEnumElementDef);
                if (atlasElementDef.getValue().equals(atlasDefaultValue)) {
                    omrsDefaultValue = omrsEnumElementDef;
                }
            }
        }
        omrsEnumDef.setElementDefs(omrsElementDefs);
        omrsEnumDef.setDefaultValue(omrsDefaultValue);

        // 2. Query RepositoryContentManager to find whether it knows of an EnumDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.

        // Ask RepositoryContentManager whether there is an EnumDef with same name as typeName
        String source = metadataCollectionId;
        AttributeTypeDef existingAttributeTypeDef;
        try {
            existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, typeName);

        } catch (OMRSLogicErrorException e) {
            // Fail the conversion - this can be achieved by simply returning before completion
            LOG.error("{}: caught exception from RepositoryHelper", methodName, e);
            return;
        }

        if (existingAttributeTypeDef == null) {
            LOG.debug("{}: repository content manager returned name not found - proceed to publish", methodName);
            // Use the candidate attribute type def
            // Check (by name) whether we have already added one to current TDBC - e.g. if there are
            // multiple attributes of the same type - they should refer to the same ATD in TDBC.

            // No existing ATD was found in RH.
            // If it does not already exist add the new ATD to the ATDs in the TypeDefGallery and use it in TDA.
            // If there is already a TDBC copy of the ATD then use that - avoid duplication.
            EnumDef tdbcCopy = typeDefsForAPI.getEnumDef(omrsEnumDef.getName());
            if (tdbcCopy != null)
                omrsEnumDef = tdbcCopy;
            // Add the OM typedef to TDBC - this will get it into the TypeDefGallery
            LOG.debug("{}: add OM EnumDef to typeDefsForAPIed - {}", methodName, omrsEnumDef);
            typeDefsForAPI.addEnumDef(omrsEnumDef);
            // In future may want to generate a descriptionGUID here, but for now these are not used
            omrsEnumDef.setDescriptionGUID(null);

        } else {

            LOG.debug("{}: repositoryHelper has an AttributeTypeDef with name {} : {}", methodName, typeName, existingAttributeTypeDef);
            if (existingAttributeTypeDef.getCategory() == ENUM_DEF) {
                // There is an EnumDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                EnumDef existingEnumDef = (EnumDef) existingAttributeTypeDef;
                boolean typematch = comp.compare(existingEnumDef, omrsEnumDef);
                // If compare matches then we can proceed to publish the def
                if (typematch) {
                    // There is exact match in the ReposHelper - we will add that to our TypeDefGallery
                    omrsEnumDef = existingEnumDef;
                    // Add the OM typedef to TDBC - this will get it into the TypeDefGallery
                    typeDefsForAPI.addEnumDef(omrsEnumDef);
                } else {
                    // If compare failed abandon processing of this EnumDef
                    LOG.debug("{}: existing AttributeTypeDef did not match", methodName);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(typeName, omrsEnumDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omrsEnumDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

            } else {
                // There is a type of this name but it is not an EnumDef - fail!
                LOG.debug("{}: existing AttributeTypeDef not an EnumDef - has category {} ", methodName, existingAttributeTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, omrsEnumDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omrsEnumDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }


    }


    // Convert an AtlasClassificationDef to an OMRS ClassificationDef
    //
    // 1. Convert the AtlasClassificationDef into the corresponding OM ClassificationDef and (implicitly) validate the content of the OM ClassificationDef
    // 2. Query RCM to find whether it knows of an ClassificationDef with the same name:
    //    a. If exists, retrieve the known typedef from RCM and deep compare the known and new ClassificationDefs.
    //       i.  If same use the existing type's GUID; add def to TypeDefGallery;
    //       ii. If different fail (audit log)
    //    b. If not exists (in RCM), generate a GUID and add to TypeDefGallery.
    //
    // Error handling: If at any point we decide not to proceed with the conversion of this ClassificationDef, we must log
    // the details of the error condition and return to the caller without having added the omrsClassificationDef to typeDefsForAPI.
    //
    private void processAtlasClassificationDef(String                 userId,
                                               AtlasClassificationDef atlasClassificationDef)
            throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "processAtlasClassificationDef";

        LOG.debug("processAtlasClassificationDef: AtlasClassificationDef {}", atlasClassificationDef);

        String typeName = atlasClassificationDef.getName();

        // Create an AtlasClassificationDefMapper to convert to an OM ClassificationDef, then invoke the RH to verify
        // whether a type with the same name is already known, and if it compares.
        // Finally add the ClassificationDef to the TDBC typeDefsForAPI.

        ClassificationDef classificationDef;
        AtlasClassificationDefMapper atlasClassificationDefMapper;
        try {
            atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, atlasClassificationDef);
            classificationDef = atlasClassificationDefMapper.toOMClassificationDef();
        } catch (RepositoryErrorException e) {
            LOG.error("{}: could not initialise mapper", methodName, e);
            throw e;
        } catch (TypeErrorException e) {
            LOG.error("{}: could not convert AtlasEntityDef {} to OM EntityDef", methodName, typeName, e);
            throw e;
        }

        if (classificationDef == null) {
            LOG.error("{}: could not convert AtlasClassificationDef {} to OM ClassificationDef", methodName, typeName);
            return;
        }

        LOG.debug("{}: AtlasClassificationDef mapped to OM ClassificationDef {}", methodName, classificationDef);

        // Query RepositoryContentManager to find whether it knows of a TypeDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.

        // Error handling:
        // If at any point we decide not to proceed with the conversion of this ClassificationDef, we must log
        // the details of the error condition and return to the caller without having added the omrsClassificationDef to typeDefsForAPI.

        // Ask RepositoryContentManager whether there is a known TypeDef with same name
        String source = metadataCollectionId;
        TypeDef existingTypeDef;
        try {
            existingTypeDef = repositoryHelper.getTypeDefByName(source, typeName);
        } catch (OMRSLogicErrorException e) {
            // Fail the conversion by returning without adding the type def to the TDBC
            LOG.error("{}: caught exception from RepositoryHelper", methodName, e);
            return;
        }

        if (existingTypeDef == null) {
            LOG.debug("{}: repository content manager returned name not found - proceed to publish", methodName);
            // In future may want to generate a descriptionGUID, but for now these are not used
            classificationDef.setDescriptionGUID(null);
        } else {
            LOG.debug("{}: there is a TypeDef with name {} : {}", methodName, typeName, existingTypeDef);

            if (existingTypeDef.getCategory() == CLASSIFICATION_DEF) {
                LOG.debug("{}: existing TypeDef has category {} ", methodName, existingTypeDef.getCategory());
                // There is a ClassificationDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                ClassificationDef existingClassificationDef = (ClassificationDef) existingTypeDef;
                boolean typematch = comp.equivalent(existingClassificationDef, classificationDef);
                // If compare matches use the known type
                if (typematch) {
                    // We will add the typedef to the TypeDefGallery
                    LOG.debug("{}: repository content manager found matching def with name {}", methodName, typeName);
                    classificationDef = existingClassificationDef;
                } else {
                    // If compare failed abandon processing of this ClassificationDef
                    LOG.debug("{}: existing TypeDef did not match", methodName);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(classificationDef.getName(), classificationDef.getGUID(), "classificationDef", methodName, repositoryName, classificationDef.toString());


                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            } else {
                // There is a type of this name but it is not a ClassificationDef - fail!
                LOG.debug("{}: existing TypeDef not a ClassificationDef - has category {} ", methodName, existingTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(classificationDef.getName(), classificationDef.getGUID(), "classificationDef", methodName, repositoryName, classificationDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        }

        // If we reached this point then we are good to go
        // Add the OM typedef to discoveredTypeDefs - this will get it into the TypeDefGallery
        LOG.debug("{}: OMRS ClassificationDef {}", methodName, classificationDef);
        // Add the OM typedef to TDBC - this will ultimately get it into the TypeDefGallery
        typeDefsForAPI.addClassificationDef(classificationDef);

    }


    private void processAtlasEntityDef(String         userId,
                                       AtlasEntityDef atlasEntityDef)
            throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "processAtlasEntityDef";
        String source           = metadataCollectionId;

        // Create an AtlasEntityDefMapper to convert to an OM EntityDef, then invoke the RH to verify
        // whether a type with the same name is already known, and if it compares.
        // Finally add the EntityDef to the TDBC typeDefsForAPI.

        String typeName = atlasEntityDef.getName();

        // Famous Five
        // Here we need to ensure that we do not process an Atlas Famous Five type
        // This may look odd because we use the omXX query method - this is because we need to to know whether,
        // if the Atlas type name were an OM type name, would it have been substituted?


        if (FamousFive.omTypeRequiresSubstitution(typeName)) {
            LOG.debug("{}: skip type {}", methodName, typeName);
            return;
        }

        LOG.debug("{}: call the mapper with typeName {}", methodName, typeName);

        AtlasEntityDefMapper atlasEntityDefMapper;
        EntityDef entityDef;
        try {
            atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, atlasEntityDef);
            entityDef = atlasEntityDefMapper.toOMEntityDef();
        } catch (RepositoryErrorException e) {
            LOG.error("{}: could not initialize mapper", methodName, e);
            throw e;
        } catch (TypeErrorException e) {
            LOG.error("{}: could not convert AtlasEntityDef {} to OM EntityDef", methodName, typeName, e);
            throw e;
        }

        if (entityDef == null) {
            LOG.error("{}: no OM EntityDef for type {}", methodName, typeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("entityDef", "load", metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("{}: AtlasEntityDef mapped to OM EntityDef {}", methodName, entityDef);

        // Validate the entity def
        // Check that if there is a supertype then it exists, either in the RCM or failing that in the typeDefsForAPI (if gallery is under construction). If
        // supertype is not found in either place then fail here - because it implies that the supertype was not converted.
        boolean superTypeKnown = false;
        TypeDefLink superTypeLink = entityDef.getSuperType();
        if (superTypeLink != null) {
            TypeDef existingSuperTypeDef = repositoryHelper.getTypeDefByName(source, superTypeLink.getName());
            if (existingSuperTypeDef != null) {
                superTypeKnown = true;
            } else {
                // Look in the typeDefsForAPI as gallery may be under construction...
                existingSuperTypeDef = typeDefsForAPI.getEntityDef(superTypeLink.getName());
                if (existingSuperTypeDef != null) {
                    superTypeKnown = true;
                }
            }
            if (!superTypeKnown) {
                // The named supertype is not know in either the RCM or in the typeDefsForPAI - fail the entity def.
                // If compare failed abandon processing of this EntityDef
                LOG.debug("{}: type name {} refers to a supertype {} that is not known in OM ", methodName, entityDef.getName(), superTypeLink.getName());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityDef.getName(), entityDef.getGUID(), "atlasEntityDef", methodName, repositoryName, entityDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }


        // Query RepositoryContentManager to find whether it knows of a TypeDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.
        //
        // Error handling:
        // If at any point we decide not to proceed with the conversion of this EntityDef, we must log
        // the details of the error condition and return to the caller without having added the omEntityDef to typeDefsForAPI.

        // Ask RepositoryContentManager whether there is a known TypeDef with same name
        TypeDef existingTypeDef;
        try {
            existingTypeDef = repositoryHelper.getTypeDefByName(source, entityDef.getName());
        } catch (OMRSLogicErrorException e) {
            // Fail the conversion by returning without adding the type def to the TDBC
            LOG.error("{}: caught exception from RepositoryHelper", methodName, e);
            return;
        }

        if (existingTypeDef == null) {
            LOG.debug("{}: repository content manager returned name not found - proceed to publish", methodName);
            // In future may want to generate a descriptionGUID, but for now these are not used
            entityDef.setDescriptionGUID(null);
        } else {
            LOG.debug("{}: there is a TypeDef with name {} : {}", methodName, entityDef.getName(), existingTypeDef);

            if (existingTypeDef.getCategory() == ENTITY_DEF) {
                LOG.debug("{}: existing TypeDef has category {} ", methodName, existingTypeDef.getCategory());
                // There is a EntityDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                EntityDef existingEntityDef = (EntityDef) existingTypeDef;
                boolean typematch = comp.equivalent(existingEntityDef, entityDef);
                // If compare matches use the known type
                if (typematch) {
                    // We will add the typedef to the TypeDefGallery
                    LOG.debug("{}: repository content manager found matching def with name {}", methodName, entityDef.getName());
                    entityDef = existingEntityDef;
                } else {
                    // If compare failed abandon processing of this EntityDef
                    LOG.debug("{}: existing TypeDef did not match", methodName);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(entityDef.getName(), entityDef.getGUID(), "atlasEntityDef", methodName, repositoryName, entityDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            } else {
                // There is a type of this name but it is not an EntityDef - fail!
                LOG.debug("{}: existing TypeDef not an EntityDef - has category {} ", methodName, existingTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityDef.getName(), entityDef.getGUID(), "atlasEntityDef", methodName, repositoryName, entityDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        // Add the OM typedef to TDBC - this will ultimately get it into the TypeDefGallery
        LOG.debug("{}: adding entityDef to typeDefsForAPI {}", methodName, entityDef);
        typeDefsForAPI.addEntityDef(entityDef);

    }


    private void processAtlasRelationshipDef(String               userId,
                                             AtlasRelationshipDef atlasRelationshipDef)
            throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "processAtlasRelationshipDef";

        // Create an AtlasRelationshipDefMapper to convert to an OM RelationshipDef, then invoke the RH to verify
        // whether a type with the same name is already known, and if it compares.
        // Finally add the EntityDef to the TDBC typeDefsForAPI.

        String typeName = atlasRelationshipDef.getName();

        RelationshipDef relationshipDef;
        try {
            AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, atlasRelationshipDef);
            relationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
        } catch (RepositoryErrorException e) {
            LOG.error("{}: could not initialize mapper", methodName, e);
            throw e;
        } catch (TypeErrorException e) {
            LOG.error("{}: could not convert AtlasEntityDef {} to OM EntityDef", methodName, typeName, e);
            throw e;
        }

        if (relationshipDef == null) {
            LOG.error("{}: could not convert AtlasRelationshipDef {} to OM RelationshipDef", methodName, typeName);
            return;
        }

        LOG.debug("{}: AtlasRelationshipDef mapped to OM EntityDef {}", methodName, relationshipDef);


        // Query RepositoryContentManager to find whether it knows of a RelationshipDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.

        // Ask RepositoryContentManager whether there is a RelationshipDef with same name as typeName

        String source = metadataCollectionId;
        TypeDef existingTypeDef;
        try {
            existingTypeDef = repositoryHelper.getTypeDefByName(source, typeName);
        } catch (OMRSLogicErrorException e) {
            // Fail the conversion by returning without adding the def to the TypeDefGallery
            LOG.error("{}: caught exception from RepositoryHelper", methodName, e);
            return;
        }
        if (existingTypeDef == null) {
            LOG.debug("{}: repository content manager returned name not found - proceed to publish", methodName);
            // In future may want to generate a descriptionGUID, but for now these are not used
            relationshipDef.setDescriptionGUID(null);
        } else {
            LOG.debug("{}: there is a TypeDef with name {} : {}", methodName, typeName, existingTypeDef);

            if (existingTypeDef.getCategory() == RELATIONSHIP_DEF) {
                // There is a RelationshipDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                RelationshipDef existingRelationshipDef = (RelationshipDef) existingTypeDef;
                boolean typematch = comp.equivalent(existingRelationshipDef, relationshipDef);
                // If compare matches use the known type
                if (typematch) {
                    // We will add the typedef to the TypeDefGallery
                    LOG.debug("{}: repository content manager found matching def with name {}", methodName, typeName);
                    relationshipDef = existingRelationshipDef;
                } else {

                    // If compare failed abandon processing of this RelationshipDef
                    LOG.debug("{}: existing TypeDef did not match", methodName);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(relationshipDef.getName(), relationshipDef.getGUID(), "atlasRelationshipDef", methodName, repositoryName, relationshipDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            } else {
                // There is a type of this name but it is not a RelationshipDef - fail!
                LOG.error("{}: existing TypeDef not an RelationshipDef - has category {} ", methodName, existingTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipDef.getName(), relationshipDef.getGUID(), "atlasRelationshipDef", methodName, repositoryName, relationshipDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
        }

        // If we reached this point then we are good to go
        // Add the OM typedef to discoveredTypeDefs - this will get it into the TypeDefGallery
        LOG.debug("{}: OMRS RelationshipDef {}", methodName, relationshipDef);
        typeDefsForAPI.addRelationshipDef(relationshipDef);

    }


    // Convert a List of OMRS TypeDefAttribute to a List of AtlasAttributeDef
    private ArrayList<AtlasStructDef.AtlasAttributeDef> convertOMAttributeDefs(List<TypeDefAttribute> omAttrs)
    {
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttributes;
        if (omAttrs == null || omAttrs.isEmpty()) {
            return null;
        } else {
            atlasAttributes = new ArrayList<>();

            for (TypeDefAttribute tda : omAttrs) {
                // Map from OMRS TypeDefAttribute to AtlasAttributeDef
                AtlasStructDef.AtlasAttributeDef aad = convertOMAttributeDef(tda);
                atlasAttributes.add(aad);
            }
        }
        return atlasAttributes;

    }


    private AtlasStructDef.AtlasAttributeDef convertOMAttributeDef(TypeDefAttribute tda)
    {

        final String methodName = "convertOMAttributeDef";

        // Convert OM TypeDefAttribute to AtlasAttributeDef
        //

        //
        // The AttributeTypeDef attributeType has:
        // AttributeTypeDefCategory category:
        //   OM defines the following values { UNKNOWN_DEF | PRIMITIVE | COLLECTION | ENUM_DEF }
        //   The corresponding Atlas TypeCategory in each case is:
        //     [OM]               [Atlas]
        //   UNKNOWN_DEF         error condition
        //   PRIMITIVE           PRIMITIVE
        //   COLLECTION          ARRAY or MAP - we can tell which by looking in the (subclass) CollectionDef CollectionDefCategory
        //                                    - which will be one of:
        //                                      OM_COLLECTION_UNKNOWN - treat as an error condition
        //                                      OM_COLLECTION_MAP     - convert to Atlas MAP
        //                                      OM_COLLECTION_ARRAY   - convert to Atlas ARRAY
        //                                      OM_COLLECTION_STRUCT  - treat as an error condition
        //   ENUM_DEF            ENUM
        //   OM guid is ignored - there is no GUID on AtlasAttributeDef
        //   OM name  -> used for AtlasAttributeDef.typeName
        //
        // The OM TypeDefAttribute has some fields that can be copied directly to the AtlasAttributeDef:
        //    [OM]                      [Atlas]
        // attributeName         ->   String name
        // valuesMinCount        ->   int valuesMinCount
        // valuesMaxCount        ->   int valuesMaxCount
        // isIndexable           ->   boolean isIndexable
        // isUnique              ->   boolean isUnique
        // defaultValue          ->   String defaultValue
        // attributeDescription  ->   String description
        //
        // OM attributeDescription --> AtlasAttributeDef.description
        //
        // Map OM Cardinality ----> combination of Atlas boolean isOptional & Cardinality cardinality  - use mapping function
        //   OM                       ->    ATLAS
        //   UNKNOWN                  ->    treat as an error condition
        //   AT_MOST_ONE              ->    isOptional && SINGLE
        //   ONE_ONLY                 ->    !isOptional && SINGLE
        //   AT_LEAST_ONE_ORDERED     ->    !isOptional && LIST
        //   AT_LEAST_ONE_UNORDERED   ->    !isOptional && SET
        //   ANY_NUMBER_ORDERED       ->    isOptional && LIST
        //   ANY_NUMBER_UNORDERED     ->    isOptional && SET

        // There are no constraints in OM so no Atlas constraints are set  List<AtlasConstraintDef> null
        // OM externalStandardMappings is ignored
        //
        LOG.debug("{}: OMAttributeDef is {}", methodName, tda);

        if (tda == null) {
            return null;
        }

        AtlasStructDef.AtlasAttributeDef aad = null;

        // We need to set the Atlas aad depending on what category of OM typedef we are converting.
        // If the OM def is a primitive or collection then there is no actual Atlas type to create;
        // we are just looking to set the Atlas typeName to one of the primitive type names or to
        // array<x> or map<x,y> as appropriate.
        // If the OM def is an EnumDef then we need to create an AtlasEnumDef and set the typename to
        // refer to it.
        AttributeTypeDef atd = tda.getAttributeType();
        AttributeTypeDefCategory category = atd.getCategory();
        switch (category) {
            case PRIMITIVE:
                aad = convertOMPrimitiveDef(tda);
                break;
            case COLLECTION:
                aad = convertOMCollectionDef(tda);
                break;
            case ENUM_DEF:
                // This is handled by setting the typeName to that of an AtlasEnumDef
                // created by an earlier call to addAttributeTypeDef. If this has not
                // been done then it is valid to bounce this call as an error condition.
                // OM has a TDA with an ATD that has an ATDCategory of ENUM_DEF
                // We want an AAD that uses the typeName of the AtlasEnumDef set to the ATD.name
                aad = convertOMEnumDefToAtlasAttributeDef(tda);
                break;
            case UNKNOWN_DEF:
                LOG.debug("{}: cannot convert OM attribute type def with category {}", methodName, category);
                break;
        }

        return aad;
    }


    private AtlasStructDef.AtlasAttributeDef convertOMPrimitiveDef(TypeDefAttribute tda)
    {


        if (tda == null) {
            return null;
        }
        AtlasStructDef.AtlasAttributeDef aad = new AtlasStructDef.AtlasAttributeDef();
        aad.setName(tda.getAttributeName());
        aad.setValuesMinCount(tda.getValuesMinCount());
        aad.setValuesMaxCount(tda.getValuesMaxCount());
        aad.setIsIndexable(tda.isIndexable());
        aad.setIsUnique(tda.isUnique());
        aad.setDefaultValue(tda.getDefaultValue());
        aad.setDescription(tda.getAttributeDescription());
        // Currently setting Atlas cardinality and optionality using a pair of method calls.
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = convertOMCardinalityToAtlasCardinality(tda.getAttributeCardinality());
        aad.setCardinality(atlasCardinality);
        boolean atlasOptionality = convertOMCardinalityToAtlasOptionality(tda.getAttributeCardinality());
        aad.setIsOptional(atlasOptionality);
        aad.setConstraints(null);
        AttributeTypeDef atd = tda.getAttributeType();
        PrimitiveDef omPrimDef = (PrimitiveDef) atd;
        PrimitiveDefCategory primDefCat = omPrimDef.getPrimitiveDefCategory();
        aad.setTypeName(primDefCat.getName());
        return aad;
    }


    private AtlasStructDef.AtlasAttributeDef convertOMCollectionDef(TypeDefAttribute tda)
    {

        final String methodName = "convertOMCollectionDef";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==>{}: TypeDefAttribute {}", methodName, tda);
        }
        if (tda == null) {
            return null;
        }
        AtlasStructDef.AtlasAttributeDef aad = new AtlasStructDef.AtlasAttributeDef();
        aad.setName(tda.getAttributeName());
        LOG.debug("==>{}: attribute name {}", methodName, tda.getAttributeName());
        aad.setValuesMinCount(tda.getValuesMinCount());
        aad.setValuesMaxCount(tda.getValuesMaxCount());
        aad.setIsIndexable(tda.isIndexable());
        aad.setIsUnique(tda.isUnique());
        aad.setDefaultValue(tda.getDefaultValue());
        aad.setDescription(tda.getAttributeDescription());
        // Currently setting Atlas cardinality and optionality using a pair of method calls.
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = convertOMCardinalityToAtlasCardinality(tda.getAttributeCardinality());
        aad.setCardinality(atlasCardinality);
        boolean atlasOptionality = convertOMCardinalityToAtlasOptionality(tda.getAttributeCardinality());
        aad.setIsOptional(atlasOptionality);
        aad.setConstraints(null);
        AttributeTypeDef atd = tda.getAttributeType();
        CollectionDef omCollDef = (CollectionDef) atd;
        String collectionTypeName = omCollDef.getName();
        LOG.debug("==>{}: collection type name {}", methodName, collectionTypeName);
        CollectionDefCategory collDefCat = omCollDef.getCollectionDefCategory();
        String atlasTypeName;
        switch (collDefCat) {
            case OM_COLLECTION_ARRAY:
                String OM_ARRAY_PREFIX = "array<";
                String OM_ARRAY_SUFFIX = ">";
                int arrayStartIdx = OM_ARRAY_PREFIX.length();
                int arrayEndIdx = collectionTypeName.length() - OM_ARRAY_SUFFIX.length();
                String elementTypeName = collectionTypeName.substring(arrayStartIdx, arrayEndIdx);
                LOG.debug("{}: handling an OM array of elements of type {}", methodName, elementTypeName);
                atlasTypeName = ATLAS_TYPE_ARRAY_PREFIX + elementTypeName + ATLAS_TYPE_ARRAY_SUFFIX;
                break;
            case OM_COLLECTION_MAP:
                String OM_MAP_PREFIX = "map<";
                String OM_MAP_SUFFIX = ">";
                int mapStartIdx = OM_MAP_PREFIX.length();
                int mapEndIdx = collectionTypeName.length() - OM_MAP_SUFFIX.length();
                String kvTypeString = collectionTypeName.substring(mapStartIdx, mapEndIdx);
                String[] parts = kvTypeString.split(",");
                String keyType = parts[0];
                String valType = parts[1];
                atlasTypeName = ATLAS_TYPE_MAP_PREFIX + keyType + ATLAS_TYPE_MAP_KEY_VAL_SEP + valType + ATLAS_TYPE_MAP_SUFFIX;
                LOG.debug("{}: atlas type name is {}", methodName, atlasTypeName);
                break;
            default:
                LOG.debug("{}: cannot convert a collection def with category {}", methodName, collDefCat);
                return null;
        }
        aad.setTypeName(atlasTypeName);
        return aad;
    }

    /*
     * Utility functions to convert OM cardinality to Atlas cardinality and optionality
     */

    private AtlasStructDef.AtlasAttributeDef.Cardinality convertOMCardinalityToAtlasCardinality(AttributeCardinality omCard)
    {
        final String methodName = "convertOMCardinalityToAtlasCardinality";

        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCard;
        switch (omCard) {
            case AT_MOST_ONE:
            case ONE_ONLY:
                atlasCard = SINGLE;
                break;
            case AT_LEAST_ONE_ORDERED:
            case ANY_NUMBER_ORDERED:
                atlasCard = LIST;
                break;
            case AT_LEAST_ONE_UNORDERED:
            case ANY_NUMBER_UNORDERED:
                atlasCard = SET;
                break;
            case UNKNOWN:
            default:
                LOG.info("{}: not specified by caller, defaulting to SINGLE", methodName);
                // There is no 'good' choice for return code here but default to single...
                atlasCard = SINGLE;
                break;
        }
        return atlasCard;
    }

    private boolean convertOMCardinalityToAtlasOptionality(AttributeCardinality omCard)
    {
        final String methodName = "convertOMCardinalityToAtlasOptionality";

        switch (omCard) {
            case AT_MOST_ONE:
            case ANY_NUMBER_ORDERED:
            case ANY_NUMBER_UNORDERED:
                return true;
            case ONE_ONLY:
            case AT_LEAST_ONE_ORDERED:
            case AT_LEAST_ONE_UNORDERED:
                return false;
            case UNKNOWN:
            default:
                LOG.info("{}: not specified by caller, defaulting to TRUE", methodName);
                // There is no 'good' choice for return code here - but default to optional
                return true;
        }
    }

    /**
     * Method to convert an OM EnumDef into an AtlasAttributeDef - they are handled differently by the two type systems
     *
     * @param tda - the TypeDefAttribute to be converted
     * @return - AtlasStructDef resulting from conversion of the OM EnumDef
     */
    private AtlasStructDef.AtlasAttributeDef convertOMEnumDefToAtlasAttributeDef(TypeDefAttribute tda)
    {

        if (tda == null) {
            return null;
        }
        AtlasStructDef.AtlasAttributeDef aad = new AtlasStructDef.AtlasAttributeDef();
        aad.setName(tda.getAttributeName());
        aad.setValuesMinCount(tda.getValuesMinCount());
        aad.setValuesMaxCount(tda.getValuesMaxCount());
        aad.setIsIndexable(tda.isIndexable());
        aad.setIsUnique(tda.isUnique());
        aad.setDefaultValue(tda.getDefaultValue());
        aad.setDescription(tda.getAttributeDescription());
        // Currently setting Atlas cardinality and optionality using a pair of method calls.
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = convertOMCardinalityToAtlasCardinality(tda.getAttributeCardinality());
        aad.setCardinality(atlasCardinality);
        boolean atlasOptionality = convertOMCardinalityToAtlasOptionality(tda.getAttributeCardinality());
        aad.setIsOptional(atlasOptionality);
        aad.setConstraints(null);

        AttributeTypeDef atd = tda.getAttributeType();
        String atlasTypeName = atd.getName();
        aad.setTypeName(atlasTypeName);
        return aad;

    }


    // This class loads all the Atlas TypeDefs into the typeDefsCache
    private void loadAtlasTypeDefs(String userId)
            throws
            RepositoryErrorException
    {

        final String methodName = "loadAtlasTypeDefs";

        // Retrieve the typedefs from Atlas, and return a TypeDefGallery that contains two lists, each sorted by category
        // as follows:
        // TypeDefGallery.attributeTypeDefs contains:
        // 1. PrimitiveDefs
        // 2. CollectionDefs
        // 3. EnumDefs
        // TypeDefGallery.newTypeDefs contains:
        // 1. EntityDefs
        // 2. RelationshipDefs
        // 3. ClassificationDefs

        // The result of the load is constructed in typeDefsForAPI - a copy of which is made in the typeDefsCache.

        // Strategy: use searchTypesDef with a null (default) SearchFilter.
        SearchFilter emptySearchFilter = new SearchFilter();
        AtlasTypesDef atd;
        try {

            atd = typeDefStore.searchTypesDef(emptySearchFilter);

        } catch (AtlasBaseException e) {

            LOG.error("{}: caught exception from Atlas searchTypesDef", methodName, e);

            // This is pretty serious - if Atlas cannot retrieve any types we are in trouble...
            OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(repositoryName, methodName, e.getMessage());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // At this point we have a potential mixture of Atlas types - some of which can be modelled in OM and others that cannot.
        // Those that cannot be modelled in OM are filtered out in the following conversion.

        // Parse the Atlas TypesDef
        // Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
        // and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
        // is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
        // supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
        // things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
        // anything that contains something that it does not understand (e.g. a reference attribute or a collection that
        // contains anything other than primitives).

        // This method will populate the typeDefsForAPI object.
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }

    }


    private AtlasRelationshipDef.PropagateTags convertOMPropagationRuleToAtlasPropagateTags(ClassificationPropagationRule omPropRule)
            throws
            TypeErrorException
    {
        final String methodName = "convertOMPropagationRuleToAtlasPropagateTags";

        AtlasRelationshipDef.PropagateTags atlasPropTags;
        switch (omPropRule) {
            case NONE:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.NONE;
                break;
            case ONE_TO_TWO:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
                break;
            case TWO_TO_ONE:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
                break;
            case BOTH:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.BOTH;
                break;
            default:
                LOG.error("{}: could not convert OM propagation rule {}", methodName, omPropRule);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PROPAGATION_RULE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omPropRule.toString(), methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }
        return atlasPropTags;
    }

    // Utility method to convert from OM properties to Atlas attributes map
    //
    private Map<String, Object> convertOMPropertiesToAtlasAttributes(InstanceProperties instanceProperties) throws RepositoryErrorException
    {

        final String methodName = "convertOMPropertiesToAtlasAttributes";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(instanceProperties={})", methodName, instanceProperties);
        }
        Map<String, Object> atlasAttrs = null;

        if (instanceProperties != null) {
            Iterator<String> propNames = instanceProperties.getPropertyNames();
            if (propNames.hasNext()) {
                atlasAttrs = new HashMap<>();
                while (propNames.hasNext()) {
                    // Create an atlas attribute for this property
                    String propName = propNames.next();
                    // Retrieve the IPV - this will actually be of a concrete class such as PrimitivePropertyValue....
                    InstancePropertyValue ipv = instanceProperties.getPropertyValue(propName);
                    InstancePropertyCategory ipvCat = ipv.getInstancePropertyCategory();
                    switch (ipvCat) {

                        case PRIMITIVE:
                            LOG.debug("{}: handling primitive value ipv={}", methodName, ipv);
                            PrimitivePropertyValue primitivePropertyValue = (PrimitivePropertyValue) ipv;
                            Object primValue = primitivePropertyValue.getPrimitiveValue();
                            atlasAttrs.put(propName, primValue);
                            break;

                        case ARRAY:
                            // Get the ArrayPropertyValue and then use getArrayCount and getArrayValues to get the length and an
                            // InstanceProperties object. You can then use the map contained in the InstanceProperties to construct
                            // and Array object that you can set as the value for Atlas...
                            ArrayPropertyValue apv = (ArrayPropertyValue) ipv;
                            int arrayLen = apv.getArrayCount();
                            ArrayList<Object> atlasArray = null;
                            if (arrayLen > 0) {
                                atlasArray = new ArrayList<>();
                                InstanceProperties arrayProperties = apv.getArrayValues();
                                Iterator<String> keys = arrayProperties.getPropertyNames();
                                while (keys.hasNext()) {
                                    String key = keys.next();
                                    InstancePropertyValue val = arrayProperties.getPropertyValue(key);
                                    if (val.getInstancePropertyCategory() == InstancePropertyCategory.PRIMITIVE) {
                                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) val;
                                        Object atlasValue = ppv.getPrimitiveValue();
                                        atlasArray.add(atlasValue);
                                    }
                                    else {
                                        // Cannot handle non-primitive array values...
                                        LOG.error("{}: cannot handle array property, name {}, with non-primitive value for key {}", methodName, propName, key);

                                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ARRAY_VALUE_NOT_PRIMITIVE;
                                        String errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(propName, methodName, repositoryName);

                                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());

                                    }
                                }
                            }
                            atlasAttrs.put(propName, atlasArray);
                            break;

                        case MAP:
                            // Get the MapPropertyValue and then use getMapElementCount and getMapValues to get the length and an
                            // InstanceProperties object. You can then use the map contained in the InstanceProperties to construct
                            // a Map object that you can set as the value for Atlas...
                            MapPropertyValue mpv = (MapPropertyValue) ipv;
                            int mapLen = mpv.getMapElementCount();
                            HashMap<String, Object> atlasMap = null;
                            if (mapLen > 0) {
                                atlasMap = new HashMap<>();
                                InstanceProperties mapProperties = mpv.getMapValues();
                                Iterator<String> keys = mapProperties.getPropertyNames();
                                while (keys.hasNext()) {
                                    String key = keys.next();
                                    InstancePropertyValue val = mapProperties.getPropertyValue(key);
                                    if (val.getInstancePropertyCategory() == InstancePropertyCategory.PRIMITIVE) {
                                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) val;
                                        Object atlasValue = ppv.getPrimitiveValue();
                                        atlasMap.put(key, atlasValue);
                                    }
                                    else {
                                        // Cannot handle non-primitive map values...
                                        LOG.error("{}: cannot handle map property, name {}, with non-primitive value for key {}", methodName, propName, key);

                                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.MAP_VALUE_NOT_PRIMITIVE;
                                        String errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(propName, key, methodName, repositoryName);

                                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());

                                    }

                                }
                            }
                            atlasAttrs.put(propName, atlasMap);
                            break;

                        case ENUM:
                            EnumPropertyValue enumPropertyValue = (EnumPropertyValue) ipv;
                            Object enumValue = enumPropertyValue.getSymbolicName();
                            atlasAttrs.put(propName, enumValue);
                            break;

                        case STRUCT:
                        case UNKNOWN:
                        default:
                            LOG.debug("{}: Unsupported attribute {} ignored", methodName, propName);
                            break;
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(atlasAttrs={})", methodName, atlasAttrs);
        }
        return atlasAttrs;
    }

    /*
     * Convenience method to translate an Atlas status enum value to the corresponding OM InstanceValue enum value
     */
    private InstanceStatus convertAtlasStatusToOMInstanceStatus(AtlasEntity.Status atlasStatus)
    {
        switch (atlasStatus) {
            case ACTIVE:
                return InstanceStatus.ACTIVE;
            case DELETED:
                return InstanceStatus.DELETED;
            default:
                return null;
        }
    }







    private List<EntityDetail> findEntitiesBySearchCriteria(String               userId,
                                                            String               entityTypeGUID,
                                                            String               searchCriteria,
                                                            List<InstanceStatus> limitResultsByStatus,
                                                            List<String>         limitResultsByClassification)
            throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "findEntitiesBySearchCriteria";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    methodName, userId, searchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        // Special case when searchCriteria is null or empty string - return an empty list
        if (searchCriteria == null || searchCriteria.equals("")) {
            // Nothing to search for
            LOG.debug("{}: no search criteria supplied, returning null", methodName);
            return null;
        }

        // This method is used from findEntitiesByPropertyValue which needs to search string properties
        // only, with the string contained in searchCriteria.


        // If entityTypeGUID is not null then find the name of the type it refers to - this is used to filter by type
        String expectedTypeName = null;
        if (entityTypeGUID != null) {

            // Use the entityTypeGUID to retrieve the associated type name
            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
            } catch (TypeDefNotKnownException | RepositoryErrorException e) {
                LOG.error("{}: Caught exception from _getTypeDefByGUID", methodName, e);
                // handle below...
                typeDef = null;
            }
            if (typeDef == null) {
                LOG.debug("{}: could not retrieve typedef for guid {}", methodName, entityTypeGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
            expectedTypeName = typeDef.getName();

        }

        /*
         * If typeGUID is not specified the search applies to all entity types. The method will therefore find
         * all the entity types and do a query for each type, and union the results.
         * If typeGUID is specified then the above search logic is filtered to include only the specified (expected)
         * type and its sub-types.
         */

        List<TypeDef> allEntityTypes;
        try {

            allEntityTypes = _findTypeDefsByCategory(userId, TypeDefCategory.ENTITY_DEF);

        } catch (RepositoryErrorException e) {

            LOG.error("{}: caught exception from _findTypeDefsByCategory", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("any EntityDef", "TypeDefCategory.ENTITY_DEF", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        if (allEntityTypes == null) {
            LOG.debug("{}: found no entity types", methodName);
            return null;
        }

        ArrayList<EntityDetail> returnEntities = null;

        LOG.debug("{}: There are {} entity types defined", methodName, allEntityTypes.size());

        if (allEntityTypes.size() > 0) {

            // Iterate over the known entity types performing a search for each...

            for (TypeDef typeDef : allEntityTypes) {
                LOG.debug("{}: checking entity type {}", methodName, typeDef.getName());

                // If entityTypeGUID parameter is not null there is an expected type, so check whether the
                // current type matches the expected type or is one of its sub-types.

                if (expectedTypeName != null) {

                    String actualTypeName = typeDef.getName();
                    boolean typeMatch = repositoryHelper.isTypeOf(metadataCollectionId, actualTypeName, expectedTypeName);
                    if (!typeMatch) {
                        LOG.debug("{}: not searching entity type {} because not a subtype of {}", methodName, actualTypeName, expectedTypeName);
                        continue;
                    }
                    LOG.debug("{}: continuing with search for entity type {} because it is a subtype of {}", methodName, actualTypeName, expectedTypeName);
                }


                // Extract the type guid and invoke a type specific search...
                String typeDefGUID = typeDef.getGUID();

                ArrayList<EntityDetail> entitiesForCurrentType =
                        findEntitiesBySearchCriteriaForType(userId, typeDefGUID, searchCriteria, limitResultsByStatus, limitResultsByClassification);

                if (entitiesForCurrentType != null && !entitiesForCurrentType.isEmpty()) {
                    if (returnEntities == null) {
                        returnEntities = new ArrayList<>();
                    }
                    LOG.debug("{}: for type {} found {} entities", methodName, typeDef.getName(), entitiesForCurrentType.size());
                    returnEntities.addAll(entitiesForCurrentType);
                } else {
                    LOG.debug("{}: for type {} found no entities", methodName, typeDef.getName());
                }

            }
        }

        int resultSize = 0;
        if (returnEntities != null)
            resultSize = returnEntities.size();

        LOG.debug("{}: Atlas found {} entities", methodName, resultSize);


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnList={}",
                    methodName, userId, searchCriteria, limitResultsByStatus, limitResultsByClassification, returnEntities);
        }
        return returnEntities;
    }


    private ArrayList<EntityDetail> findEntitiesBySearchCriteriaForType(String               userId,
                                                                        String               entityTypeGUID,
                                                                        String               searchCriteria,
                                                                        List<InstanceStatus> limitResultsByStatus,
                                                                        List<String>         limitResultsByClassification)
            throws
            RepositoryErrorException
    {

        final String methodName = "findEntitiesBySearchCriteriaForType";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    methodName, userId, entityTypeGUID, searchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        // Turn the entity type GUID into an OM type name and then into an Atlas type name for the search

        SearchParameters searchParameters = new SearchParameters();
        // If there is a non-null type specified find the type name for use in SearchParameters

        String typeName = null;
        if (entityTypeGUID != null) {
            // find the entity type name
            TypeDef tDef;
            try {
                tDef = _getTypeDefByGUID(userId, entityTypeGUID);
            } catch (TypeDefNotKnownException e) {
                LOG.error("{}: caught exception from attempt to look up type by GUID {}", methodName, entityTypeGUID, e);
                return null;
            }
            if (tDef == null) {
                LOG.error("{}: null returned by look up of type with GUID {}", methodName, entityTypeGUID);
                return null;
            }
            typeName = tDef.getName();

            if (FamousFive.omTypeRequiresSubstitution(typeName)) {
                LOG.debug("[]: mapping F5 name from  {} to {} ", methodName, typeName, FamousFive.getAtlasTypeName(typeName, null));
                typeName = FamousFive.getAtlasTypeName(typeName, null);
            }
        }
        searchParameters.setTypeName(typeName);


        boolean postFilterClassifications = false;
        if (limitResultsByClassification != null) {
            if (limitResultsByClassification.size() == 1) {
                searchParameters.setClassification(limitResultsByClassification.get(0));          // with exactly one classification, classification will be part of search
            } else {
                postFilterClassifications = true;
                searchParameters.setClassification(null);                                         // classifications will be post-filtered if requested
            }
        } else {
            searchParameters.setClassification(null);
        }


        // Frame the query string in asterisks so that it can appear anywhere in a value...
        String searchPattern = "*" + searchCriteria + "*";
        searchParameters.setQuery(searchPattern);


        searchParameters.setExcludeDeletedEntities(false);
        searchParameters.setIncludeClassificationAttributes(false);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);

        /* Care is needed at this point. If this method were to pass offset and pageSize (limit) through to the
         * entityDiscoveryService we could miss valid results. This is because any instanceStatus checking is
         * performed as a post-process in the current method, so we must not offset or limit the search performed
         * by the discovery service.
         *
         * To understand why this would be a problem consider the following:
         * Suppose there are 100 entities that would match our search.
         * If we were to specify an offset of 20 and pageSize of 30 we would receive back entities 20 - 49.
         * If any of those entities fails the subsequent instance status or classification filter then we will get
         * a subset of a page of entities - none of the current method, the caller nor the end user can tell whether
         * there are other entities outside the range 20-49 that would have been valid and passed the status filter.
         * To provide a valid and maximal result we must only apply offset and pageSize once the post-filtering
         * is complete.
         *
         * In theory, we could allow Atlas to perform a narrower search where we know we will be filtering by
         * instanceStatus by testing (limitResultsByStatus != null). However, there is also filtering out of
         * entity proxies and since we do not know which entities are proxies until after the search is complete,
         * we must always search as widely as Atlas will allow.
         */
        int atlasSearchLimit = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
        int atlasSearchOffset = 0;
        searchParameters.setLimit(atlasSearchLimit);
        searchParameters.setOffset(atlasSearchOffset);
        searchParameters.setTagFilters(null);
        searchParameters.setAttributes(null);
        searchParameters.setEntityFilters(null);

        AtlasSearchResult atlasSearchResult;
        try {

            atlasSearchResult = entityDiscoveryService.searchWithParameters(searchParameters);

        } catch (AtlasBaseException e) {

            LOG.error("{}: entity discovery service searchWithParameters threw exception", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(searchCriteria, "searchCriteria", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        ArrayList<EntityDetail> returnList = null;


        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                // We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                // to get an EntityDetail object.

                /* An AtlasEntityHeader has:
                 * String                    guid
                 * AtlasEntity.Status        status
                 * String                    displayText
                 * List<String>              classificationNames
                 * List<AtlasClassification> classifications
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;

                try {

                    atlasEntityWithExt = getAtlasEntityById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.error("{}: entity store getById threw exception", methodName, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


                if (atlasEntity.isProxy() != null && atlasEntity.isProxy()) {
                    // This entity is only a proxy - do not include it in the search result.
                    LOG.debug("{}: ignoring atlasEntity because it is a proxy {}", methodName, atlasEntity);
                    continue;
                }

                if (postFilterClassifications) {
                    // We need to ensure that this entity has all of the specified filter classifications...
                    List<AtlasClassification> entityClassifications = atlasEntity.getClassifications();
                    int numFilterClassifications = limitResultsByClassification.size();
                    int cursor = 0;
                    boolean missingClassification = false;
                    while (!missingClassification && cursor < numFilterClassifications - 1) {
                        // Look for this filterClassification in the entity's classifications
                        String filterClassificationName = limitResultsByClassification.get(cursor);
                        boolean match = false;
                        for (AtlasClassification atlasClassification : entityClassifications) {
                            if (atlasClassification.getTypeName().equals(filterClassificationName)) {
                                match = true;
                                break;
                            }
                        }
                        if (!match) {
                            missingClassification = true;     // stop looking, one miss is enough
                        }
                        cursor++;
                    }
                    if (missingClassification)
                        continue;                  // skip this entity and process the next, if any remain
                }


                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("{}: om entity {}", methodName, omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (TypeErrorException | InvalidEntityException e) {

                    LOG.error("{}: could not map AtlasEntity to entityDetail, exception", methodName, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnList={}",
                    methodName, userId, searchCriteria, limitResultsByStatus, limitResultsByClassification, returnList);
        }
        return returnList;

    }

    /*
     *
     * Internal method for search using SearchParameters
     *
     */
    private List<EntityDetail> findEntitiesByMatchProperties(String               userId,
                                                             String               entityTypeGUID,
                                                             InstanceProperties   matchProperties,
                                                             MatchCriteria        matchCriteria,
                                                             List<InstanceStatus> limitResultsByStatus,
                                                             List<String>         limitResultsByClassification)
            throws
            PropertyErrorException,
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "findEntitiesByMatchProperties";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    methodName, userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification);
        }


        // If entityTypeGUID is not null then find the name of the type it refers to - this is used to filter by type
        String expectedTypeName = null;
        if (entityTypeGUID != null) {

            // Use the entityTypeGUID to retrieve the associated type name
            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
            } catch (TypeDefNotKnownException | RepositoryErrorException e) {
                LOG.error("{}: Caught exception from _getTypeDefByGUID", methodName, e);
                // handle below...
                typeDef = null;
            }
            if (typeDef == null) {
                LOG.debug("{}: could not retrieve typedef for guid {}", methodName, entityTypeGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
            expectedTypeName = typeDef.getName();

        }


        /*
         * If typeGUID is null this method needs to search across all entity types. The method will therefore find all
         * the entity types and do a query for each type, and union the results.
         * If typeGUID is not null, then the above cross-type search is filtered to include only the specified type and
         * its sub-types.
         */

        List<TypeDef> allEntityTypes;
        try {

            allEntityTypes = _findTypeDefsByCategory(userId, TypeDefCategory.ENTITY_DEF);

        } catch (RepositoryErrorException e) {

            LOG.error("{}: caught exception from _findTypeDefsByCategory", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("any EntityDef", "TypeDefCategory.ENTITY_DEF", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        if (allEntityTypes == null) {
            LOG.debug("{}: found no entity types", methodName);
            return null;
        }

        ArrayList<EntityDetail> returnEntities = null;

        LOG.debug("{}: There are {} entity types defined", methodName, allEntityTypes.size());

        if (allEntityTypes.size() > 0) {

            // Iterate over the known entity types performing a search for each...

            for (TypeDef typeDef : allEntityTypes) {
                LOG.debug("{}: checking entity type {}", methodName, typeDef.getName());

                /*
                 * If entityTypeGUID parameter is not null there is an expected type. If so, check whether the
                 * current (loop) typeDef is, or is a sub-type of, the expected type. i.e. only the expected type
                 * and any of its sub-types will be searched.
                 * If current type is a type of expected type, search the type, otherwise skip it.
                 */

                if (expectedTypeName != null) {

                    String actualTypeName = typeDef.getName();
                    boolean typeMatch = repositoryHelper.isTypeOf(metadataCollectionId, actualTypeName, expectedTypeName);
                    if (!typeMatch) {
                        LOG.debug("{}: not searching entity type {} because not a subtype of {}", methodName, actualTypeName, expectedTypeName);
                        continue;
                    }
                    LOG.debug("{}: continuing with search for entity type {} because it is a subtype of {}", methodName, actualTypeName, expectedTypeName);
                }

                // If there are any matchProperties
                if (matchProperties != null) {

                    /*
                     * Validate that the type has the specified properties...
                     * Whether matchCriteria is ALL | ANY | NONE we need ALL the match properties to be defined in the type definition
                     * For a property definition to match a property in the matchProperties, we need name and type to match.
                     */

                    // Find what properties are defined on the type; getAllDefinedProperties() will recurse up the supertype hierarchy
                    List<TypeDefAttribute> definedAttributes;
                    try {

                        definedAttributes = getAllDefinedProperties(userId, typeDef);

                    } catch (TypeDefNotKnownException e) {

                        LOG.error("{}: caught exception from property finder", methodName, e);

                        OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("find properties", "TypeDef", methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }

                    if (definedAttributes == null) {
                        // It is obvious that this type does not have the match properties - because it has no properties
                        // Proceed to the next entity type
                        LOG.debug("{}: entity type {} has no properties - will be ignored", methodName, typeDef.getName());
                        // continue;
                    } else {
                        // This type has properties...
                        // Iterate over the match properties, matching on name and type
                        Iterator<String> matchPropNames = matchProperties.getPropertyNames();
                        boolean allPropsDefined = true;
                        while (matchPropNames.hasNext()) {
                            String matchPropName = matchPropNames.next();
                            InstancePropertyValue matchPropValue = matchProperties.getPropertyValue(matchPropName);
                            if (matchPropValue == null) {

                                LOG.error("{}: match property with name {} has null value - not supported", methodName, matchPropName);

                                OMRSErrorCode errorCode = OMRSErrorCode.BAD_PROPERTY_FOR_INSTANCE;

                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(matchPropName, "matchProperties", methodName, "LocalAtlasOMRSMetadataCollection");

                                throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }

                            String matchPropType = matchPropValue.getTypeName();
                            LOG.debug("{}: matchProp has name {} type {}", methodName, matchPropName, matchPropType);
                            // Find the current match prop in the type def
                            boolean propertyDefined = false;
                            for (TypeDefAttribute defType : definedAttributes) {
                                AttributeTypeDef atd = defType.getAttributeType();
                                String defTypeName = atd.getName();
                                if (defType.getAttributeName().equals(matchPropName) && defTypeName.equals(matchPropType)) {
                                    // Entity type def has the current match property...
                                    LOG.debug("{}: entity type {} has property name {} type {}", methodName, typeDef.getName(), matchPropName, matchPropType);
                                    propertyDefined = true;
                                    break;
                                }
                            }
                            if (!propertyDefined) {
                                // this property is missing from the def
                                LOG.debug("{}: entity type {} does not have property name {} type {}", methodName, typeDef.getName(), matchPropName, matchPropType);
                                allPropsDefined = false;
                            }
                        }
                        if (!allPropsDefined) {
                            // At least one property in the match props is not defined on the type - skip the type
                            LOG.debug("{}: entity type {} does not have all match properties - will be ignored", methodName, typeDef.getName());
                            //continue;
                        } else {
                            // The current type is suitable for a find of instances of this type.
                            LOG.debug("{}: entity type {} will be searched", methodName, typeDef.getName());

                            // Extract the type guid and invoke a type specific search...
                            String typeDefGUID = typeDef.getGUID();

                            /* Do not pass on the offset and pageSize - these need to applied once on the aggregated result (from all types)
                             * So make this search as broad as possible - i.e. set offset to 0 and pageSze to MAX.
                             */

                            ArrayList<EntityDetail> entitiesForCurrentType =
                                    findEntitiesByMatchPropertiesForType(
                                            userId,
                                            typeDefGUID,
                                            matchProperties,
                                            matchCriteria,
                                            limitResultsByStatus,
                                            limitResultsByClassification);

                            if (entitiesForCurrentType != null && !entitiesForCurrentType.isEmpty()) {
                                if (returnEntities == null) {
                                    returnEntities = new ArrayList<>();
                                }
                                LOG.debug("{}: for type {} found {} entities", methodName, typeDef.getName(), entitiesForCurrentType.size());
                                returnEntities.addAll(entitiesForCurrentType);
                            } else {
                                LOG.debug("{}: for type {} found no entities", methodName, typeDef.getName());
                            }
                        }
                    }
                }
            }
        }

        int resultSize = 0;
        if (returnEntities != null)
            resultSize = returnEntities.size();

        LOG.debug("{}: Atlas found {} entities", methodName, resultSize);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnEntities={}",
                    methodName, userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, returnEntities);
        }
        //return limitedReturnList;
        return returnEntities;
    }

    /*
     *
     * Internal method for search by classificationName using SearchParameters
     */

    private List<EntityDetail> findEntitiesByClassificationName(String               userId,
                                                                String               entityTypeGUID,
                                                                String               classificationName,
                                                                InstanceProperties   matchClassificationProperties,
                                                                MatchCriteria        matchCriteria,
                                                                List<InstanceStatus> limitResultsByStatus)

            throws
            TypeErrorException,
            RepositoryErrorException,
            PropertyErrorException

    {


        final String methodName = "findEntitiesByClassificationName";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={})",
                    methodName, userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus);
        }

        // If entityTypeGUID is not null then find the name of the type it refers to - this is used to filter by type
        String expectedTypeName = null;
        if (entityTypeGUID != null) {

            // Use the entityTypeGUID to retrieve the associated type name
            TypeDef typeDef;
            try {
                typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
            } catch (TypeDefNotKnownException | RepositoryErrorException e) {
                LOG.error("{}: Caught exception from _getTypeDefByGUID", methodName, e);
                // handle below...
                typeDef = null;
            }
            if (typeDef == null) {
                LOG.debug("{}: could not retrieve typedef for guid {}", methodName, entityTypeGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
            expectedTypeName = typeDef.getName();

        }


        /*
         * If typeGUID is null this method needs to search across all entity types. The method will therefore find all
         * the entity types and do a query for each type, and union the results.
         * If typeGUID is not null, then the above cross-type search is filtered to include only the specified type and
         * its sub-types.
         */

        List<TypeDef> allEntityTypes;
        try {

            allEntityTypes = _findTypeDefsByCategory(userId, TypeDefCategory.ENTITY_DEF);

        } catch (RepositoryErrorException e) {

            LOG.error("{}: caught exception from _findTypeDefsByCategory", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("any EntityDef", "TypeDefCategory.ENTITY_DEF", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        if (allEntityTypes == null) {
            LOG.debug("{}: found no entity types", methodName);
            return null;
        }

        ArrayList<EntityDetail> returnEntities = null;

        LOG.debug("{}: There are {} entity types defined", methodName, allEntityTypes.size());

        if (allEntityTypes.size() > 0) {

            // Iterate over the known entity types performing a search for each...

            for (TypeDef typeDef : allEntityTypes) {
                LOG.debug("{}: checking entity type {}", methodName, typeDef.getName());

                /*
                 * If entityTypeGUID parameter is not null there is an expected type. If so, check whether the
                 * current (loop) typeDef is, or is a sub-type of, the expected type. i.e. only the expected type
                 * and any of its sub-types will be searched.
                 * If current type is a type of expected type, search the type, otherwise skip it.
                 */

                if (expectedTypeName != null) {

                    String actualTypeName = typeDef.getName();
                    boolean typeMatch = repositoryHelper.isTypeOf(metadataCollectionId, actualTypeName, expectedTypeName);
                    if (!typeMatch) {
                        LOG.debug("{}: not searching entity type {} because not a subtype of {}", methodName, actualTypeName, expectedTypeName);
                        continue;
                    }
                    LOG.debug("{}: continuing with search for entity type {} because it is a subtype of {}", methodName, actualTypeName, expectedTypeName);
                }

                // Search using classificationName...

                LOG.debug("{}: entity type {} will be searched", methodName, typeDef.getName());

                // Extract the type guid and invoke a type specific search...
                String typeDefGUID = typeDef.getGUID();

                /* Do not pass on the offset and pageSize - these need to applied once on the aggregated result (from all types)
                 * So make this search as broad as possible - i.e. set offset to 0 and pageSze to MAX.
                 */

                ArrayList<EntityDetail> entitiesForCurrentType =
                        findEntitiesByClassificationNameForType(userId, typeDefGUID, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus);

                if (entitiesForCurrentType != null && !entitiesForCurrentType.isEmpty()) {
                    if (returnEntities == null) {
                        returnEntities = new ArrayList<>();
                    }
                    LOG.debug("{}: for type {} found {} entities", methodName, typeDef.getName(), entitiesForCurrentType.size());
                    returnEntities.addAll(entitiesForCurrentType);
                } else {
                    LOG.debug("{}: for type {} found no entities", methodName, typeDef.getName());
                }
            }
        }

        int resultSize = 0;
        if (returnEntities != null)
            resultSize = returnEntities.size();

        LOG.debug("{}: Atlas found {} entities", methodName, resultSize);

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={}): returnEntities={}",
                    methodName, userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus, returnEntities);
        }
        return returnEntities;
    }


    // Utility method to recurse up supertype hierarchy fetching property defs
    // Deliberately not scoped for access - default to package private
    List<TypeDefAttribute> getAllDefinedProperties(String  userId,
                                                   TypeDef tdef)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        final String methodName = "getAllDefinedProperties";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, typedef={})", methodName, userId, tdef);
        }

        List<TypeDefAttribute> propDefs = new ArrayList<>();

        // Look at the supertype (if any) and then get the properties for the current type def (if any)
        if (tdef.getSuperType() != null) {
            // recurse up the supertype hierarchy until you hit the top
            // Get the supertype's type def
            TypeDefLink superTypeDefLink = tdef.getSuperType();
            String superTypeName = superTypeDefLink.getName();
            try {
                TypeDef superTypeDef = _getTypeDefByName(userId, superTypeName);
                List<TypeDefAttribute> inheritedProps = getAllDefinedProperties(userId, superTypeDef);
                if (inheritedProps != null && !inheritedProps.isEmpty()) {
                    propDefs.addAll(inheritedProps);
                }
            } catch (RepositoryErrorException | TypeDefNotKnownException e) {
                LOG.error("{}: caught exception from _getTypeDefByName", methodName, e);
                throw e;
            }
        }
        // Add the properties defined for the current type
        List<TypeDefAttribute> currentTypePropDefs = tdef.getPropertiesDefinition();
        if (currentTypePropDefs != null && !currentTypePropDefs.isEmpty()) {
            propDefs.addAll(currentTypePropDefs);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, typedef={}): propDefs={}", methodName, userId, tdef, propDefs);
        }
        return propDefs;
    }


    private ArrayList<EntityDetail> findEntitiesByMatchPropertiesForType(String               userId,
                                                                         String               entityTypeGUID,
                                                                         InstanceProperties   matchProperties,
                                                                         MatchCriteria        matchCriteria,
                                                                         List<InstanceStatus> limitResultsByStatus,
                                                                         List<String>         limitResultsByClassification)
            throws
            PropertyErrorException,
            RepositoryErrorException
    {

        final String methodName = "findEntitiesByMatchPropertiesForType";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    methodName, userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification);
        }





        // If there is a non-null type specified find the type name for use in SearchParameters
        String typeName = null;
        if (entityTypeGUID != null) {
            // find the entity type name
            TypeDef tDef;
            try {
                tDef = _getTypeDefByGUID(userId, entityTypeGUID);
            } catch (TypeDefNotKnownException e) {
                LOG.error("{}: caught exception from attempt to look up type by GUID {}", methodName, entityTypeGUID, e);
                return null;
            }
            if (tDef == null) {
                LOG.error("{}: null returned by look up of type with GUID {}", methodName, entityTypeGUID);
                return null;
            }
            typeName = tDef.getName();

            if (FamousFive.omTypeRequiresSubstitution(typeName)) {
                LOG.debug("[]: mapping F5 name from  {} to {} ", methodName, typeName, FamousFive.getAtlasTypeName(typeName, null));
                typeName = FamousFive.getAtlasTypeName(typeName, null);
            }
        }


        // Construct the Atlas query
        SearchParameters searchParameters = new SearchParameters();
        searchParameters.setTypeName(typeName);

        boolean postFilterClassifications = false;
        if (limitResultsByClassification != null) {
            if (limitResultsByClassification.size() == 1) {
                searchParameters.setClassification(limitResultsByClassification.get(0));          // with exactly one classification, classification will be part of search
            } else {
                postFilterClassifications = true;
                searchParameters.setClassification(null);                                         // classifications will be post-filtered if requested
            }
        } else {
            searchParameters.setClassification(null);
        }


        searchParameters.setQuery(null);

        searchParameters.setExcludeDeletedEntities(false);
        searchParameters.setIncludeClassificationAttributes(false);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);
        searchParameters.setLimit(AtlasConfiguration.SEARCH_MAX_LIMIT.getInt());
        searchParameters.setOffset(0);
        searchParameters.setTagFilters(null);
        searchParameters.setAttributes(null);

        searchParameters.setEntityFilters(null);
        if (matchProperties != null) {

            SearchParameters.Operator operator;                    // applied to a single filter criterion
            SearchParameters.FilterCriteria.Condition condition;   // applied to a list of filter criteria
            if (matchCriteria == null) {
                matchCriteria = MatchCriteria.ALL;                        // default to matchCriteria of ALL
            }
            switch (matchCriteria) {
                case ALL:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.AND;
                    break;
                case ANY:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.OR;
                    break;
                case NONE:
                    // Temporary restriction until Atlas supports not() step
                    //operator = NEQ;
                    //condition = SearchParameters.FilterCriteria.Condition.AND;
                    //break;
                default:
                    LOG.error("{}: only supports matchCriteria ALL, ANY", methodName);
                    return null;
            }

            List<SearchParameters.FilterCriteria> filterCriteriaList = new ArrayList<>();

            Iterator<String> matchNames = matchProperties.getPropertyNames();
            while (matchNames.hasNext()) {

                // For the next property allocate and initialise a filter criterion and add it to the list...

                // Extract the name and value as strings for the filter criterion
                String matchPropertyName = matchNames.next();
                boolean stringProp = false;
                String strValue;
                InstancePropertyValue matchValue = matchProperties.getPropertyValue(matchPropertyName);
                if (matchValue == null) {

                    LOG.error("{}: match property with name {} has null value - not supported", methodName, matchPropertyName);

                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_PROPERTY_FOR_INSTANCE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(matchPropertyName, "matchProperties", methodName, "LocalAtlasOMRSMetadataCollection");

                    throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                InstancePropertyCategory cat = matchValue.getInstancePropertyCategory();
                switch (cat) {
                    case PRIMITIVE:
                        // matchValue is a PPV
                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) matchValue;
                        // Find out if this is a string property
                        PrimitiveDefCategory pdc = ppv.getPrimitiveDefCategory();
                        if (pdc == OM_PRIMITIVE_TYPE_STRING) {
                            stringProp = true;
                            strValue = ppv.getPrimitiveValue().toString();
                            LOG.debug("{}: string property {} has filter value {}", methodName, matchPropertyName, strValue);
                        } else {
                            // We need a string for DSL query - this does not reflect the property type
                            strValue = ppv.getPrimitiveValue().toString();
                            LOG.debug("{}: non-string property {} has filter value {}", methodName, matchPropertyName, strValue);
                        }
                        break;
                    case ARRAY:
                    case MAP:
                    case ENUM:
                    case STRUCT:
                    case UNKNOWN:
                    default:
                        LOG.error("{}: match property of cat {} not supported", methodName, cat);

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PROPERTY_CATEGORY;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(cat.toString(), methodName, repositoryName);

                        throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());


                }

                // Override the operator (locally, only for this property) if it is a string
                SearchParameters.Operator localOperator = stringProp ? SearchParameters.Operator.CONTAINS : operator;


                // Set up a criterion for this property
                SearchParameters.FilterCriteria filterCriterion = new SearchParameters.FilterCriteria();

                filterCriterion.setAttributeName(matchPropertyName);
                filterCriterion.setAttributeValue(strValue);
                filterCriterion.setOperator(localOperator);
                filterCriteriaList.add(filterCriterion);
            }
            // Finalize with a FilterCriteria that contains the list of FilterCriteria and applies the appropriate condition (based on matchCriteria)
            SearchParameters.FilterCriteria entityFilters = new SearchParameters.FilterCriteria();
            entityFilters.setCriterion(filterCriteriaList);
            entityFilters.setCondition(condition);
            searchParameters.setEntityFilters(entityFilters);
        }


        AtlasSearchResult atlasSearchResult;
        try {
            atlasSearchResult = entityDiscoveryService.searchWithParameters(searchParameters);

        } catch (AtlasBaseException e) {

            LOG.error("{}: entity discovery service searchWithParameters threw exception", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("searchWithParameters", "searchParameters", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        ArrayList<EntityDetail> returnList = null;

        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }

                /* Use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                 * to get an EntityDetail object.
                 *
                 * An AtlasEntityHeader has:
                 * String                    guid
                 * AtlasEntity.Status        status
                 * String                    displayText
                 * List<String>              classificationNames
                 * List<AtlasClassification> classifications
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                try {

                    atlasEntityWithExt = getAtlasEntityById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.debug("{}: Caught exception from Atlas entity store getById method {}", methodName, e.getMessage());

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

                if (postFilterClassifications) {
                    // We need to ensure that this entity has all of the specified filter classifications...
                    List<AtlasClassification> entityClassifications = atlasEntity.getClassifications();
                    int numFilterClassifications = limitResultsByClassification.size();
                    int cursor = 0;
                    boolean missingClassification = false;
                    while (!missingClassification && cursor < numFilterClassifications - 1) {
                        // Look for this filterClassification in the entity's classifications
                        String filterClassificationName = limitResultsByClassification.get(cursor);
                        boolean match = false;
                        for (AtlasClassification atlasClassification : entityClassifications) {
                            if (atlasClassification.getTypeName().equals(filterClassificationName)) {
                                match = true;
                                break;
                            }
                        }
                        missingClassification = !match;   // stop looking, one miss is enough
                        cursor++;
                    }
                    if (missingClassification)
                        continue;                  // skip this entity and process the next, if any remain
                }


                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("{}: om entity {}", methodName, omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (Exception e) {

                    LOG.error("{}: could not map AtlasEntity to EntityDetail", methodName, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEntity.getGuid(), "atlasEntity", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnList={}",
                    methodName, userId, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, returnList);
        }
        return returnList;

    }



    private ArrayList<EntityDetail> findEntitiesByClassificationNameForType(String               userId,
                                                                            String               entityTypeGUID,
                                                                            String               classificationName,
                                                                            InstanceProperties   matchClassificationProperties,
                                                                            MatchCriteria        matchCriteria,
                                                                            List<InstanceStatus> limitResultsByStatus)
            throws
            PropertyErrorException,
            RepositoryErrorException
    {

        final String methodName = "findEntitiesByClassificationNameForType";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={})",
                    methodName, userId, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus);
        }


        SearchParameters searchParameters = new SearchParameters();


        // If there is a non-null type specified find the type name for use in SearchParameters

        String typeName = null;
        if (entityTypeGUID != null) {
            // find the entity type name
            TypeDef tDef;
            try {
                tDef = _getTypeDefByGUID(userId, entityTypeGUID);
            } catch (TypeDefNotKnownException e) {
                LOG.error("{}: caught exception from attempt to look up type by GUID {}", methodName, entityTypeGUID, e);
                return null;
            }
            if (tDef == null) {
                LOG.error("{}: null returned by look up of type with GUID {}", methodName, entityTypeGUID);
                return null;
            }
            typeName = tDef.getName();

            if (FamousFive.omTypeRequiresSubstitution(typeName)) {
                LOG.debug("[]: mapping F5 name from  {} to {} ", methodName, typeName, FamousFive.getAtlasTypeName(typeName, null));
                typeName = FamousFive.getAtlasTypeName(typeName, null);
            }
        }
        searchParameters.setTypeName(typeName);

        searchParameters.setQuery(null);

        searchParameters.setClassification(classificationName);
        searchParameters.setExcludeDeletedEntities(false);
        searchParameters.setIncludeClassificationAttributes(false);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);

        searchParameters.setLimit(AtlasConfiguration.SEARCH_MAX_LIMIT.getInt());
        searchParameters.setOffset(0);

        searchParameters.setEntityFilters(null);
        searchParameters.setAttributes(null);


        searchParameters.setTagFilters(null);
        if (matchClassificationProperties != null) {

            SearchParameters.Operator operator;                    // applied to a single filter criterion
            SearchParameters.FilterCriteria.Condition condition;   // applied to a list of filter criteria
            if (matchCriteria == null) {
                matchCriteria = MatchCriteria.ALL;                 // default to matchCriteria of ALL
            }
            switch (matchCriteria) {
                case ALL:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.AND;
                    break;
                case ANY:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.OR;
                    break;
                case NONE:
                    //operator = NEQ;
                    //condition = SearchParameters.FilterCriteria.Condition.AND;
                    //break;
                default:
                    LOG.error("{}: only supports matchCriteria ALL, ANY", methodName);
                    return null;
            }

            List<SearchParameters.FilterCriteria> filterCriteriaList = new ArrayList<>();

            Iterator<String> matchNames = matchClassificationProperties.getPropertyNames();
            while (matchNames.hasNext()) {

                // For the next property allocate and initialise a filter criterion and add it to the list...

                // Extract the name and value as strings for the filter criterion
                String matchPropertyName = matchNames.next();
                String strValue;
                InstancePropertyValue matchValue = matchClassificationProperties.getPropertyValue(matchPropertyName);
                if (matchValue == null) {

                    LOG.error("{}: match property with name {} has null value - not supported", methodName, matchPropertyName);

                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_PROPERTY_FOR_INSTANCE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(matchPropertyName, "matchProperties", methodName, "LocalAtlasOMRSMetadataCollection");

                    throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                InstancePropertyCategory cat = matchValue.getInstancePropertyCategory();
                switch (cat) {
                    case PRIMITIVE:
                        // matchValue is a PPV
                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) matchValue;
                        // Find out if this is a string property
                        PrimitiveDefCategory pdc = ppv.getPrimitiveDefCategory();
                        if (pdc == OM_PRIMITIVE_TYPE_STRING) {
                            String actualValue = ppv.getPrimitiveValue().toString();
                            // Frame the actual value so it is suitable for regex search
                            strValue = "'*" + actualValue + "*'";
                            LOG.debug("{}: string property {} has filter value {}", methodName, matchPropertyName, strValue);
                        } else {
                            // We need a string for DSL query - this does not reflect the property type
                            strValue = ppv.getPrimitiveValue().toString();
                            LOG.debug("{}: non-string property {} has filter value {}", methodName, matchPropertyName, strValue);
                        }
                        break;
                    case ARRAY:
                    case MAP:
                    case ENUM:
                    case STRUCT:
                    case UNKNOWN:
                    default:
                        LOG.error("{}: match property of cat {} not supported", methodName, cat);

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PROPERTY_CATEGORY;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(cat.toString(), methodName, repositoryName);

                        throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                }
                // Set up a criterion for this property
                SearchParameters.FilterCriteria filterCriterion = new SearchParameters.FilterCriteria();
                filterCriterion.setAttributeName(matchPropertyName);
                filterCriterion.setAttributeValue(strValue);
                filterCriterion.setOperator(operator);
                filterCriteriaList.add(filterCriterion);
            }
            // Finalize with a FilterCriteria that contains the list of FilterCriteria and applies the appropriate condition (based on matchCriteria)
            SearchParameters.FilterCriteria tagFilters = new SearchParameters.FilterCriteria();
            tagFilters.setCriterion(filterCriteriaList);
            tagFilters.setCondition(condition);
            searchParameters.setTagFilters(tagFilters);
        }


        AtlasSearchResult atlasSearchResult;
        try {
            atlasSearchResult = entityDiscoveryService.searchWithParameters(searchParameters);

        } catch (AtlasBaseException e) {

            LOG.error("{}: entity discovery service searchWithParameters threw exception", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(classificationName, "classificationName", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        ArrayList<EntityDetail> returnList = null;

        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                // We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                // to get an EntityDetail object.

                // An AtlasEntityHeader has:
                // String                    guid
                // AtlasEntity.Status        status
                // String                    displayText
                // List<String>              classificationNames
                // List<AtlasClassification> classifications
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                try {

                    atlasEntityWithExt = getAtlasEntityById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.error("{}: entity store getById threw exception", methodName, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("{}: om entity {}", methodName, omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (Exception e) {

                    LOG.error("{}: could not map AtlasEntity to EntityDetail, exception", methodName, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEntity.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={}): returnList={}",
                    methodName, userId, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus, returnList);
        }


        return returnList;

    }


    /*
     * Helper method to avoid needing to pass all the store references to the various mappers
     */
    List<TypeDefAttribute> convertAtlasAttributeDefs(String                                 userId,
                                                     List<AtlasStructDef.AtlasAttributeDef> aads)
            throws
            RepositoryErrorException,
            TypeErrorException
    {
        final String methodName = "convertAtlasAttributeDefs";

        // Get a mapper, do the conversion and pass back the result
        AtlasAttributeDefMapper aadm;
        try {
            aadm = new AtlasAttributeDefMapper(this, userId, typeDefStore, typeRegistry, repositoryHelper, typeDefsForAPI, aads);
        } catch (RepositoryErrorException e) {
            LOG.error("{}: could not create mapper", methodName, e);
            throw e;
        }
        return aadm.convertAtlasAttributeDefs();

    }


    /*
     * Helper method for mappers so that they can retrieve Atlas types from the store
     * The results can be filtered by Atlas TypeCategory if the filter param is not null
     */

    // package private
    TypeDefLink constructTypeDefLink(String typeName,
                                     TypeCategory categoryFilter)
    {
        TypeDefLink tdl = null;

        // Look in the Atlas type registry
        AtlasBaseTypeDef atlasType = typeRegistry.getTypeDefByName(typeName);

        if (atlasType != null) {
            TypeCategory atlasCategory = atlasType.getCategory();
            if (categoryFilter != null && atlasCategory != categoryFilter) {
                // the category does not match the specific filter
                return null;
            } else {
                // there is no category filter or there is and the atlasCategory matches it
                tdl = new TypeDefLink();
                tdl.setName(typeName);
                tdl.setGUID(atlasType.getGuid());
            }
        }

        return tdl;
    }

    /**
     * Validate that type's identifier is not null.
     *
     * @param sourceName        - source of the request (used for logging)
     * @param guidParameterName - name of the parameter that passed the guid.
     * @param guid              - unique identifier for a type or an instance passed on the request
     * @param methodName        - method receiving the call
     * @throws TypeErrorException - no guid provided
     */
    public void validateTypeGUID(String userId,
                                 String sourceName,
                                 String guidParameterName,
                                 String guid,
                                 String methodName)
            throws
            TypeErrorException
    {
        if (guid != null) {
            TypeDef foundDef;

            try {
                foundDef = _getTypeDefByGUID(userId, guid);
            } catch (Exception e) {
                // swallow this exception - we are throwing TypeErrorException below, if def was not found
                foundDef = null;
            }

            if (foundDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, guidParameterName, methodName, sourceName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
        }
    }

    /**
     * Use the paging and sequencing parameters to format the results for a repository call that returns a list of
     * entity instances.
     *
     * @param fullResults        - the full list of results in an arbitrary order
     * @param fromElement        - the starting element number of the instances to return. This is used when retrieving elements
     *                           beyond the first page of results. Zero means start from the first element.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder    - Enum defining how the results should be ordered.
     * @param pageSize           - the maximum number of result entities that can be returned on this request.  Zero means
     *                           unrestricted return results size.
     * @return results array as requested
     */
    private List<EntityDetail> formatEntityResults(List<EntityDetail> fullResults,
                                                   int                fromElement,
                                                   String             sequencingProperty,
                                                   SequencingOrder    sequencingOrder,
                                                   int                pageSize)
    {
        if (fullResults == null) {
            return null;
        }

        if (fullResults.isEmpty()) {
            return null;
        }

        if (fromElement > fullResults.size()) {
            return null;
        }

        List<EntityDetail> sortedResults = sortEntityResults(fullResults, sequencingProperty, sequencingOrder);

        if ((pageSize == 0) || (pageSize > sortedResults.size())) {
            return sortedResults;
        }

        return new ArrayList<>(fullResults.subList(fromElement, fromElement + pageSize - 1));
    }

    private List<EntityDetail> sortEntityResults(List<EntityDetail> listOfEntities,
                                                 String             propertyName,
                                                 SequencingOrder    sequencingOrder)
    {

        Collections.sort(listOfEntities, new java.util.Comparator<EntityDetail>() {
                    @Override
                    public int compare(final EntityDetail object1, final EntityDetail object2)
                    {
                        /*
                         * Ideally we would not include all this type inspection in the comparison
                         * function - but we do not know the types until we are comparing the
                         * pair of instances. There is no guarantee the list is homogeneous or that
                         * the objects to be compared are of the same type.
                         */

                        int ret;
                        String o1PropertyTypeName = null;
                        String o2PropertyTypeName = null;
                        Object o1PropertyValue = null;
                        Object o2PropertyValue = null;

                        /*
                         * If object1 has the named property, retrieve its value. Same for object2.
                         * If neither object has the property return 0
                         * If one object has the property sort that higher: +1 if object1, -1 if object2
                         * If both have a value for the property, of different types, return 0.
                         * If both have a value for the property, of the same type, compare them...
                         * This is only performed for primitives, anything else is treated as ignored
                         */
                        InstanceProperties o1Props = object1.getProperties();
                        if (o1Props != null) {
                            InstancePropertyValue o1PropValue = o1Props.getPropertyValue(propertyName);
                            if (o1PropValue != null) {
                                InstancePropertyCategory o1PropCat = o1PropValue.getInstancePropertyCategory();
                                if (o1PropCat == InstancePropertyCategory.PRIMITIVE) {
                                    o1PropertyTypeName = o1PropValue.getTypeName();
                                    o1PropertyValue = ((PrimitivePropertyValue) o1PropValue).getPrimitiveValue();
                                }
                            }
                        }
                        InstanceProperties o2Props = object2.getProperties();
                        if (o2Props != null) {
                            InstancePropertyValue o2PropValue = o2Props.getPropertyValue(propertyName);
                            if (o2PropValue != null) {
                                InstancePropertyCategory o2PropCat = o2PropValue.getInstancePropertyCategory();
                                if (o2PropCat == InstancePropertyCategory.PRIMITIVE) {
                                    o2PropertyTypeName = o2PropValue.getTypeName();
                                    o2PropertyValue = ((PrimitivePropertyValue) o2PropValue).getPrimitiveValue();
                                }
                            }
                        }
                        if (o1PropertyTypeName == null && o2PropertyTypeName == null) {
                            ret = 0;
                        } else if (o1PropertyTypeName != null && o2PropertyTypeName == null) {
                            ret = 1;
                        } else if (o1PropertyTypeName == null) { // implicit: o2PropertyTypeName != null
                            ret = -1;
                        } else if (!o1PropertyTypeName.equals(o2PropertyTypeName)) {
                            ret = 0;
                        } else {

                            // Both objects have values, of the same type for the named property - compare...
                            ret = typeSpecificCompare(o1PropertyTypeName, o1PropertyValue, o2PropertyValue);

                        }
                        if (sequencingOrder == SequencingOrder.PROPERTY_DESCENDING) {
                            ret = ret * (-1);
                        }

                        return ret;
                    }
                }
        );

        return listOfEntities;

    }

    private int typeSpecificCompare(String typeName, Object v1, Object v2)
    {
        /*
         * It must have been previously established that both objects are of the type
         * indicated by the supplied typeName
         */
        int ret;
        switch (typeName) {
            case "boolean":
                ret = ((Boolean) v1).compareTo((Boolean) v2);
                break;
            case "byte":
                ret = ((Byte) v1).compareTo((Byte) v2);
                break;
            case "char":
                ret = ((Character) v1).compareTo((Character) v2);
                break;
            case "short":
                ret = ((Short) v1).compareTo((Short) v2);
                break;
            case "integer":
                ret = ((Integer) v1).compareTo((Integer) v2);
                break;
            case "long":
                ret = ((Long) v1).compareTo((Long) v2);
                break;
            case "float":
                ret = ((Float) v1).compareTo((Float) v2);
                break;
            case "double":
                ret = ((Double) v1).compareTo((Double) v2);
                break;
            case "biginteger":
                ret = ((BigInteger) v1).compareTo((BigInteger) v2);
                break;
            case "bigdecimal":
                ret = ((BigDecimal) v1).compareTo((BigDecimal) v2);
                break;
            case "string":
                ret = ((String) v1).compareTo((String) v2);
                break;
            case "date":
                ret = ((Date) v1).compareTo((Date) v2);
                break;
            default:
                LOG.debug("Property type not catered for in compare function");
                ret = 0;
        }
        return ret;

    }


    /**
     * Use the paging and sequencing parameters to format the results for a repository call that returns a list of
     * entity instances.
     *
     * @param fullResults        - the full list of results in an arbitrary order
     * @param fromElement        - the starting element number of the instances to return. This is used when retrieving elements
     *                           beyond the first page of results. Zero means start from the first element.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder    - Enum defining how the results should be ordered.
     * @param pageSize           - the maximum number of result entities that can be returned on this request.  Zero means
     *                           unrestricted return results size.
     * @return results array as requested
     */
    private List<Relationship> formatRelationshipResults(List<Relationship> fullResults,
                                                         int                fromElement,
                                                         String             sequencingProperty,
                                                         SequencingOrder    sequencingOrder,
                                                         int                pageSize)
    {
        if (fullResults == null) {
            return null;
        }

        if (fullResults.isEmpty()) {
            return null;
        }

        if (fromElement > fullResults.size()) {
            return null;
        }

        List<Relationship> sortedResults = sortRelationshipResults(fullResults, sequencingProperty, sequencingOrder);

        if ((pageSize == 0) || (pageSize > sortedResults.size())) {
            return sortedResults;
        }

        return new ArrayList<>(sortedResults.subList(fromElement, fromElement + pageSize - 1));
    }


    private List<Relationship> sortRelationshipResults(List<Relationship> listOfRelationships,
                                                       String             propertyName,
                                                       SequencingOrder    sequencingOrder)
    {

        Collections.sort(listOfRelationships, new java.util.Comparator<Relationship>() {
                    @Override
                    public int compare(final Relationship object1, final Relationship object2)
                    {

                        /*
                         * Ideally we would not include all this type inspection in the comparison
                         * function - but we do not know the types until we are comparing the
                         * pair of instances. There is no guarantee the list is homogeneous or that
                         * the objects to be compared are of the same type.
                         */

                        int ret;
                        String o1PropertyTypeName = null;
                        String o2PropertyTypeName = null;
                        Object o1PropertyValue = null;
                        Object o2PropertyValue = null;

                        /*
                         * If object1 has the named property, retrieve its value. Same for object2.
                         * If neither object has the property return 0
                         * If one object has the property sort that higher: +1 if object1, -1 if object2
                         * If both have a value for the property, of different types, return 0.
                         * If both have a value for the property, of the same type, compare them...
                         * This is only performed for primitives, anything else is treated as ignored
                         */

                        InstanceProperties o1Props = object1.getProperties();
                        if (o1Props != null) {
                            InstancePropertyValue o1PropValue = o1Props.getPropertyValue(propertyName);
                            if (o1PropValue != null) {
                                InstancePropertyCategory o1PropCat = o1PropValue.getInstancePropertyCategory();
                                if (o1PropCat == InstancePropertyCategory.PRIMITIVE) {
                                    o1PropertyTypeName = o1PropValue.getTypeName();
                                    o1PropertyValue = ((PrimitivePropertyValue) o1PropValue).getPrimitiveValue();
                                }
                            }
                        }
                        InstanceProperties o2Props = object2.getProperties();
                        if (o2Props != null) {
                            InstancePropertyValue o2PropValue = o2Props.getPropertyValue(propertyName);
                            if (o2PropValue != null) {
                                InstancePropertyCategory o2PropCat = o2PropValue.getInstancePropertyCategory();
                                if (o2PropCat == InstancePropertyCategory.PRIMITIVE) {
                                    o2PropertyTypeName = o2PropValue.getTypeName();
                                    o2PropertyValue = ((PrimitivePropertyValue) o2PropValue).getPrimitiveValue();
                                }
                            }
                        }
                        if (o1PropertyTypeName == null && o2PropertyTypeName == null) {
                            ret = 0;
                        } else if (o1PropertyTypeName != null && o2PropertyTypeName == null) {
                            ret = 1;
                        } else if (o1PropertyTypeName == null) { // implicit: o2PropertyTypeName != null
                            ret = -1;
                        } else if (!o1PropertyTypeName.equals(o2PropertyTypeName)) { // implicit: both typeNames != null
                            ret = 0;
                        } else {

                            // Both objects have values, of the same type for the named property - compare...
                            ret = typeSpecificCompare(o1PropertyTypeName, o1PropertyValue, o2PropertyValue);

                        }
                        if (sequencingOrder == SequencingOrder.PROPERTY_DESCENDING) {
                            ret = ret * (-1);
                        }

                        return ret;
                    }
                }
        );

        return listOfRelationships;

    }


    private TypeDef convertAtlasTypeDefToOMTypeDef(String           userId,
                                                   AtlasBaseTypeDef abtd)
            throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "convertAtlasTypeDefToOMTypeDef";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}: AtlasBaseTypeDef {}", methodName, abtd);
        }

        if (abtd == null) {
            LOG.debug("{}: cannot convert null type", methodName);

            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("abtd", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        String name = abtd.getName();
        String omTypeName = name;
        if (FamousFive.atlasTypeRequiresSubstitution(name)) {
            omTypeName = FamousFive.getOMTypeName(name);
        }

        // Generate a candidate OM TypeDef
        TypeDef candidateTypeDef;

        // Find the category of the Atlas typedef and invoke the relevant conversion method.
        // The only Atlas type categories that we can convert to OM TypeDef are:
        // ENTITY, RELATIONSHIP & CLASSIFICATION
        // Anything else we will bounce and return null.
        // Each of the processAtlasXXXDef methods will convert from Atlas to OM,
        // perform the RCM helper check, and finally populate the TDBC with the
        // appropriate OM type.
        // On return we just need to retrieve the TypeDef nd return it.
        TypeCategory atlasCat = abtd.getCategory();
        switch (atlasCat) {

            case ENTITY:
                /* There is no need to detect whether the OM Type is one of the Famous Five - this is
                 * checked by processAtlasEntityDef
                 */
                AtlasEntityDef atlasEntityDef = (AtlasEntityDef) abtd;
                processAtlasEntityDef(userId, atlasEntityDef);
                candidateTypeDef = typeDefsForAPI.getEntityDef(omTypeName);
                break;

            case RELATIONSHIP:
                AtlasRelationshipDef atlasRelationshipDef = (AtlasRelationshipDef) abtd;
                processAtlasRelationshipDef(userId, atlasRelationshipDef);
                candidateTypeDef = typeDefsForAPI.getRelationshipDef(name);
                break;

            case CLASSIFICATION:
                AtlasClassificationDef atlasClassificationDef = (AtlasClassificationDef) abtd;
                processAtlasClassificationDef(userId, atlasClassificationDef);
                candidateTypeDef = typeDefsForAPI.getClassificationDef(name);
                break;

            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
            case STRUCT:
            case OBJECT_ID_TYPE:
            default:
                LOG.debug("{}: Atlas type has category cannot be converted to OM TypeDef, category {} ", methodName, atlasCat);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_CATEGORY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasCat.toString(), name, methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: returning TypeDef {}", methodName, candidateTypeDef);
        }
        return candidateTypeDef;
    }


    // package-private
    OMRSRepositoryHelper getRepositoryHelper()
    {
        return this.repositoryHelper;
    }

    /*
     * Utility method to validate status fields can be modelled in Atlas
     */
    private boolean validateStatusFields(TypeDef typeDef)
    {

        final String methodName = "validateStatusFields";

        // Validate the initialStatus and validInstanceStatus fields of the passed TypeDef
        ArrayList<InstanceStatus> statusValuesCorrespondingToAtlas = new ArrayList<>(); // These are OM status values that relate to values valid in Atlas
        statusValuesCorrespondingToAtlas.add(InstanceStatus.ACTIVE);
        statusValuesCorrespondingToAtlas.add(InstanceStatus.DELETED);

        InstanceStatus initialStatus = typeDef.getInitialStatus();
        boolean isInitialStatusOK;
        if (initialStatus != null) {
            isInitialStatusOK = statusValuesCorrespondingToAtlas.contains(initialStatus);
            if (isInitialStatusOK)
                LOG.debug("{}: initialStatus {} is OK", methodName, initialStatus);
            else
                LOG.debug("{}: initialStatus {} is not OK", methodName, initialStatus);
        } else {
            isInitialStatusOK = true;
            LOG.debug("{}: initialStatus is null - which is OK", methodName);
        }

        List<InstanceStatus> validStatusList = typeDef.getValidInstanceStatusList();
        boolean isStatusListOK = true;
        if (validStatusList != null) {
            for (InstanceStatus thisStatus : validStatusList) {
                if (!statusValuesCorrespondingToAtlas.contains(thisStatus)) {
                    isStatusListOK = false;
                    LOG.debug("{}: validInstanceStatusList contains {} - which is not OK", methodName, thisStatus);
                    break;
                }
            }
            if (isStatusListOK)
                LOG.debug("{}: all members of validInstanceStatusList are OK", methodName);
        } else {
            // implicit: isStatusListOK = true;
            LOG.debug("{}: validInstanceStatusList is null - which is OK", methodName);
        }

        return (isInitialStatusOK && isStatusListOK);

    }


    /*
     * Helper method for event mapper
     */

    public String _getTypeDefGUIDByAtlasTypeName(String userId,
                                                 String atlasTypeName)
            throws
            TypeDefNotKnownException
    {

        final String methodName = "_getTypeDefGUIDByAtlasTypeName";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, atlasTypeName={})", methodName, userId, atlasTypeName);
        }

        /*
         * Check name is not null. null => throw
         * Ask the Atlas type registry for the type (by name), retrieve the GUID and return it.
         */

        AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByName(atlasTypeName);

        if (abtd == null) {
            LOG.debug("{}: Atlas does not have the type with name {} ", methodName, atlasTypeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        String typeDefGUID = abtd.getGuid();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}: atlasTypeDefGUID={}", methodName, typeDefGUID);
        }
        return typeDefGUID;

    }


    public void setEventMapper(AtlasOMRSRepositoryEventMapper eventMapper)
    {
        LOG.debug("setEventMapper: eventMapper being set to {}", eventMapper);
        this.eventMapper = eventMapper;
    }


    private AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityById(String guid)
            throws
            AtlasBaseException
    {
        return getAtlasEntityById(guid, false);
    }

    private AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityById(String  guid,
                                                                  boolean incDeleted)
            throws
            AtlasBaseException
    {

        final String methodName = "getAtlasEntityById";

        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            atlasEntityWithExt = entityStore.getById(guid);
            if (!incDeleted) {
                if (atlasEntityWithExt != null && atlasEntityWithExt.getEntity() != null && atlasEntityWithExt.getEntity().getStatus() == AtlasEntity.Status.DELETED) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);

                }
            }
            return atlasEntityWithExt;

        } catch (AtlasBaseException e) {
            // Not necessarily an error condition
            LOG.debug("{}: caught AtlasBaseException {}", methodName, e.getMessage());
            throw e;
        }
    }

    private AtlasRelationship getAtlasRelationshipById(String guid)
            throws
            AtlasBaseException
    {
        return getAtlasRelationshipById(guid, false);
    }

    private AtlasRelationship getAtlasRelationshipById(String  guid,
                                                       boolean incDeleted)
            throws
            AtlasBaseException
    {

        final String methodName = "getAtlasRelationshipById";

        AtlasRelationship atlasRelationship;
        try {
            atlasRelationship = relationshipStore.getById(guid);
            if (!incDeleted) {
                if (atlasRelationship != null && atlasRelationship.getStatus() == AtlasRelationship.Status.DELETED) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);

                }
            }
            return atlasRelationship;

        } catch (AtlasBaseException e) {
            // Not necessarily an error condition
            LOG.debug("{}: caught AtlasBaseException {}", methodName, e.getMessage());
            throw e;
        }
    }


    private List<TypeDef> _findTypeDefsByCategory(String          userId,
                                                  TypeDefCategory category)
            throws
            RepositoryErrorException
    {

        final String methodName = "_findTypeDefsByCategory";
        final String sourceName = metadataCollectionId;
        final String categoryParameterName = "category";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, category={})", methodName, userId, category);
        }


        // Strategy: use searchTypesDef with a SearchFilter with type parameter set according to category
        String typeForSearchParameter;
        switch (category) {
            case ENTITY_DEF:
                typeForSearchParameter = "ENTITY";
                break;
            case RELATIONSHIP_DEF:
                typeForSearchParameter = "RELATIONSHIP";
                break;
            case CLASSIFICATION_DEF:
                typeForSearchParameter = "CLASSIFICATION";
                break;
            default:
                LOG.error("{}: unsupported category {}", methodName, category);
                return null;
        }
        SearchFilter searchFilter = new SearchFilter();
        searchFilter.setParam(SearchFilter.PARAM_TYPE, typeForSearchParameter);
        AtlasTypesDef atd;
        try {

            atd = typeDefStore.searchTypesDef(searchFilter);

        } catch (AtlasBaseException e) {

            LOG.error("{}: caught exception from Atlas type def store {}", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("category", categoryParameterName, methodName, sourceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        /*
         * Parse the Atlas TypesDef
         * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
         * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
         * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
         * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
         * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
         * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
         * contains anything other than primitives).
         */


        // This method will populate the typeDefsForAPI object.
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }

        // Retrieve the list of typedefs from the appropriate list in the typeDefsForAPI
        List<TypeDef> ret;
        switch (category) {
            case ENTITY_DEF:
                ret = typeDefsForAPI.getEntityDefs();
                break;
            case RELATIONSHIP_DEF:
                ret = typeDefsForAPI.getRelationshipDefs();
                break;
            case CLASSIFICATION_DEF:
                ret = typeDefsForAPI.getClassificationDefs();
                break;
            default:
                LOG.error("{}: unsupported category {}", methodName, category);
                return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: ret={}", methodName, userId, category, ret);
        }
        return ret;


    }


    private List<AttributeTypeDef> _findAttributeTypeDefsByCategory(String                   userId,
                                                                    AttributeTypeDefCategory category)
            throws
            RepositoryErrorException
    {

        final String methodName            = "_findAttributeTypeDefsByCategory";
        final String categoryParameterName = "category";
        final String sourceName            = metadataCollectionId;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, category={})", methodName, userId, category);
        }


        /*
         * Strategy:
         * Atlas handles Enum defs as types - whereas in OM they are attribute type defs, so for enums (only) use a search
         * like above for findTypeDefByCategory
         * For all other categories we need to do a different search - see below...
         */


        List<AttributeTypeDef> ret;


        // If category is ENUM_DEF use searchTypesDef with a SearchFilter with type parameter set according to category
        if (category == ENUM_DEF) {
            String typeForSearchParameter = "ENUM";
            SearchFilter searchFilter = new SearchFilter();
            searchFilter.setParam(SearchFilter.PARAM_TYPE, typeForSearchParameter);
            AtlasTypesDef atd;
            try {

                atd = typeDefStore.searchTypesDef(searchFilter);

            } catch (AtlasBaseException e) {

                LOG.error("{}: caught exception from Atlas type def store {}", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.ATTRIBUTE_TYPEDEF_NAME_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("category", categoryParameterName, methodName, sourceName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            /*
             * Parse the Atlas TypesDef
             * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
             * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
             * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
             * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
             * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
             * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
             * contains anything other than primitives).
             */

            // This method will populate the typeDefsForAPI object.
            if (atd != null) {
                convertAtlasTypeDefs(userId, atd);
            }

            // Retrieve the list of typedefs from the appropriate list in the typeDefsForAPI
            ret = typeDefsForAPI.getEnumDefs();

        } else {

            /*
             * Category is not ENUM - it should be PRIMITIVE or COLLECTION - or could be UNKNOWN_DEF or invalid.
             * In the case where we are looking for all attribute type defs by category PRIMITIVE or COLLECTION,
             * the best way may be to get all types and then return the appropriate section of the TDBC...
             * ... expensive operation but cannot currently see another way to achieve it.
             */

            try {
                loadAtlasTypeDefs(userId);
            } catch (RepositoryErrorException e) {
                LOG.error("{}: caught exception from Atlas {}", methodName, e);
                throw e;
            }

            switch (category) {
                case PRIMITIVE:
                    ret = typeDefsForAPI.getPrimitiveDefs();
                    break;
                case COLLECTION:
                    ret = typeDefsForAPI.getCollectionDefs();
                    break;
                default:
                    LOG.error("{}: unsupported category {}", methodName, category);
                    ret = null;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: ret={}", methodName, ret);
        }
        return ret;
    }


    /*
     * Utility method to recursively gather property names from a TypeDef
     */
    private List<String> getPropertyNames(String  userId,
                                          TypeDef typeDef)
            throws
            RepositoryErrorException
    {

        final String methodName = "getPropertyNames";

        List<String> discoveredPropNames = new ArrayList<>();
        List<TypeDefAttribute> typeDefAttributeList = typeDef.getPropertiesDefinition();
        if (typeDefAttributeList != null) {
            for (TypeDefAttribute tda : typeDefAttributeList) {
                String attrName = tda.getAttributeName();
                discoveredPropNames.add(attrName);
            }
        }
        TypeDefLink superTypeLink = typeDef.getSuperType();
        if (superTypeLink != null) {
            // Retrieve the supertype - the TDL gives us its GUID and name
            if (superTypeLink.getName() != null) {
                TypeDef superTypeDef;
                try {
                    superTypeDef = this._getTypeDefByName(userId, superTypeLink.getName());

                } catch (Exception e) {
                    LOG.error("{}: caught exception from getTypeDefByName {}", methodName, e);
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                if (superTypeDef != null) {
                    List<String> superTypesPropNames = getPropertyNames(userId, superTypeDef);
                    if (superTypesPropNames != null) {
                        discoveredPropNames.addAll(superTypesPropNames);
                    }
                }
            }
        }
        if (discoveredPropNames.isEmpty())
            discoveredPropNames = null;
        return discoveredPropNames;
    }

    /*
     * Utility method to check whether all match names are contained in a list of property names
     */
    private boolean propertiesContainAllMatchNames(List<String> propNames,
                                                   List<String> matchNames)
    {

        // Check the currentTypePropNames contains ALL the names in matchCriteria
        if (matchNames == null) {
            // There are no matchCriteria - the list of property names implicitly passes the filter test
            return true;
        } else {
            // There are matchCriteria - inspect the list of property names
            if (propNames == null) {
                // The prop list has no properties - instant match failure
                return false;
            }
            // It has been established that both currentTypePropNames and matchPropertyNames are not null
            boolean allMatchPropsFound = true;
            for (String matchName : matchNames) {
                boolean thisMatchPropFound = false;
                for (String propName : propNames) {
                    if (propName.equals(matchName)) {
                        thisMatchPropFound = true;
                        break;
                    }
                }
                if (!thisMatchPropFound) {
                    allMatchPropsFound = false;
                    break;
                }
            }
            return allMatchPropsFound;

        }
    }

    /*
     * Utility method to compare a list of external standards mappings with a set of filter criteria.
     * If the list contains at least one member that satisfies the criteria, the method returns true.
     * This method is based on the assumption that at least one of the filterCriteria is non null, as
     * established by the validator, in the caller.
     */
    private boolean externalStandardsSatisfyCriteria(List<ExternalStandardMapping> typeDefExternalMappings,
                                                     ExternalStandardMapping       filterCriteria)
    {

        String standardCriterion     = filterCriteria.getStandardName();
        String organizationCriterion = filterCriteria.getStandardOrganization();
        String identifierCriterion   = filterCriteria.getStandardTypeName();
        /*
         * In case the caller does not establish that there is at least one non-null filter criterion, take care of that here
         * If there are no criteria then the list will pass
         */
        if (standardCriterion == null && organizationCriterion == null && identifierCriterion == null) {
            // Degenerate filterCriteria, test automatically passes
            return true;
        }

        /*
         * For each ESM in the list, check whether it satisfies the criteria in the filterCriteria.
         * If any ESM in the list matches then the list passes.
         * Return a boolean for the list.
         * When a filter criterion is null it means 'anything goes', so it is only the specific value(s)
         * that need to match.
         * If there is no filterCriteria.standard or there is and it matches the ESM passes
         * If there is no filterCriteria.organization or there is and it matches the ESM passes
         * If there is no filterCriteria.identifier or there is and it matches the ESM passes
         */
        if (typeDefExternalMappings == null) {
            // It has already been established that at least one of the filterCriteria is not null.
            // If that is the case and there are no ESMs in the list, then the list fails.
            return false;
        } else {
            boolean listOK = false;

            for (ExternalStandardMapping currentESM : typeDefExternalMappings) {

                boolean standardSatisfied = (standardCriterion == null || currentESM.getStandardName().equals(standardCriterion));
                boolean organizationSatisfied = (organizationCriterion == null || currentESM.getStandardOrganization().equals(organizationCriterion));
                boolean identifierSatisfied = (identifierCriterion == null || currentESM.getStandardTypeName().equals(identifierCriterion));

                if (standardSatisfied && organizationSatisfied && identifierSatisfied) {
                    // This ESM matches the criteria so the whole list passes...
                    listOK = true;
                    break;
                }
                // This ESM does not match the criteria but continue with the remainder of the list...
            }

            return listOK;
        }
    }

    /*
     * Internal implementation of getTypeDefByGUID()
     * This is separated so that other methods can use it without resetting the typeDefsForAPI object
     */
    private TypeDef _getTypeDefByGUID(String userId,
                                      String guid)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        final String methodName = "_getTypeDefByGUID";

        // Strategy:
        // Check guid is not null. null => return null
        // Use Atlas typedef store getByName()
        // If you get back a typedef of a category that can be converted to OM typedef then convert it and return the type def.
        // If Atlas type is not of a category that can be converted to an OM TypeDef then return throw TypeDefNotKnownException.

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }

        // This is an internal helper method so should never get null guid; but if so return null.
        if (guid == null)
            return null;


        // Retrieve the AtlasBaseTypeDef


        // Look in the Atlas type registry
        AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByGuid(guid);

        if (abtd == null) {
            LOG.debug("{}: Atlas does not have the type with guid {} ", methodName, guid);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("guid", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Underlying method handles Famous Five conversions
        TypeDef ret;
        try {
            ret = convertAtlasTypeDefToOMTypeDef(userId, abtd);
        } catch (TypeErrorException e) {
            LOG.error("{}: Failed to convert the Atlas type {} to an OM TypeDef", methodName, abtd.getName(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("guid", methodName, metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: ret={}", methodName, ret);
        }
        return ret;

    }

    /*
     * Internal implementation of getAttributeTypeDefByGUID
     * This is separated so that other methods can use it without resetting the typeDefsForAPI object
     */
    private AttributeTypeDef _getAttributeTypeDefByGUID(String userId,
                                                        String guid)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        final String methodName = "_getAttributeTypeDefByGUID";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, guid={})", methodName, userId, guid);
        }

        // Strategy:
        // Check guid is not null. null => return null (it should already have been checked)
        // Use Atlas typedef store getByName()
        // If you get back a AttributeTypeDef of a category that can be converted to OM AttributeTypeDef
        // then convert it and return the AttributeTypeDef.
        // If Atlas type is not of a category that can be converted to an OM AttributeTypeDef then return throw TypeDefNotKnownException.

        if (guid == null) {
            LOG.error("{}: guid is null", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByGUID",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Find out whether we are dealing with a PRIMITIVE Atlas type - i.e. a system type.
        // If so return the copy from the RCM rather than looking in Atlas...
        String name;
        AttributeTypeDef existingAttributeTypeDef;
        String source = metadataCollectionId;
        // Need to convert from guid to name space
        try {
            existingAttributeTypeDef = repositoryHelper.getAttributeTypeDef(source, guid, methodName);

        } catch (TypeErrorException e) {
            // Catching an exception here is not necessarily the end of the world; the by-GUID retriever
            // does not return null if type not found - and this was just a preliminary check in the RCM
            // to ascertain whether the type is a primitive or collection. If the RCM does not have it
            // then it could be an Enum from Atlas so don't despair (yet) - ask Atlas for it.
            existingAttributeTypeDef = null;
        }

        if (existingAttributeTypeDef != null) {
            // The RCM returned a type with the specified GUID - look at it to see whether we think it is a
            // primitive or collection - or neither.
            name = existingAttributeTypeDef.getName();
            LOG.debug("{}: attributeTypeDef GUID {} mapped to name {} by repository helper", methodName, guid, name);

            // Check if it is a primitive
            // If so return the copy from the RCM rather than looking in Atlas...
            PrimitiveDefCategory omrs_primitive_category = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(name);
            if (omrs_primitive_category != OM_PRIMITIVE_TYPE_UNKNOWN) {
                LOG.debug("{}: type name {} relates to a PRIMITIVE type in Atlas", methodName, name);
                return existingAttributeTypeDef;
            }

            // Check if it is a collection
            // If so return the copy from the RCM rather than looking in Atlas...
            String OM_ARRAY_PREFIX = "array<";
            String OM_ARRAY_SUFFIX = ">";
            boolean isArrayType = false;

            String OM_MAP_PREFIX = "map<";
            String OM_MAP_SUFFIX = ">";
            boolean isMapType = false;

            boolean isCollectionType = false;

            if (name.startsWith(OM_ARRAY_PREFIX) && name.endsWith(OM_ARRAY_SUFFIX))
                isArrayType = true;
            if (name.startsWith(OM_MAP_PREFIX) && name.endsWith(OM_MAP_SUFFIX))
                isMapType = true;
            if (isArrayType || isMapType)
                isCollectionType = true;

            if (isCollectionType) {
                LOG.debug("{}: type name {} relates to a COLLECTION type in Atlas", methodName, name);
                return existingAttributeTypeDef;
            }
        }

        // We did not get an existing attribute type from the RCM, so go ask Atlas...

        // Look in the Atlas type registry
        AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByGuid(guid);

        if (abtd == null) {
            LOG.debug("{}: Atlas does not have the type with guid {} ", methodName, guid);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("guid", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        AttributeTypeDef ret;

        // Generate a candidate OM TypeDef
        AttributeTypeDef candidateAttributeTypeDef;

        // Find the category of the Atlas typedef and invoke the relevant conversion method.
        // The only Atlas type categories that we can convert to OM AttributeTypeDef are:
        // PRIMITIVE: ENUM: ARRAY: MAP:
        // Anything else we will bounce and return null.
        // This will populate TDBC - you then need to retrieve the TD and return it.
        TypeCategory atlasCat = abtd.getCategory();
        switch (atlasCat) {

            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
                // For any of these categories get a candidate ATD
                //candidateAttributeTypeDef = convertAtlasBaseTypeDefToAttributeTypeDef(abtd);
                AtlasBaseTypeDefMapper abtdMapper = new AtlasBaseTypeDefMapper(abtd);
                candidateAttributeTypeDef = abtdMapper.toAttributeTypeDef();
                break;

            case ENTITY:
            case RELATIONSHIP:
            case CLASSIFICATION:
            case STRUCT:
            case OBJECT_ID_TYPE:
            default:
                LOG.error("{}: Atlas type has category cannot be converted to OM AttributeTypeDef, category {} ", methodName, atlasCat);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }


        if (candidateAttributeTypeDef == null) {
            LOG.error("{}: candidateAttributeTypeDef is null", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        } else {
            // Finally, check if the converted attributeTypeDef is known by the repos helper and whether it matches exactly.
            // If it doesn't exist in RH, return the copy we got from the TDBC.
            // If it does exist in RH then perform a deep compare; exact match => return the ATD from the RH
            // If it does exist in RH but is not an exact match => audit log and return null;

            // Ask RepositoryContentManager whether there is an AttributeTypeDef with supplied name
            name = candidateAttributeTypeDef.getName();
            try {
                existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, name);
            } catch (OMRSLogicErrorException e) {
                LOG.error("{}: Caught exception from repository helper for attribute type def with name {}", name, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            if (existingAttributeTypeDef == null) {
                // The RH does not have a typedef by the supplied name - use the candidateTypeDef
                LOG.debug("{}: repository content manager returned name not found - use the candidate AttributeTypeDef", methodName);
                candidateAttributeTypeDef.setDescriptionGUID(null);
                // candidateAttributeTypeDef will be returned at end of method
                ret = candidateAttributeTypeDef;
            } else {
                // RH returned an AttributeTypeDef; cast it by category and compare against candidate
                // If match we will use RH TD; if not match we will generate audit log entry
                LOG.debug("{}: RepositoryHelper returned a TypeDef with name {} : {}", methodName, name, existingAttributeTypeDef);
                LOG.debug("{}: RepositoryHelper TypeDef has category {} ", methodName, existingAttributeTypeDef.getCategory());
                boolean typematch;
                Comparator comp = new Comparator();
                switch (existingAttributeTypeDef.getCategory()) {

                    case PRIMITIVE:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        PrimitiveDef existingPrimitiveDef = (PrimitiveDef) existingAttributeTypeDef;
                        PrimitiveDef newPrimitiveDef = (PrimitiveDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingPrimitiveDef, newPrimitiveDef);
                        break;

                    case COLLECTION:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        CollectionDef existingCollectionDef = (CollectionDef) existingAttributeTypeDef;
                        CollectionDef newCollectionDef = (CollectionDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingCollectionDef, newCollectionDef);
                        break;

                    case ENUM_DEF:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        EnumDef existingEnumDef = (EnumDef) existingAttributeTypeDef;
                        EnumDef newEnumDef = (EnumDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingEnumDef, newEnumDef);
                        break;

                    default:
                        LOG.error("{}: repository content manager found TypeDef has category {} ", methodName, existingAttributeTypeDef.getCategory());
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                        throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }

                // If compare matches use the known type
                if (typematch) {
                    // We will add the attributeTypeDef to the TypeDefGallery
                    LOG.debug("{}: return the repository content manager TypeDef with name {}", methodName, name);
                    candidateAttributeTypeDef = existingAttributeTypeDef;
                    ret = candidateAttributeTypeDef;

                } else {
                    // If compare failed generate AUDIT log entry and abandon
                    LOG.error("{}: repository content manager found clashing def with name {}", methodName, name);
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("atlasEntityDef", methodName, metadataCollectionId);

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: ret={}", methodName, ret);
        }
        return ret;


    }


    /*
     * Internal implementation of getAttributeTypeDefByName
     */
    private AttributeTypeDef _getAttributeTypeDefByName(String userId,
                                                        String name)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        final String methodName = "_getAttributeTypeDefByName";

        AttributeTypeDef ret;
        AttributeTypeDef candidateAttributeTypeDef;


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, name={})", methodName, userId, name);
        }

        // Strategy:
        // Check name is not null. null => throw exception
        // Use Atlas typedef store getByName()
        // If you get an AttributeTypeDef of a category that can be converted to OM AttributeTypeDef
        // then convert it and return the AttributeTypeDef.
        // If Atlas type is not of a category that can be converted to an OM AttributeTypeDef then return throw TypeDefNotKnownException.

        if (name == null) {
            LOG.error("{}: Cannot get Atlas type with null name", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Find out whether we are dealing with a PRIMITIVE Atlas type - i.e. a system type.
        // If so get it from the RCM rather than Atlas...
        PrimitiveDefCategory omrs_primitive_category = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(name);
        if (omrs_primitive_category != OM_PRIMITIVE_TYPE_UNKNOWN) {
            LOG.debug("{}: type name {} relates to a PRIMITIVE type in Atlas", methodName, name);
            // Ask RepositoryContentManager whether there is an AttributeTypeDef with supplied name
            String source = metadataCollectionId;
            AttributeTypeDef existingAttributeTypeDef;
            try {
                existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, name);
            } catch (OMRSLogicErrorException e) {
                LOG.error("{}: caught exception from repository helper for type name {}", methodName, name, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            return existingAttributeTypeDef;
        }

        // Find out whether we are dealing with a COLLECTION type - i.e. a system type.
        // If so get it from the RCM rather than Atlas...
        String OM_ARRAY_PREFIX = "array<";
        String OM_ARRAY_SUFFIX = ">";
        boolean isArrayType = false;

        String OM_MAP_PREFIX = "map<";
        String OM_MAP_SUFFIX = ">";
        boolean isMapType = false;

        boolean isCollectionType = false;

        if (name.startsWith(OM_ARRAY_PREFIX) && name.endsWith(OM_ARRAY_SUFFIX))
            isArrayType = true;
        if (name.startsWith(OM_MAP_PREFIX) && name.endsWith(OM_MAP_SUFFIX))
            isMapType = true;
        if (isArrayType || isMapType)
            isCollectionType = true;

        if (isCollectionType) {
            LOG.debug("{}: type name {} relates to a COLLECTION type in Atlas", methodName, name);
            // Ask RepositoryContentManager whether there is an AttributeTypeDef with supplied name
            String source = metadataCollectionId;
            AttributeTypeDef existingAttributeTypeDef;
            try {
                existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, name);
            } catch (OMRSLogicErrorException e) {
                LOG.error("{}: caught exception from repository helper for type name {}", methodName, name, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            return existingAttributeTypeDef;
        }

        // Will only reach this point if not an Atlas system attribute...

        // Look in the Atlas type registry
        AtlasBaseTypeDef abtd = typeRegistry.getTypeDefByName(name);


        // Separate null check ensures we have covered both cases (registry and store)
        if (abtd == null) {

            LOG.debug("{}: received null return from Atlas getByName using name {}", methodName, name);
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(name, "unknown", "name", methodName, metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // From this point, we know that abtd is non-null


        // Find the category of the Atlas typedef and invoke the relevant conversion method.
        // The only Atlas type categories that we can convert to OM AttributeTypeDef are:
        // PRIMITIVE: ENUM: ARRAY: MAP:
        // Anything else we will bounce and return null.
        // This will populate TDBC - you then need to retrieve the TD and return it.
        TypeCategory atlasCat = abtd.getCategory();
        switch (atlasCat) {
            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
                // For any of these categories get a candidate ATD
                AtlasBaseTypeDefMapper abtdMapper = new AtlasBaseTypeDefMapper(abtd);
                candidateAttributeTypeDef = abtdMapper.toAttributeTypeDef();
                break;
            case ENTITY:
            case RELATIONSHIP:
            case CLASSIFICATION:
            case STRUCT:
            case OBJECT_ID_TYPE:
            default:
                LOG.debug("{}: Atlas type has category cannot be converted to OM AttributeTypeDef, category {} ", methodName, atlasCat);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }

        if (candidateAttributeTypeDef == null) {
            LOG.debug("{}: received null return from attempt to convert AtlasBaseTypeDef to AttributeTypeDef", methodName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } else {
            // Finally, check if the converted attributeTypeDef is known by the repos helper and whether it matches exactly.
            // If it doesn't exist in RH, return the copy we got from the TDBC.
            // If it does exist in RH then perform a deep compare; exact match => return the ATD from the RH
            // If it does exist in RH but is not an exact match => audit log and return null;

            // Ask RepositoryContentManager whether there is an AttributeTypeDef with supplied name
            String source = metadataCollectionId;
            AttributeTypeDef existingAttributeTypeDef;
            try {
                existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, name);
            } catch (OMRSLogicErrorException e) {
                LOG.error("{}: caught exception from repository helper for type name {}", methodName, name, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            if (existingAttributeTypeDef == null) {
                // The RH does not have a typedef by the supplied name - use the candidateTypeDef
                LOG.debug("{}: repository content manager returned name not found - use the candidate AttributeTypeDef", methodName);
                candidateAttributeTypeDef.setDescriptionGUID(null);
                // candidateAttributeTypeDef will be returned at end of method
                ret = candidateAttributeTypeDef;
            } else {
                // RH returned an AttributeTypeDef; cast it by category and compare against candidate
                // If match we will use RH TD; if not match we will generate audit log entry
                LOG.debug("{}: RepositoryHelper returned a TypeDef with name {} : {}", methodName, name, existingAttributeTypeDef);
                boolean typematch;
                Comparator comp = new Comparator();
                switch (existingAttributeTypeDef.getCategory()) {
                    case PRIMITIVE:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        PrimitiveDef existingPrimitiveDef = (PrimitiveDef) existingAttributeTypeDef;
                        PrimitiveDef newPrimitiveDef = (PrimitiveDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingPrimitiveDef, newPrimitiveDef);
                        break;
                    case COLLECTION:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        CollectionDef existingCollectionDef = (CollectionDef) existingAttributeTypeDef;
                        CollectionDef newCollectionDef = (CollectionDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingCollectionDef, newCollectionDef);
                        break;
                    case ENUM_DEF:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        EnumDef existingEnumDef = (EnumDef) existingAttributeTypeDef;
                        EnumDef newEnumDef = (EnumDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingEnumDef, newEnumDef);
                        break;
                    default:
                        LOG.debug("{}: repository content manager found TypeDef has category {} ", methodName, existingAttributeTypeDef.getCategory());
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("name", methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }

                // If compare matches use the known type
                if (typematch) {
                    // We will add the attributeTypeDef to the TypeDefGallery
                    LOG.debug("{}: return the repository content manager TypeDef with name {}", methodName, name);
                    candidateAttributeTypeDef = existingAttributeTypeDef;
                    ret = candidateAttributeTypeDef;
                } else {
                    // If compare failed generate AUDIT log entry and abandon
                    LOG.debug("{}: repository content manager found clashing def with name {}", methodName, name);
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
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
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}: ret={}", methodName, ret);
        }
        return ret;
    }

    /*
     * Utility methods to update TypeDefs
     */
    private TypeDef updateTypeDefAddOptions(TypeDef             typeDefToModify,
                                            Map<String, String> typeDefOptions)
    {
        Map<String, String> updatedOptions = typeDefToModify.getOptions();
        if (typeDefOptions != null) {
            updatedOptions.putAll(typeDefOptions);
        }
        typeDefToModify.setOptions(updatedOptions);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefDeleteOptions(TypeDef             typeDefToModify,
                                               Map<String, String> typeDefOptions)
    {
        Map<String, String> updatedOptions = typeDefToModify.getOptions();
        if (typeDefOptions != null) {
            for (String s : typeDefOptions.keySet()) {
                updatedOptions.remove(s);
            }
        }
        typeDefToModify.setOptions(updatedOptions);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefUpdateOptions(TypeDef             typeDefToModify,
                                               Map<String, String> typeDefOptions)
    {
        typeDefToModify.setOptions(typeDefOptions);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefAddAttributes(TypeDef                typeDefToModify,
                                               List<TypeDefAttribute> typeDefAttributes)
            throws
            PatchErrorException
    {
        final String methodName = "updateTypeDefAddAttributes";

        List<TypeDefAttribute> updatedAttributes = typeDefToModify.getPropertiesDefinition();
        // This operation must be additive only - check each attribute to be added does not already exist
        if (typeDefAttributes != null) {
            if (updatedAttributes == null) {
                updatedAttributes = typeDefAttributes;
            } else {
                for (TypeDefAttribute tda : typeDefAttributes) {
                    String newAttrName = tda.getAttributeName();
                    boolean nameClash = false;
                    for (TypeDefAttribute existingAttr : updatedAttributes) {
                        if (existingAttr.getAttributeName().equals(newAttrName)) {
                            // There is a name clash - error
                            nameClash = true;
                            break;
                        }
                    }
                    if (!nameClash) {
                        // add the new attribute
                        updatedAttributes.add(tda);
                    } else {
                        // exception
                        LOG.error("{}: Cannot add attribute {} because it already exists", methodName, newAttrName);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(typeDefToModify.getName(), typeDefToModify.getGUID(), "typeDefToModify", methodName, repositoryName, typeDefToModify.toString());

                        throw new PatchErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                "updateTypeDefAddAttributes",
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }
                }
            }
        }
        typeDefToModify.setPropertiesDefinition(updatedAttributes);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefUpdateDescriptions(TypeDef typeDefToModify,
                                                    String  description,
                                                    String  descriptionGUID)
    {
        typeDefToModify.setDescription(description);
        typeDefToModify.setDescriptionGUID(descriptionGUID);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefAddExternalStandards(TypeDef                       typeDefToModify,
                                                      List<ExternalStandardMapping> externalStandardMappings)
    {
        List<ExternalStandardMapping> updatedExternalStandardMappings = typeDefToModify.getExternalStandardMappings();
        if (externalStandardMappings != null) {
            updatedExternalStandardMappings.addAll(externalStandardMappings);
        }
        typeDefToModify.setExternalStandardMappings(updatedExternalStandardMappings);
        return typeDefToModify;
    }


    private TypeDef updateTypeDefDeleteExternalStandards(TypeDef                       typeDefToModify,
                                                         List<ExternalStandardMapping> externalStandardMappings)
            throws
            PatchErrorException
    {

        final String methodName = "updateTypeDefDeleteExternalStandards";

        List<ExternalStandardMapping> updatedExternalStandardMappings = typeDefToModify.getExternalStandardMappings();
        if (externalStandardMappings != null) {
            if (updatedExternalStandardMappings == null) {
                // there are no existing mappings - exception
                LOG.error("{}: Cannot delete external standard mappings because none exist", methodName);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeDefToModify.getName(), typeDefToModify.getGUID(), "typeDefToModify", methodName, repositoryName, typeDefToModify.toString());

                throw new PatchErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } else {
                /*
                 * There are existing mappings - find each mapping to be deleted and remove it from the list
                 * Our definition of equality is that all 3 components of the ExternalStandardMapping must
                 * match - the optional fields must be present and match or not be present in both objects,
                 * so we just test equality including equality of null == null. Meanwhile, the mandatory field
                 * must match. The test is therefore actually quite simple.
                 */

                // This loop is tolerant i.e it does not raise an error - if it does not find the mapping to delete
                for (ExternalStandardMapping esm : externalStandardMappings) {
                    // Find the corresponding member of the existing list
                    for (ExternalStandardMapping existingESM : updatedExternalStandardMappings) {
                        if (existingESM.getStandardTypeName().equals(esm.getStandardTypeName()) &&
                                existingESM.getStandardOrganization().equals(esm.getStandardOrganization()) &&
                                existingESM.getStandardName().equals(esm.getStandardName())) {
                            // matching entry found - remove the entry from the list
                            updatedExternalStandardMappings.remove(existingESM);
                            // no break - if there are multiple matching entries delete them all
                        }
                    }
                }
            }
        }
        typeDefToModify.setExternalStandardMappings(updatedExternalStandardMappings);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefUpdateExternalStandards(TypeDef                       typeDefToModify,
                                                         List<ExternalStandardMapping> externalStandardMappings)
    {
        typeDefToModify.setExternalStandardMappings(externalStandardMappings);
        return typeDefToModify;
    }

    // Helper method
    // Deliberately public - needed by Event Mapper
    public Relationship _getRelationship(String  userId,
                                         String  guid,
                                         boolean includeDeletedRelationships)
            throws
            RepositoryErrorException,
            RelationshipNotKnownException

    {


        final String methodName = "_getRelationship";
        final String guidParameterName = "guid";

        AtlasRelationship atlasRelationship;
        try {

            atlasRelationship = getAtlasRelationshipById(guid, includeDeletedRelationships);

        } catch (AtlasBaseException e) {

            LOG.error("{}: Caught exception from Atlas {}", methodName, e.getMessage());
            OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(methodName, metadataCollectionId);

            throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("{}: Read from atlas relationship store; relationship {}", methodName, atlasRelationship);

        try {
            AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(this, userId, atlasRelationship, entityStore);

            Relationship omRelationship = atlasRelationshipMapper.toOMRelationship();
            LOG.debug("{}: returning relationship {}", methodName, omRelationship);
            return omRelationship;

        } catch (Exception e) {
            LOG.debug("{}: caught exception from mapper ", methodName, e.getMessage());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guidParameterName,
                    methodName,
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    // Utility: to get all the type names in the supertype hierarchy - including both sides of the F5 facade
    private Map<String, AtlasStructDef.AtlasAttributeDef> getAllEntityAttributeDefs(String typeName)
            throws
            TypeErrorException
    {
        Set<String> visitedNames = new HashSet<>();
        return getAllEntityAttributeDefs(typeName, visitedNames);
    }

    // Utility: to get all the type names in the supertype hierarchy - including both sides of the F5 facade
    private Map<String, AtlasStructDef.AtlasAttributeDef> getAllEntityAttributeDefs(String      typeName,
                                                                                    Set<String> visited)
            throws
            TypeErrorException
    {

        final String methodName = "getAllEntityAttributeDefs";

        Map<String, AtlasStructDef.AtlasAttributeDef> attributeDefMap = new HashMap<>();

        // add self
        visited.add(typeName);

        try {
            AtlasBaseTypeDef abtd = typeDefStore.getByName(typeName);
            if (abtd instanceof AtlasEntityDef) {
                AtlasEntityDef atlasEntityDef = (AtlasEntityDef) abtd;
                // add attributes for self
                List<AtlasStructDef.AtlasAttributeDef> myAttrDefs = atlasEntityDef.getAttributeDefs();
                if (myAttrDefs != null && !myAttrDefs.isEmpty()) {
                    for (AtlasStructDef.AtlasAttributeDef myAttrDef : myAttrDefs) {
                        // Clone the attr def - we do not wish to modify the original typedef
                        AtlasStructDef.AtlasAttributeDef attrDef = new AtlasStructDef.AtlasAttributeDef(myAttrDef);
                        LOG.debug("{}: type {} contributes attribute {} ", methodName, typeName, attrDef.getName());
                        attributeDefMap.put(attrDef.getName(), attrDef);
                    }
                }
                Set<String> superTypeNames = atlasEntityDef.getSuperTypes();
                if (!superTypeNames.isEmpty()) {
                    Iterator<String> superTypeNameIterator = superTypeNames.iterator();
                    while (superTypeNameIterator.hasNext()) {

                        String superTypeName = superTypeNameIterator.next();
                        if (visited.contains(superTypeName)) {
                            // skip this supertype, you have already visited it
                            continue; // implicit - included for safety
                        } else {
                            Map<String, AtlasStructDef.AtlasAttributeDef> inheritedAttrDefs = getAllEntityAttributeDefs(superTypeName, visited);

                            // check the returned attrdefs carefully to see whether they are more stringent than the current map entry
                            if (inheritedAttrDefs != null && !inheritedAttrDefs.isEmpty()) {
                                Set<String> mapNames = inheritedAttrDefs.keySet();
                                Iterator<String> mapIterator = mapNames.iterator();
                                while (mapIterator.hasNext()) {
                                    String attrName = mapIterator.next();
                                    AtlasStructDef.AtlasAttributeDef attrDef = inheritedAttrDefs.get(attrName);
                                    AtlasStructDef.AtlasAttributeDef.Cardinality cardinality = attrDef.getCardinality();
                                    boolean isOptional = attrDef.getIsOptional();
                                    int valuesMinCount = attrDef.getValuesMinCount();
                                    int valuesMaxCount = attrDef.getValuesMaxCount();
                                    LOG.debug("{}: type {} inherits from {} attribute name {} has cardinality {} isOptional {} valuesMinCount {} valuesMaxCount {}", methodName, typeName, superTypeName, attrName, cardinality, isOptional, valuesMinCount, valuesMaxCount);
                                    // add attrdef to current map if not exists or cardinality is more stringent than existing def
                                    AtlasStructDef.AtlasAttributeDef knownAttr = attributeDefMap.get(attrName);
                                    if (knownAttr != null) {
                                        // compare constraints and keep the strictest
                                        if (knownAttr.getValuesMinCount() < valuesMinCount)
                                            knownAttr.setValuesMinCount(valuesMinCount);
                                        if (knownAttr.getValuesMaxCount() > valuesMaxCount)
                                            knownAttr.setValuesMaxCount(valuesMaxCount);
                                        if (!isOptional)
                                            knownAttr.setIsOptional(false);
                                    } else {
                                        // add the inherited attribute def to the map
                                        attributeDefMap.put(attrName, attrDef);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                LOG.error("{}: Found a typedef that is not an AtlasEntityDef ", methodName);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_HIERARCHY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        } catch (AtlasBaseException e) {
            LOG.error("{}: hit an exception {} ", methodName, e);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_HIERARCHY;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, methodName, repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        return attributeDefMap;
    }


    /*
     * Utility method to convert a Relationship to an AtlasRelationship.
     * Needs the RelationshipDef to find the propagation rule
     */
    private AtlasRelationship convertOMRelationshipToAtlasRelationship(Relationship    omRelationship,
                                                                       boolean         useExistingGUID,
                                                                       RelationshipDef relationshipDef)
            throws
            StatusNotSupportedException,
            RepositoryErrorException,
            TypeErrorException
    {


        final String methodName = "convertOMRelationshipToAtlasRelationship";

        /* Construct an AtlasRelationship
         * An AtlasRelationship has:
         * String              typeName
         * Map<String, Object> attributes
         * String              guid
         * AtlasObjectID       end1
         * AtlasObjectID       end2
         * String              label
         * PropagateTags       propagateTags
         * Status              status
         * String              createdBy
         * String              updatedBy
         * Date                createTime
         * Date                updateTime
         * Long                version
         */

        AtlasRelationship atlasRelationship = new AtlasRelationship();
        atlasRelationship.setTypeName(omRelationship.getType().getTypeDefName());

        /* GUID is set by Atlas to nextInternalID - you must leave it for a new AtlasRelationship,
         * unless you really want to reuse a particular GUID in which case the useGUID parameter will
         * have been set to true and the GUID to use will have been set in the OM Relationship...
         */
        if (useExistingGUID)
            atlasRelationship.setGuid(omRelationship.getGUID());

        InstanceStatus omStatus = omRelationship.getStatus();
        AtlasRelationship.Status atlasRelationshipStatus;
        switch (omStatus) {
            // AtlasEntity.Status can only be either { ACTIVE | DELETED }
            case DELETED:
                atlasRelationshipStatus = AtlasRelationship.Status.DELETED;
                break;
            case ACTIVE:
                atlasRelationshipStatus = AtlasRelationship.Status.ACTIVE;
                break;
            default:
                // unsupportable status
                LOG.error("{}: Atlas does not support relationship status {}", methodName, omStatus);
                OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new StatusNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }
        atlasRelationship.setStatus(atlasRelationshipStatus);

        if (omRelationship.getInstanceProvenanceType() != null) {
            atlasRelationship.setProvenanceType(omRelationship.getInstanceProvenanceType().getOrdinal());
        } else {
            atlasRelationship.setProvenanceType(InstanceProvenanceType.UNKNOWN.getOrdinal());
        }

        atlasRelationship.setCreatedBy(omRelationship.getCreatedBy());
        atlasRelationship.setUpdatedBy(omRelationship.getUpdatedBy());
        atlasRelationship.setCreateTime(omRelationship.getCreateTime());
        atlasRelationship.setUpdateTime(omRelationship.getUpdateTime());
        atlasRelationship.setVersion(omRelationship.getVersion());
        atlasRelationship.setHomeId(omRelationship.getMetadataCollectionId());

        // Set end1
        LOG.debug("{}: set end 1 of type {}", methodName, omRelationship.getEntityOneProxy().getType());
        AtlasObjectId end1 = new AtlasObjectId();
        end1.setGuid(omRelationship.getEntityOneProxy().getGUID());
        String end1TypeName = omRelationship.getEntityOneProxy().getType().getTypeDefName();
        if (FamousFive.omTypeRequiresSubstitution(end1TypeName)) {
            LOG.debug("[]: mapping F5 name for end 1 from  {} to {} ", methodName, end1TypeName, FamousFive.getAtlasTypeName(end1TypeName, null));
            end1TypeName = FamousFive.getAtlasTypeName(end1TypeName, null);
        }
        end1.setTypeName(end1TypeName);
        atlasRelationship.setEnd1(end1);


        // Set end2
        LOG.debug("{}: set end 2 of type {}", methodName, omRelationship.getEntityTwoProxy().getType());
        AtlasObjectId end2 = new AtlasObjectId();
        end2.setGuid(omRelationship.getEntityTwoProxy().getGUID());
        String end2TypeName = omRelationship.getEntityTwoProxy().getType().getTypeDefName();
        if (FamousFive.omTypeRequiresSubstitution(end2TypeName)) {
            LOG.debug("{}: mapping F5 name for end 2 from {} to {} ", methodName, end2TypeName, FamousFive.getAtlasTypeName(end2TypeName, null));
            end2TypeName = FamousFive.getAtlasTypeName(end2TypeName, null);
        }
        end2.setTypeName(end2TypeName);
        atlasRelationship.setEnd2(end2);


        atlasRelationship.setLabel(omRelationship.getType().getTypeDefName());           // Set the label to the type name of the relationship.

        // Set propagateTags
        ClassificationPropagationRule omPropRule = relationshipDef.getPropagationRule();
        AtlasRelationshipDef.PropagateTags atlasPropTags = convertOMPropagationRuleToAtlasPropagateTags(omPropRule);
        atlasRelationship.setPropagateTags(atlasPropTags);

        // Set attributes on AtlasRelationship
        // Map attributes from OM Relationship to AtlasRelationship
        InstanceProperties instanceProperties = omRelationship.getProperties();
        Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
        atlasRelationship.setAttributes(atlasAttrs);

        // AtlasRelationship should have been fully constructed by this point
        LOG.debug("{}: converted relationship {} ", methodName, atlasRelationship);
        return atlasRelationship;
    }


    /*
     * Utility method to convert an OM EntityDetail to an AtlasEntity
     * @param userId - the security context of the operation
     * @param entityDetail - the OM EntityDetail object to convert
     * @return atlasEntity that corresponds to supplied entityDetail
     * @throws TypeErrorException          - general type error
     * @throws RepositoryErrorException    - unknown error afflicting repository
     * @throws StatusNotSupportedException - status value is not supported
     */
    private AtlasEntity convertOMEntityDetailToAtlasEntity(String       userId,
                                                           EntityDetail entityDetail)
            throws
            TypeErrorException,
            RepositoryErrorException,
            StatusNotSupportedException
    {

        final String methodName = "convertOMEntityDetailToAtlasEntity";


        // Find the entity type
        String entityTypeName = entityDetail.getType().getTypeDefName();
        // This is the raw type name - if it is an F5 name the _getTypeDefByName method below will convert it

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByName(userId, entityTypeName);
            if (typeDef == null || typeDef.getCategory() != ENTITY_DEF) {
                LOG.error("{}: Could not find entity def with name {} ", methodName, entityTypeName);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            AtlasEntity atlasEntity = new AtlasEntity();

            String atlasTypeName = entityTypeName;
            if (FamousFive.omTypeRequiresSubstitution(entityTypeName)) {
                atlasTypeName = FamousFive.getAtlasTypeName(entityTypeName, null);
            }
            atlasEntity.setTypeName(atlasTypeName);

            /* The AtlasEntity constructor sets an unassigned GUID (initialized to nextInternalId),
             * because normally Atlas would generate a new GUID.
             *
             * However, in this particular API we want to accept a supplied identifier (could be a
             * RID from IGC for example) and use that as the GUID.
             *
             * The Atlas connector will need to later be able to identify the home repo that this
             * object came from - as well as reconstitute that repo's identifier for the object -
             * which could be an IGC rid for example. The GUID is set as supplied and the homeId
             * is set to the metadataCollectionId of the home repository.
             *
             */

            atlasEntity.setGuid(entityDetail.getGUID());
            atlasEntity.setHomeId(entityDetail.getMetadataCollectionId());

            if (entityDetail.getInstanceProvenanceType() != null) {
                atlasEntity.setProvenanceType(entityDetail.getInstanceProvenanceType().getOrdinal());
            } else {
                atlasEntity.setProvenanceType(InstanceProvenanceType.UNKNOWN.getOrdinal());
            }


            InstanceStatus omStatus = entityDetail.getStatus();
            AtlasEntity.Status atlasEntityStatus;
            switch (omStatus) {
                // AtlasEntity.Status can only be either { ACTIVE | DELETED }
                case DELETED:
                    atlasEntityStatus = AtlasEntity.Status.DELETED;
                    break;
                case ACTIVE:
                    atlasEntityStatus = AtlasEntity.Status.ACTIVE;
                    break;
                default:
                    // unsupportable status
                    LOG.error("{}: Atlas does not support entity status {}", methodName, omStatus);
                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                            this.getClass().getName(),
                            repositoryName);
                    throw new StatusNotSupportedException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

            }
            atlasEntity.setStatus(atlasEntityStatus);


            atlasEntity.setCreatedBy(entityDetail.getCreatedBy());
            atlasEntity.setUpdatedBy(entityDetail.getUpdatedBy());
            atlasEntity.setCreateTime(entityDetail.getCreateTime());
            atlasEntity.setUpdateTime(entityDetail.getUpdateTime());
            atlasEntity.setVersion(entityDetail.getVersion());
            // Cannot set classifications yet - need to do that post-create to get the entity GUID

            // Map attributes from OM EntityDetail to AtlasEntity
            InstanceProperties instanceProperties = entityDetail.getProperties();
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
            atlasEntity.setAttributes(atlasAttrs);

            // AtlasEntity has been fully constructed

            return atlasEntity;

        } catch (TypeDefNotKnownException e) {

            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

    }


    /*
     * Internal method to formulate search for relationships matching the specified criteria
     * If relationshipTypeGUID is null then all Egeria relationship types are included.
     * If relationshipTypeGUID is specified then only the specified relationship type is included - there is no supertype/subtype hierarchy for relationship types.
     * If matchProperties are specified then these are validated (as being relevant to the type or types to be searched)
     * If status filtered is requested this is performed in the delegated-to search method.
     */

    private List<Relationship> findRelationshipsByMatchProperties(String               userId,
                                                                  String               relationshipTypeGUID,
                                                                  InstanceProperties   matchProperties,
                                                                  MatchCriteria        matchCriteria,
                                                                  List<InstanceStatus> limitResultsByStatus)
            throws
            PropertyErrorException,
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "findRelationshipsByMatchProperties";


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, relationshipTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    methodName, userId, relationshipTypeGUID, matchProperties, matchCriteria, limitResultsByStatus);
        }


        // If relationshipTypeGUID is not null then find the name of the type it refers to - this is used to filter by type

        List<TypeDef> relationshipTypesToSearch = new ArrayList<>();


        if (relationshipTypeGUID != null) {

            // Use the relationshipTypeGUID to retrieve the associated type name
            TypeDef typeDef;
            try {

                typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);

            } catch (TypeDefNotKnownException | RepositoryErrorException e) {

                LOG.error("{}: Caught exception from _getTypeDefByGUID", methodName, e);
                // handle below...
                typeDef = null;

            }
            if (typeDef == null) {

                LOG.debug("{}: could not retrieve typedef for guid {}", methodName, relationshipTypeGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, "relationshipTypeGUID", methodName, metadataCollectionId);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // Only include the given type
            // If typeGUID is not null, then the above cross-type search is filtered to include only the specified type.
            // There are no sub-types for relationship types.
            relationshipTypesToSearch.add(typeDef);

        } else {

            // relationshipTypeGUID == null, so this is a wild search - include all relationship types
            List<TypeDef> allRelationshipTypes;

            try {

                allRelationshipTypes = _findTypeDefsByCategory(userId, TypeDefCategory.RELATIONSHIP_DEF);

            } catch (RepositoryErrorException e) {

                LOG.error("{}: caught exception from _findTypeDefsByCategory", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("any EntityDef", "TypeDefCategory.RELATIONSHIP_DEF", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            if (allRelationshipTypes == null) {
                LOG.debug("{}: found no relationship types", methodName);
                return null;
            } else {
                relationshipTypesToSearch = allRelationshipTypes;
            }
        }


        /*
         * For the relationship type(s) to be search, validate matchProperties are valid and perform  the search.
         * If there are multiple types, the method will validate and search for each of those types and union the results.
         *
         */

        ArrayList<Relationship> returnRelationships = null;

        LOG.debug("{}: There are {} entity types defined", methodName, relationshipTypesToSearch.size());

        if (relationshipTypesToSearch.size() > 0) {

            // Iterate over the known relationship types performing a search for each...

            for (TypeDef typeDef : relationshipTypesToSearch) {
                LOG.debug("{}: checking relationship type {}", methodName, typeDef.getName());


                // If there are any matchProperties, validate that they are defined in the type to be searched

                if (matchProperties != null) {

                    /*
                     * Validate that the type has the specified properties...
                     * Whether matchCriteria is ALL | ANY | NONE we need ALL the match properties to be defined in the type definition
                     * For a property definition to match a property in the matchProperties, we need name and type to match.
                     */

                    // Find what properties are defined on the type; getAllDefinedProperties() will recurse up the supertype hierarchy
                    List<TypeDefAttribute> definedAttributes;
                    try {

                        definedAttributes = getAllDefinedProperties(userId, typeDef);

                    } catch (TypeDefNotKnownException e) {

                        LOG.error("{}: caught exception from property finder", methodName, e);

                        OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("find properties", "TypeDef", methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }

                    if (definedAttributes == null) {
                        // It is obvious that this type does not have the match properties - because it has no properties
                        // Proceed to the next relationship type
                        LOG.debug("{}: relationship type {} has no properties - will be ignored", methodName, typeDef.getName());
                        // continue;
                    } else {
                        // This type has properties...
                        // Iterate over the match properties, matching on name and type
                        Iterator<String> matchPropNames = matchProperties.getPropertyNames();
                        boolean allPropsDefined = true;
                        while (matchPropNames.hasNext()) {
                            String matchPropName = matchPropNames.next();
                            InstancePropertyValue matchPropValue = matchProperties.getPropertyValue(matchPropName);
                            if (matchPropValue == null) {

                                LOG.error("{}: match property with name {} has null value - not supported", methodName, matchPropName);

                                OMRSErrorCode errorCode = OMRSErrorCode.BAD_PROPERTY_FOR_INSTANCE;

                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(matchPropName, "matchProperties", methodName, "LocalAtlasOMRSMetadataCollection");

                                throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }

                            String matchPropType = matchPropValue.getTypeName();
                            LOG.debug("{}: matchProp has name {} type {}", methodName, matchPropName, matchPropType);
                            // Find the current match prop in the type def
                            boolean propertyDefined = false;
                            for (TypeDefAttribute defType : definedAttributes) {
                                AttributeTypeDef atd = defType.getAttributeType();
                                String defTypeName = atd.getName();
                                if (defType.getAttributeName().equals(matchPropName) && defTypeName.equals(matchPropType)) {
                                    // relationship type def has the current match property...
                                    LOG.debug("{}: relationship type {} has property name {} type {}", methodName, typeDef.getName(), matchPropName, matchPropType);
                                    propertyDefined = true;
                                    break;
                                }
                            }
                            if (!propertyDefined) {
                                // this property is missing from the def
                                LOG.debug("{}: relationship type {} does not have property name {} type {}", methodName, typeDef.getName(), matchPropName, matchPropType);
                                allPropsDefined = false;
                            }
                        }
                        if (!allPropsDefined) {
                            // At least one property in the match props is not defined on the type - skip the type
                            LOG.debug("{}: relationship type {} does not have all match properties - will be ignored", methodName, typeDef.getName());
                            //continue;
                        } else {
                            // The current type is suitable for a find of instances of this type.
                            LOG.debug("{}: relationship type {} will be searched", methodName, typeDef.getName());

                            // Extract the type guid and invoke a type specific search...
                            String typeDefGUID = typeDef.getGUID();

                            /* Do not pass on the offset and pageSize - these need to applied once on the aggregated result (from all types)
                             * So make this search as broad as possible - i.e. set offset to 0 and pageSze to MAX.
                             */

                            ArrayList<Relationship> relationshipsForCurrentType =
                                    findRelationshipsForTypeByMatchProperties(
                                            userId,
                                            typeDefGUID,
                                            matchProperties,
                                            matchCriteria,
                                            limitResultsByStatus);

                            if (relationshipsForCurrentType != null && !relationshipsForCurrentType.isEmpty()) {
                                if (returnRelationships == null) {
                                    returnRelationships = new ArrayList<>();
                                }
                                LOG.debug("{}: for type {} found {} entities", methodName, typeDef.getName(), relationshipsForCurrentType.size());

                                returnRelationships.addAll(relationshipsForCurrentType);

                            } else {
                                LOG.debug("{}: for type {} found no entities", methodName, typeDef.getName());
                            }
                        }
                    }
                }
            }
        }

        int resultSize = 0;
        if (returnRelationships != null)
            resultSize = returnRelationships.size();

        LOG.debug("{}: Atlas found {} entities", methodName, resultSize);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnEntities={}",
                    methodName, userId, relationshipTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, returnRelationships);
        }
        //return limitedReturnList;
        return returnRelationships;
    }


    /*
     * Internal method that performs a type specific search
     * This method performs a type specific search because the calling method validates match properties on a per type basis, then aggregates.
     * It would (otherwise) be possible to use within() and a set of type names, in the query, but given the above, individual type queries are issued.
     *
     */
    private ArrayList<Relationship> findRelationshipsForTypeByMatchProperties(String               userId,
                                                                              String               relationshipTypeGUID,
                                                                              InstanceProperties   matchProperties,
                                                                              MatchCriteria        matchCriteria,
                                                                              List<InstanceStatus> limitResultsByStatus)

            throws
            TypeErrorException,
            RepositoryErrorException

    {

        final String methodName = "findRelationshipsForTypeByMatchProperties";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    methodName, userId, relationshipTypeGUID, matchProperties, matchCriteria, limitResultsByStatus);
        }


        // Where a property is of type String the match value should be used as a substring (fuzzy) match.
        // For all other types of property the match value needs to be an exact match.

        // Use the relationshipTypeGUID to retrieve the type name
        TypeDef typeDef;
        try {

            typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);

        } catch (TypeDefNotKnownException | RepositoryErrorException e) {

            LOG.error("{}: Caught exception from _getTypeDefByGUID", methodName, e);
            // handle below...
            typeDef = null;

        }
        if (typeDef == null) {

            LOG.debug("{}: could not retrieve typedef for guid {}", methodName, relationshipTypeGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, "relationshipTypeGUID", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        String typeName = typeDef.getName();


        HashMap<String, String> propertySearchTerms = new HashMap<>();

        // Get the AtlasStructType so that we can find qualified Attribute Names
        AtlasRelationshipType atlasRelationshipType = typeRegistry.getRelationshipTypeByName(typeName);
        if (atlasRelationshipType == null) {

            LOG.error("{} relationship type {} not found in type registry", methodName, typeName);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, "relationshipTypeGUID", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } else {

            // The matchProperties was already validated by the caller.
            // Find the attributes' qualifiedNames....

            Iterator<String> matchPropNames = matchProperties.getPropertyNames();
            while (matchPropNames.hasNext()) {
                String matchPropName = matchPropNames.next();
                InstancePropertyValue matchPropValue = matchProperties.getPropertyValue(matchPropName);
                InstancePropertyCategory propCat = matchPropValue.getInstancePropertyCategory();
                switch (propCat) {
                    case PRIMITIVE:
                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) matchPropValue;
                        PrimitiveDefCategory pCat = ppv.getPrimitiveDefCategory();
                        String value;
                        if (pCat == OM_PRIMITIVE_TYPE_STRING) {
                            // This should do a contains match (rather than exact match) but it does not
                            // work yet - so for now this is functionally restricted to exact match.
                            value = "'" + ppv.getPrimitiveValue().toString() + "'";
                        } else {
                            value = "'" + ppv.getPrimitiveValue().toString() + "'";
                        }

                        // Get the qualified attribute name
                        String attributeQName;
                        try {
                            attributeQName = "'" + atlasRelationshipType.getQualifiedAttributeName(matchPropName) + "'";
                            propertySearchTerms.put(attributeQName, value);
                        } catch (AtlasBaseException e) {

                            LOG.error("{} relationship type {} did not return qualifiedName for attribute {}", methodName, typeName, matchPropName);

                            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ATTRIBUTE_NOT_FOUND;

                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(matchPropName, typeName, methodName, metadataCollectionId);

                            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                        }
                        break;
                    default:
                        // For anything other than primitive throw exception
                        LOG.error("{} cannot perform find operation for type {} with non-primitive match property {}", methodName, typeName, matchPropName);

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ATTRIBUTE_NOT_FOUND;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(matchPropName, typeName, methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }
            }
        }

        try {

            // This method goes directly against the graph with a gremlin traversal because the Atlas relationship store does not provide
            // a search mechanism and the DSL and searchParameters query mechanisms are entity specific.

            AtlasGraph graph = atlasGraphProvider.getGraphInstance();

            // Traversal never includes an offset, and always uses the max search limit.
            String limitString = Integer.toString(AtlasConfiguration.SEARCH_MAX_LIMIT.getInt());

            StringBuilder sb = new StringBuilder();
            sb.append("g.E().has('__typeName', '");
            sb.append(typeName);
            sb.append("')");

            // if there are properties to search include them...
            if (!propertySearchTerms.isEmpty()) {
                switch (matchCriteria) {
                    case ALL:
                        sb.append(".and(");
                        break;
                    case ANY:
                        sb.append(".or(");
                        break;
                    case NONE:
                        sb.append(".not(or(");
                        break;
                }
                Iterator<String> searchpropNames = propertySearchTerms.keySet().iterator();
                boolean first = true;
                while (searchpropNames.hasNext()) {
                    if (!first)
                        sb.append(',');
                    String pName = searchpropNames.next();
                    String pValue = propertySearchTerms.get(pName);
                    sb.append("has(");
                    sb.append(pName);
                    sb.append(",");
                    sb.append(pValue);
                    sb.append(")");
                    first = false;
                }
                switch (matchCriteria) {
                    case ALL:
                        sb.append(")");
                        break;
                    case ANY:
                        sb.append(")");
                        break;
                    case NONE:
                        sb.append("))");
                        break;
                }
            }

            // Complete the traversal
            sb.append(".dedup().limit(");
            sb.append(limitString);
            sb.append(").toList()");
            String queryStr = sb.toString();

            Object result = graph.executeGremlinScript(queryStr, false);

            EntityGraphRetriever entityRetriever = new EntityGraphRetriever(typeRegistry);

            ArrayList<Relationship> ret = null;
            if (result instanceof List && CollectionUtils.isNotEmpty((List) result)) {

                ret = new ArrayList<>();

                List queryResult = (List) result;
                Object firstElement = queryResult.get(0);

                if (firstElement instanceof AtlasEdge) {
                    for (Object element : queryResult) {
                        if (element instanceof AtlasEdge) {
                            AtlasEdge atlasEdge = (AtlasEdge) element;
                            AtlasRelationship atlasRelationship;
                            try {
                                atlasRelationship = entityRetriever.mapEdgeToAtlasRelationship(atlasEdge);
                            } catch (AtlasBaseException e) {
                                LOG.debug("{}: could not convert edge to relationship", methodName);

                                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.CANNOT_MAP_EDGE;

                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, methodName, metadataCollectionId);

                                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }

                            Relationship omRelationship;

                            try {
                                AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                                        this,
                                        userId,
                                        atlasRelationship,
                                        entityStore);

                                omRelationship = atlasRelationshipMapper.toOMRelationship();
                                LOG.debug("{}: om relationship {}", methodName, omRelationship);

                                // Post-filter status if necessary
                                if (limitResultsByStatus != null) {
                                    // Only include the relationship if its status matches one of the status values in the status filter
                                    boolean match = false;
                                    InstanceStatus relStatus = omRelationship.getStatus();
                                    for (InstanceStatus filterStatus : limitResultsByStatus) {
                                        if (filterStatus.equals(relStatus)) {
                                            // Status filtering matches so include the relationship
                                            match = true;
                                            break;
                                        }
                                    }
                                    if (match) {
                                        ret.add(omRelationship);
                                    }
                                } else {
                                    // No status filtering so always include the relationship
                                    ret.add(omRelationship);
                                }

                            } catch (Exception e) {
                                LOG.debug("{}: caught exception from mapper ", methodName, e.getMessage());
                                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.CANNOT_MAP_RELATIONSHIP;
                                String errorMessage = errorCode.getErrorMessageId()
                                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, methodName, metadataCollectionId);

                                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                        this.getClass().getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }


                        } else {
                            LOG.warn("{}: expected an AtlasEdge; found unexpected entry in result {}", methodName, element);
                        }
                    }

                } else {
                    LOG.warn("{}: expected an AtlasEdge; found unexpected entry in result {}", methodName, firstElement);
                }
            }

            LOG.debug("{}: result={}", methodName, ret);

            return ret;

        } catch (AtlasBaseException e) {
            LOG.error("{}: Atlas Gremlin execution threw exception {}", methodName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

    }

    /*
     * Internal method for relationship search by string search criteria
     */

    private List<Relationship> findRelationshipsBySearchCriteria(String               userId,
                                                                 String               relationshipTypeGUID,
                                                                 String               searchCriteria,
                                                                 List<InstanceStatus> limitResultsByStatus)
            throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "findRelationshipsBySearchCriteria";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {}(userId={}, searchCriteria={}, limitResultsByStatus={})",
                    methodName, userId, searchCriteria, limitResultsByStatus);
        }

        // Special case when searchCriteria is null or empty string - return an empty list
        if (searchCriteria == null || searchCriteria.equals("")) {
            // Nothing to search for
            LOG.debug("{}: no search criteria supplied, returning null", methodName);
            return null;
        }

        // This method is used from findRelationshipsByPropertyValue which needs to search string properties
        // only, with the string contained in searchCriteria.


        /*
         * If typeGUID is not specified the search applies to all relationship types. The method will therefore find
         * all the relationship types and do a query for each type, and union the results.
         * If typeGUID is specified then the above search logic is filtered to include only the specified (expected)
         * type and its sub-types.
         */

        List<TypeDef> relationshipTypesToSearch;


        // If relationshipTypeGUID is not null then find the type it refers to - this is used to filter by type

        if (relationshipTypeGUID != null) {

            TypeDef typeDef;
            // Use the relationshipTypeGUID to retrieve the associated type name
            try {

                typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);

            } catch (TypeDefNotKnownException | RepositoryErrorException e) {

                LOG.error("{}: Caught exception from _getTypeDefByGUID", methodName, e);
                // handle below...
                typeDef = null;
            }

            if (typeDef == null) {
                LOG.debug("{}: could not retrieve typedef for guid {}", methodName, relationshipTypeGUID);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipTypeGUID, "relationshipTypeGUID", methodName, metadataCollectionId);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            relationshipTypesToSearch = new ArrayList<>();
            relationshipTypesToSearch.add(typeDef);


        }

        else {

            // relationshipTypeGUID == null, perform wild search

            try {

                relationshipTypesToSearch = _findTypeDefsByCategory(userId, TypeDefCategory.RELATIONSHIP_DEF);

            } catch (RepositoryErrorException e) {

                LOG.error("{}: caught exception from _findTypeDefsByCategory", methodName, e);

                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("any RelationshipDef", "TypeDefCategory.RELATIONSHIP_DEF", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
        }

        if (relationshipTypesToSearch == null || relationshipTypesToSearch.isEmpty()) {
            LOG.debug("{}: no relationship types to be searched", methodName);
            return null;
        }

        ArrayList<Relationship> returnRelationships = null;

        LOG.debug("{}: There are {} relationship types defined", methodName, relationshipTypesToSearch.size());

        if (relationshipTypesToSearch.size() > 0) {

            // Iterate over the known relationship types performing a search for each...

            for (TypeDef typeDef : relationshipTypesToSearch) {

                String actualTypeName = typeDef.getName();

                LOG.debug("{}: checking relationship type {}", methodName, actualTypeName);

                // Extract the type guid and invoke a type specific search...
                String typeDefGUID = typeDef.getGUID();

                // This method operates on string properties only, so construct a matchProperties
                // comprising all and only the string properties, then invoke the find method.
                // Using the typeDef, parse the attribute definitions and construct match properties.

                // Retrieve the type def attributes from the type def
                InstanceProperties matchStringProperties = null;
                try {
                    List<TypeDefAttribute> attrDefs = getAllDefinedProperties(userId, typeDef);
                    if (attrDefs != null) {
                        for (TypeDefAttribute tda : attrDefs) {
                            AttributeTypeDef atd = tda.getAttributeType();
                            AttributeTypeDefCategory atdCat = atd.getCategory();
                            if (atdCat == PRIMITIVE) {
                                PrimitiveDef pDef = (PrimitiveDef) atd;
                                PrimitiveDefCategory pDefCat = pDef.getPrimitiveDefCategory();
                                if (pDefCat == OM_PRIMITIVE_TYPE_STRING) {
                                    // this is a string property...
                                    if (matchStringProperties == null) {
                                        // first string property...
                                        matchStringProperties = new InstanceProperties();
                                    }
                                    PrimitivePropertyValue ppv = new PrimitivePropertyValue();
                                    ppv.setPrimitiveDefCategory(pDefCat);
                                    ppv.setPrimitiveValue(searchCriteria);
                                    matchStringProperties.setProperty(tda.getAttributeName(), ppv);
                                }
                            }
                        }
                    }
                }
                catch (TypeDefNotKnownException e) {

                    LOG.error("{}: Could not retrieve properties of entity type with GUID {}", methodName, typeDefGUID, e);

                    OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("unknown", typeDefGUID, "relationship type GUID", methodName, repositoryName);

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

                ArrayList<Relationship> relationshipsForCurrentType =
                        findRelationshipsForTypeByMatchProperties(
                                userId,
                                typeDefGUID,
                                matchStringProperties,
                                MatchCriteria.ANY,
                                limitResultsByStatus);


                if (relationshipsForCurrentType != null && !relationshipsForCurrentType.isEmpty()) {
                    if (returnRelationships == null) {
                        returnRelationships = new ArrayList<>();
                    }
                    LOG.debug("{}: for type {} found {} relationships", methodName, typeDef.getName(), relationshipsForCurrentType.size());
                    returnRelationships.addAll(relationshipsForCurrentType);

                } else {
                    LOG.debug("{}: for type {} found no relationships", methodName, typeDef.getName());
                }

            }
        }

        int resultSize = 0;
        if (returnRelationships != null)
            resultSize = returnRelationships.size();

        LOG.debug("{}: Atlas found {} relationships", methodName, resultSize);


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {}(userId={}, searchCriteria={}, limitResultsByStatus={} : returnList={}",
                    methodName, userId, searchCriteria, limitResultsByStatus, returnRelationships);
        }
        return returnRelationships;
    }


}

