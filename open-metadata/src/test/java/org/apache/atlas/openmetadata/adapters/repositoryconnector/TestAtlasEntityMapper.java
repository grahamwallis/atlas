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

import org.odpi.openmetadata.repositoryservices.ffdc.exception.InvalidEntityException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotKnownException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

//import javax.print.DocFlavor;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


/*
 * Test the AtlasEntityMapper component of the AtlasConnector
 *
 * The constructor is trivial; it is the toOMEntityDetail, etc methods that are of primary interest.
 *
 */

public class TestAtlasEntityMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasEntityMapper.class);

    // Background types - these are used and reused across multiple tests
    private Map<String,TypeDef> backgroundEntityTypes = new HashMap<>();
    private Map<String,TypeDef> backgroundClassificationTypes = new HashMap<>();


    @BeforeClass
    public void setup() {

        TypeDef referenceable = new EntityDef();
        referenceable.setName("Referenceable");
        referenceable.setGUID(UUID.randomUUID().toString());
        backgroundEntityTypes.put(referenceable.getName(), referenceable);

        TypeDef asset = new EntityDef();
        asset.setName("Asset");
        asset.setGUID(UUID.randomUUID().toString());
        backgroundEntityTypes.put(referenceable.getName(), asset);

        TypeDef infrastructure = new EntityDef();
        infrastructure.setName("Infrastructure");
        infrastructure.setGUID(UUID.randomUUID().toString());
        backgroundEntityTypes.put(referenceable.getName(), infrastructure);

        TypeDef process = new EntityDef();
        process.setName("Process");
        process.setGUID(UUID.randomUUID().toString());
        backgroundEntityTypes.put(referenceable.getName(), process);

        TypeDef dataset = new EntityDef();
        dataset.setName("DataSet");
        dataset.setGUID(UUID.randomUUID().toString());
        backgroundEntityTypes.put(referenceable.getName(), dataset);

        // type1 entity def has no attributes defined
        EntityDef type1 = new EntityDef();
        type1.setName("type1");
        type1.setGUID(UUID.randomUUID().toString());
        //type1.setCategory(TypeDefCategory.ENTITY_DEF);
        backgroundEntityTypes.put(type1.getName(), type1);

        // type2 entity def has one string attribute called attr1 defined
        // This is a unique attribute - it should appear in an EntityProxy
        EntityDef type2 = new EntityDef();
        type2.setName("type2");
        type2.setGUID(UUID.randomUUID().toString());
        //type2.setCategory(TypeDefCategory.ENTITY_DEF);
        List<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute attr1Def = new TypeDefAttribute();
        attr1Def.setAttributeName("attr1");
        attr1Def.setUnique(true);
        AttributeTypeDef atd1 = new PrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
        atd1.setName(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING.getName());
        atd1.setGUID(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING.getGUID());
        attr1Def.setAttributeType(atd1);
        properties.add(attr1Def);
        type2.setPropertiesDefinition(properties);
        backgroundEntityTypes.put(type2.getName(), type2);


        ClassificationDef type10 = new ClassificationDef();
        type10.setName("type10");
        type10.setGUID(UUID.randomUUID().toString());
        type10.setCategory(TypeDefCategory.CLASSIFICATION_DEF);
        backgroundClassificationTypes.put(type10.getName(), type10);


        /*
         * Need to get valid responses from FamousFive, but cannot mock because static, so initialize it instead.
         * The following calls populate the GUIDs as the names are mapped.
         * This emulates the addition of the F5 types during OMRS startup.
         */
        FamousFive.getAtlasTypeName( referenceable.getName(),   referenceable.getGUID() );
        FamousFive.getAtlasTypeName( asset.getName(),           asset.getGUID() );
        FamousFive.getAtlasTypeName( infrastructure.getName(),  infrastructure.getGUID() );
        FamousFive.getAtlasTypeName( process.getName(),         process.getGUID() );
        FamousFive.getAtlasTypeName( dataset.getName(),         dataset.getGUID() );

    }


    /*
     * Each object provided by the data provider is an AtlasEntity and the expected result.
     *
     */
    public enum TestResult {
        OK,
        TypeErrorException,
        RepositoryErrorException,
        InvalidEntityException,
        OKForSummaryAndProxy        // if expected results get more complex then split this class into 2 or 3, by method under test
    }

    public class TestData {
        String                 testCaption;
        AtlasEntity            atlasEntity;
        TestResult             expectedResult;
    }

    @DataProvider(name = "provideAtlasEntity")
    public Object[][] provideData() {


        // 1. Test with no atlasEntity
        AtlasEntity atlasEntityNull = null;

        // 2. Test with atlasEntity with unknown type
        AtlasEntity atlasEntityUnknownType = new AtlasEntity();
        atlasEntityUnknownType.setTypeName("unknown-type");

        // 3. Test with atlasEntity with valid type
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("type1");

        // 4. Test with atlasEntity with null version
        AtlasEntity atlasEntityVersionNull = new AtlasEntity();
        atlasEntityVersionNull.setTypeName("type1");
        atlasEntityVersionNull.setVersion(null);

        // 5. Test with atlasEntity with homeId
        AtlasEntity atlasEntityHomeId = new AtlasEntity();
        atlasEntityHomeId.setTypeName("type1");
        atlasEntityHomeId.setVersion(13L);
        String a_homeId = "recognizable_metadata_collection_id";
        atlasEntityHomeId.setHomeId(a_homeId);
        // expected values
        Map<String,Object> expectedFields5 = new HashMap<>();
        expectedFields5.put("metadataCollectionId",a_homeId);

        // 6. Test with atlasEntity with null homeId
        AtlasEntity atlasEntityNullHomeId = new AtlasEntity();
        atlasEntityNullHomeId.setTypeName("type1");
        atlasEntityNullHomeId.setVersion(13L);
        atlasEntityNullHomeId.setHomeId(null);
        // expected values
        Map<String,Object> expectedFields6 = new HashMap<>();
        expectedFields6.put("metadataCollectionId",null);

        // 7. Test with atlasEntity with status DELETED
        AtlasEntity atlasEntityStatusDeleted = new AtlasEntity();
        atlasEntityStatusDeleted.setTypeName("type1");
        atlasEntityStatusDeleted.setVersion(13L);
        atlasEntityStatusDeleted.setStatus(AtlasEntity.Status.DELETED);
        // expected values
        Map<String,Object> expectedFields7 = new HashMap<>();
        expectedFields7.put("status", InstanceStatus.DELETED);

        // 8. Test with atlasEntity with classification
        AtlasEntity atlasEntityClassification = new AtlasEntity();
        atlasEntityClassification.setTypeName("type1");
        atlasEntityClassification.setVersion(13L);
        String entityGUID = UUID.randomUUID().toString();
        atlasEntityClassification.setGuid(entityGUID);
        List<AtlasClassification> atlasClassifications8 = new ArrayList<>();
        AtlasClassification atlasClassification8_1 = new AtlasClassification("type10");
        atlasClassification8_1.setEntityGuid(entityGUID);
        atlasClassifications8.add(atlasClassification8_1);
        atlasEntityClassification.setClassifications(atlasClassifications8);
        // expected values
        Map<String,Object> expectedFields8 = new HashMap<>();
        expectedFields8.put("entityGUID",entityGUID );
        expectedFields8.put("classification_name","type10");

        // Attributes - these are not represented in EntitySummary

        // 9. Test with atlasEntity with undefined attribute (i.e. attribute not in EntityDef)
        AtlasEntity atlasEntityWithUndefinedAttribute = new AtlasEntity();
        atlasEntityWithUndefinedAttribute.setTypeName("type1");
        atlasEntityWithUndefinedAttribute.setVersion(13L);
        String entityGUID9 = UUID.randomUUID().toString();
        atlasEntityWithUndefinedAttribute.setGuid(entityGUID9);
        // attributes
        Map<String, Object> atlasAttributes9 = new HashMap<>();
        String attr9_1 = "attr9_1-value";
        atlasAttributes9.put("attr9_1",attr9_1);
        atlasEntityWithUndefinedAttribute.setAttributes(atlasAttributes9);
        // expected values
        Map<String,Object> expectedFields9 = new HashMap<>();
        expectedFields9.put("attributes",null);

        // 10. Test with atlasEntity with defined attribute (i.e. attribute in EntityDef)
        AtlasEntity atlasEntityWithDefinedAttribute = new AtlasEntity();
        atlasEntityWithDefinedAttribute.setTypeName("type2");
        atlasEntityWithDefinedAttribute.setVersion(13L);
        String entityGUID10 = UUID.randomUUID().toString();
        atlasEntityWithDefinedAttribute.setGuid(entityGUID10);
        // attributes
        Map<String, Object> atlasAttributes10 = new HashMap<>();
        String valueAttr10_1 = "test10-attr1-value";
        atlasAttributes10.put("attr1", valueAttr10_1);
        atlasEntityWithDefinedAttribute.setAttributes(atlasAttributes10);
        // expected values
        Map<String,Object> expectedFields10 = new HashMap<>();
        InstanceProperties expectedProps10 = new InstanceProperties();
        PrimitivePropertyValue instancePropertyValue10_1 = new PrimitivePropertyValue();
        instancePropertyValue10_1.setPrimitiveDefCategory(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
        instancePropertyValue10_1.setTypeName("string");
        instancePropertyValue10_1.setTypeGUID(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING.getGUID());
        instancePropertyValue10_1.setPrimitiveValue(valueAttr10_1);
        expectedProps10.setProperty("attr1",instancePropertyValue10_1 );
        expectedFields10.put("attributes",expectedProps10);

        // 11. Test with atlasEntity with isProxy set to true
        AtlasEntity atlasEntityProxy = new AtlasEntity();
        atlasEntityProxy.setTypeName("type1");
        atlasEntityProxy.setVersion(13L);
        String homeId_11 = "recognizable_metadata_collection_id";
        atlasEntityProxy.setHomeId(homeId_11);
        atlasEntityProxy.setIsProxy(true);
        // expected values
        Map<String,Object> expectedFields11 = new HashMap<>();
        expectedFields5.put("metadataCollectionId",homeId_11);


        Object[][] test_data = new Object[][] {
                { "1. no entity",           atlasEntityNull,                   TestResult.RepositoryErrorException, null},
                { "2. entity unknown type", atlasEntityUnknownType,            TestResult.TypeErrorException,       null},
                { "3. entity OK",           atlasEntity,                       TestResult.OK,                       null},
                { "4. entity null version", atlasEntityVersionNull,            TestResult.InvalidEntityException,   null},
                { "5. entity homeId",       atlasEntityHomeId,                 TestResult.OK,                       expectedFields5},
                { "6. entity null homeId",  atlasEntityHomeId,                 TestResult.OK,                       expectedFields6},
                { "7. entity DELETED",      atlasEntityStatusDeleted,          TestResult.OK,                       expectedFields7},
                { "8. entity DELETED",      atlasEntityClassification,         TestResult.OK,                       expectedFields8},
                { "9. entity undef attr",   atlasEntityWithUndefinedAttribute, TestResult.OK,                       expectedFields9},
                { "10. entity def attr",    atlasEntityWithDefinedAttribute,   TestResult.OK,                       expectedFields10},
                { "11. entity isProxy",     atlasEntityProxy,                  TestResult.OKForSummaryAndProxy,     expectedFields11},

        };

        return test_data;

    }

    /*
     * Parameters to the test methods:
     * String              testCaption;
     * AtlasEntity         atlasEntity;
     * TestResult          expectedResult;
     * Map<String,Object>  expectedFields;
     */

    @Test(dataProvider = "provideAtlasEntity")
    public void test_ToEntitySummary(String                       testCaption,
                                     AtlasEntity                  atlasEntity,
                                     TestResult                   testResult,
                                     Map<String,Object>           expectedFields)
            throws
                TypeDefNotKnownException,
                RepositoryErrorException
    {

        LOG.debug("TEST: {}", testCaption);

        if (testResult == TestResult.OKForSummaryAndProxy) {
            testResult = TestResult.OK;
        }

        String userId = "test_user";

        /*
         * Set up mocks
         */

        // MetadataCollection
        LocalAtlasOMRSMetadataCollection metadataCollection = mock(LocalAtlasOMRSMetadataCollection.class);

        when(metadataCollection.getMetadataCollectionId()).thenReturn("mock_metadata_collection");

        if (backgroundEntityTypes != null) {
            for (TypeDef backgroundType : backgroundEntityTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).
                        thenReturn(backgroundType);
            }
        }
        if (backgroundClassificationTypes != null) {
            for (TypeDef backgroundType : backgroundClassificationTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).thenReturn(backgroundType);
            }
        }




        // Specifically testing EntitySummary

        EntitySummary omEntitySummary = null;

        LOG.debug("TestAtlasEntityMapper: Construct AtlasEntityMapper");
        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(metadataCollection, userId, atlasEntity);
            omEntitySummary = atlasEntityMapper.toEntitySummary();

        } catch (TypeErrorException e) {
            LOG.debug("TestAtlasEntityMapper: Caught TypeErrorException exception");
            if (testResult.equals(TestResult.TypeErrorException)) {
                LOG.debug("TestAtlasEntityMapper: Test Passed");
                return;
            }
            else {
                LOG.debug("TestAtlasEntityMapper: Exception was unexpected");
                fail();
            }
        } catch (RepositoryErrorException e) {
            LOG.debug("TestAtlasEntityMapper: Caught RepositoryErrorException exception");
            if (testResult.equals(TestResult.RepositoryErrorException)) {
                LOG.debug("TestAtlasEntityMapper: Test Passed");
                return;
            }
            else {
                LOG.debug("TestAtlasEntityMapper: Exception was unexpected");
                fail();
            }
        }
        catch (InvalidEntityException e) {
            LOG.debug("TestAtlasEntityMapper: Caught InvalidEntityException exception");
            if (testResult.equals(TestResult.InvalidEntityException)) {
                LOG.debug("TestAtlasEntityMapper: Test Passed");
                return;
            }
            else {
                LOG.debug("TestAtlasEntityMapper: Exception was unexpected");
                fail();
            }
        }
        // Should have returned an OM EntityDef
        assertNotNull(omEntitySummary);

        switch(testResult) {

            case OK:

                // OM EntitySummary should have same version as AtlasEntity
                assertEquals(omEntitySummary.getVersion(), atlasEntity.getVersion().longValue());

                // Check GUID
                if (expectedFields != null && expectedFields.get("entityGUID") != null) {
                    LOG.debug("TestAtlasEntityMapper: entity GUID {}", omEntitySummary.getGUID());
                    assertEquals(omEntitySummary.getGUID(), expectedFields.get("entityGUID"));
                }

                // Check the InstanceType -
                InstanceType instanceType = omEntitySummary.getType();
                String typeName = instanceType.getTypeDefName();
                assertEquals(typeName,atlasEntity.getTypeName());

                // Check metadataCollectionId
                assertEquals(omEntitySummary.getMetadataCollectionId(), atlasEntity.getHomeId());

                // Check status
                if (expectedFields != null && expectedFields.get("status") != null) {
                    LOG.debug("TestAtlasEntityMapper: status {}", omEntitySummary.getStatus());
                    assertEquals(omEntitySummary.getStatus(), expectedFields.get("status"));
                }

                // Check classifications
                if (expectedFields != null && expectedFields.get("classification_name") != null) {
                    LOG.debug("TestAtlasEntityMapper: classification name {}", omEntitySummary.getClassifications().get(0).getName());
                    assertEquals(omEntitySummary.getClassifications().get(0).getName(), expectedFields.get("classification_name"));
                }


                LOG.debug("TestAtlasEntityMapper: Test Passed");
                break;


            default:
                LOG.debug("TestAtlasEntityMapper: Unexpected result from mapper");
                fail();
                break;
        }
    }

    @Test(dataProvider = "provideAtlasEntity")
    public void test_ToEntityDetail(String                       testCaption,
                                    AtlasEntity                  atlasEntity,
                                    TestResult                   testResult,
                                    Map<String,Object>           expectedFields)
            throws
            TypeDefNotKnownException,
            RepositoryErrorException
    {

        LOG.debug("TEST: {}", testCaption);

        if (testResult == TestResult.OKForSummaryAndProxy) {
            testResult = TestResult.InvalidEntityException;
        }

        final String methodName = "test_ToEntityDetail";

        String userId = "test_user";

        /*
         * Set up mocks
         */

        // MetadataCollection
        LocalAtlasOMRSMetadataCollection metadataCollection = mock(LocalAtlasOMRSMetadataCollection.class);

        when(metadataCollection.getMetadataCollectionId()).thenReturn("mock_metadata_collection");

        if (backgroundEntityTypes != null) {
            for (TypeDef backgroundType : backgroundEntityTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).
                        thenReturn(backgroundType);
            }
        }
        if (backgroundClassificationTypes != null) {
            for (TypeDef backgroundType : backgroundClassificationTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).thenReturn(backgroundType);
            }
        }

        // Specifically testing EntityDetail

        EntityDetail omEntityDetail = null;

        LOG.debug(methodName+": Construct AtlasEntityMapper");
        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(metadataCollection, userId, atlasEntity);
            omEntityDetail = atlasEntityMapper.toEntityDetail();

        } catch (TypeErrorException e) {
            LOG.debug(methodName+": Caught TypeErrorException exception");
            if (testResult.equals(TestResult.TypeErrorException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        } catch (RepositoryErrorException e) {
            LOG.debug(methodName+": Caught RepositoryErrorException exception");
            if (testResult.equals(TestResult.RepositoryErrorException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        }
        catch (InvalidEntityException e) {
            LOG.debug(methodName+": Caught InvalidEntityException exception");
            if (testResult.equals(TestResult.InvalidEntityException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        }
        // Should have returned an OM EntityDef
        assertNotNull(omEntityDetail);

        switch(testResult) {

            case OK:

                // OM EntitySummary should have same version as AtlasEntity
                assertEquals(omEntityDetail.getVersion(), atlasEntity.getVersion().longValue());

                // Check GUID
                if (expectedFields != null && expectedFields.get("entityGUID") != null) {
                    LOG.debug(methodName+": entity GUID {}", omEntityDetail.getGUID());
                    assertEquals(omEntityDetail.getGUID(), expectedFields.get("entityGUID"));
                }

                // Check the InstanceType -
                InstanceType instanceType = omEntityDetail.getType();
                String typeName = instanceType.getTypeDefName();
                assertEquals(typeName,atlasEntity.getTypeName());

                // Check metadataCollectionId
                assertEquals(omEntityDetail.getMetadataCollectionId(), atlasEntity.getHomeId());

                // Check status
                if (expectedFields != null && expectedFields.get("status") != null) {
                    LOG.debug(methodName+": status {}", omEntityDetail.getStatus());
                    assertEquals(omEntityDetail.getStatus(), expectedFields.get("status"));
                }

                // Check classifications
                if (expectedFields != null && expectedFields.get("classification_name") != null) {
                    LOG.debug(methodName+": classification name {}", omEntityDetail.getClassifications().get(0).getName());
                    assertEquals(omEntityDetail.getClassifications().get(0).getName(), expectedFields.get("classification_name"));
                }

                // check attributes
                if (expectedFields != null && expectedFields.get("attributes") != null) {
                    InstanceProperties actualProperties = omEntityDetail.getProperties();
                    LOG.debug(methodName+": actualProperties {}", actualProperties);
                    InstanceProperties expectedProperties = (InstanceProperties)(expectedFields.get("attributes"));
                    LOG.debug(methodName+": expectedProperties {}", expectedProperties);
                    // Test each expected property...
                    Iterator<String> expectedPropertyNames = expectedProperties.getPropertyNames();
                    while (expectedPropertyNames.hasNext()) {
                        String propName = expectedPropertyNames.next();
                        InstancePropertyValue expectedPropValue = expectedProperties.getPropertyValue(propName);
                        InstancePropertyValue actualPropValue   = actualProperties.getPropertyValue(propName);
                        assertEquals(expectedPropValue.getInstancePropertyCategory()  , actualPropValue.getInstancePropertyCategory());
                        LOG.debug(methodName+": expected type {}",expectedPropValue.getTypeName());
                        LOG.debug(methodName+": actual type {}", actualPropValue.getTypeName());
                        assertEquals(expectedPropValue.getTypeName()  , actualPropValue.getTypeName());
                        assertEquals(expectedPropValue.getTypeGUID()  , actualPropValue.getTypeGUID());
                    }
                }


                LOG.debug(methodName+": Test Passed");
                break;


            default:
                LOG.debug(methodName+": Unexpected result from mapper");
                fail();
                break;
        }
    }


    @Test(dataProvider = "provideAtlasEntity")
    public void test_ToEntityProxy(String                       testCaption,
                                   AtlasEntity                  atlasEntity,
                                   TestResult                   testResult,
                                   Map<String,Object>           expectedFields)
            throws
            TypeDefNotKnownException,
            RepositoryErrorException
    {

        LOG.debug("TEST: {}", testCaption);

        if (testResult == TestResult.OKForSummaryAndProxy) {
            testResult = TestResult.OK;
        }

        final String methodName = "test_ToEntityProxy";

        String userId = "test_user";

        /*
         * Set up mocks
         */

        // MetadataCollection
        LocalAtlasOMRSMetadataCollection metadataCollection = mock(LocalAtlasOMRSMetadataCollection.class);

        when(metadataCollection.getMetadataCollectionId()).thenReturn("mock_metadata_collection");

        if (backgroundEntityTypes != null) {
            for (TypeDef backgroundType : backgroundEntityTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).
                        thenReturn(backgroundType);
            }
        }
        if (backgroundClassificationTypes != null) {
            for (TypeDef backgroundType : backgroundClassificationTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).thenReturn(backgroundType);
            }
        }

        // Specifically testing EntityProxy

        EntityProxy omEntityProxy = null;

        LOG.debug(methodName+": Construct AtlasEntityMapper");
        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(metadataCollection, userId, atlasEntity);
            omEntityProxy = atlasEntityMapper.toEntityProxy();

        } catch (TypeErrorException e) {
            LOG.debug(methodName+": Caught TypeErrorException exception");
            if (testResult.equals(TestResult.TypeErrorException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        } catch (RepositoryErrorException e) {
            LOG.debug(methodName+": Caught RepositoryErrorException exception");
            if (testResult.equals(TestResult.RepositoryErrorException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        }
        catch (InvalidEntityException e) {
            LOG.debug(methodName+": Caught InvalidEntityException exception");
            if (testResult.equals(TestResult.InvalidEntityException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        }
        // Should have returned an OM EntityDef
        assertNotNull(omEntityProxy);

        switch(testResult) {

            case OK:

                // OM EntitySummary should have same version as AtlasEntity
                assertEquals(omEntityProxy.getVersion(), atlasEntity.getVersion().longValue());

                // Check GUID
                if (expectedFields != null && expectedFields.get("entityGUID") != null) {
                    LOG.debug(methodName+": entity GUID {}", omEntityProxy.getGUID());
                    assertEquals(omEntityProxy.getGUID(), expectedFields.get("entityGUID"));
                }

                // Check the InstanceType -
                InstanceType instanceType = omEntityProxy.getType();
                String typeName = instanceType.getTypeDefName();
                assertEquals(typeName,atlasEntity.getTypeName());

                // Check metadataCollectionId
                assertEquals(omEntityProxy.getMetadataCollectionId(), atlasEntity.getHomeId());

                // Check status
                if (expectedFields != null && expectedFields.get("status") != null) {
                    LOG.debug(methodName+": status {}", omEntityProxy.getStatus());
                    assertEquals(omEntityProxy.getStatus(), expectedFields.get("status"));
                }

                // Check classifications
                if (expectedFields != null && expectedFields.get("classification_name") != null) {
                    LOG.debug(methodName+": classification name {}", omEntityProxy.getClassifications().get(0).getName());
                    assertEquals(omEntityProxy.getClassifications().get(0).getName(), expectedFields.get("classification_name"));
                }

                // check attributes
                if (expectedFields != null && expectedFields.get("attributes") != null) {
                    InstanceProperties actualProperties = omEntityProxy.getUniqueProperties();
                    LOG.debug(methodName+": actualProperties {}", actualProperties);
                    InstanceProperties expectedProperties = (InstanceProperties)(expectedFields.get("attributes"));
                    LOG.debug(methodName+": expectedProperties {}", expectedProperties);

                    // Test each expected property...
                    Iterator<String> expectedPropertyNames = expectedProperties.getPropertyNames();
                    while (expectedPropertyNames.hasNext()) {
                        String propName = expectedPropertyNames.next();
                        // For entityProxy need to filter out any non-unique attributes...
                        InstanceType instanceType1 = omEntityProxy.getType();
                        String typeName1 = instanceType1.getTypeDefName();
                        TypeDef td1 = backgroundEntityTypes.get(typeName1);
                        List<TypeDefAttribute> typeDefAttributes = td1.getPropertiesDefinition();
                        for (TypeDefAttribute tda : typeDefAttributes ) {
                            if (tda.getAttributeType().getName().equals(propName)) {
                                // check whether unique - if so check it - otherwise skip this attribute
                                if (tda.isUnique()) {
                                    InstancePropertyValue expectedPropValue = expectedProperties.getPropertyValue(propName);
                                    InstancePropertyValue actualPropValue   = actualProperties.getPropertyValue(propName);
                                    assertEquals(expectedPropValue.getInstancePropertyCategory()  , actualPropValue.getInstancePropertyCategory());
                                    LOG.debug(methodName+": expected type {}",expectedPropValue.getTypeName());
                                    LOG.debug(methodName+": actual type {}", actualPropValue.getTypeName());
                                    assertEquals(expectedPropValue.getTypeName()  , actualPropValue.getTypeName());
                                    assertEquals(expectedPropValue.getTypeGUID()  , actualPropValue.getTypeGUID());
                                }
                            }
                        }
                    }
                }


                LOG.debug(methodName+": Test Passed");
                break;


            default:
                LOG.debug(methodName+": Unexpected result from mapper");
                fail();
                break;
        }
    }

}