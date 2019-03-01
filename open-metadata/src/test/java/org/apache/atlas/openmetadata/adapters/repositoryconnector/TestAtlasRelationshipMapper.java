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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class TestAtlasRelationshipMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasRelationshipMapper.class);

    // Background types - these are used and reused across multiple tests

    private Map<String,TypeDef> backgroundEntityTypes = new HashMap<>();

    // deliberately vague about the typedefs - they will generally be relationshipDefs - but not always for error testing
    private Map<String,TypeDef> backgroundRelationshipTypes = new HashMap<>();
    private Map<String,AtlasEntity.AtlasEntityWithExtInfo> backgroundEntities = new HashMap<>();

    @BeforeClass
    public void setup() {

        TypeDef referenceable = new EntityDef();
        referenceable.setGUID(UUID.randomUUID().toString());
        referenceable.setName("Referenceable");
        backgroundEntityTypes.put(referenceable.getName(), referenceable);

        TypeDef asset = new EntityDef();
        asset.setGUID(UUID.randomUUID().toString());
        asset.setName("Asset");
        backgroundEntityTypes.put(asset.getName(), asset);

        TypeDef infrastructure = new EntityDef();
        infrastructure.setGUID(UUID.randomUUID().toString());
        infrastructure.setName("Infrastructure");
        backgroundEntityTypes.put(infrastructure.getName(), infrastructure);

        TypeDef process = new EntityDef();
        process.setGUID(UUID.randomUUID().toString());
        process.setName("Process");
        backgroundEntityTypes.put(process.getName(), process);

        TypeDef dataset = new EntityDef();
        dataset.setGUID(UUID.randomUUID().toString());
        dataset.setName("DataSet");
        backgroundEntityTypes.put(dataset.getName(), dataset);

        TypeDef type1 = new EntityDef();
        type1.setGUID(UUID.randomUUID().toString());
        type1.setName("type1");
        backgroundEntityTypes.put(type1.getName(), type1);

        TypeDef type2 = new EntityDef();
        type2.setGUID(UUID.randomUUID().toString());
        type2.setName("type2");
        backgroundEntityTypes.put(type2.getName(), type2);

        // type20 has no category - it should cause failures XX THIS IS NOW PREVENTED BY EGERIA CHANGE TO abstract TypeDef
        //TypeDef type20 = new TypeDef();
        //type20.setName("type20");
        //type20.setGUID(UUID.randomUUID().toString());
        //backgroundRelationshipTypes.put(type20.getName(), type20);

        // type21 is a TypeDef but is not a RelationshipDef - it should not be accepted for the relationship
        // This test is now particularly devious - it creates an EntityDef (as TypeDef is now abstract) but
        // it then uses the setCategory method of TypeDefSummary to subvert the category to make it look like
        // a relationship def... this should hopefully get bounced....
        TypeDef type21 = new EntityDef();
        type21.setName("type21");
        type21.setGUID(UUID.randomUUID().toString());
        type21.setCategory(TypeDefCategory.RELATIONSHIP_DEF);
        backgroundRelationshipTypes.put(type21.getName(), type21);

        // type22 is a RelationshipDef - but it does not have valid end defs
        RelationshipDef type22 = new RelationshipDef();
        type22.setName("type22");
        type22.setGUID(UUID.randomUUID().toString());
        type22.setCategory(TypeDefCategory.RELATIONSHIP_DEF);
        backgroundRelationshipTypes.put(type22.getName(), type22);

        // type23 is a RelationshipDef with sufficient end defs (attributeName and entityType are set) and no defined attributes
        RelationshipDef type23 = new RelationshipDef();
        type23.setName("type23");
        type23.setGUID(UUID.randomUUID().toString());
        type23.setCategory(TypeDefCategory.RELATIONSHIP_DEF);
        RelationshipEndDef endDef1 = new RelationshipEndDef();
        endDef1.setAttributeName("type23_end1");
        endDef1.setEntityType(type1);
        type23.setEndDef1(endDef1);
        RelationshipEndDef endDef2 = new RelationshipEndDef();
        endDef2.setAttributeName("type23_end2");
        endDef2.setEntityType(type2);
        type23.setEndDef2(endDef2);
        backgroundRelationshipTypes.put(type23.getName(), type23);


        // type24 is a RelationshipDef with sufficient end defs (attributeName and entityType are set) and defined attributes
        RelationshipDef type24 = new RelationshipDef();
        type24.setName("type24");
        type24.setGUID(UUID.randomUUID().toString());
        type24.setCategory(TypeDefCategory.RELATIONSHIP_DEF);
        RelationshipEndDef endDef24_1 = new RelationshipEndDef();
        endDef24_1.setAttributeName("type24_end1");
        endDef24_1.setEntityType(type1);
        type24.setEndDef1(endDef24_1);
        RelationshipEndDef endDef24_2 = new RelationshipEndDef();
        endDef24_2.setAttributeName("type24_end2");
        endDef24_2.setEntityType(type2);
        type24.setEndDef2(endDef24_2);
        List<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute attr1Def = new TypeDefAttribute();
        attr1Def.setAttributeName("attr1");
        AttributeTypeDef atd1 = new PrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
        atd1.setName(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING.getName());
        atd1.setGUID(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING.getGUID());
        attr1Def.setAttributeType(atd1);
        properties.add(attr1Def);
        type24.setPropertiesDefinition(properties);
        backgroundRelationshipTypes.put(type24.getName(), type24);


        // Ability to serve up AtlasEntityWithExt objects in response to mocked calls to entityStore
        AtlasEntity atlasEntity1 = new AtlasEntity();
        String guid1 = UUID.randomUUID().toString();
        atlasEntity1.setGuid(guid1);
        atlasEntity1.setTypeName("type1");
        LOG.debug("AtlasEntity is {}", atlasEntity1);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityExt1 = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity1, null);
        backgroundEntities.put(guid1, atlasEntityExt1);

        AtlasEntity atlasEntity2 = new AtlasEntity();
        String guid2 = UUID.randomUUID().toString();
        atlasEntity2.setGuid(guid2);
        atlasEntity2.setTypeName("type2");
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityExt2 = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity2, null);
        backgroundEntities.put(guid2, atlasEntityExt2);



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
     * Each object provided by the data provider is an AtlasRelationship and the expected result.
     *
     */
    public enum TestResult {
        OK,
        TypeErrorException,
        RepositoryErrorException,
        EntityNotKnownException,
        InvalidParameterException,
        InvalidRelationshipException,
        ClassCastException
    }

    public class TestData {
        String                                     testCaption;
        AtlasRelationship                          atlasRelationship;
        TestAtlasRelationshipMapper.TestResult     expectedResult;
    }

    @DataProvider(name = "provideAtlasRelationships")
    public Object[][] provideData() {


        // 1. Test with no atlasRelationship
        AtlasRelationship atlasRelationshipNull = null;

        // 2. Test with atlasRelationship with no type
        AtlasRelationship atlasRelationshipNoType = new AtlasRelationship();

        // 3. Test with atlasRelationship of type with no category
        //AtlasRelationship atlasRelationshipNoCat = new AtlasRelationship();
        //atlasRelationshipNoCat.setTypeName("type20");

        // 4. Test with atlasRelationship of type with category but is not a RelationshipDef
        AtlasRelationship atlasRelationshipNotRelDef = new AtlasRelationship();
        atlasRelationshipNotRelDef.setTypeName("type21");

        // 5. Test with atlasRelationship category and a RelationshipDef, but with no ends
        AtlasRelationship atlasRelationshipNoEnds = new AtlasRelationship();
        atlasRelationshipNoEnds.setTypeName("type22");

        // 6. Test with atlasRelationship category and a RelationshipDef, with ends that have guid and typeName
        AtlasRelationship atlasRelationship = new AtlasRelationship();
        atlasRelationship.setTypeName("type23");
        String relationshipGUID = UUID.randomUUID().toString();
        atlasRelationship.setGuid(relationshipGUID);
        atlasRelationship.setStatus(AtlasRelationship.Status.DELETED);
        // Need to set the entity guid to something that is able to be mocked
        Iterator<String> entityGUIDs = backgroundEntities.keySet().iterator();
        AtlasObjectId end1 = new AtlasObjectId();
        if (entityGUIDs.hasNext()) {
            String guid = entityGUIDs.next();
            LOG.debug("entity with guid {} is {}", guid, backgroundEntities.get(guid).getEntity() );
            end1.setGuid(guid);
            end1.setTypeName(backgroundEntities.get(guid).getEntity().getTypeName());
        }
        atlasRelationship.setEnd1(end1);
        AtlasObjectId end2 = new AtlasObjectId();
        if (entityGUIDs.hasNext()) {
            String guid = entityGUIDs.next();
            LOG.debug("entity with guid {} is {}", guid, backgroundEntities.get(guid).getEntity() );
            end2.setGuid(guid);
            end2.setTypeName(backgroundEntities.get(guid).getEntity().getTypeName());
        }
        atlasRelationship.setEnd2(end2);
        // expected values
        Map<String,Object> expectedFields6 = new HashMap<>();
        expectedFields6.put("relationshipGUID",relationshipGUID );
        expectedFields6.put("status", InstanceStatus.DELETED);


        // 7. Test with atlasRelationship with undefined attribute (i.e. attribute not in RelationshipDef)
        AtlasRelationship atlasRelationship7 = new AtlasRelationship();
        atlasRelationship7.setTypeName("type23");
        atlasRelationship7.setVersion(23L);
        String relationshipGUID7 = UUID.randomUUID().toString();
        atlasRelationship7.setGuid(relationshipGUID7);
        atlasRelationship7.setStatus(AtlasRelationship.Status.DELETED);
        // Need to set the entity guid to something that is able to be mocked
        entityGUIDs = backgroundEntities.keySet().iterator();
        AtlasObjectId end7_1 = new AtlasObjectId();
        if (entityGUIDs.hasNext()) {
            String guid = entityGUIDs.next();
            LOG.debug("entity with guid {} is {}", guid, backgroundEntities.get(guid).getEntity() );
            end7_1.setGuid(guid);
            end7_1.setTypeName(backgroundEntities.get(guid).getEntity().getTypeName());
        }
        atlasRelationship7.setEnd1(end7_1);
        AtlasObjectId end7_2 = new AtlasObjectId();
        if (entityGUIDs.hasNext()) {
            String guid = entityGUIDs.next();
            LOG.debug("entity with guid {} is {}", guid, backgroundEntities.get(guid).getEntity() );
            end7_2.setGuid(guid);
            end7_2.setTypeName(backgroundEntities.get(guid).getEntity().getTypeName());
        }
        atlasRelationship7.setEnd2(end7_2);
        // attributes
        Map<String, Object> atlasAttributes7 = new HashMap<>();
        String attr7_1 = "attr7_1-value";
        atlasAttributes7.put("attr7_1",attr7_1);
        atlasRelationship7.setAttributes(atlasAttributes7);
        // expected values
        Map<String,Object> expectedFields7 = new HashMap<>();
        expectedFields7.put("relationshipGUID",relationshipGUID7 );
        expectedFields7.put("status", InstanceStatus.DELETED);
        expectedFields7.put("attributes",null);    // the attributes are not in the RelationshipDef so do not expect them


        // 8. Test with atlasRelationship with undefined attribute (i.e. attribute not in RelationshipDef)
        AtlasRelationship atlasRelationship8 = new AtlasRelationship();
        atlasRelationship8.setTypeName("type24");
        atlasRelationship8.setVersion(24L);
        String relationshipGUID8 = UUID.randomUUID().toString();
        atlasRelationship8.setGuid(relationshipGUID8);
        atlasRelationship8.setStatus(AtlasRelationship.Status.DELETED);
        // Need to set the entity guid to something that is able to be mocked
        entityGUIDs = backgroundEntities.keySet().iterator();
        AtlasObjectId end8_1 = new AtlasObjectId();
        if (entityGUIDs.hasNext()) {
            String guid = entityGUIDs.next();
            LOG.debug("entity with guid {} is {}", guid, backgroundEntities.get(guid).getEntity() );
            end8_1.setGuid(guid);
            end8_1.setTypeName(backgroundEntities.get(guid).getEntity().getTypeName());
        }
        atlasRelationship8.setEnd1(end8_1);
        AtlasObjectId end8_2 = new AtlasObjectId();
        if (entityGUIDs.hasNext()) {
            String guid = entityGUIDs.next();
            LOG.debug("entity with guid {} is {}", guid, backgroundEntities.get(guid).getEntity() );
            end8_2.setGuid(guid);
            end8_2.setTypeName(backgroundEntities.get(guid).getEntity().getTypeName());
        }
        atlasRelationship8.setEnd2(end8_2);
        // attributes
        Map<String, Object> atlasAttributes8 = new HashMap<>();
        String valueAttr8_1 = "test8-attr1-value";
        atlasAttributes8.put("attr1",valueAttr8_1);
        atlasRelationship8.setAttributes(atlasAttributes8);
        // expected values
        Map<String,Object> expectedFields8 = new HashMap<>();
        InstanceProperties expectedProps8 = new InstanceProperties();
        PrimitivePropertyValue instancePropertyValue8_1 = new PrimitivePropertyValue();
        instancePropertyValue8_1.setPrimitiveDefCategory(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
        instancePropertyValue8_1.setTypeName("string");
        instancePropertyValue8_1.setTypeGUID(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING.getGUID());
        instancePropertyValue8_1.setPrimitiveValue(valueAttr8_1);
        expectedProps8.setProperty("attr1",instancePropertyValue8_1 );
        expectedFields8.put("attributes",expectedProps8);
        // expect the ends as well - the EntityProxy mapper has separate UTs so just test tha the correct proxy is conveyed
        EntityProxy expectedEntityProxy1 = new EntityProxy();
        expectedEntityProxy1.setGUID(end8_1.getGuid());
        expectedFields8.put("end1",expectedEntityProxy1);
        EntityProxy expectedEntityProxy2 = new EntityProxy();
        expectedEntityProxy2.setGUID(end8_2.getGuid());
        expectedFields8.put("end2",expectedEntityProxy2);



        Object[][] test_data = new Object[][] {
                { "1. no relationship",       atlasRelationshipNull,        TestResult.InvalidParameterException, null},
                { "2. relationship no type",  atlasRelationshipNoType,      TestResult.TypeErrorException,        null},
                // No longer possible - TypeDef is now abstract { "3. relationship no cat",   atlasRelationshipNoCat,       TestResult.TypeErrorException,        null},
                { "4. relationship inv type", atlasRelationshipNotRelDef,   TestResult.TypeErrorException,        null},
                { "5. relationship no ends",  atlasRelationshipNoEnds,      TestResult.TypeErrorException,        null},
                { "6. relationship",          atlasRelationship,            TestResult.OK,                        expectedFields6},
                { "7. relationship",          atlasRelationship7,           TestResult.OK,                        expectedFields7},
                { "8. relationship",          atlasRelationship8,           TestResult.OK,                        expectedFields8},

        };

        return test_data;
    }

    @Test(dataProvider = "provideAtlasRelationships")
    public void test_AtlasRelationship(String              testCaption,
                                       AtlasRelationship   atlasRelationship,
                                       TestResult          testResult,
                                       Map<String,Object>  expectedFields)
        throws
            RepositoryErrorException,  // not really thrown - but needed for when.thenReturn of getMetadataCollectionId()
            AtlasBaseException,
            InvalidEntityException,
            TypeDefNotKnownException
    {

        final String methodName = "test_AtlasRelationship";

        LOG.debug("TEST: {}", testCaption);

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

        if (backgroundRelationshipTypes != null) {
            for (TypeDef backgroundType : backgroundRelationshipTypes.values()) {
                when(metadataCollection._getTypeDefByName(userId, backgroundType.getName())).
                        thenReturn(backgroundType);
            }
        }

        // EntityStore
        AtlasEntityStore entityStore = mock(AtlasEntityStore.class);
        if (backgroundEntities != null) {
            for (AtlasEntity.AtlasEntityWithExtInfo backgroundEntity : backgroundEntities.values()) {
                when(entityStore.getById(backgroundEntity.getEntity().getGuid())).
                        thenReturn(backgroundEntity);
            }
        }


        Relationship omRelationship = null;

        LOG.debug(methodName+": Construct AtlasRelationshipMapper");
        try {

            AtlasRelationshipMapper atlasRelationshipMapper =
                    new AtlasRelationshipMapper(metadataCollection, userId, atlasRelationship, entityStore);
            omRelationship = atlasRelationshipMapper.toOMRelationship();

        } catch (TypeErrorException e) {
            LOG.debug(methodName+": Caught TypeErrorException exception");
            if (testResult.equals(TestAtlasRelationshipMapper.TestResult.TypeErrorException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        } catch (RepositoryErrorException e) {
            LOG.debug(methodName+": Caught RepositoryErrorException exception");
            if (testResult.equals(TestAtlasRelationshipMapper.TestResult.RepositoryErrorException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        } catch (EntityNotKnownException e) {
            LOG.debug(methodName+": Caught EntityNotKnownException exception");
            if (testResult.equals(TestAtlasRelationshipMapper.TestResult.EntityNotKnownException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        } catch (InvalidParameterException e) {
            LOG.debug(methodName+": Caught InvalidParameterException exception");
            if (testResult.equals(TestAtlasRelationshipMapper.TestResult.InvalidParameterException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        } catch (InvalidRelationshipException e) {
            LOG.debug(methodName+": Caught InvalidRelationshipException exception");
            if (testResult.equals(TestAtlasRelationshipMapper.TestResult.InvalidRelationshipException)) {
                LOG.debug(methodName+": Test Passed");
                return;
            }
            else {
                LOG.debug(methodName+": Exception was unexpected");
                fail();
            }
        }


        // Should have returned an OM Relationship
        assertNotNull(omRelationship);

        Comparator c;
        boolean match;

        switch(testResult) {

            case OK:
                // Not using Comparator because we are directly comparing Atlas vs OM
                // We know that atlasRelationship is not null

                // OM Relationship should have same version as AtlasEntity
                assertEquals(omRelationship.getVersion(), atlasRelationship.getVersion().longValue());

                // Check GUID
                if (expectedFields != null && expectedFields.get("relationshipGUID") != null) {
                    LOG.debug("TestAtlasRelationshipMapper: relationship GUID {}", omRelationship.getGUID());
                    assertEquals(omRelationship.getGUID(), expectedFields.get("relationshipGUID"));
                }

                // Check the InstanceType -
                InstanceType instanceType = omRelationship.getType();
                String typeName = instanceType.getTypeDefName();
                assertEquals(typeName, atlasRelationship.getTypeName());

                // Check metadataCollectionId
                assertEquals(omRelationship.getMetadataCollectionId(), atlasRelationship.getHomeId());

                // Check status
                if (expectedFields != null && expectedFields.get("status") != null) {
                    LOG.debug("TestAtlasRelationshipMapper: status {}", omRelationship.getStatus());
                    assertEquals(omRelationship.getStatus(), expectedFields.get("status"));
                }

                // check attributes
                if (expectedFields != null && expectedFields.get("attributes") != null) {
                    InstanceProperties actualProperties = omRelationship.getProperties();
                    LOG.debug(methodName + ": actualProperties {}", actualProperties);
                    InstanceProperties expectedProperties = (InstanceProperties) (expectedFields.get("attributes"));
                    LOG.debug(methodName + ": expectedProperties {}", expectedProperties);
                    // Test each expected property...
                    Iterator<String> expectedPropertyNames = expectedProperties.getPropertyNames();
                    while (expectedPropertyNames.hasNext()) {
                        String propName = expectedPropertyNames.next();
                        InstancePropertyValue expectedPropValue = expectedProperties.getPropertyValue(propName);
                        InstancePropertyValue actualPropValue = actualProperties.getPropertyValue(propName);
                        assertEquals(expectedPropValue.getInstancePropertyCategory(), actualPropValue.getInstancePropertyCategory());
                        LOG.debug(methodName + ": expected type {}", expectedPropValue.getTypeName());
                        LOG.debug(methodName + ": actual type {}", actualPropValue.getTypeName());
                        assertEquals(expectedPropValue.getTypeName(), actualPropValue.getTypeName());
                        assertEquals(expectedPropValue.getTypeGUID(), actualPropValue.getTypeGUID());
                    }
                }


                // check ends
                if (expectedFields != null && expectedFields.get("end1") != null) {
                    /* The content of the proxy is tested byEntityMapper UTs, so just interested here in whether the
                     * the correct proxy is conveyed by the relationship mapper.
                     */
                    EntityProxy entityProxy1 = omRelationship.getEntityOneProxy();
                    EntityProxy expectedProxy1 = (EntityProxy)(expectedFields.get("end1"));
                    // Just compare the GUIDs
                    assertEquals(entityProxy1.getGUID(),expectedProxy1.getGUID() );
                }
                if (expectedFields != null && expectedFields.get("end2") != null) {
                    /* The content of the proxy is tested byEntityMapper UTs, so just interested here in whether the
                     * the correct proxy is conveyed by the relationship mapper.
                     */
                    EntityProxy entityProxy2 = omRelationship.getEntityTwoProxy();
                    EntityProxy expectedProxy2 = (EntityProxy)(expectedFields.get("end2"));
                    // Just compare the GUIDs
                    assertEquals(entityProxy2.getGUID(),expectedProxy2.getGUID() );
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
