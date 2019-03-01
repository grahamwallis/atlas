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
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/*
 * Test the AtlasRelationshipDefMapper component of the AtlasConnector
 *
 * The constructor is trivial; it is the toOMRelationshipDef that is of primary interest.
 *
 */
public class TestAtlasRelationshipDefMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasRelationshipDefMapper.class);

    // Background types - these are used and reused across multiple tests
    private Map<String,TypeDefLink> backgroundEntityTypes = new HashMap<>();
    private Map<String,TypeDefLink> backgroundRelationshipTypes = new HashMap<>();

    @BeforeClass
    public void setup() {

        TypeDefLink referenceable = new TypeDefLink(UUID.randomUUID().toString(),"Referenceable");
        backgroundEntityTypes.put(referenceable.getName(), referenceable);

        TypeDefLink asset = new TypeDefLink(UUID.randomUUID().toString(),"Asset");
        backgroundEntityTypes.put(asset.getName(), asset);

        TypeDefLink infrastructure = new TypeDefLink(UUID.randomUUID().toString(),"Infrastructure");
        backgroundEntityTypes.put(infrastructure.getName(), infrastructure);

        TypeDefLink process = new TypeDefLink(UUID.randomUUID().toString(),"Process");
        backgroundEntityTypes.put(process.getName(), process);

        TypeDefLink dataset = new TypeDefLink(UUID.randomUUID().toString(),"DataSet");
        backgroundEntityTypes.put(dataset.getName(), dataset);

        TypeDefLink type1 = new TypeDefLink(UUID.randomUUID().toString(),"type1");
        backgroundEntityTypes.put(type1.getName(), type1);

        TypeDefLink type2 = new TypeDefLink(UUID.randomUUID().toString(),"type2");
        backgroundEntityTypes.put(type2.getName(), type2);

        TypeDefLink type3 = new TypeDefLink(UUID.randomUUID().toString(),"type3");
        backgroundEntityTypes.put(type3.getName(), type3);

        TypeDefLink type4 = new TypeDefLink(UUID.randomUUID().toString(),"type4");
        backgroundRelationshipTypes.put(type4.getName(), type4);

        TypeDefLink type5 = new TypeDefLink(UUID.randomUUID().toString(),"type5");
        backgroundRelationshipTypes.put(type5.getName(), type5);

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
     * Each object provided by the data provider is an AtlasRelationshipDef and the expected result.
     *
     */
    public enum TestResult {
        OK,
        TypeErrorException
    }

    public class TestData {
        String                           testCaption;
        AtlasRelationshipDef             atlasRelationshipDef;
        TestResult                       expectedResult;
        Map<String,RelationshipEndDef>   expectedEnds;
        Map<String,TypeDefAttribute>     expectedAttributes;
    }

    @DataProvider(name = "provideAtlasRelationshipDefs")
    public Object[][] provideData() {


        // 1. Test with no atlasRelationshipDef
        AtlasRelationshipDef atlasRelationshipDefNull = null;

        // 2. AtlasRelationshipDef with no name
        AtlasRelationshipDef atlasRelationshipDefNoName;
        try {
            atlasRelationshipDefNoName = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }

        // 3. AtlasRelationshipDef with no guid
        AtlasRelationshipDef atlasRelationshipDefNoGUID;
        try {
            atlasRelationshipDefNoGUID = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefNoGUID.setName(backgroundRelationshipTypes.get("type4").getName());

        // 4. AtlasRelationshipDef with no version
        AtlasRelationshipDef atlasRelationshipDefNoVersion;
        try {
            atlasRelationshipDefNoVersion = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefNoVersion.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefNoVersion.setGuid(backgroundRelationshipTypes.get("type4").getGUID());

        // 5. AtlasRelationshipDef with no ends
        AtlasRelationshipDef atlasRelationshipDefNoEnds;
        try {
            atlasRelationshipDefNoEnds = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefNoEnds.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefNoEnds.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDefNoEnds.setVersion(7l);


        // 6. AtlasRelationshipDef with ends but no relationship category
        AtlasRelationshipDef atlasRelationshipDefNoCat;
        try {
            atlasRelationshipDefNoCat = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefNoCat.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefNoCat.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDefNoCat.setVersion(7l);
        // Get some ends...
        AtlasRelationshipEndDef ared1 = new AtlasRelationshipEndDef("type1","test6_end1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        AtlasRelationshipEndDef ared2 = new AtlasRelationshipEndDef("type2","test6_end2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        atlasRelationshipDefNoCat.setEndDef1(ared1);
        atlasRelationshipDefNoCat.setEndDef2(ared2);

        // 7. AtlasRelationshipDef with ends and relationship category but no prop tags
        AtlasRelationshipDef atlasRelationshipDefNoPropTags;
        try {
            atlasRelationshipDefNoPropTags = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefNoPropTags.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefNoPropTags.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDefNoPropTags.setVersion(7l);
        // Get some ends...
        AtlasRelationshipEndDef ared7_1 = new AtlasRelationshipEndDef("type1","test7_end1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        AtlasRelationshipEndDef ared7_2 = new AtlasRelationshipEndDef("type2","test7_end2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        atlasRelationshipDefNoPropTags.setEndDef1(ared7_1);
        atlasRelationshipDefNoPropTags.setEndDef2(ared7_2);
        atlasRelationshipDefNoPropTags.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);

        // 8. AtlasRelationshipDef between non-F5 entity types
        AtlasRelationshipDef atlasRelationshipDef;
        try {
            atlasRelationshipDef = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDef.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDef.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDef.setVersion(7l);
        // expected end defs
        Map<String,RelationshipEndDef> expectedEnds8 = new HashMap<>();
        RelationshipEndDef expectedEndDef8_1 = new RelationshipEndDef();
        expectedEndDef8_1.setEntityType(backgroundEntityTypes.get("type1"));
        expectedEndDef8_1.setAttributeCardinality(RelationshipEndCardinality.AT_MOST_ONE);
        expectedEnds8.put("1",expectedEndDef8_1);
        RelationshipEndDef expectedEndDef8_2 = new RelationshipEndDef();
        expectedEndDef8_2.setEntityType(backgroundEntityTypes.get("type2"));
        expectedEndDef8_2.setAttributeCardinality(RelationshipEndCardinality.ANY_NUMBER);
        expectedEnds8.put("2",expectedEndDef8_2);
        // Get some ends...
        // Note the reversal of the attribute cardinality - i.e. the setting for end1 is the one for end2 in OM and vice versa....
        AtlasRelationshipEndDef ared8_1 = new AtlasRelationshipEndDef("type1","test8_end1", AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
        AtlasRelationshipEndDef ared8_2 = new AtlasRelationshipEndDef("type2","test8_end2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        atlasRelationshipDef.setEndDef1(ared8_1);
        atlasRelationshipDef.setEndDef2(ared8_2);
        atlasRelationshipDef.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);
        atlasRelationshipDef.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);



        // 9. AtlasRelationshipDef with one F5 entity type
        AtlasRelationshipDef atlasRelationshipDefOneF5;
        try {
            atlasRelationshipDefOneF5 = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefOneF5.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefOneF5.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDefOneF5.setVersion(7l);
        // expected end defs
        Map<String,RelationshipEndDef> expectedEnds9 = new HashMap<>();
        RelationshipEndDef expectedEndDef9_1 = new RelationshipEndDef();
        expectedEndDef9_1.setEntityType(backgroundEntityTypes.get("type1"));
        expectedEndDef9_1.setAttributeCardinality(RelationshipEndCardinality.AT_MOST_ONE);
        expectedEnds9.put("1",expectedEndDef9_1);
        RelationshipEndDef expectedEndDef9_2 = new RelationshipEndDef();
        expectedEndDef9_2.setEntityType(backgroundEntityTypes.get("Asset"));
        expectedEndDef9_2.setAttributeCardinality(RelationshipEndCardinality.ANY_NUMBER);
        expectedEnds9.put("2",expectedEndDef9_2);
        // Get some ends...
        // Note the reversal of the attribute cardinality - i.e. the setting for end1 is the one for end2 in OM and vice versa....
        AtlasRelationshipEndDef ared9_1 = new AtlasRelationshipEndDef("type1","test9_end1", AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
        AtlasRelationshipEndDef ared9_2 = new AtlasRelationshipEndDef("OM_Asset","test9_end2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        atlasRelationshipDefOneF5.setEndDef1(ared9_1);
        atlasRelationshipDefOneF5.setEndDef2(ared9_2);
        atlasRelationshipDefOneF5.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);
        atlasRelationshipDefOneF5.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);

        // 10. AtlasRelationshipDef with ends and relationship category but with containership inappropriate for association
        AtlasRelationshipDef atlasRelationshipDefInappropriateContainership;
        try {
            atlasRelationshipDefInappropriateContainership = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefInappropriateContainership.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefInappropriateContainership.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDefInappropriateContainership.setVersion(7l);
        // expected end defs
        Map<String,RelationshipEndDef> expectedEnds10 = new HashMap<>();
        RelationshipEndDef expectedEndDef10_1 = new RelationshipEndDef();
        expectedEndDef10_1.setEntityType(backgroundEntityTypes.get("type1"));
        expectedEndDef10_1.setAttributeCardinality(RelationshipEndCardinality.AT_MOST_ONE);
        expectedEnds10.put("1",expectedEndDef10_1);
        RelationshipEndDef expectedEndDef10_2 = new RelationshipEndDef();
        expectedEndDef10_2.setEntityType(backgroundEntityTypes.get("Asset"));
        expectedEndDef10_2.setAttributeCardinality(RelationshipEndCardinality.ANY_NUMBER);
        expectedEnds10.put("2",expectedEndDef10_2);
        // Get some ends...
        AtlasRelationshipEndDef ared10_1 = new AtlasRelationshipEndDef("type1","test7_end1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        AtlasRelationshipEndDef ared10_2 = new AtlasRelationshipEndDef("type2","test7_end2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        ared10_2.setIsContainer(true);
        atlasRelationshipDefInappropriateContainership.setEndDef1(ared10_1);
        atlasRelationshipDefInappropriateContainership.setEndDef2(ared10_2);
        atlasRelationshipDefInappropriateContainership.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);
        atlasRelationshipDefInappropriateContainership.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);


        // 11. AtlasRelationshipDef (between non-F5 entity types) with attributes
        AtlasRelationshipDef atlasRelationshipDefWithAttributes;
        try {
            atlasRelationshipDefWithAttributes = new AtlasRelationshipDef();
        } catch (AtlasBaseException e) {
            LOG.debug("Data provider could not create AtlasRelationshipDef - aborting test");
            return null;
        }
        atlasRelationshipDefWithAttributes.setName(backgroundRelationshipTypes.get("type4").getName());
        atlasRelationshipDefWithAttributes.setGuid(backgroundRelationshipTypes.get("type4").getGUID());
        atlasRelationshipDefWithAttributes.setVersion(7l);
        // expected end defs
        Map<String,RelationshipEndDef> expectedEnds11 = new HashMap<>();
        RelationshipEndDef expectedEndDef11_1 = new RelationshipEndDef();
        expectedEndDef11_1.setEntityType(backgroundEntityTypes.get("type1"));
        expectedEndDef11_1.setAttributeCardinality(RelationshipEndCardinality.AT_MOST_ONE);
        expectedEnds11.put("1",expectedEndDef11_1);
        RelationshipEndDef expectedEndDef11_2 = new RelationshipEndDef();
        expectedEndDef11_2.setEntityType(backgroundEntityTypes.get("type2"));
        expectedEndDef11_2.setAttributeCardinality(RelationshipEndCardinality.ANY_NUMBER);
        expectedEnds11.put("2",expectedEndDef11_2);
        // Get some ends...
        // Note the reversal of the attribute cardinality - i.e. the setting for end1 is the one for end2 in OM and vice versa....
        AtlasRelationshipEndDef ared11_1 = new AtlasRelationshipEndDef("type1","test11_end1", AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
        AtlasRelationshipEndDef ared11_2 = new AtlasRelationshipEndDef("type2","test11_end2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        atlasRelationshipDefWithAttributes.setEndDef1(ared11_1);
        atlasRelationshipDefWithAttributes.setEndDef2(ared11_2);
        atlasRelationshipDefWithAttributes.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);
        atlasRelationshipDefWithAttributes.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);
        // Attributes
        AtlasStructDef.AtlasAttributeDef atlasAttr11_1 = new AtlasStructDef.AtlasAttributeDef("attr11_1","string");
        List<AtlasStructDef.AtlasAttributeDef> atlasAttributeDefs = new ArrayList<>();
        atlasAttributeDefs.add(atlasAttr11_1);
        atlasRelationshipDefWithAttributes.setAttributeDefs(atlasAttributeDefs);
        Map<String,TypeDefAttribute> expectedAttributes11 = new HashMap<>();
        TypeDefAttribute expectedAttribute11_1 = new TypeDefAttribute();
        expectedAttribute11_1.setAttributeName("attr11_1");
        AttributeTypeDef atd11_1 = new PrimitiveDef(OM_PRIMITIVE_TYPE_STRING);
        atd11_1.setName("string");
        expectedAttribute11_1.setAttributeType(atd11_1);
        expectedAttributes11.put("1",expectedAttribute11_1);



        Object[][] test_data = new Object[][] {
                { "1. no relationship def",  atlasRelationshipDefNull,                        TestResult.TypeErrorException,  null,            null},
                { "2. no name",              atlasRelationshipDefNoName,                      TestResult.TypeErrorException,  null,            null},
                { "3. no guid",              atlasRelationshipDefNoGUID,                      TestResult.TypeErrorException,  null,            null},
                { "4. no version",           atlasRelationshipDefNoVersion,                   TestResult.TypeErrorException,  null,            null},
                { "5. no ends",              atlasRelationshipDefNoEnds,                      TestResult.TypeErrorException,  null,            null},
                { "6. no category",          atlasRelationshipDefNoCat,                       TestResult.TypeErrorException,  null,            null},
                { "7. no prop tags",         atlasRelationshipDefNoPropTags,                  TestResult.TypeErrorException,  null,            null},
                { "8. with ends",            atlasRelationshipDef,                            TestResult.OK,                  expectedEnds8,   null},
                { "9. with one F5 end",      atlasRelationshipDefOneF5,                       TestResult.OK,                  expectedEnds9,   null},
                { "10. with bad container",  atlasRelationshipDefInappropriateContainership,  TestResult.TypeErrorException,  expectedEnds10,  null},
                { "11. with attributes",     atlasRelationshipDefWithAttributes,              TestResult.OK,                  expectedEnds11,  expectedAttributes11},

        };

        return test_data;

    }

    /*
     * Parameters to the test method:
     * String                    testCaption;
     * AtlasRelationshipDef      atlasRelationshipDef;
     * TestResult                expectedResult;
     * Map<RelationshipEndDef>   expectedEnds;
     */
    @Test(dataProvider = "provideAtlasRelationshipDefs")
    public void test_AtlasRelationshipDef(String                          testCaption,
                                           AtlasRelationshipDef            atlasRelationshipDef,
                                           TestResult                      testResult,
                                           Map<String,RelationshipEndDef>  expectedEndDefs,
                                           Map<String,TypeDefAttribute>    expectedAttributeDefs )
            throws
            RepositoryErrorException,  // not really thrown - but needed for when.thenReturn of getMetadataCollectionId()
            TypeErrorException
    {

        LOG.debug("TEST: {}", testCaption);

        /*
         * Set up mocks
         */

        // MetadataCollection
        LocalAtlasOMRSMetadataCollection metadataCollection = mock(LocalAtlasOMRSMetadataCollection.class);

        when(metadataCollection.getMetadataCollectionId()).thenReturn("mock_metadata_collection");

        // We are not testing the attributeDef conversion - just the handling of the attribute def list in relationship def mapper
        if (expectedAttributeDefs != null) {
            ArrayList<TypeDefAttribute> expectedAttrDefList = new ArrayList<>();
            Iterator<TypeDefAttribute> it = expectedAttributeDefs.values().iterator();
            while (it.hasNext())
                expectedAttrDefList.add(it.next());

            when(metadataCollection.convertAtlasAttributeDefs(any(String.class), anyList())).thenReturn(expectedAttrDefList);
        }
        else
            when(metadataCollection.convertAtlasAttributeDefs(any(String.class), anyList())).thenReturn(null);


        /*
         * Prepare the mock of constructTypeDefLink so that it will serve up names and guids of any background types
         */
        if (backgroundEntityTypes != null) {
            for (TypeDefLink backgroundType : backgroundEntityTypes.values()) {
                when(metadataCollection.constructTypeDefLink(backgroundType.getName(), TypeCategory.ENTITY)).
                        thenReturn(backgroundType);
            }
        }
        if (backgroundRelationshipTypes != null) {
            for (TypeDefLink backgroundType : backgroundRelationshipTypes.values()) {
                when(metadataCollection.constructTypeDefLink(backgroundType.getName(), TypeCategory.RELATIONSHIP)).
                        thenReturn(backgroundType);
            }
        }

        String userId = "test_user";

        RelationshipDef omRelationshipDef = null;

        LOG.debug("TestAtlasRelationshipDefMapper: Construct AtlasRelationshipDefMapper");
        try {

            AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(metadataCollection, userId, atlasRelationshipDef);
            omRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();

        } catch (TypeErrorException e) {
            LOG.debug("TestAtlasRelationshipDefMapper: Caught TypeErrorException exception");
            if (testResult.equals(TestResult.TypeErrorException)) {
                LOG.debug("TestAtlasRelationshipDefMapper: Test Passed");
                return;
            }
            else {
                LOG.debug("TestAtlasRelationshipDefMapper: Exception was unexpected");
                fail();
            }
        }
        // Should have returned an OM RelationshipDef
        assertNotNull(omRelationshipDef);

        Comparator c;
        boolean match;

        switch(testResult) {

            case OK:
                // Not using Comparator because we are directly comparing Atlas vs OM
                // We know that atlasRelationshipDef is not null

                // OM RelationshipDef should have same name as Atlas RelationshipDef
                String expectedName = atlasRelationshipDef.getName();
                assertEquals(omRelationshipDef.getName(), expectedName);

                // OM RelationshipDef should have same guid as Atlas RelationshipDef
                assertEquals(omRelationshipDef.getGUID(), atlasRelationshipDef.getGuid());

                // OM RelationshipDef should have same version as Atlas RelationshipDef
                assertEquals(omRelationshipDef.getVersion(), atlasRelationshipDef.getVersion().longValue());

                // OM RelationshipDef should have sensible ends
                // end1 name
                if (expectedEndDefs != null) {
                    String end1TypeName = omRelationshipDef.getEndDef1().getEntityType().getName();
                    String expectedEnd1TypeName = expectedEndDefs.get("1").getEntityType().getName();
                    assertEquals(end1TypeName, expectedEnd1TypeName);
                    // end2 name
                    String end2TypeName = omRelationshipDef.getEndDef2().getEntityType().getName();
                    String expectedEnd2TypeName = expectedEndDefs.get("2").getEntityType().getName();
                    assertEquals(end2TypeName, expectedEnd2TypeName);
                    // end1 cardinality
                    RelationshipEndCardinality actualEnd1Cardinality = omRelationshipDef.getEndDef1().getAttributeCardinality();
                    RelationshipEndCardinality expectedEnd1Cardinality = expectedEndDefs.get("1").getAttributeCardinality();
                    assertEquals(actualEnd1Cardinality, expectedEnd1Cardinality);
                    // end2 cardinality
                    RelationshipEndCardinality actualEnd2Cardinality = omRelationshipDef.getEndDef2().getAttributeCardinality();
                    RelationshipEndCardinality expectedEnd2Cardinality = expectedEndDefs.get("2").getAttributeCardinality();
                    assertEquals(actualEnd2Cardinality, expectedEnd2Cardinality);
                }

                // Check the attributes are intact
                if (expectedAttributeDefs != null) {
                    TypeDefAttribute expectedAttr1TDA = expectedAttributeDefs.get("1");
                    String expectedAttr1Name = expectedAttr1TDA.getAttributeName();
                    String expectedAttr1TypeName = expectedAttr1TDA.getAttributeType().getName();
                    List<TypeDefAttribute> propsDef = omRelationshipDef.getPropertiesDefinition();
                    LOG.debug("TestAtlasRelationshipDefMapper: propsDef = {}", propsDef);
                    TypeDefAttribute actualAttr1TDA = omRelationshipDef.getPropertiesDefinition().get(0);
                    String actualAttr1Name = expectedAttr1TDA.getAttributeName();
                    String actualAttr1TypeName = expectedAttr1TDA.getAttributeType().getName();
                    assertEquals(actualAttr1Name, expectedAttr1Name);
                    assertEquals(actualAttr1TypeName, expectedAttr1TypeName);
                }

                LOG.debug("TestAtlasRelationshipDefMapper: Test Passed");
                break;


            default:
                LOG.debug("TestAtlasRelationshipDefMapper: Unexpected result from mapper");
                fail();
                break;
        }
    }
}