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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasStructDef;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

/*
 * Test the AtlasClassificationDefMapper component of the AtlasConnector
 *
 * The constructor is trivial; it is the toOMClassificationDef that is of primary interest.
 *
 */

public class TestAtlasClassificationDefMapper {


    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasClassificationDefMapper.class);

    // Background types - these are used and reused across multiple tests
    private Map<String,TypeDefLink> backgroundEntityTypes = new HashMap<>();
    private Map<String,TypeDefLink> backgroundClassificationTypes = new HashMap<>();

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

        TypeDefLink type10 = new TypeDefLink(UUID.randomUUID().toString(),"type10");
        backgroundClassificationTypes.put(type10.getName(), type10);

        TypeDefLink type11 = new TypeDefLink(UUID.randomUUID().toString(),"type11");
        backgroundClassificationTypes.put(type11.getName(), type11);

        TypeDefLink type12 = new TypeDefLink(UUID.randomUUID().toString(),"type12");
        backgroundClassificationTypes.put(type12.getName(), type12);


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
     * Each object provided by the data provider is an AtlasEntityDef and the expected result.
     *
     */
    public enum TestResult {
        OK,
        TypeErrorException
    }

    public class TestData {
        String                 testCaption;
        AtlasClassificationDef atlasClassificationDef;
        TestResult             expectedResult;
        List<TypeDefLink>      expectedSuperTypes;
        List<TypeDefLink>      expectedValidEntityDefs;
    }

    @DataProvider(name = "provideAtlasClassificationDefs")
    public Object[][] provideData() {


        // 1. Test with no atlasClassificationDef
        AtlasClassificationDef atlasClassificationDefNull = null;

        // 2. AtlasClassificationDef with no name
        AtlasClassificationDef atlasClassificationDefNoName = new AtlasClassificationDef();

        // 3. AtlasClassificationDef with no guid
        AtlasClassificationDef atlasClassificationDefNoGUID = new AtlasClassificationDef();
        atlasClassificationDefNoGUID.setName(backgroundClassificationTypes.get("type10").getName());

        // 4. AtlasClassificationDef with no version
        AtlasClassificationDef atlasClassificationDefNoVersion = new AtlasClassificationDef();
        atlasClassificationDefNoVersion.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefNoVersion.setGuid(backgroundClassificationTypes.get("type10").getGUID());

        // 5. AtlasClassificationDef with no supertype
        AtlasClassificationDef atlasClassificationDefNotF5SupertypesNone = new AtlasClassificationDef();
        atlasClassificationDefNotF5SupertypesNone.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefNotF5SupertypesNone.setGuid(backgroundClassificationTypes.get("type10").getGUID());
        atlasClassificationDefNotF5SupertypesNone.setVersion(7L);

        // 6. AtlasClassificationDef with one supertype
        // expected supertypes
        List<TypeDefLink> expectedSuperTypes6 = new ArrayList<>();
        TypeDefLink type11 = backgroundClassificationTypes.get("type11");
        expectedSuperTypes6.add(type11);
        // atlas classification def
        // has type type10, supertype type11
        AtlasClassificationDef atlasClassificationDefSupertypeOne = new AtlasClassificationDef();
        atlasClassificationDefSupertypeOne.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefSupertypeOne.setGuid(backgroundClassificationTypes.get("type10").getGUID());
        atlasClassificationDefSupertypeOne.setVersion(7L);
        Set<String> superTypes6 = new HashSet<>();
        superTypes6.add(backgroundClassificationTypes.get("type11").getName());
        atlasClassificationDefSupertypeOne.setSuperTypes(superTypes6);

        // 7. AtlasClassificationDef with two supertypes
        // atlas classification def
        // has type type10, supertypes are type11 and type12 - should fail hence no expectedSuperTypes
        AtlasClassificationDef atlasClassificationDefSupertypeTwo = new AtlasClassificationDef();
        atlasClassificationDefSupertypeTwo.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefSupertypeTwo.setGuid(backgroundClassificationTypes.get("type10").getGUID());
        atlasClassificationDefSupertypeTwo.setVersion(7L);
        Set<String> superTypes7 = new HashSet<>();
        superTypes7.add("type11");
        superTypes7.add("type12");
        atlasClassificationDefSupertypeTwo.setSuperTypes(superTypes7);

        // 8. AtlasClassificationDef with valid entity def
        // expected valid entity defs
        List<TypeDefLink> expectedValidEntityDefs8 = new ArrayList<>();
        TypeDefLink type1 = backgroundEntityTypes.get("type1");
        expectedValidEntityDefs8.add(type1);
        // atlas classification def
        // has type type10, validEntityDef of type1
        AtlasClassificationDef atlasClassificationDefValidEntityDefOne = new AtlasClassificationDef();
        atlasClassificationDefValidEntityDefOne.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefValidEntityDefOne.setGuid(backgroundClassificationTypes.get("type10").getGUID());
        atlasClassificationDefValidEntityDefOne.setVersion(7L);
        Set<String> entityTypes8 = new HashSet<>();
        entityTypes8.add("type1");
        atlasClassificationDefValidEntityDefOne.setEntityTypes(entityTypes8);

        // 9. AtlasClassificationDef with valid entity def in F5
        // expected valid entity defs
        List<TypeDefLink> expectedValidEntityDefs9 = new ArrayList<>();
        TypeDefLink asset = backgroundEntityTypes.get("Asset");
        expectedValidEntityDefs9.add(asset);
        // atlas classification def
        // has type type10, validEntityDef of Asset
        AtlasClassificationDef atlasClassificationDefValidEntityDefOneF5 = new AtlasClassificationDef();
        atlasClassificationDefValidEntityDefOneF5.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefValidEntityDefOneF5.setGuid(backgroundClassificationTypes.get("type10").getGUID());
        atlasClassificationDefValidEntityDefOneF5.setVersion(7L);
        Set<String> entityTypes9 = new HashSet<>();
        entityTypes9.add("OM_Asset");
        atlasClassificationDefValidEntityDefOneF5.setEntityTypes(entityTypes9);


        // 10. AtlasClassificationDef with attributes
        // atlas classification def
        // has type type10
        AtlasClassificationDef atlasClassificationDefAttributeOne = new AtlasClassificationDef();
        atlasClassificationDefAttributeOne.setName(backgroundClassificationTypes.get("type10").getName());
        atlasClassificationDefAttributeOne.setGuid(backgroundClassificationTypes.get("type10").getGUID());
        atlasClassificationDefAttributeOne.setVersion(7L);
        // Attributes
        AtlasStructDef.AtlasAttributeDef atlasAttr10_1 = new AtlasStructDef.AtlasAttributeDef("attr10_1","string");
        List<AtlasStructDef.AtlasAttributeDef> atlasAttributeDefs = new ArrayList<>();
        atlasAttributeDefs.add(atlasAttr10_1);
        atlasClassificationDefAttributeOne.setAttributeDefs(atlasAttributeDefs);
        Map<String,TypeDefAttribute> expectedAttributes10 = new HashMap<>();
        TypeDefAttribute expectedAttribute10_1 = new TypeDefAttribute();
        expectedAttribute10_1.setAttributeName("attr10_1");
        AttributeTypeDef atd10_1 = new PrimitiveDef(OM_PRIMITIVE_TYPE_STRING);
        atd10_1.setName("string");
        expectedAttribute10_1.setAttributeType(atd10_1);
        expectedAttributes10.put("1",expectedAttribute10_1);

        Object[][] test_data = new Object[][] {
                { "1. no entity def",     atlasClassificationDefNull,                 TestResult.TypeErrorException,  null,                 null,                      null},
                { "2. no name",           atlasClassificationDefNoName,               TestResult.TypeErrorException,  null,                 null,                      null},
                { "3. no guid",           atlasClassificationDefNoGUID,               TestResult.TypeErrorException,  null,                 null,                      null},
                { "4. no version",        atlasClassificationDefNoVersion,            TestResult.TypeErrorException,  null,                 null,                      null},
                { "5. no supertypes",     atlasClassificationDefNotF5SupertypesNone,  TestResult.OK,                  null,                 null,                      null},
                { "6. one supertype",     atlasClassificationDefSupertypeOne,         TestResult.OK,                  expectedSuperTypes6,  null,                      null},
                { "7. two supertypes",    atlasClassificationDefSupertypeTwo,         TestResult.TypeErrorException,  null,                 null,                      null},
                { "8. valid entity def",  atlasClassificationDefValidEntityDefOne,    TestResult.OK,                  null,                 expectedValidEntityDefs8,  null},
                { "9. F5 entity def",     atlasClassificationDefValidEntityDefOneF5,  TestResult.OK,                  null,                 expectedValidEntityDefs9,  null},
                { "10. with attribute",   atlasClassificationDefAttributeOne,         TestResult.OK,                  null,                 null,                      expectedAttributes10},
        };

        return test_data;

    }

    /*
     * Parameters to the test method:
     * String            testCaption;
     * AtlasClassificationDef    atlasClassificationDef;
     * TestResult        expectedResult;
     * List<TypeDefLink> expectedSuperTypes;
     */
    @Test(dataProvider = "provideAtlasClassificationDefs")
    public void test_AtlasClassificationDef(String                       testCaption,
                                            AtlasClassificationDef       atlasClassificationDef,
                                            TestResult                   testResult,
                                            List<TypeDefLink>            expectedSuperTypes,
                                            List<TypeDefLink>            expectedValidEntityDefs,
                                            Map<String,TypeDefAttribute> expectedAttributeDefs )
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

        // We are not testing the attributeDef conversion - just the handling of the attribute def list in classification def mapper
        if (expectedAttributeDefs != null) {
            ArrayList<TypeDefAttribute> expectedAttrDefList = new ArrayList<>();
            Iterator<TypeDefAttribute> it = expectedAttributeDefs.values().iterator();
            while (it.hasNext())
                expectedAttrDefList.add(it.next());

            when(metadataCollection.convertAtlasAttributeDefs(any(String.class),anyList())).thenReturn(expectedAttrDefList);
        }
        else
            when(metadataCollection.convertAtlasAttributeDefs(any(String.class),anyList())).thenReturn(null);


        /*
         * Prepare the mock of constructTypeDefLink so that it will serve up names and guids of any background types
         */
        if (backgroundClassificationTypes != null) {
            for (TypeDefLink backgroundType : backgroundClassificationTypes.values()) {
                when(metadataCollection.constructTypeDefLink(backgroundType.getName(), TypeCategory.CLASSIFICATION)).
                        thenReturn(backgroundType);
            }
        }
        if (backgroundEntityTypes != null) {
            for (TypeDefLink backgroundType : backgroundEntityTypes.values()) {
                when(metadataCollection.constructTypeDefLink(backgroundType.getName(), TypeCategory.ENTITY)).
                        thenReturn(backgroundType);
            }
        }

        String userId = "test_user";

        ClassificationDef omClassificationDef = null;

        LOG.debug("TestAtlasClassificationDefMapper: Construct AtlasClassificationDefMapper");
        try {

            AtlasClassificationDefMapper atlasClassificationDefMapper = new AtlasClassificationDefMapper(metadataCollection, userId, atlasClassificationDef);
            omClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();

        } catch (TypeErrorException e) {
            LOG.debug("TestAtlasClassificationDefMapper: Caught TypeErrorException exception");
            if (testResult.equals(TestResult.TypeErrorException)) {
                LOG.debug("TestAtlasClassificationDefMapper: Test Passed");
                return;
            }
            else {
                LOG.debug("TestAtlasClassificationDefMapper: Exception was unexpected");
                fail();
            }
        }
        // Should have returned an OM EntityDef
        assertNotNull(omClassificationDef);

        Comparator c;
        boolean match;

        switch(testResult) {

            case OK:
                // Not using Comparator because we are directly comparing Atlas vs OM
                // We know that atlasClassificationDef is not null

                // OM ClassificationDef should have same name as Atlas ClassificationDef
                String expectedName = atlasClassificationDef.getName();
                assertEquals(omClassificationDef.getName(), expectedName);

                // OM ClassificationDef should have same guid as AtlasClassificationDef
                assertEquals(omClassificationDef.getGUID(), atlasClassificationDef.getGuid());

                // OM ClassificationDef should have same version as AtlasClassificationDef
                assertEquals(omClassificationDef.getVersion(), atlasClassificationDef.getVersion().longValue());

                // OM ClassificationDef should have correct superTypes
                if (expectedSuperTypes != null) {
                    // Compare the supertypes
                    c = new Comparator();
                    match = c.compare(omClassificationDef.getSuperType(), expectedSuperTypes.get(0));
                    assertEquals(match, true);
                }

                // OM ClassificationDef should have correct superTypes
                if (expectedValidEntityDefs != null) {
                    // Compare the supertypes
                    c = new Comparator();
                    match = c.compareListTypeDefLink(omClassificationDef.getValidEntityDefs(), expectedValidEntityDefs);
                    assertEquals(match, true);
                }

                // Check the attributes are intact
                if (expectedAttributeDefs != null) {
                    TypeDefAttribute expectedAttr1TDA = expectedAttributeDefs.get("1");
                    String expectedAttr1Name = expectedAttr1TDA.getAttributeName();
                    String expectedAttr1TypeName = expectedAttr1TDA.getAttributeType().getName();
                    List<TypeDefAttribute> propsDef = omClassificationDef.getPropertiesDefinition();
                    LOG.debug("TestAtlasClassificationDefMapper: propsDef = {}", propsDef);
                    TypeDefAttribute actualAttr1TDA = omClassificationDef.getPropertiesDefinition().get(0);
                    String actualAttr1Name = expectedAttr1TDA.getAttributeName();
                    String actualAttr1TypeName = expectedAttr1TDA.getAttributeType().getName();
                    assertEquals(actualAttr1Name, expectedAttr1Name);
                    assertEquals(actualAttr1TypeName, expectedAttr1TypeName);
                }

                LOG.debug("TestAtlasClassificationDefMapper: Test Passed");
                break;


            default:
                LOG.debug("TestAtlasClassificationDefMapper: Unexpected result from mapper");
                fail();
                break;
        }
    }
}
