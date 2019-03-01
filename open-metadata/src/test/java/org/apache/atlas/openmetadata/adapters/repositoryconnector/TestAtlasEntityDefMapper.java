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


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasEntityDef;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
 * Test the AtlasEntityDefMapper component of the AtlasConnector
 *
 * The constructor is trivial; it is the toOMEntityDef that is of primary interest.
 *
 */
public class TestAtlasEntityDefMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasEntityDefMapper.class);

    // Background types - these are used and reused across multiple tests
    private Map<String,TypeDefLink> backgroundEntityTypes = new HashMap<>();

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
        String            testCaption;
        AtlasEntityDef    atlasEntityDef;
        TestResult        expectedResult;
        List<TypeDefLink> expectedSuperTypes;
    }

    @DataProvider(name = "provideAtlasEntityDefs")
    public Object[][] provideData() {


        // 1. Test with no atlasEntityDef
        AtlasEntityDef atlasEntityDefNull = null;

        // 2. AtlasEntityDef with no name
        AtlasEntityDef atlasEntityDefNoName = new AtlasEntityDef();

        // 3. AtlasEntityDef with no guid
        AtlasEntityDef atlasEntityDefNoGUID = new AtlasEntityDef();
        atlasEntityDefNoGUID.setName(backgroundEntityTypes.get("type1").getName());

        // 4. AtlasEntityDef with no version
        AtlasEntityDef atlasEntityDefNoVersion = new AtlasEntityDef();
        atlasEntityDefNoVersion.setName(backgroundEntityTypes.get("type1").getName());
        atlasEntityDefNoVersion.setGuid(backgroundEntityTypes.get("type1").getGUID());

        // 5. AtlasEntityDef with no supertype
        AtlasEntityDef atlasEntityDefNotF5SupertypesNone = new AtlasEntityDef();
        atlasEntityDefNotF5SupertypesNone.setName(backgroundEntityTypes.get("type1").getName());
        atlasEntityDefNotF5SupertypesNone.setGuid(backgroundEntityTypes.get("type1").getGUID());
        atlasEntityDefNotF5SupertypesNone.setVersion(7l);

        // 6. AtlasEntityDef with one non-F5 supertype
        // expected supertypes
        List<TypeDefLink> expectedSuperTypes6 = new ArrayList<>();
        TypeDefLink type2 = backgroundEntityTypes.get("type2");
        expectedSuperTypes6.add(type2);
        // atlas entity def
        // has type backgroundType1, supertype is backgroundType2
        AtlasEntityDef atlasEntityDefNotF5SupertypeOne = new AtlasEntityDef();
        atlasEntityDefNotF5SupertypeOne.setName(backgroundEntityTypes.get("type1").getName());
        atlasEntityDefNotF5SupertypeOne.setGuid(backgroundEntityTypes.get("type1").getGUID());
        atlasEntityDefNotF5SupertypeOne.setVersion(7l);
        Set<String> superTypes6 = new HashSet<>();
        superTypes6.add(backgroundEntityTypes.get("type2").getName());
        atlasEntityDefNotF5SupertypeOne.setSuperTypes(superTypes6);

        // 7. AtlasEntityDef with two non-F5 supertypes
        // atlas entity def
        // has type type1, supertypes are type2 and type3 - should fail hence no expectedSuperTypes
        AtlasEntityDef atlasEntityDefNotF5SupertypeTwo = new AtlasEntityDef();
        atlasEntityDefNotF5SupertypeTwo.setName(backgroundEntityTypes.get("type1").getName());
        atlasEntityDefNotF5SupertypeTwo.setGuid(backgroundEntityTypes.get("type1").getGUID());
        atlasEntityDefNotF5SupertypeTwo.setVersion(7l);
        Set<String> superTypes7 = new HashSet<>();
        superTypes7.add("type2");
        superTypes7.add("type3");
        atlasEntityDefNotF5SupertypeTwo.setSuperTypes(superTypes7);

        // 8. AtlasEntityDef with one F5 supertype
        // expected supertypes
        List<TypeDefLink> expectedSuperTypes8 = new ArrayList<>();
        expectedSuperTypes8.add(backgroundEntityTypes.get("Referenceable"));
        // atlas entity def
        // has type type1, supertype is referenceable
        AtlasEntityDef atlasEntityDefNotF5SupertypeOneF5 = new AtlasEntityDef();
        atlasEntityDefNotF5SupertypeOneF5.setName(backgroundEntityTypes.get("type1").getName());
        atlasEntityDefNotF5SupertypeOneF5.setGuid(backgroundEntityTypes.get("type1").getGUID());
        atlasEntityDefNotF5SupertypeOneF5.setVersion(7l);
        Set<String> superTypes8 = new HashSet<>();
        // The Atlas supertype will be using the converted F5 name
        superTypes8.add("OM_Referenceable");
        atlasEntityDefNotF5SupertypeOneF5.setSuperTypes(superTypes8);

        // 9. AtlasEntityDef F5 with no supertypes
        // expected supertypes
        // This configuration should throw an exception so no supertypes on completion
        // atlas entity def
        // has type type1, supertype is referenceable
        AtlasEntityDef atlasEntityDefF5SupertypeNone = new AtlasEntityDef();
        atlasEntityDefF5SupertypeNone.setName("OM_Referenceable");
        atlasEntityDefF5SupertypeNone.setGuid(FamousFive.getRecordedGUID("Referenceable"));
        atlasEntityDefF5SupertypeNone.setVersion(7l);
        // Deliberately omit supertypes..

        // 10. AtlasEntityDef F5 with one F5 supertype
        // expected supertypes
        // This configuration is one that should have no supertypes on completion
        // atlas entity def
        // has type type1, supertype is referenceable
        AtlasEntityDef atlasEntityDefF5SupertypeOneF5 = new AtlasEntityDef();
        atlasEntityDefF5SupertypeOneF5.setName("OM_Referenceable");
        atlasEntityDefF5SupertypeOneF5.setGuid(FamousFive.getRecordedGUID("Referenceable"));
        atlasEntityDefF5SupertypeOneF5.setVersion(7l);
        Set<String> superTypes9 = new HashSet<>();
        // The Atlas supertype is the auto-added original Atlas type
        superTypes9.add("Referenceable");
        atlasEntityDefF5SupertypeOneF5.setSuperTypes(superTypes9);


        // 11. AtlasEntityDef F5 with two F5 supertypes
        // expected supertypes
        List<TypeDefLink> expectedSuperTypes11 = new ArrayList<>();
        expectedSuperTypes11.add(backgroundEntityTypes.get("Referenceable"));
        // This configuration is one that should have no supertypes on completion
        // atlas entity def
        // has type type1, supertype is referenceable
        AtlasEntityDef atlasEntityDefF5SupertypeTwoF5 = new AtlasEntityDef();
        atlasEntityDefF5SupertypeTwoF5.setName("OM_Asset");
        atlasEntityDefF5SupertypeTwoF5.setGuid(FamousFive.getRecordedGUID("Asset"));
        atlasEntityDefF5SupertypeTwoF5.setVersion(7l);
        Set<String> superTypes11 = new HashSet<>();
        // The Atlas supertype is the auto-added original Atlas type
        superTypes11.add("OM_Referenceable");
        superTypes11.add("Asset");
        atlasEntityDefF5SupertypeTwoF5.setSuperTypes(superTypes11);



        Object[][] test_data = new Object[][] {
                { "1. no entity def",            atlasEntityDefNull,                 TestResult.TypeErrorException,  null},
                { "2. no name",                  atlasEntityDefNoName,               TestResult.TypeErrorException,  null},
                { "3. no guid",                  atlasEntityDefNoGUID,               TestResult.TypeErrorException,  null},
                { "4. no version",               atlasEntityDefNoVersion,            TestResult.TypeErrorException,  null},
                { "5. not F5 no supertypes",     atlasEntityDefNotF5SupertypesNone,  TestResult.OK,                  null},
                { "6. not F5 one !F5 supertype", atlasEntityDefNotF5SupertypeOne,    TestResult.OK,                  expectedSuperTypes6},
                { "7. not F5 one F5 supertype",  atlasEntityDefNotF5SupertypeTwo,    TestResult.TypeErrorException,  null},
                { "8. not F5 one F5 supertype",  atlasEntityDefNotF5SupertypeOneF5,  TestResult.OK,                  expectedSuperTypes8},
                { "9. not F5 no supertypes",     atlasEntityDefF5SupertypeNone,      TestResult.TypeErrorException,  null},
                { "10. not F5 one F5 supertype", atlasEntityDefF5SupertypeOneF5,     TestResult.OK,                  null},
                { "11. not F5 one F5 supertype", atlasEntityDefF5SupertypeTwoF5,     TestResult.OK,                  expectedSuperTypes11},
        };

        return test_data;

    }

    /*
     * Parameters to the test method:
     * String            testCaption;
     * AtlasEntityDef    atlasEntityDef;
     * TestResult        expectedResult;
     * List<TypeDefLink> expectedSuperTypes;
     */
    @Test(dataProvider = "provideAtlasEntityDefs")
    public void test_AtlasEntityDef(String             testCaption,
                                    AtlasEntityDef     atlasEntityDef,
                                    TestResult         testResult,
                                    List<TypeDefLink>  expectedSuperTypes  )
        throws
            RepositoryErrorException  // not really thrown - but needed for when.thenReturn of getMetadataCollectionId()
    {

        LOG.debug("TEST: {}", testCaption);

        /*
         * Set up mocks
         */

        // MetadataCollection
        LocalAtlasOMRSMetadataCollection metadataCollection = mock(LocalAtlasOMRSMetadataCollection.class);

        when(metadataCollection.getMetadataCollectionId()).thenReturn("mock_metadata_collection");

        /*
         * Prepare the mock of constructTypeDefLink so that it will serve up names and guids of any background types
         */
        if (backgroundEntityTypes != null) {
            for (TypeDefLink backgroundType : backgroundEntityTypes.values()) {
                when(metadataCollection.constructTypeDefLink(backgroundType.getName(),TypeCategory.ENTITY)).
                        thenReturn(backgroundType);
            }
        }

        String userId = "test_user";

        EntityDef omEntityDef = null;

        LOG.debug("TestAtlasEntityDefMapper: Construct AtlasEntityDefMapper");
        try {

            AtlasEntityDefMapper atlasEntityDefMapper = new AtlasEntityDefMapper(metadataCollection, userId, atlasEntityDef);
            omEntityDef = atlasEntityDefMapper.toOMEntityDef();

        } catch (TypeErrorException e) {
            LOG.debug("TestAtlasEntityDefMapper: Caught TypeErrorException exception");
            if (testResult.equals(TestResult.TypeErrorException)) {
                LOG.debug("TestAtlasEntityDefMapper: Test Passed");
                return;
            }
            else {
                LOG.debug("TestAtlasEntityDefMapper: Exception was unexpected");
                fail();
            }
        }
        // Should have returned an OM EntityDef
        assertNotNull(omEntityDef);

        Comparator c;
        boolean match;

        switch(testResult) {

            case OK:
                // Not using Comparator because we are directly comparing Atlas vs OM
                // We know that atlasEntityDef is not null

                // OM EntityDef should have same name as Atlas EntityDef unless F5
                String expectedName = atlasEntityDef.getName();
                if (FamousFive.atlasTypeRequiresSubstitution(atlasEntityDef.getName()))
                    expectedName = FamousFive.getOMTypeName(atlasEntityDef.getName());
                assertEquals(omEntityDef.getName(), expectedName);

                // OM EntityDef should have same guid as Atlas EntityDef
                assertEquals(omEntityDef.getGUID(), atlasEntityDef.getGuid());

                // OM EntityDef should have same version as Atlas EntityDef
                assertEquals(omEntityDef.getVersion(), atlasEntityDef.getVersion().longValue());

                // OM EntityDef should have correct superTypes
                if (expectedSuperTypes != null) {
                    // Compare the supertypes
                    c = new Comparator();
                    match = c.compare(omEntityDef.getSuperType(), expectedSuperTypes.get(0));
                    assertEquals(match, true);
                }
                LOG.debug("TestAtlasEntityDefMapper: Test Passed");
                break;


            default:
                LOG.debug("TestAtlasEntityDefMapper: Unexpected result from mapper");
                fail();
                break;
        }
    }
}