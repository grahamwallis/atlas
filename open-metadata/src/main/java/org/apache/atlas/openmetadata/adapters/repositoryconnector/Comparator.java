/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.openmetadata.adapters.repositoryconnector;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;


/* Comparison utility class to compare complex objects.
 * This class is needed because equals() is defined differently for OMRS classes
 * that may be deemed to be equal if they match only by name and guid. We need a
 * more detailed comparison or exact matches and equivalence checks.
 *
 * The compare() method performs an exact match comparison, whereas an equivalent()
 * performs a semantic equivalence check.
 *
 * The first few methods may seem gratuitous but they make everything consistent.
 */

public class Comparator {

    private static final Logger LOG = LoggerFactory.getLogger(Comparator.class);

    // All arrays are assumed to be order significant unless specified in the list below.
    private static final boolean ARRAY_ORDER_SIGNIFICANT = true;
    private static final boolean ARRAY_ORDER_SIGNIFICANCE_SUPERTYPES = false;
    private static final boolean ARRAY_ORDER_SIGNIFICANCE_VALID_ENTITY_DEFS = false;

    private boolean compare(int a, int b) {
        return (a == b);
    }

    private boolean compare(boolean a, boolean b) {
        return (a == b);
    }

    private boolean compare(Long a, Long b) {
        return (a.equals(b));
    }

    private boolean compare(String a, String b) {
        if (a == null && b == null) {
            LOG.debug("Compare String: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare String: the {} string is null", a==null?"first":"second");
            return false;
        }
        if (!(a.equals(b))) {
            LOG.debug("Compare String: compare failed, {} vs {}", a,b);
            return false;
        }
        return true;
    }

    private boolean compare(Date a, Date b) {
        if (a == null && b == null) {
            LOG.debug("Compare Date: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare Date: the {} date is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.equals(b))) {
            LOG.debug("Compare Date: the dates are different {} vs {}", a, b);
            return false;
        }
        return true;

    }

    private boolean compare(Map<String, String> a, Map<String, String> b) {
        if (a == null && b == null) {
            LOG.debug("Compare Map: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare Map: the {} map is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare Map: maps are different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        for (String key : a.keySet()) {
            if (!(compare(a.get(key), b.get(key)))) {
                LOG.debug("Compare Map: map values are different {} vs {}", a.get(key), b.get(key));
                return false;
            }
        }
        return true;
    }

    public boolean compare(TypeDefLink a, TypeDefLink b) {
        return compare(true, a, b);
    }
    private boolean compare(boolean strict, TypeDefLink a, TypeDefLink b) {
        if (a == null && b == null) {
            LOG.debug("Compare TypeDefLink: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare TypeDefLink: the {} TypeDefLink is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getGUID(), b.getGUID()))) {
            LOG.debug("Compare TypeDefLink: GUID did not match {} vs {}", a.getGUID(), b.getGUID());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare TypeDefLink: Name did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        // Would need to delegate upward to TypeDefElementHeader if we also want to compare type version
        return true;
    }

    private boolean compare(TypeDefCategory a, TypeDefCategory b) {
        if (a == null && b == null) {
            LOG.debug("Compare TypeDefCategory: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare TypeDefCategory: the {} TypeDefCategory is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            LOG.debug("Compare TypeDefCategory: TypeCode did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare TypeDefCategory: TypeName did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare TypeDefCategory: TypeDescription did not match {} vs {}", a.getDescription(), b.getDescription());
            return false;
        }
        return true;
    }

    private boolean compare(TypeDefSummary a, TypeDefSummary b) {
        if (a == null && b == null) {
            LOG.debug("Compare TypeDefSummary: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare TypeDefSummary: the {} TypeDefSummary is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getVersion(),b.getVersion()))) {
            LOG.debug("Compare TypeDefSummary: Version did not match {} vs {}", a.getVersion(), b.getVersion());
            return false;
        }
        if (!(compare(a.getVersionName(),b.getVersionName()))) {
            LOG.debug("Compare TypeDefSummary: VersionName did not match {} vs {}", a.getVersionName(), b.getVersionName());
            return false;
        }
        if (!(compare(a.getCategory(),b.getCategory()))) {
            LOG.debug("Compare TypeDefSummary: Category did not match {} vs {}", a.getCategory(), b.getCategory());
            return false;
        }
        // Delegate upwards
        if (!(compare((TypeDefLink)a,(TypeDefLink)b))) {
            LOG.debug("Compare TypeDefSummary: TypeDefLink did not match");
            return false;
        }
        return true;
    }

    private boolean compare(ExternalStandardMapping a, ExternalStandardMapping b) {
        return compare(true, a, b);
    }
    private boolean compare(boolean strict, ExternalStandardMapping a, ExternalStandardMapping b) {
        if (a == null && b == null) {
            LOG.debug("Compare ExternalStandardMapping: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare ExternalStandardMapping: the {} ExternalStandardMapping is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getStandardName(),b.getStandardName()))) {
            LOG.debug("Compare ExternalStandardMapping: StandardName did not match {} vs {}", a.getStandardName(), b.getStandardName());
            return false;
        }
        if (!(compare(a.getStandardOrganization(),b.getStandardOrganization()))) {
            LOG.debug("Compare ExternalStandardMapping: StandardOrganization did not match {} vs {}", a.getStandardOrganization(), b.getStandardOrganization());
            return false;
        }
        if (!(compare(a.getStandardTypeName(),b.getStandardTypeName()))) {
            LOG.debug("Compare ExternalStandardMapping: StandardTypeName did not match {} vs {}", a.getStandardTypeName(), b.getStandardTypeName());
            return false;
        }
        return true;
    }

    private boolean compare(AttributeCardinality a, AttributeCardinality b) {
        if (a == null && b == null) {
            LOG.debug("Compare AttributeCardinality: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare AttributeCardinality: the {} AttributeCardinality is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            LOG.debug("Compare AttributeCardinality: Ordinal did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare AttributeCardinality: Name did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare AttributeCardinality: Description did not match {} vs {}", a.getDescription(), b.getDescription());
            return false;
        }
        return true;
    }


    private boolean compare(RelationshipEndCardinality a, RelationshipEndCardinality b) {
        if (a != b) {
            LOG.debug("Compare RelationshipEndCardinality: did not match {} vs {}", a, b);
            return false;
        }
        return true;
    }


    // Method is private because AttributeTypeDef is abstract
    // This method is delegated to by comparators for concrete classes:
    //  CollectionDef
    //  EnumDef
    //  PrimitiveDef
    private boolean compare(AttributeTypeDef a, AttributeTypeDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare AttributeTypeDef: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare AttributeTypeDef: the {} AttributeTypeDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getCategory(),b.getCategory()))) {
            LOG.debug("Compare AttributeTypeDef: Category did not match {} vs {}", a.getCategory(), b.getCategory());
            return false;
        }
        if (!(compare(a.getGUID(),b.getGUID()))) {
            LOG.debug("Compare AttributeTypeDef: GUID did not match: {} vs {}", a.getGUID(), b.getGUID());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare AttributeTypeDef: Name did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare AttributeTypeDef: Description did not match {} vs {}", a.getDescription(),b.getDescription());
            return false;
        }
        if (!(compare(a.getDescriptionGUID(),b.getDescriptionGUID()))) {
            LOG.debug("Compare AttributeTypeDef: descriptionGUID did not match {} vs {}", a.getDescriptionGUID(), b.getDescriptionGUID());
            return false;
        }
        // Delegate up to TypeDefElementHeader if we also need to compare type version
        return true;
    }


    public boolean compare(boolean strict, CollectionDef a, CollectionDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare CollectionDef: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare CollectionDef: the {} CollectionDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getCollectionDefCategory(),b.getCollectionDefCategory()))) {
            LOG.debug("Compare CollectionDef: CollectionDefCategory did not match {} vs {}", a.getCollectionDefCategory(), b.getCollectionDefCategory());
            return false;
        }
        if (!(compare(a.getArgumentCount(),b.getArgumentCount()))) {
            LOG.debug("Compare CollectionDef: ArgumentCount did not match {} vs {}", a.getArgumentCount(), b.getArgumentCount());
            return false;
        }
        if (!(compareListPrimitiveDefCategory(ARRAY_ORDER_SIGNIFICANT,a.getArgumentTypes(),b.getArgumentTypes()))) {
            LOG.debug("Compare CollectionDef: ArgumentTypes did not match {} vs {}", a.getArgumentTypes(), b.getArgumentTypes());
            return false;
        }
        // Delegate up to AttributeTypeDef
        if (!(compare((AttributeTypeDef)a,(AttributeTypeDef)b))) {
            LOG.debug("Compare CollectionDef: AttributeTypeDefs did not match");
            return false;
        }
        return true;
    }

    public boolean compare(EnumDef a, EnumDef b) {
        return compare(false, a, b);
    }
    public boolean compare(boolean strict, EnumDef a, EnumDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare EnumDef: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare EnumDef: the {} EnumDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compareListEnumElementDef(strict,a.getElementDefs(),b.getElementDefs()))) {
            LOG.debug("Compare EnumDef: ElementDefs did not match {} vs {}", a.getElementDefs(), b.getElementDefs());
            return false;
        }
        if (!(compare(a.getDefaultValue(),b.getDefaultValue()))) {
            LOG.debug("Compare EnumDef: DefaultValue did not match {} vs {}", a.getDefaultValue(), b.getDefaultValue());
            return false;
        }
        // Delegate up to AttributeTypeDef
        if (!(compare((AttributeTypeDef)a,(AttributeTypeDef)b))) {
            LOG.debug("Compare EnumDef: AttributeTypeDefs did not match");
            return false;
        }
        return true;
    }

    public boolean compare(boolean strict, PrimitiveDef a, PrimitiveDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare PrimitiveDef: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare PrimitiveDef: the {} PrimitiveDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(true, a.getPrimitiveDefCategory(),b.getPrimitiveDefCategory()))) {
            LOG.debug("Compare PrimitiveDef: PrimitiveDefCategory did not match {} vs {}", a.getPrimitiveDefCategory(), b.getPrimitiveDefCategory());
            return false;
        }
        // Delegate up to AttributeTypeDef
        if (!(compare((AttributeTypeDef)a,(AttributeTypeDef)b))) {
            LOG.debug("Compare PrimitiveDef: AttributeTypeDefs did not match");
            return false;
        }
        return true;
    }


    private boolean compareListEnumElementDef(boolean strict, List<EnumElementDef> a,  List<EnumElementDef> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<EnumElementDef>: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<EnumElementDef>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<EnumElementDef>: Arrays of EnumElementDef have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict,a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<EnumElementDef>: Elements are different {} vs {}",a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    //LOG.debug("compare i={},left={},j={},right={}",i,j,a.get(i),b.get(j));
                    if (compare(strict,a.get(i), b.get(j))) {
                        //LOG.debug("compare succeeded");
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<EnumElementDef>: Did not find {} in {}",a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean compare(EnumElementDef a, EnumElementDef b) {
        return compare(true, a, b);
    }
    private boolean compare(boolean strict, EnumElementDef a, EnumElementDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare EnumElementDef: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare EnumElementDef: the {} EnumElementDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            // likely to be in a list so only issue debug on a mismatch if in strict mode; handling for ordinal will suffice
            if (strict) LOG.debug("Compare EnumElementDef: Ordinal did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getValue(),b.getValue()))) {
            LOG.debug("Compare EnumElementDef: Value did not match {} vs {}", a.getValue(), b.getValue());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare EnumElementDef: Description did not match {} vs {}", a.getDescription(), b.getDescription());
            return false;
        }
        // Delegate up to TypeDefElementHeader if we also need to compare type version
        return true;
    }

    private boolean compare(CollectionDefCategory a, CollectionDefCategory b) {
        if (a == null && b == null) {
            LOG.debug("Compare CollectionDefCategory: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare CollectionDefCategory: the {} CollectionDefCategory is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            LOG.debug("Compare CollectionDefCategory: Code did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare CollectionDefCategory: Name did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getArgumentCount(),b.getArgumentCount()))) {
            LOG.debug("Compare CollectionDefCategory: ArgumentCount did not match {} vs {}", a.getArgumentCount(), b.getArgumentCount());
            return false;
        }
        if (!(compare(a.getJavaClassName(),b.getJavaClassName()))) {
            LOG.debug("Compare CollectionDefCategory: JavaClassName did not match {} vs {}", a.getJavaClassName(), b.getJavaClassName());
            return false;
        }
        return true;
    }

    private boolean compareListPrimitiveDefCategory(boolean strict, List<PrimitiveDefCategory> a, List<PrimitiveDefCategory> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<PrimitiveDefCategory>: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<PrimitiveDefCategory>: ListPrimitiveDefCategory: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<PrimitiveDefCategory>: Arrays of PrimitiveDefCategory have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<PrimitiveDefCategory>: Entries different {} vs {}", a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compare(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<PrimitiveDefCategory>: Could not find {} in {}", a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean compare(PrimitiveDefCategory a, PrimitiveDefCategory b) {
        return compare(true, a, b);
    }
    private boolean compare(boolean strict, PrimitiveDefCategory a, PrimitiveDefCategory b) {
        if (a == null && b == null) {
            LOG.debug("Compare PrimitiveDefCategory: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare PrimitiveDefCategory: the {} PrimitiveDefCategory is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            LOG.debug("Compare PrimitiveDefCategory: Code did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare PrimitiveDefCategory: Name did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getJavaClassName(),b.getJavaClassName()))) {
            LOG.debug("Compare PrimitiveDefCategory: JavaClassName did not match {} vs {}", a.getJavaClassName(), b.getJavaClassName());
            return false;
        }
        if (!(compare(a.getGUID(),b.getGUID()))) {
            LOG.debug("Compare PrimitiveDefCategory: GUID did not match {} vs {}", a.getGUID(), b.getGUID());
            return false;
        }
        return true;
    }

    private boolean compare(AttributeTypeDefCategory a, AttributeTypeDefCategory b) {
        if (a == null && b == null) {
            LOG.debug("Compare AttributeTypeDefCategory: both null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare AttributeTypeDefCategory: the {} AttributeTypeDefCategory is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            LOG.debug("Compare AttributeTypeDefCategory: TypeCode did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare AttributeTypeDefCategory: TypeName did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare AttributeTypeDefCategory: TypeDescription did not match {} vs {}", a.getDescription(), b.getDescription());
            return false;
        }
        return true;
    }

    private boolean compare(TypeDefAttribute a, TypeDefAttribute b) {
            return compare(true, a, b);
    }
    private boolean compare(boolean strict, TypeDefAttribute a, TypeDefAttribute b) {
        if (a == null && b == null) {
            LOG.debug("Compare TypeDefAttribute: both TypeDefAttributes are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare TypeDefAttribute: TypeDefAttribute: the {} TypeDefAttribute is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getAttributeName(),b.getAttributeName()))) {
            LOG.debug("Compare TypeDefAttribute: AttributeName did not match {} vs {}", a.getAttributeName(), b.getAttributeName());
            return false;
        }
        if (!(compare(a.getAttributeType(),b.getAttributeType()))) {
            LOG.debug("Compare TypeDefAttribute: AttributeType did not match {} vs {}", a.getAttributeType(), b.getAttributeType());
            return false;
        }
        if (!(compare(a.getAttributeDescription(),b.getAttributeDescription()))) {
            LOG.debug("Compare TypeDefAttribute: AttributeDescription did not match {} vs {}", a.getAttributeDescription(), b.getAttributeDescription());
            return false;
        }
        if (!(compare(a.getAttributeCardinality(),b.getAttributeCardinality()))) {
            LOG.debug("Compare TypeDefAttribute: AttributeCardinality did not match {} vs {}", a.getAttributeCardinality(), b.getAttributeCardinality());
            return false;
        }
        if (!(compare(a.getValuesMinCount(),b.getValuesMinCount()))) {
            LOG.debug("Compare TypeDefAttribute: ValuesMinCount did not match {} vs {}", a.getValuesMinCount(), b.getValuesMinCount());
            return false;
        }
        if (!(compare(a.getValuesMaxCount(),b.getValuesMaxCount()))) {
            LOG.debug("Compare TypeDefAttribute: ValuesMaxCount did not match {} vs {}", a.getValuesMaxCount(), b.getValuesMaxCount());
            return false;
        }
        if (!(compare(a.isIndexable(),b.isIndexable()))) {
            LOG.debug("Compare TypeDefAttribute: isIndexable did not match {} vs {}", a.isIndexable(), b.isIndexable());
            return false;
        }
        if (!(compare(a.isUnique(),b.isUnique()))) {
            LOG.debug("Compare TypeDefAttribute: Unique did not match {} vs {}", a.isUnique(), b.isUnique());
            return false;
        }
        if (!(compare(a.getDefaultValue(),b.getDefaultValue()))) {
            LOG.debug("Compare TypeDefAttribute: DefaultValue did not match {} vs {}", a.getDefaultValue(), b.getDefaultValue());
            return false;
        }
        if (!(compareListExternalStandardMappings(ARRAY_ORDER_SIGNIFICANT, a.getExternalStandardMappings(),b.getExternalStandardMappings()))) {
            LOG.debug("Compare TypeDefAttribute: ExternalStandardMappings did not match {} vs {}", a.getExternalStandardMappings(), b.getExternalStandardMappings());
            return false;
        }
        return true;
    }

    private boolean compareListExternalStandardMappings(boolean strict, List<ExternalStandardMapping> a, List<ExternalStandardMapping> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<ExternalStandardMappings>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<ExternalStandardMappings>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<ExternalStandardMappings>: Arrays of ExternalStandardMappings have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<ExternalStandardMappings>: elements are different {} vs {}", a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compare(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<ExternalStandardMappings>: could not find {} in {}", a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean compareListTypeDefAttribute(boolean strict, List<TypeDefAttribute> a, List<TypeDefAttribute> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<TypeDefAttribute>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<TypeDefAttribute>: ListTypeDefAttribute: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<TypeDefAttribute>: Arrays of TypeDef have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<TypeDefAttribute>: elements differ {} vs {}", a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compare(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<TypeDefAttribute>: could not find {} in {}", a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    public boolean compareListTypeDefLink(List<TypeDefLink> a, List<TypeDefLink> b) { return compareListTypeDefLink(true, a,b); }
    private boolean compareListTypeDefLink(boolean strict, List<TypeDefLink> a, List<TypeDefLink> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<TypeDefLink>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<TypeDefLink>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<TypeDefLink>: Arrays of TypeDefLink have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<TypeDefLink>: elements different {} vs {}",a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compare(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<TypeDefLink>: could not find {} in {}",a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean compareListExternalStandardMapping(boolean strict, List<ExternalStandardMapping> a, List<ExternalStandardMapping> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<ExternalStandardMapping>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<ExternalStandardMapping>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<ExternalStandardMapping>: Arrays of ExternalStandardMapping have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<ExternalStandardMapping>: elements different {} vs {}",  a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compare(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<ExternalStandardMapping>: could not find {} in {}",  a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean compare(InstanceStatus a, InstanceStatus b) {
            return compare(true, a, b);
    }
    private boolean compare(boolean strict, InstanceStatus a, InstanceStatus b) {
        if (a == null && b == null) {
            LOG.debug("Compare InstanceStatus: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare InstanceStatus: InstanceStatus: the {} InstanceStatus is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            // Only flag a mismatch if struct. Non-strict is used in comparison of a list of instance status
            if (strict) LOG.debug("Compare InstanceStatus: Ordinal did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Compare InstanceStatus: StatusName did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare InstanceStatus: StatusDescription did not match {} vs {}", a.getDescription(), b.getDescription());
            return false;
        }
        return true;
    }

    private boolean compareListInstanceStatus(boolean strict, List<InstanceStatus> a, List<InstanceStatus> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<InstanceStatus>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<InstanceStatus>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<InstanceStatus>: Arrays of InstanceStatus have different sizes {} vs {}",a.size(),b.size());
            return false;
        }
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compare(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<InstanceStatus>: elements different {} vs {}",a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compare(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<InstanceStatus>: could not find {} in {}",a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    // Method is private because TypeDef is abstract
    // This method is delegated to by List comparator and comparators for concrete classes:
    //  ClassificationDef
    //  EntityDef
    //  RelationshipDef
    private boolean compare(TypeDef a,TypeDef b) {
        return compare(true, a, b);
    }
    private boolean compare(boolean strict, TypeDef a,TypeDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare TypeDef: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare TypeDef: the {} parameter is null {}", a==null?"first":"second");
            return false;
        }
        // assumed non-strict because order of superTypes does not matter
        if (!(compare(a.getSuperType(),b.getSuperType()))) {
            LOG.debug("Compare TypeDef: SuperTypes did not match {} vs {}", a.getSuperType(),b.getSuperType());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Compare TypeDef: Description did not match {} vs {}", a.getDescription(),b.getDescription());
            return false;
        }
        if (!(compare(a.getDescriptionGUID(),b.getDescriptionGUID()))) {
            LOG.debug("Compare TypeDef: descriptionGUID did not match {} vs {}", a.getDescriptionGUID(), b.getDescriptionGUID());
            return false;
        }
        if (!(compare(a.getOrigin(),b.getOrigin()))) {
            LOG.debug("Compare TypeDef: Origin did not match {} vs {}", a.getOrigin(),b.getOrigin());
            return false;
        }
        if (!(compare(a.getCreatedBy(),b.getCreatedBy()))) {
            LOG.debug("Compare TypeDef: CreatedBy did not match {} vs {}", a.getCreatedBy(),b.getCreatedBy());
            return false;
        }
        if (!(compare(a.getUpdatedBy(),b.getUpdatedBy()))) {
            LOG.debug("Compare TypeDef: UpdatedBy did not match {} vs {}", a.getUpdatedBy(),b.getUpdatedBy());
            return false;
        }
        if (!(compare(a.getCreateTime(),b.getCreateTime()))) {
            LOG.debug("Compare TypeDef: CreateTime did not match {} vs {}", a.getCreateTime(),b.getCreateTime());
            return false;
        }
        if (!(compare(a.getUpdateTime(),b.getUpdateTime()))) {
            LOG.debug("Compare TypeDef: UpdateTime did not match {} vs {}", a.getUpdateTime(),b.getUpdateTime());
            return false;
        }
        if (!(compare(a.getOptions(),b.getOptions()))) {
            LOG.debug("Compare TypeDef: Options did not match {} vs {}", a.getOptions(),b.getOptions());
            return false;
        }
        if (!(compareListExternalStandardMapping(ARRAY_ORDER_SIGNIFICANT,a.getExternalStandardMappings(),b.getExternalStandardMappings()))) {
            LOG.debug("Compare TypeDef: ExternalStandardMappings did not match {} vs {}", a.getExternalStandardMappings(),b.getExternalStandardMappings());
            return false;
        }
        if (!(compareListInstanceStatus(ARRAY_ORDER_SIGNIFICANT,a.getValidInstanceStatusList(),b.getValidInstanceStatusList()))) {
            LOG.debug("Compare TypeDef: ValidInstanceStatusList did not match {} vs {}", a.getValidInstanceStatusList(),b.getValidInstanceStatusList());
            return false;
        }
        if (!(compare(a.getInitialStatus(),b.getInitialStatus()))) {
            LOG.debug("Compare TypeDef: InitialStatus did not match {} vs {}", a.getInitialStatus(),b.getInitialStatus());
            return false;
        }
        if (!(compareListTypeDefAttribute(ARRAY_ORDER_SIGNIFICANT,a.getPropertiesDefinition(),b.getPropertiesDefinition()))) {
            LOG.debug("Compare TypeDef: PropertiesDefinition did not match {} vs {}", a.getPropertiesDefinition(),b.getPropertiesDefinition());
            return false;
        }
        // Delegate upwards
        if (!(compare((TypeDefSummary)a,(TypeDefSummary)b))) {
            LOG.debug("Compare TypeDef: TypeDefSummary did not match");
            return false;
        }
        return true;
    }


    // This method enables us to compare arrays of heterogeneous def types - e.g. a mixture of ClassificationDef, EntityDef, etc..
    public boolean compareListTypeDef(boolean strict, List<TypeDef> a, List<TypeDef> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<TypeDef>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<TypeDef>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<TypeDef>: arrays have different sizes {} vs {}", a.size(), b.size());
            return false;
        }
        // Compare the arrays - for each element do not call TypeDef compare directly because TypeDef is abstract - instead call the concrete class comparator
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compareConcreteTypeDefs(strict,  a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<TypeDef>: elements different {} vs {}", a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compareConcreteTypeDefs(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<TypeDef>: could not find {} in {}", a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    // This method enables us to compare arrays of attribute type defs
    public boolean compareListAttributeTypeDef(boolean strict, List<AttributeTypeDef> a, List<AttributeTypeDef> b) {
        if (a == null && b == null) {
            LOG.debug("Compare List<AttributeTypeDef>: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare List<AttributeTypeDef>: the {} array is null {}", a==null?"first":"second");
            return false;
        }
        if (!(a.size() == b.size())) {
            LOG.debug("Compare List<AttributeTypeDef>: arrays have different sizes {} vs {}", a.size(), b.size());
            return false;
        }
        // Compare the arrays - for each element do not call AttributeTypeDef compare directly because AttributeTypeDef is abstract -
        // instead call the concrete class comparator - e.g. compare PrimitiveDef, CollectionDef, EnumDef etc.
        if (strict) {
            for (int i=0; i<a.size();i++) {
                if (!(compareConcreteAttributeTypeDefs(strict, a.get(i),b.get(i)))) {
                    LOG.debug("Compare List<AttributeTypeDef>: elements different {} vs {}", a.get(i),b.get(i));
                    return false;
                }
            }
            return true;
        }
        if (!strict) {
            for (int i = 0; i < a.size(); i++) {
                boolean found = false;
                for (int j = 0; j < b.size(); j++) {
                    if (compareConcreteAttributeTypeDefs(strict, a.get(i), b.get(j))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.debug("Compare List<AttributeTypeDef>: could not find {} in {}", a.get(i),b);
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    // strict is only used to disable debugging in the case where we are performing non-strict compares
    private boolean compareConcreteAttributeTypeDefs(boolean strict, AttributeTypeDef a, AttributeTypeDef b) {
        // Check they are the same class
        String aClass = a.getClass().getSimpleName();
        String bClass = b.getClass().getSimpleName();
        if (!(aClass.equals(bClass))) {
            LOG.debug("ConcreteAttributeTypeDef classes different {} vs {}", aClass, bClass);
            return false;
        }
        // Compare the concrete class objects
        switch (aClass) {
            case "PrimitiveDef":
                if (!(compare(strict, (PrimitiveDef) a, (PrimitiveDef) b))) {
                    LOG.debug("Compare PrimitiveDef: returned false");
                    return false;
                }
                break;
            case "CollectionDef":
                if (!(compare(strict, (CollectionDef)a,(CollectionDef)b))) {
                    LOG.debug("Compare CollectionDef: returned false");
                    return false;
                }
                break;
            case "EnumDef":
                if (!(compare(strict, (EnumDef)a,(EnumDef)b))) {
                    LOG.debug("Compare EnumDef: returned false");
                    return false;
                }
                break;
        }
        return true;
    }


    // strict is only used to disable debug logging where we are performing non-strict compares
    private boolean compareConcreteTypeDefs(boolean strict, TypeDef a, TypeDef b) {
        // Check they are the same class
        String aClass = a.getClass().getSimpleName();
        String bClass = b.getClass().getSimpleName();
        if (!(aClass.equals(bClass))) {
            LOG.debug("TypeDef classes different {} vs {}", aClass, bClass);
            return false;
        }
        // Compare the concrete class objects
        switch (aClass) {
            case "ClassificationDef":
                if (!(compare(strict, (ClassificationDef) a, (ClassificationDef) b))) {
                    LOG.debug("Compare ClassificationDef: returned false");
                    return false;
                }
                break;
            case "EntityDef":
                if (!(compare(strict, (EntityDef)a,(EntityDef)b))) {
                    LOG.debug("Compare EntityDef: returned false");
                    return false;
                }
                break;
            case "RelationshipDef":
                if (!(compare(strict, (RelationshipDef)a,(RelationshipDef)b))) {
                    LOG.debug("Compare RelationshipDef: returned false");
                    return false;
                }
                break;
        }
        return true;
    }

    public boolean compare(ClassificationDef a, ClassificationDef b) {
            return compare(true, a, b);
    }
    public boolean compare(boolean strict, ClassificationDef a, ClassificationDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare ClassificationDef: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare ClassificationDef: the {} ClassificationDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compareListTypeDefLink(ARRAY_ORDER_SIGNIFICANCE_VALID_ENTITY_DEFS,a.getValidEntityDefs(),b.getValidEntityDefs()))) {
            LOG.debug("Compare ClassificationDef: validEntityDefs did not match {} vs {}", a.getValidEntityDefs(),b.getValidEntityDefs());
            return false;
        }
        if (!(compare(a.isPropagatable(),b.isPropagatable()))) {
            LOG.debug("Compare ClassificationDef: isPropagatable did not match {} vs {}", a.isPropagatable(),b.isPropagatable());
            return false;
        }
        // Delegate upwards
        if (!(compare(strict, (TypeDef)a,(TypeDef)b))) {
            LOG.debug("Compare ClassificationDef: TypeDef did not match");
            return false;
        }
        return true;
    }

    public boolean compare(EntityDef a, EntityDef b) {
        return compare(true, a, b);
    }
    public boolean compare(boolean strict, EntityDef a, EntityDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare EntityDef: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare EntityDef: the {} EntityDef is null {}", a==null?"first":"second");
            return false;
        }
        // Delegate upwards
        if (!(compare(strict, (TypeDef)a,(TypeDef)b))) {
            LOG.debug("Compare EntityDef: TypeDef did not match");
            return false;
        }
        return true;
    }

    public boolean compare(RelationshipDef a, RelationshipDef b) {
        return compare(true, a, b);
    }
    public boolean compare(boolean strict, RelationshipDef a, RelationshipDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare RelationshipDef: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare RelationshipDef: the {} RelationshipDef is null {}", a==null?"first":"second");
            return false;
        }
        // The following code is preserved in case relationship category and containership are re-introduced.
        // if (!(compare(a.getRelationshipCategory(),b.getRelationshipCategory()))) {
        //    LOG.debug("RelationshipDef: RelationshipCategory did not match {} vs {}", a.getRelationshipCategory(),b.getRelationshipCategory());
        //    return false;
        //}
        // if (!(compare(a.getRelationshipContainerEnd(),b.getRelationshipContainerEnd()))) {
        //    LOG.debug("RelationshipDef: RelationshipContainerEnd did not match {} vs {}", a.getRelationshipContainerEnd(),b.getRelationshipContainerEnd());
        //    return false;
        //}
        if (!(compare(a.getPropagationRule(),b.getPropagationRule()))) {
            LOG.debug("Compare RelationshipDef: ClassificationPropagationRule did not match {} vs {}", a.getPropagationRule(),b.getPropagationRule());
            return false;
        }
        if (!(compare(a.getEndDef1(),b.getEndDef1()))) {
            LOG.debug("Compare RelationshipDef: RelationshipEndDef endDef1 did not match {} vs {}", a.getEndDef1(),b.getEndDef1());
            return false;
        }
        if (!(compare(a.getEndDef2(),b.getEndDef2()))) {
            LOG.debug("Compare RelationshipDef: RelationshipEndDef endDef2 did not match {} vs {}", a.getEndDef2(),b.getEndDef2());
            return false;
        }
        // Delegate upwards
        if (!(compare(strict, (TypeDef)a,(TypeDef)b))) {
            LOG.debug("Compare RelationshipDef: TypeDef did not match");
            return false;
        }
        return true;
    }

//    public boolean compare(RelationshipCategory a, RelationshipCategory b) {
//        if (a == null && b == null) {
//            LOG.debug("Compare RelationshipCategory: both are null");
//            return true;
//        }
//        if (a == null ^ b == null) {
//            LOG.debug("Compare RelationshipCategory: the {} RelationshipCategory is null {}", a==null?"first":"second");
//            return false;
//        }
//        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
//            LOG.debug("Compare RelationshipCategory: Ordinal did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
//            return false;
//        }
//        if (!(compare(a.getName(),b.getName()))) {
//            LOG.debug("Compare RelationshipCategory: Name did not match {} vs {}", a.getName(), b.getName());
//            return false;
//        }
//        if (!(compare(a.getDescription(),b.getDescription()))) {
//            LOG.debug("Compare RelationshipCategory: Description did not match {} vs {}", a.getDescription(), b.getDescription());
//            return false;
//        }
//        return true;
//    }



//    public boolean compare(RelationshipContainerEnd a, RelationshipContainerEnd b) {
//        if (a == null && b == null) {
//            LOG.debug("Compare RelationshipContainerEnd: both are null");
//            return true;
//       }
//        if (a == null ^ b == null) {
//            LOG.debug("Compare RelationshipContainerEnd: the {} RelationshipContainerEnd is null {}", a==null?"first":"second");
//            return false;
//        }
//        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
//            LOG.debug("Compare RelationshipContainerEnd: Ordinal did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
//            return false;
//        }
//        if (!(compare(a.getName(),b.getName()))) {
//            LOG.debug("Compare RelationshipContainerEnd: Name did not match {} vs {}", a.getName(), b.getName());
//            return false;
//        }
//        if (!(compare(a.getDescription(),b.getDescription()))) {
//            LOG.debug("Compare RelationshipContainerEnd: Description did not match {} vs {}", a.getDescription(), b.getDescription());
//            return false;
//        }
//        return true;
//    }


    public boolean compare(ClassificationPropagationRule a, ClassificationPropagationRule b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("ClassificationPropagationRule: the {} ClassificationPropagationRule is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getOrdinal(),b.getOrdinal()))) {
            LOG.debug("Ordinal did not match {} vs {}", a.getOrdinal(), b.getOrdinal());
            return false;
        }
        if (!(compare(a.getName(),b.getName()))) {
            LOG.debug("Name did not match {} vs {}", a.getName(), b.getName());
            return false;
        }
        if (!(compare(a.getDescription(),b.getDescription()))) {
            LOG.debug("Description did not match {} vs {}", a.getDescription(), b.getDescription());
            return false;
        }
        return true;
    }

    public boolean compare(RelationshipEndDef a, RelationshipEndDef b) {
        if (a == null && b == null) {
            LOG.debug("Compare RelationshipEndDef: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare RelationshipEndDef: the {} RelationshipEndDef is null {}", a==null?"first":"second");
            return false;
        }
        if (!(compare(a.getEntityType(),b.getEntityType()))) {
            LOG.debug("Compare RelationshipEndDef: EntityType did not match {} vs {}", a.getEntityType(), b.getEntityType());
            return false;
        }
        if (!(compare(a.getAttributeName(),b.getAttributeName()))) {
            LOG.debug("Compare RelationshipEndDef: AttributeName did not match {} vs {}", a.getAttributeName(), b.getAttributeName());
            return false;
        }
        if (!(compare(a.getAttributeDescription(),b.getAttributeDescription()))) {
            LOG.debug("Compare RelationshipEndDef: AttributeDescription did not match {} vs {}", a.getAttributeDescription(), b.getAttributeDescription());
            return false;
        }
        if (!(compare(a.getAttributeCardinality(),b.getAttributeCardinality()))) {
            LOG.debug("Compare RelationshipEndDef: AttributeCardinality did not match {} vs {}", a.getAttributeCardinality(), b.getAttributeCardinality());
            return false;
        }
        return true;
    }

    // Compare TypeDefGallery objects
    // This method enables us to compare arrays of heterogeneous def types - e.g. a mixture of ClassificationDef, EntityDef, etc..
    public boolean compare(boolean strict, TypeDefGallery a, TypeDefGallery b) {
        if (a == null && b == null) {
            LOG.debug("Compare TypeDefGallery: both are null");
            return true;
        }
        if (a == null ^ b == null) {
            LOG.debug("Compare TypeDefGallery: the {} gallery is null {}", a==null?"first":"second");
            return false;
        }
        // Compare the galleries
        LOG.debug("Compare TypeDefGallery: compare lists of TypeDef");
        if (!(compareListTypeDef(strict, a.getTypeDefs(), b.getTypeDefs())))
            return false;
        LOG.debug("Compare TypeDefGallery: compare lists of AttributeTypeDef");
        if (!(compareListAttributeTypeDef(strict, a.getAttributeTypeDefs(), b.getAttributeTypeDefs())))
            return false;
        return true;
    }



    /**
     * This method performs an 'equivalence' check on a pair of TypeDef objects. They can be considered
     * equivalent if the following fields match:
     *   category
     *   name
     *   guid
     *   supertypes,
     *   properties,
     *   classifications,
     *   valid status list,
     *   relationship end defs
     *   cardinality,
     *   relationship category - e.g. association vs composition
     *   version??
     *   versionName??
     *
     * All other fields can differ. Specifically, this method does not care about the following:
     *   createdBy
     *   updatedBy
     *   createdTime
     *   updatedTime
     *   externalStandardsMappings
     *   origin
     *   description and descriptionGUID
     *   initialStatus
     *
     * This type of check is needed during verifyTypeDef().
     *
     * @param def1 - the first TypeDef in the equivalence check
     * @param def2 - the second TypeDef in the equivalence check
     * @return - whether or not the objects are equivalent
     */
    public boolean equivalent(TypeDef def1, TypeDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent TypeDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent TypeDef: the {} Typedef is null {}", def1==null?"first":"second");
            return false;
        }

        // Delegate up to Compare for TypeDefSummary (everything at that level and above must match).
        if ( !compare( (TypeDefSummary)def1,(TypeDefSummary)def2 ) ) {
            LOG.debug("Equivalent TypeDef: TypeDefSummary different {} vs {}", def1, def2);
            return false;
        }

        // Compare just the necessary subset of fields of TypeDef
        if ( !compare(def1.getOptions(),def2.getOptions()) ) {
            LOG.debug("Equivalent TypeDef: Options different {} vs {}", def1.getOptions(),def2.getOptions());
            return false;
        }
        if ( !compareListInstanceStatus(false, def1.getValidInstanceStatusList(),def2.getValidInstanceStatusList()) ) {
            LOG.debug("Equivalent TypeDef: ValidInstanceStatusList different {} vs {}", def1.getValidInstanceStatusList(),def2.getValidInstanceStatusList());
            return false;
        }
        if ( !compareListTypeDefAttribute(false, def1.getPropertiesDefinition(),def2.getPropertiesDefinition()) ) {
            LOG.debug("Equivalent TypeDef: PropertiesDefinition different {} vs {}", def1.getPropertiesDefinition(),def2.getPropertiesDefinition());
            return false;
        }
        return true;
    }

    public boolean equivalent(EntityDef def1, EntityDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent EntityDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent EntityDef: the {} EntityDef is null {}", def1==null?"first":"second");
            return false;
        }
        /*
         * Delegate up to TypeDef equivalence check
         */
        if (!equivalent((TypeDef) def1, (TypeDef) def2)) {
            LOG.debug("Equivalent EntityDef: TypeDefs are not equivalent");
            return false;
        }
        /*
         * EntityDef adds no further significant fields
         */
        return true;
    }

    public boolean equivalent(ClassificationDef def1, ClassificationDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent ClassificationDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent ClassificationDef: the {} ClassificationDef is null {}", def1==null?"first":"second");
            return false;
        }
        /*
         * Delegate up to TypeDef equivalence check
         */
        if (!equivalent((TypeDef) def1, (TypeDef) def2)) {
            LOG.debug("Equivalent ClassificationDef: TypeDefs are not equivalent");
            return false;
        }
        /*
         * ClassificationDef adds the following significant fields
         *   validEntityDefs
         *   propagatable
         */
        if ( !compareListTypeDefLink(false, def1.getValidEntityDefs(),def2.getValidEntityDefs()) ) {
            LOG.debug("Equivalent ClassificationDef: Compare of getValidEntityDefs failed");
            return false;
        }
        if ( !compare(def1.isPropagatable(),def2.isPropagatable()) ) {
            LOG.debug("Equivalent ClassificationDef: Compare of isPropagatable failed");
            return false;
        }
        return true;
    }

    public boolean equivalent(RelationshipDef def1, RelationshipDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent RelationshipDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent RelationshipDef: the {} RelationshipDef is null {}", def1==null?"first":"second");
            return false;
        }
        /*
         * Delegate up to TypeDef equivalence check
         */
        if (!equivalent((TypeDef) def1, (TypeDef) def2)) {
            LOG.debug("Equivalent RelationshipDef: TypeDefs are not equivalent");
            return false;
        }
        /*
         * RelationshipDef adds the following significant fields
         *   relationshipCategory
         *   relationshipContainerEnd
         *   propagationRule
         *   endDef1
         *   endDef2
         */
        // The following code is preserved in case relationship category and containership are re-introduced.
        // if ( !compare(def1.getRelationshipCategory(),def2.getRelationshipCategory()) )
        //    return false;
        // if ( !compare(def1.getRelationshipContainerEnd(),def2.getRelationshipContainerEnd()) )
        //    return false;
        if ( !compare(def1.getPropagationRule(),def2.getPropagationRule()) ) {
            LOG.debug("Equivalent RelationshipDef: compare getPropagationRule failed");
            return false;
        }
        // We don't need the ends to match exactly - descriptions and descriptionGUIDs can differ....
        if ( !equivalent(def1.getEndDef1(),def2.getEndDef1()) ) {
            LOG.debug("Equivalent RelationshipDef: equivalent getEndDef1 failed");
            return false;
        }
        if ( !equivalent(def1.getEndDef2(),def2.getEndDef2()) ) {
            LOG.debug("Equivalent RelationshipDef: equivalent getEndDef2 failed");
            return false;
        }
        return true;
    }

    public boolean equivalent(RelationshipEndDef def1, RelationshipEndDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent RelationshipEndDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent RelationshipEndDef: the {} RelationshipEndDef is null {}", def1==null?"first":"second");
            return false;
        }
        /*
         * RelationshipEndDef adds the following significant fields
         *   entityType
         *   attributeName
         *   attributeCardinality
         */
        if ( !compare(def1.getEntityType(), def2.getEntityType())) {
            LOG.debug("Equivalent RelationshipEndDef: compare getEntityType failed");
            return false;
        }
        if ( !compare(def1.getAttributeName(), def2.getAttributeName())) {
            LOG.debug("Equivalent RelationshipEndDef: compare getAttributeName failed");
            return false;
        }
        if ( !compare(def1.getAttributeCardinality(), def2.getAttributeCardinality())) {
            LOG.debug("Equivalent RelationshipEndDef: compare getAttributeCardinality failed");
            return false;
        }
        return true;
    }



    /**
     * This method performs an 'equivalence' check on a pair of AttributeTypeDef objects. They can be considered
     * equivalent if the following fields match:
     *   category
     *   name
     *   guid
     *
     * All other fields can differ. Specifically, this method does not care about the following:
     *   description and descriptionGUID
     *
     * This type of check is needed during verifyAttributeTypeDef().
     *
     * @param def1 - the first TypeDef in the equivalence check
     * @param def2 - the second TypeDef in the equivalence check
     * @return - whether or not the objects are equivalent
     */
    public boolean equivalent(AttributeTypeDef def1, AttributeTypeDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent AttributeTypeDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent AttributeTypeDef: the {} AttributeTypeDef is null {}", def1==null?"first":"second");
            return false;
        }
        // Compare just the necessary subset of fields of AttributeTypeDef
        if (!compare(def1.getCategory(), def2.getCategory())) {
            LOG.debug("Equivalent AttributeTypeDef: compare category failed");
            return false;
        }
        if (!compare(def1.getGUID(), def2.getGUID())) {
            LOG.debug("Equivalent AttributeTypeDef: compare GUID failed");
            return false;
        }
        if (!compare(def1.getName(), def2.getName())) {
            LOG.debug("Equivalent AttributeTypeDef: compare name failed");
            return false;
        }
        return true;
    }

    public boolean equivalent(PrimitiveDef def1, PrimitiveDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent PrimitiveDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent PrimitiveDef: the {} PrimitiveDef is null {}", def1==null?"first":"second");
            return false;
        }
        /*
         * Delegate up to AttributeTypeDef equivalence check
         */
        if ( !equivalent( (AttributeTypeDef)def1, (AttributeTypeDef)def2) ) {
            LOG.debug("Equivalent PrimitiveDef: equivalent AttributeTypeDef failed");
            return false;
        }
        /*
         * PrimitiveDef adds the following fields...
         */
        if ( !compare(def1.getPrimitiveDefCategory(),def2.getPrimitiveDefCategory()) ) {
            LOG.debug("Equivalent PrimitiveDef: compare getPrimitiveDefCategory failed");
            return false;
        }
        return true;
    }

    public boolean equivalent(CollectionDef def1, CollectionDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent CollectionDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent CollectionDef: the {} CollectionDef is null {}", def1==null?"first":"second");
            return false;
        }
         /*
         * Delegate up to AttributeTypeDef equivalence check
         */
        if ( !equivalent( (AttributeTypeDef)def1, (AttributeTypeDef)def2) ) {
            LOG.debug("Equivalent CollectionDef: equivalent AttributeTypeDef failed");
            return false;
        }
        /*
         * CollectionDef adds the following fields...
         *   collectionDefCategory
         *   argumentCount
         *   argumentTypes
         */
        if ( !compare(def1.getCollectionDefCategory(),def2.getCollectionDefCategory()) ) {
            LOG.debug("Equivalent CollectionDef: compare getCollectionDefCategory failed");
            return false;
        }
        if ( !compare(def1.getArgumentCount(),def2.getArgumentCount()) ) {
            LOG.debug("Equivalent CollectionDef: compare getArgumentCount failed");
            return false;
        }
        if ( !compareListPrimitiveDefCategory(true, def1.getArgumentTypes(),def2.getArgumentTypes()) ) {
            LOG.debug("Equivalent CollectionDef: compare ListPrimitiveDefCategory failed");
            return false;
        }
        return true;
    }

    public boolean equivalent(EnumDef def1, EnumDef def2) {
        if (def1 == null && def2 == null) {
            LOG.debug("Equivalent EnumDef: both are null");
            return true;
        }
        if (def1 == null ^ def2 == null) {
            LOG.debug("Equivalent EnumDef: the {} EnumDef is null {}", def1==null?"first":"second");
            return false;
        }
         /*
         * Delegate up to AttributeTypeDef equivalence check
         */
        if ( !equivalent( (AttributeTypeDef)def1, (AttributeTypeDef)def2) ) {
            LOG.debug("Equivalent EnumDef: equivalent AttributeTypeDef failed");
            return false;
        }
        /*
         * EnumDef adds the following fields...
         *   elementDefs
         *   defaultValue
         */
        // possibly too strict...may want to allow enum elements to appear in any order....
        if ( !compareListEnumElementDef(true, def1.getElementDefs(),def2.getElementDefs()) ) {
            LOG.debug("Equivalent EnumDef: compare ListEnumElementDef failed");
            return false;
        }
        if ( !compare(def1.getDefaultValue(),def2.getDefaultValue()) ) {
            LOG.debug("Equivalent EnumDef: compare getDefaultValue failed");
            return false;
        }
        return true;
    }
}
