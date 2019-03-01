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
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import static org.apache.atlas.model.TypeCategory.PRIMITIVE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.CollectionDefCategory.OM_COLLECTION_ARRAY;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.CollectionDefCategory.OM_COLLECTION_MAP;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AtlasBaseTypeDefMapper {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAttributeMapper.class);


    private AtlasBaseTypeDef abtd;

    public AtlasBaseTypeDefMapper(AtlasBaseTypeDef abtd) {
        this.abtd = abtd;
    }


    // Convert an AtlasBaseTypeDef into an AttributeTypeDef
    //
    // GUID will always be wild for a collection (MAP or ARRAY) - use the one from the repos helper or create a new UUID
    // descriptionGUID is will always be wild for all categories -  use the one from the repos helper or create a new UUID
    // Until it is used properly, descriptionGUID is always set to null.
    //
    // package private
    AttributeTypeDef toAttributeTypeDef() {


        final String methodName = "toAttributeTypeDef";

        LOG.debug("toAttributeTypeDef: AtlasBaseTypeDef is {}", abtd);

        if (abtd == null) {
            return null;
        }

        AttributeTypeDef ret;

        // Find the category
        String atlasTypeName = abtd.getName();
        TypeCategory atlasCategory;
        if (atlasTypeName.equals(ATLAS_TYPE_DATE)) {
            // In Atlas dates are handled as primitives, so we need to adopt a category of PRIMITIVE
            atlasCategory = PRIMITIVE;
        } else {
            atlasCategory = abtd.getCategory();
        }

        LOG.debug("{}: atlasTypeName {} atlasCategory {}", methodName, atlasTypeName, atlasCategory);

        // Based on the Atlas type category., produce the appropriate category of OM ATD
        AttributeTypeDef attributeTypeDef;
        switch (atlasCategory) {

            case PRIMITIVE:

                // Handle atlas primitives and date
                PrimitiveDefCategory primDefCat =  TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(atlasTypeName);
                LOG.debug("{} : handling an Atlas primitive {}", methodName, primDefCat);
                PrimitiveDef primitiveDef = new PrimitiveDef(primDefCat);
                primitiveDef.setGUID(primDefCat.getGUID());
                primitiveDef.setName(atlasTypeName);
                AttributeTypeDefCategory primitiveDefCategory = primitiveDef.getCategory();
                if (primitiveDefCategory != null)
                    primitiveDef.setDescription(primitiveDefCategory.getDescription());
                primitiveDef.setDescriptionGUID(null);
                attributeTypeDef = primitiveDef;
                ret = attributeTypeDef;
                break;

            case ARRAY:

                int startIdx = ATLAS_TYPE_ARRAY_PREFIX.length();
                int endIdx = atlasTypeName.length() - ATLAS_TYPE_ARRAY_SUFFIX.length();
                String elementTypeName = atlasTypeName.substring(startIdx, endIdx);
                LOG.debug("{} : handling an Atlas array in which the elements are of type {}", methodName, elementTypeName);
                // Convert this into a CollectionDef of the appropriate element type...
                CollectionDef collectionDef = new CollectionDef(OM_COLLECTION_ARRAY);
                // Set up the list of argument types...
                ArrayList<PrimitiveDefCategory> argTypes = new ArrayList<>();
                PrimitiveDefCategory pdc = TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(elementTypeName);
                argTypes.add(pdc);
                collectionDef.setArgumentTypes(argTypes);
                // The collection name can be generated from the template name in the collection def cat with parameter substitution.
                String nameTemplate = collectionDef.getCollectionDefCategory().getName();
                // I'm not sure how this was intended to be used so this might be rather odd but here goes...
                LOG.debug("{}: Generate Array Attributes: name template is {}", methodName, nameTemplate);
                String[] parts = nameTemplate.split("[{}]");
                String omrsArrayTypeName = parts[0] + elementTypeName + parts[2];
                LOG.debug("{}: name resolved to {}", methodName, omrsArrayTypeName);
                collectionDef.setName(omrsArrayTypeName);

                // Set inherited ATD fields
                collectionDef.setGUID(null);
                collectionDef.setName(atlasTypeName);
                collectionDef.setDescription(collectionDef.getCategory().getDescription());
                collectionDef.setDescriptionGUID(null);
                attributeTypeDef = collectionDef;
                ret = attributeTypeDef;
                break;

            case MAP:

                int mapStartIdx = ATLAS_TYPE_MAP_PREFIX.length();
                int mapEndIdx = atlasTypeName.length() - ATLAS_TYPE_MAP_SUFFIX.length();
                String[] keyValueTypes = atlasTypeName.substring(mapStartIdx, mapEndIdx).split(ATLAS_TYPE_MAP_KEY_VAL_SEP, 2);
                String keyTypeName = keyValueTypes.length > 0 ? keyValueTypes[0] : null;
                String valueTypeName = keyValueTypes.length > 1 ? keyValueTypes[1] : null;
                if (keyTypeName == null || valueTypeName == null) {
                    LOG.error("{}: map type contained key type name {}, value type name {} - cannot process nulls, attribute skipped", methodName, keyTypeName, valueTypeName );
                    return null;
                }
                LOG.debug("{} : handling an Atlas map in which the elements are of type {} {}", methodName, keyTypeName, valueTypeName);
                // Convert this into a CollectionDef of the appropriate element type...
                CollectionDef omrsCollectionDef = new CollectionDef(OM_COLLECTION_MAP);
                // Set up the list of argument types...
                ArrayList<PrimitiveDefCategory> mapArgTypes = new ArrayList<>();
                PrimitiveDefCategory pdck =  TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(keyTypeName);
                PrimitiveDefCategory pdcv =  TypeNameUtils.convertAtlasTypeNameToPrimitiveDefCategory(valueTypeName);
                mapArgTypes.add(pdck);
                mapArgTypes.add(pdcv);
                omrsCollectionDef.setArgumentTypes(mapArgTypes);
                // The collection name can be generated from the name in the collection def cat with suitable parameter substitution...
                String mapNameTemplate = omrsCollectionDef.getCollectionDefCategory().getName();
                // I'm not sure how this was intended to be used so this might be rather odd but here goes...
                LOG.debug("{}: Generate Map Attributes: name template is {}", methodName, mapNameTemplate);
                String[] mapNameParts = mapNameTemplate.split("[{}]");
                String omrsMapTypeName = mapNameParts[0] + keyTypeName + mapNameParts[2] + valueTypeName + mapNameParts[4];
                LOG.debug("{}: name resolved to {}", methodName, omrsMapTypeName);
                omrsCollectionDef.setName(omrsMapTypeName);

                // Set inherited ATD fields
                omrsCollectionDef.setGUID(null);
                omrsCollectionDef.setName(atlasTypeName);
                AttributeTypeDefCategory collectionDefCategory = omrsCollectionDef.getCategory();
                if (collectionDefCategory != null) {
                    omrsCollectionDef.setDescription(collectionDefCategory.getDescription());
                }
                omrsCollectionDef.setDescriptionGUID(null);
                attributeTypeDef = omrsCollectionDef;
                ret = attributeTypeDef;
                break;

            case ENUM:

                LOG.debug("{} : handling an Atlas enum {}", methodName);
                EnumDef enumDef = new EnumDef();
                // Set common fields
                enumDef.setGUID(abtd.getGuid());
                enumDef.setName(abtd.getName());
                Long atlasVersion = abtd.getVersion();
                enumDef.setVersion(atlasVersion);
                enumDef.setVersionName(Long.toString(atlasVersion));
                enumDef.setDescription(abtd.getDescription());
                enumDef.setDescriptionGUID(null);

                // Additional fields on an AtlasEnumDef and OM EnumDef are elementDefs and defaultValue
                // Initialize the Atlas and OMRS default values so we can test and set default value in the loop
                AtlasEnumDef atlasEnumDef = (AtlasEnumDef) abtd;
                String atlasDefaultValue = atlasEnumDef.getDefaultValue();
                EnumElementDef omrsDefaultValue = null;
                ArrayList<EnumElementDef> omrsElementDefs;
                List<AtlasEnumDef.AtlasEnumElementDef> atlasElemDefs = atlasEnumDef.getElementDefs();
                if (atlasElemDefs != null) {
                    omrsElementDefs = new ArrayList<>();
                    for (AtlasEnumDef.AtlasEnumElementDef atlasElementDef : atlasElemDefs) {
                        EnumElementDef omrsEnumElementDef = new EnumElementDef();
                        omrsEnumElementDef.setValue(atlasElementDef.getValue());
                        omrsEnumElementDef.setDescription(atlasElementDef.getDescription());
                        omrsEnumElementDef.setOrdinal(atlasElementDef.getOrdinal());
                        omrsEnumElementDef.setDescriptionGUID(null);
                        omrsElementDefs.add(omrsEnumElementDef);
                        if (atlasElementDef.getValue().equals(atlasDefaultValue)) {
                            omrsDefaultValue = omrsEnumElementDef;
                        }
                    }
                    enumDef.setElementDefs(omrsElementDefs);
                    enumDef.setDefaultValue(omrsDefaultValue);
                }

                attributeTypeDef = enumDef;
                ret = attributeTypeDef;

                break;

            default:
                LOG.debug("{}: cannot convert Atlas type with category {} to an AttributeTypeDef", methodName, atlasCategory);
                return null;

        }

        return ret;
    }

}
