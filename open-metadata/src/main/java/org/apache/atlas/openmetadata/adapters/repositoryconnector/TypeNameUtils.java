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

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;



public class TypeNameUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TypeNameUtils.class);

    // Utility method to convert frm an Atlas typename to an OM primitive def category.
    // Most of the Atlas types are primitives in Atlas but DAT is not - that doesn't matter
    // as DATE is a primitive in OM so it is just a question of mapping the Atlas DATE
    // type to the corresponding OM PrimitiveDefCategory.

    // package-private
    static PrimitiveDefCategory convertAtlasTypeNameToPrimitiveDefCategory(String name) {
        final String methodName = "convertAtlasTypeNameToPrimitiveDefCategory";
        switch (name) {
            // No breaks because every case returns
            case ATLAS_TYPE_BOOLEAN:
                return OM_PRIMITIVE_TYPE_BOOLEAN;
            case ATLAS_TYPE_BYTE:
                return OM_PRIMITIVE_TYPE_BYTE;
            case ATLAS_TYPE_SHORT:
                return OM_PRIMITIVE_TYPE_SHORT;
            case ATLAS_TYPE_INT:
                return OM_PRIMITIVE_TYPE_INT;
            case ATLAS_TYPE_LONG:
                return OM_PRIMITIVE_TYPE_LONG;
            case ATLAS_TYPE_FLOAT:
                return OM_PRIMITIVE_TYPE_FLOAT;
            case ATLAS_TYPE_DOUBLE:
                return OM_PRIMITIVE_TYPE_DOUBLE;
            case ATLAS_TYPE_BIGINTEGER:
                return OM_PRIMITIVE_TYPE_BIGINTEGER;
            case ATLAS_TYPE_BIGDECIMAL:
                return OM_PRIMITIVE_TYPE_BIGDECIMAL;
            case ATLAS_TYPE_STRING:
                return OM_PRIMITIVE_TYPE_STRING;
            case ATLAS_TYPE_DATE:
                // Note - technically not a primitive in Atlas, but it is in OM
                return OM_PRIMITIVE_TYPE_DATE;
            default:
                LOG.debug("{}: Cannot map Atlas primitive type {}", methodName, name);
                return OM_PRIMITIVE_TYPE_UNKNOWN;
        }
    }

}
