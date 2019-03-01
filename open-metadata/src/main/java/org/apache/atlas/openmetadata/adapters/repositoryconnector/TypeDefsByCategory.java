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

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class to marshall typedefs as they are discovered - which can then be used for
 * existence checks and can be projected as a list or gallery of lists in specified order
 */

public class TypeDefsByCategory {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDefsByCategory.class);

    private ArrayList<AttributeTypeDef> primitiveDefs;
    private ArrayList<AttributeTypeDef> collectionDefs;
    private ArrayList<AttributeTypeDef> enumDefs;
    private ArrayList<TypeDef>          entityDefs;
    private ArrayList<TypeDef>          relationshipDefs;
    private ArrayList<TypeDef>          classificationDefs;

    public TypeDefsByCategory() {
        primitiveDefs      = new ArrayList<>();
        collectionDefs     = new ArrayList<>();
        enumDefs           = new ArrayList<>();
        entityDefs         = new ArrayList<>();
        relationshipDefs   = new ArrayList<>();
        classificationDefs = new ArrayList<>();
    }

    public TypeDefGallery convertTypeDefsToGallery() {
        // Produce the output gallery by converting from this TypeDefsByCategory object
        TypeDefGallery typeDefGallery = new TypeDefGallery();
        ArrayList<AttributeTypeDef> attrTypeDefs = new ArrayList<>();
        ArrayList<TypeDef> typeDefs = new ArrayList<>();
        if (this.getPrimitiveDefs() != null)
            attrTypeDefs.addAll(this.getPrimitiveDefs());
        if (this.getCollectionDefs() != null)
            attrTypeDefs.addAll(this.getCollectionDefs());
        if (this.getEnumDefs() != null)
            attrTypeDefs.addAll(this.getEnumDefs());
        if (this.getEntityDefs() != null)
            typeDefs.addAll(this.getEntityDefs());
        if (this.getRelationshipDefs() != null)
            typeDefs.addAll(this.getRelationshipDefs());
        if (this.getClassificationDefs() != null)
            typeDefs.addAll(this.getClassificationDefs());
        typeDefGallery.setAttributeTypeDefs(attrTypeDefs);
        typeDefGallery.setTypeDefs(typeDefs);
        LOG.debug("TypeDefsByCategory: typeDefGallery {}", typeDefGallery);
        return typeDefGallery;
    }

    public List<AttributeTypeDef> getPrimitiveDefs() {
        return primitiveDefs;
    }

    public void setPrimitiveDefs(ArrayList<AttributeTypeDef> listPrimitiveDefs) {
        primitiveDefs = listPrimitiveDefs;
    }

    public PrimitiveDef getPrimitiveDef(String name) {
        if (primitiveDefs != null) {
            for (AttributeTypeDef e : primitiveDefs) {
                if (e.getName().equals(name))
                    return (PrimitiveDef) e;
            }
        }
        return null;
    }

    public void addPrimitiveDef(AttributeTypeDef def) {
        if (getPrimitiveDef(def.getName()) == null) {
            primitiveDefs.add(def);
        }
    }

    public List<AttributeTypeDef> getCollectionDefs() {
        return collectionDefs;
    }

    public void setCollectionDefs(ArrayList<AttributeTypeDef> listCollectionDefs) {
        collectionDefs = listCollectionDefs;
    }

    public CollectionDef getCollectionDef(String name) {
        if (collectionDefs != null) {
            for (AttributeTypeDef e : collectionDefs) {
                if (e.getName().equals(name))
                    return (CollectionDef) e;
            }
        }
        return null;
    }

    public void addCollectionDef(AttributeTypeDef def) {
        if (getCollectionDef(def.getName()) == null) {
            collectionDefs.add(def);
        }
    }

    public List<AttributeTypeDef> getEnumDefs() {
        return enumDefs;
    }

    public void setEnumDefs(ArrayList<AttributeTypeDef> listEnumDefs) {
        enumDefs = listEnumDefs;
    }

    public EnumDef getEnumDef(String name) {
        if (enumDefs != null) {
            for (AttributeTypeDef e : enumDefs) {
                if (e.getName().equals(name))
                    return (EnumDef) e;
            }
        }
        return null;
    }

    public void addEnumDef(AttributeTypeDef def) {
        if (getEnumDef(def.getName()) == null) {
            enumDefs.add(def);
        }
    }

    public List<TypeDef> getEntityDefs() {
        return entityDefs;
    }

    public void setEntityDefs(ArrayList<TypeDef> listEntityDefs) {
        entityDefs = listEntityDefs;
    }

    public EntityDef getEntityDef(String name) {
        if (entityDefs != null) {
            for (TypeDef e : entityDefs) {
                if (e.getName().equals(name))
                    return (EntityDef) e;
            }
        }
        return null;
    }

    public void addEntityDef(TypeDef def) {
        if (getEntityDef(def.getName()) == null) {
            entityDefs.add(def);
        }
    }

    public List<TypeDef> getRelationshipDefs() {
        return relationshipDefs;
    }

    public void setRelationshipDefs(ArrayList<TypeDef> listRelationshipDefs) {
        relationshipDefs = listRelationshipDefs;
    }

    public RelationshipDef getRelationshipDef(String name) {
        if (relationshipDefs != null) {
            for (TypeDef e : relationshipDefs) {
                if (e.getName().equals(name))
                    return (RelationshipDef) e;
            }
        }
        return null;
    }

    public void addRelationshipDef(TypeDef def) {
        if (getRelationshipDef(def.getName()) == null) {
            relationshipDefs.add(def);
        }
    }

    public List<TypeDef> getClassificationDefs() {
        return classificationDefs;
    }

    public void setClassificationDefs(ArrayList<TypeDef> listClassificationDefs) {
        classificationDefs = listClassificationDefs;
    }

    public ClassificationDef getClassificationDef(String name) {
        if (classificationDefs != null) {
            for (TypeDef e : classificationDefs) {
                if (e.getName().equals(name))
                    return (ClassificationDef) e;
            }
        }
        return null;
    }

    public void addClassificationDef(TypeDef def) {
        if (getClassificationDef(def.getName()) == null) {
            classificationDefs.add(def);
        }
    }

    @Override
    public String toString() {
        return "TypeDefsByCategory{" +
                "primitiveDefs=" + primitiveDefs +
                ", collectionDefs=" + collectionDefs +
                ", enumDefs=" + enumDefs +
                ", entityDefs=" + entityDefs +
                ", relationshipDefs=" + relationshipDefs +
                ", classificationDefs=" + classificationDefs +
                '}';
    }
}