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


import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
@Component
public class AtlasStoresProxyImpl implements AtlasStoresProxy {


    private AtlasTypeRegistry      typeRegistry;
    private AtlasTypeDefStore      typeDefStore;
    private AtlasEntityStore       entityStore;
    private AtlasRelationshipStore relationshipStore;
    private EntityDiscoveryService entityDiscoveryService;
    private AtlasGraphProvider     atlasGraphProvider;

    @Inject
    public AtlasStoresProxyImpl(AtlasTypeRegistry      typeRegistry,
                                AtlasTypeDefStore      typeDefStore,
                                AtlasEntityStore       entityStore,
                                AtlasRelationshipStore relationshipStore,
                                EntityDiscoveryService entityDiscoveryService,
                                AtlasGraphProvider     atlasGraphProvider)
    {

        this.typeRegistry           = typeRegistry;
        this.typeDefStore           = typeDefStore;
        this.entityStore            = entityStore;
        this.relationshipStore      = relationshipStore;
        this.entityDiscoveryService = entityDiscoveryService;
        this.atlasGraphProvider     = atlasGraphProvider;
    }

    public AtlasTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public AtlasTypeDefStore getTypeDefStore() {
        return typeDefStore;
    }

    public AtlasEntityStore getEntityStore() {
        return entityStore;
    }

    public AtlasRelationshipStore getRelationshipStore() {
        return relationshipStore;
    }

    public EntityDiscoveryService getEntityDiscoveryService() {
        return entityDiscoveryService;
    }

    public AtlasGraphProvider     getAtlasGraphProvider() { return atlasGraphProvider; }
}