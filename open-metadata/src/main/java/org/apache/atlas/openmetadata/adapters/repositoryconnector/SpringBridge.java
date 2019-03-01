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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/*
 * This class provides a means of bridging between the Spring framework and
 * the injection of the Atlas stores needed by the AtlasConnector.
 */

/**
 * Register this SpringBridgeEventMapper as a Spring Component.
 */
@Component
public class SpringBridge implements ISpringBridge, ApplicationContextAware {

    private static ApplicationContext applicationContext;

    @Autowired
    private AtlasTypeDefStore typeDefStore;
    @Autowired
    private AtlasTypeRegistry typeRegistry;
    @Autowired
    private AtlasEntityStore entityStore;
    @Autowired
    private AtlasRelationshipStore relationshipStore;
    @Autowired
    private EntityDiscoveryService entityDiscoveryService;
    @Autowired
    private AtlasGraphProvider atlasGraphProvider;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * A static method to lookup the ISpringBridgeEventMapper Bean in
     * the applicationContext. It is an instance of itself, which
     * was registered by the @Component annotation.
     *
     * @return the ISpringBridgeEventMapper, which exposes all the
     * Spring services that are bridged from the Spring context.
     */
    public static ISpringBridge services() {
        return applicationContext.getBean(ISpringBridge.class);
    }

    @Override
    public AtlasTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }
    @Override
    public AtlasTypeDefStore getTypeDefStore() {
        return typeDefStore;
    }
    @Override
    public AtlasEntityStore getEntityStore() {
        return entityStore;
    }
    @Override
    public AtlasRelationshipStore getRelationshipStore() {
        return relationshipStore;
    }
    @Override
    public EntityDiscoveryService getEntityDiscoveryService() {
        return entityDiscoveryService;
    }
    @Override
    public AtlasGraphProvider getAtlasGraphProvider() {
        return atlasGraphProvider;
    }

}