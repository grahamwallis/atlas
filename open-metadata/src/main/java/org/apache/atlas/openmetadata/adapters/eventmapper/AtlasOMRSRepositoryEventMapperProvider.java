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
package org.apache.atlas.openmetadata.adapters.eventmapper;

import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnectorProviderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In the Egeria Open Connector Framework (OCF), a ConnectorProvider is a factory for a specific type of connector.
 * The AtlasOMRSRepositoryEventMapperProvider is the connector provider for the AtlasOMRSRepositoryEventMapper.
 * It extends OMRSRepositoryEventMapperProviderBase which in turn extends the OCF ConnectorProviderBase.
 * ConnectorProviderBase supports the creation of connector instances.
 *
 * The AtlasOMRSRepositoryEventMapperProvider must initialize ConnectorProviderBase with the Java class
 * name of the OMRS Connector implementation (by calling super.setConnectorClassName(className)).
 * Then the connector provider will work.
 */
public class AtlasOMRSRepositoryEventMapperProvider extends OMRSRepositoryConnectorProviderBase
{
    private static final Logger LOG = LoggerFactory.getLogger(AtlasOMRSRepositoryEventMapperProvider.class);

    static final String  CONNECTOR_TYPE_GUID = "121ea5d1-2f9c-4580-9f40-442ea503b2ec";
    static final String  CONNECTOR_TYPE_NAME = "OMRS Atlas Event Mapper Connector";
    static final String  CONNECTOR_TYPE_DESCRIPTION = "OMRS Atlas Event Mapper Connector that processes events from an Atlas repository.";
    /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * OMRS Connector implementation.
     */
    public AtlasOMRSRepositoryEventMapperProvider()
    {
        LOG.debug("AtlasOMRSRepositoryEventMapperProvider invoked");

        Class    connectorClass = AtlasOMRSRepositoryEventMapper.class;

        super.setConnectorClassName(connectorClass.getName());

        ConnectorType connectorType = new ConnectorType();
        connectorType.setType(ConnectorType.getConnectorTypeType());
        connectorType.setGUID(CONNECTOR_TYPE_GUID);
        connectorType.setQualifiedName(CONNECTOR_TYPE_NAME);
        connectorType.setDisplayName(CONNECTOR_TYPE_NAME);
        connectorType.setDescription(CONNECTOR_TYPE_DESCRIPTION);
        connectorType.setConnectorProviderClassName(this.getClass().getName());

        super.connectorTypeBean = connectorType;
    }
}