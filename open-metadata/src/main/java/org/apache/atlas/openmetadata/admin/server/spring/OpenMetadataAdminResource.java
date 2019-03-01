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
package org.apache.atlas.openmetadata.admin.server.spring;

import org.apache.atlas.openmetadata.adapters.eventmapper.AtlasOMRSRepositoryEventMapperProvider;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSRepositoryConnectorProvider;

import org.odpi.openmetadata.adminservices.OMAGServerAdminServices;
import org.odpi.openmetadata.adminservices.OMAGServerOperationalServices;
import org.odpi.openmetadata.adminservices.configuration.properties.*;
import org.odpi.openmetadata.adminservices.rest.OMAGServerConfigResponse;
import org.odpi.openmetadata.adminservices.rest.VoidResponse;
import org.odpi.openmetadata.frameworks.connectors.properties.beans.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;


import java.util.Map;
import java.util.UUID;

/**
 * OpenMetadataAdminResource provides the spring annotations for the administrative
 * interface for configuring the Open Metadata connector for Atlas.
 * It provides the configuration properties and delegates administration requests
 * to the Open Metadata Repository Services (OMRS).
 * <p>
 * There are four types of operations defined by the interface:
 * </p>
 * <ul>
 * <li>
 * Basic configuration - these methods use the minimum of configuration information to run the
 * server using default properties.
 * </li>
 * <li>
 * Advanced Configuration - provides access to all configuration properties to provide
 * fine-grained control of the server.
 * </li>
 * <li>
 * Initialization and shutdown - these methods control the initialization and shutdown of the
 * open metadata and governance service instance based on the supplied configuration.
 * </li>
 * </ul>
 */
@RestController
@RequestMapping("/open-metadata/admin-services/users/{userId}/servers/{serverName}")
public class OpenMetadataAdminResource
{

    private static final Logger LOG = LoggerFactory.getLogger(OpenMetadataAdminResource.class);

    private OMAGServerAdminServices adminAPI = new OMAGServerAdminServices();
    private OMAGServerOperationalServices operationsAPI = new OMAGServerOperationalServices();


    /*
     * =============================================================
     * Help the client discover the type of the server
     */


    /**
     * Return the origin of this server implementation.
     *
     * @return Open Metadata Server Origin
     */
    @RequestMapping(method = RequestMethod.GET, path = "/server-origin")
    public String getServerOrigin()
    {
        return "Atlas Server";
    }


    /*
     * =============================================================
     * Configure server - basic options using defaults
     */

    /**
     * Set up the root URL for this server that is used to construct full URL paths to calls for
     * this server's REST interfaces.  The default value is "localhost:8080".
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param url  String url.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or serverURLRoot parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/server-url-root")
    public VoidResponse setServerURLRoot(@PathVariable String userId,
                                         @PathVariable String serverName,
                                         @RequestParam String url)
    {
        return adminAPI.setServerURLRoot(userId, serverName, url);
    }


    /**
     * Set up the descriptive type of the server.  This value is added to distributed events to
     * make it easier to understand the source of events.  The default value is "Open Metadata and Governance Server".
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param typeName  short description for the type of server.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or serverType parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/server-type")
    public VoidResponse setServerType(@PathVariable String userId,
                                      @PathVariable String serverName,
                                      @RequestParam String typeName)
    {
        return adminAPI.setServerType(userId, serverName, typeName);
    }


    /**
     * Set up the name of the organization that is running this server.  This value is added to distributed events to
     * make it easier to understand the source of events.  The default value is null.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param name  String name of the organization.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or organizationName parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/organization-name")
    public VoidResponse setOrganizationName(@PathVariable String userId,
                                            @PathVariable String serverName,
                                            @RequestParam String name)
    {
        return adminAPI.setOrganizationName(userId, serverName, name);
    }


    /**
     * Set up the user id to use when there is no external user driving the work (for example when processing events
     * from another server).
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param id - String user id for the server.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or serverURLRoot parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/server-user-id")
    public VoidResponse setServerUserId(@PathVariable String userId,
                                        @PathVariable String serverName,
                                        @RequestParam String id)
    {
        return adminAPI.setServerUserId(userId, serverName, id);
    }


    /**
     * Set an upper limit on the page size that can be requested on a REST call to the server.  The default
     * value is 1000.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param limit  max number of elements that can be returned on a request.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or maxPageSize parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/max-page-size")
    public VoidResponse setMaxPageSize(@PathVariable String  userId,
                                       @PathVariable String  serverName,
                                       @RequestParam int     limit)
    {
        return adminAPI.setMaxPageSize(userId, serverName, limit);
    }


    /**
     * Set up the default event bus for embedding in event-driven connector.   The resulting connector will
     * be used in the OMRS Topic Connector for each cohort, and the in and out topics for the local repository's
     * event mapper.
     *
     * @param userId  user that is issuing the request.
     * @param serverName local server name.
     * @param connectorProvider  connector provider for the event bus (if it is null then Kafka is assumed).
     * @param topicURLRoot the common root of the topics used by the open metadata server.
     * @param additionalProperties  property name/value pairs used to configure the connection to the event bus connector
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGConfigurationErrorException it is too late to configure the event bus - other configuration already exists or
     * OMAGInvalidParameterException invalid serverName or serviceMode parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/event-bus")
    public VoidResponse setEventBus(@PathVariable                   String              userId,
                                    @PathVariable                   String              serverName,
                                    @RequestParam(required = false) String              connectorProvider,
                                    @RequestParam(required = false) String              topicURLRoot,
                                    @RequestBody (required = false) Map<String, Object> additionalProperties)
    {
        return adminAPI.setEventBus(userId, serverName, connectorProvider, topicURLRoot, additionalProperties);
    }

    /**
     * Provide the connection to the local repository - used when the local repository is Atlas.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or repositoryProxyConnection parameter or
     * OMAGConfigurationErrorException the local repository mode has not been set
     */
    @RequestMapping(method = RequestMethod.POST, path = "/atlas-repository")
    public VoidResponse setRepositoryProxyConnection(@PathVariable                    String     userId,
                                                     @PathVariable                    String     serverName)

    {
        // No request body because this method will create an Atlas Connection object
        LOG.debug("setRepositoryProxyConnection: userId={} serverName={}", userId, serverName);
        Connection atlasConnection = getLocalAtlasRepositoryLocalConnection(userId, serverName);
        LOG.debug("setRepositoryProxyConnection: repository connector connection={} ", atlasConnection);
        adminAPI.setRepositoryProxyConnection(userId, serverName, atlasConnection);
        LOG.debug("setRepositoryProxyConnection: create connection for Atlas event mapper", userId, serverName);
        Connection atlasEventMapperConnection = getAtlasEventMapperConnection(serverName);
        LOG.debug("setRepositoryProxyConnection: event mapper connection={} ", atlasEventMapperConnection);
        return adminAPI.setLocalRepositoryEventMapper(userId, serverName, atlasEventMapperConnection);
    }


    /**
     * Provide the connection to the local repository - used when the local repository is Atlas.
     *
     * @param userId   user that is issuing the request.
     * @param serverName   local server name.
     * @param additionalProperties      additional parameters to pass to the repository connector
     * @return void response or
     * OMAGNotAuthorizedException     the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or repositoryProxyConnection parameter or
     * OMAGConfigurationErrorException the local repository mode has not been set.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/atlas-repository/details")
    public VoidResponse setRepositoryProxyConnection(@PathVariable                   String               userId,
                                                     @PathVariable                   String               serverName,
                                                     @RequestBody(required = false)  Map<String, Object>  additionalProperties)
    {
        LOG.debug("setRepositoryProxyConnection: userId={} serverName={}", userId, serverName);
        String connectorProvider = "LocalAtlasOMRSRepositoryConnectorProvider";
        LOG.debug("setRepositoryProxyConnection: additionalProperties={} ", additionalProperties);
        return adminAPI.setRepositoryProxyConnection(userId, serverName, connectorProvider, additionalProperties);
    }


    /**
     * Provide the connection to the local repository's event mapper if needed.  The default value is null which
     * means no event mapper.  An event mapper is needed if the local repository has additional APIs that can change
     * the metadata in the repository without going through the open metadata and governance services.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param connection  connection to the OMRS repository event mapper.
     * @return void response
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or localRepositoryEventMapper parameter or
     * OMAGConfigurationErrorException the local repository mode, or the event mapper has not been set
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/event-mapper-connection")
    public VoidResponse setLocalRepositoryEventMapper(@PathVariable String     userId,
                                                      @PathVariable String     serverName,
                                                      @RequestBody  Connection connection)
    {
        return adminAPI.setLocalRepositoryEventMapper(userId, serverName, connection);
    }


    /**
     * Provide the connection to the local repository's event mapper if needed.  The default value is null which
     * means no event mapper.  An event mapper is needed if the local repository has additional APIs that can change
     * the metadata in the repository without going through the open metadata and governance services.
     *
     * @param userId                      user that is issuing the request.
     * @param serverName                  local server name.
     * @param connectorProvider           Java class name of the connector provider for the OMRS repository event mapper.
     * @param eventSource                 topic name or URL to the native event source.
     * @param additionalProperties        additional properties for the event mapper connection
     * @return void response or
     * OMAGNotAuthorizedException    the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or localRepositoryEventMapper parameter or
     * OMAGConfigurationErrorException the local repository mode has not been set.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/event-mapper-details")
    public VoidResponse setLocalRepositoryEventMapper(@PathVariable                 String               userId,
                                                      @PathVariable                 String               serverName,
                                                      @RequestParam                 String               connectorProvider,
                                                      @RequestParam                 String               eventSource,
                                                      @RequestBody(required=false)  Map<String, Object>  additionalProperties)
    {
        return adminAPI.setLocalRepositoryEventMapper(userId, serverName, connectorProvider, eventSource, additionalProperties);
    }


    /**
     * Enable registration of server to an open metadata repository cohort.  This is a group of open metadata
     * repositories that are sharing metadata.  An Atlas server can connect to zero, one or more cohorts.
     * Each cohort needs a unique name.  The members of the cohort use a shared topic to exchange registration
     * information and events related to changes in their supported metadata types and instances.
     * They are also able to query each other's metadata directly through REST calls.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param cohortName  name of the cohort.
     * @param additionalProperties additional properties for the event bus connection
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName, cohortName or serviceMode parameter or
     * OMAGConfigurationErrorException the event bus is not set.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/cohorts/{cohortName}")
    public VoidResponse enableCohortRegistration(@PathVariable                   String               userId,
                                                 @PathVariable                   String               serverName,
                                                 @PathVariable                   String               cohortName,
                                                 @RequestBody(required = false)  Map<String, Object>  additionalProperties)
    {
        return adminAPI.enableCohortRegistration(userId, serverName, cohortName, additionalProperties);
    }


    /**
     * Unregister this server from an open metadata repository cohort.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param cohortName  name of the cohort.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName, cohortName or serviceMode parameter.
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/cohorts/{cohortName}")
    public VoidResponse disableCohortRegistration(@PathVariable String          userId,
                                                  @PathVariable String          serverName,
                                                  @PathVariable String          cohortName)
    {
        return adminAPI.disableCohortRegistration(userId, serverName, cohortName);
    }


    /*
     * =============================================================
     * Configure server - advanced options overriding defaults
     */

    /**
     * Set up the configuration for the local repository.  This overrides the current values.
     *
     * @param userId  user that is issuing the request.
     * @param serverName  local server name.
     * @param localRepositoryConfig  configuration properties for the local repository.
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName or localRepositoryConfig parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/configuration")
    public VoidResponse setLocalRepositoryConfig(@PathVariable String                userId,
                                                 @PathVariable String                serverName,
                                                 @RequestBody  LocalRepositoryConfig localRepositoryConfig)
    {
        return adminAPI.setLocalRepositoryConfig(userId, serverName, localRepositoryConfig);
    }



    /**
     * Set up the configuration properties for a cohort.  This may reconfigure an existing cohort or create a
     * cohort.  Use setCohortMode to delete a cohort.
     *
     * @param userId  user that is issuing the request
     * @param serverName  local server name
     * @param cohortName  name of the cohort
     * @param cohortConfig  configuration for the cohort
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName, cohortName or cohortConfig parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/cohorts/{cohortName}/configuration")
    public VoidResponse setCohortConfig(@PathVariable String       userId,
                                        @PathVariable String       serverName,
                                        @PathVariable String       cohortName,
                                        @RequestBody  CohortConfig cohortConfig)
    {
        return adminAPI.setCohortConfig(userId, serverName, cohortName, cohortConfig);
    }


    /*
     * =============================================================
     * Query current configuration
     */


    /**
     * Return the stored configuration document for the server.
     *
     * @param userId  user that is issuing the request
     * @param serverName  local server name
     * @return OMAGServerConfig properties or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException invalid serverName parameter.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/configuration")
    public OMAGServerConfigResponse getCurrentConfiguration(@PathVariable String userId,
                                                            @PathVariable String serverName)
    {
        return adminAPI.getStoredConfiguration(userId, serverName);
    }


    /*
     * ========================================================================================
     * Activate and deactivate the open metadata and governance capabilities in the Atlas Server
     */

    /**
     * Activate the open metadata and governance services using the stored configuration information.
     *
     * @param userId  user that is issuing the request
     * @param serverName  local server name
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException the server name is invalid or
     * OMAGConfigurationErrorException there is a problem using the supplied configuration.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/instance")
    public VoidResponse activateWithStoredConfig(@PathVariable String userId,
                                                 @PathVariable String serverName)
    {
        return operationsAPI.activateWithStoredConfig(userId, serverName);
    }


    /**
     * Activate the open metadata and governance services using the supplied configuration
     * document.
     *
     * @param userId  user that is issuing the request
     * @param configuration  properties used to initialize the services
     * @param serverName  local server name
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException the server name is invalid or
     * OMAGConfigurationErrorException there is a problem using the supplied configuration.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/instance/configuration")
    public VoidResponse activateWithSuppliedConfig(@PathVariable String           userId,
                                                   @PathVariable String           serverName,
                                                   @RequestParam OMAGServerConfig configuration)
    {
        return operationsAPI.activateWithSuppliedConfig(userId, serverName, configuration);
    }


    /**
     * Temporarily deactivate any open metadata and governance services.
     *
     * @param userId  user that is issuing the request
     * @param serverName  local server name
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException the serverName is invalid.
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/instance")
    public VoidResponse deactivateTemporarily(@PathVariable String  userId,
                                              @PathVariable String  serverName)
    {
        return operationsAPI.deactivateTemporarily(userId, serverName);
    }


    /**
     * Permanently deactivate any open metadata and governance services and unregister from
     * any cohorts.
     *
     * @param userId  user that is issuing the request
     * @param serverName  local server name
     * @return void response or
     * OMAGNotAuthorizedException the supplied userId is not authorized to issue this command or
     * OMAGInvalidParameterException the serverName is invalid.
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "")
    public VoidResponse deactivatePermanently(@PathVariable String  userId,
                                              @PathVariable String  serverName)
    {
        return operationsAPI.deactivatePermanently(userId, serverName);
    }


    /*
     * Return an Atlas graph repository connection using the LocalAtlasOMRSRepositoryConnector.
     *
     * @param localServerName   name of the local server
     * @return Connection object
     */
    private Connection getLocalAtlasRepositoryLocalConnection(String userId, String localServerName)
    {
        final String endpointGUID                         = "d4e53a33-82d2-41b2-a25f-821342ee9855";
        final String connectorTypeGUID                    = "9d086f0b-81e3-46ec-a4a9-520038e99c12";
        final String connectionGUID                       = "2119a4cd-2666-46dd-9487-c8b6122de59a";

        final String endpointDescription                  = "Atlas OMRS repository REST API endpoint.";
        final String connectorTypeDescription             = "Local Atlas OMRS repository connector type.";
        final String connectorProviderClassName           = LocalAtlasOMRSRepositoryConnectorProvider.class.getName();

        String endpointName                               = "AtlasOMRSRepositoryRESTAPI.Endpoint." + localServerName;
        String address                                    = "Server URL not found in OMAGServerConfig";
        OMAGServerConfigResponse omagServerConfigResponse = adminAPI.getStoredConfiguration(userId, localServerName);

        if (omagServerConfigResponse != null) {
            OMAGServerConfig omagServerConfig = omagServerConfigResponse.getOMAGServerConfig();
            if (omagServerConfig != null) {
                address = omagServerConfig.getLocalServerURL();
            }
        }

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(address);


        String connectorTypeName = "LocalAtlasOMRSRepository.ConnectorType." + localServerName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorProviderClassName);


        final String connectionDescription = "Local Atlas OMRS repository connection.";

        String connectionName = "LocalAtlasOMRSRepository.Connection." + localServerName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setConnectorType(connectorType);
        connection.setEndpoint(endpoint);

        return connection;
    }


    /*
     * Return an Atlas repository event mapper connection.
     *
     * @param localServerName   name of the local server
     * @return Connection object
     */
    private Connection getAtlasEventMapperConnection(String serverName) {

        final String endpointGUID             = UUID.randomUUID().toString();
        final String connectorTypeGUID        = UUID.randomUUID().toString();
        final String connectionGUID           = UUID.randomUUID().toString();
        final String endpointDescription      = "Atlas event mapper endpoint.";
        final String connectorTypeDescription = "Atlas event mapper connector type.";
        final String connectionDescription    = "Atlas event mapper connection.";
        final String eventSource              = "Atlas repository notifications";

        final String connectorProviderClassName = AtlasOMRSRepositoryEventMapperProvider.class.getName();

        String endpointName    = "AtlasEventMapper.Endpoint." + serverName;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(eventSource);

        String connectorTypeName = "AtlasEventMapper.ConnectorType." + serverName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorProviderClassName);

        String connectionName = "AtlasEventMapper.Connection." + serverName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    private ElementType getConnectorTypeType()
    {
        ElementType elementType = ConnectorType.getConnectorTypeType();

        elementType.setElementOrigin(ElementOrigin.CONFIGURATION);

        return elementType;
    }

    private ElementType getConnectionType()
    {
        ElementType elementType = Connection.getConnectionType();

        elementType.setElementOrigin(ElementOrigin.CONFIGURATION);

        return elementType;
    }

    private ElementType getEndpointType()
    {
        ElementType elementType = Endpoint.getEndpointType();

        elementType.setElementOrigin(ElementOrigin.CONFIGURATION);

        return elementType;
    }

}
