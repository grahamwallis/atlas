<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# The 'open-metadata' module for ODPi Egeria OMRS Repository Connector and Event Mapper

## Egeria

The Egeria project (https://github.com/odpi/egeria) provides an open metadata and governance type system,
frameworks, APIs, event payloads and interchange protocols to enable tools, engines and platforms to exchange
metadata.

## Open Metadata Repository Service (OMRS)

The Egeria project defines the concept of a Cohort of servers which supports the exchange of metadata within the Cohort.
The Open Metadata Repository Services (OMRS) layer of Egeria enables this exchange metadata.
For a repository to participate in the cohort, it needs to connect to OMRS.


## The open-metadata module

The open-metadata module demonstrates how an Atlas metadata repository can participate in an Egeria cohort.
The open-metadata module provides an OMRS Repository Connector and OMRS Event Mapper:

* The **OMRS Repository Connector** - enables other servers in the Egeria cohort to interact with a repository
that is registered with a cohort.

* The **OMRS Event Mapper** - enables a repository registered with a cohort to send OMRSEvent notifications to
the other members of a cohort, whenever a significant event occurs.

## Getting started

It is recommended that you familiarize yourself with the README.md at the Egeria web-site - see the link above.
It is also recommended that you configure the Atlas server to enable relationship notifications and reduce the
graph lock-wait time to a reasonable duration, e.g. 10ms.

In order to add the Atlas server to an Egeria cohort, you need to configure and activate the OMRS support in the
Atlas server. This can be achieved as described in the following examples, which use HTTP POST to access the
admin-services REST API provided in the Atlas open-metadata module.

Replace the {atlas-host} and {atlas-port} parameters with the hostname (or address) and port number of the Atlas server.
If the Atlas server is using the default host and port, you would POST to localhost:21000/egeria/...

The {userName} and {serverName} parameters should be replaced with the user name and server name you wish to use.

The {cohortName} parameter should be replaced with the name of the cohort to which you are registering the Atlas server.

In all the REST interactions with the Atlas server, include an HTTP authorization header with valid credentials.

### Event bus configuration

```
POST to {atlas-host}:{atlas-port}/egeria/open-metadata/admin-services/users/{userName}/servers/{serverName}/event-bus
```

The request body should be as follows, with the {kafka-host} and {kafka-port} parameters replaced by the
host and port of the Kafka broker supporting your Egeria cohort OMRSTopic :

```json
{
    "producer":
    {
        "bootstrap.servers":"{kafka-host}:{kafka-port}"
    },
    "consumer":
    {
        "bootstrap.servers":"{kafka-host}:{kafka-port}"
    }
}
```

### Server root configuration

The server root must be configured as shown here, prior to setting the repository type (below).

```
POST to {atlas-host}:{atlas-port}/egeria/open-metadata/admin-services/users/{userName}/servers/{serverName}/server-url-root?url={atlas-host}:{atlas-port}/egeria
```

### Repository type configuration

The server root must have already been configured prior to setting the repository type.

```
POST to {atlas-host}:{atlas-port}/egeria/open-metadata/admin-services/users/{userName}/servers/{serverName}/atlas-repository
```

### Cohort registration configuration

```
POST to {atlas-host}:{atlas-port}/egeria/open-metadata/admin-services/users/{userName}/servers/{serverName}/cohorts/{cohortName}
```

### Activation

```
POST to {atlas-host}:{atlas-port}/egeria/open-metadata/admin-services/users/{userName}/servers/{serverName}/instance
```

### General use

Once the open-metadata module has been activated, other OMAG servers can invoke the Egeria repository-services API
to interact with the Atlas server.

If you wanted to test what happens when the repository-services API is invoked, you could emulate an Egeria request by
POSTing to the repository-services API. The repository-services API supports multi-tenancy so the URI format is slightly
different to the format used for the admin-services API above.

For example, to create an entity the following REST request would be issued:

```
POST to {atlas-host}:{atlas-port}/egeria/servers/{serverName}/open-metadata/repository-services/users/{userName}/instances/entity
```

The request body should contain a serialized EntityCreateRequest, for example:

```json
{
    "class":"EntityCreateRequest",
    "entityTypeGUID":"b46cddb3-9864-4c5d-8a49-266b3fc95cb8",
    "initialProperties":
    {
        "class":"InstanceProperties",
        "instanceProperties":
        {
            "qualifiedName":
            {
                "class":"PrimitivePropertyValue",
                "instancePropertyCategory":"PRIMITIVE",
                "primitiveDefCategory":"OM_PRIMITIVE_TYPE_STRING",
                "primitiveValue":"my-first-entity"
            }
        },
        "propertyNames":["qualifiedName"],
        "propertyCount":1
    }
}
```


The remaining repository-services API methods are similar. Check the Egeria documentation for details of the URIs and bodies.

When a notifiable event occurs in the Atlas server, the OMRS Event Mapper will create and post an OMRSEventV1 event to the OMRSTopic for the cohort.