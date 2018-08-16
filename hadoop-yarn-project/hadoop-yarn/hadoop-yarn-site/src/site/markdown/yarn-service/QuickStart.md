<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Quick Start

This document describes how to deploy services on YARN using the YARN Service framework.

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

## Configure and start HDFS and YARN components

Start all the hadoop components for HDFS and YARN as usual.
To enable the YARN Service framework, add this property to `yarn-site.xml` and restart the ResourceManager or set the property before the ResourceManager is started.
This property is required for using the YARN Service framework through the CLI or the REST API.

```
  <property>
    <description>
      Enable services rest api on ResourceManager.
    </description>
    <name>yarn.webapp.api-service.enable</name>
    <value>true</value>
  </property>
```

## Example service 
Below is a simple service definition that launches sleep containers on YARN by writing a simple spec file and without writing any code.

```
{
  "name": "sleeper-service",
  "version": "1.0",
  "components" : 
    [
      {
        "name": "sleeper",
        "number_of_containers": 1,
        "launch_command": "sleep 900000",
        "resource": {
          "cpus": 1, 
          "memory": "256"
       }
      }
    ]
}
```
User can simply run a pre-built example service on YARN using below command:
```
yarn app -launch <service-name> <example-name>
```
e.g. Below command launches a `sleeper` service named as `my-sleeper` on YARN.
```
yarn app -launch my-sleeper sleeper
```
For launching docker based services using YARN Service framework, please refer to [API doc](YarnServiceAPI.html).

## Manage services on YARN via CLI
Below steps walk you through deploying a services on YARN using CLI.
Refer to [Yarn Commands](../YarnCommands.html) for the full list of commands and options.
### Deploy a service
```
yarn app -launch ${SERVICE_NAME} ${PATH_TO_SERVICE_DEF_FILE}
```
Params:
- SERVICE_NAME: The name of the service. Note that this needs to be unique across running services for the current user.
- PATH_TO_SERVICE_DEF: The path to the service definition file in JSON format.

For example:
```
yarn app -launch sleeper-service /path/to/local/sleeper.json
```

### Flex a component of a service
Increase or decrease the number of containers for a component.
```
yarn app -flex ${SERVICE_NAME} -component ${COMPONENT_NAME} ${NUMBER_OF_CONTAINERS}
```
For example, for a service named `sleeper-service`:

Set the `sleeper` component to `2` containers (absolute number).

```
yarn app -flex sleeper-service -component sleeper 2
```

Relative changes are also supported for the ${NUMBER_OF_CONTAINERS} in the flex command, such as +2 or -2.

### Stop a service
Stopping a service will stop all containers of the service and the ApplicationMaster, but does not delete the state of a service, such as the service root folder on hdfs.
```
yarn app -stop ${SERVICE_NAME}
```

### Restart a stopped service
Restarting a stopped service is easy - just call start!
```
yarn app -start ${SERVICE_NAME}
```

### Destroy a service
In addition to stopping a service, it also deletes the service root folder on hdfs and the records in YARN Service Registry.
```
yarn app -destroy ${SERVICE_NAME}
```

## Manage services on YARN via REST API

The YARN API Server REST API is activated as part of the ResourceManager when `yarn.webapp.api-service.enable` is set to true.

Services can be deployed on YARN through the ResourceManager web endpoint.

Refer to [API doc](YarnServiceAPI.html)  for the detailed API specificatiosn.

### Deploy a service

POST the aforementioned example service definition to the ResourceManager api-server endpoint:
```
POST  http://localhost:8088/app/v1/services
```

### Get a service status
```
GET  http://localhost:8088/app/v1/services/${SERVICE_NAME}
```

### Flex a component of a service
```
PUT  http://localhost:8088/app/v1/services/${SERVICE_NAME}/components/${COMPONENT_NAME}
```
`PUT` Request body:
```
{
    "name": "${COMPONENT_NAME}",
    "number_of_containers": ${COUNT}
}
```
For example:
```
{
    "name": "sleeper",
    "number_of_containers": 2
}
```

### Stop a service
Stopping a service will stop all containers of the service and the ApplicationMaster, but does not delete the state of a service, such as the service root folder on hdfs.

```
PUT  http://localhost:8088/app/v1/services/${SERVICE_NAME}
```

`PUT` Request body:
```
{
  "name": "${SERVICE_NAME}",
  "state": "STOPPED"
}
```

### Restart a stopped service
Restarting a stopped service is easy.

```
PUT  http://localhost:8088/app/v1/services/${SERVICE_NAME}
```

`PUT` Request body:
```
{
  "name": "${SERVICE_NAME}",
  "state": "STARTED"
}
```
### Destroy a service
In addition to stopping a service, it also deletes the service root folder on hdfs and the records in YARN Service Registry.
```
DELETE  http://localhost:8088/app/v1/services/${SERVICE_NAME}
```

## Services UI with YARN UI2 and Timeline Service v2
A new `service` tab is added in the YARN UI2 specially to show YARN Services in a first class manner. 
The services framework posts the data into TimelineService and the `service` UI reads data from TimelineService to render its content.

### Enable Timeline Service v2
Please refer to [TimeLineService v2 doc](../TimelineServiceV2.html) for how to enable Timeline Service v2.

### Enable new YARN UI

Set below config in `yarn-site.xml` and start ResourceManager. 
If you are building from source code, make sure you use `-Pyarn-ui` in the `mvn` command - this will generate the war file for the new YARN UI.
```
  <property>
    <description>To enable RM web ui2 application.</description>
    <name>yarn.webapp.ui2.enable</name>
    <value>true</value>
  </property>
```

# Run with security
YARN service framework supports running in a secure (kerberized) environment. User needs to specify the kerberos principal name and keytab when they launch the service.
E.g. A typical configuration looks like below:
```
{
  "name": "sample-service",
  ...
  ...
  "kerberos_principal" : {
    "principal_name" : "hdfs-demo/_HOST@EXAMPLE.COM",
    "keytab" : "file:///etc/security/keytabs/hdfs.headless.keytab"
  }
}
```
Note that `_HOST` is required in the `principal_name` field because Hadoop client validates that the server's (in this case, the AM's) principal has hostname present when communicating to the server.
* principal_name : the principal name of the user who launches the service
* keytab : URI of the keytab. Currently supports only files present on the bare host.
    * URI starts with `file://` - A path on the local host where the keytab is stored. It is assumed that admin pre-installs the keytabs on the local host before AM launches.

# Run with Docker
The above example is only for a non-docker container based service. YARN Service Framework also provides first-class support for managing docker based services.
Most of the steps for managing docker based services are the same except that in docker the `Artifact` type for a component is `DOCKER` and the Artifact `id` is the name of the docker image.
For details in how to setup docker on YARN, please check [Docker on YARN](../DockerContainers.html).

With docker support, it also opens up a set of new possibilities to implement features such as discovering service containers on YARN with DNS.
Check [ServiceDiscovery](ServiceDiscovery.html) for more details.
