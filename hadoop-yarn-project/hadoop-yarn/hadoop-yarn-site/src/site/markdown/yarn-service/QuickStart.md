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

## Start HDFS and YARN components

 Start all the hadoop components HDFS, YARN as usual.


## Example service 
Below is a simple service definition that launches sleep containers on YARN by writing a simple spec file and without writing any code.

```
{
  "name": "sleeper-service",
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

For launching docker based services using YARN Service framework, please refer to [API doc](YarnServiceAPI.md).

## Manage services on YARN via CLI
Below steps walk you through deploying a services on YARN using CLI.
Refer to [Yarn Commands](../YarnCommands.md) for the full list of commands and options.
### Deploy a service
```
yarn service create --file ${PATH_TO_SERVICE_DEF_FILE}
```
Params:
- SERVICE_NAME: The name of the service. Note that this needs to be unique across all running services.
- PATH_TO_SERVICE_DEF: The path to the service definition file in JSON format.

For example:
```
yarn service create --file /path/to/local/sleeper.json
```

### Flex a component of a service
Increase or decrease the number of containers for a component.
```
yarn service flex ${SERVICE_NAME} --component ${COMPONENT_NAME} ${NUMBER_OF_CONTAINERS}
```
For example, for a service named `sleeper-service`:

Set the `sleeper` component to `2` containers (absolute number).

```
yarn service flex sleeper-service --component sleeper 2
```

### Stop a service
Stopping a service will stop all containers of the service and the ApplicationMaster, but does not delete the state of a service, such as the service root folder on hdfs.
```
yarn service stop ${SERVICE_NAME}
```

### Restart a stopped service
Restarting a stopped service is easy - just call start!
```
yarn service start ${SERVICE_NAME}
```

### Destroy a service
In addition to stopping a service, it also deletes the service root folder on hdfs and the records in YARN Service Registry.
```
yarn service destroy ${SERVICE_NAME}
```

## Manage services on YARN via REST API
Below steps walk you through deploying services on YARN via REST API.
 Refer to [API doc](YarnServiceAPI.md)  for the detailed API specificatiosn.
### Start API-Server for deploying services on YARN
API server is the service that sits in front of YARN ResourceManager and lets users submit their API specs via HTTP.
```
 yarn --daemon start apiserver
 ```
The above command starts the API Server on the localhost at port 9191 by default. 

### Deploy a service
POST the aforementioned example service definition to the api-server endpoint: 
```
POST  http://localhost:9191/ws/v1/services
```

### Get a service status
```
GET  http://localhost:9191/ws/v1/services/${SERVICE_NAME}
```

### Flex a component of a service
```
PUT  http://localhost:9191/ws/v1/services/${SERVICE_NAME}/components/${COMPONENT_NAME}
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
PUT  http://localhost:9191/ws/v1/services/${SERVICE_NAME}
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
PUT  http://localhost:9191/ws/v1/services/${SERVICE_NAME}
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
DELETE  http://localhost:9191/ws/v1/services/${SERVICE_NAME}
```

## Services UI with YARN UI2 and Timeline Service v2
A new `service` tab is added in the YARN UI2 specially to show YARN Services in a first class manner. 
The services framework posts the data into TimelineService and the `service` UI reads data from TimelineService to render its content.

### Enable Timeline Service v2
Please refer to [TimeLineService v2 doc](../TimelineServiceV2.md) for how to enable Timeline Service v2.

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

## Service Discovery with YARN DNS
YARN Service framework comes with a DNS server (backed by YARN Service Registry) which enables DNS based discovery of services deployed on YARN.
That is, user can simply access their services in a well-defined naming format as below:

```
${COMPONENT_INSTANCE_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}
```
For example, in a cluster whose domain name is `yarncluster` (as defined by the `hadoop.registry.dns.domain-name` in `yarn-site.xml`), a service named `hbase` deployed by user `dev` 
with two components `hbasemaster` and `regionserver` can be accessed as below:

This URL points to the usual hbase master UI
```
http://hbasemaster-0.hbase.dev.yarncluster:16010/master-status
```


Note that YARN service framework assigns COMPONENT_INSTANCE_NAME for each container in a sequence of monotonically increasing integers. For example, `hbasemaster-0` gets
assigned `0` since it is the first and only instance for the `hbasemaster` component. In case of `regionserver` component, it can have multiple containers
 and so be named as such: `regionserver-0`, `regionserver-1`, `regionserver-2` ... etc 
 
`Disclaimer`: The DNS implementation is still experimental. It should not be used as a fully-functional corporate DNS. 

### Start the DNS server 
By default, the DNS runs on non-privileged port `5353`.
If it is configured to use the standard privileged port `53`, the DNS server needs to be run as root:
```
sudo su - -c "yarn org.apache.hadoop.registry.server.dns.RegistryDNSServer > /${HADOOP_LOG_FOLDER}/registryDNS.log 2>&1 &" root
```
Please refer to [YARN DNS doc](ServicesDiscovery.md) for the full list of configurations.