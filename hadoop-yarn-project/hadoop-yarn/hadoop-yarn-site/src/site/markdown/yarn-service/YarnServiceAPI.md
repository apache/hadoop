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

# YARN Service API

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

## Introduction
Bringing a new service on YARN today is not a simple experience. The APIs of existing
frameworks are either too low level (native YARN), require writing new code (for frameworks with programmatic APIs)
or writing a complex spec (for declarative frameworks).

This simplified REST API can be used to create and manage the lifecycle of YARN services.
In most cases, the application owner will not be forced to make any changes to their applications.
This is primarily true if the application is packaged with containerization technologies like Docker.

This document describes the API specifications (aka. YarnFile) for deploying/managing
containerized services on YARN. The same JSON spec can be used for both REST API
and CLI to manage the services.


### Version information
Version: 1.0.0

### License information
License: Apache 2.0
License URL: http://www.apache.org/licenses/LICENSE-2.0.html

### URI scheme
Host: host.mycompany.com

Port: 8088(default RM port)

Schemes: HTTP

### Consumes

* application/json


### Produces

* application/json


## Paths
### Create a service
```
POST /app/v1/services
```

#### Description

Create a service. The request JSON is a service object with details required for creation. If the request is successful it returns 202 Accepted. A success of this API only confirms success in submission of the service creation request. There is no guarantee that the service will actually reach a RUNNING state. Resource availability and several other factors determines if the service will be deployed in the cluster. It is expected that clients would subsequently call the GET API to get details of the service and determine its state.

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|BodyParameter|Service|Service request object|true|Service||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|202|The request to create a service is accepted|No Content|
|400|Invalid service definition provided in the request body|No Content|
|500|Failed to create a service|No Content|
|default|Unexpected error|ServiceStatus|


### (TBD) List of services running in the cluster.
```
GET /app/v1/services
```

#### Description

Get a list of all currently running services (response includes a minimal projection of the service info). For more details do a GET on a specific service name.

#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|An array of services|Service array|
|default|Unexpected error|ServiceStatus|


### Get current version of the API server.
```
GET /app/v1/services/version
```

#### Description

Get current version of the API server.

#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Successful request|No Content|


### Update a service or upgrade the binary version of the components of a running service
```
PUT /app/v1/services/{service_name}
```

#### Description

Update the runtime properties of a service. Currently the following operations are supported - update lifetime, stop/start a service. The PUT operation is also used to orchestrate an upgrade of the service containers to a newer version of their artifacts (TBD).

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|service_name|Service name|true|string||
|BodyParameter|Service|The updated service definition. It can contain the updated lifetime of a service or the desired state (STOPPED/STARTED) of a service to initiate a start/stop operation against the specified service|true|Service||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|204|Update or upgrade was successful|No Content|
|404|Service does not exist|No Content|
|default|Unexpected error|ServiceStatus|


### Get details of a service.
```
GET /app/v1/services/{service_name}
```

#### Description

Return the details (including containers) of a running service

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|service_name|Service name|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|a service object|object|
|404|Service does not exist|No Content|
|default|Unexpected error|ServiceStatus|


### Destroy a service
```
DELETE /app/v1/services/{service_name}
```

#### Description

Destroy a service and release all resources. This API might have to return JSON data providing location of logs (TBD), etc.

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|service_name|Service name|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|204|Destroy was successful|No Content|
|404|Service does not exist|No Content|
|default|Unexpected error|ServiceStatus|


### Flex a component's number of instances.
```
PUT /app/v1/services/{service_name}/components/{component_name}
```

#### Description

Set a component's desired number of instanes

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|service_name|Service name|true|string||
|PathParameter|component_name|Component name|true|string||
|BodyParameter|Component|The definition of a component which contains the updated number of instances.|true|Component||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Flex was successful|No Content|
|404|Service does not exist|No Content|
|default|Unexpected error|ServiceStatus|


## Definitions
### Artifact

Artifact of a service component. If not specified, component will just run the bare launch command and no artifact will be localized.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|id|Artifact id. Examples are package location uri for tarball based services, image name for docker, name of service, etc.|true|string||
|type|Artifact type, like docker, tarball, etc. (optional). For TARBALL type, the specified tarball will be localized to the container local working directory under a folder named lib. For SERVICE type, the service specified will be read and its components will be added into this service. The original component with artifact type SERVICE will be removed (any properties specified in the original component will be ignored).|false|enum (DOCKER, TARBALL, SERVICE)|DOCKER|
|uri|Artifact location to support multiple artifact stores (optional).|false|string||


### Component

One or more components of the service. If the service is HBase say, then the component can be a simple role like master or regionserver. If the service is a complex business webapp then a component can be other services say Kafka or Storm. Thereby it opens up the support for complex and nested services.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|name|Name of the service component (mandatory). If Registry DNS is enabled, the max length is 44 characters.|true|string||
|state|The state of the component|false|ComponentState||
|dependencies|An array of service components which should be in READY state (as defined by readiness check), before this component can be started. The dependencies across all components of a service should be represented as a DAG.|false|string array||
|readiness_check|Readiness check for this component.|false|ReadinessCheck||
|artifact|Artifact of the component (optional). If not specified, the service level global artifact takes effect.|false|Artifact||
|launch_command|The custom launch command of this component (optional for DOCKER component, required otherwise). When specified at the component level, it overrides the value specified at the global level (if any). If docker image supports ENTRYPOINT, launch_command is delimited by comma(,) instead of space.|false|string||
|resource|Resource of this component (optional). If not specified, the service level global resource takes effect.|false|Resource||
|number_of_containers|Number of containers for this component (optional). If not specified, the service level global number_of_containers takes effect.|false|integer (int64)||
|decommissioned_instances|List of decommissioned component instances.|false|string array||
|containers|Containers of a started component. Specifying a value for this attribute for the POST payload raises a validation error. This blob is available only in the GET response of a started service.|false|Container array||
|run_privileged_container|Run all containers of this component in privileged mode (YARN-4262).|false|boolean||
|placement_policy|Advanced scheduling and placement policies for all containers of this component.|false|PlacementPolicy||
|configuration|Config properties for this component.|false|Configuration||
|quicklinks|A list of quicklink keys defined at the service level, and to be resolved by this component.|false|string array||
|restart_policy|Policy of restart component. Including ALWAYS (Always restart
 component even if instance exit code = 0); ON_FAILURE (Only restart component if instance exit code != 0); NEVER (Do not restart in any cases). Flexing is not supported for components which have restart_policy=ON_FAILURE/NEVER|false|string|ALWAYS|


### ComponentState

The state of the component

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|state|enum of the state of the component|false|enum (INIT, FLEXING, STABLE, UPGRADING)||


### ConfigFile

A config file that needs to be created and made available as a volume in a service component container.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|type|Config file in the standard format like xml, properties, json, yaml, template or static/archive resource files. When static/archive types are specified, file must be uploaded to remote file system before launching the job, and YARN service framework will localize files prior to launching containers. Archive files are unwrapped during localization |false|enum (XML, PROPERTIES, JSON, YAML, TEMPLATE, HADOOP_XML, STATIC, ARCHIVE)||
|dest_file|The path that this configuration file should be created as. If it is an absolute path, it will be mounted into the DOCKER container. Absolute paths are only allowed for DOCKER containers.  If it is a relative path, only the file name should be provided, and the file will be created in the container local working directory under a folder named conf for all types other than static/archive. For static/archive resource types, the files are available under resources directory.|false|string||
|src_file|This provides the source location of the configuration file, the content of which is dumped to dest_file post property substitutions, in the format as specified in type. Typically the src_file would point to a source controlled network accessible file maintained by tools like puppet, chef, or hdfs etc. Currently, only hdfs is supported.|false|string||
|properties|A blob of key value pairs that will be dumped in the dest_file in the format as specified in type. If src_file is specified, src_file content are dumped in the dest_file and these properties will overwrite, if any, existing properties in src_file or be added as new properties in src_file.|false|object||


### Configuration

Set of configuration properties that can be injected into the service components via envs, files and custom pluggable helper docker containers. Files of several standard formats like xml, properties, json, yaml and templates will be supported.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|properties|A blob of key-value pairs for configuring the YARN service AM.|false|object||
|env|A blob of key-value pairs which will be appended to the default system properties and handed off to the service at start time. All placeholder references to properties will be substituted before injection.|false|object||
|files|Array of list of files that needs to be created and made available as volumes in the service component containers.|false|ConfigFile array||


### Container

An instance of a running service container.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|id|Unique container id of a running service, e.g. container_e3751_1458061340047_0008_01_000002.|false|string||
|launch_time|The time when the container was created, e.g. 2016-03-16T01:01:49.000Z. This will most likely be different from cluster launch time.|false|string (date)||
|ip|IP address of a running container, e.g. 172.31.42.141. The IP address and hostname attribute values are dependent on the cluster/docker network setup as per YARN-4007.|false|string||
|hostname|Fully qualified hostname of a running container, e.g. ctr-e3751-1458061340047-0008-01-000002.examplestg.site. The IP address and hostname attribute values are dependent on the cluster/docker network setup as per YARN-4007.|false|string||
|bare_host|The bare node or host in which the container is running, e.g. cn008.example.com.|false|string||
|state|State of the container of a service.|false|ContainerState||
|component_instance_name|Name of the component instance that this container instance belongs to. Component instance name is named as $COMPONENT_NAME-i, where i is a monotonically increasing integer. E.g. A componet called nginx can have multiple component instances named as nginx-0, nginx-1 etc. Each component instance is backed by a container instance.|false|string||
|resource|Resource used for this container.|false|Resource||
|artifact|Artifact used for this container.|false|Artifact||
|privileged_container|Container running in privileged mode or not.|false|boolean||


### ContainerState

The current state of the container of a service.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|state|enum of the state of the container|false|enum (INIT, STARTED, READY)||


### KerberosPrincipal

The kerberos principal info of the user who launches the service.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|principal_name|The principal name of the user who launches the service. Note that `_HOST` is required in the `principal_name` field such as `testuser/_HOST@EXAMPLE.COM` because Hadoop client validates that the server's (in this case, the AM's) principal has hostname present when communicating to the server.|false|string||
|keytab|The URI of the kerberos keytab. Currently supports only files present on the bare host. URI starts with "file://" - A path on the local host where the keytab is stored. It is assumed that admin pre-installs the keytabs on the local host before AM launches.|false|string||


### PlacementConstraint

Placement constraint details.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|name|An optional name associated to this constraint.|false|string||
|type|The type of placement.|true|PlacementType||
|scope|The scope of placement.|true|PlacementScope||
|target_tags|The name of the components that this component's placement policy is depending upon are added as target tags. So for affinity say, this component's containers are requesting to be placed on hosts where containers of the target tag component(s) are running on. Target tags can also contain the name of this component, in which case it implies that for anti-affinity say, no more than one container of this component can be placed on a host. Similarly, for cardinality, it would mean that containers of this component is requesting to be placed on hosts where at least minCardinality but no more than maxCardinality containers of the target tag component(s) are running.|false|string array||
|node_attributes|Node attributes are a set of key:value(s) pairs associated with nodes.|false|object||
|node_partitions|Node partitions where the containers of this component can run.|false|string array||
|min_cardinality|When placement type is cardinality, the minimum number of containers of the depending component that a host should have, where containers of this component can be allocated on.|false|integer (int64)||
|max_cardinality|When placement type is cardinality, the maximum number of containers of the depending component that a host should have, where containers of this component can be allocated on.|false|integer (int64)||


### PlacementPolicy

Advanced placement policy of the components of a service.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|constraints|Placement constraint details.|true|PlacementConstraint array||


### PlacementScope

The scope of placement for the containers of a component.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|type||false|enum (NODE, RACK)||


### PlacementType

The type of placement - affinity/anti-affinity/affinity-with-cardinality with containers of another component or containers of the same component (self).

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|type||false|enum (AFFINITY, ANTI_AFFINITY, AFFINITY_WITH_CARDINALITY)||


### ReadinessCheck

A check to be performed to determine the readiness of a component instance (a container).
If no readiness check is specified, the default readiness check will be used unless the yarn.service.default-readiness-check.enabled configuration property is set to false at the component or global level.
The artifact field is currently unsupported but may be implemented in the future, enabling a pluggable helper container to support advanced use cases.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|type|DEFAULT (AM checks whether the container has an IP and optionally performs a DNS lookup for the container hostname), HTTP (AM performs default checks, plus sends a REST call to the container and expects a response code between 200 and 299), or PORT (AM performs default checks, plus attempts to open a socket connection to the container on a specified port).|true|enum (DEFAULT, HTTP, PORT)||
|properties|A blob of key value pairs that will be used to configure the check.|false|object||
|artifact|Artifact of the pluggable readiness check helper container (optional). If specified, this helper container typically hosts the http uri and encapsulates the complex scripts required to perform actual container readiness check. At the end it is expected to respond a 204 No content just like the simplified use case. This pluggable framework benefits service owners who can run services without any packaging modifications. Note, artifacts of type docker only is supported for now. NOT IMPLEMENTED YET|false|Artifact||


### Resource

Resource determines the amount of resources (vcores, memory, network, etc.) usable by a container. This field determines the resource to be applied for all the containers of a component or service. The resource specified at the service (or global) level can be overriden at the component level. Only one of profile OR cpu & memory are expected. It raises a validation exception otherwise.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|profile|Each resource profile has a unique id which is associated with a cluster-level predefined memory, cpus, etc.|false|string||
|cpus|Amount of vcores allocated to each container (optional but overrides cpus in profile if specified).|false|integer (int32)||
|memory|Amount of memory allocated to each container (optional but overrides memory in profile if specified). Currently accepts only an integer value and default unit is in MB.|false|string||
|additional|A map of resource type name to resource type information. Including value (integer), unit (string) and optional attributes (map). This will be used to specify resource other than cpu and memory. Please refer to example below.|false|object||


### ResourceInformation

ResourceInformation determines unit/value of resource types in addition to memory and vcores. It will be part of Resource object.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|value|Integer value of the resource.|false|integer (int64)||
|unit|Unit of the resource, acceptable values are - p/n/u/m/k/M/G/T/P/Ki/Mi/Gi/Ti/Pi. By default it is empty means no unit.|false|string||


### Service

a service resource has the following attributes.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|name|A unique service name. If Registry DNS is enabled, the max length is 63 characters.|true|string||
|version|Version of the service.|true|string||
|description|Description of the service.|false|string||
|id|A unique service id.|false|string||
|artifact|The default artifact for all components of the service except the components which has Artifact type set to SERVICE (optional).|false|Artifact||
|resource|The default resource for all components of the service (optional).|false|Resource||
|launch_time|The time when the service was created, e.g. 2016-03-16T01:01:49.000Z.|false|string (date)||
|number_of_running_containers|In get response this provides the total number of running containers for this service (across all components) at the time of request. Note, a subsequent request can return a different number as and when more containers get allocated until it reaches the total number of containers or if a flex request has been made between the two requests.|false|integer (int64)||
|lifetime|Life time (in seconds) of the service from the time it reaches the STARTED state (after which it is automatically destroyed by YARN). For unlimited lifetime do not set a lifetime value.|false|integer (int64)||
|components|Components of a service.|false|Component array||
|configuration|Config properties of a service. Configurations provided at the service/global level are available to all the components. Specific properties can be overridden at the component level.|false|Configuration||
|state|State of the service. Specifying a value for this attribute for the PUT payload means update the service to this desired state.|false|ServiceState||
|quicklinks|A blob of key-value pairs of quicklinks to be exported for a service.|false|object||
|queue|The YARN queue that this service should be submitted to.|false|string||
|kerberos_principal|The principal info of the user who launches the service|false|KerberosPrincipal||
|docker_client_config|URI of the file containing the docker client configuration (e.g. hdfs:///tmp/config.json)|false|string||
|dependencies|A list of service names that this service depends on.| false | string array ||

### ServiceState

The current state of a service.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|state|enum of the state of the service|false|enum (ACCEPTED, STARTED, STABLE, STOPPED, FAILED, FLEX, UPGRADING)||


### ServiceStatus

The current status of a submitted service, returned as a response to the GET API.

|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|diagnostics|Diagnostic information (if any) for the reason of the current state of the service. It typically has a non-null value, if the service is in a non-running state.|false|string||
|state|Service state.|false|ServiceState||
|code|An error code specific to a scenario which service owners should be able to use to understand the failure in addition to the diagnostic information.|false|integer (int32)||



## Examples

### Create a simple single-component service with most attribute values as defaults
```
POST URL - http://localhost:8088/app/v1/services
```

#### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 2,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256"
        }
      }
    ]
}
```

#### GET Response JSON
```
GET URL - http://localhost:8088/app/v1/services/hello-world
```

Note, lifetime value of -1 means unlimited lifetime.

```json
{
    "name": "hello-world",
    "version": "1.0.0",
    "description": "hello world example",
    "id": "application_1503963985568_0002",
    "lifetime": -1,
    "state": "STABLE",
    "components": [
        {
            "name": "hello",
            "state": "STABLE",
            "resource": {
                "cpus": 1,
                "memory": "256"
            },
            "configuration": {
                "properties": {},
                "env": {},
                "files": []
            },
            "quicklinks": [],
            "containers": [
                {
                    "id": "container_e03_1503963985568_0002_01_000002",
                    "ip": "10.22.8.143",
                    "hostname": "ctr-e03-1503963985568-0002-01-000002.example.site",
                    "state": "READY",
                    "launch_time": 1504051512412,
                    "bare_host": "host100.cloud.com",
                    "component_instance_name": "hello-0"
                },
                {
                    "id": "container_e03_1503963985568_0002_01_000003",
                    "ip": "10.22.8.144",
                    "hostname": "ctr-e03-1503963985568-0002-01-000003.example.site",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "host100.cloud.com",
                    "component_instance_name": "hello-1"
                }
            ],
            "launch_command": "./start_nginx.sh",
            "number_of_containers": 1,
            "run_privileged_container": false
        }
    ],
    "configuration": {
        "properties": {},
        "env": {},
        "files": []
    },
    "quicklinks": {}
}

```
### Update to modify the lifetime of a service
```
PUT URL - http://localhost:8088/app/v1/services/hello-world
```

#### PUT Request JSON

Note, irrespective of what the current lifetime value is, this update request will set the lifetime of the service to be 3600 seconds (1 hour) from the time the request is submitted. Hence, if a service has remaining lifetime of 5 mins (say) and would like to extend it to an hour OR if an application has remaining lifetime of 5 hours (say) and would like to reduce it down to an hour, then for both scenarios you need to submit the same request below.

```json
{
  "lifetime": 3600
}
```
### Stop a service
```
PUT URL - http://localhost:8088/app/v1/services/hello-world
```

#### PUT Request JSON
```json
{
  "state": "STOPPED"
}
```

### Start a service
```
PUT URL - http://localhost:8088/app/v1/services/hello-world
```

#### PUT Request JSON
```json
{
  "state": "STARTED"
}
```

### Update to flex up/down the number of containers (instances) of a component of a service
```
PUT URL - http://localhost:8088/app/v1/services/hello-world/components/hello
```

#### PUT Request JSON
```json
{
  "number_of_containers": 3
}
```

Alternatively, you can specify the entire "components" section instead.
```
PUT URL - http://localhost:8088/app/v1/services/hello-world
```

#### PUT Request JSON
```json
{
  "state": "FLEX",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 3
      }
    ]
}
```

### Destroy a service
```
DELETE URL - http://localhost:8088/app/v1/services/hello-world
```

***

### Create a complicated service  - HBase
```
POST URL - http://localhost:8088:/app/v1/services/hbase-app-1
```

#### POST Request JSON

```json
{
  "name": "hbase-app-1",
  "version": "1.0.0",
  "description": "hbase service",
  "lifetime": "3600",
  "components": [
    {
      "name": "hbasemaster",
      "number_of_containers": 1,
      "artifact": {
        "id": "hbase:latest",
        "type": "DOCKER"
      },
      "launch_command": "/usr/hdp/current/hbase-master/bin/hbase master start",
      "resource": {
        "cpus": 1,
        "memory": "2048"
      },
      "configuration": {
        "env": {
          "HBASE_LOG_DIR": "<LOG_DIR>"
        },
        "files": [
          {
            "type": "XML",
            "dest_file": "/etc/hadoop/conf/core-site.xml",
            "properties": {
              "fs.defaultFS": "${CLUSTER_FS_URI}"
            }
          },
          {
            "type": "XML",
            "dest_file": "/etc/hbase/conf/hbase-site.xml",
            "properties": {
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "${CLUSTER_ZK_QUORUM}",
              "hbase.rootdir": "${SERVICE_HDFS_DIR}/hbase",
              "zookeeper.znode.parent": "${SERVICE_ZK_PATH}",
              "hbase.master.hostname": "hbasemaster.${SERVICE_NAME}.${USER}.${DOMAIN}",
              "hbase.master.info.port": "16010"
            }
          }
        ]
      }
    },
    {
      "name": "regionserver",
      "number_of_containers": 3,
      "artifact": {
        "id": "hbase:latest",
        "type": "DOCKER"
      },
      "launch_command": "/usr/hdp/current/hbase-regionserver/bin/hbase regionserver start",
      "resource": {
        "cpus": 1,
        "memory": "2048"
      },
      "configuration": {
        "env": {
          "HBASE_LOG_DIR": "<LOG_DIR>"
        },
        "files": [
          {
            "type": "XML",
            "dest_file": "/etc/hadoop/conf/core-site.xml",
            "properties": {
              "fs.defaultFS": "${CLUSTER_FS_URI}"
            }
          },
          {
            "type": "XML",
            "dest_file": "/etc/hbase/conf/hbase-site.xml",
            "properties": {
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "${CLUSTER_ZK_QUORUM}",
              "hbase.rootdir": "${SERVICE_HDFS_DIR}/hbase",
              "zookeeper.znode.parent": "${SERVICE_ZK_PATH}",
              "hbase.master.hostname": "hbasemaster.${SERVICE_NAME}.${USER}.${DOMAIN}",
              "hbase.master.info.port": "16010",
              "hbase.regionserver.hostname": "${COMPONENT_INSTANCE_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}"
            }
          }
        ]
      }
    }
  ],
  "quicklinks": {
    "HBase Master Status UI": "http://hbasemaster0.${SERVICE_NAME}.${USER}.${DOMAIN}:16010/master-status",
    "Proxied HBase Master Status UI": "http://app-proxy/${DOMAIN}/${USER}/${SERVICE_NAME}/hbasemaster/16010/"
  }
}
```

### Create a service requesting GPUs in addition to CPUs and RAM
```
POST URL - http://localhost:8088/app/v1/services
```

#### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example with GPUs",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 2,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256",
          "additional" : {
            "yarn.io/gpu" : {
              "value" : 4,
              "unit" : ""
            }
          }
        }
      }
    ]
}
```

### Create a service with a component requesting anti-affinity placement policy
```
POST URL - http://localhost:8088/app/v1/services
```

#### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example with anti-affinity",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 3,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256"
        },
        "placement_policy": {
          "constraints": [
            {
              "type": "ANTI_AFFINITY",
              "scope": "NODE",
              "node_attributes": {
                "os": ["centos6", "centos7"],
                "fault_domain": ["fd1", "fd2"]
              },
              "node_partitions": [
                "gpu",
                "fast-disk"
              ]
            }
          ]
        }
      }
    ]
}
```

#### GET Response JSON
```
GET URL - http://localhost:8088/app/v1/services/hello-world
```

Note, for an anti-affinity component no more than 1 container will be allocated
in a specific node. In this example, 3 containers have been requested by
component "hello". All 3 containers were allocated on separated centos7 nodes
because the node attributes expects to run on centos7 nodes.
If the cluster had less than 3 NMs then less than
3 containers would be allocated. In cases when the number of allocated containers
are less than the number of requested containers, the component and the service
will be in non-STABLE state.

```json
{
    "name": "hello-world",
    "version": "1.0.0",
    "description": "hello world example with anti-affinity",
    "id": "application_1503963985568_0003",
    "lifetime": -1,
    "state": "STABLE",
    "components": [
        {
            "name": "hello",
            "state": "STABLE",
            "resource": {
                "cpus": 1,
                "memory": "256"
            },
            "placement_policy": {
              "constraints": [
                {
                  "type": "AFFINITY",
                  "scope": "NODE",
                  "node_attributes": {
                    "os": ["centos7"]
                  },
                  "node_partitions": [
                    "gpu",
                    "fast-disk"
                  ]
                },
                {
                  "type": "ANTI_AFFINITY",
                  "scope": "NODE",
                  "target_tags": [
                    "hello"
                  ]
                }
              ]
            },
            "configuration": {
                "properties": {},
                "env": {},
                "files": []
            },
            "quicklinks": [],
            "containers": [
                {
                    "id": "container_e03_1503963985568_0003_01_000002",
                    "ip": "10.22.8.143",
                    "hostname": "ctr-e03-1503963985568-0003-01-000002.example.site",
                    "state": "READY",
                    "launch_time": 1504051512412,
                    "bare_host": "host100.cloud.com",
                    "component_instance_name": "hello-0"
                },
                {
                    "id": "container_e03_1503963985568_0003_01_000003",
                    "ip": "10.22.8.144",
                    "hostname": "ctr-e03-1503963985568-0003-01-000003.example.site",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "host101.cloud.com",
                    "component_instance_name": "hello-1"
                },
                {
                    "id": "container_e03_1503963985568_0003_01_000004",
                    "ip": "10.22.8.145",
                    "hostname": "ctr-e03-1503963985568-0003-01-000004.example.site",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "host102.cloud.com",
                    "component_instance_name": "hello-2"
                }
            ],
            "launch_command": "./start_nginx.sh",
            "number_of_containers": 1,
            "run_privileged_container": false
        }
    ],
    "configuration": {
        "properties": {},
        "env": {},
        "files": []
    },
    "quicklinks": {}
}
```

### Create a service with health threshold monitor enabled for a component
```
POST URL - http://localhost:8088/app/v1/services
```

#### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example with health threshold monitor",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 100,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256"
        },
        "configuration": {
          "properties": {
            "yarn.service.container-health-threshold.percent": "90",
            "yarn.service.container-health-threshold.window-secs": "400",
            "yarn.service.container-health-threshold.init-delay-secs": "800"
          }
        }
      }
    ]
}
```

