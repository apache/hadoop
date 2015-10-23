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

NodeManager REST API's
=======================

* [Overview](#Overview)
* [Enabling CORS support](#Enabling_CORS_support)
* [NodeManager Information API](#NodeManager_Information_API)
* [Applications API](#Applications_API)
* [Application API](#Application_API)
* [Containers API](#Containers_API)
* [Container API](#Container_API)

Overview
--------

The NodeManager REST API's allow the user to get status on the node and information about applications and containers running on that node.

Enabling CORS support
---------------------
To enable cross-origin support (CORS) for the NM only(without enabling it for the RM), please set the following configuration parameters:

In core-site.xml, add org.apache.hadoop.security.HttpCrossOriginFilterInitializer to hadoop.http.filter.initializers.
In yarn-site.xml, set yarn.nodemanager.webapp.cross-origin.enabled to true.

NodeManager Information API
---------------------------

The node information resource provides overall information about that particular node.

### URI

Both of the following URI's give you the cluster information.

      * http://<nm http address:port>/ws/v1/node
      * http://<nm http address:port>/ws/v1/node/info

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *nodeInfo* object

| Properties | Data Type | Description |
|:---- |:---- |:---- |
| id | long | The NodeManager id |
| nodeHostName | string | The host name of the NodeManager |
| totalPmemAllocatedContainersMB | long | The amount of physical memory allocated for use by containers in MB |
| totalVmemAllocatedContainersMB | long | The amount of virtual memory allocated for use by containers in MB |
| totalVCoresAllocatedContainers | long | The number of virtual cores allocated for use by containers |
| lastNodeUpdateTime | long | The last timestamp at which the health report was received (in ms since epoch) |
| healthReport | string | The diagnostic health report of the node |
| nodeHealthy | boolean | true/false indicator of if the node is healthy |
| nodeManagerVersion | string | Version of the NodeManager |
| nodeManagerBuildVersion | string | NodeManager build string with build version, user, and checksum |
| nodeManagerVersionBuiltOn | string | Timestamp when NodeManager was built(in ms since epoch) |
| hadoopVersion | string | Version of hadoop common |
| hadoopBuildVersion | string | Hadoop common build string with build version, user, and checksum |
| hadoopVersionBuiltOn | string | Timestamp when hadoop common was built(in ms since epoch) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "nodeInfo" : {
      "hadoopVersionBuiltOn" : "Mon Jan  9 14:58:42 UTC 2012",
      "nodeManagerBuildVersion" : "0.23.1-SNAPSHOT from 1228355 by user1 source checksum 20647f76c36430e888cc7204826a445c",
      "lastNodeUpdateTime" : 1326222266126,
      "totalVmemAllocatedContainersMB" : 17203,
      "totalVCoresAllocatedContainers" : 8,
      "nodeHealthy" : true,
      "healthReport" : "",
      "totalPmemAllocatedContainersMB" : 8192,
      "nodeManagerVersionBuiltOn" : "Mon Jan  9 15:01:59 UTC 2012",
      "nodeManagerVersion" : "0.23.1-SNAPSHOT",
      "id" : "host.domain.com:8041",
      "hadoopBuildVersion" : "0.23.1-SNAPSHOT from 1228292 by user1 source checksum 3eba233f2248a089e9b28841a784dd00",
      "nodeHostName" : "host.domain.com",
      "hadoopVersion" : "0.23.1-SNAPSHOT"
   }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://<nm http address:port>/ws/v1/node/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 983
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<nodeInfo>
  <healthReport/>
  <totalVmemAllocatedContainersMB>17203</totalVmemAllocatedContainersMB>
  <totalPmemAllocatedContainersMB>8192</totalPmemAllocatedContainersMB>
  <totalVCoresAllocatedContainers>8</totalVCoresAllocatedContainers>
  <lastNodeUpdateTime>1326222386134</lastNodeUpdateTime>
  <nodeHealthy>true</nodeHealthy>
  <nodeManagerVersion>0.23.1-SNAPSHOT</nodeManagerVersion>
  <nodeManagerBuildVersion>0.23.1-SNAPSHOT from 1228355 by user1 source checksum 20647f76c36430e888cc7204826a445c</nodeManagerBuildVersion>
  <nodeManagerVersionBuiltOn>Mon Jan  9 15:01:59 UTC 2012</nodeManagerVersionBuiltOn>
  <hadoopVersion>0.23.1-SNAPSHOT</hadoopVersion>
  <hadoopBuildVersion>0.23.1-SNAPSHOT from 1228292 by user1 source checksum 3eba233f2248a089e9b28841a784dd00</hadoopBuildVersion>
  <hadoopVersionBuiltOn>Mon Jan  9 14:58:42 UTC 2012</hadoopVersionBuiltOn>
  <id>host.domain.com:8041</id>
  <nodeHostName>host.domain.com</nodeHostName>
</nodeInfo>
```

Applications API
----------------

With the Applications API, you can obtain a collection of resources, each of which represents an application. When you run a GET operation on this resource, you obtain a collection of Application Objects. See also [Application API](#Application_API) for syntax of the application object.

### URI

      * http://<nm http address:port>/ws/v1/node/apps

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple paramters can be specified.

      * state - application state 
      * user - user name

### Elements of the *apps* (Applications) object

When you make a request for the list of applications, the information will be returned as a collection of app objects. See also [Application API](#Application_API) for syntax of the app object.

| Properties | Data Type | Description |
|:---- |:---- |:---- |
| app | array of app objects(JSON)/zero or more app objects(XML) | A collection of application objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/apps

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "apps" : {
      "app" : [
         {
            "containerids" : [
               "container_1326121700862_0003_01_000001",
               "container_1326121700862_0003_01_000002"
            ],
            "user" : "user1",
            "id" : "application_1326121700862_0003",
            "state" : "RUNNING"
         },
         {
            "user" : "user1",
            "id" : "application_1326121700862_0002",
            "state" : "FINISHED"
         }
      ]
   }
}
```

**XML response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/apps
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 400
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<apps>
  <app>
    <id>application_1326121700862_0002</id>
    <state>FINISHED</state>
    <user>user1</user>
  </app>
  <app>
    <id>application_1326121700862_0003</id>
    <state>RUNNING</state>
    <user>user1</user>
    <containerids>container_1326121700862_0003_01_000002</containerids>
    <containerids>container_1326121700862_0003_01_000001</containerids>
  </app>
</apps>
```

Application API
---------------

An application resource contains information about a particular application that was run or is running on this NodeManager.

### URI

Use the following URI to obtain an app Object, for a application identified by the appid value.

      * http://<nm http address:port>/ws/v1/node/apps/{appid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *app* (Application) object

| Properties | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The application id |
| user | string | The user who started the application |
| state | string | The state of the application - valid states are: NEW, INITING, RUNNING, FINISHING\_CONTAINERS\_WAIT, APPLICATION\_RESOURCES\_CLEANINGUP, FINISHED |
| containerids | array of containerids(JSON)/zero or more containerids(XML) | The list of containerids currently being used by the application on this node. If not present then no containers are currently running for this application. |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/apps/application_1326121700862_0005

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "app" : {
      "containerids" : [
         "container_1326121700862_0005_01_000003",
         "container_1326121700862_0005_01_000001"
      ],
      "user" : "user1",
      "id" : "application_1326121700862_0005",
      "state" : "RUNNING"
   }
}
```

**XML response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/apps/application_1326121700862_0005
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 281 
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<app>
  <id>application_1326121700862_0005</id>
  <state>RUNNING</state>
  <user>user1</user>
  <containerids>container_1326121700862_0005_01_000003</containerids>
  <containerids>container_1326121700862_0005_01_000001</containerids>
</app>
```

Containers API
--------------

With the containers API, you can obtain a collection of resources, each of which represents a container. When you run a GET operation on this resource, you obtain a collection of Container Objects. See also [Container API](#Container_API) for syntax of the container object.

### URI

      * http://<nm http address:port>/ws/v1/node/containers

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *containers* object

When you make a request for the list of containers, the information will be returned as collection of container objects. See also [Container API](#Container_API) for syntax of the container object.

| Properties | Data Type | Description |
|:---- |:---- |:---- |
| containers | array of container objects(JSON)/zero or more container objects(XML) | A collection of container objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/containers

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "containers" : {
      "container" : [
         {
            "nodeId" : "host.domain.com:8041",
            "totalMemoryNeededMB" : 2048,
            "totalVCoresNeeded" : 1,
            "state" : "RUNNING",
            "diagnostics" : "",
            "containerLogsLink" : "http://host.domain.com:8042/node/containerlogs/container_1326121700862_0006_01_000001/user1",
            "user" : "user1",
            "id" : "container_1326121700862_0006_01_000001",
            "exitCode" : -1000
         },
         {
            "nodeId" : "host.domain.com:8041",
            "totalMemoryNeededMB" : 2048,
            "totalVCoresNeeded" : 2,
            "state" : "RUNNING",
            "diagnostics" : "",
            "containerLogsLink" : "http://host.domain.com:8042/node/containerlogs/container_1326121700862_0006_01_000003/user1",
            "user" : "user1",
            "id" : "container_1326121700862_0006_01_000003",
            "exitCode" : -1000
         }
      ]
   }
}
```

**XML response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/containers
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 988
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<containers>
  <container>
    <id>container_1326121700862_0006_01_000001</id>
    <state>RUNNING</state>
    <exitCode>-1000</exitCode>
    <diagnostics/>
    <user>user1</user>
    <totalMemoryNeededMB>2048</totalMemoryNeededMB>
    <totalVCoresNeeded>1</totalVCoresNeeded>
    <containerLogsLink>http://host.domain.com:8042/node/containerlogs/container_1326121700862_0006_01_000001/user1</containerLogsLink>
    <nodeId>host.domain.com:8041</nodeId>
  </container>
  <container>
    <id>container_1326121700862_0006_01_000003</id>
    <state>DONE</state>
    <exitCode>0</exitCode>
    <diagnostics>Container killed by the ApplicationMaster.</diagnostics>
    <user>user1</user>
    <totalMemoryNeededMB>2048</totalMemoryNeededMB>
    <totalVCoresNeeded>2</totalVCoresNeeded>
    <containerLogsLink>http://host.domain.com:8042/node/containerlogs/container_1326121700862_0006_01_000003/user1</containerLogsLink>
    <nodeId>host.domain.com:8041</nodeId>
  </container>
</containers>
```

Container API
-------------

A container resource contains information about a particular container that is running on this NodeManager.

### URI

Use the following URI to obtain a Container Object, from a container identified by the containerid value.

      * http://<nm http address:port>/ws/v1/node/containers/{containerid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *container* object

| Properties | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The container id |
| state | string | State of the container - valid states are: NEW, LOCALIZING, LOCALIZATION\_FAILED, LOCALIZED, RUNNING, EXITED\_WITH\_SUCCESS, EXITED\_WITH\_FAILURE, KILLING, CONTAINER\_CLEANEDUP\_AFTER\_KILL, CONTAINER\_RESOURCES\_CLEANINGUP, DONE |
| nodeId | string | The id of the node the container is on |
| containerLogsLink | string | The http link to the container logs |
| user | string | The user name of the user which started the container |
| exitCode | int | Exit code of the container |
| diagnostics | string | A diagnostic message for failed containers |
| totalMemoryNeededMB | long | Total amout of memory needed by the container (in MB) |
| totalVCoresNeeded | long | Total number of virtual cores needed by the container |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/nodes/containers/container_1326121700862_0007_01_000001

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "container" : {
      "nodeId" : "host.domain.com:8041",
      "totalMemoryNeededMB" : 2048,
      "totalVCoresNeeded" : 1,
      "state" : "RUNNING",
      "diagnostics" : "",
      "containerLogsLink" : "http://host.domain.com:8042/node/containerlogs/container_1326121700862_0007_01_000001/user1",
      "user" : "user1",
      "id" : "container_1326121700862_0007_01_000001",
      "exitCode" : -1000
   }
}
```

**XML response**

HTTP Request:

      GET http://<nm http address:port>/ws/v1/node/containers/container_1326121700862_0007_01_000001
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 491 
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<container>
  <id>container_1326121700862_0007_01_000001</id>
  <state>RUNNING</state>
  <exitCode>-1000</exitCode>
  <diagnostics/>
  <user>user1</user>
  <totalMemoryNeededMB>2048</totalMemoryNeededMB>
  <totalVCoresNeeded>1</totalVCoresNeeded>
  <containerLogsLink>http://host.domain.com:8042/node/containerlogs/container_1326121700862_0007_01_000001/user1</containerLogsLink>
  <nodeId>host.domain.com:8041</nodeId>
</container>
```
