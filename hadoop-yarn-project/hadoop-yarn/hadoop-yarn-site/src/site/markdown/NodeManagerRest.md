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

<!-- MACRO{toc|fromDepth=0|toDepth=1} -->

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

      * http://nm-http-address:port/ws/v1/node
      * http://nm-http-address:port/ws/v1/node/info

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
| vmemCheckEnabled | boolean | Whether virtual memory checking is enabled for preemption |
| pmemCheckEnabled | boolean | Whether physical memory checking is enabled for preemption |
| lastNodeUpdateTime | long | The last timestamp at which the health report was received (in ms since epoch) |
| nmStartupTime | long | The timestamp at which the node was started (in ms since epoch) |
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

      GET http://nm-http-address:port/ws/v1/node/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "nodeInfo": {
    "healthReport": "",
    "totalVmemAllocatedContainersMB": 17203,
    "totalPmemAllocatedContainersMB": 8192,
    "totalVCoresAllocatedContainers": 8,
    "vmemCheckEnabled": false,
    "pmemCheckEnabled": true,
    "lastNodeUpdateTime": 1485814574224,
    "nodeHealthy": true,
    "nodeManagerVersion": "3.0.0",
    "nodeManagerBuildVersion": "3.0.0",
    "nodeManagerVersionBuiltOn": "2017-01-30T17:42Z",
    "hadoopVersion": "3.0.0",
    "hadoopBuildVersion": "3.0.0",
    "hadoopVersionBuiltOn": "2017-01-30T17:39Z",
    "id": "host.domain.com:46077",
    "nodeHostName": "host.domain.com",
    "nmStartupTime": 1485800887841
  }
}

```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://nm-http-address:port/ws/v1/node/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 983
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<nodeInfo>
    <healthReport></healthReport>
    <totalVmemAllocatedContainersMB>17203</totalVmemAllocatedContainersMB>
    <totalPmemAllocatedContainersMB>8192</totalPmemAllocatedContainersMB>
    <totalVCoresAllocatedContainers>8</totalVCoresAllocatedContainers>
    <vmemCheckEnabled>false</vmemCheckEnabled>
    <pmemCheckEnabled>true</pmemCheckEnabled>
    <lastNodeUpdateTime>1485815774203</lastNodeUpdateTime>
    <nodeHealthy>true</nodeHealthy>
    <nodeManagerVersion>3.0.0</nodeManagerVersion>
    <nodeManagerBuildVersion>3.0.0</nodeManagerBuildVersion>
    <nodeManagerVersionBuiltOn>2017-01-30T17:42Z</nodeManagerVersionBuiltOn>
    <hadoopVersion>3.0.0</hadoopVersion>
    <hadoopBuildVersion>3.0.0</hadoopBuildVersion>
    <hadoopVersionBuiltOn>2017-01-30T17:39Z</hadoopVersionBuiltOn>
    <id>host.domain.com:46077</id>
    <nodeHostName>host.domain.com</nodeHostName>
    <nmStartupTime>1485800887841</nmStartupTime>
</nodeInfo>
```

Applications API
----------------

With the Applications API, you can obtain a collection of resources, each of which represents an application. When you run a GET operation on this resource, you obtain a collection of Application Objects. See also [Application API](#Application_API) for syntax of the application object.

### URI

      * http://nm-http-address:port/ws/v1/node/apps

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple parameters can be specified.

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

      GET http://nm-http-address:port/ws/v1/node/apps

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

      GET http://nm-http-address:port/ws/v1/node/apps
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

      * http://nm-http-address:port/ws/v1/node/apps/{appid}

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

      GET http://nm-http-address:port/ws/v1/node/apps/application_1326121700862_0005

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

      GET http://nm-http-address:port/ws/v1/node/apps/application_1326121700862_0005
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

      * http://nm-http-address:port/ws/v1/node/containers

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

      GET http://nm-http-address:port/ws/v1/node/containers

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
            "exitCode" : -1000,
            "executionType": "GUARANTEED",
            "containerLogFiles": [
              "stdout",
              "stderr",
              "syslog"
            ]
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
            "exitCode" : -1000,
            "executionType": "GUARANTEED",
            "containerLogFiles": [
              "stdout",
              "stderr",
              "syslog"
            ]
         }
      ]
   }
}
```

**XML response**

HTTP Request:

      GET http://nm-http-address:port/ws/v1/node/containers
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
    <executionType>GUARANTEED</executionType>
    <containerLogFiles>stdout</containerLogFiles>
    <containerLogFiles>stderr</containerLogFiles>
    <containerLogFiles>syslog</containerLogFiles>
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
    <executionType>GUARANTEED</executionType>
    <containerLogFiles>stdout</containerLogFiles>
    <containerLogFiles>stderr</containerLogFiles>
    <containerLogFiles>syslog</containerLogFiles>
  </container>
</containers>
```

Container API
-------------

A container resource contains information about a particular container that is running on this NodeManager.

### URI

Use the following URI to obtain a Container Object, from a container identified by the containerid value.

      * http://nm-http-address:port/ws/v1/node/containers/{containerid}

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
| executionType | string | Container type of GUARANTEED or OPPORTUNISTIC |
| containerLogFiles | array of strings | Container log file names |

### Response Examples

**JSON response**

HTTP Request:

      GET http://nm-http-address:port/ws/v1/node/containers/container_1326121700862_0007_01_000001

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
      "exitCode" : -1000,
      "executionType": "GUARANTEED",
      "containerLogFiles": [
        "stdout",
        "stderr",
        "syslog"
      ]
   }
}
```

**XML response**

HTTP Request:

      GET http://nm-http-address:port/ws/v1/node/containers/container_1326121700862_0007_01_000001
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
  <executionType>GUARANTEED</executionType>
  <containerLogFiles>stdout</containerLogFiles>
  <containerLogFiles>stderr</containerLogFiles>
  <containerLogFiles>syslog</containerLogFiles>
</container>
```

Auxiliary Services API
----------------------

With the auxiliary services API, you can obtain a collection of resources, each of which represents an auxiliary service. When you run a GET operation on this resource, you obtain a collection of auxiliary service information objects.

A YARN admin can use a PUT operation to update the auxiliary services running on the NodeManager. The body of the request should be of the same format as an auxiliary services manifest file.

### URI

      * http://nm-http-address:port/ws/v1/node/auxiliaryservices

### HTTP Operations Supported

 * GET
 * PUT

### Query Parameters Supported

      None

### Elements of the *auxiliaryservices* object

When you make a request for the list of auxiliary services, the information will be returned as collection of service information objects.

| Properties | Data Type | Description |
|:---- |:---- |:---- |
| services | array of service information objects(JSON)/zero or more service information objects (XML) | A collection of service information objects |

### GET Response Examples

**JSON response**

HTTP Request:

      GET http://nm-http-address:port/ws/v1/node/auxiliaryservices

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
    "services": {
        "service": [
            {
                "name": "timeline_collector",
                "startTime": "2018-12-18 21:24:27",
                "version": "1"
            },
            {
                "name": "mapreduce_shuffle",
                "startTime": "2018-12-18 21:24:27",
                "version": "2"
            }
        ]
    }
}
```

**XML response**

HTTP Request:

      GET http://nm-http-address:port/ws/v1/node/auxiliaryservices
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 299
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<services>
  <service>
    <name>timeline_collector</name>
    <version>1</version>
    <startTime>2018-12-18 21:00:00</startTime>
  </service>
  <service>
    <name>mapreduce_shuffle</name>
    <version>2</version>
    <startTime>2018-12-18 21:00:00</startTime>
  </service>
</services>
```
