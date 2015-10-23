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

ResourceManager REST API's.
===========================

* [Overview](#Overview)
* [Enabling CORS support](#Enabling_CORS_support)
* [Cluster Information API](#Cluster_Information_API)
* [Cluster Metrics API](#Cluster_Metrics_API)
* [Cluster Scheduler API](#Cluster_Scheduler_API)
* [Cluster Applications API](#Cluster_Applications_API)
* [Cluster Application Statistics API](#Cluster_Application_Statistics_API)
* [Cluster Application API](#Cluster_Application_API)
* [Cluster Application Attempts API](#Cluster_Application_Attempts_API)
* [Cluster Nodes API](#Cluster_Nodes_API)
* [Cluster Node API](#Cluster_Node_API)
* [Cluster Writeable APIs](#Cluster_Writeable_APIs)
* [Cluster New Application API](#Cluster_New_Application_API)
* [Cluster Applications API(Submit Application)](#Cluster_Applications_APISubmit_Application)
* [Cluster Application State API](#Cluster_Application_State_API)
* [Cluster Application Queue API](#Cluster_Application_Queue_API)
* [Cluster Delegation Tokens API](#Cluster_Delegation_Tokens_API)

Overview
--------

The ResourceManager REST API's allow the user to get information about the cluster - status on the cluster, metrics on the cluster, scheduler information, information about nodes in the cluster, and information about applications on the cluster.

Enabling CORS support
---------------------
To enable cross-origin support (CORS) for the RM only(without enabling it for the NM), please set the following configuration parameters:

In core-site.xml, add org.apache.hadoop.security.HttpCrossOriginFilterInitializer to hadoop.http.filter.initializers.
In yarn-site.xml, set yarn.resourcemanager.webapp.cross-origin.enabled to true.

Cluster Information API
-----------------------

The cluster information resource provides overall information about the cluster.

### URI

Both of the following URI's give you the cluster information.

      * http://<rm http address:port>/ws/v1/cluster
      * http://<rm http address:port>/ws/v1/cluster/info

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *clusterInfo* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | long | The cluster id |
| startedOn | long | The time the cluster started (in ms since epoch) |
| state | string | The ResourceManager state - valid values are: NOTINITED, INITED, STARTED, STOPPED |
| haState | string | The ResourceManager HA state - valid values are: INITIALIZING, ACTIVE, STANDBY, STOPPED |
| resourceManagerVersion | string | Version of the ResourceManager |
| resourceManagerBuildVersion | string | ResourceManager build string with build version, user, and checksum |
| resourceManagerVersionBuiltOn | string | Timestamp when ResourceManager was built (in ms since epoch) |
| hadoopVersion | string | Version of hadoop common |
| hadoopBuildVersion | string | Hadoop common build string with build version, user, and checksum |
| hadoopVersionBuiltOn | string | Timestamp when hadoop common was built(in ms since epoch) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "clusterInfo":
  {
    "id":1324053971963,
    "startedOn":1324053971963,
    "state":"STARTED",
    "resourceManagerVersion":"0.23.1-SNAPSHOT",
    "resourceManagerBuildVersion":"0.23.1-SNAPSHOT from 1214049 by user1 source checksum 050cd664439d931c8743a6428fd6a693",
    "resourceManagerVersionBuiltOn":"Tue Dec 13 22:12:48 CST 2011",
    "hadoopVersion":"0.23.1-SNAPSHOT",
    "hadoopBuildVersion":"0.23.1-SNAPSHOT from 1214049 by user1 source checksum 11458df3bb77342dca5f917198fad328",
    "hadoopVersionBuiltOn":"Tue Dec 13 22:12:26 CST 2011"
  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://<rm http address:port>/ws/v1/cluster/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 712
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<clusterInfo>
  <id>1324053971963</id>
  <startedOn>1324053971963</startedOn>
  <state>STARTED</state>
  <resourceManagerVersion>0.23.1-SNAPSHOT</resourceManagerVersion>
  <resourceManagerBuildVersion>0.23.1-SNAPSHOT from 1214049 by user1 source checksum 050cd664439d931c8743a6428fd6a693</resourceManagerBuildVersion>
  <resourceManagerVersionBuiltOn>Tue Dec 13 22:12:48 CST 2011</resourceManagerVersionBuiltOn>
  <hadoopVersion>0.23.1-SNAPSHOT</hadoopVersion>
  <hadoopBuildVersion>0.23.1-SNAPSHOT from 1214049 by user1 source checksum 11458df3bb77342dca5f917198fad328</hadoopBuildVersion>
  <hadoopVersionBuiltOn>Tue Dec 13 22:12:48 CST 2011</hadoopVersionBuiltOn>
</clusterInfo>
```

Cluster Metrics API
-------------------

The cluster metrics resource provides some overall metrics about the cluster. More detailed metrics should be retrieved from the jmx interface.

### URI

      * http://<rm http address:port>/ws/v1/cluster/metrics

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *clusterMetrics* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| appsSubmitted | int | The number of applications submitted |
| appsCompleted | int | The number of applications completed |
| appsPending | int | The number of applications pending |
| appsRunning | int | The number of applications running |
| appsFailed | int | The number of applications failed |
| appsKilled | int | The number of applications killed |
| reservedMB | long | The amount of memory reserved in MB |
| availableMB | long | The amount of memory available in MB |
| allocatedMB | long | The amount of memory allocated in MB |
| totalMB | long | The amount of total memory in MB |
| reservedVirtualCores | long | The number of reserved virtual cores |
| availableVirtualCores | long | The number of available virtual cores |
| allocatedVirtualCores | long | The number of allocated virtual cores |
| totalVirtualCores | long | The total number of virtual cores |
| containersAllocated | int | The number of containers allocated |
| containersReserved | int | The number of containers reserved |
| containersPending | int | The number of containers pending |
| totalNodes | int | The total number of nodes |
| activeNodes | int | The number of active nodes |
| lostNodes | int | The number of lost nodes |
| unhealthyNodes | int | The number of unhealthy nodes |
| decommissionedNodes | int | The number of nodes decommissioned |
| rebootedNodes | int | The number of nodes rebooted |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/metrics

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "clusterMetrics":
  {
    "appsSubmitted":0,
    "appsCompleted":0,
    "appsPending":0,
    "appsRunning":0,
    "appsFailed":0,
    "appsKilled":0,
    "reservedMB":0,
    "availableMB":17408,
    "allocatedMB":0,
    "reservedVirtualCores":0,
    "availableVirtualCores":7,
    "allocatedVirtualCores":1,
    "containersAllocated":0,
    "containersReserved":0,
    "containersPending":0,
    "totalMB":17408,
    "totalVirtualCores":8,
    "totalNodes":1,
    "lostNodes":0,
    "unhealthyNodes":0,
    "decommissionedNodes":0,
    "rebootedNodes":0,
    "activeNodes":1
  }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/metrics
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 432
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<clusterMetrics>
  <appsSubmitted>0</appsSubmitted>
  <appsCompleted>0</appsCompleted>
  <appsPending>0</appsPending>
  <appsRunning>0</appsRunning>
  <appsFailed>0</appsFailed>
  <appsKilled>0</appsKilled>
  <reservedMB>0</reservedMB>
  <availableMB>17408</availableMB>
  <allocatedMB>0</allocatedMB>
  <reservedVirtualCores>0</reservedVirtualCores>
  <availableVirtualCores>7</availableVirtualCores>
  <allocatedVirtualCores>1</allocatedVirtualCores>
  <containersAllocated>0</containersAllocated>
  <containersReserved>0</containersReserved>
  <containersPending>0</containersPending>
  <totalMB>17408</totalMB>
  <totalVirtualCores>8</totalVirtualCores>
  <totalNodes>1</totalNodes>
  <lostNodes>0</lostNodes>
  <unhealthyNodes>0</unhealthyNodes>
  <decommissionedNodes>0</decommissionedNodes>
  <rebootedNodes>0</rebootedNodes>
  <activeNodes>1</activeNodes>
</clusterMetrics>
```

Cluster Scheduler API
---------------------

A scheduler resource contains information about the current scheduler configured in a cluster. It currently supports both the Fifo and Capacity Scheduler. You will get different information depending on which scheduler is configured so be sure to look at the type information.

### URI

      * http://<rm http address:port>/ws/v1/cluster/scheduler

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Capacity Scheduler API

The capacity scheduler supports hierarchical queues. This one request will print information about all the queues and any subqueues they have. Queues that can actually have jobs submitted to them are referred to as leaf queues. These queues have additional data associated with them.

### Elements of the *schedulerInfo* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | string | Scheduler type - capacityScheduler |
| capacity | float | Configured queue capacity in percentage relative to its parent queue |
| usedCapacity | float | Used queue capacity in percentage |
| maxCapacity | float | Configured maximum queue capacity in percentage relative to its parent queue |
| queueName | string | Name of the queue |
| queues | array of queues(JSON)/zero or more queue objects(XML) | A collection of queue resources |

### Elements of the queues object for a Parent queue

| Item | Data Type | Description |
|:---- |:---- |:---- |
| capacity | float | Configured queue capacity in percentage relative to its parent queue |
| usedCapacity | float | Used queue capacity in percentage |
| maxCapacity | float | Configured maximum queue capacity in percentage relative to its parent queue |
| absoluteCapacity | float | Absolute capacity percentage this queue can use of entire cluster |
| absoluteMaxCapacity | float | Absolute maximum capacity percentage this queue can use of the entire cluster |
| absoluteUsedCapacity | float | Absolute used capacity percentage this queue is using of the entire cluster |
| numApplications | int | The number of applications currently in the queue |
| usedResources | string | A string describing the current resources used by the queue |
| queueName | string | The name of the queue |
| state | string of QueueState | The state of the queue |
| queues | array of queues(JSON)/zero or more queue objects(XML) | A collection of sub-queue information |
| resourcesUsed | A single resource object | The total amount of resources used by this queue |

### Elements of the queues object for a Leaf queue - contains all elements in parent plus the following:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | String | type of the queue - capacitySchedulerLeafQueueInfo |
| numActiveApplications | int | The number of active applications in this queue |
| numPendingApplications | int | The number of pending applications in this queue |
| numContainers | int | The number of containers being used |
| maxApplications | int | The maximum number of applications this queue can have |
| maxApplicationsPerUser | int | The maximum number of applications per user this queue can have |
| maxActiveApplications | int | The maximum number of active applications this queue can have |
| maxActiveApplicationsPerUser | int | The maximum number of active applications per user this queue can have |
| userLimit | int | The minimum user limit percent set in the configuration |
| userLimitFactor | float | The user limit factor set in the configuration |
| users | array of users(JSON)/zero or more user objects(XML) | A collection of user objects containing resources used |

### Elements of the user object for users:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| username | String | The username of the user using the resources |
| resourcesUsed | A single resource object | The amount of resources used by the user in this queue |
| numActiveApplications | int | The number of active applications for this user in this queue |
| numPendingApplications | int | The number of pending applications for this user in this queue |

### Elements of the resource object for resourcesUsed in user and queues:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | The amount of memory used (in MB) |
| vCores | int | The number of virtual cores |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/scheduler

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
    "scheduler": {
        "schedulerInfo": {
            "capacity": 100.0, 
            "maxCapacity": 100.0, 
            "queueName": "root", 
            "queues": {
                "queue": [
                    {
                        "absoluteCapacity": 10.5, 
                        "absoluteMaxCapacity": 50.0, 
                        "absoluteUsedCapacity": 0.0, 
                        "capacity": 10.5, 
                        "maxCapacity": 50.0, 
                        "numApplications": 0, 
                        "queueName": "a", 
                        "queues": {
                            "queue": [
                                {
                                    "absoluteCapacity": 3.15, 
                                    "absoluteMaxCapacity": 25.0, 
                                    "absoluteUsedCapacity": 0.0, 
                                    "capacity": 30.000002, 
                                    "maxCapacity": 50.0, 
                                    "numApplications": 0, 
                                    "queueName": "a1", 
                                    "queues": {
                                        "queue": [
                                            {
                                                "absoluteCapacity": 2.6775, 
                                                "absoluteMaxCapacity": 25.0, 
                                                "absoluteUsedCapacity": 0.0, 
                                                "capacity": 85.0, 
                                                "maxActiveApplications": 1, 
                                                "maxActiveApplicationsPerUser": 1, 
                                                "maxApplications": 267, 
                                                "maxApplicationsPerUser": 267, 
                                                "maxCapacity": 100.0, 
                                                "numActiveApplications": 0, 
                                                "numApplications": 0, 
                                                "numContainers": 0, 
                                                "numPendingApplications": 0, 
                                                "queueName": "a1a", 
                                                "resourcesUsed": {
                                                    "memory": 0, 
                                                    "vCores": 0
                                                }, 
                                                "state": "RUNNING", 
                                                "type": "capacitySchedulerLeafQueueInfo", 
                                                "usedCapacity": 0.0, 
                                                "usedResources": "<memory:0, vCores:0>", 
                                                "userLimit": 100, 
                                                "userLimitFactor": 1.0, 
                                                "users": null
                                            }, 
                                            {
                                                "absoluteCapacity": 0.47250003, 
                                                "absoluteMaxCapacity": 25.0, 
                                                "absoluteUsedCapacity": 0.0, 
                                                "capacity": 15.000001, 
                                                "maxActiveApplications": 1, 
                                                "maxActiveApplicationsPerUser": 1, 
                                                "maxApplications": 47, 
                                                "maxApplicationsPerUser": 47, 
                                                "maxCapacity": 100.0, 
                                                "numActiveApplications": 0, 
                                                "numApplications": 0, 
                                                "numContainers": 0, 
                                                "numPendingApplications": 0, 
                                                "queueName": "a1b", 
                                                "resourcesUsed": {
                                                    "memory": 0, 
                                                    "vCores": 0
                                                }, 
                                                "state": "RUNNING", 
                                                "type": "capacitySchedulerLeafQueueInfo", 
                                                "usedCapacity": 0.0, 
                                                "usedResources": "<memory:0, vCores:0>", 
                                                "userLimit": 100, 
                                                "userLimitFactor": 1.0, 
                                                "users": null
                                            }
                                        ]
                                    }, 
                                    "resourcesUsed": {
                                        "memory": 0, 
                                        "vCores": 0
                                    }, 
                                    "state": "RUNNING", 
                                    "usedCapacity": 0.0, 
                                    "usedResources": "<memory:0, vCores:0>"
                                }, 
                                {
                                    "absoluteCapacity": 7.35, 
                                    "absoluteMaxCapacity": 50.0, 
                                    "absoluteUsedCapacity": 0.0, 
                                    "capacity": 70.0, 
                                    "maxActiveApplications": 1, 
                                    "maxActiveApplicationsPerUser": 100, 
                                    "maxApplications": 735, 
                                    "maxApplicationsPerUser": 73500, 
                                    "maxCapacity": 100.0, 
                                    "numActiveApplications": 0, 
                                    "numApplications": 0, 
                                    "numContainers": 0, 
                                    "numPendingApplications": 0, 
                                    "queueName": "a2", 
                                    "resourcesUsed": {
                                        "memory": 0, 
                                        "vCores": 0
                                    }, 
                                    "state": "RUNNING", 
                                    "type": "capacitySchedulerLeafQueueInfo", 
                                    "usedCapacity": 0.0, 
                                    "usedResources": "<memory:0, vCores:0>", 
                                    "userLimit": 100, 
                                    "userLimitFactor": 100.0, 
                                    "users": null
                                }
                            ]
                        }, 
                        "resourcesUsed": {
                            "memory": 0, 
                            "vCores": 0
                        }, 
                        "state": "RUNNING", 
                        "usedCapacity": 0.0, 
                        "usedResources": "<memory:0, vCores:0>"
                    }, 
                    {
                        "absoluteCapacity": 89.5, 
                        "absoluteMaxCapacity": 100.0, 
                        "absoluteUsedCapacity": 0.0, 
                        "capacity": 89.5, 
                        "maxCapacity": 100.0, 
                        "numApplications": 2, 
                        "queueName": "b", 
                        "queues": {
                            "queue": [
                                {
                                    "absoluteCapacity": 53.7, 
                                    "absoluteMaxCapacity": 100.0, 
                                    "absoluteUsedCapacity": 0.0, 
                                    "capacity": 60.000004, 
                                    "maxActiveApplications": 1, 
                                    "maxActiveApplicationsPerUser": 100, 
                                    "maxApplications": 5370, 
                                    "maxApplicationsPerUser": 537000, 
                                    "maxCapacity": 100.0, 
                                    "numActiveApplications": 1, 
                                    "numApplications": 2, 
                                    "numContainers": 0, 
                                    "numPendingApplications": 1, 
                                    "queueName": "b1", 
                                    "resourcesUsed": {
                                        "memory": 0, 
                                        "vCores": 0
                                    }, 
                                    "state": "RUNNING", 
                                    "type": "capacitySchedulerLeafQueueInfo", 
                                    "usedCapacity": 0.0, 
                                    "usedResources": "<memory:0, vCores:0>", 
                                    "userLimit": 100, 
                                    "userLimitFactor": 100.0, 
                                    "users": {
                                        "user": [
                                            {
                                                "numActiveApplications": 0, 
                                                "numPendingApplications": 1, 
                                                "resourcesUsed": {
                                                    "memory": 0, 
                                                    "vCores": 0
                                                }, 
                                                "username": "user2"
                                            }, 
                                            {
                                                "numActiveApplications": 1, 
                                                "numPendingApplications": 0, 
                                                "resourcesUsed": {
                                                    "memory": 0, 
                                                    "vCores": 0
                                                }, 
                                                "username": "user1"
                                            }
                                        ]
                                    }
                                }, 
                                {
                                    "absoluteCapacity": 35.3525, 
                                    "absoluteMaxCapacity": 100.0, 
                                    "absoluteUsedCapacity": 0.0, 
                                    "capacity": 39.5, 
                                    "maxActiveApplications": 1, 
                                    "maxActiveApplicationsPerUser": 100, 
                                    "maxApplications": 3535, 
                                    "maxApplicationsPerUser": 353500, 
                                    "maxCapacity": 100.0, 
                                    "numActiveApplications": 0, 
                                    "numApplications": 0, 
                                    "numContainers": 0, 
                                    "numPendingApplications": 0, 
                                    "queueName": "b2", 
                                    "resourcesUsed": {
                                        "memory": 0, 
                                        "vCores": 0
                                    }, 
                                    "state": "RUNNING", 
                                    "type": "capacitySchedulerLeafQueueInfo", 
                                    "usedCapacity": 0.0, 
                                    "usedResources": "<memory:0, vCores:0>", 
                                    "userLimit": 100, 
                                    "userLimitFactor": 100.0, 
                                    "users": null
                                }, 
                                {
                                    "absoluteCapacity": 0.4475, 
                                    "absoluteMaxCapacity": 100.0, 
                                    "absoluteUsedCapacity": 0.0, 
                                    "capacity": 0.5, 
                                    "maxActiveApplications": 1, 
                                    "maxActiveApplicationsPerUser": 100, 
                                    "maxApplications": 44, 
                                    "maxApplicationsPerUser": 4400, 
                                    "maxCapacity": 100.0, 
                                    "numActiveApplications": 0, 
                                    "numApplications": 0, 
                                    "numContainers": 0, 
                                    "numPendingApplications": 0, 
                                    "queueName": "b3", 
                                    "resourcesUsed": {
                                        "memory": 0, 
                                        "vCores": 0
                                    }, 
                                    "state": "RUNNING", 
                                    "type": "capacitySchedulerLeafQueueInfo", 
                                    "usedCapacity": 0.0, 
                                    "usedResources": "<memory:0, vCores:0>", 
                                    "userLimit": 100, 
                                    "userLimitFactor": 100.0, 
                                    "users": null
                                }
                            ]
                        }, 
                        "resourcesUsed": {
                            "memory": 0, 
                            "vCores": 0
                        }, 
                        "state": "RUNNING", 
                        "usedCapacity": 0.0, 
                        "usedResources": "<memory:0, vCores:0>"
                    }
                ]
            }, 
            "type": "capacityScheduler", 
            "usedCapacity": 0.0
        }
    }
}
```json

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://<rm http address:port>/ws/v1/cluster/scheduler

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 5778
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<scheduler>
  <schedulerInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="capacityScheduler">
    <capacity>100.0</capacity>
    <usedCapacity>0.0</usedCapacity>
    <maxCapacity>100.0</maxCapacity>
    <queueName>root</queueName>
    <queues>
      <queue>
        <capacity>10.5</capacity>
        <usedCapacity>0.0</usedCapacity>
        <maxCapacity>50.0</maxCapacity>
        <absoluteCapacity>10.5</absoluteCapacity>
        <absoluteMaxCapacity>50.0</absoluteMaxCapacity>
        <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
        <numApplications>0</numApplications>
        <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
        <queueName>a</queueName>
        <state>RUNNING</state>
        <queues>
          <queue>
            <capacity>30.000002</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>50.0</maxCapacity>
            <absoluteCapacity>3.15</absoluteCapacity>
            <absoluteMaxCapacity>25.0</absoluteMaxCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <numApplications>0</numApplications>
            <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
            <queueName>a1</queueName>
            <state>RUNNING</state>
            <queues>
              <queue xsi:type="capacitySchedulerLeafQueueInfo">
                <capacity>85.0</capacity>
                <usedCapacity>0.0</usedCapacity>
                <maxCapacity>100.0</maxCapacity>
                <absoluteCapacity>2.6775</absoluteCapacity>
                <absoluteMaxCapacity>25.0</absoluteMaxCapacity>
                <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
                <numApplications>0</numApplications>
                <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
                <queueName>a1a</queueName>
                <state>RUNNING</state>
                <resourcesUsed>
                  <memory>0</memory>
                  <vCores>0</vCores>
                </resourcesUsed>
                <numActiveApplications>0</numActiveApplications>
                <numPendingApplications>0</numPendingApplications>
                <numContainers>0</numContainers>
                <maxApplications>267</maxApplications>
                <maxApplicationsPerUser>267</maxApplicationsPerUser>
                <maxActiveApplications>1</maxActiveApplications>
                <maxActiveApplicationsPerUser>1</maxActiveApplicationsPerUser>
                <userLimit>100</userLimit>
                <users/>
                <userLimitFactor>1.0</userLimitFactor>
              </queue>
              <queue xsi:type="capacitySchedulerLeafQueueInfo">
                <capacity>15.000001</capacity>
                <usedCapacity>0.0</usedCapacity>
                <maxCapacity>100.0</maxCapacity>
                <absoluteCapacity>0.47250003</absoluteCapacity>
                <absoluteMaxCapacity>25.0</absoluteMaxCapacity>
                <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
                <numApplications>0</numApplications>
                <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
                <queueName>a1b</queueName>
                <state>RUNNING</state>
                <resourcesUsed>
                  <memory>0</memory>
                  <vCores>0</vCores>
                </resourcesUsed>
                <numActiveApplications>0</numActiveApplications>
                <numPendingApplications>0</numPendingApplications>
                <numContainers>0</numContainers>
                <maxApplications>47</maxApplications>
                <maxApplicationsPerUser>47</maxApplicationsPerUser>
                <maxActiveApplications>1</maxActiveApplications>
                <maxActiveApplicationsPerUser>1</maxActiveApplicationsPerUser>
                <userLimit>100</userLimit>
                <users/>
                <userLimitFactor>1.0</userLimitFactor>
              </queue>
            </queues>
            <resourcesUsed>
              <memory>0</memory>
              <vCores>0</vCores>
            </resourcesUsed>
          </queue>
          <queue xsi:type="capacitySchedulerLeafQueueInfo">
            <capacity>70.0</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>100.0</maxCapacity>
            <absoluteCapacity>7.35</absoluteCapacity>
            <absoluteMaxCapacity>50.0</absoluteMaxCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <numApplications>0</numApplications>
            <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
            <queueName>a2</queueName>
            <state>RUNNING</state>
            <resourcesUsed>
              <memory>0</memory>
              <vCores>0</vCores>
            </resourcesUsed>
            <numActiveApplications>0</numActiveApplications>
            <numPendingApplications>0</numPendingApplications>
            <numContainers>0</numContainers>
            <maxApplications>735</maxApplications>
            <maxApplicationsPerUser>73500</maxApplicationsPerUser>
            <maxActiveApplications>1</maxActiveApplications>
            <maxActiveApplicationsPerUser>100</maxActiveApplicationsPerUser>
            <userLimit>100</userLimit>
            <users/>
            <userLimitFactor>100.0</userLimitFactor>
          </queue>
        </queues>
        <resourcesUsed>
          <memory>0</memory>
          <vCores>0</vCores>
        </resourcesUsed>
      </queue>
      <queue>
        <capacity>89.5</capacity>
        <usedCapacity>0.0</usedCapacity>
        <maxCapacity>100.0</maxCapacity>
        <absoluteCapacity>89.5</absoluteCapacity>
        <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
        <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
        <numApplications>2</numApplications>
        <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
        <queueName>b</queueName>
        <state>RUNNING</state>
        <queues>
          <queue xsi:type="capacitySchedulerLeafQueueInfo">
            <capacity>60.000004</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>100.0</maxCapacity>
            <absoluteCapacity>53.7</absoluteCapacity>
            <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <numApplications>2</numApplications>
            <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
            <queueName>b1</queueName>
            <state>RUNNING</state>
            <resourcesUsed>
              <memory>0</memory>
              <vCores>0</vCores>
            </resourcesUsed>
            <numActiveApplications>1</numActiveApplications>
            <numPendingApplications>1</numPendingApplications>
            <numContainers>0</numContainers>
            <maxApplications>5370</maxApplications>
            <maxApplicationsPerUser>537000</maxApplicationsPerUser>
            <maxActiveApplications>1</maxActiveApplications>
            <maxActiveApplicationsPerUser>100</maxActiveApplicationsPerUser>
            <userLimit>100</userLimit>
            <users>
              <user>
                <username>user2</username>
                <resourcesUsed>
                  <memory>0</memory>
                  <vCores>0</vCores>
                </resourcesUsed>
                <numPendingApplications>1</numPendingApplications>
                <numActiveApplications>0</numActiveApplications>
              </user>
              <user>
                <username>user1</username>
                <resourcesUsed>
                  <memory>0</memory>
                  <vCores>0</vCores>
                </resourcesUsed>
                <numPendingApplications>0</numPendingApplications>
                <numActiveApplications>1</numActiveApplications>
              </user>
            </users>
            <userLimitFactor>100.0</userLimitFactor>
          </queue>
          <queue xsi:type="capacitySchedulerLeafQueueInfo">
            <capacity>39.5</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>100.0</maxCapacity>
            <absoluteCapacity>35.3525</absoluteCapacity>
            <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <numApplications>0</numApplications>
            <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
            <queueName>b2</queueName>
            <state>RUNNING</state>
            <resourcesUsed>
              <memory>0</memory>
              <vCores>0</vCores>
            </resourcesUsed>
            <numActiveApplications>0</numActiveApplications>
            <numPendingApplications>0</numPendingApplications>
            <numContainers>0</numContainers>
            <maxApplications>3535</maxApplications>
            <maxApplicationsPerUser>353500</maxApplicationsPerUser>
            <maxActiveApplications>1</maxActiveApplications>
            <maxActiveApplicationsPerUser>100</maxActiveApplicationsPerUser>
            <userLimit>100</userLimit>
            <users/>
            <userLimitFactor>100.0</userLimitFactor>
          </queue>
          <queue xsi:type="capacitySchedulerLeafQueueInfo">
            <capacity>0.5</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>100.0</maxCapacity>
            <absoluteCapacity>0.4475</absoluteCapacity>
            <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <numApplications>0</numApplications>
            <usedResources>&lt;memory:0, vCores:0&gt;</usedResources>
            <queueName>b3</queueName>
            <state>RUNNING</state>
            <resourcesUsed>
              <memory>0</memory>
              <vCores>0</vCores>
            </resourcesUsed>
            <numActiveApplications>0</numActiveApplications>
            <numPendingApplications>0</numPendingApplications>
            <numContainers>0</numContainers>
            <maxApplications>44</maxApplications>
            <maxApplicationsPerUser>4400</maxApplicationsPerUser>
            <maxActiveApplications>1</maxActiveApplications>
            <maxActiveApplicationsPerUser>100</maxActiveApplicationsPerUser>
            <userLimit>100</userLimit>
            <users/>
            <userLimitFactor>100.0</userLimitFactor>
          </queue>
        </queues>
        <resourcesUsed>
          <memory>0</memory>
          <vCores>0</vCores>
        </resourcesUsed>
      </queue>
    </queues>
  </schedulerInfo>
</scheduler>
```

### Fifo Scheduler API

### Elements of the *schedulerInfo* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | string | Scheduler type - fifoScheduler |
| capacity | float | Queue capacity in percentage |
| usedCapacity | float | Used queue capacity in percentage |
| qstate | string | State of the queue - valid values are: STOPPED, RUNNING |
| minQueueMemoryCapacity | int | Minimum queue memory capacity |
| maxQueueMemoryCapacity | int | Maximum queue memory capacity |
| numNodes | int | The total number of nodes |
| usedNodeCapacity | int | The used node capacity |
| availNodeCapacity | int | The available node capacity |
| totalNodeCapacity | int | The total node capacity |
| numContainers | int | The number of containers |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/scheduler

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "scheduler":
  {
    "schedulerInfo":
    {
      "type":"fifoScheduler",
      "capacity":1,
      "usedCapacity":"NaN",
      "qstate":"RUNNING",
      "minQueueMemoryCapacity":1024,
      "maxQueueMemoryCapacity":10240,
      "numNodes":0,
      "usedNodeCapacity":0,
      "availNodeCapacity":0,
      "totalNodeCapacity":0,
      "numContainers":0
    }
  }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/scheduler
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 432
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<scheduler>
  <schedulerInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="fifoScheduler">
    <capacity>1.0</capacity>
    <usedCapacity>NaN</usedCapacity>
    <qstate>RUNNING</qstate>
    <minQueueMemoryCapacity>1024</minQueueMemoryCapacity>
    <maxQueueMemoryCapacity>10240</maxQueueMemoryCapacity>
    <numNodes>0</numNodes>
    <usedNodeCapacity>0</usedNodeCapacity>
    <availNodeCapacity>0</availNodeCapacity>
    <totalNodeCapacity>0</totalNodeCapacity>
    <numContainers>0</numContainers>
  </schedulerInfo>
</scheduler>
```

Cluster Applications API
------------------------

With the Applications API, you can obtain a collection of resources, each of which represents an application. When you run a GET operation on this resource, you obtain a collection of Application Objects.

### URI

      * http://<rm http address:port>/ws/v1/cluster/apps

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple parameters can be specified for GET operations. The started and finished times have a begin and end parameter to allow you to specify ranges. For example, one could request all applications that started between 1:00am and 2:00pm on 12/19/2011 with startedTimeBegin=1324256400&startedTimeEnd=1324303200. If the Begin parameter is not specified, it defaults to 0, and if the End parameter is not specified, it defaults to infinity.

      * state [deprecated] - state of the application
      * states - applications matching the given application states, specified as a comma-separated list.
      * finalStatus - the final status of the application - reported by the application itself
      * user - user name
      * queue - queue name
      * limit - total number of app objects to be returned
      * startedTimeBegin - applications with start time beginning with this time, specified in ms since epoch
      * startedTimeEnd - applications with start time ending with this time, specified in ms since epoch
      * finishedTimeBegin - applications with finish time beginning with this time, specified in ms since epoch
      * finishedTimeEnd - applications with finish time ending with this time, specified in ms since epoch
      * applicationTypes - applications matching the given application types, specified as a comma-separated list.
      * applicationTags - applications matching any of the given application tags, specified as a comma-separated list.

### Elements of the *apps* (Applications) object

When you make a request for the list of applications, the information will be returned as a collection of app objects. See also [Application API](#Application_API) for syntax of the app object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| app | array of app objects(JSON)/zero or more application objects(XML) | The collection of application objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/apps

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "apps":
  {
    "app":
    [
       {
          "finishedTime" : 1326815598530,
          "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326815542473_0001_01_000001",
          "trackingUI" : "History",
          "state" : "FINISHED",
          "user" : "user1",
          "id" : "application_1326815542473_0001",
          "clusterId" : 1326815542473,
          "finalStatus" : "SUCCEEDED",
          "amHostHttpAddress" : "host.domain.com:8042",
          "progress" : 100,
          "name" : "word count",
          "startedTime" : 1326815573334,
          "elapsedTime" : 25196,
          "diagnostics" : "",
          "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473_1_1",
          "queue" : "default",
          "allocatedMB" : 0,
          "allocatedVCores" : 0,
          "runningContainers" : 0,
          "memorySeconds" : 151730,
          "vcoreSeconds" : 103
       },
       {
          "finishedTime" : 1326815789546,
          "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326815542473_0002_01_000001",
          "trackingUI" : "History",
          "state" : "FINISHED",
          "user" : "user1",
          "id" : "application_1326815542473_0002",
          "clusterId" : 1326815542473,
          "finalStatus" : "SUCCEEDED",
          "amHostHttpAddress" : "host.domain.com:8042",
          "progress" : 100,
          "name" : "Sleep job",
          "startedTime" : 1326815641380,
          "elapsedTime" : 148166,
          "diagnostics" : "",
          "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326815542473_0002/jobhistory/job/job_1326815542473_2_2",
          "queue" : "default",
          "allocatedMB" : 0,
          "allocatedVCores" : 0,
          "runningContainers" : 1,
          "memorySeconds" : 640064,
          "vcoreSeconds" : 442
       } 
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/apps
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2459
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<apps>
  <app>
    <id>application_1326815542473_0001</id>
    <user>user1</user>
    <name>word count</name>
    <applicationType>MAPREDUCE</applicationType>
    <queue>default</queue>
    <state>FINISHED</state>
    <finalStatus>SUCCEEDED</finalStatus>
    <progress>100.0</progress>
    <trackingUI>History</trackingUI>
    <trackingUrl>http://host.domain.com:8088/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473_1_1</trackingUrl>
    <diagnostics/>
    <clusterId>1326815542473</clusterId>
    <startedTime>1326815573334</startedTime>
    <finishedTime>1326815598530</finishedTime>
    <elapsedTime>25196</elapsedTime>
    <amContainerLogs>http://host.domain.com:8042/node/containerlogs/container_1326815542473_0001_01_000001</amContainerLogs>
    <amHostHttpAddress>host.domain.com:8042</amHostHttpAddress>
    <allocatedMB>0</allocatedMB>
    <allocatedVCores>0</allocatedVCores>
    <runningContainers>0</runningContainers>
    <memorySeconds>151730</memorySeconds>
    <vcoreSeconds>103</vcoreSeconds>
  </app>
  <app>
    <id>application_1326815542473_0002</id>
    <user>user1</user>
    <name>Sleep job</name>
    <applicationType>YARN</applicationType>
    <queue>default</queue>
    <state>FINISHED</state>
    <finalStatus>SUCCEEDED</finalStatus>
    <progress>100.0</progress>
    <trackingUI>History</trackingUI>
    <trackingUrl>http://host.domain.com:8088/proxy/application_1326815542473_0002/jobhistory/job/job_1326815542473_2_2</trackingUrl>
    <diagnostics/>
    <clusterId>1326815542473</clusterId>
    <startedTime>1326815641380</startedTime>
    <finishedTime>1326815789546</finishedTime>
    <elapsedTime>148166</elapsedTime>
    <amContainerLogs>http://host.domain.com:8042/node/containerlogs/container_1326815542473_0002_01_000001</amContainerLogs>
    <amHostHttpAddress>host.domain.com:8042</amHostHttpAddress>
    <allocatedMB>0</allocatedMB>
    <allocatedVCores>0</allocatedVCores>
    <runningContainers>0</runningContainers>
    <memorySeconds>640064</memorySeconds>
    <vcoreSeconds>442</vcoreSeconds>
  </app>
</apps>
```

Cluster Application Statistics API
----------------------------------

With the Application Statistics API, you can obtain a collection of triples, each of which contains the application type, the application state and the number of applications of this type and this state in ResourceManager context. Note that with the performance concern, we currently only support at most one applicationType per query. We may support multiple applicationTypes per query as well as more statistics in the future. When you run a GET operation on this resource, you obtain a collection of statItem objects.

### URI

      * http://<rm http address:port>/ws/v1/cluster/appstatistics

### HTTP Operations Supported

      * GET

### Query Parameters Required

Two paramters can be specified. The parameters are case insensitive.

      * states - states of the applications, specified as a comma-separated list. If states is not provided, the API will enumerate all application states and return the counts of them.
      * applicationTypes - types of the applications, specified as a comma-separated list. If applicationTypes is not provided, the API will count the applications of any application type. In this case, the response shows * to indicate any application type. Note that we only support at most one applicationType temporarily. Otherwise, users will expect an BadRequestException.

### Elements of the *appStatInfo* (statItems) object

When you make a request for the list of statistics items, the information will be returned as a collection of statItem objects

| Item | Data Type | Description |
|:---- |:---- |:---- |
| statItem | array of statItem objects(JSON)/zero or more statItem objects(XML) | The collection of statItem objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/appstatistics?states=accepted,running,finished&applicationTypes=mapreduce

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "appStatInfo":
  {
    "statItem":
    [
       {
          "state" : "accepted",
          "type" : "mapreduce",
          "count" : 4
       },
       {
          "state" : "running",
          "type" : "mapreduce",
          "count" : 1
       },
       {
          "state" : "finished",
          "type" : "mapreduce",
          "count" : 7
       }
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/appstatistics?states=accepted,running,finished&applicationTypes=mapreduce
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2459
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<appStatInfo>
  <statItem>
    <state>accepted</state>
    <type>mapreduce</type>
    <count>4</count>
  </statItem>
  <statItem>
    <state>running</state>
    <type>mapreduce</type>
    <count>1</count>
  </statItem>
  <statItem>
    <state>finished</state>
    <type>mapreduce</type>
    <count>7</count>
  </statItem>
</appStatInfo>
```

Cluster Application API
-----------------------

An application resource contains information about a particular application that was submitted to a cluster.

### URI

Use the following URI to obtain an app object, from a application identified by the appid value.

      * http://<rm http address:port>/ws/v1/cluster/apps/{appid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *app* (Application) object

Note that depending on security settings a user might not be able to see all the fields.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The application id |
| user | string | The user who started the application |
| name | string | The application name |
| Application Type | string | The application type |
| queue | string | The queue the application was submitted to |
| state | string | The application state according to the ResourceManager - valid values are members of the YarnApplicationState enum: NEW, NEW\_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED |
| finalStatus | string | The final status of the application if finished - reported by the application itself - valid values are: UNDEFINED, SUCCEEDED, FAILED, KILLED |
| progress | float | The progress of the application as a percent |
| trackingUI | string | Where the tracking url is currently pointing - History (for history server) or ApplicationMaster |
| trackingUrl | string | The web URL that can be used to track the application |
| diagnostics | string | Detailed diagnostics information |
| clusterId | long | The cluster id |
| startedTime | long | The time in which application started (in ms since epoch) |
| finishedTime | long | The time in which the application finished (in ms since epoch) |
| elapsedTime | long | The elapsed time since the application started (in ms) |
| amContainerLogs | string | The URL of the application master container logs |
| amHostHttpAddress | string | The nodes http address of the application master |
| allocatedMB | int | The sum of memory in MB allocated to the application's running containers |
| allocatedVCores | int | The sum of virtual cores allocated to the application's running containers |
| runningContainers | int | The number of containers currently running for the application |
| memorySeconds | long | The amount of memory the application has allocated (megabyte-seconds) |
| vcoreSeconds | long | The amount of CPU resources the application has allocated (virtual core-seconds) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1326821518301_0005

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "app" : {
      "finishedTime" : 1326824991300,
      "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001",
      "trackingUI" : "History",
      "state" : "FINISHED",
      "user" : "user1",
      "id" : "application_1326821518301_0005",
      "clusterId" : 1326821518301,
      "finalStatus" : "SUCCEEDED",
      "amHostHttpAddress" : "host.domain.com:8042",
      "progress" : 100,
      "name" : "Sleep job",
      "applicationType" : "Yarn",
      "startedTime" : 1326824544552,
      "elapsedTime" : 446748,
      "diagnostics" : "",
      "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326821518301_0005/jobhistory/job/job_1326821518301_5_5",
      "queue" : "a1",
      "memorySeconds" : 151730,
      "vcoreSeconds" : 103
   }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1326821518301_0005
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 847
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<app>
  <id>application_1326821518301_0005</id>
  <user>user1</user>
  <name>Sleep job</name>
  <queue>a1</queue>
  <state>FINISHED</state>
  <finalStatus>SUCCEEDED</finalStatus>
  <progress>100.0</progress>
  <trackingUI>History</trackingUI>
  <trackingUrl>http://host.domain.com:8088/proxy/application_1326821518301_0005/jobhistory/job/job_1326821518301_5_5</trackingUrl>
  <diagnostics/>
  <clusterId>1326821518301</clusterId>
  <startedTime>1326824544552</startedTime>
  <finishedTime>1326824991300</finishedTime>
  <elapsedTime>446748</elapsedTime>
  <amContainerLogs>http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001</amContainerLogs>
  <amHostHttpAddress>host.domain.com:8042</amHostHttpAddress>
  <memorySeconds>151730</memorySeconds>
  <vcoreSeconds>103</vcoreSeconds>
</app>
```

Cluster Application Attempts API
--------------------------------

With the application attempts API, you can obtain a collection of resources that represent an application attempt. When you run a GET operation on this resource, you obtain a collection of App Attempt Objects.

### URI

      * http://<rm http address:port>/ws/v1/cluster/apps/{appid}/appattempts

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *appAttempts* object

When you make a request for the list of app attempts, the information will be returned as an array of app attempt objects.

appAttempts:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| appAttempt | array of app attempt objects(JSON)/zero or more app attempt objects(XML) | The collection of app attempt objects |

### Elements of the *appAttempt* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The app attempt id |
| nodeId | string | The node id of the node the attempt ran on |
| nodeHttpAddress | string | The node http address of the node the attempt ran on |
| logsLink | string | The http link to the app attempt logs |
| containerId | string | The id of the container for the app attempt |
| startTime | long | The start time of the attempt (in ms since epoch) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1326821518301_0005/appattempts

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "appAttempts" : {
      "appAttempt" : [
         {
            "nodeId" : "host.domain.com:8041",
            "nodeHttpAddress" : "host.domain.com:8042",
            "startTime" : 1326381444693,
            "id" : 1,
            "logsLink" : "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001/user1",
            "containerId" : "container_1326821518301_0005_01_000001"
         }
      ]
   }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1326821518301_0005/appattempts
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 575
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<appAttempts>
  <appttempt>
    <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
    <nodeId>host.domain.com:8041</nodeId>
    <id>1</id>
    <startTime>1326381444693</startTime>
    <containerId>container_1326821518301_0005_01_000001</containerId>
    <logsLink>http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001/user1</logsLink>
  </appAttempt>
</appAttempts>
```

Cluster Nodes API
-----------------

With the Nodes API, you can obtain a collection of resources, each of which represents a node. When you run a GET operation on this resource, you obtain a collection of Node Objects.

### URI

      * http://<rm http address:port>/ws/v1/cluster/nodes

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      * state - the state of the node
      * healthy - true or false 

### Elements of the *nodes* object

When you make a request for the list of nodes, the information will be returned as a collection of node objects. See also [Node API](#Node_API) for syntax of the node object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| node | array of node objects(JSON)/zero or more node objects(XML) | A collection of node objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/nodes

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "nodes":
  {
    "node":
    [
      {
        "rack":"\/default-rack",
        "state":"NEW",
        "id":"h2:1235",
        "nodeHostName":"h2",
        "nodeHTTPAddress":"h2:2",
        "healthStatus":"Healthy",
        "lastHealthUpdate":1324056895432,
        "healthReport":"Healthy",
        "numContainers":0,
        "usedMemoryMB":0,
        "availMemoryMB":8192,
        "usedVirtualCores":0,
        "availableVirtualCores":8
      },
      {
        "rack":"\/default-rack",
        "state":"NEW",
        "id":"h1:1234",
        "nodeHostName":"h1",
        "nodeHTTPAddress":"h1:2",
        "healthStatus":"Healthy",
        "lastHealthUpdate":1324056895092,
        "healthReport":"Healthy",
        "numContainers":0,
        "usedMemoryMB":0,
        "availMemoryMB":8192,
        "usedVirtualCores":0,
        "availableVirtualCores":8
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/nodes
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 1104
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<nodes>
  <node>
    <rack>/default-rack</rack>
    <state>RUNNING</state>
    <id>h2:1234</id>
    <nodeHostName>h2</nodeHostName>
    <nodeHTTPAddress>h2:2</nodeHTTPAddress>
    <healthStatus>Healthy</healthStatus>
    <lastHealthUpdate>1324333268447</lastHealthUpdate>
    <healthReport>Healthy</healthReport>
    <numContainers>0</numContainers>
    <usedMemoryMB>0</usedMemoryMB>
    <availMemoryMB>5120</availMemoryMB>
    <usedVirtualCores>0</usedVirtualCores>
    <availableVirtualCores>8</availableVirtualCores>
  </node>
  <node>
    <rack>/default-rack</rack>
    <state>RUNNING</state>
    <id>h1:1234</id>
    <nodeHostName>h1</nodeHostName>
    <nodeHTTPAddress>h1:2</nodeHTTPAddress>
    <healthStatus>Healthy</healthStatus>
    <lastHealthUpdate>1324333268447</lastHealthUpdate>
    <healthReport>Healthy</healthReport>
    <numContainers>0</numContainers>
    <usedMemoryMB>0</usedMemoryMB>
    <availMemoryMB>5120</availMemoryMB>
    <usedVirtualCores>0</usedVirtualCores>
    <availableVirtualCores>8</availableVirtualCores>
  </node>
</nodes>
```

Cluster Node API
----------------

A node resource contains information about a node in the cluster.

### URI

Use the following URI to obtain a Node Object, from a node identified by the nodeid value.

      * http://<rm http address:port>/ws/v1/cluster/nodes/{nodeid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *node* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| rack | string | The rack location of this node |
| state | string | State of the node - valid values are: NEW, RUNNING, UNHEALTHY, DECOMMISSIONED, LOST, REBOOTED |
| id | string | The node id |
| nodeHostName | string | The host name of the node |
| nodeHTTPAddress | string | The nodes HTTP address |
| healthStatus | string | The health status of the node - Healthy or Unhealthy |
| healthReport | string | A detailed health report |
| lastHealthUpdate | long | The last time the node reported its health (in ms since epoch) |
| usedMemoryMB | long | The total amount of memory currently used on the node (in MB) |
| availMemoryMB | long | The total amount of memory currently available on the node (in MB) |
| usedVirtualCores | long | The total number of vCores currently used on the node |
| availableVirtualCores | long | The total number of vCores available on the node |
| numContainers | int | The total number of containers currently running on the node |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/nodes/h2:1235

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "node":
  {
    "rack":"\/default-rack",
    "state":"NEW",
    "id":"h2:1235",
    "nodeHostName":"h2",
    "nodeHTTPAddress":"h2:2",
    "healthStatus":"Healthy",
    "lastHealthUpdate":1324056895432,
    "healthReport":"Healthy",
    "numContainers":0,
    "usedMemoryMB":0,
    "availMemoryMB":5120,
    "usedVirtualCores":0,
    "availableVirtualCores":8
  }
}
```

**XML response**

HTTP Request:

      GET http://<rm http address:port>/ws/v1/cluster/node/h2:1235
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 552
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<node>
  <rack>/default-rack</rack>
  <state>NEW</state>
  <id>h2:1235</id>
  <nodeHostName>h2</nodeHostName>
  <nodeHTTPAddress>h2:2</nodeHTTPAddress>
  <healthStatus>Healthy</healthStatus>
  <lastHealthUpdate>1324333268447</lastHealthUpdate>
  <healthReport>Healthy</healthReport>
  <numContainers>0</numContainers>
  <usedMemoryMB>0</usedMemoryMB>
  <availMemoryMB>5120</availMemoryMB>
  <usedVirtualCores>0</usedVirtualCores>
  <availableVirtualCores>5120</availableVirtualCores>
</node>
```

Cluster Writeable APIs
----------------------

The setions below refer to APIs which allow to create and modify applications. These APIs are currently in alpha and may change in the future.

Cluster New Application API
---------------------------

With the New Application API, you can obtain an application-id which can then be used as part of the [Cluster Submit Applications API](#Cluster_Applications_APISubmit_Application) to submit applications. The response also includes the maximum resource capabilities available on the cluster.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://<rm http address:port>/ws/v1/cluster/apps/new-application

### HTTP Operations Supported

      * POST

### Query Parameters Supported

      None

### Elements of the NewApplication object

The NewApplication response contains the following elements:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| application-id | string | The newly created application id |
| maximum-resource-capabilities | object | The maximum resource capabilities available on this cluster |

The *maximum-resource-capabilites* object contains the following elements:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | The maxiumim memory available for a container |
| vCores | int | The maximum number of cores available for a container |

### Response Examples

**JSON response**

HTTP Request:

      POST http://<rm http address:port>/ws/v1/cluster/apps/new-application

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "application-id":"application_1404198295326_0003",
  "maximum-resource-capability":
    {
      "memory":8192,
      "vCores":32
    }
}
```

**XML response**

HTTP Request:

      POST http://<rm http address:port>/ws/v1/cluster/apps/new-application

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 248
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<NewApplication>
  <application-id>application_1404198295326_0003</application-id>
  <maximum-resource-capability>
    <memory>8192</memory>
    <vCores>32</vCores>
  </maximum-resource-capability>
</NewApplication>
```

Cluster Applications API(Submit Application)
--------------------------------------------

The Submit Applications API can be used to submit applications. In case of submitting applications, you must first obtain an application-id using the [Cluster New Application API](#Cluster_New_Application_API). The application-id must be part of the request body. The response contains a URL to the application page which can be used to track the state and progress of your application.

### URI

      * http://<rm http address:port>/ws/v1/cluster/apps

### HTTP Operations Supported

      * POST

### POST Response Examples

POST requests can be used to submit apps to the ResourceManager. As mentioned above, an application-id must be obtained first. Successful submissions result in a 202 response code and a Location header specifying where to get information about the app. Please note that in order to submit an app, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

Please note that this feature is currently in the alpha stage and may change in the future.

#### Elements of the POST request object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| application-id | string | The application id |
| application-name | string | The application name |
| queue | string | The name of the queue to which the application should be submitted |
| priority | int | The priority of the application |
| am-container-spec | object | The application master container launch context, described below |
| unmanaged-AM | boolean | Is the application using an unmanaged application master |
| max-app-attempts | int | The max number of attempts for this application |
| resource | object | The resources the application master requires, described below |
| application-type | string | The application type(MapReduce, Pig, Hive, etc) |
| keep-containers-across-application-attempts | boolean | Should YARN keep the containers used by this application instead of destroying them |
| application-tags | object | List of application tags, please see the request examples on how to speciy the tags |

Elements of the *am-container-spec* object

The am-container-spec object should be used to provide the container launch context for the application master.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| local-resources | object | Object describing the resources that need to be localized, described below |
| environment | object | Environment variables for your containers, specified as key value pairs |
| commands | object | The commands for launching your container, in the order in which they should be executed |
| service-data | object | Application specific service data; key is the name of the auxiliary servce, value is base-64 encoding of the data you wish to pass |
| credentials | object | The credentials required for your application to run, described below |
| application-acls | objec | ACLs for your application; the key can be "VIEW\_APP" or "MODIFY\_APP", the value is the list of users with the permissions |

Elements of the *local-resources* object

The object is a collection of key-value pairs. They key is an identifier for the resources to be localized and the value is the details of the resource. The elements of the value are described below:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| resource | string | Location of the resource to be localized |
| type | string | Type of the resource; options are "ARCHIVE", "FILE", and "PATTERN" |
| visibility | string | Visibility the resource to be localized; options are "PUBLIC", "PRIVATE", and "APPLICATION" |
| size | long | Size of the resource to be localized |
| timestamp | long | Timestamp of the resource to be localized |

Elements of the *credentials* object

The credentials object should be used to pass data required for the application to authenticate itself such as delegation-tokens and secrets.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| tokens | object | Tokens that you wish to pass to your application, specified as key-value pairs. The key is an identifier for the token and the value is the token(which should be obtained using the respective web-services) |
| secrets | object | Secrets that you wish to use in your application, specified as key-value pairs. They key is an identifier and the value is the base-64 encoding of the secret |

Elements of the POST request body *resource* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | Memory required for each container |
| vCores | int | Virtual cores required for each container |

**JSON response**

HTTP Request:

```json
  POST http://<rm http address:port>/ws/v1/cluster/apps
  Accept: application/json
  Content-Type: application/json
  {
    "application-id":"application_1404203615263_0001",
    "application-name":"test",
    "am-container-spec":
    {
      "local-resources":
      {
        "entry":
        [
          {
            "key":"AppMaster.jar",
            "value":
            {
              "resource":"hdfs://hdfs-namenode:9000/user/testuser/DistributedShell/demo-app/AppMaster.jar",
              "type":"FILE",
              "visibility":"APPLICATION",
              "size": 43004,
              "timestamp": 1405452071209
            }
          }
        ]
      },
      "commands":
      {
        "command":"{{JAVA_HOME}}/bin/java -Xmx10m org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster --container_memory 10 --container_vcores 1 --num_containers 1 --priority 0 1><LOG_DIR>/AppMaster.stdout 2><LOG_DIR>/AppMaster.stderr"
      },
      "environment":
      {
        "entry":
        [
          {
            "key": "DISTRIBUTEDSHELLSCRIPTTIMESTAMP",
            "value": "1405459400754"
          },
          {
            "key": "CLASSPATH",
            "value": "{{CLASSPATH}}<CPS>./*<CPS>{{HADOOP_CONF_DIR}}<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/*<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*<CPS>./log4j.properties"
          },
          {
            "key": "DISTRIBUTEDSHELLSCRIPTLEN",
            "value": "6"
          },
          {
            "key": "DISTRIBUTEDSHELLSCRIPTLOCATION",
            "value": "hdfs://hdfs-namenode:9000/user/testuser/demo-app/shellCommands"
          }
        ]
      }
    },
    "unmanaged-AM":false,
    "max-app-attempts":2,
    "resource":
    {
      "memory":1024,
      "vCores":1
    },
    "application-type":"YARN",
    "keep-containers-across-application-attempts":false
  }
```

Response Header:

      HTTP/1.1 202
      Transfer-Encoding: chunked
      Location: http://<rm http address:port>/ws/v1/cluster/apps/application_1404203615263_0001
      Content-Type: application/json
      Server: Jetty(6.1.26)

Response Body:

      No response body

**XML response**

HTTP Request:

```xml
POST http://<rm http address:port>/ws/v1/cluster/apps
Accept: application/xml
Content-Type: application/xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<application-submission-context>
  <application-id>application_1404204891930_0002</application-id>
  <application-name>test</application-name>
  <queue>testqueue</queue>
  <priority>3</priority>
  <am-container-spec>
    <local-resources>
      <entry>
        <key>example</key>
        <value>
          <resource>hdfs://hdfs-namenode:9000/user/testuser/DistributedShell/demo-app/AppMaster.jar</resource>
          <type>FILE</type>
          <visibility>APPLICATION</visibility>
          <size>43004</size>
          <timestamp>1405452071209</timestamp>
        </value>
      </entry>
    </local-resources>
    <environment>
      <entry>
        <key>DISTRIBUTEDSHELLSCRIPTTIMESTAMP</key>
        <value>1405459400754</value>
      </entry>
      <entry>
        <key>CLASSPATH</key>
        <value>{{CLASSPATH}}&lt;CPS&gt;./*&lt;CPS&gt;{{HADOOP_CONF_DIR}}&lt;CPS&gt;{{HADOOP_COMMON_HOME}}/share/hadoop/common/*&lt;CPS&gt;{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*&lt;CPS&gt;{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*&lt;CPS&gt;{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*&lt;CPS&gt;{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*&lt;CPS&gt;{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*&lt;CPS&gt;./log4j.properties</value>
      </entry>
      <entry>
        <key>DISTRIBUTEDSHELLSCRIPTLEN</key>
        <value>6</value>
      </entry>
      <entry>
        <key>DISTRIBUTEDSHELLSCRIPTLOCATION</key>
        <value>hdfs://hdfs-namenode:9000/user/testuser/demo-app/shellCommands</value>
      </entry>
    </environment>
    <commands>
      <command>{{JAVA_HOME}}/bin/java -Xmx10m org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster --container_memory 10 --container_vcores 1 --num_containers 1 --priority 0 1&gt;&lt;LOG_DIR&gt;/AppMaster.stdout 2&gt;&lt;LOG_DIR&gt;/AppMaster.stderr</command>
    </commands>
    <service-data>
      <entry>
        <key>test</key>
        <value>dmFsdWUxMg</value>
      </entry>
    </service-data>
    <credentials>
      <tokens/>
      <secrets>
        <entry>
          <key>secret1</key>
          <value>c2VjcmV0MQ</value>
        </entry>
      </secrets>
    </credentials>
    <application-acls>
      <entry>
        <key>VIEW_APP</key>
        <value>testuser3, testuser4</value>
      </entry>
      <entry>
        <key>MODIFY_APP</key>
        <value>testuser1, testuser2</value>
      </entry>
    </application-acls>
  </am-container-spec>
  <unmanaged-AM>false</unmanaged-AM>
  <max-app-attempts>2</max-app-attempts>
  <resource>
    <memory>1024</memory>
    <vCores>1</vCores>
  </resource>
  <application-type>YARN</application-type>
  <keep-containers-across-application-attempts>false</keep-containers-across-application-attempts>
  <application-tags>
    <tag>tag 2</tag>
    <tag>tag1</tag>
  </application-tags>
</application-submission-context>
```

Response Header:

      HTTP/1.1 202
      Transfer-Encoding: chunked
      Location: http://<rm http address:port>/ws/v1/cluster/apps/application_1404204891930_0002
      Content-Type: application/xml
      Server: Jetty(6.1.26)

Response Body:

      No response body

Cluster Application State API
-----------------------------

With the application state API, you can query the state of a submitted app as well kill a running app by modifying the state of a running app using a PUT request with the state set to "KILLED". To perform the PUT operation, authentication has to be setup for the RM web services. In addition, you must be authorized to kill the app. Currently you can only change the state to "KILLED"; an attempt to change the state to any other results in a 400 error response. Examples of the unauthorized and bad request errors are below. When you carry out a successful PUT, the iniital response may be a 202. You can confirm that the app is killed by repeating the PUT request until you get a 200, querying the state using the GET method or querying for app information and checking the state. In the examples below, we repeat the PUT request and get a 200 response.

Please note that in order to kill an app, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://<rm http address:port>/ws/v1/cluster/apps/{appid}/state

### HTTP Operations Supported

      * GET
      * PUT

### Query Parameters Supported

      None

### Elements of *appstate* object

When you make a request for the state of an app, the information returned has the following fields

| Item | Data Type | Description |
|:---- |:---- |:---- |
| state | string | The application state - can be one of "NEW", "NEW\_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING", "FINISHED", "FAILED", "KILLED" |

### Response Examples

**JSON responses**

HTTP Request

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Transfer-Encoding: chunked
    Server: Jetty(6.1.26)

Response Body:

    {
      "state":"ACCEPTED"
    }

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    {
      "state":"KILLED"
    }

Response Header:

    HTTP/1.1 202 Accepted
    Content-Type: application/json
    Transfer-Encoding: chunked
    Location: http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003
    Server: Jetty(6.1.26)

Response Body:

    {
      "state":"ACCEPTED"
    }

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    {
      "state":"KILLED"
    }

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Transfer-Encoding: chunked
    Server: Jetty(6.1.26)

Response Body:

    {
      "state":"KILLED"
    }

**XML responses**

HTTP Request

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 99
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>ACCEPTED</state>
    </appstate>

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>KILLED</state>
    </appstate>

Response Header:

    HTTP/1.1 202 Accepted
    Content-Type: application/xml
    Content-Length: 794
    Location: http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>ACCEPTED</state>
    </appstate>

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>KILLED</state>
    </appstate>

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 917
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>KILLED</state>
    </appstate>

**Unauthorized Error Response**

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>KILLED</state>
    </appstate>

Response Header:

    HTTP/1.1 403 Unauthorized
    Server: Jetty(6.1.26)

**Bad Request Error Response**

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>RUNNING</state>
    </appstate>

Response Header:

    HTTP/1.1 400
    Content-Length: 295
    Content-Type: application/xml
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <RemoteException>
      <exception>BadRequestException</exception>
      <message>java.lang.Exception: Only 'KILLED' is allowed as a target state.</message>
      <javaClassName>org.apache.hadoop.yarn.webapp.BadRequestException</javaClassName>
    </RemoteException>

Cluster Application Queue API
-----------------------------

With the application queue API, you can query the queue of a submitted app as well move a running app to another queue using a PUT request specifying the target queue. To perform the PUT operation, authentication has to be setup for the RM web services. In addition, you must be authorized to move the app. Currently you can only move the app if you're using the Capacity scheduler or the Fair scheduler.

Please note that in order to move an app, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://<rm http address:port>/ws/v1/cluster/apps/{appid}/queue

### HTTP Operations Supported

      * GET
      * PUT

### Query Parameters Supported

      None

### Elements of *appqueue* object

When you make a request for the state of an app, the information returned has the following fields

| Item | Data Type | Description |
|:---- |:---- |:---- |
| queue | string | The application queue |

### Response Examples

**JSON responses**

HTTP Request

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/queue

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Transfer-Encoding: chunked
    Server: Jetty(6.1.26)

Response Body:

    {
      "queue":"default"
    }

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/queue

Request Body:

    {
      "queue":"test"
    }

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Transfer-Encoding: chunked
    Server: Jetty(6.1.26)

Response Body:

    {
      "queue":"test"
    }

**XML responses**

HTTP Request

      GET http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/queue

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 98
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appqueue>
      <queue>default</queue>
    </appqueue>

HTTP Request

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/queue

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appqueue>
      <queue>test</queue>
    </appqueue>

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 95
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appqueue>
      <queue>test</queue>
    </appqueue>

Cluster Delegation Tokens API
-----------------------------

The Delegation Tokens API can be used to create, renew and cancel YARN ResourceManager delegation tokens. All delegation token requests must be carried out on a Kerberos authenticated connection(using SPNEGO). Carrying out operations on a non-kerberos connection will result in a FORBIDDEN response. In case of renewing a token, only the renewer specified when creating the token can renew the token. Other users(including the owner) are forbidden from renewing tokens. It should be noted that when cancelling or renewing a token, the token to be cancelled or renewed is specified by setting a header.

This feature is currently in the alpha stage and may change in the future.

### URI

Use the following URI to create and cancel delegation tokens.

      * http://<rm http address:port>/ws/v1/cluster/delegation-token

Use the following URI to renew delegation tokens.

      * http://<rm http address:port>/ws/v1/cluster/delegation-token/expiration

### HTTP Operations Supported

      * POST
      * DELETE

### Query Parameters Supported

      None

### Elements of the *delegation-token* object

The response from the delegation tokens API contains one of the fields listed below.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| token | string | The delegation token |
| renewer | string | The user who is allowed to renew the delegation token |
| owner | string | The owner of the delegation token |
| kind | string | The kind of delegation token |
| expiration-time | long | The expiration time of the token |
| max-validity | long | The maximum validity of the token |

### Response Examples

#### Creating a token

**JSON response**

HTTP Request:

      POST http://<rm http address:port>/ws/v1/cluster/delegation-token
      Accept: application/json
      Content-Type: application/json
      {
        "renewer" : "test-renewer"
      }

Response Header

      HTTP/1.1 200 OK
      WWW-Authenticate: Negotiate ...
      Date: Sat, 28 Jun 2014 18:08:11 GMT
      Server: Jetty(6.1.26)
      Set-Cookie: ...
      Content-Type: application/json

Response body

```json
  {
    "token":"MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUckiEZpigFHSJTKaQECFN9EMM9BzfPoDxu572EVUpzqhnSGE1JNX0RFTEVHQVRJT05fVE9LRU4A",
    "renewer":"test-renewer",
    "owner":"client@EXAMPLE.COM",
    "kind":"RM_DELEGATION_TOKEN",
    "expiration-time":1405153616489,
    "max-validity":1405672016489
  }
```

**XML response**

HTTP Request

      POST http://<rm http address:port>/ws/v1/cluster/delegation-token
      Accept: application/xml
      Content-Type: application/xml
      <delegation-token>
        <renewer>test-renewer</renewer>
      </delegation-token>

Response Header

      HTTP/1.1 200 OK
      WWW-Authenticate: Negotiate ...
      Date: Sat, 28 Jun 2014 18:08:11 GMT
      Content-Length: 423
      Server: Jetty(6.1.26)
      Set-Cookie: ...
      Content-Type: application/xml

Response Body

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<delegation-token>  <token>MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUckgZ8yigFHSI4jMgcCFDTG8X6XFFn2udQngzSXQL8vWaKIE1JNX0RFTEVHQVRJT05fVE9LRU4A</token>
  <renewer>test-renewer</renewer>
  <owner>client@EXAMPLE.COM</owner>
  <kind>RM_DELEGATION_TOKEN</kind>
  <expiration-time>1405153180466</expiration-time>
  <max-validity>1405671580466</max-validity>
</delegation-token>
```

#### Renewing a token

**JSON response**

HTTP Request:

      POST http://<rm http address:port>/ws/v1/cluster/delegation-token/expiration
      Accept: application/json
      Hadoop-YARN-RM-Delegation-Token: MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUbjqcHHigFHB7ZFxwQCFKWD3znCkDSy6SQIjRCLDydxbxvgE1JNX0RFTEVHQVRJT05fVE9LRU4A
      Content-Type: application/json

Response Header

      HTTP/1.1 200 OK
      WWW-Authenticate: Negotiate ...
      Date: Sat, 28 Jun 2014 18:08:11 GMT
      Server: Jetty(6.1.26)
      Set-Cookie: ...
      Content-Type: application/json

Response body

      {
        "expiration-time":1404112520402
      }

**XML response**

HTTP Request

      POST http://<rm http address:port>/ws/v1/cluster/delegation-token/expiration
      Accept: application/xml
      Content-Type: application/xml
      Hadoop-YARN-RM-Delegation-Token: MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUbjqcHHigFHB7ZFxwQCFKWD3znCkDSy6SQIjRCLDydxbxvgE1JNX0RFTEVHQVRJT05fVE9LRU4A

Response Header

      HTTP/1.1 200 OK
      WWW-Authenticate: Negotiate ...
      Date: Sat, 28 Jun 2014 18:08:11 GMT
      Content-Length: 423
      Server: Jetty(6.1.26)
      Set-Cookie: ...
      Content-Type: application/xml

Response Body

      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <delegation-token>
        <expiration-time>1404112520402</expiration-time>
      </delegation-token>

#### Cancelling a token

HTTP Request

    DELETE http://<rm http address:port>/ws/v1/cluster/delegation-token
    Hadoop-YARN-RM-Delegation-Token: MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUbjqcHHigFHB7ZFxwQCFKWD3znCkDSy6SQIjRCLDydxbxvgE1JNX0RFTEVHQVRJT05fVE9LRU4A
    Accept: application/xml

Response Header

      HTTP/1.1 200 OK
      WWW-Authenticate: Negotiate ...
      Date: Sun, 29 Jun 2014 07:25:18 GMT
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)
      Set-Cookie: ...
      Content-Type: application/xml

No response body.

### Authentication using delegation tokens

This feature is in the alpha mode and may change in the future.

You can use delegation tokens to authenticate yourself when using YARN RM webservices. However, this requires setting the right configurations. The conditions for this are:

* Hadoop is setup in secure mode with the authentication type set to kerberos.

* Hadoop HTTP authentication is setup with the authentication type set to kerberos

Once setup, delegation tokens can be fetched using the web services listed above and used as shown in an example below:

      PUT http://<rm http address:port>/ws/v1/cluster/apps/application_1399397633663_0003/state
      X-Hadoop-Delegation-Token: MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUbjqcHHigFHB7ZFxwQCFKWD3znCkDSy6SQIjRCLDydxbxvgE1JNX0RFTEVHQVRJT05fVE9LRU4A
      Content-Type: application/json; charset=UTF8
      {
        "state":"KILLED"
      }
