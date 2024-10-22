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

<!-- MACRO{toc|fromDepth=0|toDepth=1} -->

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

      * http://rm-http-address:port/ws/v1/cluster
      * http://rm-http-address:port/ws/v1/cluster/info

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
| rmStateStoreName | string | Fully qualified name of class that implements the storage of ResourceManager state |
| resourceManagerVersion | string | Version of the ResourceManager |
| resourceManagerBuildVersion | string | ResourceManager build string with build version, user, and checksum |
| resourceManagerVersionBuiltOn | string | Timestamp when ResourceManager was built (in ms since epoch) |
| hadoopVersion | string | Version of hadoop common |
| hadoopBuildVersion | string | Hadoop common build string with build version, user, and checksum |
| hadoopVersionBuiltOn | string | Timestamp when hadoop common was built(in ms since epoch) |
| haZooKeeperConnectionState | string | State of ZooKeeper connection of the high availability service |

### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/info

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
    "haState":"ACTIVE",
    "rmStateStoreName":"org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore",
    "resourceManagerVersion":"3.0.0-SNAPSHOT",
    "resourceManagerBuildVersion":"3.0.0-SNAPSHOT from unknown by user1 source checksum 11111111111111111111111111111111",
    "resourceManagerVersionBuiltOn":"2016-01-01T01:00Z",
    "hadoopVersion":"3.0.0-SNAPSHOT",
    "hadoopBuildVersion":"3.0.0-SNAPSHOT from unknown by user1 source checksum 11111111111111111111111111111111",
    "hadoopVersionBuiltOn":"2016-01-01T01:00Z",
    "haZooKeeperConnectionState": "ResourceManager HA is not enabled."  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 712
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<clusterInfo>
  <id>1476912658570</id>
  <startedOn>1476912658570</startedOn>
  <state>STARTED</state>
  <haState>ACTIVE</haState>
  <rmStateStoreName>org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore</rmStateStoreName>
  <resourceManagerVersion>3.0.0-SNAPSHOT</resourceManagerVersion>
  <resourceManagerBuildVersion>3.0.0-SNAPSHOT from unknown by user1 source checksum 11111111111111111111111111111111</resourceManagerBuildVersion>
  <resourceManagerVersionBuiltOn>2016-01-01T01:00Z</resourceManagerVersionBuiltOn>
  <hadoopVersion>3.0.0-SNAPSHOT</hadoopVersion>
  <hadoopBuildVersion>3.0.0-SNAPSHOT from unknown by user1 source checksum 11111111111111111111111111111111</hadoopBuildVersion>
  <hadoopVersionBuiltOn>2016-01-01T01:00Z</hadoopVersionBuiltOn>
  <haZooKeeperConnectionState>ResourceManager HA is not enabled.</haZooKeeperConnectionState>
</clusterInfo>
```

Cluster Metrics API
-------------------

The cluster metrics resource provides some overall metrics about the cluster. More detailed metrics should be retrieved from the jmx interface.

### URI

      * http://rm-http-address:port/ws/v1/cluster/metrics

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
| decommissioningNodes | int | The number of nodes being decommissioned |
| decommissionedNodes | int | The number of nodes decommissioned |
| rebootedNodes | int | The number of nodes rebooted |
| shutdownNodes | int | The number of nodes shut down |

### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/metrics

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
    "decommissioningNodes":0,
    "decommissionedNodes":0,
    "rebootedNodes":0,
    "activeNodes":1,
    "shutdownNodes":0
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/metrics
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
  <decommissioningNodes>0</decommissioningNodes>
  <decommissionedNodes>0</decommissionedNodes>
  <rebootedNodes>0</rebootedNodes>
  <activeNodes>1</activeNodes>
  <shutdownNodes>0</shutdownNodes>
</clusterMetrics>
```

Cluster Scheduler API
---------------------

A scheduler resource contains information about the current scheduler configured in a cluster. It currently supports the Fifo, Capacity and Fair Scheduler. You will get different information depending on which scheduler is configured so be sure to look at the type information.

### URI

      * http://rm-http-address:port/ws/v1/cluster/scheduler

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
| capacity | float | **Configured** queue capacity in percentage relative to its parent queue in legacy-queue-mode, otherwise the **effective** capacity  |
| usedCapacity | float | Used queue capacity in percentage |
| maxCapacity | float | **Configured** maximum queue capacity in percentage relative to its parent queue in legacy-queue-mode, otherwise the **effective** max capacity |
| queueName | string | Name of the queue |
| queues | array of queues(JSON)/zero or more queue objects(XML) | A collection of queue resources |
| health | A single health object | The health metrics of capacity scheduler. This metrics existed since 2.8.0, but the output was not well formatted. Hence users can not make use of this field cleanly, this is optimized from 3.2.0 onwards. |
| weight | float | The configured weight of the queue |
| normalizedWeight | float | The normalized weight by siblings: `[0.0, 1.0]` in legacy-queue-mode, otherwise 0 |

### Elements of the queues object for a Parent queue

| Item | Data Type | Description |
|:---- |:---- |:---- |
| capacity | float | **Configured** queue capacity in percentage relative to its parent queue in legacy-queue-mode, otherwise the **effective** capacity  |
| usedCapacity | float | Used queue capacity in percentage |
| maxCapacity | float | **Configured** maximum queue capacity in percentage relative to its parent queue in legacy-queue-mode, otherwise the **effective** max capacity |
| absoluteCapacity | float | Absolute capacity percentage this queue can use of entire cluster |
| absoluteMaxCapacity | float | Absolute maximum capacity percentage this queue can use of the entire cluster |
| absoluteUsedCapacity | float | Absolute used capacity percentage this queue is using of the entire cluster |
| numApplications | int | The number of applications currently in the queue |
| maxParallelApps | int | Maximum number of applications that can run at the same time |
| usedResources | string | A string describing the current resources used by the queue |
| queueName | string | The name of the queue |
| state | string of QueueState | The state of the queue |
| queues | array of queues(JSON)/zero or more queue objects(XML) | A collection of sub-queue information. Omitted if the queue has no sub-queues. |
| resourcesUsed | A single resource object | The total amount of resources used by this queue |
| mode | string | The configured capacity mode: percentage, absolute, weight, mixed |
| weight | float | The configured weight of the queue |
| normalizedWeight | float | The normalized weight by the queue siblings: `[0.0, 1.0]` in legacy-queue-mode, otherwise 0 |

### Elements of the queues object for a Leaf queue - contains all the elements in parent except 'queues' plus the following:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | String | type of the queue - capacitySchedulerLeafQueueInfo |
| numActiveApplications | int | The number of active applications in this queue |
| numPendingApplications | int | The number of pending applications in this queue |
| numContainers | int | The number of containers being used |
| allocatedContainers | int | The number of allocated containers in this queue |
| reservedContainers | int | The number of reserved containers in this queue |
| pendingContainers | int | The number of pending containers in this queue |
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
| memory | long | The amount of memory used (in MB) |
| vCores | int | The number of virtual cores |

### Elements of the health object in schedulerInfo:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| lastrun | long | The time in which application started (in ms since epoch) |
| operationsInfo | array of operations(JSON)/operation objects(XML) | A collection of operation objects |
| lastRunDetails | array of lastRunDetails(JSON)/lastRunDetail objects(XML) | A collection of lastRunDetail objects |

### Elements of the operation object in health:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| operation | string | The type of operation |
| nodeId | string | The id of the node to which the operation relates |
| containerId | string | The id of the container to which the operation relates |
| queue | string | The name of the queue to which the operation relates |

### Elements of the lastRunDetail object in health:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| operation | string | The type of operation  |
| count | long | The id of the node to which the operation relates |
| resources | A single resource object | The resources to which the operation relates |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/scheduler

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "scheduler" : {
    "schedulerInfo" : {
      "type" : "capacityScheduler",
      "capacity" : 100,
      "usedCapacity" : 0,
      "maxCapacity" : 100,
      "weight" : -1,
      "normalizedWeight" : 0,
      "queueCapacityVectorInfo" : {
        "configuredCapacityVector" : "[memory-mb=100.0%,vcores=100.0%]",
        "capacityVectorEntries" : [ {
          "resourceName" : "memory-mb",
          "resourceValue" : "100.0%"
        }, {
          "resourceName" : "vcores",
          "resourceValue" : "100.0%"
        } ]
      },
      "queueName" : "root",
      "queuePath" : "root",
      "maxParallelApps" : 2147483647,
      "isAbsoluteResource" : false,
      "queues" : {
        "queue" : [ {
          "type" : "capacitySchedulerLeafQueueInfo",
          "queuePath" : "root.a",
          "capacity" : 12.5,
          "usedCapacity" : 0,
          "maxCapacity" : 50,
          "absoluteCapacity" : 12.5,
          "absoluteMaxCapacity" : 50,
          "absoluteUsedCapacity" : 0,
          "weight" : -1,
          "normalizedWeight" : 0,
          "numApplications" : 0,
          "maxParallelApps" : 2147483647,
          "queueName" : "a",
          "isAbsoluteResource" : false,
          "state" : "RUNNING",
          "resourcesUsed" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "hideReservationQueues" : false,
          "nodeLabels" : [ "*" ],
          "allocatedContainers" : 0,
          "reservedContainers" : 0,
          "pendingContainers" : 0,
          "capacities" : {
            "queueCapacitiesByPartition" : [ {
              "partitionName" : "",
              "queueCapacityVectorInfo" : {
                "configuredCapacityVector" : "[memory-mb=12.5%,vcores=12.5%]",
                "capacityVectorEntries" : [ {
                  "resourceName" : "memory-mb",
                  "resourceValue" : "12.5%"
                }, {
                  "resourceName" : "vcores",
                  "resourceValue" : "12.5%"
                } ]
              },
              "capacity" : 12.5,
              "usedCapacity" : 0,
              "maxCapacity" : 50,
              "absoluteCapacity" : 12.5,
              "absoluteUsedCapacity" : 0,
              "absoluteMaxCapacity" : 50,
              "maxAMLimitPercentage" : 10,
              "weight" : -1,
              "normalizedWeight" : 0,
              "configuredMinResource" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 8192,
                    "minimumAllocation" : 1024,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 4,
                    "minimumAllocation" : 1,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "configuredMaxResource" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 8192,
                    "minimumAllocation" : 1024,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 4,
                    "minimumAllocation" : 1,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "effectiveMinResource" : {
                "memory" : 4096,
                "vCores" : 4,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 4096
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 4
                  } ]
                }
              },
              "effectiveMaxResource" : {
                "memory" : 16384,
                "vCores" : 16,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 16384
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 16
                  } ]
                }
              }
            } ]
          },
          "resources" : {
            "resourceUsagesByPartition" : [ {
              "partitionName" : "",
              "used" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "reserved" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "pending" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "amUsed" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "amLimit" : {
                "memory" : 2048,
                "vCores" : 1,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 2048
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 1
                  } ]
                }
              },
              "userAmLimit" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              }
            } ]
          },
          "minEffectiveCapacity" : {
            "memory" : 4096,
            "vCores" : 4,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 4096
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 4
              } ]
            }
          },
          "maxEffectiveCapacity" : {
            "memory" : 16384,
            "vCores" : 16,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 16384
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 16
              } ]
            }
          },
          "maximumAllocation" : {
            "memory" : 8192,
            "vCores" : 4,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 8192
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 4
              } ]
            }
          },
          "queueAcls" : {
            "queueAcl" : [ {
              "accessType" : "ADMINISTER_QUEUE",
              "accessControlList" : " "
            }, {
              "accessType" : "APPLICATION_MAX_PRIORITY",
              "accessControlList" : "*"
            }, {
              "accessType" : "SUBMIT_APP",
              "accessControlList" : " "
            } ]
          },
          "queuePriority" : 0,
          "orderingPolicyInfo" : "fifo",
          "autoCreateChildQueueEnabled" : false,
          "leafQueueTemplate" : { },
          "mode" : "percentage",
          "queueType" : "leaf",
          "creationMethod" : "static",
          "autoCreationEligibility" : "off",
          "autoQueueTemplateProperties" : { },
          "autoQueueParentTemplateProperties" : { },
          "autoQueueLeafTemplateProperties" : { },
          "numActiveApplications" : 0,
          "numPendingApplications" : 0,
          "numContainers" : 0,
          "maxApplications" : 1250,
          "maxApplicationsPerUser" : 1250,
          "userLimit" : 100,
          "users" : { },
          "userLimitFactor" : 1,
          "configuredMaxAMResourceLimit" : 0.1,
          "AMResourceLimit" : {
            "memory" : 2048,
            "vCores" : 1,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 2048
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 1
              } ]
            }
          },
          "usedAMResource" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "userAMResourceLimit" : {
            "memory" : 2048,
            "vCores" : 1,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 2048
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 1
              } ]
            }
          },
          "preemptionDisabled" : true,
          "intraQueuePreemptionDisabled" : true,
          "defaultPriority" : 0,
          "isAutoCreatedLeafQueue" : false,
          "maxApplicationLifetime" : -1,
          "defaultApplicationLifetime" : -1
        }, {
          "type" : "capacitySchedulerLeafQueueInfo",
          "queuePath" : "root.b",
          "capacity" : 50,
          "usedCapacity" : 0,
          "maxCapacity" : 100,
          "absoluteCapacity" : 50,
          "absoluteMaxCapacity" : 100,
          "absoluteUsedCapacity" : 0,
          "weight" : -1,
          "normalizedWeight" : 0,
          "numApplications" : 0,
          "maxParallelApps" : 2147483647,
          "queueName" : "b",
          "isAbsoluteResource" : false,
          "state" : "RUNNING",
          "resourcesUsed" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "hideReservationQueues" : false,
          "nodeLabels" : [ "*" ],
          "allocatedContainers" : 0,
          "reservedContainers" : 0,
          "pendingContainers" : 0,
          "capacities" : {
            "queueCapacitiesByPartition" : [ {
              "partitionName" : "",
              "queueCapacityVectorInfo" : {
                "configuredCapacityVector" : "[memory-mb=50.0%,vcores=50.0%]",
                "capacityVectorEntries" : [ {
                  "resourceName" : "memory-mb",
                  "resourceValue" : "50.0%"
                }, {
                  "resourceName" : "vcores",
                  "resourceValue" : "50.0%"
                } ]
              },
              "capacity" : 50,
              "usedCapacity" : 0,
              "maxCapacity" : 100,
              "absoluteCapacity" : 50,
              "absoluteUsedCapacity" : 0,
              "absoluteMaxCapacity" : 100,
              "maxAMLimitPercentage" : 10,
              "weight" : -1,
              "normalizedWeight" : 0,
              "configuredMinResource" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 8192,
                    "minimumAllocation" : 1024,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 4,
                    "minimumAllocation" : 1,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "configuredMaxResource" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 8192,
                    "minimumAllocation" : 1024,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 4,
                    "minimumAllocation" : 1,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "effectiveMinResource" : {
                "memory" : 16384,
                "vCores" : 16,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 16384
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 16
                  } ]
                }
              },
              "effectiveMaxResource" : {
                "memory" : 32768,
                "vCores" : 32,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 32768
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 32
                  } ]
                }
              }
            } ]
          },
          "resources" : {
            "resourceUsagesByPartition" : [ {
              "partitionName" : "",
              "used" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "reserved" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "pending" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "amUsed" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "amLimit" : {
                "memory" : 4096,
                "vCores" : 1,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 4096
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 1
                  } ]
                }
              },
              "userAmLimit" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              }
            } ]
          },
          "minEffectiveCapacity" : {
            "memory" : 16384,
            "vCores" : 16,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 16384
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 16
              } ]
            }
          },
          "maxEffectiveCapacity" : {
            "memory" : 32768,
            "vCores" : 32,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 32768
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 32
              } ]
            }
          },
          "maximumAllocation" : {
            "memory" : 8192,
            "vCores" : 4,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 8192
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 4
              } ]
            }
          },
          "queueAcls" : {
            "queueAcl" : [ {
              "accessType" : "ADMINISTER_QUEUE",
              "accessControlList" : " "
            }, {
              "accessType" : "APPLICATION_MAX_PRIORITY",
              "accessControlList" : "*"
            }, {
              "accessType" : "SUBMIT_APP",
              "accessControlList" : " "
            } ]
          },
          "queuePriority" : 0,
          "orderingPolicyInfo" : "fifo",
          "autoCreateChildQueueEnabled" : false,
          "leafQueueTemplate" : { },
          "mode" : "percentage",
          "queueType" : "leaf",
          "creationMethod" : "static",
          "autoCreationEligibility" : "off",
          "autoQueueTemplateProperties" : { },
          "autoQueueParentTemplateProperties" : { },
          "autoQueueLeafTemplateProperties" : { },
          "numActiveApplications" : 0,
          "numPendingApplications" : 0,
          "numContainers" : 0,
          "maxApplications" : 5000,
          "maxApplicationsPerUser" : 5000,
          "userLimit" : 100,
          "users" : { },
          "userLimitFactor" : 1,
          "configuredMaxAMResourceLimit" : 0.1,
          "AMResourceLimit" : {
            "memory" : 4096,
            "vCores" : 1,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 4096
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 1
              } ]
            }
          },
          "usedAMResource" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "userAMResourceLimit" : {
            "memory" : 4096,
            "vCores" : 1,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 4096
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 1
              } ]
            }
          },
          "preemptionDisabled" : true,
          "intraQueuePreemptionDisabled" : true,
          "defaultPriority" : 0,
          "isAutoCreatedLeafQueue" : false,
          "maxApplicationLifetime" : -1,
          "defaultApplicationLifetime" : -1
        }, {
          "type" : "capacitySchedulerLeafQueueInfo",
          "queuePath" : "root.c",
          "capacity" : 37.5,
          "usedCapacity" : 0,
          "maxCapacity" : 100,
          "absoluteCapacity" : 37.5,
          "absoluteMaxCapacity" : 100,
          "absoluteUsedCapacity" : 0,
          "weight" : -1,
          "normalizedWeight" : 0,
          "numApplications" : 0,
          "maxParallelApps" : 2147483647,
          "queueName" : "c",
          "isAbsoluteResource" : false,
          "state" : "RUNNING",
          "resourcesUsed" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "hideReservationQueues" : false,
          "nodeLabels" : [ "*" ],
          "allocatedContainers" : 0,
          "reservedContainers" : 0,
          "pendingContainers" : 0,
          "capacities" : {
            "queueCapacitiesByPartition" : [ {
              "partitionName" : "",
              "queueCapacityVectorInfo" : {
                "configuredCapacityVector" : "[memory-mb=37.5%,vcores=37.5%]",
                "capacityVectorEntries" : [ {
                  "resourceName" : "memory-mb",
                  "resourceValue" : "37.5%"
                }, {
                  "resourceName" : "vcores",
                  "resourceValue" : "37.5%"
                } ]
              },
              "capacity" : 37.5,
              "usedCapacity" : 0,
              "maxCapacity" : 100,
              "absoluteCapacity" : 37.5,
              "absoluteUsedCapacity" : 0,
              "absoluteMaxCapacity" : 100,
              "maxAMLimitPercentage" : 10,
              "weight" : -1,
              "normalizedWeight" : 0,
              "configuredMinResource" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 8192,
                    "minimumAllocation" : 1024,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 4,
                    "minimumAllocation" : 1,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "configuredMaxResource" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 8192,
                    "minimumAllocation" : 1024,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 4,
                    "minimumAllocation" : 1,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "effectiveMinResource" : {
                "memory" : 12288,
                "vCores" : 12,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 12288
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 12
                  } ]
                }
              },
              "effectiveMaxResource" : {
                "memory" : 32768,
                "vCores" : 32,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 32768
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 32
                  } ]
                }
              }
            } ]
          },
          "resources" : {
            "resourceUsagesByPartition" : [ {
              "partitionName" : "",
              "used" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "reserved" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "pending" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "amUsed" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              },
              "amLimit" : {
                "memory" : 4096,
                "vCores" : 1,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 4096
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 1
                  } ]
                }
              },
              "userAmLimit" : {
                "memory" : 0,
                "vCores" : 0,
                "resourceInformations" : {
                  "resourceInformation" : [ {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "memory-mb",
                    "resourceType" : "COUNTABLE",
                    "units" : "Mi",
                    "value" : 0
                  }, {
                    "attributes" : { },
                    "maximumAllocation" : 9223372036854775807,
                    "minimumAllocation" : 0,
                    "name" : "vcores",
                    "resourceType" : "COUNTABLE",
                    "units" : "",
                    "value" : 0
                  } ]
                }
              }
            } ]
          },
          "minEffectiveCapacity" : {
            "memory" : 12288,
            "vCores" : 12,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 12288
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 12
              } ]
            }
          },
          "maxEffectiveCapacity" : {
            "memory" : 32768,
            "vCores" : 32,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 32768
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 32
              } ]
            }
          },
          "maximumAllocation" : {
            "memory" : 8192,
            "vCores" : 4,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 8192
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 4
              } ]
            }
          },
          "queueAcls" : {
            "queueAcl" : [ {
              "accessType" : "ADMINISTER_QUEUE",
              "accessControlList" : " "
            }, {
              "accessType" : "APPLICATION_MAX_PRIORITY",
              "accessControlList" : "*"
            }, {
              "accessType" : "SUBMIT_APP",
              "accessControlList" : " "
            } ]
          },
          "queuePriority" : 0,
          "orderingPolicyInfo" : "fifo",
          "autoCreateChildQueueEnabled" : false,
          "leafQueueTemplate" : { },
          "mode" : "percentage",
          "queueType" : "leaf",
          "creationMethod" : "static",
          "autoCreationEligibility" : "off",
          "autoQueueTemplateProperties" : { },
          "autoQueueParentTemplateProperties" : { },
          "autoQueueLeafTemplateProperties" : { },
          "numActiveApplications" : 0,
          "numPendingApplications" : 0,
          "numContainers" : 0,
          "maxApplications" : 3750,
          "maxApplicationsPerUser" : 3750,
          "userLimit" : 100,
          "users" : { },
          "userLimitFactor" : 1,
          "configuredMaxAMResourceLimit" : 0.1,
          "AMResourceLimit" : {
            "memory" : 4096,
            "vCores" : 1,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 4096
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 1
              } ]
            }
          },
          "usedAMResource" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "userAMResourceLimit" : {
            "memory" : 4096,
            "vCores" : 1,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 4096
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 1
              } ]
            }
          },
          "preemptionDisabled" : true,
          "intraQueuePreemptionDisabled" : true,
          "defaultPriority" : 0,
          "isAutoCreatedLeafQueue" : false,
          "maxApplicationLifetime" : -1,
          "defaultApplicationLifetime" : -1
        } ]
      },
      "capacities" : {
        "queueCapacitiesByPartition" : [ {
          "partitionName" : "",
          "queueCapacityVectorInfo" : {
            "configuredCapacityVector" : "[memory-mb=100.0%,vcores=100.0%]",
            "capacityVectorEntries" : [ {
              "resourceName" : "memory-mb",
              "resourceValue" : "100.0%"
            }, {
              "resourceName" : "vcores",
              "resourceValue" : "100.0%"
            } ]
          },
          "capacity" : 100,
          "usedCapacity" : 0,
          "maxCapacity" : 100,
          "absoluteCapacity" : 100,
          "absoluteUsedCapacity" : 0,
          "absoluteMaxCapacity" : 100,
          "maxAMLimitPercentage" : 0,
          "weight" : -1,
          "normalizedWeight" : 0,
          "configuredMinResource" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 8192,
                "minimumAllocation" : 1024,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 4,
                "minimumAllocation" : 1,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "configuredMaxResource" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 8192,
                "minimumAllocation" : 1024,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 4,
                "minimumAllocation" : 1,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          },
          "effectiveMinResource" : {
            "memory" : 32768,
            "vCores" : 32,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 32768
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 32
              } ]
            }
          },
          "effectiveMaxResource" : {
            "memory" : 32768,
            "vCores" : 32,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 32768
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 32
              } ]
            }
          }
        } ]
      },
      "health" : {
        "lastrun" : 0,
        "operationsInfo" : [ {
          "operation" : "last-allocation",
          "nodeId" : "N/A",
          "containerId" : "N/A",
          "queue" : "N/A"
        }, {
          "operation" : "last-release",
          "nodeId" : "N/A",
          "containerId" : "N/A",
          "queue" : "N/A"
        }, {
          "operation" : "last-preemption",
          "nodeId" : "N/A",
          "containerId" : "N/A",
          "queue" : "N/A"
        }, {
          "operation" : "last-reservation",
          "nodeId" : "N/A",
          "containerId" : "N/A",
          "queue" : "N/A"
        } ],
        "lastRunDetails" : [ {
          "operation" : "releases",
          "count" : 0,
          "resources" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          }
        }, {
          "operation" : "allocations",
          "count" : 0,
          "resources" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          }
        }, {
          "operation" : "reservations",
          "count" : 0,
          "resources" : {
            "memory" : 0,
            "vCores" : 0,
            "resourceInformations" : {
              "resourceInformation" : [ {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "memory-mb",
                "resourceType" : "COUNTABLE",
                "units" : "Mi",
                "value" : 0
              }, {
                "attributes" : { },
                "maximumAllocation" : 9223372036854775807,
                "minimumAllocation" : 0,
                "name" : "vcores",
                "resourceType" : "COUNTABLE",
                "units" : "",
                "value" : 0
              } ]
            }
          }
        } ]
      },
      "maximumAllocation" : {
        "memory" : 8192,
        "vCores" : 4,
        "resourceInformations" : {
          "resourceInformation" : [ {
            "attributes" : { },
            "maximumAllocation" : 9223372036854775807,
            "minimumAllocation" : 0,
            "name" : "memory-mb",
            "resourceType" : "COUNTABLE",
            "units" : "Mi",
            "value" : 8192
          }, {
            "attributes" : { },
            "maximumAllocation" : 9223372036854775807,
            "minimumAllocation" : 0,
            "name" : "vcores",
            "resourceType" : "COUNTABLE",
            "units" : "",
            "value" : 4
          } ]
        }
      },
      "queueAcls" : {
        "queueAcl" : [ {
          "accessType" : "ADMINISTER_QUEUE",
          "accessControlList" : "*"
        }, {
          "accessType" : "APPLICATION_MAX_PRIORITY",
          "accessControlList" : "*"
        }, {
          "accessType" : "SUBMIT_APP",
          "accessControlList" : "*"
        } ]
      },
      "queuePriority" : 0,
      "orderingPolicyInfo" : "utilization",
      "mode" : "percentage",
      "queueType" : "parent",
      "creationMethod" : "static",
      "autoCreationEligibility" : "off",
      "autoQueueTemplateProperties" : { },
      "autoQueueParentTemplateProperties" : { },
      "autoQueueLeafTemplateProperties" : { }
    }
  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/scheduler

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 5778
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8"?><scheduler>
  <schedulerInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="capacityScheduler">
    <capacity>100.0</capacity>
    <usedCapacity>0.0</usedCapacity>
    <maxCapacity>100.0</maxCapacity>
    <weight>-1.0</weight>
    <normalizedWeight>0.0</normalizedWeight>
    <queueCapacityVectorInfo>
      <configuredCapacityVector>[memory-mb=100.0%,vcores=100.0%]</configuredCapacityVector>
      <capacityVectorEntries>
        <resourceName>memory-mb</resourceName>
        <resourceValue>100.0%</resourceValue>
      </capacityVectorEntries>
      <capacityVectorEntries>
        <resourceName>vcores</resourceName>
        <resourceValue>100.0%</resourceValue>
      </capacityVectorEntries>
    </queueCapacityVectorInfo>
    <queueName>root</queueName>
    <queuePath>root</queuePath>
    <maxParallelApps>2147483647</maxParallelApps>
    <isAbsoluteResource>false</isAbsoluteResource>
    <queues>
      <queue xsi:type="capacitySchedulerLeafQueueInfo">
        <queuePath>root.a</queuePath>
        <capacity>12.5</capacity>
        <usedCapacity>0.0</usedCapacity>
        <maxCapacity>50.0</maxCapacity>
        <absoluteCapacity>12.5</absoluteCapacity>
        <absoluteMaxCapacity>50.0</absoluteMaxCapacity>
        <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
        <weight>-1.0</weight>
        <normalizedWeight>0.0</normalizedWeight>
        <numApplications>0</numApplications>
        <maxParallelApps>2147483647</maxParallelApps>
        <queueName>a</queueName>
        <isAbsoluteResource>false</isAbsoluteResource>
        <state>RUNNING</state>
        <resourcesUsed>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </resourcesUsed>
        <hideReservationQueues>false</hideReservationQueues>
        <nodeLabels>*</nodeLabels>
        <allocatedContainers>0</allocatedContainers>
        <reservedContainers>0</reservedContainers>
        <pendingContainers>0</pendingContainers>
        <capacities>
          <queueCapacitiesByPartition>
            <partitionName/>
            <queueCapacityVectorInfo>
              <configuredCapacityVector>[memory-mb=12.5%,vcores=12.5%]</configuredCapacityVector>
              <capacityVectorEntries>
                <resourceName>memory-mb</resourceName>
                <resourceValue>12.5%</resourceValue>
              </capacityVectorEntries>
              <capacityVectorEntries>
                <resourceName>vcores</resourceName>
                <resourceValue>12.5%</resourceValue>
              </capacityVectorEntries>
            </queueCapacityVectorInfo>
            <capacity>12.5</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>50.0</maxCapacity>
            <absoluteCapacity>12.5</absoluteCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <absoluteMaxCapacity>50.0</absoluteMaxCapacity>
            <maxAMLimitPercentage>10.0</maxAMLimitPercentage>
            <weight>-1.0</weight>
            <normalizedWeight>0.0</normalizedWeight>
            <configuredMinResource>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>8192</maximumAllocation>
                  <minimumAllocation>1024</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>4</maximumAllocation>
                  <minimumAllocation>1</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </configuredMinResource>
            <configuredMaxResource>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>8192</maximumAllocation>
                  <minimumAllocation>1024</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>4</maximumAllocation>
                  <minimumAllocation>1</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </configuredMaxResource>
            <effectiveMinResource>
              <memory>4096</memory>
              <vCores>4</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>4096</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>4</value>
                </resourceInformation>
              </resourceInformations>
            </effectiveMinResource>
            <effectiveMaxResource>
              <memory>16384</memory>
              <vCores>16</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>16384</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>16</value>
                </resourceInformation>
              </resourceInformations>
            </effectiveMaxResource>
          </queueCapacitiesByPartition>
        </capacities>
        <resources>
          <resourceUsagesByPartition>
            <partitionName/>
            <used>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </used>
            <reserved>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </reserved>
            <pending>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </pending>
            <amUsed>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </amUsed>
            <amLimit>
              <memory>2048</memory>
              <vCores>1</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>2048</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>1</value>
                </resourceInformation>
              </resourceInformations>
            </amLimit>
            <userAmLimit>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </userAmLimit>
          </resourceUsagesByPartition>
        </resources>
        <minEffectiveCapacity>
          <memory>4096</memory>
          <vCores>4</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>4096</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>4</value>
            </resourceInformation>
          </resourceInformations>
        </minEffectiveCapacity>
        <maxEffectiveCapacity>
          <memory>16384</memory>
          <vCores>16</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>16384</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>16</value>
            </resourceInformation>
          </resourceInformations>
        </maxEffectiveCapacity>
        <maximumAllocation>
          <memory>8192</memory>
          <vCores>4</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>8192</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>4</value>
            </resourceInformation>
          </resourceInformations>
        </maximumAllocation>
        <queueAcls>
          <queueAcl>
            <accessType>ADMINISTER_QUEUE</accessType>
            <accessControlList> </accessControlList>
          </queueAcl>
          <queueAcl>
            <accessType>APPLICATION_MAX_PRIORITY</accessType>
            <accessControlList>*</accessControlList>
          </queueAcl>
          <queueAcl>
            <accessType>SUBMIT_APP</accessType>
            <accessControlList> </accessControlList>
          </queueAcl>
        </queueAcls>
        <queuePriority>0</queuePriority>
        <orderingPolicyInfo>fifo</orderingPolicyInfo>
        <autoCreateChildQueueEnabled>false</autoCreateChildQueueEnabled>
        <leafQueueTemplate/>
        <mode>percentage</mode>
        <queueType>leaf</queueType>
        <creationMethod>static</creationMethod>
        <autoCreationEligibility>off</autoCreationEligibility>
        <autoQueueTemplateProperties/>
        <autoQueueParentTemplateProperties/>
        <autoQueueLeafTemplateProperties/>
        <numActiveApplications>0</numActiveApplications>
        <numPendingApplications>0</numPendingApplications>
        <numContainers>0</numContainers>
        <maxApplications>1250</maxApplications>
        <maxApplicationsPerUser>1250</maxApplicationsPerUser>
        <userLimit>100.0</userLimit>
        <users/>
        <userLimitFactor>1.0</userLimitFactor>
        <configuredMaxAMResourceLimit>0.1</configuredMaxAMResourceLimit>
        <AMResourceLimit>
          <memory>2048</memory>
          <vCores>1</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>2048</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>1</value>
            </resourceInformation>
          </resourceInformations>
        </AMResourceLimit>
        <usedAMResource>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </usedAMResource>
        <userAMResourceLimit>
          <memory>2048</memory>
          <vCores>1</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>2048</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>1</value>
            </resourceInformation>
          </resourceInformations>
        </userAMResourceLimit>
        <preemptionDisabled>true</preemptionDisabled>
        <intraQueuePreemptionDisabled>true</intraQueuePreemptionDisabled>
        <defaultPriority>0</defaultPriority>
        <isAutoCreatedLeafQueue>false</isAutoCreatedLeafQueue>
        <maxApplicationLifetime>-1</maxApplicationLifetime>
        <defaultApplicationLifetime>-1</defaultApplicationLifetime>
      </queue>
      <queue xsi:type="capacitySchedulerLeafQueueInfo">
        <queuePath>root.b</queuePath>
        <capacity>50.0</capacity>
        <usedCapacity>0.0</usedCapacity>
        <maxCapacity>100.0</maxCapacity>
        <absoluteCapacity>50.0</absoluteCapacity>
        <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
        <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
        <weight>-1.0</weight>
        <normalizedWeight>0.0</normalizedWeight>
        <numApplications>0</numApplications>
        <maxParallelApps>2147483647</maxParallelApps>
        <queueName>b</queueName>
        <isAbsoluteResource>false</isAbsoluteResource>
        <state>RUNNING</state>
        <resourcesUsed>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </resourcesUsed>
        <hideReservationQueues>false</hideReservationQueues>
        <nodeLabels>*</nodeLabels>
        <allocatedContainers>0</allocatedContainers>
        <reservedContainers>0</reservedContainers>
        <pendingContainers>0</pendingContainers>
        <capacities>
          <queueCapacitiesByPartition>
            <partitionName/>
            <queueCapacityVectorInfo>
              <configuredCapacityVector>[memory-mb=50.0%,vcores=50.0%]</configuredCapacityVector>
              <capacityVectorEntries>
                <resourceName>memory-mb</resourceName>
                <resourceValue>50.0%</resourceValue>
              </capacityVectorEntries>
              <capacityVectorEntries>
                <resourceName>vcores</resourceName>
                <resourceValue>50.0%</resourceValue>
              </capacityVectorEntries>
            </queueCapacityVectorInfo>
            <capacity>50.0</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>100.0</maxCapacity>
            <absoluteCapacity>50.0</absoluteCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
            <maxAMLimitPercentage>10.0</maxAMLimitPercentage>
            <weight>-1.0</weight>
            <normalizedWeight>0.0</normalizedWeight>
            <configuredMinResource>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>8192</maximumAllocation>
                  <minimumAllocation>1024</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>4</maximumAllocation>
                  <minimumAllocation>1</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </configuredMinResource>
            <configuredMaxResource>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>8192</maximumAllocation>
                  <minimumAllocation>1024</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>4</maximumAllocation>
                  <minimumAllocation>1</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </configuredMaxResource>
            <effectiveMinResource>
              <memory>16384</memory>
              <vCores>16</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>16384</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>16</value>
                </resourceInformation>
              </resourceInformations>
            </effectiveMinResource>
            <effectiveMaxResource>
              <memory>32768</memory>
              <vCores>32</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>32768</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>32</value>
                </resourceInformation>
              </resourceInformations>
            </effectiveMaxResource>
          </queueCapacitiesByPartition>
        </capacities>
        <resources>
          <resourceUsagesByPartition>
            <partitionName/>
            <used>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </used>
            <reserved>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </reserved>
            <pending>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </pending>
            <amUsed>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </amUsed>
            <amLimit>
              <memory>4096</memory>
              <vCores>1</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>4096</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>1</value>
                </resourceInformation>
              </resourceInformations>
            </amLimit>
            <userAmLimit>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </userAmLimit>
          </resourceUsagesByPartition>
        </resources>
        <minEffectiveCapacity>
          <memory>16384</memory>
          <vCores>16</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>16384</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>16</value>
            </resourceInformation>
          </resourceInformations>
        </minEffectiveCapacity>
        <maxEffectiveCapacity>
          <memory>32768</memory>
          <vCores>32</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>32768</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>32</value>
            </resourceInformation>
          </resourceInformations>
        </maxEffectiveCapacity>
        <maximumAllocation>
          <memory>8192</memory>
          <vCores>4</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>8192</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>4</value>
            </resourceInformation>
          </resourceInformations>
        </maximumAllocation>
        <queueAcls>
          <queueAcl>
            <accessType>ADMINISTER_QUEUE</accessType>
            <accessControlList> </accessControlList>
          </queueAcl>
          <queueAcl>
            <accessType>APPLICATION_MAX_PRIORITY</accessType>
            <accessControlList>*</accessControlList>
          </queueAcl>
          <queueAcl>
            <accessType>SUBMIT_APP</accessType>
            <accessControlList> </accessControlList>
          </queueAcl>
        </queueAcls>
        <queuePriority>0</queuePriority>
        <orderingPolicyInfo>fifo</orderingPolicyInfo>
        <autoCreateChildQueueEnabled>false</autoCreateChildQueueEnabled>
        <leafQueueTemplate/>
        <mode>percentage</mode>
        <queueType>leaf</queueType>
        <creationMethod>static</creationMethod>
        <autoCreationEligibility>off</autoCreationEligibility>
        <autoQueueTemplateProperties/>
        <autoQueueParentTemplateProperties/>
        <autoQueueLeafTemplateProperties/>
        <numActiveApplications>0</numActiveApplications>
        <numPendingApplications>0</numPendingApplications>
        <numContainers>0</numContainers>
        <maxApplications>5000</maxApplications>
        <maxApplicationsPerUser>5000</maxApplicationsPerUser>
        <userLimit>100.0</userLimit>
        <users/>
        <userLimitFactor>1.0</userLimitFactor>
        <configuredMaxAMResourceLimit>0.1</configuredMaxAMResourceLimit>
        <AMResourceLimit>
          <memory>4096</memory>
          <vCores>1</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>4096</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>1</value>
            </resourceInformation>
          </resourceInformations>
        </AMResourceLimit>
        <usedAMResource>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </usedAMResource>
        <userAMResourceLimit>
          <memory>4096</memory>
          <vCores>1</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>4096</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>1</value>
            </resourceInformation>
          </resourceInformations>
        </userAMResourceLimit>
        <preemptionDisabled>true</preemptionDisabled>
        <intraQueuePreemptionDisabled>true</intraQueuePreemptionDisabled>
        <defaultPriority>0</defaultPriority>
        <isAutoCreatedLeafQueue>false</isAutoCreatedLeafQueue>
        <maxApplicationLifetime>-1</maxApplicationLifetime>
        <defaultApplicationLifetime>-1</defaultApplicationLifetime>
      </queue>
      <queue xsi:type="capacitySchedulerLeafQueueInfo">
        <queuePath>root.c</queuePath>
        <capacity>37.5</capacity>
        <usedCapacity>0.0</usedCapacity>
        <maxCapacity>100.0</maxCapacity>
        <absoluteCapacity>37.5</absoluteCapacity>
        <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
        <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
        <weight>-1.0</weight>
        <normalizedWeight>0.0</normalizedWeight>
        <numApplications>0</numApplications>
        <maxParallelApps>2147483647</maxParallelApps>
        <queueName>c</queueName>
        <isAbsoluteResource>false</isAbsoluteResource>
        <state>RUNNING</state>
        <resourcesUsed>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </resourcesUsed>
        <hideReservationQueues>false</hideReservationQueues>
        <nodeLabels>*</nodeLabels>
        <allocatedContainers>0</allocatedContainers>
        <reservedContainers>0</reservedContainers>
        <pendingContainers>0</pendingContainers>
        <capacities>
          <queueCapacitiesByPartition>
            <partitionName/>
            <queueCapacityVectorInfo>
              <configuredCapacityVector>[memory-mb=37.5%,vcores=37.5%]</configuredCapacityVector>
              <capacityVectorEntries>
                <resourceName>memory-mb</resourceName>
                <resourceValue>37.5%</resourceValue>
              </capacityVectorEntries>
              <capacityVectorEntries>
                <resourceName>vcores</resourceName>
                <resourceValue>37.5%</resourceValue>
              </capacityVectorEntries>
            </queueCapacityVectorInfo>
            <capacity>37.5</capacity>
            <usedCapacity>0.0</usedCapacity>
            <maxCapacity>100.0</maxCapacity>
            <absoluteCapacity>37.5</absoluteCapacity>
            <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
            <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
            <maxAMLimitPercentage>10.0</maxAMLimitPercentage>
            <weight>-1.0</weight>
            <normalizedWeight>0.0</normalizedWeight>
            <configuredMinResource>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>8192</maximumAllocation>
                  <minimumAllocation>1024</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>4</maximumAllocation>
                  <minimumAllocation>1</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </configuredMinResource>
            <configuredMaxResource>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>8192</maximumAllocation>
                  <minimumAllocation>1024</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>4</maximumAllocation>
                  <minimumAllocation>1</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </configuredMaxResource>
            <effectiveMinResource>
              <memory>12288</memory>
              <vCores>12</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>12288</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>12</value>
                </resourceInformation>
              </resourceInformations>
            </effectiveMinResource>
            <effectiveMaxResource>
              <memory>32768</memory>
              <vCores>32</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>32768</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>32</value>
                </resourceInformation>
              </resourceInformations>
            </effectiveMaxResource>
          </queueCapacitiesByPartition>
        </capacities>
        <resources>
          <resourceUsagesByPartition>
            <partitionName/>
            <used>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </used>
            <reserved>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </reserved>
            <pending>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </pending>
            <amUsed>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </amUsed>
            <amLimit>
              <memory>4096</memory>
              <vCores>1</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>4096</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>1</value>
                </resourceInformation>
              </resourceInformations>
            </amLimit>
            <userAmLimit>
              <memory>0</memory>
              <vCores>0</vCores>
              <resourceInformations>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>memory-mb</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units>Mi</units>
                  <value>0</value>
                </resourceInformation>
                <resourceInformation>
                  <attributes/>
                  <maximumAllocation>9223372036854775807</maximumAllocation>
                  <minimumAllocation>0</minimumAllocation>
                  <name>vcores</name>
                  <resourceType>COUNTABLE</resourceType>
                  <units/>
                  <value>0</value>
                </resourceInformation>
              </resourceInformations>
            </userAmLimit>
          </resourceUsagesByPartition>
        </resources>
        <minEffectiveCapacity>
          <memory>12288</memory>
          <vCores>12</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>12288</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>12</value>
            </resourceInformation>
          </resourceInformations>
        </minEffectiveCapacity>
        <maxEffectiveCapacity>
          <memory>32768</memory>
          <vCores>32</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>32768</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>32</value>
            </resourceInformation>
          </resourceInformations>
        </maxEffectiveCapacity>
        <maximumAllocation>
          <memory>8192</memory>
          <vCores>4</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>8192</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>4</value>
            </resourceInformation>
          </resourceInformations>
        </maximumAllocation>
        <queueAcls>
          <queueAcl>
            <accessType>ADMINISTER_QUEUE</accessType>
            <accessControlList> </accessControlList>
          </queueAcl>
          <queueAcl>
            <accessType>APPLICATION_MAX_PRIORITY</accessType>
            <accessControlList>*</accessControlList>
          </queueAcl>
          <queueAcl>
            <accessType>SUBMIT_APP</accessType>
            <accessControlList> </accessControlList>
          </queueAcl>
        </queueAcls>
        <queuePriority>0</queuePriority>
        <orderingPolicyInfo>fifo</orderingPolicyInfo>
        <autoCreateChildQueueEnabled>false</autoCreateChildQueueEnabled>
        <leafQueueTemplate/>
        <mode>percentage</mode>
        <queueType>leaf</queueType>
        <creationMethod>static</creationMethod>
        <autoCreationEligibility>off</autoCreationEligibility>
        <autoQueueTemplateProperties/>
        <autoQueueParentTemplateProperties/>
        <autoQueueLeafTemplateProperties/>
        <numActiveApplications>0</numActiveApplications>
        <numPendingApplications>0</numPendingApplications>
        <numContainers>0</numContainers>
        <maxApplications>3750</maxApplications>
        <maxApplicationsPerUser>3750</maxApplicationsPerUser>
        <userLimit>100.0</userLimit>
        <users/>
        <userLimitFactor>1.0</userLimitFactor>
        <configuredMaxAMResourceLimit>0.1</configuredMaxAMResourceLimit>
        <AMResourceLimit>
          <memory>4096</memory>
          <vCores>1</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>4096</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>1</value>
            </resourceInformation>
          </resourceInformations>
        </AMResourceLimit>
        <usedAMResource>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </usedAMResource>
        <userAMResourceLimit>
          <memory>4096</memory>
          <vCores>1</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>4096</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>1</value>
            </resourceInformation>
          </resourceInformations>
        </userAMResourceLimit>
        <preemptionDisabled>true</preemptionDisabled>
        <intraQueuePreemptionDisabled>true</intraQueuePreemptionDisabled>
        <defaultPriority>0</defaultPriority>
        <isAutoCreatedLeafQueue>false</isAutoCreatedLeafQueue>
        <maxApplicationLifetime>-1</maxApplicationLifetime>
        <defaultApplicationLifetime>-1</defaultApplicationLifetime>
      </queue>
    </queues>
    <capacities>
      <queueCapacitiesByPartition>
        <partitionName/>
        <queueCapacityVectorInfo>
          <configuredCapacityVector>[memory-mb=100.0%,vcores=100.0%]</configuredCapacityVector>
          <capacityVectorEntries>
            <resourceName>memory-mb</resourceName>
            <resourceValue>100.0%</resourceValue>
          </capacityVectorEntries>
          <capacityVectorEntries>
            <resourceName>vcores</resourceName>
            <resourceValue>100.0%</resourceValue>
          </capacityVectorEntries>
        </queueCapacityVectorInfo>
        <capacity>100.0</capacity>
        <usedCapacity>0.0</usedCapacity>
        <maxCapacity>100.0</maxCapacity>
        <absoluteCapacity>100.0</absoluteCapacity>
        <absoluteUsedCapacity>0.0</absoluteUsedCapacity>
        <absoluteMaxCapacity>100.0</absoluteMaxCapacity>
        <maxAMLimitPercentage>0.0</maxAMLimitPercentage>
        <weight>-1.0</weight>
        <normalizedWeight>0.0</normalizedWeight>
        <configuredMinResource>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>8192</maximumAllocation>
              <minimumAllocation>1024</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>4</maximumAllocation>
              <minimumAllocation>1</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </configuredMinResource>
        <configuredMaxResource>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>8192</maximumAllocation>
              <minimumAllocation>1024</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>4</maximumAllocation>
              <minimumAllocation>1</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </configuredMaxResource>
        <effectiveMinResource>
          <memory>32768</memory>
          <vCores>32</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>32768</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>32</value>
            </resourceInformation>
          </resourceInformations>
        </effectiveMinResource>
        <effectiveMaxResource>
          <memory>32768</memory>
          <vCores>32</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>32768</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>32</value>
            </resourceInformation>
          </resourceInformations>
        </effectiveMaxResource>
      </queueCapacitiesByPartition>
    </capacities>
    <health>
      <lastrun>0</lastrun>
      <operationsInfo>
        <operation>last-allocation</operation>
        <nodeId>N/A</nodeId>
        <containerId>N/A</containerId>
        <queue>N/A</queue>
      </operationsInfo>
      <operationsInfo>
        <operation>last-release</operation>
        <nodeId>N/A</nodeId>
        <containerId>N/A</containerId>
        <queue>N/A</queue>
      </operationsInfo>
      <operationsInfo>
        <operation>last-preemption</operation>
        <nodeId>N/A</nodeId>
        <containerId>N/A</containerId>
        <queue>N/A</queue>
      </operationsInfo>
      <operationsInfo>
        <operation>last-reservation</operation>
        <nodeId>N/A</nodeId>
        <containerId>N/A</containerId>
        <queue>N/A</queue>
      </operationsInfo>
      <lastRunDetails>
        <operation>releases</operation>
        <count>0</count>
        <resources>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </resources>
      </lastRunDetails>
      <lastRunDetails>
        <operation>allocations</operation>
        <count>0</count>
        <resources>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </resources>
      </lastRunDetails>
      <lastRunDetails>
        <operation>reservations</operation>
        <count>0</count>
        <resources>
          <memory>0</memory>
          <vCores>0</vCores>
          <resourceInformations>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>memory-mb</name>
              <resourceType>COUNTABLE</resourceType>
              <units>Mi</units>
              <value>0</value>
            </resourceInformation>
            <resourceInformation>
              <attributes/>
              <maximumAllocation>9223372036854775807</maximumAllocation>
              <minimumAllocation>0</minimumAllocation>
              <name>vcores</name>
              <resourceType>COUNTABLE</resourceType>
              <units/>
              <value>0</value>
            </resourceInformation>
          </resourceInformations>
        </resources>
      </lastRunDetails>
    </health>
    <maximumAllocation>
      <memory>8192</memory>
      <vCores>4</vCores>
      <resourceInformations>
        <resourceInformation>
          <attributes/>
          <maximumAllocation>9223372036854775807</maximumAllocation>
          <minimumAllocation>0</minimumAllocation>
          <name>memory-mb</name>
          <resourceType>COUNTABLE</resourceType>
          <units>Mi</units>
          <value>8192</value>
        </resourceInformation>
        <resourceInformation>
          <attributes/>
          <maximumAllocation>9223372036854775807</maximumAllocation>
          <minimumAllocation>0</minimumAllocation>
          <name>vcores</name>
          <resourceType>COUNTABLE</resourceType>
          <units/>
          <value>4</value>
        </resourceInformation>
      </resourceInformations>
    </maximumAllocation>
    <queueAcls>
      <queueAcl>
        <accessType>ADMINISTER_QUEUE</accessType>
        <accessControlList>*</accessControlList>
      </queueAcl>
      <queueAcl>
        <accessType>APPLICATION_MAX_PRIORITY</accessType>
        <accessControlList>*</accessControlList>
      </queueAcl>
      <queueAcl>
        <accessType>SUBMIT_APP</accessType>
        <accessControlList>*</accessControlList>
      </queueAcl>
    </queueAcls>
    <queuePriority>0</queuePriority>
    <orderingPolicyInfo>utilization</orderingPolicyInfo>
    <mode>percentage</mode>
    <queueType>parent</queueType>
    <creationMethod>static</creationMethod>
    <autoCreationEligibility>off</autoCreationEligibility>
    <autoQueueTemplateProperties/>
    <autoQueueParentTemplateProperties/>
    <autoQueueLeafTemplateProperties/>
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

      GET http://rm-http-address:port/ws/v1/cluster/scheduler

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

      GET http://rm-http-address:port/ws/v1/cluster/scheduler
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

### Fair Scheduler API

### Elements of the *schedulerInfo* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | string | Scheduler type - fairScheduler |
| rootQueue | The root queue object | A collection of root queue resources |

### Elements of all queue objects

| Item | Data Type | Description |
|:---- |:---- |:---- |
| maxApps | int | The maximum number of applications the queue can have |
| minResources | A single resource object | The configured minimum resources that are guaranteed to the queue |
| maxResources | A single resource object | The configured maximum resources that are allowed to the queue |
| usedResources | A single resource object | The sum of resources allocated to containers within the queue |
| fairResources | A single resource object | The queue's fair share of resources |
| clusterResources | A single resource object | The capacity of the cluster |
| queueName | string | The name of the queue |
| schedulingPolicy | string | The name of the scheduling policy used by the queue |
| childQueues | array of queues(JSON)/queue objects(XML) | A collection of sub-queue information. Omitted if the queue has no childQueues or is a leaf queue. |
| allocatedContainers | int | The number of allocated containers |
| demandResources | A single resource object | The resources that have been requested by containers in this queue which have not been fulfilled by the scheduler |
| pendingContainers | int | The number of pending containers |
| preemptable | boolean | true if containers in this queue can be preempted |
| reservedContainers | int | The number of reserved containers |
| steadyFairResources | A single resource object | The steady fair share for the queue |

### Additional elements of leaf queue objects (with the exception of the 'childQueues' property)

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | string | The type of the queue - fairSchedulerLeafQueueInfo |
| numActiveApps | int | The number of active applications in this queue |
| numPendingApps | int | The number of pending applications in this queue |

### Elements of the (cluster/demand/fair/max/min/used/*)Resources object in queues

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | long | The amount of memory used (in MB) |
| vCores | int | The number of virtual cores |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/scheduler

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
            "rootQueue": {
                "allocatedContainers": 0,
                "childQueues": {
                    "queue": [
                        {
                            "allocatedContainers": 0,
                            "clusterResources": {
                                "memory": 8192,
                                "vCores": 8
                            },
                            "demandResources": {
                                "memory": 0,
                                "vCores": 0
                            },
                            "fairResources": {
                                "memory": 0,
                                "vCores": 0
                            },
                            "maxApps": 2147483647,
                            "maxResources": {
                                "memory": 8192,
                                "vCores": 8
                            },
                            "minResources": {
                                "memory": 0,
                                "vCores": 0
                            },
                            "numActiveApps": 0,
                            "numPendingApps": 0,
                            "pendingContainers": 0,
                            "preemptable": true,
                            "queueName": "root.default",
                            "reservedContainers": 0,
                            "schedulingPolicy": "fair",
                            "steadyFairResources": {
                                "memory": 4096,
                                "vCores": 0
                            },
                            "type": "fairSchedulerLeafQueueInfo",
                            "usedResources": {
                                "memory": 0,
                                "vCores": 0
                            }
                        },
                        {
                            "allocatedContainers": 0,
                            "childQueues": {
                                "queue": [
                                    {
                                        "allocatedContainers": 0,
                                        "clusterResources": {
                                            "memory": 8192,
                                            "vCores": 8
                                        },
                                        "demandResources": {
                                            "memory": 0,
                                            "vCores": 0
                                        },
                                        "fairResources": {
                                            "memory": 10000,
                                            "vCores": 0
                                        },
                                        "maxApps": 2147483647,
                                        "maxResources": {
                                            "memory": 8192,
                                            "vCores": 8
                                        },
                                        "minResources": {
                                            "memory": 5000,
                                            "vCores": 0
                                        },
                                        "numActiveApps": 0,
                                        "numPendingApps": 0,
                                        "pendingContainers": 0,
                                        "preemptable": true,
                                        "queueName": "root.sample_queue.sample_sub_queue",
                                        "reservedContainers": 0,
                                        "schedulingPolicy": "fair",
                                        "steadyFairResources": {
                                            "memory": 4096,
                                            "vCores": 0
                                        },
                                        "type": "fairSchedulerLeafQueueInfo",
                                        "usedResources": {
                                            "memory": 0,
                                            "vCores": 0
                                        }
                                    }
                                ]
                            },
                            "clusterResources": {
                                "memory": 8192,
                                "vCores": 8
                            },
                            "demandResources": {
                                "memory": 0,
                                "vCores": 0
                            },
                            "fairResources": {
                                "memory": 10000,
                                "vCores": 0
                            },
                            "maxApps": 50,
                            "maxResources": {
                                "memory": 8192,
                                "vCores": 0
                            },
                            "minResources": {
                                "memory": 10000,
                                "vCores": 0
                            },
                            "pendingContainers": 0,
                            "preemptable": true,
                            "queueName": "root.sample_queue",
                            "reservedContainers": 0,
                            "schedulingPolicy": "fair",
                            "steadyFairResources": {
                                "memory": 4096,
                                "vCores": 0
                            },
                            "usedResources": {
                                "memory": 0,
                                "vCores": 0
                            }
                        }
                    ]
                },
                "clusterResources": {
                    "memory": 8192,
                    "vCores": 8
                },
                "demandResources": {
                    "memory": 0,
                    "vCores": 0
                },
                "fairResources": {
                    "memory": 8192,
                    "vCores": 8
                },
                "maxApps": 2147483647,
                "maxResources": {
                    "memory": 8192,
                    "vCores": 8
                },
                "minResources": {
                    "memory": 0,
                    "vCores": 0
                },
                "pendingContainers": 0,
                "preemptable": true,
                "queueName": "root",
                "reservedContainers": 0,
                "schedulingPolicy": "fair",
                "steadyFairResources": {
                    "memory": 8192,
                    "vCores": 8
                },
                "usedResources": {
                    "memory": 0,
                    "vCores": 0
                }
            },
            "type": "fairScheduler"
        }
    }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/scheduler
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2321
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<scheduler>
  <schedulerInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="fairScheduler">
    <rootQueue>
      <maxApps>2147483647</maxApps>
      <minResources>
        <memory>0</memory>
        <vCores>0</vCores>
      </minResources>
      <maxResources>
        <memory>8192</memory>
        <vCores>8</vCores>
      </maxResources>
      <usedResources>
        <memory>0</memory>
        <vCores>0</vCores>
      </usedResources>
      <demandResources>
        <memory>0</memory>
        <vCores>0</vCores>
      </demandResources>
      <steadyFairResources>
        <memory>8192</memory>
        <vCores>8</vCores>
      </steadyFairResources>
      <fairResources>
        <memory>8192</memory>
        <vCores>8</vCores>
      </fairResources>
      <clusterResources>
        <memory>8192</memory>
        <vCores>8</vCores>
      </clusterResources>
      <pendingContainers>0</pendingContainers>
      <allocatedContainers>0</allocatedContainers>
      <reservedContainers>0</reservedContainers>
      <queueName>root</queueName>
      <schedulingPolicy>fair</schedulingPolicy>
      <preemptable>true</preemptable>
      <childQueues>
        <queue xsi:type="fairSchedulerLeafQueueInfo">
          <maxApps>2147483647</maxApps>
          <minResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </minResources>
          <maxResources>
            <memory>8192</memory>
            <vCores>8</vCores>
          </maxResources>
          <usedResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </usedResources>
          <demandResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </demandResources>
          <steadyFairResources>
            <memory>4096</memory>
            <vCores>0</vCores>
          </steadyFairResources>
          <fairResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </fairResources>
          <clusterResources>
            <memory>8192</memory>
            <vCores>8</vCores>
          </clusterResources>
          <pendingContainers>0</pendingContainers>
          <allocatedContainers>0</allocatedContainers>
          <reservedContainers>0</reservedContainers>
          <queueName>root.default</queueName>
          <schedulingPolicy>fair</schedulingPolicy>
          <preemptable>true</preemptable>
          <numPendingApps>0</numPendingApps>
          <numActiveApps>0</numActiveApps>
        </queue>
        <queue>
          <maxApps>50</maxApps>
          <minResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </minResources>
          <maxResources>
            <memory>8192</memory>
            <vCores>0</vCores>
          </maxResources>
          <usedResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </usedResources>
          <demandResources>
            <memory>0</memory>
            <vCores>0</vCores>
          </demandResources>
          <steadyFairResources>
            <memory>4096</memory>
            <vCores>0</vCores>
          </steadyFairResources>
          <fairResources>
            <memory>10000</memory>
            <vCores>0</vCores>
          </fairResources>
          <clusterResources>
            <memory>8192</memory>
            <vCores>8</vCores>
          </clusterResources>
          <pendingContainers>0</pendingContainers>
          <allocatedContainers>0</allocatedContainers>
          <reservedContainers>0</reservedContainers>
          <queueName>root.sample_queue</queueName>
          <schedulingPolicy>fair</schedulingPolicy>
          <preemptable>true</preemptable>
          <childQueues>
            <queue xsi:type="fairSchedulerLeafQueueInfo">
              <maxApps>2147483647</maxApps>
              <minResources>
                <memory>5000</memory>
                <vCores>0</vCores>
              </minResources>
              <maxResources>
                <memory>8192</memory>
                <vCores>8</vCores>
              </maxResources>
              <usedResources>
                <memory>0</memory>
                <vCores>0</vCores>
              </usedResources>
              <demandResources>
                <memory>0</memory>
                <vCores>0</vCores>
              </demandResources>
              <steadyFairResources>
                <memory>4096</memory>
                <vCores>0</vCores>
              </steadyFairResources>
              <fairResources>
                <memory>10000</memory>
                <vCores>0</vCores>
              </fairResources>
              <clusterResources>
                <memory>8192</memory>
                <vCores>8</vCores>
              </clusterResources>
              <pendingContainers>0</pendingContainers>
              <allocatedContainers>0</allocatedContainers>
              <reservedContainers>0</reservedContainers>
              <queueName>root.sample_queue.sample_sub_queue</queueName>
              <schedulingPolicy>fair</schedulingPolicy>
              <preemptable>true</preemptable>
              <numPendingApps>0</numPendingApps>
              <numActiveApps>0</numActiveApps>
            </queue>
          </childQueues>
        </queue>
      </childQueues>
    </rootQueue>
  </schedulerInfo>
</scheduler>
```


Cluster Applications API
------------------------

With the Applications API, you can obtain a collection of resources, each of which represents an application. When you run a GET operation on this resource, you obtain a collection of Application Objects.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple parameters can be specified for GET operations. The started and finished times have a begin and end parameter to allow you to specify ranges. For example, one could request all applications that started between 1:00am and 2:00pm on 12/19/2011 with startedTimeBegin=1324256400&startedTimeEnd=1324303200. If the Begin parameter is not specified, it defaults to 0, and if the End parameter is not specified, it defaults to infinity. All query parameters for this api will filter on all applications. However the `queue` query parameter will only implicitly filter on unfinished applications that are currently in the given queue.

      * state [deprecated] - state of the application
      * states - applications matching the given application states, specified as a comma-separated list.
      * finalStatus - the final status of the application - reported by the application itself
      * user - user name
      * queue - unfinished applications that are currently in this queue
      * limit - total number of app objects to be returned
      * startedTimeBegin - applications with start time beginning with this time, specified in ms since epoch
      * startedTimeEnd - applications with start time ending with this time, specified in ms since epoch
      * finishedTimeBegin - applications with finish time beginning with this time, specified in ms since epoch
      * finishedTimeEnd - applications with finish time ending with this time, specified in ms since epoch
      * applicationTypes - applications matching the given application types, specified as a comma-separated list.
      * applicationTags - applications matching any of the given application tags, specified as a comma-separated list.
      * name - name of the application
      * deSelects - a generic fields which will be skipped in the result.

### Elements of the *apps* (Applications) object

When you make a request for the list of applications, the information will be returned as a collection of app objects. See also [Application API](#Application_API) for syntax of the app object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| app | array of app objects(JSON)/zero or more application objects(XML) | The collection of application objects |

###Elements of the *deSelects* parameter

Help requesters who don't need certain information to reduce the overhead.

Current supported items:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| resourceRequests | comma separated string | Skip resource requests of application in return |

e.g:

      * http://rm-http-address:port/ws/v1/cluster/apps?deSelects=resourceRequests


### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps

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
        "id": "application_1476912658570_0002",
        "user": "user2",
        "name": "word count",
        "queue": "default",
        "state": "FINISHED",
        "finalStatus": "SUCCEEDED",
        "progress": 100,
        "trackingUI": "History",
        "trackingUrl": "http://host.domain.com:8088/cluster/app/application_1476912658570_0002",
        "diagnostics": "...",
        "clusterId": 1476912658570,
        "applicationType": "MAPREDUCE",
        "applicationTags": "",
        "priority": -1,
        "startedTime": 1476913457320,
        "finishedTime": 1476913761898,
        "elapsedTime": 304578,
        "amContainerLogs": "http://host.domain.com:8042/node/containerlogs/container_1476912658570_0002_02_000001/user2",
        "amHostHttpAddress": "host.domain.com:8042",
        "allocatedMB": 0,
        "allocatedVCores": 0,
        "runningContainers": 0,
        "memorySeconds": 206464,
        "vcoreSeconds": 201,
        "queueUsagePercentage": 0,
        "clusterUsagePercentage": 0,
        "preemptedResourceMB": 0,
        "preemptedResourceVCores": 0,
        "numNonAMContainerPreempted": 0,
        "numAMContainerPreempted": 0,
        "logAggregationStatus": "DISABLED",
        "unmanagedApplication": false,
        "appNodeLabelExpression": "",
        "amNodeLabelExpression": "",
        "resourceRequests": [
        {
            "capability": {
                "memory": 4096,
                "virtualCores": 1
            },
            "nodeLabelExpression": "",
            "numContainers": 0,
            "priority": {
                "priority": 0
            },
            "relaxLocality": true,
            "resourceName": "*"
        },
        {
            "capability": {
                "memory": 4096,
                "virtualCores": 1
            },
            "nodeLabelExpression": "",
            "numContainers": 0,
            "priority": {
                "priority": 20
            },
            "relaxLocality": true,
            "resourceName": "host1.domain.com"
        },
        {
            "capability": {
                "memory": 4096,
                "virtualCores": 1
            },
            "nodeLabelExpression": "",
            "numContainers": 0,
            "priority": {
                "priority": 20
            },
            "relaxLocality": true,
            "resourceName": "host2.domain.com"
        }]
      },
      {
        "id": "application_1476912658570_0001",
        "user": "user1",
        "name": "Sleep job",
        "queue": "default",
        "state": "FINISHED",
        "finalStatus": "SUCCEEDED",
        "progress": 100,
        "trackingUI": "History",
        "trackingUrl": "http://host.domain.com:8088/cluster/app/application_1476912658570_0001",
        "diagnostics": "...",
        "clusterId": 1476912658570,
        "applicationType": "YARN",
        "applicationTags": "",
        "priority": -1,
        "startedTime": 1476913464750,
        "finishedTime": 1476913863175,
        "elapsedTime": 398425,
        "amContainerLogs": "http://host.domain.com:8042/node/containerlogs/container_1476912658570_0001_02_000001/user1",
        "amHostHttpAddress": "host.domain.com:8042",
        "allocatedMB": 0,
        "allocatedVCores": 0,
        "runningContainers": 0,
        "memorySeconds": 205410,
        "vcoreSeconds": 200,
        "queueUsagePercentage": 0,
        "clusterUsagePercentage": 0,
        "preemptedResourceMB": 0,
        "preemptedResourceVCores": 0,
        "numNonAMContainerPreempted": 0,
        "numAMContainerPreempted": 0,
        "logAggregationStatus": "DISABLED",
        "unmanagedApplication": false,
        "appNodeLabelExpression": "",
        "amNodeLabelExpression": "",
        "resourceRequests": [
        {
            "capability": {
                "memory": 4096,
                "virtualCores": 1
            },
            "nodeLabelExpression": "",
            "numContainers": 0,
            "priority": {
                "priority": 0
            },
            "relaxLocality": true,
            "resourceName": "*"
        },
        {
            "capability": {
                "memory": 4096,
                "virtualCores": 1
            },
            "nodeLabelExpression": "",
            "numContainers": 0,
            "priority": {
                "priority": 20
            },
            "relaxLocality": true,
            "resourceName": "host3.domain.com"
        },
        {
            "capability": {
                "memory": 4096,
                "virtualCores": 1
            },
            "nodeLabelExpression": "",
            "numContainers": 0,
            "priority": {
                "priority": 20
            },
            "relaxLocality": true,
            "resourceName": "host4.domain.com"
        }]
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps
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
        <id>application_1476912658570_0002</id>
        <user>user2</user>
        <name>word count</name>
        <queue>default</queue>
        <state>FINISHED</state>
        <finalStatus>SUCCEEDED</finalStatus>
        <progress>100.0</progress>
        <trackingUI>History</trackingUI>
        <trackingUrl>http://host.domain.com:8088/cluster/app/application_1476912658570_0002</trackingUrl>
        <diagnostics>...</diagnostics>
        <clusterId>1476912658570</clusterId>
        <applicationType>YARN</applicationType>
        <applicationTags></applicationTags>
        <priority>-1</priority>
        <startedTime>1476913457320</startedTime>
        <finishedTime>1476913761898</finishedTime>
        <elapsedTime>304578</elapsedTime>
        <amContainerLogs>http://host.domain.com:8042/node/containerlogs/container_1476912658570_0002_02_000001/user2</amContainerLogs>
        <amHostHttpAddress>host.domain.com:8042</amHostHttpAddress>
        <allocatedMB>-1</allocatedMB>
        <allocatedVCores>-1</allocatedVCores>
        <runningContainers>-1</runningContainers>
        <memorySeconds>206464</memorySeconds>
        <vcoreSeconds>201</vcoreSeconds>
        <queueUsagePercentage>0.0</queueUsagePercentage>
        <clusterUsagePercentage>0.0</clusterUsagePercentage>
        <preemptedResourceMB>0</preemptedResourceMB>
        <preemptedResourceVCores>0</preemptedResourceVCores>
        <numNonAMContainerPreempted>0</numNonAMContainerPreempted>
        <numAMContainerPreempted>0</numAMContainerPreempted>
        <logAggregationStatus>DISABLED</logAggregationStatus>
        <unmanagedApplication>false</unmanagedApplication>
        <appNodeLabelExpression></appNodeLabelExpression>
        <amNodeLabelExpression></amNodeLabelExpression>
        <resourceRequests>
          <capability>
            <memory>4096</memory>
            <virtualCores>1</virtualCores>
          </capability>
          <nodeLabelExpression/>
          <numContainers>0</numContainers>
          <priority>
            <priority>0</priority>
          </priority>
          <relaxLocality>true</relaxLocality>
          <resourceName>*</resourceName>
        </resourceRequests>
        <resourceRequests>
          <capability>
            <memory>4096</memory>
            <virtualCores>1</virtualCores>
          </capability>
          <nodeLabelExpression/>
          <numContainers>0</numContainers>
          <priority>
            <priority>20</priority>
          </priority>
          <relaxLocality>true</relaxLocality>
          <resourceName>host1.domain.com</resourceName>
        </resourceRequests>
        <resourceRequests>
          <capability>
            <memory>4096</memory>
            <virtualCores>1</virtualCores>
          </capability>
          <nodeLabelExpression/>
          <numContainers>0</numContainers>
          <priority>
            <priority>20</priority>
          </priority>
          <relaxLocality>true</relaxLocality>
          <resourceName>host2.domain.com</resourceName>
        </resourceRequests>
    </app>
    <app>
        <id>application_1476912658570_0001</id>
        <user>user1</user>
        <name>Sleep job</name>
        <queue>default</queue>
        <state>FINISHED</state>
        <finalStatus>SUCCEEDED</finalStatus>
        <progress>100.0</progress>
        <trackingUI>History</trackingUI>
        <trackingUrl>http://host.domain.com:8088/cluster/app/application_1476912658570_0001</trackingUrl>
        <diagnostics>...</diagnostics>
        <clusterId>1476912658570</clusterId>
        <applicationType>YARN</applicationType>
        <applicationTags></applicationTags>
        <priority>-1</priority>
        <startedTime>1476913464750</startedTime>
        <finishedTime>1476913863175</finishedTime>
        <elapsedTime>398425</elapsedTime>
        <amContainerLogs>http://host.domain.com:8042/node/containerlogs/container_1476912658570_0001_02_000001/user1</amContainerLogs>
        <amHostHttpAddress>host.domain.com:8042</amHostHttpAddress>
        <allocatedMB>-1</allocatedMB>
        <allocatedVCores>-1</allocatedVCores>
        <runningContainers>-1</runningContainers>
        <memorySeconds>205410</memorySeconds>
        <vcoreSeconds>200</vcoreSeconds>
        <queueUsagePercentage>0.0</queueUsagePercentage>
        <clusterUsagePercentage>0.0</clusterUsagePercentage>
        <preemptedResourceMB>0</preemptedResourceMB>
        <preemptedResourceVCores>0</preemptedResourceVCores>
        <numNonAMContainerPreempted>0</numNonAMContainerPreempted>
        <numAMContainerPreempted>0</numAMContainerPreempted>
        <logAggregationStatus>DISABLED</logAggregationStatus>
        <unmanagedApplication>false</unmanagedApplication>
        <appNodeLabelExpression></appNodeLabelExpression>
        <amNodeLabelExpression></amNodeLabelExpression>
        <resourceRequests>
          <capability>
            <memory>4096</memory>
            <virtualCores>1</virtualCores>
          </capability>
          <nodeLabelExpression/>
          <numContainers>0</numContainers>
          <priority>
            <priority>0</priority>
          </priority>
          <relaxLocality>true</relaxLocality>
          <resourceName>*</resourceName>
        </resourceRequests>
        <resourceRequests>
          <capability>
            <memory>4096</memory>
            <virtualCores>1</virtualCores>
          </capability>
          <nodeLabelExpression/>
          <numContainers>0</numContainers>
          <priority>
            <priority>20</priority>
          </priority>
          <relaxLocality>true</relaxLocality>
          <resourceName>host1.domain.com</resourceName>
        </resourceRequests>
        <resourceRequests>
          <capability>
            <memory>4096</memory>
            <virtualCores>1</virtualCores>
          </capability>
          <nodeLabelExpression/>
          <numContainers>0</numContainers>
          <priority>
            <priority>20</priority>
          </priority>
          <relaxLocality>true</relaxLocality>
          <resourceName>host2.domain.com</resourceName>
        </resourceRequests>
    </app>
</apps>
```

Cluster Application Statistics API
----------------------------------

With the Application Statistics API, you can obtain a collection of triples, each of which contains the application type, the application state and the number of applications of this type and this state in ResourceManager context. Note that with the performance concern, we currently only support at most one applicationType per query. We may support multiple applicationTypes per query as well as more statistics in the future. When you run a GET operation on this resource, you obtain a collection of statItem objects.

### URI

      * http://rm-http-address:port/ws/v1/cluster/appstatistics

### HTTP Operations Supported

      * GET

### Query Parameters Required

Two parameters can be specified. The parameters are case insensitive.

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

      GET http://rm-http-address:port/ws/v1/cluster/appstatistics?states=accepted,running,finished&applicationTypes=mapreduce

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

      GET http://rm-http-address:port/ws/v1/cluster/appstatistics?states=accepted,running,finished&applicationTypes=mapreduce
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

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}

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
| queue | string | The queue the application was submitted to |
| state | string | The application state according to the ResourceManager - valid values are members of the YarnApplicationState enum: NEW, NEW\_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED |
| finalStatus | string | The final status of the application if finished - reported by the application itself - valid values are the members of the FinalApplicationStatus enum: UNDEFINED, SUCCEEDED, FAILED, KILLED |
| progress | float | The progress of the application as a percent |
| trackingUI | string | Where the tracking url is currently pointing - History (for history server) or ApplicationMaster |
| trackingUrl | string | The web URL that can be used to track the application |
| diagnostics | string | Detailed diagnostics information |
| clusterId | long | The cluster id |
| applicationType | string | The application type |
| applicationTags | string | Comma separated tags of an application |
| priority | string | Priority of the submitted application |
| startedTime | long | The time in which application started (in ms since epoch) |
| finishedTime | long | The time in which the application finished (in ms since epoch) |
| elapsedTime | long | The elapsed time since the application started (in ms) |
| amContainerLogs | string | The URL of the application master container logs |
| amHostHttpAddress | string | The nodes http address of the application master |
| amRPCAddress | string | The RPC address of the application master |
| allocatedMB | int | The sum of memory in MB allocated to the application's running containers |
| allocatedVCores | int | The sum of virtual cores allocated to the application's running containers |
| runningContainers | int | The number of containers currently running for the application |
| memorySeconds | long | The amount of memory the application has allocated (megabyte-seconds) |
| vcoreSeconds | long | The amount of CPU resources the application has allocated (virtual core-seconds) |
| queueUsagePercentage | float | The percentage of resources of the queue that the app is using |
| clusterUsagePercentage | float | The percentage of resources of the cluster that the app is using. |
| preemptedResourceMB | long | Memory used by preempted container |
| preemptedResourceVCores | long | Number of virtual cores used by preempted container |
| numNonAMContainerPreempted | int | Number of standard containers preempted |
| numAMContainerPreempted | int | Number of application master containers preempted |
| logAggregationStatus | string | Status of log aggregation - valid values are the members of the LogAggregationStatus enum: DISABLED, NOT\_START, RUNNING, RUNNING\_WITH\_FAILURE, SUCCEEDED, FAILED, TIME\_OUT |
| unmanagedApplication | boolean | Is the application unmanaged. |
| appNodeLabelExpression | string | Node Label expression which is used to identify the nodes on which application's containers are expected to run by default.|
| amNodeLabelExpression | string | Node Label expression which is used to identify the node on which application's  AM container is expected to run.|

### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1476912658570_0002

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "app": {
    "id": "application_1476912658570_0002",
    "user": "user2",
    "name": "word count",
    "queue": "default",
    "state": "FINISHED",
    "finalStatus": "SUCCEEDED",
    "progress": 100,
    "trackingUI": "History",
    "trackingUrl": "http://host.domain.com:8088/cluster/app/application_1476912658570_0002",
    "diagnostics": "...",
    "clusterId": 1476912658570,
    "applicationType": "YARN",
    "applicationTags": "",
    "priority": -1,
    "startedTime": 1476913457320,
    "finishedTime": 1476913761898,
    "elapsedTime": 304578,
    "amContainerLogs": "http://host.domain.com:8042/node/containerlogs/container_1476912658570_0002_02_000001/dr.who",
    "amHostHttpAddress": "host.domain.com:8042",
    "allocatedMB": -1,
    "allocatedVCores": -1,
    "runningContainers": -1,
    "memorySeconds": 206464,
    "vcoreSeconds": 201,
    "queueUsagePercentage": 0,
    "clusterUsagePercentage": 0,
    "preemptedResourceMB": 0,
    "preemptedResourceVCores": 0,
    "numNonAMContainerPreempted": 0,
    "numAMContainerPreempted": 0,
    "logAggregationStatus": "DISABLED",
    "unmanagedApplication": false,
    "appNodeLabelExpression": "",
    "amNodeLabelExpression": ""
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1326821518301_0005
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
    <id>application_1476912658570_0002</id>
    <user>user2</user>
    <name>word count</name>
    <queue>default</queue>
    <state>FINISHED</state>
    <finalStatus>SUCCEEDED</finalStatus>
    <progress>100.0</progress>
    <trackingUI>History</trackingUI>
    <trackingUrl>http://host.domain.com:8088/cluster/app/application_1476912658570_0002</trackingUrl>
    <diagnostics>...</diagnostics>
    <clusterId>1476912658570</clusterId>
    <applicationType>YARN</applicationType>
    <applicationTags></applicationTags>
    <priority>-1</priority>
    <startedTime>1476913457320</startedTime>
    <finishedTime>1476913761898</finishedTime>
    <elapsedTime>304578</elapsedTime>
    <amContainerLogs>http://host.domain.com:8042/node/containerlogs/container_1476912658570_0002_02_000001/dr.who</amContainerLogs>
    <amHostHttpAddress>host.domain.com:8042</amHostHttpAddress>
    <allocatedMB>-1</allocatedMB>
    <allocatedVCores>-1</allocatedVCores>
    <runningContainers>-1</runningContainers>
    <memorySeconds>206464</memorySeconds>
    <vcoreSeconds>201</vcoreSeconds>
    <queueUsagePercentage>0.0</queueUsagePercentage>
    <clusterUsagePercentage>0.0</clusterUsagePercentage>
    <preemptedResourceMB>0</preemptedResourceMB>
    <preemptedResourceVCores>0</preemptedResourceVCores>
    <numNonAMContainerPreempted>0</numNonAMContainerPreempted>
    <numAMContainerPreempted>0</numAMContainerPreempted>
    <logAggregationStatus>DISABLED</logAggregationStatus>
    <unmanagedApplication>false</unmanagedApplication>
    <appNodeLabelExpression></appNodeLabelExpression>
    <amNodeLabelExpression></amNodeLabelExpression>
</app>
```

Cluster Application Attempts API
--------------------------------

With the application attempts API, you can obtain a collection of resources that represent an application attempt. When you run a GET operation on this resource, you obtain a collection of App Attempt Objects.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts

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
| appAttemptState | string | The state of the application attempt - valid values are members of the RMAppAttemptState enum: NEW, SUBMITTED, SCHEDULED, ALLOCATED, LAUNCHED, FAILED, RUNNING, FINISHING, FINISHED, KILLED, ALLOCATED_SAVING, LAUNCHED_UNMANAGED_SAVING, FINAL_SAVING |

### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1326821518301_0005/appattempts

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
            "containerId" : "container_1326821518301_0005_01_000001",
            "appAttemptState" : "RUNNING"
         }
      ]
   }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1326821518301_0005/appattempts
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
    <appAttemptState>RUNNING</appAttemptState>
  </appAttempt>
</appAttempts>
```

Containers for an Application Attempt API
-----------------------------------------

With Containers for an Application Attempt API you can obtain the list of containers, which belongs to an Application Attempt.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts/{appAttemptId}/containers

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *containers* object

When you make a request for the list of containers, the information will be returned as an array of container objects.

containers:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| containers | array of app container objects(JSON)/zero or more container objects(XML) | The collection of app container objects |

### Elements of the *container* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| containerId | string | The container id |
| allocatedMB | long | The amount of memory allocated for the container in MB |
| allocatedVCores | int | The amount of virtual cores allocated for the container |
| assignedNodeId | string | The node id of the node the attempt ran on |
| priority | int | Allocated priority of the container |
| startedTime | long | The start time of the attempt (in ms since epoch) |
| finishedTime | long | The finish time of the attempt (in ms since epoch) 0 if not finished |
| elapsedTime | long | The elapsed time in ms since the startedTime |
| logUrl | string | The web URL that can be used to check the log for the container |
| containerExitStatus | int | Final exit status of the container |
| containerState | string | State of the container, can be NEW, RUNNING, or COMPLETE |
| nodeHttpAddress | string | The node http address of the node the attempt ran on ||
| nodeId | string | The node id of the node the attempt ran on |
| allocatedResources |array of resource(JSON)/zero or more resource objects(XML) | Allocated resources for the container |

### Elements of the *resource* object
| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | The maximum memory for the container |
| vCores | int | The maximum number of vcores for the container |

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts/{appAttemptId}/containers

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "containers" : {
    "container": [
      {
      "containerId": "container_1531404209605_0008_01_000001",
      "allocatedMB": "1536",
      "allocatedVCores": "1",
      "assignedNodeId": "host.domain.com:37814",
      "priority": "0",
      "startedTime": "1531405909444",
      "finishedTime": "0",
      "elapsedTime": "4112",
      "logUrl": "http://host.domain.com:8042/node/containerlogs/container_1531404209605_0008_01_000001/systest",
      "containerExitStatus": "0",
      "containerState": "RUNNING",
      "nodeHttpAddress": "http://host.domain.com:8042",
      "nodeId": "host.domain.com:37814",
      "allocatedResources": [
         {
            "key": "memory-mb",
            "value": "1536"
         },
         {
            "key": "vcores",
            "value": "1"
         }
       ]
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts/{appAttemptId}/containers
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 1104
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<containers>
  <container>
    <containerId>container_1531404209605_0008_01_000001</containerId>
    <allocatedMB>1536</allocatedMB>
    <allocatedVCores>1</allocatedVCores>
    <assignedNodeId>host.domain.com:37814</assignedNodeId>
    <priority>0</priority>
    <startedTime>1531405909444</startedTime>
    <finishedTime>0</finishedTime>
    <elapsedTime>4112</elapsedTime>
    <logUrl>
    http://host.domain.com:8042/node/containerlogs/container_1531404209605_0008_01_000001/systest
    </logUrl>
    <containerExitStatus>0</containerExitStatus>
    <containerState>RUNNING</containerState>
    <nodeHttpAddress>http://host.domain.com:8042</nodeHttpAddress>
    <nodeId>host.domain.com:37814</nodeId>
    <allocatedResources>
      <entry>
        <key>memory-mb</key>
        <value>1536</value>
      </entry>
      <entry>
        <key>vcores</key>
        <value>1</value>
      </entry>
    </allocatedResources>
  </container>
</containers>
```

Specific Container for an Application Attempt API
-------------------------------------------------

With Specific Container for an Application Attempt API you can obtain information about a specific container, which belongs to an Application Attempt and selected by the container id.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts/{appAttemptId}/containers/{containerId}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *container* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| containerId | string | The container id |
| allocatedMB | long | The amount of memory allocated for the container in MB |
| allocatedVCores | int | The amount of virtual cores allocated for the container |
| assignedNodeId | string | The node id of the node the attempt ran on |
| priority | int | Allocated priority of the container |
| startedTime | long | The start time of the attempt (in ms since epoch) |
| finishedTime | long | The finish time of the attempt (in ms since epoch) 0 if not finished |
| elapsedTime | long | The elapsed time in ms since the startedTime |
| logUrl | string | The web URL that can be used to check the log for the container |
| containerExitStatus | int | Final exit status of the container |
| containerState | string | State of the container, can be NEW, RUNNING, or COMPLETE |
| nodeHttpAddress | string | The node http address of the node the attempt ran on ||
| nodeId | string | The node id of the node the attempt ran on |
| allocatedResources |array of resource(JSON)/zero or more resource objects(XML) | Allocated resources for the container |

### Elements of the *resource* object
| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | The maximum memory for the container |
| vCores | int | The maximum number of vcores for the container |

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts/{appAttemptId}/containers/{containerId}

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "container": {
    "containerId": "container_1531404209605_0008_01_000001",
    "allocatedMB": "1536",
    "allocatedVCores": "1",
    "assignedNodeId": "host.domain.com:37814",
    "priority": "0",
    "startedTime": "1531405909444",
    "finishedTime": "0",
    "elapsedTime": "4112",
    "logUrl": "http://host.domain.com:8042/node/containerlogs/container_1531404209605_0008_01_000001/systest",
    "containerExitStatus": "0",
    "containerState": "RUNNING",
    "nodeHttpAddress": "http://host.domain.com:8042",
    "nodeId": "host.domain.com:37814",
    "allocatedResources": [
       {
          "key": "memory-mb",
          "value": "1536"
       },
       {
          "key": "vcores",
          "value": "1"
       }
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/appattempts/{appAttemptId}/containers/{containerId}
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 1104
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<container>
  <containerId>container_1531404209605_0008_01_000001</containerId>
  <allocatedMB>1536</allocatedMB>
  <allocatedVCores>1</allocatedVCores>
  <assignedNodeId>host.domain.com:37814</assignedNodeId>
  <priority>0</priority>
  <startedTime>1531405909444</startedTime>
  <finishedTime>0</finishedTime>
  <elapsedTime>4112</elapsedTime>
  <logUrl>
  http://host.domain.com:8042/node/containerlogs/container_1531404209605_0008_01_000001/systest
  </logUrl>
  <containerExitStatus>0</containerExitStatus>
  <containerState>RUNNING</containerState>
  <nodeHttpAddress>http://host.domain.com:8042</nodeHttpAddress>
  <nodeId>host.domain.com:37814</nodeId>
  <allocatedResources>
    <entry>
      <key>memory-mb</key>
      <value>1536</value>
    </entry>
    <entry>
      <key>vcores</key>
      <value>1</value>
    </entry>
  </allocatedResources>
</container>
```

Cluster Nodes API
-----------------

With the Nodes API, you can obtain a collection of resources, each of which represents a node. When you run a GET operation on this resource, you obtain a collection of Node Objects.

### URI

      * http://rm-http-address:port/ws/v1/cluster/nodes

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      * states - the states of the node, specified as a comma-separated list, valid values are: NEW, RUNNING, UNHEALTHY, DECOMMISSIONING, DECOMMISSIONED, LOST, REBOOTED, SHUTDOWN

### Elements of the *nodes* object

When you make a request for the list of nodes, the information will be returned as a collection of node objects. See also [Node API](#Node_API) for syntax of the node object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| node | array of node objects(JSON)/zero or more node objects(XML) | A collection of node objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/nodes

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
        "state":"RUNNING",
        "id":"host.domain.com:54158",
        "nodeHostName":"host.domain.com",
        "nodeHTTPAddress":"host.domain.com:8042",
        "lastHealthUpdate": 1476995346399,
        "version": "3.0.0-alpha2-SNAPSHOT",
        "healthReport":"",
        "numContainers":0,
        "usedMemoryMB":0,
        "availMemoryMB":8192,
        "usedVirtualCores":0,
        "availableVirtualCores":8,
        "resourceUtilization":
        {
          "nodePhysicalMemoryMB":1027,
          "nodeVirtualMemoryMB":1027,
          "nodeCPUUsage":0.016661113128066063,
          "aggregatedContainersPhysicalMemoryMB":0,
          "aggregatedContainersVirtualMemoryMB":0,
          "containersCPUUsage":0
        }
      },
      {
        "rack":"\/default-rack",
        "state":"RUNNING",
        "id":"host.domain.com:54158",
        "nodeHostName":"host.domain.com",
        "nodeHTTPAddress":"host.domain.com:8042",
        "lastHealthUpdate":1476995346399,
        "version":"3.0.0-alpha2-SNAPSHOT",
        "healthReport":"",
        "numContainers":0,
        "usedMemoryMB":0,
        "availMemoryMB":8192,
        "usedVirtualCores":0,
        "availableVirtualCores":8,
        "resourceUtilization":
        {
          "nodePhysicalMemoryMB":1027,
          "nodeVirtualMemoryMB":1027,
          "nodeCPUUsage":0.016661113128066063,
          "aggregatedContainersPhysicalMemoryMB":0,
          "aggregatedContainersVirtualMemoryMB":0,
          "containersCPUUsage":0
        }
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/nodes
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
    <id>host1.domain.com:54158</id>
    <nodeHostName>host1.domain.com</nodeHostName>
    <nodeHTTPAddress>host1.domain.com:8042</nodeHTTPAddress>
    <lastHealthUpdate>1476995346399</lastHealthUpdate>
    <version>3.0.0-SNAPSHOT</version>
    <healthReport></healthReport>
    <numContainers>0</numContainers>
    <usedMemoryMB>0</usedMemoryMB>
    <availMemoryMB>8192</availMemoryMB>
    <usedVirtualCores>0</usedVirtualCores>
    <availableVirtualCores>8</availableVirtualCores>
    <resourceUtilization>
        <nodePhysicalMemoryMB>1027</nodePhysicalMemoryMB>
        <nodeVirtualMemoryMB>1027</nodeVirtualMemoryMB>
        <nodeCPUUsage>0.006664445623755455</nodeCPUUsage>
        <aggregatedContainersPhysicalMemoryMB>0</aggregatedContainersPhysicalMemoryMB>
        <aggregatedContainersVirtualMemoryMB>0</aggregatedContainersVirtualMemoryMB>
        <containersCPUUsage>0.0</containersCPUUsage>
    </resourceUtilization>
  </node>
  <node>
    <rack>/default-rack</rack>
    <state>RUNNING</state>
    <id>host2.domain.com:54158</id>
    <nodeHostName>host2.domain.com</nodeHostName>
    <nodeHTTPAddress>host2.domain.com:8042</nodeHTTPAddress>
    <lastHealthUpdate>1476995346399</lastHealthUpdate>
    <version>3.0.0-SNAPSHOT</version>
    <healthReport></healthReport>
    <numContainers>0</numContainers>
    <usedMemoryMB>0</usedMemoryMB>
    <availMemoryMB>8192</availMemoryMB>
    <usedVirtualCores>0</usedVirtualCores>
    <availableVirtualCores>8</availableVirtualCores>
    <resourceUtilization>
        <nodePhysicalMemoryMB>1027</nodePhysicalMemoryMB>
        <nodeVirtualMemoryMB>1027</nodeVirtualMemoryMB>
        <nodeCPUUsage>0.006664445623755455</nodeCPUUsage>
        <aggregatedContainersPhysicalMemoryMB>0</aggregatedContainersPhysicalMemoryMB>
        <aggregatedContainersVirtualMemoryMB>0</aggregatedContainersVirtualMemoryMB>
        <containersCPUUsage>0.0</containersCPUUsage>
    </resourceUtilization>
  </node>
</nodes>
```

Cluster Node API
----------------

A node resource contains information about a node in the cluster.

### URI

Use the following URI to obtain a Node Object, from a node identified by the nodeid value.

      * http://rm-http-address:port/ws/v1/cluster/nodes/{nodeid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *node* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| rack | string | The rack location of this node |
| state | string | State of the node - valid values are: NEW, RUNNING, UNHEALTHY, DECOMMISSIONING, DECOMMISSIONED, LOST, REBOOTED, SHUTDOWN |
| id | string | The node id |
| nodeHostName | string | The host name of the node |
| nodeHTTPAddress | string | The nodes HTTP address |
| lastHealthUpdate | long | The last time the node reported its health (in ms since epoch) |
| version | string | Version of hadoop running on node |
| healthReport | string | A detailed health report |
| numContainers | int | The total number of containers currently running on the node |
| usedMemoryMB | long | The total amount of memory currently used on the node (in MB) |
| availMemoryMB | long | The total amount of memory currently available on the node (in MB) |
| usedVirtualCores | long | The total number of vCores currently used on the node |
| availableVirtualCores | long | The total number of vCores available on the node |
| resourceUtilization | object | Resource utilization on the node |
| totalResource | object | Resources on the node |

The *resourceUtilization* object contains the following elements:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| nodePhysicalMemoryMB | int | Node physical memory utilization |
| nodeVirtualMemoryMB | int | Node virtual memory utilization |
| nodeCPUUsage | double | Node CPU utilization |
| aggregatedContainersPhysicalMemoryMB | int | The aggregated physical memory utilization of the containers |
| aggregatedContainersVirtualMemoryMB | int | The aggregated virtual memory utilization of the containers |
| containersCPUUsage | double | The aggregated CPU utilization of the containers |

### Response Examples

**JSON response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/nodes/h2:1235

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
    "state":"RUNNING",
    "id":"host.domain.com:54158",
    "nodeHostName":"host.domain.com",
    "nodeHTTPAddress":"host.domain.com:8042",
    "lastHealthUpdate":1476916746399,
    "version":"3.0.0-SNAPSHOT",
    "healthReport":"",
    "numContainers":0,
    "usedMemoryMB":0,
    "availMemoryMB":8192,
    "usedVirtualCores":0,
    "availableVirtualCores":8,
    "resourceUtilization":
    {
      "nodePhysicalMemoryMB": 968,
      "nodeVirtualMemoryMB": 968,
      "nodeCPUUsage": 0.01332889124751091,
      "aggregatedContainersPhysicalMemoryMB": 0,
      "aggregatedContainersVirtualMemoryMB": 0,
      "containersCPUUsage": 0
    },
    "totalResource":
    {
      "memory": 2048,
      "vCores": 5
    }
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/node/h2:1235
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
  <state>RUNNING</state>
  <id>host.domain.com:54158</id>
  <nodeHostName>host.domain.com</nodeHostName>
  <nodeHTTPAddress>host.domain.com:8042</nodeHTTPAddress>
  <lastHealthUpdate>1476916746399</lastHealthUpdate>
  <version>3.0.0-SNAPSHOT</version>
  <healthReport></healthReport>
  <numContainers>0</numContainers>
  <usedMemoryMB>0</usedMemoryMB>
  <availMemoryMB>8192</availMemoryMB>
  <usedVirtualCores>0</usedVirtualCores>
  <availableVirtualCores>8</availableVirtualCores>
  <resourceUtilization>
    <nodePhysicalMemoryMB>968</nodePhysicalMemoryMB>
    <nodeVirtualMemoryMB>968</nodeVirtualMemoryMB>
    <nodeCPUUsage>0.01332889124751091</nodeCPUUsage>
    <aggregatedContainersPhysicalMemoryMB>0</aggregatedContainersPhysicalMemoryMB>
    <aggregatedContainersVirtualMemoryMB>0</aggregatedContainersVirtualMemoryMB>
    <containersCPUUsage>0.0</containersCPUUsage>
  </resourceUtilization>
  <totalResource>
    <memory>2048</memory>
    <vCores>5</vCores>
  </totalResource>
</node>
```

Cluster Node Update Resource API
--------------------------------

Update the total resources in a node.

### URI

Use the following URI to update the resources of a Node Object identified by the nodeid value.

      http://rm-http-address:port/ws/v1/cluster/nodes/{nodeid}/resource

### HTTP Operations Supported

      POST

### Query Parameters Supported

      None

### Elements of the *resourceOption* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | long | The total amount of memory to set on the node (in MB) |
| vcores | long | The total number of vCores to set on the node |
| overCommitTimeout | long | The timeout to preempt containers |

### Response Examples

**JSON response**

HTTP Request:

      POST http://rm-http-address:port/ws/v1/cluster/nodes/h2:1235/resource

Request body:

```json
{
  "resource":
  {
    "memory": 1024,
    "vCores": 3
  },
  "overCommitTimeout": -1
}
```

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "resourceInfo":
  {
    "memory": 8192,
    "vCores": 5
  }
}
```

**XML response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/node/h2:1235/resource
      Accept: application/xml

Request body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<resourceOption>
  <resource>
    <memory>8192</memory>
    <vCores>5</vCores>
  </resource>
  <overCommitTimeout>1000</overCommitTimeout>
</resourceOption>
```

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 552
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<resourceInfo>
  <memory>8192</memory>
  <vCores>5</vCores>
</resourceInfo>
```

Cluster Writeable APIs
----------------------

The sections below refer to APIs which allow to create and modify applications. These APIs are currently in alpha and may change in the future.

Cluster New Application API
---------------------------

With the New Application API, you can obtain an application-id which can then be used as part of the [Cluster Submit Applications API](#Cluster_Applications_APISubmit_Application) to submit applications. The response also includes the maximum resource capabilities available on the cluster.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/new-application

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

The *maximum-resource-capabilities* object contains the following elements:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | The maximum memory available for a container |
| vCores | int | The maximum number of cores available for a container |

### Response Examples

**JSON response**

HTTP Request:

      POST http://rm-http-address:port/ws/v1/cluster/apps/new-application

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

      POST http://rm-http-address:port/ws/v1/cluster/apps/new-application

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

      * http://rm-http-address:port/ws/v1/cluster/apps

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
| application-tags | object | List of application tags, please see the request examples on how to specify the tags |
| log-aggregation-context| object | Represents all of the information needed by the NodeManager to handle the logs for this application |
| attempt-failures-validity-interval| long | The failure number will no take attempt failures which happen out of the validityInterval into failure count|
| reservation-id| string | Represent the unique id of the corresponding reserved resource allocation in the scheduler |
| am-black-listing-requests| object | Contains blacklisting information such as "enable/disable AM blacklisting" and "disable failure threshold" |

Elements of the *am-container-spec* object

The am-container-spec object should be used to provide the container launch context for the application master.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| local-resources | object | Object describing the resources that need to be localized, described below |
| environment | object | Environment variables for your containers, specified as key value pairs |
| commands | object | The commands for launching your container, in the order in which they should be executed |
| service-data | object | Application specific service data; key is the name of the auxiliary service, value is base-64 encoding of the data you wish to pass |
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

Elements of the POST request body *log-aggregation-context* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| log-include-pattern | string | The log files which match the defined include pattern will be uploaded when the application finishes |
| log-exclude-pattern | string | The log files which match the defined exclude pattern will not be uploaded when the application finishes |
| rolled-log-include-pattern | string | The log files which match the defined include pattern will be aggregated in a rolling fashion |
| rolled-log-exclude-pattern | string | The log files which match the defined exclude pattern will not be aggregated in a rolling fashion |
| log-aggregation-policy-class-name | string | The policy which will be used by NodeManager to aggregate the logs |
| log-aggregation-policy-parameters | string | The parameters passed to the policy class |

Elements of the POST request body *am-black-listing-requests* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| am-black-listing-enabled | boolean | Whether AM Blacklisting is enabled |
| disable-failure-threshold | float | AM Blacklisting disable failure threshold |

**JSON response**

HTTP Request:

```json
  POST http://rm-http-address:port/ws/v1/cluster/apps
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
    "keep-containers-across-application-attempts":false,
    "log-aggregation-context":
    {
      "log-include-pattern":"file1",
      "log-exclude-pattern":"file2",
      "rolled-log-include-pattern":"file3",
      "rolled-log-exclude-pattern":"file4",
      "log-aggregation-policy-class-name":"org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AllContainerLogAggregationPolicy",
      "log-aggregation-policy-parameters":""
    },
    "attempt-failures-validity-interval":3600000,
    "reservation-id":"reservation_1454114874_1",
    "am-black-listing-requests":
    {
      "am-black-listing-enabled":true,
      "disable-failure-threshold":0.01
    }
  }
```

Response Header:

      HTTP/1.1 202
      Transfer-Encoding: chunked
      Location: http://rm-http-address:port/ws/v1/cluster/apps/application_1404203615263_0001
      Content-Type: application/json
      Server: Jetty(6.1.26)

Response Body:

      No response body

**XML response**

HTTP Request:

```xml
POST http://rm-http-address:port/ws/v1/cluster/apps
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
  <log-aggregation-context>
    <log-include-pattern>file1</log-include-pattern>
    <log-exclude-pattern>file2</log-exclude-pattern>
    <rolled-log-include-pattern>file3</rolled-log-include-pattern>
    <rolled-log-exclude-pattern>file4</rolled-log-exclude-pattern>
    <log-aggregation-policy-class-name>org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AllContainerLogAggregationPolicy</log-aggregation-policy-class-name>
    <log-aggregation-policy-parameters></log-aggregation-policy-parameters>
  </log-aggregation-context>
  <attempt-failures-validity-interval>3600000</attempt-failures-validity-interval>
  <reservation-id>reservation_1454114874_1</reservation-id>
  <am-black-listing-requests>
    <am-black-listing-enabled>true</am-black-listing-enabled>
    <disable-failure-threshold>0.01</disable-failure-threshold>
  </am-black-listing-requests>
</application-submission-context>
```

Response Header:

      HTTP/1.1 202
      Transfer-Encoding: chunked
      Location: http://rm-http-address:port/ws/v1/cluster/apps/application_1404204891930_0002
      Content-Type: application/xml
      Server: Jetty(6.1.26)

Response Body:

      No response body

Cluster Application State API
-----------------------------

With the application state API, you can query the state of a submitted app as well kill a running app by modifying the state of a running app using a PUT request with the state set to "KILLED". To perform the PUT operation, authentication has to be setup for the RM web services. In addition, you must be authorized to kill the app. Currently you can only change the state to "KILLED"; an attempt to change the state to any other results in a 400 error response. Examples of the unauthorized and bad request errors are below. When you carry out a successful PUT, the initial response may be a 202. You can confirm that the app is killed by repeating the PUT request until you get a 200, querying the state using the GET method or querying for app information and checking the state. In the examples below, we repeat the PUT request and get a 200 response.

Please note that in order to kill an app, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/state

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

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

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

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    {
      "state":"KILLED"
    }

Response Header:

    HTTP/1.1 202 Accepted
    Content-Type: application/json
    Transfer-Encoding: chunked
    Location: http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003
    Server: Jetty(6.1.26)

Response Body:

    {
      "state":"ACCEPTED"
    }

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

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

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

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

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>KILLED</state>
    </appstate>

Response Header:

    HTTP/1.1 202 Accepted
    Content-Type: application/xml
    Content-Length: 794
    Location: http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>ACCEPTED</state>
    </appstate>

HTTP Request

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

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

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <appstate>
      <state>KILLED</state>
    </appstate>

Response Header:

    HTTP/1.1 401 Unauthorized
    Server: Jetty(6.1.26)

**Bad Request Error Response**

HTTP Request

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state

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

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/queue

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

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/queue

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

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/queue

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

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/queue

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

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/queue

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

Cluster Application Priority API
-----------------------------

With the application priority API, you can query the priority of a submitted app as well update priority of a running or accepted app using a PUT request specifying the target priority. To perform the PUT operation, authentication has to be setup for the RM web services. In addition, you must be authorized to update the app priority. Currently you can only update the app priority if you're using the Capacity scheduler.

Please note that in order to update priority of an app, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/priority

### HTTP Operations Supported

      * GET
      * PUT

### Query Parameters Supported

      None

### Elements of *apppriority* object

When you make a request for the state of an app, the information returned has the following fields

| Item | Data Type | Description |
|:---- |:---- |:---- |
| priority | int | The application priority |

### Response Examples

**JSON responses**

HTTP Request

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/priority

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Transfer-Encoding: chunked
    Server: Jetty(6.1.26)

Response Body:

    {
      "priority":0
    }

HTTP Request

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/priority

Request Body:

    {
      "priority":8
    }

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Transfer-Encoding: chunked
    Server: Jetty(6.1.26)

Response Body:

    {
      "priority":8
    }

**XML responses**

HTTP Request

      GET http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/priority

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 98
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <applicationpriority>
      <priority>0</priority>
    </applicationpriority>

HTTP Request

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/priority

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <applicationpriority>
      <priority>8</priority>
    </applicationpriority>

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 95
    Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <applicationpriority>
      <priority>8</priority>
    </applicationpriority>

Cluster Delegation Tokens API
-----------------------------

The Delegation Tokens API can be used to create, renew and cancel YARN ResourceManager delegation tokens. All delegation token requests must be carried out on a Kerberos authenticated connection(using SPNEGO). Carrying out operations on a non-kerberos connection will result in a FORBIDDEN response. In case of renewing a token, only the renewer specified when creating the token can renew the token. Other users(including the owner) are forbidden from renewing tokens. It should be noted that when cancelling or renewing a token, the token to be cancelled or renewed is specified by setting a header.

This feature is currently in the alpha stage and may change in the future.

### URI

Use the following URI to create and cancel delegation tokens.

      * http://rm-http-address:port/ws/v1/cluster/delegation-token

Use the following URI to renew delegation tokens.

      * http://rm-http-address:port/ws/v1/cluster/delegation-token/expiration

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

      POST http://rm-http-address:port/ws/v1/cluster/delegation-token
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

      POST http://rm-http-address:port/ws/v1/cluster/delegation-token
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

      POST http://rm-http-address:port/ws/v1/cluster/delegation-token/expiration
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

      POST http://rm-http-address:port/ws/v1/cluster/delegation-token/expiration
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

    DELETE http://rm-http-address:port/ws/v1/cluster/delegation-token
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

      PUT http://rm-http-address:port/ws/v1/cluster/apps/application_1399397633663_0003/state
      X-Hadoop-Delegation-Token: MgASY2xpZW50QEVYQU1QTEUuQ09NDHRlc3QtcmVuZXdlcgCKAUbjqcHHigFHB7ZFxwQCFKWD3znCkDSy6SQIjRCLDydxbxvgE1JNX0RFTEVHQVRJT05fVE9LRU4A
      Content-Type: application/json; charset=UTF8
      {
        "state":"KILLED"
      }

Cluster Reservation API List
----------------------------

The Cluster Reservation API can be used to list reservations. When listing reservations the user must specify the constraints in terms of a queue, reservation-id, start time or end time. The user must also specify whether or not to include the full resource allocations of the reservations being listed. The resulting page returns a response containing information related to the reservation such as the acceptance time, the user, the resource allocations, the reservation-id, as well as the reservation definition.

### URI

    * http://rm-http-address:port/ws/v1/cluster/reservation/list

### HTTP Operations Supported

    * GET

### Query Parameters Supported

      * queue - the queue name containing the reservations to be listed. if not set, this value will default to "default".
      * reservation-id - the reservation-id of the reservation which will be listed. If this parameter is present, start-time and end-time will be ignored.
      * start-time - reservations that end after this start-time will be listed. If unspecified or invalid, this will default to 0.
      * end-time - reservations that start after this end-time will be listed. If unspecified or invalid, this will default to Long.MaxValue.
      * include-resource-allocations - true or false. If true, the resource allocations of the reservation will be included in the response. If false, no resource allocations will be included in the response. This will default to false.

### Elements of the *ReservationListInfo* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservations | array of ReservationInfo(JSON) / zero or more ReservationInfo objects(XML) | The reservations that are listed with the given query |

### Elements of the *reservations* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| acceptance-time | long | Time that the reservation was accepted |
| resource-allocations | array of ResourceAllocationInfo(JSON) / zero or more ResourceAllocationInfo objects(XML) | Resource allocation information for the reservation |
| reservation-id | A single ReservationId string | The unique reservation identifier |
| reservation-definition | A single ReservationDefinition Object | A set of constraints representing the need for resources over time of a user |
| user | string | User who made the reservation |

### Elements of the *resource-allocations* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| resource | A single Resource object | The resources allocated for the reservation allocation |
| startTime | long | Start time that the resource is allocated for |
| endTime | long | End time that the resource is allocated for |

### Elements of the  *resource* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | The memory allocated for the reservation allocation |
| vCores | int | The number of cores allocated for the reservation allocation |

### Elements of the *reservation-definition* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| arrival | long | The UTC time representation of the earliest time this reservation can be allocated from. |
| deadline | long | The UTC time representation of the latest time within which this reservation can be allocated. |
| reservation-name | string | A mnemonic name of the reservation (not a valid identifier). |
| reservation-requests | object | A list of "stages" or phases of this reservation, each describing resource requirements and duration |
| priority | int | An integer representing the priority of the reservation. A lower number for priority indicates a higher priority reservation. Recurring reservations are always higher priority than non-recurring reservations. Priority for non-recurring reservations are only compared with non-recurring reservations. Likewise with recurring reservations. |
| recurrence-expression | string | A recurrence expression which represents the time period of a periodic job. Currently, only long values are supported. Later, support for regular expressions denoting arbitrary recurrence patterns (e.g., every Tuesday and Thursday) will be added. Recurrence is represented in milliseconds for periodic jobs. Recurrence is 0 for non-periodic jobs. Periodic jobs are valid until they are explicitly   cancelled and have higher priority than non-periodic jobs (during initial placement and re-planning). Periodic job allocations are consistent across runs (flexibility in allocation is leveraged only during initial placement, allocations remain consistent thereafter). Note that the recurrence expression must be greater than the duration of the reservation (deadline - arrival). Also note that the configured max period must be divisible by the recurrence expression. |

### Elements of the *reservation-requests* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservation-request-interpreter | int | A numeric choice of how to interpret the set of ReservationRequest: 0 is an ANY, 1 for ALL, 2 for ORDER, 3 for ORDER\_NO\_GAP |
| reservation-request | object | The description of the resource and time capabilities for a phase/stage of this reservation |

### Elements of the *reservation-request* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| duration | long | The duration of a ReservationRequest in milliseconds (amount of consecutive milliseconds a satisfiable allocation for this portion of the reservation should exist for). |
| num-containers | int | The number of containers required in this phase of the reservation (capture the maximum parallelism of the job(s) in this phase). |
| min-concurrency | int | The minimum number of containers that must be concurrently allocated to satisfy this allocation (capture min-parallelism, useful to express gang semantics). |
| capability | object | Allows to specify the size of each container (memory, vCores).|

### GET Response Examples

Get requests can be used to list reservations to the ResourceManager. As mentioned above, information pertaining to the reservation is returned upon success (in the body of the answer). Successful list requests result in a 200 response. Please note that in order to submit a reservation, you must have an authentication filter setup for the HTTP interface. the functionality requires that the username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response. Please note that this feature is currently in the alpha stage and may change in the future.

**JSON response**

This request return all active reservations within the start time 1455159355000 and 1475160036000. Since include-resource-allocations is set to true, the full set of resource allocations will be included in the response.

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/reservation/list?queue=dedicated&start-time=1455159355000&end-time=1475160036000&include-resource-allocations=true

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Cache-Control: no-cache
      Content-Encoding: gzip
      Pragma: no-cache,no-cache
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "reservations": {
    "acceptance-time": "1455160008442",
    "user": "submitter",
    "resource-allocations": [
      {
        "resource": {
          "memory": "0",
          "vCores": "0"
        },
        "startTime": "1465541532000",
        "endTime": "1465542250000"
      },
      {
        "resource": {
          "memory": "1024",
          "vCores": "1"
        },
        "startTime": "1465542250000",
        "endTime": "1465542251000"
      },
      {
        "resource": {
          "memory": "0",
          "vCores": "0"
        },
        "startTime": "1465542251000",
        "endTime": "1465542252000"
      }
    ],
    "reservation-id": "reservation_1458852875788_0002",
    "reservation-definition": {
      "arrival": "1465541532000",
      "deadline": "1465542252000",
      "reservation-requests": {
        "reservation-request-interpreter": "0",
        "reservation-request": {
          "capability": {
            "memory": "1024",
            "vCores": "1"
          },
          "min-concurrency": "1",
          "num-containers": "1",
          "duration": "60"
        }
      },
      "reservation-name": "res_1"
    }
  }
}
```

**XML Response**

HTTP Request:

      GET http://rm-http-address:port/ws/v1/cluster/reservation/list?queue=dedicated&start-time=1455159355000&end-time=1475160036000&include-resource-allocations=true

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-length: 395
      Cache-Control: no-cache
      Content-Encoding: gzip
      Pragma: no-cache,no-cache
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <reservationListInfo>
        <reservations>
        <acceptance-time>1455233661003</acceptance-time>
        <user>dr.who</user>
        <resource-allocations>
            <resource>
                <memory>0</memory>
                <vCores>0</vCores>
            </resource>
            <startTime>1465541532000</startTime>
            <endTime>1465542251000</endTime>
        </resource-allocations>
        <resource-allocations>
            <resource>
                <memory>1024</memory>
                <vCores>1</vCores>
            </resource>
            <startTime>1465542251000</startTime>
            <endTime>1465542252000</endTime>
        </resource-allocations>
        <reservation-id>reservation_1458852875788_0002</reservation-id>
        <reservation-definition>
            <arrival>1465541532000</arrival>
            <deadline>1465542252000</deadline>
            <reservation-requests>
                <reservation-request-interpreter>0</reservation-request-interpreter>
                <reservation-request>
                    <capability>
                        <memory>1024</memory>
                        <vCores>1</vCores>
                    </capability>
                    <min-concurrency>1</min-concurrency>
                    <num-containers>1</num-containers>
                    <duration>60</duration>
                </reservation-request>
            </reservation-requests>
            <reservation-name>res_1</reservation-name>
        </reservation-definition>
    </reservations>
</reservationListInfo>
```

Cluster Reservation API Create
---------------------------

Use the New Reservation API, to obtain a reservation-id which can then be used as part of the [Cluster Reservation API Submit](#Cluster_Reservation_API_Submit) to submit reservations.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://rm-http-address:port/ws/v1/cluster/reservation/new-reservation

### HTTP Operations Supported

      * POST

### Query Parameters Supported

      None

### Elements of the new-reservation object

The new-reservation response contains the following elements:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservation-id | string | The newly created reservation id |

### Response Examples

**JSON response**

HTTP Request:

      POST http://rm-http-address:port/ws/v1/cluster/reservation/new-reservation

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "reservation-id":"reservation_1404198295326_0003"
}
```

**XML response**

HTTP Request:

      POST http://rm-http-address:port/ws/v1/cluster/reservation/new-reservation

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 248
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<new-reservation>
  <reservation-id>reservation_1404198295326_0003</reservation-id>
</new-reservation>
```

Cluster Reservation API Submit
------------------------------

The Cluster Reservation API can be used to submit reservations. When submitting a reservation the user specifies the constraints in terms of resources, and time that is required. The resulting response is successful if the reservation can be made. If a reservation-id is used to submit a reservation multiple times, the request will succeed if the reservation definition is the same, but only one reservation will be created. If the reservation definition is different, the server will respond with an error response. When the reservation is made, the user can use the reservation-id used to submit the reservation to get access to the resources by specifying it as part of [Cluster Submit Applications API](#Cluster_Applications_APISubmit_Application).

### URI

      * http://rm-http-address:port/ws/v1/cluster/reservation/submit

### HTTP Operations Supported

      * POST

### POST Response Examples

POST requests can be used to submit reservations to the ResourceManager. As mentioned above, a reservation-id is returned upon success (in the body of the answer). Successful submissions result in a 200 response. Please note that in order to submit a reservation, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

Please note that this feature is currently in the alpha stage and may change in the future.

#### Elements of the POST request object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| queue | string | The (reservable) queue you are submitting to|
| reservation-definition | object | A set of constraints representing the need for resources over time of a user. |
| reservation-id | string | The reservation id to use to submit the reservation. |

Elements of the *reservation-definition* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
|arrival | long | The UTC time representation of the earliest time this reservation can be allocated from. |
| deadline | long | The UTC time representation of the latest time within which this reservation can be allocated. |
| reservation-name | string | A mnemonic name of the reservation (not a valid identifier). |
| reservation-requests | object | A list of "stages" or phases of this reservation, each describing resource requirements and duration |
| priority | int | An integer representing the priority of the reservation. A lower number for priority indicates a higher priority reservation. Recurring reservations are always higher priority than non-recurring reservations. Priority for non-recurring reservations are only compared with non-recurring reservations. Likewise with recurring reservations. |

Elements of the *reservation-requests* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservation-request-interpreter | int | A numeric choice of how to interpret the set of ReservationRequest: 0 is an ANY, 1 for ALL, 2 for ORDER, 3 for ORDER\_NO\_GAP |
| reservation-request | object | The description of the resource and time capabilities for a phase/stage of this reservation |

Elements of the *reservation-request* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| duration | long | The duration of a ReservationRequest in milliseconds (amount of consecutive milliseconds a satisfiable allocation for this portion of the reservation should exist for). |
| num-containers | int | The number of containers required in this phase of the reservation (capture the maximum parallelism of the job(s) in this phase). |
| min-concurrency | int | The minimum number of containers that must be concurrently allocated to satisfy this allocation (capture min-parallelism, useful to express gang semantics). |
| capability | object | Allows to specify the size of each container (memory, vCores).|

Elements of the *capability* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | the number of MB of memory for this container |
| vCores | int | the number of virtual cores for this container |


**JSON response**

This examples contains a reservation composed of two stages (alternative to each other as the *reservation-request-interpreter* is set to 0), so that the first is shorter and "taller" and "gang"
with exactly 220 containers for 60 seconds, while the second alternative is longer with 120 seconds duration and less tall with 110 containers (and a min-concurrency of 1 container, thus no gang semantics).

HTTP Request:

```json
POST http://rmdns:8088/ws/v1/cluster/reservation/submit
Content-Type: application/json
{
  "queue" : "dedicated",
  "reservation-id":"reservation_1404198295326_0003"
  "reservation-definition" : {
     "arrival" : 1765541532000,
     "deadline" : 1765542252000,
     "reservation-name" : "res_1",
     "reservation-requests" : {
        "reservation-request-interpreter" : 0,
        "reservation-request" : [
           {
             "duration" : 60000,
             "num-containers" : 220,
             "min-concurrency" : 220,
             "capability" : {
               "memory" : 1024,
               "vCores" : 1
             }
           },
           {
             "duration" : 120000,
             "num-containers" : 110,
             "min-concurrency" : 1,
             "capability" : {
               "memory" : 1024,
               "vCores" : 1
             }
           }
        ]
     }
   }
}
```

Response Header:

200 OK
Cache-Control:  no-cache
Expires:  Thu, 17 Dec 2015 23:36:34 GMT, Thu, 17 Dec 2015 23:36:34 GMT
Date:  Thu, 17 Dec 2015 23:36:34 GMT, Thu, 17 Dec 2015 23:36:34 GMT
Pragma:  no-cache, no-cache
Content-Type:  application/xml
Content-Encoding:  gzip
Content-Length:  137
Server:  Jetty(6.1.26)

Response Body:

      No response body

**XML response**

HTTP Request:

```xml
POST http://rmdns:8088/ws/v1/cluster/reservation/submit
Accept: application/xml
Content-Type: application/xml
<reservation-submission-context>
  <queue>dedicated</queue>
  <reservation-id>reservation_1404198295326_0003</reservation-id>
  <reservation-definition>
     <arrival>1765541532000</arrival>
     <deadline>1765542252000</deadline>
     <reservation-name>res_1</reservation-name>
     <reservation-requests>
        <reservation-request-interpreter>0</reservation-request-interpreter>
        <reservation-request>
             <duration>60000</duration>
             <num-containers>220</num-containers>
             <min-concurrency>220</min-concurrency>
             <capability>
               <memory>1024</memory>
               <vCores>1</vCores>
             </capability>
        </reservation-request>
        <reservation-request>
             <duration>120000</duration>
             <num-containers>110</num-containers>
             <min-concurrency>1</min-concurrency>
             <capability>
               <memory>1024</memory>
               <vCores>1</vCores>
             </capability>
        </reservation-request>
     </reservation-requests>
  </reservation-definition>
</reservation-submission-context>
```

Response Header:

200 OK
Cache-Control:  no-cache
Expires:  Thu, 17 Dec 2015 23:49:21 GMT, Thu, 17 Dec 2015 23:49:21 GMT
Date:  Thu, 17 Dec 2015 23:49:21 GMT, Thu, 17 Dec 2015 23:49:21 GMT
Pragma:  no-cache, no-cache
Content-Type:  application/xml
Content-Encoding:  gzip
Content-Length:  137
Server:  Jetty(6.1.26)

Response Body:

      No response body

Cluster Reservation API Update
------------------------------

The Cluster Reservation API Update can be used to update existing reservations.Update of a Reservation works similarly to submit described above, but the user submits the reservation-id of an existing reservation to be updated. The semantics is a try-and-swap, successful operation will modify the existing reservation based on the requested update parameter, while a failed execution will leave the existing reservation unchanged.

### URI

      * http://rm-http-address:port/ws/v1/cluster/reservation/update

### HTTP Operations Supported

      * POST

### POST Response Examples

POST requests can be used to update reservations to the ResourceManager. Successful submissions result in a 200 response, indicate in-place update of the existing reservation (id does not change). Please note that in order to update a reservation, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

Please note that this feature is currently in the alpha stage and may change in the future.

#### Elements of the POST request object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservation-id | string | The id of the reservation to be updated (the system automatically looks up the right queue from this)|
| reservation-definition | object | A set of constraints representing the need for resources over time of a user. |

Elements of the *reservation-definition* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
|arrival | long | The UTC time representation of the earliest time this reservation can be allocated from. |
| deadline | long | The UTC time representation of the latest time within which this reservation can be allocated. |
| reservation-name | string | A mnemonic name of the reservation (not a valid identifier). |
| reservation-requests | object | A list of "stages" or phases of this reservation, each describing resource requirements and duration |
| priority | int | An integer representing the priority of the reservation. A lower number for priority indicates a higher priority reservation. Recurring reservations are always higher priority than non-recurring reservations. Priority for non-recurring reservations are only compared with non-recurring reservations. Likewise with recurring reservations. |

Elements of the *reservation-requests* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservation-request-interpreter | int | A numeric choice of how to interpret the set of ReservationRequest: 0 is an ANY, 1 for ALL, 2 for ORDER, 3 for ORDER\_NO\_GAP |
| reservation-request | object | The description of the resource and time capabilities for a phase/stage of this reservation |

Elements of the *reservation-request* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| duration | long | The duration of a ReservationRequest in milliseconds (amount of consecutive milliseconds a satisfiable allocation for this portion of the reservation should exist for). |
| num-containers | int | The number of containers required in this phase of the reservation (capture the maximum parallelism of the job(s) in this phase). |
| min-concurrency | int | The minimum number of containers that must be concurrently allocated to satisfy this allocation (capture min-parallelism, useful to express gang semantics). |
| capability | object | Allows to specify the size of each container (memory, vCores).|

Elements of the *capability* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| memory | int | the number of MB of memory for this container |
| vCores | int | the number of virtual cores for this container |


**JSON response**

This examples updates an existing reservation identified by *reservation_1449259268893_0005* with two stages (in order as the *reservation-request-interpreter* is set to 2), with the first stage being a "gang" of 10 containers for 5 minutes (min-concurrency of 10 containers) followed by a 50 containers for 10 minutes(min-concurrency of 1 container, thus no gang semantics).

HTTP Request:

```json
POST http://rmdns:8088/ws/v1/cluster/reservation/update
Accept: application/json
Content-Type: application/json
{
  "reservation-id" : "reservation_1449259268893_0005",
  "reservation-definition" : {
     "arrival" : 1765541532000,
     "deadline" : 1765542252000,
     "reservation-name" : "res_1",
     "reservation-requests" : {
        "reservation-request-interpreter" : 2,
        "reservation-request" : [
           {
             "duration" : 300000,
             "num-containers" : 10,
             "min-concurrency" : 10,
             "capability" : {
               "memory" : 1024,
               "vCores" : 1
             }
           },
           {
             "duration" : 60000,
             "num-containers" : 50,
             "min-concurrency" : 1,
             "capability" : {
               "memory" : 1024,
               "vCores" : 1
             }
           }
          ]
     }
   }
}
```

Response Header:

200 OK
Cache-Control:  no-cache
Expires:  Thu, 17 Dec 2015 23:36:34 GMT, Thu, 17 Dec 2015 23:36:34 GMT
Date:  Thu, 17 Dec 2015 23:36:34 GMT, Thu, 17 Dec 2015 23:36:34 GMT
Pragma:  no-cache, no-cache
Content-Type:  application/json
Content-Encoding:  gzip
Content-Length:  137
Server:  Jetty(6.1.26)

Response Body:

      No response body

**XML response**

HTTP Request:

```xml
POST http://rmdns:8088/ws/v1/cluster/reservation/update
Accept: application/xml
Content-Type: application/xml
<reservation-update-context>
  <reservation-id>reservation_1449259268893_0005</reservation-id>
  <reservation-definition>
     <arrival>1765541532000</arrival>
     <deadline>1765542252000</deadline>
     <reservation-name>res_1</reservation-name>
     <reservation-requests>
        <reservation-request-interpreter>2</reservation-request-interpreter>
        <reservation-request>
             <duration>300000</duration>
             <num-containers>10</num-containers>
             <min-concurrency>10</min-concurrency>
             <capability>
               <memory>1024</memory>
               <vCores>1</vCores>
             </capability>
        </reservation-request>
        <reservation-request>
             <duration>60000</duration>
             <num-containers>50</num-containers>
             <min-concurrency>1</min-concurrency>
             <capability>
               <memory>1024</memory>
               <vCores>1</vCores>
             </capability>
        </reservation-request>
     </reservation-requests>
  </reservation-definition>
</reservation-update-context>
```

Response Header:

200 OK
Cache-Control:  no-cache
Expires:  Thu, 17 Dec 2015 23:49:21 GMT, Thu, 17 Dec 2015 23:49:21 GMT
Date:  Thu, 17 Dec 2015 23:49:21 GMT, Thu, 17 Dec 2015 23:49:21 GMT
Pragma:  no-cache, no-cache
Content-Type:  application/xml
Content-Encoding:  gzip
Content-Length:  137
Server:  Jetty(6.1.26)

Response Body:

      No response body

Cluster Reservation API Delete
------------------------------

The Cluster Reservation API Delete can be used to delete existing reservations.Delete works similar to update. The requests contains the reservation-id, and if successful the reservation is cancelled, otherwise the reservation remains in the system.

### URI

      * http://rm-http-address:port/ws/v1/cluster/reservation/delete

### HTTP Operations Supported

      * POST

### POST Response Examples

POST requests can be used to delete reservations to the ResourceManager. Successful submissions result in a 200 response, indicating that the delete succeeded. Please note that in order to delete a reservation, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

Please note that this feature is currently in the alpha stage and may change in the future.

#### Elements of the POST request object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| reservation-id | string | The id of the reservation to be deleted (the system automatically looks up the right queue from this)|


**JSON response**

This examples deletes an existing reservation identified by *reservation_1449259268893_0006*

HTTP Request:

```json
POST http://10.200.91.98:8088/ws/v1/cluster/reservation/delete
Accept: application/json
Content-Type: application/json
{
  "reservation-id" : "reservation_1449259268893_0006"
}
```

Response Header:

200 OK
Cache-Control:  no-cache
Expires:  Fri, 18 Dec 2015 01:31:05 GMT, Fri, 18 Dec 2015 01:31:05 GMT
Date:  Fri, 18 Dec 2015 01:31:05 GMT, Fri, 18 Dec 2015 01:31:05 GMT
Pragma:  no-cache, no-cache
Content-Type:  application/json
Content-Encoding:  gzip
Transfer-Encoding:  chunked
Server:  Jetty(6.1.26)

Response Body:

      No response body

**XML response**

HTTP Request:

```xml
POST http://10.200.91.98:8088/ws/v1/cluster/reservation/delete
Accept: application/xml
Content-Type: application/xml
<reservation-delete-context>
<reservation-id>reservation_1449259268893_0006</reservation-id>
</reservation-delete-context>
```

Response Header:

200 OK
Cache-Control:  no-cache
Expires:  Fri, 18 Dec 2015 01:33:23 GMT, Fri, 18 Dec 2015 01:33:23 GMT
Date:  Fri, 18 Dec 2015 01:33:23 GMT, Fri, 18 Dec 2015 01:33:23 GMT
Pragma:  no-cache, no-cache
Content-Type:  application/xml
Content-Encoding:  gzip
Content-Length:  101
Server:  Jetty(6.1.26)

Response Body:

      No response body

Cluster Application Timeouts API
--------------------------------

Cluster Application Timeouts API can be used to get all configured timeouts of an application. When you run a GET operation on this resource, a collection of timeout objects is returned. Each timeout object is composed of a timeout type, expiry-time and remaining time in seconds.


### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeouts

### HTTP Operations Supported

      * GET

### Elements of the *timeouts* (Application Timeouts) object

When you make a request for the list of application timeouts, the information will be returned as a collection of timeout objects. See also [Cluster Application Timeout API](#Cluster_Application_Timeout_API) for syntax of the timeout object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| timeout | array of timeout objects(JSON)/zero or more application objects(XML) | The collection of application timeout objects |

**JSON response**

HTTP Request:

      Accept: application/json
      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeouts

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "timeouts":
  {
    "timeout":
    [
      {
        "type": "LIFETIME",
        "expiryTime": "2016-12-05T22:51:00.104+0530",
        "remainingTimeInSeconds": 27
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeouts

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 712
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<timeouts>
    <timeout>
       <type>LIFETIME</type>
       <expiryTime>2016-12-05T22:51:00.104+0530</expiryTime>
       <remainingTimeInSeconds>27</remainingTimeInSeconds>
    </timeout>
</timeouts>
```

Cluster Application Timeout API
--------------------------------

The Cluster Application Timeout resource contains information about timeout.


### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeouts/{type}

### HTTP Operations Supported

      * GET

### Elements of the *timeout* (Application Timeout) object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | string | Timeout type. Valid values are the members of the ApplicationTimeoutType enum. LIFETIME is currently the only valid value. |
| expiryTime | string | Time at which the application will expire in ISO8601 yyyy-MM-dd'T'HH:mm:ss.SSSZ format. If UNLIMITED, then application will run forever.  |
| remainingTimeInSeconds | long | Remaining time for configured application timeout. -1 indicates that application is not configured with timeout. Zero(0) indicates that application has expired with configured timeout type. |

**JSON response**

HTTP Request:

      Accept: application/json
      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeouts/LIFETIME

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
"timeout":
   {
     "type": "LIFETIME",
     "expiryTime": "2016-12-05T22:51:00.104+0530",
     "remainingTimeInSeconds": 27
   }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeouts/LIFETIME

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 712
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<timeout>
    <type>LIFETIME</type>
    <expiryTime>2016-12-05T22:51:00.104+0530</expiryTime>
    <remainingTimeInSeconds>27</remainingTimeInSeconds>
</timeout>
```

Cluster Application Timeout Update API
--------------------------------

Update timeout of an application for given timeout type.


### URI

      * http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeout

### HTTP Operations Supported

      * PUT

### Elements of the *timeout* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| type | string | Timeout type. Valid values are the members of the ApplicationTimeoutType enum. LIFETIME is currently the only valid value. |
| expiryTime | string | Time at which the application will expire in ISO8601 yyyy-MM-dd'T'HH:mm:ss.SSSZ format.  |

**JSON response**

HTTP Request:

```json
      Accept: application/json
      PUT http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeout
      Content-Type: application/json
        {
        "timeout":
                   {
                     "type": "LIFETIME",
                     "expiryTime": "2016-11-27T09:36:16.678+05:30"
                   }
        }
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
"timeout":
   {
     "type": "LIFETIME",
     "expiryTime": "2016-11-27T09:36:16.678+05:30",
     "remainingTimeInSeconds": 90
   }
}
```

**XML response**

HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/apps/{appid}/timeout
      Content-Type: application/xml
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <timeout>
            <type>LIFETIME</type>
            <expiryTime>2016-11-27T09:36:16.678+05:30</expiryTime>
        </timeout>
```

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 712
      Server: Jetty(6.1.26)

Response Body:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<timeout>
    <type>LIFETIME</type>
    <expiryTime>2016-11-27T09:36:16.678+05:30</expiryTime>
    <remainingTimeInSeconds>90</remainingTimeInSeconds>
</timeout>
```

Scheduler Configuration Mutation API
--------------------------------

The scheduler configuration mutation API provides a way to modify scheduler/queue configuration and queue hierarchy.

Please note that this feature is currently in the alpha stage and is subject to change.


### URI

      * http://rm-http-address:port/ws/v1/cluster/scheduler-conf

### HTTP Operations Supported

      * GET
      * PUT

### Elements of the *sched-conf* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| update-queue | object | A queue whose configurations should be updated |
| add-queue | object | A queue to add to the scheduler along with this queue's configurations |
| remove-queue | string | Full path name of a queue to remove |
| global-updates | map | Map of key value pairs to update scheduler's global configuration |

### GET Request Examples

Get requests are used to retrieve the scheduler's configuration that is currently loaded into scheduler's context.

**XML response**

HTTP Request:

      Accept: application/xml
      Content-Type: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/scheduler-conf

Response Header:

      TTP/1.1 200 OK
      Content-Type: application/xml; charset=utf-8
      Transfer-Encoding: chunked

Response Body:


```xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <configuration>
        <property>
          <name>yarn.scheduler.capacity.root.queues</name>
          <value>default</value>
        </property>
        <property>
          <name>yarn.scheduler.capacity.maximum-applications</name>
          <value>10000</value>
        </property>
        <property>
          <name>yarn.scheduler.capacity.root.default.capacity</name>
          <value>100</value>
        </property>
      </configuration>
```

### PUT Request Examples

Put requests are used to modify the scheduler configuration. A successful mutation results in a 200 response. A malformed request or one which resulted in an invalid scheduler configuration results in a 400 response.

**Updating queue configuration(s)**

Request for updating queue configurations.

*Elements of the* update-queue *object*

| Item | Data Type | Description |
|:---- |:---- |:---- |
| queue-name | string | Full path name of the queue to update |
| params | map | A map of key value configuration pairs to update for this queue |

Assuming we are using the capacity scheduler and the current queue configuration is a single queue *root.default*, this example sets *root.default*'s maximum applications to 100 and its minimum user limit percent to 10.

HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/scheduler-conf
      Content-Type: application/xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <sched-conf>
        <update-queue>
          <queue-name>root.default</queue-name>
          <params>
            <entry>
              <key>maximum-applications</key>
              <value>100</value>
            </entry>
            <entry>
              <key>minimum-user-limit-percent</key>
              <value>10</value>
            </entry>
          </params>
        </update-queue>
      </sched-conf>
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Transfer-Encoding: chunked


**Adding a queue**

Request for adding queues/updating queue configurations.

*Elements of the* add-queue *object*

| Item | Data Type | Description |
|:---- |:---- |:---- |
| queue-name | string | Full path name of the queue to add |
| params | map | A map of key value configuration pairs to set for this queue |

Assuming we are using the capacity scheduler and the current queue configuration is a single queue *root.default*, this example adds a queue *root.a* with capacity/maximum-capacity 10, and adjusts *root.default*'s capacity/maximum-capacity to 90. (More complex examples include adding a queue whose parent is also being added in the same request, or adding multiple sibling queues.)

HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/scheduler-conf
      Content-Type: application/xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <sched-conf>
        <add-queue>
          <queue-name>root.a</queue-name>
          <params>
            <entry>
              <key>capacity</key>
              <value>10</value>
            </entry>
            <entry>
              <key>maximum-capacity</key>
              <value>10</value>
            </entry>
          </params>
        </add-queue>
        <update-queue>
          <queue-name>root.default</queue-name>
          <params>
            <entry>
              <key>capacity</key>
              <value>90</value>
            </entry>
            <entry>
              <key>maximum-capacity</key>
              <value>90</value>
            </entry>
          </params>
        </update-queue>
      </sched-conf>
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Transfer-Encoding: chunked

**Removing queues**

Request for removing queues from the queue hierarchy.

Assuming we are using the capacity scheduler and the current queue configuration is three queues *root.default*, *root.a*, and *root.b*, this example removes both *root.a* and *root.b*. (More complex examples include removing a parent queue and its children.)

**Note:** Queues must be put into `STOPPED` state before they are deleted. Any updated queue configuration should be a valid one i.e. queue-capacity at each *level* should be equal to 100%.


HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/scheduler-conf
      Content-Type: application/xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <sched-conf>
        <remove-queue>root.a</remove-queue>
        <remove-queue>root.b</remove-queue>
      </sched-conf>
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Transfer-Encoding: chunked

**Updating global scheduler configurations**

Request for updating global scheduler configurations. Assuming we are using the capacity scheduler, this example enables queue mappings. For global configuration updates, the full configuration key must be specified.

HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/scheduler-conf
      Content-Type: application/xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <sched-conf>
        <global-updates>
          <entry>
            <key>yarn.scheduler.capacity.queue-mappings-override.enable</key>
            <value>true</value>
          </entry>
        </global-updates>
      </sched-conf>
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Transfer-Encoding: chunked

**Adding Node Labels to a queue**

Assuming we are using the capacity scheduler and the current queue configuration is two queues root.default, and root.a, this example adds a Node Label x to the queue root.a. Create a Node Label x and assign the nodes with below commands.

```yarn rmadmin -addToClusterNodeLabels "x(exclusive=true)"```

```yarn rmadmin -replaceLabelsOnNode "<nodeId>=x"```

HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/scheduler-conf
      Content-Type: application/xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <sched-conf>
        <update-queue>
          <queue-name>root.a</queue-name>
          <params>
            <entry>
              <key>accessible-node-labels</key>
              <value>x</value>
            </entry>
            <entry>
              <key>accessible-node-labels.x.capacity</key>
              <value>100</value>
            </entry>
          </params>
        </update-queue>
        <update-queue>
          <queue-name>root</queue-name>
          <params>
            <entry>
              <key>accessible-node-labels.x.capacity</key>
              <value>100</value>
            </entry>
          </params>
        </update-queue>
      </sched-conf>
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Transfer-Encoding: chunked

**Removing Node Labels from a queue**

Assuming we are using the capacity scheduler and the current queue configuration is two queues root.default, and root.a and Node Label x is assigned to queue root.a. This example unsets the Node Label x from the queue root.a and removes it.

HTTP Request:

```xml
      Accept: application/xml
      PUT http://rm-http-address:port/ws/v1/cluster/scheduler-conf
      Content-Type: application/xml
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <sched-conf>
        <update-queue>
          <queue-name>root.a</queue-name>
          <params>
            <entry>
              <key>accessible-node-labels</key>
              <value></value>
            </entry>
          </params>
        </update-queue>
      </sched-conf>
```


Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Transfer-Encoding: chunked

```yarn rmadmin -removeFromClusterNodeLabels x```


Cluster Container Signal API
--------------------------------

With the Container Signal API, you can send a signal to a specified container with one of the following commands: OUTPUT_THREAD_DUMP, GRACEFUL_SHUTDOWN and FORCEFUL_SHUTDOWN.

### URI

      http://rm-http-address:port/ws/v1/cluster/containers/{containerId}/signal/{command}

### HTTP Operations Supported

      POST

### Query Parameters Supported

      None

### Response Examples

**JSON response**

HTTP Request:

      POST http://rm-http-address:port/ws/v1/cluster/containers/container_1531404209605_0008_01_000001/signal/OUTPUT_THREAD_DUMP
      Accept: application/json
      Content-Type: application/json

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

No response body.

**XML response**

HTTP Request:

      POST http://rm-http-address:port/ws/v1/cluster/containers/container_1531404209605_0008_01_000001/signal/OUTPUT_THREAD_DUMP
      Accept: application/xml
      Content-Type: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 552
      Server: Jetty(6.1.26)

No response body.


Scheduler Activities API
--------------------------------

  The scheduler activities RESTful API is available if you are using capacity scheduler and can fetch scheduler activities info recorded in a scheduling cycle. The API returns a message that includes important scheduling activities info which has a hierarchical layout with following fields:

  * **Activities** - Activities is the root object of scheduler activities.
  * **Allocations** - Allocations are allocation attempts based on partition or reservation.
  * **Hierarchical Queues** - Hierarchical Queues where the scheduler have been tried to allocate containers to, each of them contains queue name, allocation state, optional diagnostic and optional children.
  * **Applications** - Applications are shown as children of leaf queue, each of them contains the basic info about the application.
  * **Requests** - Requests are shown as children of application, each of them contains the basic info about the request.
  * **Nodes** - Nodes are shown as children of request, each of them contains node id, allocation state, optional name which should appear after allocating or reserving a container on the node, and optional diagnostic which should present if failed to allocate or reserve a container on this node. For aggregated nodes grouped by allocation state and diagnostic, each of them contains allocation state, aggregated node IDs and optional diagnostic.

### URI

      * http://rm-http-address:port/ws/v1/cluster/scheduler/activities

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple parameters can be specified for GET operations.

      * nodeId - specified node ID, if not specified, the scheduler will record the scheduling activities info for the next scheduling cycle on all nodes.
      * groupBy - aggregation type of application activities, currently only support "diagnostic" with which user can query aggregated activities grouped by allocation state and diagnostic.

### Elements of the *Activities* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| nodeId | string | The node ID on which scheduler tries to allocate containers. |
| timestamp | long | Timestamp of the activities. |
| dateTime | string | Date time of the activities. |
| diagnostic | string | Top diagnostic of the activities about empty results, unavailable environments, or illegal input parameters, such as "waiting for display", "waiting for the next allocation", "No node manager running in the cluster", "Got invalid groupBy: xx, valid groupBy types: DIAGNOSTICS" |
| allocations | array of allocations | A collection of allocation objects. |

### Elements of the *Allocation* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| partition | string | Partition of the allocation. |
| updatedContainerId | string | Updated(allocated or reserved) container id in the allocation. |
| finalAllocationState | string | Final allocation state of the allocation, including ALLOCATED, ALLOCATED_FROM_RESERVED, RESERVED, and SKIPPED. |
| root | Queue | Queue object |

### Elements of the *Queue* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | Name of the queue. |
| allocationState | string | Final allocation state of the queue, including ACCEPTED, REJECTED, and SKIPPED. |
| diagnostic | string | Diagnostic of the queue. |
| children | array of queues or applications | A collection of queue objects (for parent queues) or app objects (for leaf queues) |

### Elements of the *Application* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | Name of the application. |
| appPriority | string | Priority of the application. |
| allocationState | string | Final allocation state of the application, including ALLOCATED, RESERVED, RE_RESERVED, and SKIPPED. |
| diagnostic | string | Diagnostic of the application. |
| children | array of requests | A collection of request objects |

### Elements of the *Request* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | Name of the request, the value format is "request_&lt;requestPriority&gt;_&lt;allocationRequestId&gt;" which can be used to identify different requests. |
| requestPriority | string | Priority of the request. |
| allocationRequestId | string | Allocation request id of the request. |
| allocationState | string | Final allocation state of the application, including ALLOCATED, RESERVED, RE_RESERVED, and SKIPPED. |
| diagnostic | string | Diagnostic of the request. |
| children | array of nodes | A collection of node objects. |

### Elements of the *Node* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | Container information which is optional and can be shown when allocation state is ALLOCATED, RESERVED or ALLOCATED_FROM_RESERVED. |
| allocationState | string | Final allocation state of the application, including ALLOCATED, RESERVED, RE_RESERVED, and SKIPPED. |
| diagnostic | string | Diagnostic about node in normal mode or nodes in aggregation mode |
| nodeId | string | The node id on which the scheduling process is based, won’t show for aggregation query with groupBy parameter. |
| nodeIds | array of strings | A collection of node IDs for aggregated activities, only show for aggregation query with groupBy parameter. |
| count | int | The number of node IDs, only show for aggregation query with groupBy parameter. |


### Response Examples

**JSON response**

HTTP Request:

      Accept: application/json
      GET http://rm-http-address:port/ws/v1/cluster/scheduler/activities

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

Following is an output example when global scheduling is enabled, there are multiple activities at the node level for a single request. For single node placement, differences are that nodeId will be shown as a field of activities and there should be only one activity at the node level for every request.

```json
{
  "activities": {
    "timestamp": 1562296830912,
    "dateTime": "Fri Jul 05 11:20:30 CST 2019",
    "allocations": [
      {
        "partition": "",
        "finalAllocationState": "ALLOCATED",
        "root": {
          "name": "root",
          "allocationState": "ACCEPTED",
          "children": [
            {
              "name": "a",
              "allocationState": "SKIPPED",
              "diagnostic": "Queue does not need more resources"
            },
            {
              "name": "b",
              "allocationState": "ACCEPTED",
              "children": [
                {
                  "name": "application_1562296828063_0001",
                  "appPriority": 0,
                  "allocationState": "ALLOCATED",
                  "children": [
                    {
                      "name": "request_1_-1",
                      "requestPriority": 1,
                      "allocationRequestId": -1,
                      "allocationState": "ALLOCATED",
                      "children": [
                        {
                          "allocationState": "SKIPPED",
                          "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=<memory:3072, vCores:1>, available=<memory:2048, vCores:2>",
                          "nodeId": "127.0.0.2:1234"
                        },
                        {
                          "allocationState": "SKIPPED",
                          "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=<memory:3072, vCores:1>, available=<memory:2048, vCores:2>",
                          "nodeId": "127.0.0.3:1234"
                        },
                        {
                          "allocationState": "SKIPPED",
                          "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=<memory:3072, vCores:1>, available=<memory:2048, vCores:2>",
                          "nodeId": "127.0.0.4:1234"
                        },
                        {
                          "name": "Container: [ContainerId: null, AllocationRequestId: -1, Version: 0, NodeId: 127.0.0.1:1234, NodeHttpAddress: 127.0.0.1:2, Resource: <memory:3072, vCores:1>, Priority: 1, Token: null, ExecutionType: GUARANTEED, ]",
                          "allocationState": "ALLOCATED",
                          "nodeId": "127.0.0.1:1234"
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
```

When we query scheduler activities REST API with query parameters("groupBy=diagnostic"), 4 node activities info will be grouped into 2 groups: SKIPPED group with the same diagnostic "Node's total resource is insufficient for request, insufficient resources=[memory-mb]" and ALLOCATED group, the output should be as follows:

```json
{
  "activities": {
    "timestamp": 1562297536893,
    "dateTime": "Fri Jul 05 11:32:16 CST 2019",
    "allocations": [
      {
        "partition": "",
        "finalAllocationState": "ALLOCATED",
        "root": {
          "name": "root",
          "allocationState": "ACCEPTED",
          "children": [
            {
              "name": "a",
              "allocationState": "SKIPPED",
              "diagnostic": "Queue does not need more resources"
            },
            {
              "name": "b",
              "allocationState": "ACCEPTED",
              "children": [
                {
                  "name": "application_1562297533892_0001",
                  "appPriority": 0,
                  "allocationState": "ALLOCATED",
                  "children": [
                    {
                      "name": "request_1_-1",
                      "requestPriority": 1,
                      "allocationRequestId": -1,
                      "allocationState": "ALLOCATED",
                      "children": [
                        {
                          "allocationState": "ALLOCATED",
                          "count": 1,
                          "nodeIds": [
                            "127.0.0.1:1234"
                          ]
                        },
                        {
                          "allocationState": "SKIPPED",
                          "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]",
                          "count": 3,
                          "nodeIds": [
                            "127.0.0.2:1234",
                            "127.0.0.3:1234",
                            "127.0.0.4:1234"
                          ]
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/scheduler/activities

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml; charset=utf-8
      Transfer-Encoding: chunked

Response Body:


```xml
<?xml version="1.0" encoding="utf-8"?>
<activities>
  <timestamp>1562296830912</timestamp>
  <dateTime>Fri Jul 05 11:20:30 CST 2019</dateTime>
  <allocations>
    <partition></partition>
    <finalAllocationState>ALLOCATED</finalAllocationState>
    <root>
      <name>root</name>
      <allocationState>ACCEPTED</allocationState>
      <children>
        <name>a</name>
        <allocationState>SKIPPED</allocationState>
        <diagnostic>Queue does not need more resource</diagnostic>
      </children>
      <children>
        <name>b</name>
        <allocationState>ACCEPTED</allocationState>
        <children>
          <name>application_1562296828063_0001</name>
          <appPriority>0</appPriority>
          <allocationState>ACCEPTED</allocationState>
          <children>
            <name>request_1_-1</name>
            <requestPriority>1</requestPriority>
            <allocationRequestId>-1</allocationRequestId>
            <allocationState>ALLOCATED</allocationState>
            <children>
              <allocationState>SKIPPED</allocationState>
              <diagnostic>Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:3072, vCores:1&gt;, available=&lt;memory:2048, vCores:2&gt;</diagnostic>
              <nodeId>127.0.0.2:1234</nodeId>
            </children>
            <children>
              <allocationState>SKIPPED</allocationState>
              <diagnostic>Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:3072, vCores:1&gt;, available=&lt;memory:2048, vCores:2&gt;</diagnostic>
              <nodeId>127.0.0.3:1234</nodeId>
            </children>
            <children>
              <allocationState>SKIPPED</allocationState>
              <diagnostic>Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:3072, vCores:1&gt;, available=&lt;memory:2048, vCores:2&gt;</diagnostic>
              <nodeId>127.0.0.4:1234</nodeId>
            </children>
            <children>
              <name>Container: [ContainerId: null, AllocationRequestId: -1, Version: 0, NodeId: 127.0.0.1:1234, NodeHttpAddress: 127.0.0.1:2, Resource: &lt;memory:3072, vCores:1&gt;, Priority: 1, Token: null, ExecutionType: GUARANTEED, ]</name>
              <allocationState>ALLOCATED</allocationState>
              <nodeId>127.0.0.1:1234</nodeId>
            </children>
          </children>
        </children>
      </children>
    </root>
  </allocations>
</activities>
```


Scheduler Bulk Activities API
--------------------------------

  The scheduler bulk activities RESTful API can fetch scheduler activities info recorded for multiple scheduling cycle. This may take time
to return as it internally waits until a certain amount of records are generated specified by activitiesCount.

### URI

      * http://rm-http-address:port/ws/v1/cluster/scheduler/bulk-activities

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple parameters can be specified for GET operations.

      * activitiesCount - number of schecduling cycle to record with maximum of 500.
      * groupBy - aggregation type of application activities, currently only support "diagnostic" with which
        user can query aggregated activities grouped by allocation state and diagnostic.


### Response Examples

**JSON response**

HTTP Request:

      Accept: application/json
      GET http://rm-http-address:port/ws/v1/cluster/scheduler/bulk-activities?activitiesCount=2

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

Following is an output example with query parameter activitiesCount set to 2. This fetches scheduler activities info
recorded in last two scheduling cycle.

```json
{
  "bulkActivities": {
    "activities": [
      {
        "nodeId": "127.0.0.1:1234",
        "timestamp": 1593684431432,
        "dateTime": "Thu Jul 02 10:07:11 UTC 2020",
        "allocations": [
          {
            "partition": "",
            "finalAllocationState": "SKIPPED",
            "root": {
              "name": "root",
              "allocationState": "SKIPPED",
              "diagnostic": "Queue does not need more resource"
            }
          }
        ]
      },
      {
        "nodeId": "127.0.0.2:1234",
        "timestamp": 1593684431432,
        "dateTime": "Thu Jul 02 10:07:11 UTC 2020",
        "allocations": [
          {
            "partition": "",
            "finalAllocationState": "SKIPPED",
            "root": {
              "name": "root",
              "allocationState": "SKIPPED",
              "diagnostic": "Queue does not need more resource"
            }
          }
        ]
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/scheduler/bulk-activities?activitiesCount=2

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml; charset=utf-8
      Transfer-Encoding: chunked

Response Body:

```xml
<bulkActivities>
  <activities>
    <nodeId>127.0.0.1:1234</nodeId>
    <timestamp>1593683816380</timestamp>
    <dateTime>Thu Jul 02 09:56:56 UTC 2020</dateTime>
    <allocations>
      <partition/>
      <finalAllocationState>SKIPPED</finalAllocationState>
      <root>
        <name>root</name>
        <allocationState>SKIPPED</allocationState>
        <diagnostic>Queue does not need more resource</diagnostic>
      </root>
    </allocations>
  </activities>
  <activities>
    <nodeId>127.0.0.2:1234</nodeId>
    <timestamp>1593683816385</timestamp>
    <dateTime>Thu Jul 02 09:56:56 UTC 2020</dateTime>
    <allocations>
      <partition/>
      <finalAllocationState>SKIPPED</finalAllocationState>
      <root>
        <name>root</name>
        <allocationState>SKIPPED</allocationState>
        <diagnostic>Queue does not need more resource</diagnostic>
      </root>
    </allocations>
  </activities>
</bulkActivities>
```


Application Activities API
--------------------------------

Application activities RESTful API is available if you are using capacity scheduler and can fetch useful scheduling info for a specified application, the response has a hierarchical layout with following fields:

* **AppActivities** - AppActivities are root element of application activities within basic information.
* **Allocations** - Allocations are allocation attempts at app level queried from the cache.
* **Requests** - Requests are shown as children of allocation, each of them contains request name, request priority, allocation request id, allocation state and optional children.
* **Nodes** - Nodes are shown as children of request, each of them contains node id, allocation state, optional name which should appear after allocating or reserving a container on the node, and optional diagnostic which should appear if failed to allocate or reserve a container on the node. For aggregated nodes grouped by allocation state and diagnostic, each of them contains allocation state, aggregated node IDs and optional diagnostic.

### URI

      * http://rm-http-address:port/ws/v1/cluster/scheduler/app-activities/{appid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

Multiple parameters can be specified for GET operations.

      * maxTime - the max duration in seconds from now on for recording application activities. If not specified, this will default to 3 (seconds).
      * requestPriorities - the priorities of request, used to filter application activities, specified as a comma-separated list.
      * allocationRequestIds - the allocation request IDs of request, used to filter application activities, specified as a comma-separated list.
      * groupBy - the aggregation type of application activities, currently only support “diagnostic” with which user can query aggregated activities grouped by allocation state and diagnostic.
      * limit - the limit of application activities which can reduce the cost for both server and client side.
      * actions - the required actions of app activities including "refresh" and "get", specified as a comma-separated list.
      * summarize - whether app activities in multiple scheduling processes need to be summarized, specified as boolean, it's useful when multi-node placement disabled, because only one node can be considered in a single scheduling process, enabling this can give us a summary with diagnostics on all nodes.

### Elements of the *AppActivities* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| applicationId | string | The specified application id. |
| allocations | array of allocations | A collection of allocation objects in descending order by time. |

### Elements of the *Allocation* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| nodeId | string | ID of the node. |
| timestamp | long | Timestamp of the allocation. |
| dateTime | string | Date time of the allocation. |
| queueName | string | Queue name of the application. |
| appPriority | int | Priority of the application |
| allocationState | string | Final allocation state of the application, including ALLOCATED, RESERVED, RE_RESERVED, and SKIPPED. |
| diagnostic | string | Diagnostic of the application. |
| children | array of requests | A collection of request objects |

### Elements of the *Request* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| requestPriority | string | Priority of the request. |
| allocationRequestId | string | Allocation request id of the request. |
| allocationState | string | Final allocation state of the application, including ALLOCATED, RESERVED, RE_RESERVED, and SKIPPED. |
| diagnostic | string | Diagnostic of the request. |
| children | array of nodes | A collection of node objects. |

### Elements of the *Node* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | ID of the updated container, will show "Container-Id-Not-Assigned" if not assigned. |
| allocationState | string | Final allocation state of the application, including ALLOCATED, RESERVED, RE_RESERVED, and SKIPPED. |
| diagnostic | string | Diagnostic about node in normal mode or nodes in aggregation mode |
| nodeId | string | The node id on which the scheduling process is based, won’t show for aggregation query with groupBy parameter. |
| nodeIds | array of strings | A collection of node IDs for aggregated app activities, only show for aggregation query with groupBy parameter. |
| count | int | The number of node IDs, only show for aggregation query with groupBy parameter. |


### Response Examples

**JSON response**

HTTP Request:

      Accept: application/json
      GET http://rm-http-address:port/ws/v1/cluster/scheduler/app-activities/{appid}

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  "appActivities": {
    "applicationId": "application_1562312049195_0001",
    "allocations": [
      {
        "timestamp": 1562312052039,
        "dateTime": "Fri Jul 05 15:34:12 CST 2019",
        "queueName": "b",
        "appPriority": 0,
        "allocationState": "RESERVED",
        "children": [
          {
            "requestPriority": 1,
            "allocationRequestId": -1,
            "allocationState": "RESERVED",
            "children": [
              {
                "name": "Container-Id-Not-Assigned",
                "allocationState": "SKIPPED",
                "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=<memory:4096, vCores:1>, available=<memory:2048, vCores:2>",
                "nodeId": "127.0.0.3:1234"
              },
              {
                "name": "Container-Id-Not-Assigned",
                "allocationState": "SKIPPED",
                "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=<memory:4096, vCores:1>, available=<memory:2048, vCores:2>",
                "nodeId": "127.0.0.4:1234"
              },
              {
                "name": "Container-Id-Not-Assigned",
                "allocationState": "SKIPPED",
                "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=<memory:4096, vCores:1>, available=<memory:2048, vCores:2>",
                "nodeId": "127.0.0.2:1234"
              },
              {
                "name": "Container-Id-Not-Assigned",
                "allocationState": "RESERVED",
                "diagnostic": "Node does not have sufficient resource for request, insufficient resources=[memory-mb]\nrequired=<memory:4096, vCores:1>, available=<memory:1024, vCores:3>",
                "nodeId": "127.0.0.1:1234"
              }
            ]
          }
        ]
      },
      {
        "timestamp": 1562312052031,
        "dateTime": "Fri Jul 05 15:34:12 CST 2019",
        "queueName": "b",
        "appPriority": 0,
        "allocationState": "ALLOCATED",
        "children": [
          {
            "requestPriority": 0,
            "allocationRequestId": -1,
            "allocationState": "ALLOCATED",
            "children": [
              {
                "name": "Container-Id-Not-Assigned",
                "allocationState": "ALLOCATED",
                "nodeId": "127.0.0.2:1234"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

When we query scheduler activities REST API with a query parameter ("groupBy=diagnostic"), 4 node activities will be separated into two groups: SKIPPED group with the same diagnostic “Node's total resource is insufficient for request, insufficient resources=[memory-mb]” and ALLOCATED group, the output should be as follows:

```json
{
  "appActivities": {
    "applicationId": "application_1562312658870_0001",
    "allocations": [
      {
        "timestamp": 1562312661536,
        "dateTime": "Fri Jul 05 15:44:21 CST 2019",
        "queueName": "b",
        "appPriority": 0,
        "allocationState": "RESERVED",
        "children": [
          {
            "requestPriority": 1,
            "allocationRequestId": -1,
            "allocationState": "RESERVED",
            "children": [
              {
                "allocationState": "SKIPPED",
                "diagnostic": "Node's total resource is insufficient for request, insufficient resources=[memory-mb]",
                "count": 3,
                "nodeIds": [
                  "127.0.0.3:1234",
                  "127.0.0.4:1234",
                  "127.0.0.2:1234"
                ]
              },
              {
                "allocationState": "RESERVED",
                "diagnostic": "Node does not have sufficient resource for request, insufficient resources=[memory-mb]",
                "count": 1,
                "nodeIds": [
                  "127.0.0.1:1234"
                ]
              }
            ]
          }
        ]
      },
      {
        "timestamp": 1562312661522,
        "dateTime": "Fri Jul 05 15:44:21 CST 2019",
        "queueName": "b",
        "appPriority": 0,
        "allocationState": "ALLOCATED",
        "children": [
          {
            "requestPriority": 0,
            "allocationRequestId": -1,
            "allocationState": "ALLOCATED",
            "children": [
              {
                "allocationState": "ALLOCATED",
                "count": 1,
                "nodeIds": [
                  "127.0.0.2:1234"
                ]
              }
            ]
          }
        ]
      }
    ]
  }
}
```

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://rm-http-address:port/ws/v1/cluster/scheduler/app-activities/{appid}

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml; charset=utf-8
      Transfer-Encoding: chunked

Response Body:

```xml
<?xml version="1.0" encoding="utf-8"?>

<appActivities>
  <applicationId>application_1562312049195_0001</applicationId>
  <allocations>
    <timestamp>1562312052039</timestamp>
    <dateTime>Fri Jul 05 15:34:12 CST 2019</dateTime>
    <queueName>b</queueName>
    <appPriority>0</appPriority>
    <allocationState>RESERVED</allocationState>
    <children>
      <requestPriority>1</requestPriority>
      <allocationRequestId>-1</allocationRequestId>
      <allocationState>RESERVED</allocationState>
      <children>
        <name>Container-Id-Not-Assigned</name>
        <allocationState>SKIPPED</allocationState>
        <diagnostic>Node's total resource is insufficient for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:4096, vCores:1&gt;, available=&lt;memory:2048, vCores:2&gt;</diagnostic>
        <nodeId>127.0.0.3:1234</nodeId>
      </children>
      <children>
        <name>Container-Id-Not-Assigned</name>
        <allocationState>SKIPPED</allocationState>
        <diagnostic>Node does not have sufficient resource for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:4096, vCores:1&gt;, available=&lt;memory:2048, vCores:2&gt;</diagnostic>
        <nodeId>127.0.0.4:1234</nodeId>
      </children>
      <children>
        <name>Container-Id-Not-Assigned</name>
        <allocationState>SKIPPED</allocationState>
        <diagnostic>Node does not have sufficient resource for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:4096, vCores:1&gt;, available=&lt;memory:2048, vCores:2&gt;</diagnostic>
        <nodeId>127.0.0.2:1234</nodeId>
      </children>
      <children>
        <name>Container-Id-Not-Assigned</name>
        <allocationState>RESERVED</allocationState>
        <diagnostic>Node does not have sufficient resource for request, insufficient resources=[memory-mb]\nrequired=&lt;memory:4096, vCores:1&gt;, available=&lt;memory:1024, vCores:3&gt;</diagnostic>
        <nodeId>127.0.0.1:1234</nodeId>
      </children>
    </children>
  </allocations>
  <allocations>
    <timestamp>1562312052031</timestamp>
    <dateTime>Fri Jul 05 15:34:12 CST 2019</dateTime>
    <queueName>b</queueName>
    <appPriority>0</appPriority>
    <allocationState>ALLOCATED</allocationState>
    <children>
      <requestPriority>0</requestPriority>
      <allocationRequestId>-1</allocationRequestId>
      <allocationState>ALLOCATED</allocationState>
      <children>
        <name>Container-Id-Not-Assigned</name>
        <allocationState>ALLOCATED</allocationState>
        <nodeId>127.0.0.2:1234</nodeId>
      </children>
    </children>
  </allocations>
</appActivitiesInfo>
```
