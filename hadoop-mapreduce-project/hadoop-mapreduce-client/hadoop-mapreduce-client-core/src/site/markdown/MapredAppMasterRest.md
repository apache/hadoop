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

MapReduce Application Master REST API's.
========================================

<!-- MACRO{toc|fromDepth=0|toDepth=1} -->

Overview
--------

The MapReduce Application Master REST API's allow the user to get status on the running MapReduce application master. Currently this is the equivalent to a running MapReduce job. The information includes the jobs the app master is running and all the job particulars like tasks, counters, configuration, attempts, etc. The application master should be accessed via the proxy. This proxy is configurable to run either on the resource manager or on a separate host. The proxy URL usually looks like: `http://proxy-http-address:port/proxy/appid`.

Mapreduce Application Master Info API
-------------------------------------

The MapReduce application master information resource provides overall information about that mapreduce application master. This includes application id, time it was started, user, name, etc.

### URI

Both of the following URI's give you the MapReduce application master information, from an application id identified by the appid value.

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce
      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/info

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *info* object

When you make a request for the mapreduce application master information, the information will be returned as an info object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| appId | long | The application id |
| startedOn | long | The time the application started (in ms since epoch) |
| name | string | The name of the application |
| user | string | The user name of the user who started the application |
| elapsedTime | long | The time since the application was started (in ms) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0003/ws/v1/mapreduce/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
      "info" : {
          "appId" : "application_1326232085508_0003",
          "startedOn" : 1326238244047,
          "user" : "user1",
          "name" : "Sleep job",
          "elapsedTime" : 32374
       }
    }

**XML response**

HTTP Request:

      Accept: application/xml
      GET http://proxy-http-address:port/proxy/application_1326232085508_0003/ws/v1/mapreduce/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 223
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <info>
      <appId>application_1326232085508_0003</appId>
      <name>Sleep job</name>
      <user>user1</user>
      <startedOn>1326238244047</startedOn>
      <elapsedTime>32407</elapsedTime>
    </info>

Jobs API
--------

The jobs resource provides a list of the jobs running on this application master. See also [Job API](#Job_API) for syntax of the job object.

### URI

      *  http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *jobs* object

When you make a request for the list of jobs, the information will be returned as a collection of job objects. See also [Job API](#Job_API) for syntax of the job object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| job | array of job objects(JSON)/Zero or more job objects(XML) | The collection of job objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
      "jobs" : {
          "job" : [
             {
                "runningReduceAttempts" : 1,
                "reduceProgress" : 100,
                "failedReduceAttempts" : 0,
                "newMapAttempts" : 0,
                "mapsRunning" : 0,
                "state" : "RUNNING",
                "successfulReduceAttempts" : 0,
                "reducesRunning" : 1,
                "acls" : [
                   {
                      "value" : " ",
                      "name" : "mapreduce.job.acl-modify-job"
                   },
                   {
                      "value" : " ",
                      "name" : "mapreduce.job.acl-view-job"
                   }
                ],
                "reducesPending" : 0,
                "user" : "user1",
                "reducesTotal" : 1,
                "mapsCompleted" : 1,
                "startTime" : 1326238769379,
                "id" : "job_1326232085508_4_4",
                "successfulMapAttempts" : 1,
                "runningMapAttempts" : 0,
                "newReduceAttempts" : 0,
                "name" : "Sleep job",
                "mapsPending" : 0,
                "elapsedTime" : 59377,
                "reducesCompleted" : 0,
                "mapProgress" : 100,
                "diagnostics" : "",
                "failedMapAttempts" : 0,
                "killedReduceAttempts" : 0,
                "mapsTotal" : 1,
                "uberized" : false,
                "killedMapAttempts" : 0,
                "finishTime" : 0
             }
         ]
       }
     }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 1214
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobs>
      <job>
        <startTime>1326238769379</startTime>
        <finishTime>0</finishTime>
        <elapsedTime>59416</elapsedTime>
        <id>job_1326232085508_4_4</id>
        <name>Sleep job</name>
        <user>user1</user>
        <state>RUNNING</state>
        <mapsTotal>1</mapsTotal>
        <mapsCompleted>1</mapsCompleted>
        <reducesTotal>1</reducesTotal>
        <reducesCompleted>0</reducesCompleted>
        <mapProgress>100.0</mapProgress>
        <reduceProgress>100.0</reduceProgress>
        <mapsPending>0</mapsPending>
        <mapsRunning>0</mapsRunning>
        <reducesPending>0</reducesPending>
        <reducesRunning>1</reducesRunning>
        <uberized>false</uberized>
        <diagnostics/>
        <newReduceAttempts>0</newReduceAttempts>
        <runningReduceAttempts>1</runningReduceAttempts>
        <failedReduceAttempts>0</failedReduceAttempts>
        <killedReduceAttempts>0</killedReduceAttempts>
        <successfulReduceAttempts>0</successfulReduceAttempts>
        <newMapAttempts>0</newMapAttempts>
        <runningMapAttempts>0</runningMapAttempts>
        <failedMapAttempts>0</failedMapAttempts>
        <killedMapAttempts>0</killedMapAttempts>
        <successfulMapAttempts>1</successfulMapAttempts>
        <acls>
          <name>mapreduce.job.acl-modify-job</name>
          <value> </value>
        </acls>
        <acls>
          <name>mapreduce.job.acl-view-job</name>
          <value> </value>
        </acls>
      </job>
    </jobs>

Job API
-------

A job resource contains information about a particular job that was started by this application master. Certain fields are only accessible if user has permissions - depends on acl settings.

### URI

Use the following URI to obtain a job object, for a job identified by the jobid value.

      * http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/{jobid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *job* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The job id |
| name | string | The job name |
| user | string | The user name |
| state | string | the job state - valid values are: NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL\_WAIT, KILLED, ERROR |
| startTime | long | The time the job started (in ms since epoch) |
| finishTime | long | The time the job finished (in ms since epoch) |
| elapsedTime | long | The elapsed time since job started (in ms) |
| mapsTotal | int | The total number of maps |
| mapsCompleted | int | The number of completed maps |
| reducesTotal | int | The total number of reduces |
| reducesCompleted | int | The number of completed reduces |
| diagnostics | string | A diagnostic message |
| uberized | boolean | Indicates if the job was an uber job - ran completely in the application master |
| mapsPending | int | The number of maps still to be run |
| mapsRunning | int | The number of running maps |
| reducesPending | int | The number of reduces still to be run |
| reducesRunning | int | The number of running reduces |
| newReduceAttempts | int | The number of new reduce attempts |
| runningReduceAttempts | int | The number of running reduce attempts |
| failedReduceAttempts | int | The number of failed reduce attempts |
| killedReduceAttempts | int | The number of killed reduce attempts |
| successfulReduceAttempts | int | The number of successful reduce attempts |
| newMapAttempts | int | The number of new map attempts |
| runningMapAttempts | int | The number of running map attempts |
| failedMapAttempts | int | The number of failed map attempts |
| killedMapAttempts | int | The number of killed map attempts |
| successfulMapAttempts | int | The number of successful map attempts |
| acls | array of acls(json)/zero or more acls objects(xml) | A collection of acls objects |

### Elements of the *acls* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| value | string | The acl value |
| name | string | The acl name |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Server: Jetty(6.1.26)
      Content-Length: 720

Response Body:

    {
       "job" : {
          "runningReduceAttempts" : 1,
          "reduceProgress" : 100,
          "failedReduceAttempts" : 0,
          "newMapAttempts" : 0,
          "mapsRunning" : 0,
          "state" : "RUNNING",
          "successfulReduceAttempts" : 0,
          "reducesRunning" : 1,
          "acls" : [
             {
                "value" : " ",
                "name" : "mapreduce.job.acl-modify-job"
             },
             {
                "value" : " ",
                "name" : "mapreduce.job.acl-view-job"
             }
          ],
          "reducesPending" : 0,
          "user" : "user1",
          "reducesTotal" : 1,
          "mapsCompleted" : 1,
          "startTime" : 1326238769379,
          "id" : "job_1326232085508_4_4",
          "successfulMapAttempts" : 1,
          "runningMapAttempts" : 0,
          "newReduceAttempts" : 0,
          "name" : "Sleep job",
          "mapsPending" : 0,
          "elapsedTime" : 59437,
          "reducesCompleted" : 0,
          "mapProgress" : 100,
          "diagnostics" : "",
          "failedMapAttempts" : 0,
          "killedReduceAttempts" : 0,
          "mapsTotal" : 1,
          "uberized" : false,
          "killedMapAttempts" : 0,
          "finishTime" : 0
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 1201
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <job>
      <startTime>1326238769379</startTime>
      <finishTime>0</finishTime>
      <elapsedTime>59474</elapsedTime>
      <id>job_1326232085508_4_4</id>
      <name>Sleep job</name>
      <user>user1</user>
      <state>RUNNING</state>
      <mapsTotal>1</mapsTotal>
      <mapsCompleted>1</mapsCompleted>
      <reducesTotal>1</reducesTotal>
      <reducesCompleted>0</reducesCompleted>
      <mapProgress>100.0</mapProgress>
      <reduceProgress>100.0</reduceProgress>
      <mapsPending>0</mapsPending>
      <mapsRunning>0</mapsRunning>
      <reducesPending>0</reducesPending>
      <reducesRunning>1</reducesRunning>
      <uberized>false</uberized>
      <diagnostics/>
      <newReduceAttempts>0</newReduceAttempts>
      <runningReduceAttempts>1</runningReduceAttempts>
      <failedReduceAttempts>0</failedReduceAttempts>
      <killedReduceAttempts>0</killedReduceAttempts>
      <successfulReduceAttempts>0</successfulReduceAttempts>
      <newMapAttempts>0</newMapAttempts>
      <runningMapAttempts>0</runningMapAttempts>
      <failedMapAttempts>0</failedMapAttempts>
      <killedMapAttempts>0</killedMapAttempts>
      <successfulMapAttempts>1</successfulMapAttempts>
      <acls>
        <name>mapreduce.job.acl-modify-job</name>
        <value> </value>
      </acls>
      <acls>
        <name>mapreduce.job.acl-view-job</name>    <value> </value>
      </acls>
    </job>

Job Attempts API
----------------

With the job attempts API, you can obtain a collection of resources that represent the job attempts. When you run a GET operation on this resource, you obtain a collection of Job Attempt Objects.

### URI

      * http://history-server-http-address:port/ws/v1/history/jobs/{jobid}/jobattempts

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *jobAttempts* object

When you make a request for the list of job attempts, the information will be returned as an array of job attempt objects.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| jobAttempt | array of job attempt objects(JSON)/zero or more job attempt objects(XML) | The collection of job attempt objects |

### Elements of the *jobAttempt* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The job attempt id |
| nodeId | string | The node id of the node the attempt ran on |
| nodeHttpAddress | string | The node http address of the node the attempt ran on |
| logsLink | string | The http link to the job attempt logs |
| containerId | string | The id of the container for the job attempt |
| startTime | long | The start time of the attempt (in ms since epoch) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/jobattempts

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "jobAttempts" : {
          "jobAttempt" : [
             {
                "nodeId" : "host.domain.com:8041",
                "nodeHttpAddress" : "host.domain.com:8042",
                "startTime" : 1326238773493,
                "id" : 1,
                "logsLink" : "http://host.domain.com:8042/node/containerlogs/container_1326232085508_0004_01_000001",
                "containerId" : "container_1326232085508_0004_01_000001"
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/jobattempts
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 498
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobAttempts>
      <jobAttempt>
        <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
        <nodeId>host.domain.com:8041</nodeId>
        <id>1</id>
        <startTime>1326238773493</startTime>
        <containerId>container_1326232085508_0004_01_000001</containerId>
        <logsLink>http://host.domain.com:8042/node/containerlogs/container_1326232085508_0004_01_000001</logsLink>
      </jobAttempt>
    </jobAttempts>

Job Counters API
----------------

With the job counters API, you can object a collection of resources that represent all the counters for that job.

### URI

      * http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/{jobid}/counters

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *jobCounters* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The job id |
| counterGroup | array of counterGroup objects(JSON)/zero or more counterGroup objects(XML) | A collection of counter group objects |

### Elements of the *counterGroup* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| counterGroupName | string | The name of the counter group |
| counter | array of counter objects(JSON)/zero or more counter objects(XML) | A collection of counter objects |

### Elements of the *counter* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the counter |
| reduceCounterValue | long | The counter value of reduce tasks |
| mapCounterValue | long | The counter value of map tasks |
| totalCounterValue | long | The counter value of all tasks |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/counters

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "jobCounters" : {
          "id" : "job_1326232085508_4_4",
          "counterGroup" : [
             {
                "counterGroupName" : "Shuffle Errors",
                "counter" : [
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "BAD_ID"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "CONNECTION"
                   },
                  {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "IO_ERROR"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "WRONG_LENGTH"
                   },                {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "WRONG_MAP"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "WRONG_REDUCE"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.FileSystemCounter",
                "counter" : [
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 2483,
                      "name" : "FILE_BYTES_READ"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 108763,
                      "name" : "FILE_BYTES_WRITTEN"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "FILE_READ_OPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "FILE_LARGE_READ_OPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "FILE_WRITE_OPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 48,
                      "name" : "HDFS_BYTES_READ"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "HDFS_BYTES_WRITTEN"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1,
                      "name" : "HDFS_READ_OPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "HDFS_LARGE_READ_OPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "HDFS_WRITE_OPS"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.TaskCounter",
                "counter" : [
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1,
                      "name" : "MAP_INPUT_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1200,
                      "name" : "MAP_OUTPUT_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 4800,
                      "name" : "MAP_OUTPUT_BYTES"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 2235,
                      "name" : "MAP_OUTPUT_MATERIALIZED_BYTES"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 48,
                      "name" : "SPLIT_RAW_BYTES"
                   },
                  {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "COMBINE_INPUT_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "COMBINE_OUTPUT_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 460,
                      "name" : "REDUCE_INPUT_GROUPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 2235,
                      "name" : "REDUCE_SHUFFLE_BYTES"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 460,
                      "name" : "REDUCE_INPUT_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "REDUCE_OUTPUT_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1200,
                      "name" : "SPILLED_RECORDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1,
                      "name" : "SHUFFLED_MAPS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "FAILED_SHUFFLE"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1,
                      "name" : "MERGED_MAP_OUTPUTS"
                   },                {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 58,
                      "name" : "GC_TIME_MILLIS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1580,
                      "name" : "CPU_MILLISECONDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 462643200,
                      "name" : "PHYSICAL_MEMORY_BYTES"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 2149728256,
                      "name" : "VIRTUAL_MEMORY_BYTES"
                   },
                  {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 357957632,
                      "name" : "COMMITTED_HEAP_BYTES"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter",
                "counter" : [
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "BYTES_READ"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter",
                "counter" : [
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 0,
                      "name" : "BYTES_WRITTEN"
                   }
                ]
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/counters
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 7027
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobCounters>
      <id>job_1326232085508_4_4</id>
      <counterGroup>
        <counterGroupName>Shuffle Errors</counterGroupName>
        <counter>
          <name>BAD_ID</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>CONNECTION</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>IO_ERROR</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>WRONG_LENGTH</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>WRONG_MAP</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>WRONG_REDUCE</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
      </counterGroup>
      <counterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.FileSystemCounter</counterGroupName>
        <counter>
          <name>FILE_BYTES_READ</name>
          <totalCounterValue>2483</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>FILE_BYTES_WRITTEN</name>
          <totalCounterValue>108763</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>FILE_READ_OPS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>FILE_LARGE_READ_OPS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>FILE_WRITE_OPS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>HDFS_BYTES_READ</name>
          <totalCounterValue>48</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>HDFS_BYTES_WRITTEN</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>HDFS_READ_OPS</name>
          <totalCounterValue>1</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>HDFS_LARGE_READ_OPS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>HDFS_WRITE_OPS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
      </counterGroup>
      <counterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.TaskCounter</counterGroupName>
        <counter>
          <name>MAP_INPUT_RECORDS</name>
          <totalCounterValue>1</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>MAP_OUTPUT_RECORDS</name>
          <totalCounterValue>1200</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>MAP_OUTPUT_BYTES</name>
          <totalCounterValue>4800</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>MAP_OUTPUT_MATERIALIZED_BYTES</name>
          <totalCounterValue>2235</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>SPLIT_RAW_BYTES</name>
          <totalCounterValue>48</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>COMBINE_INPUT_RECORDS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>COMBINE_OUTPUT_RECORDS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>REDUCE_INPUT_GROUPS</name>
          <totalCounterValue>460</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>REDUCE_SHUFFLE_BYTES</name>
          <totalCounterValue>2235</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>REDUCE_INPUT_RECORDS</name>
          <totalCounterValue>460</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>REDUCE_OUTPUT_RECORDS</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>SPILLED_RECORDS</name>
          <totalCounterValue>1200</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>SHUFFLED_MAPS</name>
          <totalCounterValue>1</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>FAILED_SHUFFLE</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>MERGED_MAP_OUTPUTS</name>
          <totalCounterValue>1</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>GC_TIME_MILLIS</name>
          <totalCounterValue>58</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>CPU_MILLISECONDS</name>
          <totalCounterValue>1580</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>PHYSICAL_MEMORY_BYTES</name>
          <totalCounterValue>462643200</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>VIRTUAL_MEMORY_BYTES</name>
          <totalCounterValue>2149728256</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>COMMITTED_HEAP_BYTES</name>
          <totalCounterValue>357957632</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
      </counterGroup>
      <counterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter</counterGroupName>
        <counter>
          <name>BYTES_READ</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>  </counterGroup>  <counterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter</counterGroupName>
        <counter>      <name>BYTES_WRITTEN</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
      </counterGroup>
    </jobCounters>

Job Conf API
------------

A job configuration resource contains information about the job configuration for this job.

### URI

Use the following URI to obtain th job configuration information, from a job identified by the jobid value.

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/conf

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *conf* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| path | string | The path to the job configuration file |
| property | array of the configuration properties(JSON)/zero or more property objects(XML) | Collection of property objects |

### Elements of the *property* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the configuration property |
| value | string | The value of the configuration property |
| source | string | The location this configuration object came from. If there is more then one of these it shows the history with the latest source at the end of the list. |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/conf

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

This is a small snippet of the output as the output if very large. The real output contains every property in your job configuration file.

    {
       "conf" : {
          "path" : "hdfs://host.domain.com:9000/user/user1/.staging/job_1326232085508_0004/job.xml",
          "property" : [
             {
                "value" : "/home/hadoop/hdfs/data",
                "name" : "dfs.datanode.data.dir",
                "source" : ["hdfs-site.xml", "job.xml"]
             },
             {
                "value" : "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer",
                "name" : "hadoop.http.filter.initializers"
                "source" : ["programmatically", "job.xml"]
             },
             {
                "value" : "/home/hadoop/tmp",
                "name" : "mapreduce.cluster.temp.dir"
                "source" : ["mapred-site.xml"]
             },
             ...
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/conf
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 552
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <conf>
      <path>hdfs://host.domain.com:9000/user/user1/.staging/job_1326232085508_0004/job.xml</path>
      <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/hadoop/hdfs/data</value>
        <source>hdfs-site.xml</source>
        <source>job.xml</source>
      </property>
      <property>
        <name>hadoop.http.filter.initializers</name>
        <value>org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer</value>
        <source>programmatically</source>
        <source>job.xml</source>
      </property>
      <property>
        <name>mapreduce.cluster.temp.dir</name>
        <value>/home/hadoop/tmp</value>
        <source>mapred-site.xml</source>
      </property>
      ...
    </conf>

Tasks API
---------

With the tasks API, you can obtain a collection of resources that represent all the tasks for a job. When you run a GET operation on this resource, you obtain a collection of Task Objects.

### URI

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      * type - type of task, valid values are m or r.  m for map task or r for reduce task.

### Elements of the *tasks* object

When you make a request for the list of tasks , the information will be returned as an array of task objects. See also [Task API](#Task_API) for syntax of the task object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| task | array of task objects(JSON)/zero or more task objects(XML) | The collection of task objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "tasks" : {
          "task" : [
             {
                "progress" : 100,
                "elapsedTime" : 2768,
                "state" : "SUCCEEDED",
                "startTime" : 1326238773493,
                "id" : "task_1326232085508_4_4_m_0",
                "type" : "MAP",
                "successfulAttempt" : "attempt_1326232085508_4_4_m_0_0",
                "finishTime" : 1326238776261
             },
             {
                "progress" : 100,
                "elapsedTime" : 0,
                "state" : "RUNNING",
                "startTime" : 1326238777460,
                "id" : "task_1326232085508_4_4_r_0",
                "type" : "REDUCE",
                "successfulAttempt" : "",
                "finishTime" : 0
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 603
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <tasks>
      <task>
        <startTime>1326238773493</startTime>
        <finishTime>1326238776261</finishTime>
        <elapsedTime>2768</elapsedTime>
        <progress>100.0</progress>
        <id>task_1326232085508_4_4_m_0</id>
        <state>SUCCEEDED</state>
        <type>MAP</type>
        <successfulAttempt>attempt_1326232085508_4_4_m_0_0</successfulAttempt>
      </task>
      <task>
        <startTime>1326238777460</startTime>
        <finishTime>0</finishTime>
        <elapsedTime>0</elapsedTime>
        <progress>100.0</progress>
        <id>task_1326232085508_4_4_r_0</id>
        <state>RUNNING</state>
        <type>REDUCE</type>
        <successfulAttempt/>
      </task>
    </tasks>

Task API
--------

A Task resource contains information about a particular task within a job.

### URI

Use the following URI to obtain an Task Object, from a task identified by the taskid value.

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *task* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task id |
| state | string | The state of the task - valid values are: NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL\_WAIT, KILLED |
| type | string | The task type - MAP or REDUCE |
| successfulAttempt | string | The the id of the last successful attempt |
| progress | float | The progress of the task as a percent |
| startTime | long | The time in which the task started (in ms since epoch) |
| finishTime | long | The time in which the task finished (in ms since epoch) |
| elapsedTime | long | The elapsed time since the application started (in ms) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "task" : {
          "progress" : 100,
          "elapsedTime" : 0,
          "state" : "RUNNING",
          "startTime" : 1326238777460,
          "id" : "task_1326232085508_4_4_r_0",
          "type" : "REDUCE",
          "successfulAttempt" : "",
          "finishTime" : 0
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 299
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <task>
      <startTime>1326238777460</startTime>
      <finishTime>0</finishTime>
      <elapsedTime>0</elapsedTime>
      <progress>100.0</progress>
      <id>task_1326232085508_4_4_r_0</id>
      <state>RUNNING</state>
      <type>REDUCE</type>
      <successfulAttempt/>
    </task>

Task Counters API
-----------------

With the task counters API, you can object a collection of resources that represent all the counters for that task.

### URI

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *jobTaskCounters* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task id |
| taskcounterGroup | array of counterGroup objects(JSON)/zero or more counterGroup objects(XML) | A collection of counter group objects |

### Elements of the *counterGroup* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| counterGroupName | string | The name of the counter group |
| counter | array of counter objects(JSON)/zero or more counter objects(XML) | A collection of counter objects |

### Elements of the *counter* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the counter |
| value | long | The value of the counter |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/counters

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "jobTaskCounters" : {
          "id" : "task_1326232085508_4_4_r_0",
          "taskCounterGroup" : [
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.FileSystemCounter",
                "counter" : [
                   {
                      "value" : 2363,
                      "name" : "FILE_BYTES_READ"
                   },
                   {
                      "value" : 54372,
                      "name" : "FILE_BYTES_WRITTEN"
                   },
                   {
                      "value" : 0,
                      "name" : "FILE_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "FILE_LARGE_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "FILE_WRITE_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_BYTES_READ"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_BYTES_WRITTEN"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_LARGE_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_WRITE_OPS"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.TaskCounter",
                "counter" : [
                   {
                      "value" : 0,
                      "name" : "COMBINE_INPUT_RECORDS"
                   },
                   {
                      "value" : 0,
                      "name" : "COMBINE_OUTPUT_RECORDS"
                   },
                   {
                      "value" : 460,
                      "name" : "REDUCE_INPUT_GROUPS"
                   },
                   {
                      "value" : 2235,
                      "name" : "REDUCE_SHUFFLE_BYTES"
                   },
                   {
                      "value" : 460,
                      "name" : "REDUCE_INPUT_RECORDS"
                   },
                   {
                      "value" : 0,
                      "name" : "REDUCE_OUTPUT_RECORDS"
                   },
                   {
                      "value" : 0,
                      "name" : "SPILLED_RECORDS"
                   },
                   {
                      "value" : 1,
                      "name" : "SHUFFLED_MAPS"
                   },
                   {
                      "value" : 0,
                      "name" : "FAILED_SHUFFLE"
                   },
                   {
                      "value" : 1,
                      "name" : "MERGED_MAP_OUTPUTS"
                   },
                   {
                      "value" : 26,
                      "name" : "GC_TIME_MILLIS"
                   },
                   {
                      "value" : 860,
                      "name" : "CPU_MILLISECONDS"
                   },
                   {
                      "value" : 107839488,
                      "name" : "PHYSICAL_MEMORY_BYTES"
                   },
                   {
                      "value" : 1123147776,
                      "name" : "VIRTUAL_MEMORY_BYTES"
                   },
                   {
                      "value" : 57475072,
                      "name" : "COMMITTED_HEAP_BYTES"
                   }
                ]
             },
             {
                "counterGroupName" : "Shuffle Errors",
                "counter" : [
                   {
                      "value" : 0,
                      "name" : "BAD_ID"
                   },
                   {
                      "value" : 0,
                      "name" : "CONNECTION"
                   },
                   {
                      "value" : 0,
                      "name" : "IO_ERROR"
                   },
                   {
                      "value" : 0,
                      "name" : "WRONG_LENGTH"
                   },
                   {
                      "value" : 0,
                      "name" : "WRONG_MAP"
                   },
                   {
                      "value" : 0,
                      "name" : "WRONG_REDUCE"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter",
                "counter" : [
                   {
                      "value" : 0,
                      "name" : "BYTES_WRITTEN"
                   }
                ]
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/counters
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2660
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskCounters>
      <id>task_1326232085508_4_4_r_0</id>
      <taskCounterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.FileSystemCounter</counterGroupName>
        <counter>
          <name>FILE_BYTES_READ</name>
          <value>2363</value>
        </counter>
        <counter>
          <name>FILE_BYTES_WRITTEN</name>
          <value>54372</value>
        </counter>
        <counter>
          <name>FILE_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>FILE_LARGE_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>FILE_WRITE_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_BYTES_READ</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_BYTES_WRITTEN</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_LARGE_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_WRITE_OPS</name>
          <value>0</value>
        </counter>
      </taskCounterGroup>
      <taskCounterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.TaskCounter</counterGroupName>
        <counter>
          <name>COMBINE_INPUT_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>COMBINE_OUTPUT_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>REDUCE_INPUT_GROUPS</name>
          <value>460</value>
        </counter>
        <counter>
          <name>REDUCE_SHUFFLE_BYTES</name>
          <value>2235</value>
        </counter>
        <counter>
          <name>REDUCE_INPUT_RECORDS</name>
          <value>460</value>
        </counter>
        <counter>
          <name>REDUCE_OUTPUT_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>SPILLED_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>SHUFFLED_MAPS</name>
          <value>1</value>
        </counter>
        <counter>
          <name>FAILED_SHUFFLE</name>
          <value>0</value>
        </counter>
        <counter>
          <name>MERGED_MAP_OUTPUTS</name>
          <value>1</value>
        </counter>
        <counter>
          <name>GC_TIME_MILLIS</name>
          <value>26</value>
        </counter>
        <counter>
          <name>CPU_MILLISECONDS</name>
          <value>860</value>
        </counter>
        <counter>
          <name>PHYSICAL_MEMORY_BYTES</name>
          <value>107839488</value>
        </counter>
        <counter>
          <name>VIRTUAL_MEMORY_BYTES</name>
          <value>1123147776</value>
        </counter>
        <counter>
          <name>COMMITTED_HEAP_BYTES</name>
          <value>57475072</value>
        </counter>
      </taskCounterGroup>
      <taskCounterGroup>
        <counterGroupName>Shuffle Errors</counterGroupName>
        <counter>
          <name>BAD_ID</name>
          <value>0</value>
        </counter>
        <counter>
          <name>CONNECTION</name>
          <value>0</value>
        </counter>
        <counter>
          <name>IO_ERROR</name>
          <value>0</value>
        </counter>
        <counter>
          <name>WRONG_LENGTH</name>
          <value>0</value>
        </counter>
        <counter>
          <name>WRONG_MAP</name>
          <value>0</value>
        </counter>
        <counter>
          <name>WRONG_REDUCE</name>
          <value>0</value>
        </counter>
      </taskCounterGroup>
      <taskCounterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter</counterGroupName>
        <counter>
          <name>BYTES_WRITTEN</name>
          <value>0</value>
        </counter>
      </taskCounterGroup>
    </jobTaskCounters>

Task Attempts API
-----------------

With the task attempts API, you can obtain a collection of resources that represent a task attempt within a job. When you run a GET operation on this resource, you obtain a collection of Task Attempt Objects.

### URI

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *taskAttempts* object

When you make a request for the list of task attempts, the information will be returned as an array of task attempt objects. See also [Task Attempt API](#Task_Attempt_API) for syntax of the task object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| taskAttempt | array of task attempt objects(JSON)/zero or more task attempt objects(XML) | The collection of task attempt objects |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/attempts

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "taskAttempts" : {
          "taskAttempt" : [
             {
                "elapsedMergeTime" : 47,
                "shuffleFinishTime" : 1326238780052,
                "assignedContainerId" : "container_1326232085508_0004_01_000003",
                "progress" : 100,
                "elapsedTime" : 0,
                "state" : "RUNNING",
                "elapsedShuffleTime" : 2592,
                "mergeFinishTime" : 1326238780099,
                "rack" : "/98.139.92.0",
                "elapsedReduceTime" : 0,
                "nodeHttpAddress" : "host.domain.com:8042",
                "type" : "REDUCE",
                "startTime" : 1326238777460,
                "id" : "attempt_1326232085508_4_4_r_0_0",
                "finishTime" : 0
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/attempts
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 807
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <taskAttempts>
      <taskAttempt xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="reduceTaskAttemptInfo">
        <startTime>1326238777460</startTime>
        <finishTime>0</finishTime>
        <elapsedTime>0</elapsedTime>
        <progress>100.0</progress>
        <id>attempt_1326232085508_4_4_r_0_0</id>
        <rack>/98.139.92.0</rack>
        <state>RUNNING</state>
        <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
        <type>REDUCE</type>
        <assignedContainerId>container_1326232085508_0004_01_000003</assignedContainerId>
        <shuffleFinishTime>1326238780052</shuffleFinishTime>
        <mergeFinishTime>1326238780099</mergeFinishTime>
        <elapsedShuffleTime>2592</elapsedShuffleTime>
        <elapsedMergeTime>47</elapsedMergeTime>
        <elapsedReduceTime>0</elapsedReduceTime>
      </taskAttempt>
    </taskAttempts>

Task Attempt API
----------------

A Task Attempt resource contains information about a particular task attempt within a job.

### URI

Use the following URI to obtain an Task Attempt Object, from a task identified by the attemptid value.

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *taskAttempt* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task id |
| rack | string | The rack |
| state | string | The state of the task attempt - valid values are: NEW, UNASSIGNED, ASSIGNED, RUNNING, COMMIT\_PENDING, SUCCESS\_CONTAINER\_CLEANUP, SUCCEEDED, FAIL\_CONTAINER\_CLEANUP, FAIL\_TASK\_CLEANUP, FAILED, KILL\_CONTAINER\_CLEANUP, KILL\_TASK\_CLEANUP, KILLED |
| type | string | The type of task |
| assignedContainerId | string | The container id this attempt is assigned to |
| nodeHttpAddress | string | The http address of the node this task attempt ran on |
| diagnostics | string | The diagnostics message |
| progress | float | The progress of the task attempt as a percent |
| startTime | long | The time in which the task attempt started (in ms since epoch) |
| finishTime | long | The time in which the task attempt finished (in ms since epoch) |
| elapsedTime | long | The elapsed time since the task attempt started (in ms) |

For reduce task attempts you also have the following fields:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| shuffleFinishTime | long | The time at which shuffle finished (in ms since epoch) |
| mergeFinishTime | long | The time at which merge finished (in ms since epoch) |
| elapsedShuffleTime | long | The time it took for the shuffle phase to complete (time in ms between reduce task start and shuffle finish) |
| elapsedMergeTime | long | The time it took for the merge phase to complete (time in ms between the shuffle finish and merge finish) |
| elapsedReduceTime | long | The time it took for the reduce phase to complete (time in ms between merge finish to end of reduce task) |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/attempts/attempt_1326232085508_4_4_r_0_0

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "taskAttempt" : {
          "elapsedMergeTime" : 47,
          "shuffleFinishTime" : 1326238780052,
          "assignedContainerId" : "container_1326232085508_0004_01_000003",
          "progress" : 100,
          "elapsedTime" : 0,
          "state" : "RUNNING",
          "elapsedShuffleTime" : 2592,
          "mergeFinishTime" : 1326238780099,
          "rack" : "/98.139.92.0",
          "elapsedReduceTime" : 0,
          "nodeHttpAddress" : "host.domain.com:8042",
          "startTime" : 1326238777460,
          "id" : "attempt_1326232085508_4_4_r_0_0",
          "type" : "REDUCE",
          "finishTime" : 0
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/attempts/attempt_1326232085508_4_4_r_0_0
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 691
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <taskAttempt>
      <startTime>1326238777460</startTime>
      <finishTime>0</finishTime>
      <elapsedTime>0</elapsedTime>
      <progress>100.0</progress>
      <id>attempt_1326232085508_4_4_r_0_0</id>
      <rack>/98.139.92.0</rack>
      <state>RUNNING</state>
      <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
      <type>REDUCE</type>
      <assignedContainerId>container_1326232085508_0004_01_000003</assignedContainerId>
      <shuffleFinishTime>1326238780052</shuffleFinishTime>
      <mergeFinishTime>1326238780099</mergeFinishTime>
      <elapsedShuffleTime>2592</elapsedShuffleTime>
      <elapsedMergeTime>47</elapsedMergeTime>
      <elapsedReduceTime>0</elapsedReduceTime>
    </taskAttempt>

Task Attempt State API
-------------------------
With the task attempt state API, you can query the state of a submitted task attempt as well kill a running task attempt by modifying the state of a running task attempt using a PUT request with the state set to "KILLED". To perform the PUT operation, authentication has to be setup for the AM web services. In addition, you must be authorized to kill the task attempt. Currently you can only change the state to "KILLED"; an attempt to change the state to any other results in a 400 error response. Examples of the unauthorized and bad request errors are below. When you carry out a successful PUT, the iniital response may be a 202. You can confirm that the app is killed by repeating the PUT request until you get a 200, querying the state using the GET method or querying for task attempt information and checking the state. In the examples below, we repeat the PUT request and get a 200 response.

Please note that in order to kill a task attempt, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an "UNAUTHORIZED" response.

This feature is currently in the alpha stage and may change in the future.

### URI

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/state

### HTTP Operations Supported

      * GET
      * POST

### Query Parameters Supported

      None

### Elements of *jobTaskAttemptState* object

When you make a request for the state of an app, the information returned has the following fields

| Item | Data Type | Description |
|:---- |:---- |:---- |
| state | string | The application state - can be one of "NEW", "STARTING", "RUNNING", "COMMIT_PENDING", "SUCCEEDED", "FAILED", "KILLED" |

### Response Examples

**JSON responses**

HTTP Request

      GET http://proxy-http-address:port/proxy/application_1429692837321_0001/ws/v1/mapreduce/jobs/job_1429692837321_0001/tasks/task_1429692837321_0001_m_000000/attempts/attempt_1429692837321_0001_m_000000_0/state

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Server: Jetty(6.1.26)
    Content-Length: 20

Response Body:

    {
      "state":"STARTING"
    }

HTTP Request

      PUT http://proxy-http-address:port/proxy/application_1429692837321_0001/ws/v1/mapreduce/jobs/job_1429692837321_0001/tasks/task_1429692837321_0001_m_000000/attempts/attempt_1429692837321_0001_m_000000_0/state

Request Body:

    {
      "state":"KILLED"
    }

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/json
    Server: Jetty(6.1.26)
    Content-Length: 18

Response Body:

    {
      "state":"KILLED"
    }

**XML responses**

HTTP Request

      GET http://proxy-http-address:port/proxy/application_1429692837321_0001/ws/v1/mapreduce/jobs/job_1429692837321_0001/tasks/task_1429692837321_0001_m_000000/attempts/attempt_1429692837321_0001_m_000000_0/state

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Server: Jetty(6.1.26)
    Content-Length: 121

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptState>
      <state>STARTING</state>
    </jobTaskAttemptState>

HTTP Request

      PUT http://proxy-http-address:port/proxy/application_1429692837321_0001/ws/v1/mapreduce/jobs/job_1429692837321_0001/tasks/task_1429692837321_0001_m_000000/attempts/attempt_1429692837321_0001_m_000000_0/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptState>
      <state>KILLED</state>
    </jobTaskAttemptState>

Response Header:

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Server: Jetty(6.1.26)
    Content-Length: 121

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptState>
      <state>KILLED</state>
    </jobTaskAttemptState>

**Unauthorized Error Response**

HTTP Request

      PUT http://proxy-http-address:port/proxy/application_1429692837321_0001/ws/v1/mapreduce/jobs/job_1429692837321_0001/tasks/task_1429692837321_0001_m_000000/attempts/attempt_1429692837321_0001_m_000000_0/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptState>
      <state>KILLED</state>
    </jobTaskAttemptState>

Response Header:

    HTTP/1.1 403 Unauthorized
    Content-Type: application/json
    Server: Jetty(6.1.26)

**Bad Request Error Response**

HTTP Request

      PUT http://proxy-http-address:port/proxy/application_1429692837321_0001/ws/v1/mapreduce/jobs/job_1429692837321_0001/tasks/task_1429692837321_0001_m_000000/attempts/attempt_1429692837321_0001_m_000000_0/state

Request Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptState>
      <state>RUNNING</state>
    </jobTaskAttemptState>

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

Task Attempt Counters API
-------------------------

With the task attempt counters API, you can object a collection of resources that represent al the counters for that task attempt.

### URI

      * http://proxy-http-address:port/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *jobTaskAttemptCounters* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task attempt id |
| taskAttemptcounterGroup | array of task attempt counterGroup objects(JSON)/zero or more task attempt counterGroup objects(XML) | A collection of task attempt counter group objects |

### Elements of the *taskAttemptCounterGroup* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| counterGroupName | string | The name of the counter group |
| counter | array of counter objects(JSON)/zero or more counter objects(XML) | A collection of counter objects |

### Elements of the *counter* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the counter |
| value | long | The value of the counter |

### Response Examples

**JSON response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/attempts/attempt_1326232085508_4_4_r_0_0/counters

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "jobTaskAttemptCounters" : {
          "taskAttemptCounterGroup" : [
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.FileSystemCounter",
                "counter" : [
                   {
                      "value" : 2363,
                      "name" : "FILE_BYTES_READ"
                   },
                   {
                      "value" : 54372,
                      "name" : "FILE_BYTES_WRITTEN"
                   },
                   {
                      "value" : 0,
                      "name" : "FILE_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "FILE_LARGE_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "FILE_WRITE_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_BYTES_READ"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_BYTES_WRITTEN"
                   },
                  {
                      "value" : 0,
                      "name" : "HDFS_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_LARGE_READ_OPS"
                   },
                   {
                      "value" : 0,
                      "name" : "HDFS_WRITE_OPS"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.TaskCounter",
                "counter" : [
                   {
                      "value" : 0,
                      "name" : "COMBINE_INPUT_RECORDS"
                   },
                   {
                      "value" : 0,
                      "name" : "COMBINE_OUTPUT_RECORDS"
                   },
                   {
                      "value" : 460,
                      "name" : "REDUCE_INPUT_GROUPS"
                   },
                   {
                      "value" : 2235,
                      "name" : "REDUCE_SHUFFLE_BYTES"
                   },
                   {
                      "value" : 460,
                      "name" : "REDUCE_INPUT_RECORDS"
                   },
                   {
                      "value" : 0,
                      "name" : "REDUCE_OUTPUT_RECORDS"
                   },
                   {
                      "value" : 0,
                      "name" : "SPILLED_RECORDS"
                   },
                   {
                      "value" : 1,
                      "name" : "SHUFFLED_MAPS"
                   },
                   {
                      "value" : 0,
                      "name" : "FAILED_SHUFFLE"
                   },
                   {
                      "value" : 1,
                      "name" : "MERGED_MAP_OUTPUTS"
                   },
                   {
                      "value" : 26,
                      "name" : "GC_TIME_MILLIS"
                   },
                   {
                      "value" : 860,
                      "name" : "CPU_MILLISECONDS"
                   },
                   {
                      "value" : 107839488,
                      "name" : "PHYSICAL_MEMORY_BYTES"
                   },
                   {
                      "value" : 1123147776,
                      "name" : "VIRTUAL_MEMORY_BYTES"
                   },
                   {
                      "value" : 57475072,
                      "name" : "COMMITTED_HEAP_BYTES"
                   }
                ]
             },
             {
                "counterGroupName" : "Shuffle Errors",
                "counter" : [
                   {
                      "value" : 0,
                      "name" : "BAD_ID"
                   },
                   {
                      "value" : 0,
                      "name" : "CONNECTION"
                   },
                   {
                      "value" : 0,
                      "name" : "IO_ERROR"
                   },
                   {
                      "value" : 0,
                      "name" : "WRONG_LENGTH"
                   },
                   {
                      "value" : 0,
                      "name" : "WRONG_MAP"
                   },
                   {
                      "value" : 0,
                      "name" : "WRONG_REDUCE"
                   }
                ]
             },
             {
                "counterGroupName" : "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter",
                "counter" : [
                   {
                      "value" : 0,
                      "name" : "BYTES_WRITTEN"
                   }
                ]
             }
          ],
          "id" : "attempt_1326232085508_4_4_r_0_0"
       }
    }

**XML response**

HTTP Request:

      GET http://proxy-http-address:port/proxy/application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/tasks/task_1326232085508_4_4_r_0/attempts/attempt_1326232085508_4_4_r_0_0/counters
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2735
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptCounters>
      <id>attempt_1326232085508_4_4_r_0_0</id>
      <taskAttemptCounterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.FileSystemCounter</counterGroupName>
        <counter>
          <name>FILE_BYTES_READ</name>
          <value>2363</value>
        </counter>
        <counter>
          <name>FILE_BYTES_WRITTEN</name>
          <value>54372</value>
        </counter>
        <counter>
          <name>FILE_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>FILE_LARGE_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>FILE_WRITE_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_BYTES_READ</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_BYTES_WRITTEN</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_LARGE_READ_OPS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>HDFS_WRITE_OPS</name>
          <value>0</value>
        </counter>
      </taskAttemptCounterGroup>
      <taskAttemptCounterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.TaskCounter</counterGroupName>
        <counter>
          <name>COMBINE_INPUT_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>COMBINE_OUTPUT_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>REDUCE_INPUT_GROUPS</name>
          <value>460</value>
        </counter>
        <counter>
          <name>REDUCE_SHUFFLE_BYTES</name>
          <value>2235</value>
        </counter>
        <counter>
          <name>REDUCE_INPUT_RECORDS</name>
          <value>460</value>
        </counter>
        <counter>
          <name>REDUCE_OUTPUT_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>SPILLED_RECORDS</name>
          <value>0</value>
        </counter>
        <counter>
          <name>SHUFFLED_MAPS</name>
          <value>1</value>
        </counter>
        <counter>
          <name>FAILED_SHUFFLE</name>
          <value>0</value>
        </counter>
        <counter>
          <name>MERGED_MAP_OUTPUTS</name>
          <value>1</value>
        </counter>
        <counter>
          <name>GC_TIME_MILLIS</name>
          <value>26</value>
        </counter>
        <counter>
          <name>CPU_MILLISECONDS</name>
          <value>860</value>
        </counter>
        <counter>
          <name>PHYSICAL_MEMORY_BYTES</name>
          <value>107839488</value>
        </counter>
        <counter>
          <name>VIRTUAL_MEMORY_BYTES</name>
          <value>1123147776</value>
        </counter>
        <counter>
          <name>COMMITTED_HEAP_BYTES</name>
          <value>57475072</value>
        </counter>
      </taskAttemptCounterGroup>
      <taskAttemptCounterGroup>
        <counterGroupName>Shuffle Errors</counterGroupName>
        <counter>
          <name>BAD_ID</name>
          <value>0</value>
        </counter>
        <counter>
          <name>CONNECTION</name>
          <value>0</value>
        </counter>
        <counter>
          <name>IO_ERROR</name>
          <value>0</value>
        </counter>
        <counter>
          <name>WRONG_LENGTH</name>
          <value>0</value>
        </counter>
        <counter>
          <name>WRONG_MAP</name>
          <value>0</value>
        </counter>
        <counter>
          <name>WRONG_REDUCE</name>
          <value>0</value>
        </counter>
      </taskAttemptCounterGroup>
      <taskAttemptCounterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter</counterGroupName>
        <counter>
          <name>BYTES_WRITTEN</name>
          <value>0</value>
        </counter>
      </taskAttemptCounterGroup>
    </jobTaskAttemptCounters>
