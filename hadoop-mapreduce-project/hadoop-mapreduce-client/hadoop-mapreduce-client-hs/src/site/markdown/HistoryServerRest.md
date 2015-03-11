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

MapReduce History Server REST API's.
====================================

* [MapReduce History Server REST API's.](#MapReduce_History_Server_REST_APIs.)
    * [Overview](#Overview)
    * [History Server Information API](#History_Server_Information_API)
        * [URI](#URI)
        * [HTTP Operations Supported](#HTTP_Operations_Supported)
        * [Query Parameters Supported](#Query_Parameters_Supported)
        * [Elements of the historyInfo object](#Elements_of_the_historyInfo_object)
        * [Response Examples](#Response_Examples)
    * [MapReduce API's](#MapReduce_APIs)
        * [Jobs API](#Jobs_API)
        * [Job API](#Job_API)
        * [Elements of the acls object](#Elements_of_the_acls_object)
        * [Job Attempts API](#Job_Attempts_API)
        * [Job Counters API](#Job_Counters_API)
        * [Job Conf API](#Job_Conf_API)
        * [Tasks API](#Tasks_API)
        * [Task API](#Task_API)
        * [Task Counters API](#Task_Counters_API)
        * [Task Attempts API](#Task_Attempts_API)
        * [Task Attempt API](#Task_Attempt_API)
        * [Task Attempt Counters API](#Task_Attempt_Counters_API)

Overview
--------

The history server REST API's allow the user to get status on finished applications.

History Server Information API
------------------------------

The history server information resource provides overall information about the history server.

### URI

Both of the following URI's give you the history server information, from an application id identified by the appid value.

      * http://<history server http address:port>/ws/v1/history
      * http://<history server http address:port>/ws/v1/history/info

### HTTP Operations Supported

      * GET

### Query Parameters Supported

      None

### Elements of the *historyInfo* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| startedOn | long | The time the history server was started (in ms since epoch) |
| hadoopVersion | string | Version of hadoop common |
| hadoopBuildVersion | string | Hadoop common build string with build version, user, and checksum |
| hadoopVersionBuiltOn | string | Timestamp when hadoop common was built |

### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/info

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "historyInfo" : {
          "startedOn":1353512830963,
          "hadoopVersionBuiltOn" : "Wed Jan 11 21:18:36 UTC 2012",
          "hadoopBuildVersion" : "0.23.1-SNAPSHOT from 1230253 by user1 source checksum bb6e554c6d50b0397d826081017437a7",
          "hadoopVersion" : "0.23.1-SNAPSHOT"
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/info
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 330
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <historyInfo>
      <startedOn>1353512830963</startedOn>
      <hadoopVersion>0.23.1-SNAPSHOT</hadoopVersion>
      <hadoopBuildVersion>0.23.1-SNAPSHOT from 1230253 by user1 source checksum bb6e554c6d50b0397d826081017437a7</hadoopBuildVersion>
      <hadoopVersionBuiltOn>Wed Jan 11 21:18:36 UTC 2012</hadoopVersionBuiltOn>
    </historyInfo>

MapReduce API's
---------------

The following list of resources apply to MapReduce.

### Jobs API

The jobs resource provides a list of the MapReduce jobs that have finished. It does not currently return a full list of parameters

#### URI

      *  http://<history server http address:port>/ws/v1/history/mapreduce/jobs

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

Multiple paramters can be specified. The started and finished times have a begin and end parameter to allow you to specify ranges. For example, one could request all jobs that started between 1:00am and 2:00pm on 12/19/2011 with startedTimeBegin=1324256400&startedTimeEnd=1324303200. If the Begin parameter is not specfied, it defaults to 0, and if the End parameter is not specified, it defaults to infinity.

      * user - user name
      * state - the job state
      * queue - queue name
      * limit - total number of app objects to be returned
      * startedTimeBegin - jobs with start time beginning with this time, specified in ms since epoch
      * startedTimeEnd - jobs with start time ending with this time, specified in ms since epoch
      * finishedTimeBegin - jobs with finish time beginning with this time, specified in ms since epoch
      * finishedTimeEnd - jobs with finish time ending with this time, specified in ms since epoch

#### Elements of the *jobs* object

When you make a request for the list of jobs, the information will be returned as an array of job objects. See also
[Job API](#Job_API)
for syntax of the job object. Except this is a subset of a full job. Only startTime, finishTime, id, name, queue, user, state, mapsTotal, mapsCompleted, reducesTotal, and reducesCompleted are returned.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| job | array of job objects(json)/zero or more job objects(XML) | The collection of job objects |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs

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
                "submitTime" : 1326381344449,
                "state" : "SUCCEEDED",
                "user" : "user1",
                "reducesTotal" : 1,
                "mapsCompleted" : 1,
                "startTime" : 1326381344489,
                "id" : "job_1326381300833_1_1",
                "name" : "word count",
                "reducesCompleted" : 1,
                "mapsTotal" : 1,
                "queue" : "default",
                "finishTime" : 1326381356010
             },
             {
                "submitTime" : 1326381446500
                "state" : "SUCCEEDED",
                "user" : "user1",
                "reducesTotal" : 1,
                "mapsCompleted" : 1,
                "startTime" : 1326381446529,
                "id" : "job_1326381300833_2_2",
                "name" : "Sleep job",
                "reducesCompleted" : 1,
                "mapsTotal" : 1,
                "queue" : "default",
                "finishTime" : 1326381582106
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 1922
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobs>
      <job>
        <submitTime>1326381344449</submitTime>
        <startTime>1326381344489</startTime>
        <finishTime>1326381356010</finishTime>
        <id>job_1326381300833_1_1</id>
        <name>word count</name>
        <queue>default</queue>
        <user>user1</user>
        <state>SUCCEEDED</state>
        <mapsTotal>1</mapsTotal>
        <mapsCompleted>1</mapsCompleted>
        <reducesTotal>1</reducesTotal>
        <reducesCompleted>1</reducesCompleted>
      </job>
      <job>
        <submitTime>1326381446500</submitTime>
        <startTime>1326381446529</startTime>
        <finishTime>1326381582106</finishTime>
        <id>job_1326381300833_2_2</id>
        <name>Sleep job</name>
        <queue>default</queue>
        <user>user1</user>
        <state>SUCCEEDED</state>
        <mapsTotal>1</mapsTotal>
        <mapsCompleted>1</mapsCompleted>
        <reducesTotal>1</reducesTotal>
        <reducesCompleted>1</reducesCompleted>
      </job>
    </jobs>

### Job API

A Job resource contains information about a particular job identified by jobid.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *job* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The job id |
| name | string | The job name |
| queue | string | The queue the job was submitted to |
| user | string | The user name |
| state | string | the job state - valid values are: NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL\_WAIT, KILLED, ERROR |
| diagnostics | string | A diagnostic message |
| submitTime | long | The time the job submitted (in ms since epoch) |
| startTime | long | The time the job started (in ms since epoch) |
| finishTime | long | The time the job finished (in ms since epoch) |
| mapsTotal | int | The total number of maps |
| mapsCompleted | int | The number of completed maps |
| reducesTotal | int | The total number of reduces |
| reducesCompleted | int | The number of completed reduces |
| uberized | boolean | Indicates if the job was an uber job - ran completely in the application master |
| avgMapTime | long | The average time of a map task (in ms) |
| avgReduceTime | long | The average time of the reduce (in ms) |
| avgShuffleTime | long | The average time of the shuffle (in ms) |
| avgMergeTime | long | The average time of the merge (in ms) |
| failedReduceAttempts | int | The number of failed reduce attempts |
| killedReduceAttempts | int | The number of killed reduce attempts |
| successfulReduceAttempts | int | The number of successful reduce attempts |
| failedMapAttempts | int | The number of failed map attempts |
| killedMapAttempts | int | The number of killed map attempts |
| successfulMapAttempts | int | The number of successful map attempts |
| acls | array of acls(json)/zero or more acls objects(xml) | A collection of acls objects |

### Elements of the *acls* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| value | string | The acl value |
| name | string | The acl name |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Server: Jetty(6.1.26)
      Content-Length: 720

Response Body:

    {
       "job" : {
          "submitTime":  1326381446500,
          "avgReduceTime" : 124961,
          "failedReduceAttempts" : 0,
          "state" : "SUCCEEDED",
          "successfulReduceAttempts" : 1,
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
          "user" : "user1",
          "reducesTotal" : 1,
          "mapsCompleted" : 1,
          "startTime" : 1326381446529,
          "id" : "job_1326381300833_2_2",
          "avgMapTime" : 2638,
          "successfulMapAttempts" : 1,
          "name" : "Sleep job",
          "avgShuffleTime" : 2540,
          "reducesCompleted" : 1,
          "diagnostics" : "",
          "failedMapAttempts" : 0,
          "avgMergeTime" : 2589,
          "killedReduceAttempts" : 0,
          "mapsTotal" : 1,
          "queue" : "default",
          "uberized" : false,
          "killedMapAttempts" : 0,
          "finishTime" : 1326381582106
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 983
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <job>
      <submitTime>1326381446500</submitTime>
      <startTime>1326381446529</startTime>
      <finishTime>1326381582106</finishTime>
      <id>job_1326381300833_2_2</id>
      <name>Sleep job</name>
      <queue>default</queue>
      <user>user1</user>
      <state>SUCCEEDED</state>
      <mapsTotal>1</mapsTotal>
      <mapsCompleted>1</mapsCompleted>
      <reducesTotal>1</reducesTotal>
      <reducesCompleted>1</reducesCompleted>
      <uberized>false</uberized>
      <diagnostics/>
      <avgMapTime>2638</avgMapTime>
      <avgReduceTime>124961</avgReduceTime>
      <avgShuffleTime>2540</avgShuffleTime>
      <avgMergeTime>2589</avgMergeTime>
      <failedReduceAttempts>0</failedReduceAttempts>
      <killedReduceAttempts>0</killedReduceAttempts>
      <successfulReduceAttempts>1</successfulReduceAttempts>
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

### Job Attempts API

With the job attempts API, you can obtain a collection of resources that represent a job attempt. When you run a GET operation on this resource, you obtain a collection of Job Attempt Objects.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/jobattempts

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *jobAttempts* object

When you make a request for the list of job attempts, the information will be returned as an array of job attempt objects.

jobAttempts:

| Item | Data Type | Description |
|:---- |:---- |:---- |
| jobAttempt | array of job attempt objects(JSON)/zero or more job attempt objects(XML) | The collection of job attempt objects |

#### Elements of the *jobAttempt* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The job attempt id |
| nodeId | string | The node id of the node the attempt ran on |
| nodeHttpAddress | string | The node http address of the node the attempt ran on |
| logsLink | string | The http link to the job attempt logs |
| containerId | string | The id of the container for the job attempt |
| startTime | long | The start time of the attempt (in ms since epoch) |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/jobattempts

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
                "startTime" : 1326381444693,
                "id" : 1,
                "logsLink" : "http://host.domain.com:19888/jobhistory/logs/host.domain.com:8041/container_1326381300833_0002_01_000001/job_1326381300833_2_2/user1",
                "containerId" : "container_1326381300833_0002_01_000001"
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/jobattmpts
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 575
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobAttempts>
      <jobAttempt>
        <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
        <nodeId>host.domain.com:8041</nodeId>
        <id>1</id>
        <startTime>1326381444693</startTime>
        <containerId>container_1326381300833_0002_01_000001</containerId>
        <logsLink>http://host.domain.com:19888/jobhistory/logs/host.domain.com:8041/container_1326381300833_0002_01_000001/job_1326381300833_2_2/user1</logsLink>
      </jobAttempt>
    </jobAttempts>

### Job Counters API

With the job counters API, you can object a collection of resources that represent al the counters for that job.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/counters

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *jobCounters* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The job id |
| counterGroup | array of counterGroup objects(JSON)/zero or more counterGroup objects(XML) | A collection of counter group objects |

#### Elements of the *counterGroup* objecs

| Item | Data Type | Description |
|:---- |:---- |:---- |
| counterGroupName | string | The name of the counter group |
| counter | array of counter objects(JSON)/zero or more counter objects(XML) | A collection of counter objects |

#### Elements of the *counter* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the counter |
| reduceCounterValue | long | The counter value of reduce tasks |
| mapCounterValue | long | The counter value of map tasks |
| totalCounterValue | long | The counter value of all tasks |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/counters

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "jobCounters" : {
          "id" : "job_1326381300833_2_2",
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
                   },
                   {
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
                      "totalCounterValue" : 108525,
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
                      "totalCounterValue" : 1200,
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
                      "totalCounterValue" : 1200,
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
                      "totalCounterValue" : 2400,
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
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 113,
                      "name" : "GC_TIME_MILLIS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 1830,
                      "name" : "CPU_MILLISECONDS"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 478068736,
                      "name" : "PHYSICAL_MEMORY_BYTES"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 2159284224,
                      "name" : "VIRTUAL_MEMORY_BYTES"
                   },
                   {
                      "reduceCounterValue" : 0,
                      "mapCounterValue" : 0,
                      "totalCounterValue" : 378863616,
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

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/counters
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 7030
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobCounters>
      <id>job_1326381300833_2_2</id>
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
          <totalCounterValue>108525</totalCounterValue>
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
          <totalCounterValue>1200</totalCounterValue>
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
          <totalCounterValue>1200</totalCounterValue>
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
          <totalCounterValue>2400</totalCounterValue>
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
          <totalCounterValue>113</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>CPU_MILLISECONDS</name>
          <totalCounterValue>1830</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>PHYSICAL_MEMORY_BYTES</name>
          <totalCounterValue>478068736</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>VIRTUAL_MEMORY_BYTES</name>
          <totalCounterValue>2159284224</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
        <counter>
          <name>COMMITTED_HEAP_BYTES</name>
          <totalCounterValue>378863616</totalCounterValue>
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
        </counter>
      </counterGroup>
      <counterGroup>
        <counterGroupName>org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter</counterGroupName>
        <counter>
          <name>BYTES_WRITTEN</name>
          <totalCounterValue>0</totalCounterValue>
          <mapCounterValue>0</mapCounterValue>
          <reduceCounterValue>0</reduceCounterValue>
        </counter>
      </counterGroup>
    </jobCounters>

### Job Conf API

A job configuration resource contains information about the job configuration for this job.

#### URI

Use the following URI to obtain th job configuration information, from a job identified by the jobid value.

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/conf

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *conf* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| path | string | The path to the job configuration file |
| property | array of the configuration properties(JSON)/zero or more configuration properties(XML) | Collection of configuration property objects |

#### Elements of the *property* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the configuration property |
| value | string | The value of the configuration property |
| source | string | The location this configuration object came from. If there is more then one of these it shows the history with the latest source at the end of the list. |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/conf

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

This is a small snippet of the output as the output if very large. The real output contains every property in your job configuration file.

    {
       "conf" : {
          "path" : "hdfs://host.domain.com:9000/user/user1/.staging/job_1326381300833_0002/job.xml",
          "property" : [
             {
                "value" : "/home/hadoop/hdfs/data",
                "name" : "dfs.datanode.data.dir"
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

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/conf
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 552
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <conf>
      <path>hdfs://host.domain.com:9000/user/user1/.staging/job_1326381300833_0002/job.xml</path>
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

### Tasks API

With the tasks API, you can obtain a collection of resources that represent a task within a job. When you run a GET operation on this resource, you obtain a collection of Task Objects.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      * type - type of task, valid values are m or r.  m for map task or r for reduce task.

#### Elements of the *tasks* object

When you make a request for the list of tasks , the information will be returned as an array of task objects. See also
[Task API](#Task_API)
for syntax of the task object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| task | array of task objects(JSON)/zero or more task objects(XML) | The collection of task objects. |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks

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
                "elapsedTime" : 6777,
                "state" : "SUCCEEDED",
                "startTime" : 1326381446541,
                "id" : "task_1326381300833_2_2_m_0",
                "type" : "MAP",
                "successfulAttempt" : "attempt_1326381300833_2_2_m_0_0",
                "finishTime" : 1326381453318
             },
             {
                "progress" : 100,
                "elapsedTime" : 135559,
                "state" : "SUCCEEDED",
                "startTime" : 1326381446544,
                "id" : "task_1326381300833_2_2_r_0",
                "type" : "REDUCE",
                "successfulAttempt" : "attempt_1326381300833_2_2_r_0_0",
                "finishTime" : 1326381582103
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 653
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <tasks>
      <task>
        <startTime>1326381446541</startTime>
        <finishTime>1326381453318</finishTime>
        <elapsedTime>6777</elapsedTime>
        <progress>100.0</progress>
        <id>task_1326381300833_2_2_m_0</id>
        <state>SUCCEEDED</state>
        <type>MAP</type>
        <successfulAttempt>attempt_1326381300833_2_2_m_0_0</successfulAttempt>
      </task>
      <task>
        <startTime>1326381446544</startTime>
        <finishTime>1326381582103</finishTime>
        <elapsedTime>135559</elapsedTime>
        <progress>100.0</progress>
        <id>task_1326381300833_2_2_r_0</id>
        <state>SUCCEEDED</state>
        <type>REDUCE</type>
        <successfulAttempt>attempt_1326381300833_2_2_r_0_0</successfulAttempt>
      </task>
    </tasks>

### Task API

A Task resource contains information about a particular task within a job.

#### URI

Use the following URI to obtain an Task Object, from a task identified by the taskid value.

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *task* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task id |
| state | string | The state of the task - valid values are: NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL\_WAIT, KILLED |
| type | string | The task type - MAP or REDUCE |
| successfulAttempt | string | The id of the last successful attempt |
| progress | float | The progress of the task as a percent |
| startTime | long | The time in which the task started (in ms since epoch) or -1 if it was never started |
| finishTime | long | The time in which the task finished (in ms since epoch) |
| elapsedTime | long | The elapsed time since the application started (in ms) |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "task" : {
          "progress" : 100,
          "elapsedTime" : 6777,
          "state" : "SUCCEEDED",
          "startTime" : 1326381446541,
          "id" : "task_1326381300833_2_2_m_0",
          "type" : "MAP",
          "successfulAttempt" : "attempt_1326381300833_2_2_m_0_0",
          "finishTime" : 1326381453318
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 299
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <task>
      <startTime>1326381446541</startTime>
      <finishTime>1326381453318</finishTime>
      <elapsedTime>6777</elapsedTime>
      <progress>100.0</progress>
      <id>task_1326381300833_2_2_m_0</id>
      <state>SUCCEEDED</state>
      <type>MAP</type>
      <successfulAttempt>attempt_1326381300833_2_2_m_0_0</successfulAttempt>
    </task>

### Task Counters API

With the task counters API, you can object a collection of resources that represent all the counters for that task.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *jobTaskCounters* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task id |
| taskcounterGroup | array of counterGroup objects(JSON)/zero or more counterGroup objects(XML) | A collection of counter group objects |

#### Elements of the *counterGroup* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| counterGroupName | string | The name of the counter group |
| counter | array of counter objects(JSON)/zero or more counter objects(XML) | A collection of counter objects |

#### Elements of the *counter* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the counter |
| value | long | The value of the counter |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/counters

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "jobTaskCounters" : {
          "id" : "task_1326381300833_2_2_m_0",
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

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/counters
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2660
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskCounters>
      <id>task_1326381300833_2_2_m_0</id>
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

### Task Attempts API

With the task attempts API, you can obtain a collection of resources that represent a task attempt within a job. When you run a GET operation on this resource, you obtain a collection of Task Attempt Objects.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *taskAttempts* object

When you make a request for the list of task attempts, the information will be returned as an array of task attempt objects. See also
[Task Attempt API](#Task_Attempt_API)
for syntax of the task object.

| Item | Data Type | Description |
|:---- |:---- |:---- |
| taskAttempt | array of task attempt objects(JSON)/zero or more task attempt objects(XML) | The collection of task attempt objects |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/attempts

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
                "assignedContainerId" : "container_1326381300833_0002_01_000002",
                "progress" : 100,
                "elapsedTime" : 2638,
                "state" : "SUCCEEDED",
                "diagnostics" : "",
                "rack" : "/98.139.92.0",
                "nodeHttpAddress" : "host.domain.com:8042",
                "startTime" : 1326381450680,
                "id" : "attempt_1326381300833_2_2_m_0_0",
                "type" : "MAP",
                "finishTime" : 1326381453318
             }
          ]
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/attempts
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 537
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <taskAttempts>
      <taskAttempt>
        <startTime>1326381450680</startTime>
        <finishTime>1326381453318</finishTime>
        <elapsedTime>2638</elapsedTime>
        <progress>100.0</progress>
        <id>attempt_1326381300833_2_2_m_0_0</id>
        <rack>/98.139.92.0</rack>
        <state>SUCCEEDED</state>
        <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
        <diagnostics/>
        <type>MAP</type>
        <assignedContainerId>container_1326381300833_0002_01_000002</assignedContainerId>
      </taskAttempt>
    </taskAttempts>

### Task Attempt API

A Task Attempt resource contains information about a particular task attempt within a job.

#### URI

Use the following URI to obtain an Task Attempt Object, from a task identified by the attemptid value.

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *taskAttempt* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task id |
| rack | string | The rack |
| state | string | The state of the task attempt - valid values are: NEW, UNASSIGNED, ASSIGNED, RUNNING, COMMIT\_PENDING, SUCCESS\_CONTAINER\_CLEANUP, SUCCEEDED, FAIL\_CONTAINER\_CLEANUP, FAIL\_TASK\_CLEANUP, FAILED, KILL\_CONTAINER\_CLEANUP, KILL\_TASK\_CLEANUP, KILLED |
| type | string | The type of task |
| assignedContainerId | string | The container id this attempt is assigned to |
| nodeHttpAddress | string | The http address of the node this task attempt ran on |
| diagnostics | string | A diagnostics message |
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

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/attempts/attempt_1326381300833_2_2_m_0_0

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

    {
       "taskAttempt" : {
          "assignedContainerId" : "container_1326381300833_0002_01_000002",
          "progress" : 100,
          "elapsedTime" : 2638,
          "state" : "SUCCEEDED",
          "diagnostics" : "",
          "rack" : "/98.139.92.0",
          "nodeHttpAddress" : "host.domain.com:8042",
          "startTime" : 1326381450680,
          "id" : "attempt_1326381300833_2_2_m_0_0",
          "type" : "MAP",
          "finishTime" : 1326381453318
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/attempts/attempt_1326381300833_2_2_m_0_0
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 691
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <taskAttempt>
      <startTime>1326381450680</startTime>
      <finishTime>1326381453318</finishTime>
      <elapsedTime>2638</elapsedTime>
      <progress>100.0</progress>
      <id>attempt_1326381300833_2_2_m_0_0</id>
      <rack>/98.139.92.0</rack>
      <state>SUCCEEDED</state>
      <nodeHttpAddress>host.domain.com:8042</nodeHttpAddress>
      <diagnostics/>
      <type>MAP</type>
      <assignedContainerId>container_1326381300833_0002_01_000002</assignedContainerId>
    </taskAttempt>

### Task Attempt Counters API

With the task attempt counters API, you can object a collection of resources that represent al the counters for that task attempt.

#### URI

      * http://<history server http address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}/counters

#### HTTP Operations Supported

      * GET

#### Query Parameters Supported

      None

#### Elements of the *jobTaskAttemptCounters* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| id | string | The task attempt id |
| taskAttemptcounterGroup | array of task attempt counterGroup objects(JSON)/zero or more task attempt counterGroup objects(XML) | A collection of task attempt counter group objects |

#### Elements of the *taskAttemptCounterGroup* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| counterGroupName | string | The name of the counter group |
| counter | array of counter objects(JSON)/zero or more counter objects(XML) | A collection of counter objects |

#### Elements of the *counter* object

| Item | Data Type | Description |
|:---- |:---- |:---- |
| name | string | The name of the counter |
| value | long | The value of the counter |

#### Response Examples

**JSON response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/attempts/attempt_1326381300833_2_2_m_0_0/counters

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
          "id" : "attempt_1326381300833_2_2_m_0_0"
       }
    }

**XML response**

HTTP Request:

      GET http://<history server http address:port>/ws/v1/history/mapreduce/jobs/job_1326381300833_2_2/tasks/task_1326381300833_2_2_m_0/attempts/attempt_1326381300833_2_2_m_0_0/counters
      Accept: application/xml

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/xml
      Content-Length: 2735
      Server: Jetty(6.1.26)

Response Body:

    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <jobTaskAttemptCounters>
      <id>attempt_1326381300833_2_2_m_0_0</id>
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
