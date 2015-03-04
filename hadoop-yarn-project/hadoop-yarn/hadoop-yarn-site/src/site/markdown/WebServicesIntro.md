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

Hadoop YARN - Introduction to the web services REST API's
==========================================================

* [Overview](#Overview)
* [URI's](#URIs)
* [HTTP Requests](#HTTP_Requests)
    * [Summary of HTTP operations](#Summary_of_HTTP_operations)
    * [Security](#Security)
    * [Headers Supported](#Headers_Supported)
* [HTTP Responses](#HTTP_Responses)
    * [Compression](#Compression)
    * [Response Formats](#Response_Formats)
    * [Response Errors](#Response_Errors)
    * [Response Examples](#Response_Examples)
* [Sample Usage](#Sample_Usage)

Overview
--------

The Hadoop YARN web service REST APIs are a set of URI resources that give access to the cluster, nodes, applications, and application historical information. The URI resources are grouped into APIs based on the type of information returned. Some URI resources return collections while others return singletons.

URI's
-----

The URIs for the REST-based Web services have the following syntax:

      http://{http address of service}/ws/{version}/{resourcepath}

The elements in this syntax are as follows:

      {http address of service} - The http address of the service to get information about. 
                                  Currently supported are the ResourceManager, NodeManager, 
                                  MapReduce application master, and history server.
      {version} - The version of the APIs. In this release, the version is v1.
      {resourcepath} - A path that defines a singleton resource or a collection of resources. 

HTTP Requests
-------------

To invoke a REST API, your application calls an HTTP operation on the URI associated with a resource.

### Summary of HTTP operations

Currently only GET is supported. It retrieves information about the resource specified.

### Security

The web service REST API's go through the same security as the web UI. If your cluster adminstrators have filters enabled you must authenticate via the mechanism they specified.

### Headers Supported

      * Accept 
      * Accept-Encoding

Currently the only fields used in the header is `Accept` and `Accept-Encoding`. `Accept` currently supports XML and JSON for the response type you accept. `Accept-Encoding` currently supports only gzip format and will return gzip compressed output if this is specified, otherwise output is uncompressed. All other header fields are ignored.

HTTP Responses
--------------

The next few sections describe some of the syntax and other details of the HTTP Responses of the web service REST APIs.

### Compression

This release supports gzip compression if you specify gzip in the Accept-Encoding header of the HTTP request (Accept-Encoding: gzip).

### Response Formats

This release of the web service REST APIs supports responses in JSON and XML formats. JSON is the default. To set the response format, you can specify the format in the Accept header of the HTTP request.

As specified in HTTP Response Codes, the response body can contain the data that represents the resource or an error message. In the case of success, the response body is in the selected format, either JSON or XML. In the case of error, the resonse body is in either JSON or XML based on the format requested. The Content-Type header of the response contains the format requested. If the application requests an unsupported format, the response status code is 500. Note that the order of the fields within response body is not specified and might change. Also, additional fields might be added to a response body. Therefore, your applications should use parsing routines that can extract data from a response body in any order.

### Response Errors

After calling an HTTP request, an application should check the response status code to verify success or detect an error. If the response status code indicates an error, the response body contains an error message. The first field is the exception type, currently only RemoteException is returned. The following table lists the items within the RemoteException error message:

|      Item | Data Type |          Description |
|:---- |:---- |:---- |
|   exception |   String |         Exception type |
| javaClassName |   String |  Java class name of exception |
|    message |   String | Detailed message of exception |

### Response Examples

#### JSON response with single resource

HTTP Request: GET http://rmhost.domain:8088/ws/v1/cluster/app/application\_1324057493980\_0001

Response Status Line: HTTP/1.1 200 OK

Response Header:

      HTTP/1.1 200 OK
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
  app":
  {
    "id":"application_1324057493980_0001",
    "user":"user1",
    "name":"",
    "queue":"default",
    "state":"ACCEPTED",
    "finalStatus":"UNDEFINED",
    "progress":0,
    "trackingUI":"UNASSIGNED",
    "diagnostics":"",
    "clusterId":1324057493980,
    "startedTime":1324057495921,
    "finishedTime":0,
    "elapsedTime":2063,
    "amContainerLogs":"http:\/\/amNM:2\/node\/containerlogs\/container_1324057493980_0001_01_000001",
    "amHostHttpAddress":"amNM:2"
  }
}
```

#### JSON response with Error response

Here we request information about an application that doesn't exist yet.

HTTP Request: GET http://rmhost.domain:8088/ws/v1/cluster/app/application\_1324057493980\_9999

Response Status Line: HTTP/1.1 404 Not Found

Response Header:

      HTTP/1.1 404 Not Found
      Content-Type: application/json
      Transfer-Encoding: chunked
      Server: Jetty(6.1.26)

Response Body:

```json
{
   "RemoteException" : {
      "javaClassName" : "org.apache.hadoop.yarn.webapp.NotFoundException",
      "exception" : "NotFoundException",
      "message" : "java.lang.Exception: app with id: application_1324057493980_9999 not found"
   }
}
```

Sample Usage
-------------

You can use any number of ways/languages to use the web services REST API's. This example uses the curl command line interface to do the REST GET calls.

In this example, a user submits a MapReduce application to the ResourceManager using a command like:

      hadoop jar hadoop-mapreduce-test.jar sleep -Dmapred.job.queue.name=a1 -m 1 -r 1 -rt 1200000 -mt 20

The client prints information about the job submitted along with the application id, similar to:

    12/01/18 04:25:15 INFO mapred.ResourceMgrDelegate: Submitted application application_1326821518301_0010 to ResourceManager at host.domain.com/10.10.10.10:8032
    12/01/18 04:25:15 INFO mapreduce.Job: Running job: job_1326821518301_0010
    12/01/18 04:25:21 INFO mapred.ClientServiceDelegate: The url to track the job: host.domain.com:8088/proxy/application_1326821518301_0010/
    12/01/18 04:25:22 INFO mapreduce.Job: Job job_1326821518301_0010 running in uber mode : false
    12/01/18 04:25:22 INFO mapreduce.Job:  map 0% reduce 0%

The user then wishes to track the application. The users starts by getting the information about the application from the ResourceManager. Use the --comopressed option to request output compressed. curl handles uncompressing on client side.

    curl --compressed -H "Accept: application/json" -X GET "http://host.domain.com:8088/ws/v1/cluster/apps/application_1326821518301_0010" 

Output:

```json
{
   "app" : {
      "finishedTime" : 0,
      "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0010_01_000001",
      "trackingUI" : "ApplicationMaster",
      "state" : "RUNNING",
      "user" : "user1",
      "id" : "application_1326821518301_0010",
      "clusterId" : 1326821518301,
      "finalStatus" : "UNDEFINED",
      "amHostHttpAddress" : "host.domain.com:8042",
      "progress" : 82.44703,
      "name" : "Sleep job",
      "startedTime" : 1326860715335,
      "elapsedTime" : 31814,
      "diagnostics" : "",
      "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326821518301_0010/",
      "queue" : "a1"
   }
}
```

The user then wishes to get more details about the running application and goes directly to the MapReduce application master for this application. The ResourceManager lists the trackingUrl that can be used for this application: http://host.domain.com:8088/proxy/application\_1326821518301\_0010. This could either go to the web browser or use the web service REST API's. The user uses the web services REST API's to get the list of jobs this MapReduce application master is running:

     curl --compressed -H "Accept: application/json" -X GET "http://host.domain.com:8088/proxy/application_1326821518301_0010/ws/v1/mapreduce/jobs"

Output:

```json
{
   "jobs" : {
      "job" : [
         {
            "runningReduceAttempts" : 1,
            "reduceProgress" : 72.104515,
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
            "startTime" : 1326860720902,
            "id" : "job_1326821518301_10_10",
            "successfulMapAttempts" : 1,
            "runningMapAttempts" : 0,
            "newReduceAttempts" : 0,
            "name" : "Sleep job",
            "mapsPending" : 0,
            "elapsedTime" : 64432,
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
```

The user then wishes to get the task details about the job with job id job\_1326821518301\_10\_10 that was listed above.

     curl --compressed -H "Accept: application/json" -X GET "http://host.domain.com:8088/proxy/application_1326821518301_0010/ws/v1/mapreduce/jobs/job_1326821518301_10_10/tasks" 

Output:

```json
{
   "tasks" : {
      "task" : [
         {
            "progress" : 100,
            "elapsedTime" : 5059,
            "state" : "SUCCEEDED",
            "startTime" : 1326860725014,
            "id" : "task_1326821518301_10_10_m_0",
            "type" : "MAP",
            "successfulAttempt" : "attempt_1326821518301_10_10_m_0_0",
            "finishTime" : 1326860730073
         },
         {
            "progress" : 72.104515,
            "elapsedTime" : 0,
            "state" : "RUNNING",
            "startTime" : 1326860732984,
            "id" : "task_1326821518301_10_10_r_0",
            "type" : "REDUCE",
            "successfulAttempt" : "",
            "finishTime" : 0
         }
      ]
   }
}
```

The map task has finished but the reduce task is still running. The users wishes to get the task attempt information for the reduce task task\_1326821518301\_10\_10\_r\_0, note that the Accept header isn't really required here since JSON is the default output format:

      curl --compressed -X GET "http://host.domain.com:8088/proxy/application_1326821518301_0010/ws/v1/mapreduce/jobs/job_1326821518301_10_10/tasks/task_1326821518301_10_10_r_0/attempts"

Output:

```json
{
   "taskAttempts" : {
      "taskAttempt" : [
         {
            "elapsedMergeTime" : 158,
            "shuffleFinishTime" : 1326860735378,
            "assignedContainerId" : "container_1326821518301_0010_01_000003",
            "progress" : 72.104515,
            "elapsedTime" : 0,
            "state" : "RUNNING",
            "elapsedShuffleTime" : 2394,
            "mergeFinishTime" : 1326860735536,
            "rack" : "/10.10.10.0",
            "elapsedReduceTime" : 0,
            "nodeHttpAddress" : "host.domain.com:8042",
            "type" : "REDUCE",
            "startTime" : 1326860732984,
            "id" : "attempt_1326821518301_10_10_r_0_0",
            "finishTime" : 0
         }
      ]
   }
}
```

The reduce attempt is still running and the user wishes to see the current counter values for that attempt:

     curl --compressed -H "Accept: application/json"  -X GET "http://host.domain.com:8088/proxy/application_1326821518301_0010/ws/v1/mapreduce/jobs/job_1326821518301_10_10/tasks/task_1326821518301_10_10_r_0/attempts/attempt_1326821518301_10_10_r_0_0/counters" 

Output:

```json
{
   "JobTaskAttemptCounters" : {
      "taskAttemptCounterGroup" : [
         {
            "counterGroupName" : "org.apache.hadoop.mapreduce.FileSystemCounter",
            "counter" : [
               {
                  "value" : 4216,
                  "name" : "FILE_BYTES_READ"
               }, 
               {
                  "value" : 77151,
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
                  "value" : 1767,
                  "name" : "REDUCE_INPUT_GROUPS"
               },
               {  
                  "value" : 25104,
                  "name" : "REDUCE_SHUFFLE_BYTES"
               },
               {
                  "value" : 1767,
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
                  "value" : 50,
                  "name" : "GC_TIME_MILLIS"
               },
               {
                  "value" : 1580,
                  "name" : "CPU_MILLISECONDS"
               },
               {
                  "value" : 141320192,
                  "name" : "PHYSICAL_MEMORY_BYTES"
               },
              {
                  "value" : 1118552064,
                  "name" : "VIRTUAL_MEMORY_BYTES"
               }, 
               {  
                  "value" : 73728000,
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
      "id" : "attempt_1326821518301_10_10_r_0_0"
   }
}
```

The job finishes and the user wishes to get the final job information from the history server for this job.

      curl --compressed -X GET "http://host.domain.com:19888/ws/v1/history/mapreduce/jobs/job_1326821518301_10_10" 

Output:

```json
{
   "job" : {
      "avgReduceTime" : 1250784,
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
      "startTime" : 1326860720902,
      "id" : "job_1326821518301_10_10",
      "avgMapTime" : 5059,
      "successfulMapAttempts" : 1,
      "name" : "Sleep job",
      "avgShuffleTime" : 2394,
      "reducesCompleted" : 1,
      "diagnostics" : "",
      "failedMapAttempts" : 0,
      "avgMergeTime" : 2552,
      "killedReduceAttempts" : 0,
      "mapsTotal" : 1,
      "queue" : "a1",
      "uberized" : false,
      "killedMapAttempts" : 0,
      "finishTime" : 1326861986164
   }
}
```

The user also gets the final applications information from the ResourceManager.

      curl --compressed -H "Accept: application/json" -X GET "http://host.domain.com:8088/ws/v1/cluster/apps/application_1326821518301_0010" 

Output:

```json
{
   "app" : {
      "finishedTime" : 1326861991282,
      "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0010_01_000001",
      "trackingUI" : "History",
      "state" : "FINISHED",
      "user" : "user1",
      "id" : "application_1326821518301_0010",
      "clusterId" : 1326821518301,
      "finalStatus" : "SUCCEEDED",
      "amHostHttpAddress" : "host.domain.com:8042",
      "progress" : 100,
      "name" : "Sleep job",
      "startedTime" : 1326860715335,
      "elapsedTime" : 1275947,
      "diagnostics" : "",
      "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326821518301_0010/jobhistory/job/job_1326821518301_10_10",
      "queue" : "a1"
   }
}
```