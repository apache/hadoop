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

YARN Timeline Server
====================

* [Overview](#Overview)
    * [Introduction](#Introduction)
    * [Current Status](#Current_Status)
    * [Timeline Structure](#Timeline_Structure)
* [Deployment](#Deployment)
    * [Configurations](#Configurations)
    * [Running Timeline server](#Running_Timeline_server)
    * [Accessing generic-data via command-line](#Accessing_generic-data_via_command-line)
* [Publishing of application specific data](#Publishing_of_application_specific_data)

Overview
---------

### Introduction  

 Storage and retrieval of application's current as well as historic information in a generic fashion is solved in YARN through the Timeline Server. This serves two responsibilities:

#### Application specific information

  Supports collection of information completely specific to an application or framework. For example, Hadoop MapReduce framework can include pieces of information like number of map tasks, reduce tasks, counters etc. Application developers can publish the specific information to the Timeline server via TimelineClient, the ApplicationMaster and/or the application's containers. This information is then queryable via REST APIs for rendering by application/framework specific UIs.

#### Generic information about completed applications
  
  Previously this was done by Application History Server but with  timeline server its just one use case of Timeline server functionality. Generic information includes application level data like queue-name, user information etc in the ApplicationSubmissionContext, list of application-attempts that ran for an application, information about each application-attempt, list of containers run under each application-attempt, and information about each container. Generic data is published by ResourceManager to the timeline store and used by the web-UI to display information about completed applications.
 

### Current Status

  The essential functionality of the timeline server have been completed and it can work in both secure and non secure modes. The generic history service is also built on timeline store. In subsequent releases we will be rolling out next generation timeline service which is scalable and reliable. Currently, Application specific information is only available via RESTful APIs using JSON type content. The ability to install framework specific UIs in YARN is not supported yet.

### Timeline Structure

![Timeline Structure] (./images/timeline_structure.jpg)

#### TimelineDomain

  Domain is like namespace for Timeline server and users can host multiple entities, isolating them from others. Timeline server Security is defined at this level. Domain majorly stores owner info, read & write ACL information, created and modified time stamp information. Domain is uniquely identified by ID.

#### TimelineEntity

  Entity contains the the meta information of some conceptual entity and its related events. The entity can be an application, an application attempt, a container or whatever the user-defined object. It contains Primary filters which will be used to index the entities in TimelineStore, such that users should carefully choose the information they want to store as the primary filters. The remaining data can be stored as other information. Entity is uniquely identified by EntityId and EntityType.

#### TimelineEvent

  TimelineEvent contains the information of an event that is related to some conceptual entity of an application. Users are free to define what the event means, such as starting an application, getting allocated a container and etc.

Deployment
----------

###Configurations

#### Basic Configuration

| Configuration Property | Description |
|:---- |:---- |
| `yarn.timeline-service.enabled` | Indicate to clients whether Timeline service is enabled or not. If enabled, the TimelineClient library used by end-users will post entities and events to the Timeline server. Defaults to false. |
| `yarn.resourcemanager.system-metrics-publisher.enabled` | The setting that controls whether yarn system metrics is published on the timeline server or not by RM. Defaults to false. |
| `yarn.timeline-service.generic-application-history.enabled` | Indicate to clients whether to query generic application data from timeline history-service or not. If not enabled then application data is queried only from Resource Manager. Defaults to false. |

#### Advanced configuration

| Configuration Property | Description |
|:---- |:---- |
| `yarn.timeline-service.ttl-enable` | Enable age off of timeline store data. Defaults to true. |
| `yarn.timeline-service.ttl-ms` | Time to live for timeline store data in milliseconds. Defaults to 604800000 (7 days). |
| `yarn.timeline-service.handler-thread-count` | Handler thread count to serve the client RPC requests. Defaults to 10. |
| `yarn.timeline-service.client.max-retries` | Default maximum number of retires for timeline servive client. Defaults to 30. |
| `yarn.timeline-service.client.retry-interval-ms` | Default retry time interval for timeline servive client. Defaults to 1000. |

#### Timeline store and state store configuration

| Configuration Property | Description |
|:---- |:---- |
| `yarn.timeline-service.store-class` | Store class name for timeline store. Defaults to org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore. |
| `yarn.timeline-service.leveldb-timeline-store.path` | Store file name for leveldb timeline store. Defaults to ${hadoop.tmp.dir}/yarn/timeline. |
| `yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms` | Length of time to wait between deletion cycles of leveldb timeline store in milliseconds. Defaults to 300000. |
| `yarn.timeline-service.leveldb-timeline-store.read-cache-size` | Size of read cache for uncompressed blocks for leveldb timeline store in bytes. Defaults to 104857600. |
| `yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size` | Size of cache for recently read entity start times for leveldb timeline store in number of entities. Defaults to 10000. |
| `yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size` | Size of cache for recently written entity start times for leveldb timeline store in number of entities. Defaults to 10000. |
| `yarn.timeline-service.recovery.enabled` | Defaults to false. |
| `yarn.timeline-service.state-store-class` | Store class name for timeline state store. Defaults to org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore. |
| `yarn.timeline-service.leveldb-state-store.path` | Store file name for leveldb timeline state store. |

#### Web and RPC Configuration

| Configuration Property | Description |
|:---- |:---- |
| `yarn.timeline-service.hostname` | The hostname of the Timeline service web application. Defaults to 0.0.0.0. |
| `yarn.timeline-service.address` | Address for the Timeline server to start the RPC server. Defaults to ${yarn.timeline-service.hostname}:10200. |
| `yarn.timeline-service.webapp.address` | The http address of the Timeline service web application. Defaults to ${yarn.timeline-service.hostname}:8188. |
| `yarn.timeline-service.webapp.https.address` | The https address of the Timeline service web application. Defaults to ${yarn.timeline-service.hostname}:8190. |
| `yarn.timeline-service.bind-host` | The actual address the server will bind to. If this optional address is set, the RPC and webapp servers will bind to this address and the port specified in yarn.timeline-service.address and yarn.timeline-service.webapp.address, respectively. This is most useful for making the service listen to all interfaces by setting to 0.0.0.0. |
| `yarn.timeline-service.http-cross-origin.enabled` | Enables cross-origin support (CORS) for web services where cross-origin web response headers are needed. For example, javascript making a web services request to the timeline server. Defaults to false. |
| `yarn.timeline-service.http-cross-origin.allowed-origins` | Comma separated list of origins that are allowed for web services needing cross-origin (CORS) support. Wildcards `(*)` and patterns allowed. Defaults to `*`. |
| yarn.timeline-service.http-cross-origin.allowed-methods | Comma separated list of methods that are allowed for web services needing cross-origin (CORS) support. Defaults to GET,POST,HEAD. |
| `yarn.timeline-service.http-cross-origin.allowed-headers` | Comma separated list of headers that are allowed for web services needing cross-origin (CORS) support. Defaults to X-Requested-With,Content-Type,Accept,Origin. |
| `yarn.timeline-service.http-cross-origin.max-age` | The number of seconds a pre-flighted request can be cached for web services needing cross-origin (CORS) support. Defaults to 1800. |

#### Security Configuration

 Security can be enabled by setting yarn.timeline-service.http-authentication.type to kerberos and further following configurations can be done.

| Configuration Property | Description |
|:---- |:---- |
| `yarn.timeline-service.http-authentication.type` | Defines authentication used for the timeline server HTTP endpoint. Supported values are: simple / kerberos / #AUTHENTICATION_HANDLER_CLASSNAME#. Defaults to simple. |
| `yarn.timeline-service.http-authentication.simple.anonymous.allowed` | Indicates if anonymous requests are allowed by the timeline server when using 'simple' authentication. Defaults to true. |
| `yarn.timeline-service.principal` | The Kerberos principal for the timeline server. |
| yarn.timeline-service.keytab | The Kerberos keytab for the timeline server. Defaults to /etc/krb5.keytab. |
| `yarn.timeline-service.delegation.key.update-interval` | Defaults to 86400000 (1 day). |
| `yarn.timeline-service.delegation.token.renew-interval` | Defaults to 86400000 (1 day). |
| `yarn.timeline-service.delegation.token.max-lifetime` | Defaults to 604800000 (7 day). |

#### Enabling the timeline service and the generic history service

  Following are the basic configuration to start Timeline server.

```
<property>
  <description>Indicate to clients whether Timeline service is enabled or not.
  If enabled, the TimelineClient library used by end-users will post entities
  and events to the Timeline server.</description>
  <name>yarn.timeline-service.enabled</name>
  <value>true</value>
</property>

<property>
  <description>The setting that controls whether yarn system metrics is
  published on the timeline server or not by RM.</description>
  <name>yarn.resourcemanager.system-metrics-publisher.enabled</name>
  <value>true</value>
</property>

<property>
  <description>Indicate to clients whether to query generic application
  data from timeline history-service or not. If not enabled then application
  data is queried only from Resource Manager.</description>
  <name>yarn.timeline-service.generic-application-history.enabled</name>
  <value>true</value>
</property>
```

### Running Timeline server

  Assuming all the aforementioned configurations are set properly, admins can start the Timeline server/history service with the following command:

```
  $ yarn timelineserver
```

  Or users can start the Timeline server / history service as a daemon:

```
  $ $HADOOP_YARN_HOME/sbin/yarn-daemon.sh start timelineserver
```

### Accessing generic-data via command-line

  Users can access applications' generic historic data via the command line as below. Note that the same commands are usable to obtain the corresponding information about running applications.

```
  $ yarn application -status <Application ID>
  $ yarn applicationattempt -list <Application ID>
  $ yarn applicationattempt -status <Application Attempt ID>
  $ yarn container -list <Application Attempt ID>
  $ yarn container -status <Container ID>
```

Publishing of application specific data
------------------------------------------------

  Developers can define what information they want to record for their applications by composing `TimelineEntity` and  `TimelineEvent` objects, and put the entities and events to the Timeline server via `TimelineClient`. Below is an example:

```
  // Create and start the Timeline client
  TimelineClient client = TimelineClient.createTimelineClient();
  client.init(conf);
  client.start();

  try {
    TimelineDomain myDomain = new TimelineDomain();
    myDomain.setID("MyDomain");
    // Compose other Domain info ....

    client.putDomain(myDomain);

    TimelineEntity myEntity = new TimelineEntity();
    myEntity.setDomainId(myDomain.getId());
    myEntity.setEntityType("APPLICATION");
    myEntity.setEntityID("MyApp1")
    // Compose other entity info

    TimelinePutResponse response = client.putEntities(entity);

    
    TimelineEvent event = new TimelineEvent();
    event.setEventType("APP_FINISHED");
    event.setTimestamp(System.currentTimeMillis());
    event.addEventInfo("Exit Status", "SUCCESS");
    // Compose other Event info ....

    myEntity.addEvent(event);
    timelineClient.putEntities(entity);

  } catch (IOException e) {
    // Handle the exception
  } catch (YarnException e) {
    // Handle the exception
  }

  // Stop the Timeline client
  client.stop();
```

  **Note** : Following are the points which needs to be observed during updating a entity.

  * Domain ID should not be modified for already existing entity.

  * Its advisable to have same primary filters for all updates on entity. As on modification of primary filter by subsequent updates will result in not fetching the information before the update when queried with updated primary filter.

  * On modification of Primary filter value, new value will be appended with the old value.
