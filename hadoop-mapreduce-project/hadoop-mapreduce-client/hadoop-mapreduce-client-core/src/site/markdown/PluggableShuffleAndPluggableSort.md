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

Hadoop: Pluggable Shuffle and Pluggable Sort
============================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

The pluggable shuffle and pluggable sort capabilities allow replacing the built in shuffle and sort logic with alternate implementations. Example use cases for this are: using a different application protocol other than HTTP such as RDMA for shuffling data from the Map nodes to the Reducer nodes; or replacing the sort logic with custom algorithms that enable Hash aggregation and Limit-N query.

**IMPORTANT:** The pluggable shuffle and pluggable sort capabilities are experimental and unstable. This means the provided APIs may change and break compatibility in future versions of Hadoop.

Implementing a Custom Shuffle and a Custom Sort
-----------------------------------------------

A custom shuffle implementation requires a
`org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices.AuxiliaryService`
implementation class running in the NodeManagers and a
`org.apache.hadoop.mapred.ShuffleConsumerPlugin`
implementation class running in the Reducer tasks.

The default implementations provided by Hadoop can be used as references:

* `org.apache.hadoop.mapred.ShuffleHandler`
* `org.apache.hadoop.mapreduce.task.reduce.Shuffle`

A custom sort implementation requires a `org.apache.hadoop.mapred.MapOutputCollector` implementation class running in the Mapper tasks and (optionally, depending on the sort implementation) a `org.apache.hadoop.mapred.ShuffleConsumerPlugin` implementation class running in the Reducer tasks.

The default implementations provided by Hadoop can be used as references:

* `org.apache.hadoop.mapred.MapTask$MapOutputBuffer`
* `org.apache.hadoop.mapreduce.task.reduce.Shuffle`

Configuration
-------------

Except for the auxiliary service running in the NodeManagers serving the shuffle (by default the `ShuffleHandler`), all the pluggable components run in the job tasks. This means, they can be configured on per job basis. The auxiliary service servicing the Shuffle must be configured in the NodeManagers configuration.

### Job Configuration Properties (on per job basis):

| **Property** | **Default Value** | **Explanation** |
|:---- |:---- |:---- |
| `mapreduce.job.reduce.shuffle.consumer.plugin.class` | `org.apache.hadoop.mapreduce.task.reduce.Shuffle` | The `ShuffleConsumerPlugin` implementation to use |
| `mapreduce.job.map.output.collector.class` | `org.apache.hadoop.mapred.MapTask$MapOutputBuffer` | The `MapOutputCollector` implementation(s) to use |

These properties can also be set in the `mapred-site.xml` to change the default values for all jobs.

The collector class configuration may specify a comma-separated list of collector implementations. In this case, the map task will attempt to instantiate each in turn until one of the implementations successfully initializes. This can be useful if a given collector implementation is only compatible with certain types of keys or values, for example.

### NodeManager Configuration properties, `yarn-site.xml` in all nodes:

| **Property** | **Default Value** | **Explanation** |
|:---- |:---- |:---- |
| `yarn.nodemanager.aux-services` | `...,mapreduce_shuffle` | The auxiliary service name |
| `yarn.nodemanager.aux-services.mapreduce_shuffle.class` | `org.apache.hadoop.mapred.ShuffleHandler` | The auxiliary service class to use |
| `yarn.nodemanager.aux-services.%s.classpath` | NONE | local directory which includes the related jar file as well as all the dependencies’ jar file. We could specify the single jar file or use /dep/* to load all jars under the dep directory. |
| `yarn.nodemanager.aux-services.%s.remote-classpath` | NONE | The remote absolute or relative path to jar file |

#### Example of loading jar file from HDFS:

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle,AuxServiceFromHDFS</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services.AuxServiceFromHDFS.remote-classpath</name>
        <value>/aux/test/aux-service-hdfs.jar</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services.AuxServiceFromHDFS.class</name>
        <value>org.apache.auxtest.AuxServiceFromHDFS2</value>
    </property>
</configuration>
```

#### Example of loading jar file from local file system:

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle,AuxServiceFromHDFS</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services.AuxServiceFromHDFS.classpath</name>
        <value>/aux/test/aux-service-hdfs.jar</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services.AuxServiceFromHDFS.class</name>
        <value>org.apache.auxtest.AuxServiceFromHDFS2</value>
    </property>
</configuration>
```

**IMPORTANT:** If setting an auxiliary service in addition the default
`mapreduce_shuffle` service, then a new service key should be added to the
`yarn.nodemanager.aux-services` property, for example `mapred.shufflex`.
Then the property defining the corresponding class must be
`yarn.nodemanager.aux-services.mapreduce_shufflex.class`.
