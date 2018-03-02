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

This document describes how to configure the services to be deployed on YARN

There are mainly three types of configurations:

* The configurations specific to the custom service running on YARN . E.g. the hbase-site.xml for a Hbase service running on YARN.
    * It can be specified at both global service level(`Service#configuration`) or component level(`Component#configuration`).
    * Service-level configs are considered as the default configs for all components and component-level config can override service level config.
    * All config properties that uses constant variables as described below are subject to substitutions.
* The configurations specific to YARN service AM. (`Configuration#properties`).
    * E.g. The `yarn.service.am-restart.max-attempts` which controls how many times
the framework AM can be retried if it fails. These configs are mainly to control the behavior of the framework AM , rather than the custom services launched by the framework.
* Some constants such as `SERVICE_NAME` for referring some system properties.
    * They are substituted by the service AM before writing the config files.

Below describes the details for each type of configurations.

## Configuration for custom service
Below is how a configuration object typically looks like:
```
"configuration" : {
    "properties" : {
        "yarn.service.am-restart.max-attempts" : 10  // config for the service AM
    },
    "env" : {                 // The environment variables to be exported when container gets launched
        "env1" : "val1"
    },
    "files" : [               // The list of configuration files to be mounted for the container
        {
            "type": "HADOOP_XML", // The format of the config file into which the "properties" are dumped
            "dest_file": "/etc/hadoop/conf/core-site.xml", // The location where the config file is mounted inside the container
            "properties" : {             // The list of key/value pairs to be dumped into the config file
                "fs.defaultFS" : "hdfs://myhdfs" // This property will be written into core-site.xml
            }
        },
        {
            "type": "HADOOP_XML",    // The format of the config file.
            "src_file" : ""hdfs://mycluster/tmp/conf/yarn-site.xml""  // The location of the source config file to be downloaded
            "dest_file": "/etc/hadoop/conf/yarn-site.xml",            // The location where the config file will be mounted inside the container/
            "properties" : {
                "yarn.resourcemanager.webapp.address" : "${COMPONENT_INSTANCE_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}"  // Merge into (or override existing property in) yarn-site.xml
            }
        }
    ]
}
```

* properties: the configurations for service AM. Details below.
* env :  the environment variables to be exported when container gets launched.
* files :  The list of configuration files to be mounted inside the container.
    * type: The format of the config file(`dest_file`) to be mounted inside the container. If `src_file` is specified, it is also the format for both `src_file` and `dest_file`.
       * HADOOP_XML : the  hadoop xml format. If `src_file` is specified, the file content will be read as parsed in hadoop xml format.
       * XML : the standard xml format
       * JSON : the standard JSON format
       * YAML : the YAML format
       * PROPERTIES : the java PROPERTIES format
       * TEMPLATE : the plain text format. If `src_file` is specified, the content of the `src_file` will be written into `dest_file` post constant substitution.
        If `src_file` is not specified, use `content` as the key in `properties` field, and the value will be the actual content to be written in the `dest_file` post constant substitution. E.g

           ```
           {
               "type": "TEMPLATE"
               "dest_file": "/etc/conf/hello"
               "properties" : {
                    "content" : "Hello world"
               }
           }
           ```
           The string `Hello world` will be written into a file located at `/etc/conf/hello` inside the container.
    * src_file : [optional], the source location of the config file at a network accessible location such as hdfs.
       * The format of both `src_file` and `dest_file` are defined by `type`.
       * Currently, It only works with `HADOOP_XML` and `TEMPLATE` type.
       * The `src_file` will be downloaded by YARN NodeManager and be mounted inside the container as in the location specified by `dest_file`.
       * If any properties specified in the `properties` field, they are added into (or overwriting existing properties in) the `src_file`.
       * If `src_file` is not specified, only the properties in the `properties` field will be written into the `dest_file` file.
    * dest_file : the location where the config file is mounted inside the container. The file format is defined by `type`.
                 dest_file can be an absolute path or a relative path. If it's a relative path, the file will be located in the `$PWD/conf` directory (where `$PWD` is the container local directory which is mounted to all docker containers launched by YARN)
    * properties : The list of key/value pair configs to be written into the `dest_file` in the format as defined in `type`. If `src_file` is specified, these properties will be added into (or overwriting existing properties in) the `src_file`.

## Configuration for YARN service AM
This section describes the configurations for configuring the YARN service AM.

These can be specified either in the cluster `yarn-site.xml` at the global level or in the `properties` field of the `Configuration` object as per service basis like below:
```
{
    "configuration" : {
        "properties" : {
            "yarn.service.am-restart.max-attempts" : 10
        }
    }
}
```
Above config make the service AM to be retried at max 10 times.

#### Available configurations:

| Name | Description |
| ------------ | ------------- |
|yarn.service.client-am.retry.max-wait-ms | the max retry time in milliseconds for the service client to talk to the service AM. By default, it is set to 0, which means no retry |
|yarn.service.client-am.retry-interval-ms | the retry interval in milliseconds for the service client to talk to the service AM. By default, it is 2000, i.e. 2 seconds |
|yarn.service.container-failure.retry.max | the max number of retries for the container to be auto restarted if it fails. By default, it is set to -1, which means forever.
|yarn.service.container-failure.retry-interval-ms| the retry interval in milliseconds for the container to be restarted. By default, it is 30000, i.e. 30 seconds |
|yarn.service.am-restart.max-attempts| the max number of attempts for the framework AM
|yarn.service.am-resource.memory | the memory size in GB for the framework AM. By default, it is set to 1024
|yarn.service.queue | the default queue to which the service will be submitted. By default, it is submitted to `default` queue
|yarn.service.base.path | the root location for the service artifacts on hdfs for a user. By default, it is under ${user_home_dir}/.yarn/
|yarn.service.container-failure-per-component.threshold | the max number of container failures for a given component before the AM exits.
|yarn.service.node-blacklist.threshold | Maximum number of container failures on a node before the node is blacklisted by the AM
|yarn.service.failure-count-reset.window | The interval in seconds when the `yarn.service.container-failure-per-component.threshold` and `yarn.service.node-blacklist.threshold` gets reset. By default, it is 21600, i.e. 6 hours
|yarn.service.readiness-check-interval.seconds | The interval in seconds between readiness checks. By default, it is 30 seconds
|yarn.service.log.include-pattern| The regex expression for including log files whose file name matches it when aggregating the logs after the application completes.
|yarn.service.log.exclude-pattern| The regex expression for excluding log files whose file name matches it when aggregating the logs after the application completes. If the log file name matches both include and exclude pattern, this file will be excluded.
|yarn.service.rolling-log.include-pattern| The regex expression for including log files whose file name matches it when aggregating the logs while app is running.
|yarn.service.rolling-log.exclude-pattern| The regex expression for excluding log files whose file name matches it when aggregating the logs while app is running. If the log file name matches both include and exclude pattern, this file will be excluded.
|yarn.service.container-recovery.timeout.ms| The timeout in milliseconds after which the service AM releases all the containers of previous attempt which are not yet recovered by the RM. By default, it is set to 120000, i.e. 2 minutes.

## Constant variables for custom service
The service framework provides some constant variables for user to configure their services. These variables are either dynamically generated by the system or are static ones such as service name defined by the user.
User can use these constants in their configurations to be dynamically substituted by the service AM.E.g.
```
{
    "type" : "HADOOP_XML",
    "dest_file" : "/etc/hadoop/hbase-site.xml",
    "properties" : {
        "hbase.regionserver.hostname": "${COMPONENT_INSTANCE_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}"
    }
}
```
Here, `COMPONENT_INSTANCE_NAME` and `SERVICE_NAME` are the constants to be substituted by the system.

Suppose the `COMPONENT_INSTANCE_NAME` is `regionserver-0` and `SERVICE_NAME` is defined by user as `hbase`, user name is `devuser` and domain name is `hwxdev.site`.
Then, the config will be substituted by the service AM and written in the config file `/etc/hadoop/hbase-site.xml` inside the container as below:
```
<property>
  <name>hbase.regionserver.hostname</name>
  <value>regionserver-0.hbase.devuser.hwxdev.site</value>
</property>
```
where `regionserver-0` is the actual component instance name assigned by the system for this container.

#### Available constants:
| Name | Description |
| ------------ | ------------- |
| SERVICE_NAME | name of the service defined by the user
| USER | user who submits the service. Note that user name which has "\_" will be converted to use "-", to conform with DNS hostname RFC format which doesn't allow "\_", and all characters will be lowercased E.g. "Bob_dev" will be converted to "bob-dev"  |
| DOMAIN | the domain name for the cluster |
| COMPONENT_NAME | the name for a given component |
| COMPONENT_INSTANCE_NAME | the name for a given component instance (i.e. container) |
| COMPONENT_ID | the monotonically increasing integer for a given component
| CONTAINER_ID | the YARN container Id for a given container |
| ${COMPONENT_INSTANCE_NAME}_HOST | the hostname for a component instance (i.e. container), e.g. REGIONSERVER-0_HOST will be substituted by the actual hostname of the component instance. Note all characters must be uppercase. |
| ${COMPONENT_INSTANCE_NAME}_IP | the ip for a component instance (i.e. container), e.g. REGIONSERVER-0_IP will be substituted by the actual IP address of the component instance. Note all characters must be uppercase. |
| CLUSTER_FS_URI | the URI of the cluster hdfs |
