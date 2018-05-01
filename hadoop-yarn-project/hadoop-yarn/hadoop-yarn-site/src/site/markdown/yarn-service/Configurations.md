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

# YARN Service Configurations

This document describes how to configure the services to be deployed on YARN.

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

### System-wide configuration properties
System-wide service AM properties can only be configured in the cluster `yarn-site.xml` file.

| System-Level Config Name | Description |
| ------------ | ------------- |
|yarn.service.framework.path | HDFS path of the service AM dependency tarball. When no file exists at this location, AM dependencies will be uploaded by the RM the first time a service is started or launched. If the RM user does not have permission to upload the file to this location or the location is not world readable, the AM dependency jars will be uploaded each time a service is started or launched. If unspecified, value will be assumed to be /yarn-services/${hadoop.version}/service-dep.tar.gz.|
|yarn.service.base.path | HDFS parent directory where service artifacts will be stored (default ${user_home_dir}/.yarn/).
|yarn.service.client-am.retry.max-wait-ms | Max retry time in milliseconds for the service client to talk to the service AM (default 900000, i.e. 15 minutes).|
|yarn.service.client-am.retry-interval-ms | Retry interval in milliseconds for the service client to talk to the service AM (default 2000, i.e. 2 seconds).|
|yarn.service.queue | Default queue to which the service will be submitted (default submits to the `default` queue). Note that queue can be specified per-service through the queue field, rather than through the service-level configuration properties.|

### Service-level configuration properties
Service-level service AM configuration properties can be specified either in the cluster `yarn-site.xml` at the global level (effectively overriding the default values system-wide) or specified per service in the `properties` field of the `Configuration` object as in the example below:
```
{
    "configuration" : {
        "properties" : {
            "yarn.service.am-restart.max-attempts" : 10
        }
    }
}
```
The above config allows the service AM to be retried a maximum of 10 times.

| Service-Level Config Name | Description |
| ------------ | ------------- |
|yarn.service.am-restart.max-attempts | Max number of times to start the service AM, after which the service will be killed (default 20).|
|yarn.service.am-resource.memory | Memory size in GB for the service AM (default 1024).|
|yarn.service.am.java.opts | Additional JVM options for the service AM (default " -Xmx768m" will be appended to any JVM opts that do not specify -Xmx).|
|yarn.service.container-recovery.timeout.ms | Timeout in milliseconds after which a newly started service AM releases all the containers of previous AM attempts which are not yet recovered from the RM (default 120000, i.e. 2 minutes).|
|yarn.service.failure-count-reset.window | Interval in seconds after which the container failure counts that will be evaluated for the per-component `yarn.service.container-failure-per-component.threshold` and `yarn.service.node-blacklist.threshold` are reset (default 21600, i.e. 6 hours).|
|yarn.service.readiness-check-interval.seconds | Interval in seconds between readiness checks (default 30 seconds).|
|yarn.service.log.include-pattern | Regex expression for including log files by name when aggregating the logs after the application completes (default includes all files).|
|yarn.service.log.exclude-pattern | Regex expression for excluding log files by name when aggregating the logs after the application completes. If the log file name matches both include and exclude pattern, this file will be excluded (default does not exclude any files).|
|yarn.service.rolling-log.include-pattern | Regex expression for including log files by name when aggregating the logs while app is running.|
|yarn.service.rolling-log.exclude-pattern | Regex expression for excluding log files by name when aggregating the logs while app is running. If the log file name matches both include and exclude pattern, this file will be excluded.|

### Component-level configuration properties
Component-level service AM configuration properties can be specified either in the cluster `yarn-site.xml` at the global level (effectively overriding the default values system-wide), specified per service in the `properties` field of the `Configuration` object, or specified per component in the `properties` field of the component's `Configuration` object.

| Component-Level Config Name | Description |
| ------------ | ------------- |
|yarn.service.container-failure.retry.max | Max number of retries for the container to be auto restarted if it fails (default -1, which means forever).|
|yarn.service.container-failure.retry-interval-ms | Retry interval in milliseconds for the container to be restarted (default 30000, i.e. 30 seconds).|
|yarn.service.container-failure.validity-interval-ms | Failure validity interval in milliseconds. When set to a value greater than 0, the container retry policy will not take the failures that happened outside of this interval into the failure count (default -1, which means that all the failures so far will be included in the failure count).|
|yarn.service.container-failure-per-component.threshold | Max absolute number of container failures (not including retries) for a given component before the AM stops the service (default 10).|
|yarn.service.node-blacklist.threshold | Maximum number of container failures on a node (not including retries) before the node is blacklisted by the AM (default 3).|
|yarn.service.default-readiness-check.enabled | Whether or not the default readiness check is enabled (default true).|
|yarn.service.container-health-threshold.percent | The container health threshold percent when explicitly set for a specific component or globally for all components, will schedule a health check monitor to periodically check for the percentage of healthy containers. A container is healthy if it is in READY state. It runs the check at a specified/default poll frequency. It allows a component to be below the health threshold for a specified/default window after which it considers the service to be unhealthy and triggers a service stop. When health threshold percent is enabled, yarn.service.container-failure-per-component.threshold is ignored.
|yarn.service.container-health-threshold.poll-frequency-secs | Health check monitor poll frequency. It is an advanced setting and does not need to be set unless the service owner understands the implication and does not want the default. The default is 10 secs.
|yarn.service.container-health-threshold.window-secs | The amount of time the health check monitor allows a specific component to be below the health threshold after which it considers the service to be unhealthy. The default is 600 secs (10 mins).
|yarn.service.container-health-threshold.init-delay-secs | The amount of initial time the health check monitor waits before the first check kicks in. It gives a lead time for the service containers to come up for the first time. The default is 600 secs (10 mins).

There is one component-level configuration property that is set differently in the `yarn-site.xml` file than it is in the service specification.
To select the docker network type that will be used for docker containers, `docker.network` may be set in the service `Configuration` `properties` or the component `Configuration` `properties`.
The system-wide default for the docker network type (for both YARN service containers and all other application containers) is set via the `yarn.nodemanager.runtime.linux.docker.default-container-network` property in the `yarn-site.xml` file.

### Component-level readiness check properties
The AM can be configured to perform readiness checks for containers through the `Component` field `readiness_check`.
A container will not reach the `READY` state until its readiness check succeeds.
If no readiness check is specified, the default readiness check is performed unless it is disabled through the `yarn.service.default-readiness-check.enabled` component-level configuration property.

The default readiness check succeeds when an IP becomes available for a container.
There are also optional properties that configure a DNS check in addition to the IP check.
DNS checking ensures that a DNS lookup succeeds for the container hostname before the container is considered ready.
For example, DNS checking can be enabled for the default readiness check as follows:
```
      "readiness_check": {
        "type": "DEFAULT",
        "properties": {
          "dns.check.enabled": "true"
        }
      },
```

Here is a full list of configurable properties for readiness checks that can be performed by the AM.

| Readiness Check | Configurable Property | Description |
| ------------ | ------------- | ------------- |
|DEFAULT, HTTP, PORT| dns.check.enabled | true if DNS check should be performed (default false)|
|DEFAULT, HTTP, PORT| dns.address | optional IP:port address of DNS server to use for DNS check|
|HTTP| url | required URL for HTTP response check, e.g. http://${THIS_HOST}:8080|
|HTTP| timeout | connection timeout (default 1000)|
|HTTP| min.success | minimum response code considered successful (default 200)|
|HTTP| max.success | maximum response code considered successful (default 299)|
|PORT| port | required port for socket connection|
|PORT| timeout | socket connection timeout (default 1000)|

HTTP readiness check example:
```
      "readiness_check": {
        "type": "HTTP",
        "properties": {
          "url": "http://${THIS_HOST}:8080"
        }
      },
```

PORT readiness check example:
```
      "readiness_check": {
        "type": "PORT",
        "properties": {
          "port": "8080"
        }
      },
```

#### Warning on configuring readiness checks with `host` network for docker containers
When the `host` docker network is configured for a component that has more than one container and the containers are binding to a specific port, there will be a port collision if the containers happen to be allocated on the same host.
HTTP and PORT readiness checks will not be valid in this situation.
In particular, both containers (the one that successfully binds to the port and the one that does not) may have their HTTP or PORT readiness check succeed since the checks are being performed against the same IP (the host's IP).
A valid configuration for such a service could use the anti-affinity placement policy, ensuring that containers will be assigned on different hosts so that port collisions will not occur.

## Constant variables for custom service
The service framework provides some constant variables for user to configure their services. These variables are either dynamically generated by the system or are static ones such as service name defined by the user.
User can use these constants in their configurations to be dynamically substituted by the service AM. E.g.
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

Suppose the `COMPONENT_INSTANCE_NAME` is `regionserver-0` and `SERVICE_NAME` is defined by user as `hbase`, user name is `devuser` and domain name is `dev.test`.
Then, the config will be substituted by the service AM and written in the config file `/etc/hadoop/hbase-site.xml` inside the container as below:
```
<property>
  <name>hbase.regionserver.hostname</name>
  <value>regionserver-0.hbase.devuser.dev.test</value>
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
