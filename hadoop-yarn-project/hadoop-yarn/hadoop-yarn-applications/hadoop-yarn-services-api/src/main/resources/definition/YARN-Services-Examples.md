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

## Examples

### Create a simple single-component service with most attribute values as defaults
POST URL - http://localhost:9191/ws/v1/services

##### POST Request JSON
```json
{
  "name": "hello-world",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 1,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256"
       }
      }
    ]
}
```

##### GET Response JSON
GET URL - http://localhost:9191/ws/v1/services/hello-world

Note, lifetime value of -1 means unlimited lifetime.

```json
{
    "name": "hello-world",
    "id": "application_1503963985568_0002",
    "lifetime": -1,
    "components": [
        {
            "name": "hello",
            "dependencies": [],
            "resource": {
                "cpus": 1,
                "memory": "256"
            },
            "configuration": {
                "properties": {},
                "env": {},
                "files": []
            },
            "quicklinks": [],
            "containers": [
                {
                    "id": "container_e03_1503963985568_0002_01_000001",
                    "ip": "10.22.8.143",
                    "hostname": "myhost.local",
                    "state": "READY",
                    "launch_time": 1504051512412,
                    "bare_host": "10.22.8.143",
                    "component_name": "hello-0"
                },
                {
                    "id": "container_e03_1503963985568_0002_01_000002",
                    "ip": "10.22.8.143",
                    "hostname": "myhost.local",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "10.22.8.143",
                    "component_name": "hello-1"
                }
            ],
            "launch_command": "./start_nginx.sh",
            "number_of_containers": 1,
            "run_privileged_container": false
        }
    ],
    "configuration": {
        "properties": {},
        "env": {},
        "files": []
    },
    "quicklinks": {}
}

```
### Update to modify the lifetime of a service
PUT URL - http://localhost:9191/ws/v1/services/hello-world

##### PUT Request JSON

Note, irrespective of what the current lifetime value is, this update request will set the lifetime of the service to be 3600 seconds (1 hour) from the time the request is submitted. Hence, if a a service has remaining lifetime of 5 mins (say) and would like to extend it to an hour OR if an application has remaining lifetime of 5 hours (say) and would like to reduce it down to an hour, then for both scenarios you need to submit the same request below.

```json
{
  "lifetime": 3600
}
```
### Stop a service
PUT URL - http://localhost:9191/ws/v1/services/hello-world

##### PUT Request JSON
```json
{
    "state": "STOPPED"
}
```

### Start a service
PUT URL - http://localhost:9191/ws/v1/services/hello-world

##### PUT Request JSON
```json
{
    "state": "STARTED"
}
```

### Update to flex up/down the no of containers (instances) of a component of a service
PUT URL - http://localhost:9191/ws/v1/services/hello-world/components/hello

##### PUT Request JSON
```json
{
    "name": "hello",
    "number_of_containers": 3
}
```

### Destroy a service
DELETE URL - http://localhost:9191/ws/v1/services/hello-world

***

### Create a complicated service  - HBase
POST URL - http://localhost:9191:/ws/v1/services/hbase-app-1

##### POST Request JSON

```json
{
  "name": "hbase-app-1",
  "lifetime": "3600",
  "components": [
    {
      "name": "hbasemaster",
      "number_of_containers": 1,
      "artifact": {
        "id": "hbase:latest",
        "type": "DOCKER"
      },
      "launch_command": "/usr/hdp/current/hbase-master/bin/hbase master start",
      "resource": {
        "cpus": 1,
        "memory": "2048"
      },
      "configuration": {
        "env": {
          "HBASE_LOG_DIR": "<LOG_DIR>"
        },
        "files": [
          {
            "type": "XML",
            "dest_file": "/etc/hadoop/conf/core-site.xml",
            "properties": {
              "fs.defaultFS": "${CLUSTER_FS_URI}"
            }
          },
          {
            "type": "XML",
            "dest_file": "/etc/hbase/conf/hbase-site.xml",
            "properties": {
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "${CLUSTER_ZK_QUORUM}",
              "hbase.rootdir": "${SERVICE_HDFS_DIR}/hbase",
              "zookeeper.znode.parent": "${SERVICE_ZK_PATH}",
              "hbase.master.hostname": "hbasemaster.${SERVICE_NAME}.${USER}.${DOMAIN}",
              "hbase.master.info.port": "16010"
            }
          }
        ]
      }
    },
    {
      "name": "regionserver",
      "number_of_containers": 3,
      "unique_component_support": "true",
      "artifact": {
        "id": "hbase:latest",
        "type": "DOCKER"
      },
      "launch_command": "/usr/hdp/current/hbase-regionserver/bin/hbase regionserver start",
      "resource": {
        "cpus": 1,
        "memory": "2048"
      },
      "configuration": {
        "env": {
          "HBASE_LOG_DIR": "<LOG_DIR>"
        },
        "files": [
          {
            "type": "XML",
            "dest_file": "/etc/hadoop/conf/core-site.xml",
            "properties": {
              "fs.defaultFS": "${CLUSTER_FS_URI}"
            }
          },
          {
            "type": "XML",
            "dest_file": "/etc/hbase/conf/hbase-site.xml",
            "properties": {
              "hbase.cluster.distributed": "true",
              "hbase.zookeeper.quorum": "${CLUSTER_ZK_QUORUM}",
              "hbase.rootdir": "${SERVICE_HDFS_DIR}/hbase",
              "zookeeper.znode.parent": "${SERVICE_ZK_PATH}",
              "hbase.master.hostname": "hbasemaster.${SERVICE_NAME}.${USER}.${DOMAIN}",
              "hbase.master.info.port": "16010",
              "hbase.regionserver.hostname": "${COMPONENT_INSTANCE_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}"
            }
          }
        ]
      }
    }
  ],
  "quicklinks": {
    "HBase Master Status UI": "http://hbasemaster0.${SERVICE_NAME}.${USER}.${DOMAIN}:16010/master-status",
    "Proxied HBase Master Status UI": "http://app-proxy/${DOMAIN}/${USER}/${SERVICE_NAME}/hbasemaster/16010/"
  }
}
```
