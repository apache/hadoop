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
POST URL - http://localhost:8088/app/v1/services

##### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 2,
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
GET URL - http://localhost:8088/app/v1/services/hello-world

Note, lifetime value of -1 means unlimited lifetime.

```json
{
    "name": "hello-world",
    "version": "1.0.0",
    "description": "hello world example",
    "id": "application_1503963985568_0002",
    "lifetime": -1,
    "state": "STABLE",
    "components": [
        {
            "name": "hello",
            "state": "STABLE",
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
                    "id": "container_e03_1503963985568_0002_01_000002",
                    "ip": "10.22.8.143",
                    "hostname": "ctr-e03-1503963985568-0002-01-000002.example.site",
                    "state": "READY",
                    "launch_time": 1504051512412,
                    "bare_host": "host100.cloud.com",
                    "component_instance_name": "hello-0"
                },
                {
                    "id": "container_e03_1503963985568_0002_01_000003",
                    "ip": "10.22.8.144",
                    "hostname": "ctr-e03-1503963985568-0002-01-000003.example.site",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "host100.cloud.com",
                    "component_instance_name": "hello-1"
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
PUT URL - http://localhost:8088/app/v1/services/hello-world

##### PUT Request JSON

Note, irrespective of what the current lifetime value is, this update request will set the lifetime of the service to be 3600 seconds (1 hour) from the time the request is submitted. Hence, if a a service has remaining lifetime of 5 mins (say) and would like to extend it to an hour OR if an application has remaining lifetime of 5 hours (say) and would like to reduce it down to an hour, then for both scenarios you need to submit the same request below.

```json
{
  "lifetime": 3600
}
```
### Stop a service
PUT URL - http://localhost:8088/app/v1/services/hello-world

##### PUT Request JSON
```json
{
  "state": "STOPPED"
}
```

### Start a service
PUT URL - http://localhost:8088/app/v1/services/hello-world

##### PUT Request JSON
```json
{
  "state": "STARTED"
}
```

### Update to flex up/down the number of containers (instances) of a component of a service
PUT URL - http://localhost:8088/app/v1/services/hello-world/components/hello

##### PUT Request JSON
```json
{
  "number_of_containers": 3
}
```

Alternatively, you can specify the entire "components" section instead.

PUT URL - http://localhost:8088/app/v1/services/hello-world
##### PUT Request JSON
```json
{
  "state": "FLEX",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 3
      }
    ]
}
```

### Destroy a service
DELETE URL - http://localhost:8088/app/v1/services/hello-world

***

### Create a complicated service  - HBase
POST URL - http://localhost:8088:/app/v1/services/hbase-app-1

##### POST Request JSON

```json
{
  "name": "hbase-app-1",
  "version": "1.0.0",
  "description": "hbase service",
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

### Create a service requesting GPUs in addition to CPUs and RAM
POST URL - http://localhost:8088/app/v1/services

##### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example with GPUs",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 2,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256",
          "additional" : {
            "yarn.io/gpu" : {
              "value" : 4,
              "unit" : ""
            }
          }
        }
      }
    ]
}
```

### Create a service with a component requesting anti-affinity placement policy
POST URL - http://localhost:8088/app/v1/services

##### POST Request JSON
```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "description": "hello world example with anti-affinity",
  "components" :
    [
      {
        "name": "hello",
        "number_of_containers": 3,
        "artifact": {
          "id": "nginx:latest",
          "type": "DOCKER"
        },
        "launch_command": "./start_nginx.sh",
        "resource": {
          "cpus": 1,
          "memory": "256"
        },
        "placement_policy": {
          "constraints": [
            {
              "type": "ANTI_AFFINITY",
              "scope": "NODE",
              "node_attributes": {
                "os": ["linux", "windows"],
                "fault_domain": ["fd1", "fd2"]
              },
              "node_partitions": [
                "gpu",
                "fast-disk"
              ],
              "target_tags": [
                "hello"
              ]
            }
          ]
        }
      }
    ]
}
```

##### GET Response JSON
GET URL - http://localhost:8088/app/v1/services/hello-world

Note, for an anti-affinity component no more than 1 container will be allocated
in a specific node. In this example, 3 containers have been requested by
component "hello". All 3 containers were allocated because the cluster had 3 or
more NMs. If the cluster had less than 3 NMs then less than 3 containers would
be allocated. In cases when the number of allocated containers are less than the
number of requested containers, the component and the service will be in
non-STABLE state.

```json
{
    "name": "hello-world",
    "version": "1.0.0",
    "description": "hello world example with anti-affinity",
    "id": "application_1503963985568_0003",
    "lifetime": -1,
    "state": "STABLE",
    "components": [
        {
            "name": "hello",
            "state": "STABLE",
            "resource": {
                "cpus": 1,
                "memory": "256"
            },
            "placement_policy": {
              "constraints": [
                {
                  "type": "ANTI_AFFINITY",
                  "scope": "NODE",
                  "node_attributes": {
                    "os": ["linux", "windows"],
                    "fault_domain": ["fd1", "fd2"]
                  },
                  "node_partitions": [
                    "gpu",
                    "fast-disk"
                  ],
                  "target_tags": [
                    "hello"
                  ]
                }
              ]
            },
            "configuration": {
                "properties": {},
                "env": {},
                "files": []
            },
            "quicklinks": [],
            "containers": [
                {
                    "id": "container_e03_1503963985568_0003_01_000002",
                    "ip": "10.22.8.143",
                    "hostname": "ctr-e03-1503963985568-0003-01-000002.example.site",
                    "state": "READY",
                    "launch_time": 1504051512412,
                    "bare_host": "host100.cloud.com",
                    "component_instance_name": "hello-0"
                },
                {
                    "id": "container_e03_1503963985568_0003_01_000003",
                    "ip": "10.22.8.144",
                    "hostname": "ctr-e03-1503963985568-0003-01-000003.example.site",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "host101.cloud.com",
                    "component_instance_name": "hello-1"
                },
                {
                    "id": "container_e03_1503963985568_0003_01_000004",
                    "ip": "10.22.8.145",
                    "hostname": "ctr-e03-1503963985568-0003-01-000004.example.site",
                    "state": "READY",
                    "launch_time": 1504051536450,
                    "bare_host": "host102.cloud.com",
                    "component_instance_name": "hello-2"
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

