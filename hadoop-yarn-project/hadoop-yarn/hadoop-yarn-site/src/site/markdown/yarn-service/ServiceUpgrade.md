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

# Service Upgrade (Experimental Feature - Tech Preview)

Yarn service provides a way of upgrading/downgrading long running applications without
shutting down the application to minimize the downtime during this process. This is
an experimental feature which is currently not enabled by default.

## Overview

Upgrading a Yarn Service is a 3 steps (or 2 steps when auto-finalization of
upgrade is chosen) process:

1. Initiate service upgrade.\
This step involves providing the service spec of the newer version of the service.
Once, the service upgrade is initiated, the state of the service is changed to
`UPGRADING`.

2. Upgrade component instances.\
This step involves triggering upgrade of individual component instance.
By providing an API to upgrade at instance level, users can orchestrate upgrade
of the entire service in any order which is relevant for the service.\
In addition, there are APIs to upgrade multiple instances, all instances of a
component, and all instances of multiple components.

3. Finalize upgrade.\
This step involves finalization of upgrade. With an explicit step to finalize the
upgrade, users have a chance to cancel current upgrade in progress. When the
user chose to cancel, the service will make the best effort to revert to the
previous version.\
\
When the upgrade is finalized, the old service definition is
overwritten by the new service definition and the service state changes to `STABLE`.\
A service can be auto-finalized when the upgrade is initialized with
`-autoFinalize` option. With auto-finalization, when all the component-instances of
the service have been upgraded, finalization will be performed automatically by the
service framework.\
\
**NOTE**: Cancel of upgrade is not implemented yet.

## Upgrade Example
This example shows upgrade of sleeper service. Below is the sleeper service
definition

```
{
  "name": "sleeper-service",
  "components" :
    [
      {
        "name": "sleeper",
        "version": "1.0.0",
        "number_of_containers": 1,
        "launch_command": "sleep 900000",
        "resource": {
          "cpus": 1,
          "memory": "256"
       }
      }
    ]
}
```
Assuming, user launched an instance of sleeper service named as `my-sleeper`:
```
{
  "components":
    [
      {
        "configuration": {...},
        "containers":
          [
            {
              "bare_host": "0.0.0.0",
              "component_instance_name": "sleeper-0",
              "hostname": "example.local",
              "id": "container_1531508836237_0002_01_000002",
              "ip": "0.0.0.0",
              "launch_time": 1531941023675,
              "state": "READY"
            },
            {
              "bare_host": "0.0.0.0",
              "component_instance_name": "sleeper-1",
              "hostname": "example.local",
              "id": "container_1531508836237_0002_01_000003",
              "ip": "0.0.0.0",
              "launch_time": 1531941024680,
              "state": "READY"
            }
          ],
        "dependencies": [],
        "launch_command": "sleep 900000",
        "name": "sleeper",
        "number_of_containers": 2,
        "quicklinks": [],
        "resource": {...},
        "restart_policy": "ALWAYS",
        "run_privileged_container": false,
        "state": "STABLE"
      }
    ],
  "configuration": {...},
  "id": "application_1531508836237_0002",
  "kerberos_principal": {},
  "lifetime": -1,
  "name": "my-sleeper",
  "quicklinks": {},
  "state": "STABLE",
  "version": "1.0.0"
}
```

### Enable Service Upgrade
Below is the configuration in `yarn-site.xml` required for enabling service
upgrade.

```
  <property>
    <name>yarn.service.upgrade.enabled</name>
    <value>true</value>
  </property>
```

### Initiate Upgrade
User can initiate upgrade using the below command:
```
yarn app -upgrade ${service_name} -initate ${path_to_new_service_def_file} [-autoFinalize]
```

e.g. To upgrade `my-sleeper` to sleep for *1200000* instead of *900000*, the user
can upgrade the service to version 1.0.1. Below is the service definition for
version 1.0.1 of sleeper-service:

```
{
  "components" :
    [
      {
        "name": "sleeper",
        "version": "1.0.1",
        "number_of_containers": 1,
        "launch_command": "sleep 1200000",
        "resource": {
          "cpus": 1,
          "memory": "256"
        }
      }
    ]
}
```
The command below initiates the upgrade to version 1.0.1.
```
yarn app -upgrade my-sleeper -initiate sleeper_v101.json
```

### Upgrade Instance
User can upgrade a component instance using the below command:
```
yarn app -upgrade ${service_name} -instances ${comma_separated_list_of_instance_names}
```
e.g. The command below upgrades `sleeper-0` and `sleeper-1` instances of `my-service`:
```
yarn app -upgrade my-sleeper -instances sleeper-0,sleeper-1
```

### Upgrade Component
User can upgrade a component, that is, all the instances of a component with
one command:
```
yarn app -upgrade ${service_name} -components ${comma_separated_list_of_component_names}
```
e.g. The command below upgrades all the instances of `sleeper` component of `my-service`:
```
yarn app -ugrade my-sleeper -components sleeper
```

### Finalize Upgrade
User must finalize the upgrade using the below command (since autoFinalize was not specified during initiate):
```
yarn app -upgrade ${service_name} -finalize
```
e.g. The command below finalizes the upgrade of `my-sleeper`:
```
yarn app -upgrade my-sleeper -finalize
```
