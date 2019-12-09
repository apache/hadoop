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

# System Services

## Overview
System services are admin configured services which are auto deployed during bootstrap of ResourceManager. This would work only when API-Server is started as part of ResourceManager. Refer [Manage services on YARN](QuickStart.html#Manage_services_on_YARN_via_REST_API). This document describes how to configure and deploy system services.

## Configuration

| Name | Description |
| ------------ | ------------- |
|yarn.service.system-service.dir| FS directory path to load and deploy admin configured services. These service spec files should be kept with proper hierarchy.|

## Hierarchy of FS path
After configuring *yarn.service.system-service.dir* path, the spec files should be kept with below hierarchy.
````
$SYSTEM_SERVICE_DIR_PATH/<Launch-Mode>/<Users>/<Yarnfiles>.
````
### Launch-Mode
Launch-Mode indicates that how the service should be deployed. Services can be auto deployed either synchronously or asynchronously.

#### sync
These services are started synchronously along with RM. This might delay a bit RM transition to active period. This is useful when deploying critical services to get started sooner.

#### async
These services are started asynchronously without impacting RM transition period.

### Users
Users are the owner of the system service who has full access to modify it. Each users can own multiple services. Note that service names are unique per user.

### Yarnfiles
YarnFiles are the spec files to launch services. These files must have .yarnfile extension otherwise those files are ignored.

### Example of hierarchy to configure system services.

```
SYSTEM_SERVICE_DIR_PATH
|---- sync
|     |--- user1
|     |    |---- service1.yarnfile
|     |    |---- service2.yarnfile
|     |--- user2
|     |    |---- service3.yarnfile
|     |    ....
|     |
|---- async
|     |--- user3
|     |    |---- service1.yarnfile
|     |    |---- service2.yarnfile
|     |--- user4
|     |    |---- service3.yarnfile
|     |    ....
|     |
```