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

# MaWo: A Master Worker framework on YARN Services

## Overview

MaWo is a YARN service based framework which handles Master Worker based workload.
This is an app which can take an input job specification with tasks, their expected durations and have a Master dish the tasks off to a predetermined set of workers.
The components will be responsible to finish the job within specific time duration.

## MaWo Components

MaWo app is a YARN Service Application. It has mainly two components.

* Master
 - Read MaWo-Payload file and create a queue of Tasks
 - Register Worker
 - Assign tasks to worker nodes
 - Monitor status of Tasks
 - Log Task status

* Worker
 - Send heartbeat to Worker
 - Execute Task