---
title: "SCMCLI"
date: 2017-08-10

---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

SCM is the block service for Ozone. It is also the workhorse for ozone. But user process never talks to SCM. However, being able to read the state of SCM is useful.

SCMCLI allows the developer to access SCM directly. Please note: Improper usage of this tool can destroy your cluster. Unless you know exactly what you are doing, Please do *not* use this tool. In other words, this is a developer only tool. We might even remove this command in future to prevent improper use.

[^1]: This assumes that you have a working docker installation on the development machine.
