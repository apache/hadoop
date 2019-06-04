---
title: Getting started
name: Getting started
identifier: Starting
menu: main
weight: 1
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

## Getting Started

<div class="panel panel-default">
  <div class="panel-heading">Based on docker images from dockerhub</div>
  <div class="panel-body">
    <a href="{{< ref "start/StartFromDockerHub.md" >}}"><button class="btn btn-default btn-lg">Full cluster in one container (local, single container)</button></a>

    <a href="{{< ref "start/StartFromDockerHubCompose.md" >}}"><button class="btn btn-default btn-lg">Using multiple containers (local, multi-container)</button></a>


  </div>
</div>

<div class="panel panel-default">
  <div class="panel-heading">Using a release package</div>
  <div class="panel-body">
    <a href="{{< ref "start/RunningViaDocker.md" >}}"><button class="btn btn-danger btn-lg">Docker-compose (local, multi-container)</button></a>

    <a href="{{< ref "start/Kubernetes.md" >}}"><button class="btn btn-default btn-lg">Running on kubernetes</button></a>
    <a href="{{< ref "start/OnPrem.md" >}}"><button class="btn btn-default btn-lg">Running on-prem</button></a>


  </div>
</div>


<div class="panel panel-default">
  <div class="panel-heading">Using the source code</div>
  <div class="panel-body">
    <a href="{{< ref "start/FromSource.md" >}}"><button class="btn btn-default btn-lg">Compile from the source and run with docker (local, multi-node)</button></a>


  </div>