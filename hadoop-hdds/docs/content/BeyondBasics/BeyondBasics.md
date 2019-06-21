---
title: "Beyond Basics"
date: "2017-10-10"
menu: main
weight: 7

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

<nav aria-label="breadcrumb">
  <ol class="breadcrumb">
    <li class="breadcrumb-item"><a href="/">Home</a></li>
    <li class="breadcrumb-item active" aria-current="page">Beyond Basics</li>
  </ol>
</nav>


<div class="jumbotron jumbotron-fluid">
  <div class="container">
    <h3 class="display-4">Beyond Basics  </h3>
    <p class="lead">
  Beyond Basics pages go into custom configurations of Ozone, including how
  to run Ozone concurrently with an existing HDFS cluster. These pages also
  take deep into how to run profilers and leverage tracing support built into
  Ozone.
</div>
</div>

<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Runing with HDFS</h2>
    <p class="card-text">Ozone is designed to run concurrently with HDFS.
    This page explains how to deploy Ozone in a exisiting HDFS cluster.</p>
     <a href="{{< ref "BeyondBasics/RunningWithHDFS.md" >}}"
    class="btn btn-primary">HDFS & Ozone</a>
</div>
</div>
</div>



<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Ozone and Containers</h2>
    <p class="card-text">
    Ozone uses containers extensively for testing. This page documents the
    usage and best practices of Ozone.
    </p>
     <a href="{{< ref "BeyondBasics/Containers.md" >}}"
    class="btn btn-primary">Ozone & Containers</a>
</div>
</div>
</div>




<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Tools</h2>
    <p class="card-text">Ozone supports a set of tools that are handy for
    developers.Here is a quick list of command line tools.
    </p>
     <a href="{{< ref "BeyondBasics/Tools.md" >}}"
    class="btn btn-primary">Tools</a>
</div>
</div>
</div>

<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Docker Cheat Sheet</h2>
    <p class="card-text"> Docker Compose cheat sheet to help you remember the
     common commands to control an Ozone cluster running on top of Docker.
     </p>
     <a href="{{< ref "BeyondBasics/DockerCheatSheet.md" >}}"
    class="btn btn-primary">Cheat Sheet</a>
</div>
</div>


</div>


<br>
<a href="{{< ref "BeyondBasics/RunningWithHDFS.md" >}}"> <button type="button"
class="btn  btn-success btn-lg">Next >></button>
