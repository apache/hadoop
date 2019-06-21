---
title: Getting Started
name: Getting Started
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


<nav aria-label="breadcrumb">
  <ol class="breadcrumb">
    <li class="breadcrumb-item"><a href="/">Home</a></li>
    <li class="breadcrumb-item active" aria-current="page">Getting Started</li>
  </ol>
</nav>

<div class="jumbotron jumbotron-fluid">
  <div class="container">
    <h3 class="display-4">Installing Ozone </h3>
    <p class="lead">
There are many ways to install and run Ozone. Starting from simple docker
deployments on
local nodes, to full scale multi-node cluster deployment on
kubernetes or bare-metal.</p>
  </div>
</div>



<div class="card">
  <div class="card-header">
    Easy Start
  </div>
  <div class="card-body">
    <h3 class="card-title">Running Ozone from Docker Hub</h3>
    <p class="card-text"> You can try out Ozone using docker hub without
    downloading the official release. This makes it easy to explore Ozone.</p>

<div class="row">
<div class="card" >
  <img class="card-img-top" src="homepage-docker-logo.png" alt="Card
  image cap">
  <div class="card-body">
    <h4 class="card-title">Starting ozone inside a single container</h4>
    <p class="card-text">The simplest and easiest way to start an ozone cluster
     to explore what it can do is to start ozone via docker.</p>
     <a href="{{< ref "start/StartFromDockerHub.md" >}}"
    class="btn btn-primary"> Ozone in Docker</a>
  </div>
</div>


  </div>
</div>

<br>
<br>

<div class="card">
  <div class="card-header">
    Recommended
  </div>
  <div class="card-body">
    <h3 class="card-title">Running Ozone from an Offical Release.</h3>
    <p class="card-text">
    Apache Ozone can also be run from the official release packages. Along with
    the official source releases, we also release a set of convenience binary
    packages. It is easy to run these binaries in different configurations.</p>
<div class="row">
  <div class="col-sm-6">
<div class="card" >
  <img class="card-img-top" src="hadoop.png" alt="Card image cap">
  <div class="card-body">
    <h4 class="card-title">Deploying Ozone on a physical cluster.</h4>
    <p class="card-text">Ozone is designed to work concurrently with HDFS.
    The physical cluster instructions explain each component of Ozone and
    how to deploy with maximum control.</p>
     <a href="{{< ref "start/onPrem.md" >}}"
    class="btn btn-primary">On-Prem Ozone Cluster</a>
  </div>
</div>
</div>

<div class="card" >
  <div class="col-sm-6">
  <img class="card-img-top" src="k8s.logo.png" alt="Card image cap">
  <div class="card-body">
    <h4 class="card-title">Deploying Ozone on K8s</h4>
    <p class="card-text">Ozone is designed to work well under kubernetes.
    These are
     instructions on how to deploy Ozone on  K8s platform. Ozone provides a
     replicated storage solution for K8s based Applications.
     </p>
     <a href="{{< ref "start/Kubernetes.md" >}}"
    class="btn btn-primary">Ozone on Kubernetes</a>
  </div>
</div>
</div>


<br>
<br>
<br>
<div class="card" >
  <div class="col-sm-6">
  <img class="card-img-top" src="minikube-logo.png" alt="Card image cap">
  <div class="card-body">
    <h4 class="card-title">Deploy Ozone using MiniKube.</h4>
    <p class="card-text"> Ozone comes with a standard set of K8s
    resources.
    You can deploy them to MiniKube and experiment with the K8s based
    deployments.
    <br>
    </p>
     <a href="{{< ref "start/Minikube.md" >}}"
    class="btn btn-primary">MiniKube Cluster</a>
  </div>
</div>
</div>
 <br>
 <br>
 <br>
<div class="card" >
  <div class="col-sm-6">
  <img class="card-img-top" src="homepage-docker-logo.png" alt="Card image cap">
  <div class="card-body">
    <h4 class="card-title">An Ozone cluster in Local Node.</h4>
    <p class="card-text"> We also ship standard docker files with official
    release, if you want to use them. These are part of official release and
    not depend upon Docker Hub.
    </p>
     <a href="{{< ref "start/RunningViaDocker.md" >}}"
    class="btn btn-primary">Multi-Container Cluster</a>
  </div>
</div>
</div>
</div>
</div>

<br>
<br>

<div class="card">
  <div class="card-header">
    Hadoop Ninja
  </div>
  <div class="card-body">
    <h3 class="card-title">Building From Sources.</h3>
    <p class="card-text"> Instructions to build Ozone from source to create
    deployment packages.</p>

<div class="row">
<div class="card" >
  <img class="card-img-top" src="hadoop.png" alt="Card image cap">
  <div class="card-body">
    <h4 class="card-title"></h4>
    <p class="card-text">If you are a Hadoop ninja,
    and wise in the ways of Apache, you already know that a real
    Apache release is a source release. We believe that even ninjas need
    help at times. </p>
     <a href="{{< ref "start/FromSource.md" >}}"
    class="btn btn-primary">Creating a Release Package from Source.</a>
  </div>
</div>
</div>
</div>
</div>
<br>
<br>
<a href="{{< ref "start/StartFromDockerHub.md" >}}"> <button type="button"
class="btn  btn-success btn-lg">Next >></button>
