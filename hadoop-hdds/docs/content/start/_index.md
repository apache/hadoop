---
title: Getting Started
name: Getting Started
identifier: Starting
menu: main
weight: 1
cards: "false"
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


{{<jumbotron title="Installing Ozone">}}
There are many ways to install and run Ozone. Starting from simple docker
deployments on
local nodes, to full scale multi-node cluster deployment on
Kubernetes or bare-metal.
{{</jumbotron>}}

<section class="row cardgroup">

<span class="label label-warning label-">Easy Start</span>

<h2>Running Ozone from Docker Hub</h2>

You can try out Ozone using docker hub without downloading the official release. This makes it easy to explore Ozone.
<br />
  {{<card title="Starting ozone inside a single container" link="start/StartFromDockerHub.md" link-text="Ozone In Docker" image="start/docker.png">}}
  The simplest and easiest way to start an ozone cluster
      to explore what it can do is to start ozone via docker.
  {{</card>}}

</section>

<section class="row cardgroup">

<span class="label label-success">Recommended</span>


<h2>Running Ozone from an Official Release</h2>

 Apache Ozone can also be run from the official release packages. Along with the official source releases, we also release a set of convenience binary packages. It is easy to run these binaries in different configurations.
<br />
  {{<card title="Ozone on a physical cluster" link="start/OnPrem" link-text="On-Prem Ozone Cluster" image="start/hadoop.png">}}
Ozone is designed to work concurrently with HDFS. The physical cluster instructions explain each component of Ozone and how to deploy with maximum control.
  {{</card>}}

  {{<card title="Ozone on K8s" link="start/Kubernetes" link-text="Kubernetes" image="start/k8s.png">}}
Ozone is designed to work well under Kubernetes. These are instructions to deploy Ozone on K8s. Ozone provides a replicated storage solution for K8s based apps.
  {{</card>}}

  {{<card title="Ozone using MiniKube" link="start/Minikube" link-text="Minikube cluster" image="start/minikube.png">}}
Ozone comes with a standard set of K8s resources. You can deploy them to MiniKube and experiment with the K8s based deployments.
  {{</card>}}

  {{<card title="Ozone cluster in Local Node" link="start/RunningViaDocker.md" link-text="docker-compose" image="start/docker.png">}}
 We also ship standard docker files with official release. These are part of official release and not depend upon Docker Hub.
  {{</card>}}

</section>

<section class="row cardgroup">

<span class="label label-danger">Hadoop Ninja</span>

<h2>Building From Sources </h2>

 Instructions to build Ozone from source to create deployment packages.

  {{<card title="Building From Sources" link="start/FromSource.md" link-text="Build ozone from source" image="start/hadoop.png">}}
If you are a Hadoop ninja, and wise in the ways of Apache, you already know that a real Apache release is a source release. We believe that even ninjas need help at times.
  {{</card>}}

</section>
