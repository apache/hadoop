---
title: Concepts
date: "2017-10-10"
menu: main
weight: 6

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
    <li class="breadcrumb-item active" aria-current="page">Concepts</li>
  </ol>
</nav>


<div class="jumbotron jumbotron-fluid">
  <div class="container">
    <h3 class="display-4">Ozone Architecture </h3>
    <p class="lead">
Ozone's architectural elements are explained in the following pages. The
metadata layer, data layer, protocol bus, replication layer and Recon  are
discussed in the following pages. These concepts are useful if you want to
understand how ozone works in depth.
</div>
</div>

<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Overview</h2>
    <p class="card-text">Ozone's overview and components that make up Ozone.</p>
     <a href="{{< ref "concept/Overview.md" >}}"
    class="btn btn-primary">Overview</a>
</div>
</div>
</div>



<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Ozone Manager</h2>
    <p class="card-text">Ozone Manager is te principal name space service of
    Ozone. OM manages the life cycle of volumes, buckets and Keys.
    </p>
     <a href="{{< ref "concept/OzoneManager.md" >}}"
    class="btn btn-primary">Ozone Manager</a>
</div>
</div>
</div>

<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Storage Container Manager</h2>
    <p class="card-text"> Storage Container Manager or SCM is the core
    metadata service of Ozone. SCM provides a distrubuted block layer for
    Ozone.
     </p>
     <a href="{{< ref "concept/Hdds.md" >}}"
    class="btn btn-primary">Storage Container Manager</a>
</div>
</div>
</div>


<div class="row">
       <div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Datanodes</h2>
    <p class="card-text">Ozone supports Amazon's Simple Storage Service (S3)
   protocol. In fact, You can use S3 clients and S3 SDK based applications
   without any modifications with Ozone.
    </p>
     <a href="{{< ref "concept/datanodes.md" >}}"
    class="btn btn-primary">Datanodes</a>
</div>
</div>
</div>
</div>


<a href="{{< ref "concept/Overview.md" >}}"> <button type="button"
class="btn  btn-success btn-lg">Next >></button>