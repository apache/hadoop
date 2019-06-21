---
title: Recipes
date: "2017-10-10"
menu: main
weight: 8

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
        <li class="breadcrumb-item active" aria-current="page">recipes
        </li>
    </ol>
</nav>
<div class="jumbotron jumbotron-fluid">
    <div class="container">
        <h3 class="display-4">Recipes of Ozone </h3>
        <p class="lead">
          Standard How-to documents which describe how to use Ozone with
          other Software. For example, How to use Ozone with Apache Spark.
        </p>
    </div>
</div>

<br>
<br>

<div class="row">
<div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Prometheus</h2>
    <p class="card-text">A Simple recipe to monitor Ozone using Prometheus </p>
     <a href="{{< ref "recipe/Prometheus.md" >}}"
    class="btn btn-primary">Prometheus</a>
</div>
</div>
</div>

<div class="row">
<div class="col-sm-6">
<div class="card" >
  <div class="card-body">
    <h2 class="card-title">Apache Spark</h2>
    <p class="card-text">How to use Apache Spark with Ozone on K8s.</p>
     <a href="{{< ref "recipe/SparkOzoneFSK8S.md" >}}"
    class="btn btn-primary">Volume</a>
</div>
</div>
</div>
</div>

<br>

<a href="{{< ref "recipe/Prometheus.md" >}}"> <button type="button"
class="btn  btn-success btn-lg">Next >></button>
