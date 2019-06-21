---
title: "Storage Container Manager"
date: "2017-09-14"

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
     <li class="breadcrumb-item"><a href="{{< ref "Concept.md" >}}">
        Concepts</a>
    <li class="breadcrumb-item active" aria-current="page">Storage Container Manager
    </li>
  </ol>
</nav>

Storage container manager provides multiple critical functions for the Ozone
cluster.  SCM acts as the cluster manager, Certificate authority, Block
manager and the replica manager.


   <div class="card" >
        <div class="col-sm-6">
            <img class="card-img-top" src="" alt="">
            <span style="font-size: 50px" class="
glyphicon glyphicon-tasks"
                aria-hidden="true"></span>
            <div class="card-body">
                <h4 class="card-title">1.Cluster Management</h4>
                <p class="card-text"> SCM is in charge of creating an Ozone
                cluster. When an SCM is booted up via <kbd>init</kbd>
                command, SCM creates the cluster identity and root
                certificates needed for the SCM certificate authority. SCM
                manages the life cycle of a data node in the cluster.
                </p>
            </div>
        </div>
    </div>
   <div class="card" >
        <div class="col-sm-6">
            <img class="card-img-top" src="" alt="">
            <span style="font-size: 50px" class="
glyphicon glyphicon-eye-open"
                aria-hidden="true"></span>
            <div class="card-body">
                <h4 class="card-title">2. Service Identity Management</h4>
                <p class="card-text"> SCM's Ceritificate authority is in
                charge of issuing identity certificates for each and every
                service in the cluster. This certificate infrastructre makes
                it easy to enable mTLS at network layer and also the block
                token infrastructure depends on this certificate infrastructure.
                </p>
            </div>
        </div>
    </div>

   <div class="card" >
           <div class="col-sm-6">
               <img class="card-img-top" src="" alt="">
               <span style="font-size: 50px" class="glyphicon glyphicon-th"
                   aria-hidden="true"></span>
               <div class="card-body">
                   <h4 class="card-title">3. Block Management</h4>
                   <p class="card-text"> SCM is the block manager. SCM
                   allocates blocks and assigns them to data nodes. Clients
                   read and write these blocks directly.
                   </p>
               </div>
           </div>
       </div>
     <div class="card" >
             <div class="col-sm-6">
                 <img class="card-img-top" src="" alt="">
                 <span style="font-size: 50px" class="glyphicon glyphicon-link"
                     aria-hidden="true"></span>
                 <div class="card-body">
                     <h4 class="card-title">4. Replica Management</h4>
                     <p class="card-text"> SCM keeps track of all the block
                     replicas. If there is a loss of data node or a disk, SCM
                      detects it and instructs data nodes make copies of the
                      missing blocks to ensure high avialablity.
                     </p>
                 </div>
             </div>
         </div>


 <a href="{{< ref "concept/datanodes.md" >}}"> <button type="button"
 class="btn  btn-success btn-lg">Next >></button>