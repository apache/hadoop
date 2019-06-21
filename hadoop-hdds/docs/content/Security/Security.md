---
title: Security
name: Security
identifier: SecureOzone
menu: main
weight: 5
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
        <li class="breadcrumb-item active" aria-current="page">Security
        </li>
    </ol>
</nav>
<div class="jumbotron jumbotron-fluid">
    <div class="container">
        <h3 class="display-4">Securing Ozone </h3>
        <p class="lead">
          Ozone is an enterprise class, secure storage system. There many
          optional security features in Ozone. Following pages discuss how
          you can leverage the security features of Ozone.
        </p>
    </div>
</div>

<!--- Empty Line breaks before we start the security cards --->
<br>
<br>


<!--- security cards --->

<div class="card">
<div class="card-body">
<h3 class="card-title">Securing Ozone</h3>
<p class="card-text"> Depending on your needs, there are multiple optional
    steps in securing ozone.
</p>
<div class="row">
    <div class="col-sm-6">
        <div class="card" >
            <img class="card-img-top" src="" alt="">
            <span style="font-size: 50px" class="glyphicon glyphicon-tower"
                aria-hidden="true"></span>
            <div class="card-body">
                <h4 class="card-title"> 1. Security Overview</h4>
                <p class="card-text">
                   Overview of Ozone security concepts and steps to secure
                   Ozone Manager and SCM.
                </p>
                <a href="{{< ref "Security/SecureOzone.md" >}}"
                class="btn btn-primary">Securing Ozone</a>
            </div>
        </div>
    </div>
    <div class="card" >
        <div class="col-sm-6">
            <img class="card-img-top" src="" alt="">
            <span style="font-size: 50px" class="glyphicon glyphicon-th"
                aria-hidden="true"></span>
            <div class="card-body">
                <h4 class="card-title">2. Securing Data nodes</h4>
                <p class="card-text"> Explains different modes of securing data nodes.
                    These range from kerberos to auto approval.
                </p>
                <a href="{{< ref "Security/SecuringDatanodes.md" >}}"
                class="btn btn-primary">Data node Security</a>
            </div>
        </div>
    </div>
    <div class="card" >
        <div class="col-sm-6">
            <img class="card-img-top" src="" alt="">
            <span style="font-size: 50px" class="glyphicon glyphicon-lock"
                aria-hidden="true"></span>
            <div class="card-body">
                <h4 class="card-title">3. Transparent Data Encryption</h4>
                <p class="card-text">TDE allows  data on the disks to be
                    encrypted-at-rest and automatically decrypted during access. You can
                    enable this per key or per bucket.
                </p>
                <a href="{{< ref "Security/SecuringTDE.md" >}}"
                class="btn btn-primary">Disk Encyption</a>
            </div>
        </div>
    </div>
    <div class="card" >
        <div class="col-sm-6">
            <img class="card-img-top" src="" alt="">
            <span style="font-size: 50px" class="glyphicon glyphicon-cloud"
                aria-hidden="true"></span>
            <div class="card-body">
                <h4 class="card-title">4. Secure S3 Access</h4>
                <p class="card-text"> Ozone supports S3 protocol, and uses  AWS
                    Signature Version 4 protocol whick allows a seemless  S3 experience.
                </p>
                <a href="{{< ref "Security/SecuringS3.md" >}}"
                class="btn btn-primary">S3 Security</a>
            </div>
        </div>
    </div>
        <div class="card" >
            <div class="col-sm-6">
                <img class="card-img-top" src="" alt="">
                <span style="font-size: 50px" class="glyphicon glyphicon-user"
                    aria-hidden="true"></span>
                <div class="card-body">
                    <h4 class="card-title">5. Apache Ranger Support</h4>
                    <p class="card-text"> Apache Rangeris a framework to enable,
                    monitor and manage comprehensive data security across the Hadoop platform.
                    </p>
                    <a href="{{< ref "Security/SecuityWithRanger.md" >}}"
                    class="btn btn-primary">Enabling Ranger</a>
                </div>
            </div>
        </div>
            <div class="col-sm-6">
                        <img class="card-img-top" src="" alt="">
                        <span style="font-size: 50px" class="glyphicon glyphicon-check"
                            aria-hidden="true"></span>
                        <div class="card-body">
                            <h4 class="card-title">6. Ozone native ACLs</h4>
                            <p class="card-text"> Ozone supports S3 protocol, and uses  AWS
                                Signature Version 4 protocol whick allows a seemless  S3 experience.
                            </p>
                            <a href="{{< ref "Security/SecurityAcls.md" >}}"
                            class="btn btn-primary">Ozone ACLs</a>
                        </div>
                    </div>
                </div>
</div>
<br>

If you would like to understand Ozone's security architecture at a greater
depth, please take a look at [Ozone security architecture.]
(https://issues.apache.org/jira/secure/attachment/12911638/HadoopStorageLayerSecurity.pdf)
<!--- End of security cards --->

<a href="{{< ref "Security/SecureOzone.md" >}}"> <button type="button"
class="btn  btn-success btn-lg">Next >></button>
