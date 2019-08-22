---
title: "Securing Ozone"
date: "2019-April-03"
summary: Overview of Ozone security concepts and steps to secure Ozone Manager and SCM.
weight: 1
icon: tower
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


# Kerberos

Ozone depends on [Kerberos](https://web.mit.edu/kerberos/) to make the
clusters secure. Historically, HDFS has supported running in an isolated
secure networks where it is possible to deploy without securing the cluster.

This release of Ozone follows that model, but soon will move to _secure by
default._  Today to enable security in ozone cluster, we need to set the
configuration **ozone.security.enabled** to true.

Property|Value
----------------------|---------
ozone.security.enabled| **true**

# Tokens #

Ozone uses a notion of tokens to avoid overburdening the Kerberos server.
When you serve thousands of requests per second, involving Kerberos might not
work well. Hence once an authentication is done, Ozone issues delegation
tokens and block tokens to the clients. These tokens allow applications to do
specified operations against the cluster, as if they have kerberos tickets
with them. Ozone supports following kinds of tokens.

### Delegation Token ###
Delegation tokens allow an application to impersonate a users kerberos
credentials. This token is based on verification of kerberos identity and is
issued by the Ozone Manager. Delegation tokens are enabled by default when
security is enabled.

### Block Token ###

Block tokens allow a client to read or write a block. This is needed so that
data nodes know that the user/client has permission to read or make
modifications to the block.

### S3Token ###

S3 uses a very different shared secret security scheme. Ozone supports the AWS Signature Version 4 protocol,
and from the end users perspective Ozone's s3 feels exactly like AWS S3.

The S3 credential tokens are called S3 tokens in the code. These tokens are
also enabled by default when security is enabled.


Each of the service daemons that make up Ozone needs a  Kerberos service
principal name and a corresponding [kerberos key tab]({{https://web.mit.edu/kerberos/krb5-latest/doc/basic/keytab_def.html}}) file.

All these settings should be made in ozone-site.xml.

<div class="card-group">
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">Storage Container Manager</h3>
      <p class="card-text">
       <br>
        SCM requires two Kerberos principals, and the corresponding key tab files
        for both of these principals.
        <br>
        <table class="table table-dark">
          <thead>
            <tr>
              <th scope="col">Property</th>
              <th scope="col">Description</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <th scope="row">hdds.scm.kerberos.principal</th>
              <td>The SCM service principal. e.g. scm/HOST@REALM.COM</td>
            </tr>
            <tr>
              <th scope="row">hdds.scm.kerberos.keytab.file</th>
              <td>The keytab file used by SCM daemon to login as its service principal.</td>
            </tr>
            <tr>
              <th scope="row">hdds.scm.http.kerberos.principal</th>
              <td>SCM http server service principal.</td>
            </tr>
            <tr>
              <th scope="row">hdds.scm.http.kerberos.keytab</th>
              <td>The keytab file used by SCM http server to login as its service principal.</td>
            </tr>
          </tbody>
        </table>
    </div>
  </div>
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">Ozone Manager</h3>
      <p class="card-text">
             <br>
              Like SCM, OM also requires two Kerberos principals, and the
              corresponding key tab files for both of these principals.
              <br>
       <table class="table table-dark">
                <thead>
                  <tr>
                    <th scope="col">Property</th>
                    <th scope="col">Description</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <th scope="row">ozone.om.kerberos.principal</th>
                    <td>The OzoneManager service principal. e.g. om/_HOST@REALM
                    .COM</td>
                  </tr>
                  <tr>
                    <th scope="row">ozone.om.kerberos.keytab.file</th>
                    <td>TThe keytab file used by SCM daemon to login as its service principal.</td>
                  </tr>
                  <tr>
                    <th scope="row">ozone.om.http.kerberos.principal</th>
                    <td>Ozone Manager http server service principal.</td>
                  </tr>
                  <tr>
                    <th scope="row">ozone.om.http.kerberos.keytab</th>
                    <td>The keytab file used by OM http server to login as its service principal.</td>
                  </tr>
                </tbody>
              </table>
      </div>
  </div>
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">S3 Gateway</h3>
      <p class="card-text">
        <br>
        S3 gateway requires one service principal and here the configuration values
         needed in the ozone-site.xml.
        <br>
      <table class="table table-dark">
                      <thead>
                        <tr>
                          <th scope="col">Property</th>
                          <th scope="col">Description</th>
                        </tr>
                      </thead>
                                        <tr>
                                          <th scope="row">ozone.s3g.keytab.file</th>
                                          <td>The keytab file used by S3 gateway</td>
                                        </tr>
                                        <tr>
                                          <th scope="row">ozone.s3g.authentication.kerberos
                                                                                    .principal</th>
                                          <td>S3 Gateway principal. e.g. HTTP/_HOST@EXAMPLE.COM</td>
                                        </tr>
                                      </tbody>
                    </table>
      </div>
  </div>
</div>
