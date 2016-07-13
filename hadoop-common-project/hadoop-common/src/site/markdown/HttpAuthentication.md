<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Authentication for Hadoop HTTP web-consoles
===========================================

* [Authentication for Hadoop HTTP web-consoles](#Authentication_for_Hadoop_HTTP_web-consoles)
    * [Introduction](#Introduction)
    * [Configuration](#Configuration)

Introduction
------------

This document describes how to configure Hadoop HTTP web-consoles to require user authentication.

By default Hadoop HTTP web-consoles (JobTracker, NameNode, TaskTrackers and DataNodes) allow access without any form of authentication.

Hadoop HTTP web-consoles can be configured to require Kerberos authentication using HTTP SPNEGO protocol (supported by browsers like Firefox and Internet Explorer).

In addition, Hadoop HTTP web-consoles support the equivalent of Hadoop's Pseudo/Simple authentication. If this option is enabled, the user name must be specified in the first browser interaction using the user.name query string parameter. e.g. `http://localhost:50030/jobtracker.jsp?user.name=babu`.

If a custom authentication mechanism is required for the HTTP web-consoles, it is possible to implement a plugin to support the alternate authentication mechanism (refer to Hadoop hadoop-auth for details on writing an `AuthenticatorHandler`).

The next section describes how to configure Hadoop HTTP web-consoles to require user authentication.

Configuration
-------------

The following properties should be in the `core-site.xml` of all the nodes in the cluster.

| Property Name                                          | Default Value                                  | Description                                                                                                                                                                                                                                                                                                                                                   |
|:------------------------------------------------------ |:---------------------------------------------- |:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hadoop.http.filter.initializers`                      |                                                | Add to this property the `org.apache.hadoop.security.AuthenticationFilterInitializer` initializer class.                                                                                                                                                                                                                                                      |
| `hadoop.http.authentication.type`                      | `simple`                                       | Defines authentication used for the HTTP web-consoles. The supported values are: `simple` \| `kerberos` \| `#AUTHENTICATION_HANDLER_CLASSNAME#`.                                                                                                                                                                                                              |
| `hadoop.http.authentication.token.validity`            | `36000`                                        | Indicates how long (in seconds) an authentication token is valid before it has to be renewed.                                                                                                                                                                                                                                                                 |
| `hadoop.http.authentication.signature.secret.file`     | `$user.home/hadoop-http-auth-signature-secret` | The signature secret file for signing the authentication tokens. The same secret should be used for all nodes in the cluster, JobTracker, NameNode, DataNode and TastTracker. This file should be readable only by the Unix user running the daemons.                                                                                                         |
| `hadoop.http.authentication.cookie.domain`             |                                                | The domain to use for the HTTP cookie that stores the authentication token. For authentication to work correctly across all nodes in the cluster the domain must be correctly set. There is no default value, the HTTP cookie will not have a domain working only with the hostname issuing the HTTP cookie.                                                  |
| `hadoop.http.authentication.simple.anonymous.allowed`  | `true`                                         | Indicates whether anonymous requests are allowed when using 'simple' authentication.                                                                                                                                                                                                                                                                          |
| `hadoop.http.authentication.kerberos.principal`        | `HTTP/_HOST@$LOCALHOST`                        | Indicates the Kerberos principal to be used for HTTP endpoint when using 'kerberos' authentication. The principal short name must be `HTTP` per Kerberos HTTP SPNEGO specification. `_HOST` -if present- is replaced with bind address of the HTTP server.                                                                                                    |
| `hadoop.http.authentication.kerberos.keytab`           | `$user.home/hadoop.keytab`                     | Location of the keytab file with the credentials for the Kerberos principal used for the HTTP endpoint.                                                                                                                                                                                                                                                       |

CORS
----
To enable cross-origin support (CORS), please set the following configuration parameters:

Add org.apache.hadoop.security.HttpCrossOriginFilterInitializer to hadoop.http.filter.initializers in core-site.xml. You will also need to set the following properties in core-site.xml -


| Property                                 | Default Value                                 | Description                                                                            |
|:---------------------------------------- |:--------------------------------------------- |:-------------------------------------------------------------------------------------  |
| hadoop.http.cross-origin.enabled         | `false`                                       | Enables cross origin support for all web-services                                      |
| hadoop.http.cross-origin.allowed-origins | `*`                                           | Comma separated list of origins that are allowed, wildcards (`*`) and patterns allowed |
| hadoop.http.cross-origin.allowed-methods | `GET,POST,HEAD`                               | Comma separated list of methods that are allowed                                       |
| hadoop.http.cross-origin.allowed-headers | `X-Requested-With,Content-Type,Accept,Origin` | Comma separated list of headers that are allowed                                       |
| hadoop.http.cross-origin.max-age         | `1800`                                        | Number of seconds a pre-flighted request can be cached                                 |
