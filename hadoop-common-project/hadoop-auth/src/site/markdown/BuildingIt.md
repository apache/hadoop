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

Hadoop Auth, Java HTTP SPNEGO - Building It
===========================================

Requirements
------------

* Java 7+
* Maven 3+
* Kerberos KDC (for running Kerberos test cases)

Building
--------

Use Maven goals: clean, test, compile, package, install

Available profiles: docs, testKerberos

Testing
-------

By default Kerberos testcases are not run.

The requirements to run Kerberos testcases are a running KDC, a keytab file with a client principal and a kerberos principal.

To run Kerberos tescases use the `testKerberos` Maven profile:

    $ mvn test -PtestKerberos

The following Maven `-D` options can be used to change the default values:

* `hadoop-auth.test.kerberos.realm`: default value **LOCALHOST**
* `hadoop-auth.test.kerberos.client.principal`: default value **client**
* `hadoop-auth.test.kerberos.server.principal`: default value **HTTP/localhost** (it must start 'HTTP/')
* `hadoop-auth.test.kerberos.keytab.file`: default value **$HOME/$USER.keytab**

### Generating Documentation

To create the documentation use the `docs` Maven profile:

    $ mvn package -Pdocs

The generated documentation is available at `hadoop-auth/target/site/`.
