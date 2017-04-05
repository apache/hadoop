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

Hadoop HDFS over HTTP - Documentation Sets
==========================================

HttpFS is a server that provides a REST HTTP gateway supporting all HDFS File System operations (read and write). And it is interoperable with the **webhdfs** REST HTTP API.

HttpFS can be used to transfer data between clusters running different versions of Hadoop (overcoming RPC versioning issues), for example using Hadoop DistCP.

HttpFS can be used to access data in HDFS on a cluster behind of a firewall (the HttpFS server acts as a gateway and is the only system that is allowed to cross the firewall into the cluster).

HttpFS can be used to access data in HDFS using HTTP utilities (such as curl and wget) and HTTP libraries Perl from other languages than Java.

The **webhdfs** client FileSystem implementation can be used to access HttpFS using the Hadoop filesystem command (`hadoop fs`) line tool as well as from Java applications using the Hadoop FileSystem Java API.

HttpFS has built-in security supporting Hadoop pseudo authentication and HTTP SPNEGO Kerberos and other pluggable authentication mechanisms. It also provides Hadoop proxy user support.

How Does HttpFS Works?
----------------------

HttpFS is a separate service from Hadoop NameNode.

HttpFS itself is Java Jetty web-application.

HttpFS HTTP web-service API calls are HTTP REST calls that map to a HDFS file system operation. For example, using the `curl` Unix command:

* `$ curl 'http://httpfs-host:14000/webhdfs/v1/user/foo/README.txt?op=OPEN&user.name=foo'` returns the contents of the HDFS `/user/foo/README.txt` file.

* `$ curl 'http://httpfs-host:14000/webhdfs/v1/user/foo?op=LISTSTATUS&user.name=foo'` returns the contents of the HDFS `/user/foo` directory in JSON format.

* `$ curl 'http://httpfs-host:14000/webhdfs/v1/user/foo?op=GETTRASHROOT&user.name=foo'` returns the path `/user/foo/.Trash`, if `/` is an encrypted zone, returns the path `/.Trash/foo`. See [more details](../hadoop-project-dist/hadoop-hdfs/TransparentEncryption.html#Rename_and_Trash_considerations) about trash path in an encrypted zone.

* `$ curl -X POST 'http://httpfs-host:14000/webhdfs/v1/user/foo/bar?op=MKDIRS&user.name=foo'` creates the HDFS `/user/foo/bar` directory.

User and Developer Documentation
--------------------------------

* [HttpFS Server Setup](./ServerSetup.html)

* [Using HTTP Tools](./UsingHttpTools.html)


