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

# YARN Service Registry

The Service registry is a service which can be deployed in a Hadoop cluster
to allow deployed applications to register themselves and the means of
communicating with them. Client applications can then locate services
and use the binding information to connect with the services's network-accessible
endpoints, be they REST, IPC, Web UI, Zookeeper quorum+path or some other protocol.

* [Architecture](yarn-registry.html)
* [Configuration](registry-configuration.html)
* [Using the YARN Service registry](using-the-yarn-service-registry.html)
* [Security](registry-security.html)
