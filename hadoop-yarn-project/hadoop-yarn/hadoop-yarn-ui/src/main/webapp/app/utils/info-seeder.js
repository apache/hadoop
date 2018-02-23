/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default {
  serviceName: "A unique application name",
  queueName: "The YARN queue that this application should be submitted to",
  lifetime: "Life time (in seconds) of the application from the time it reaches the STARTED state (after which it is automatically destroyed by YARN). For unlimited lifetime do not set a lifetime value.",
  components: "One or more components of the application. If the application is HBase say, then the component can be a simple role like master or regionserver. If the application is a complex business webapp then a component can be other applications say Kafka or Storm. Thereby it opens up the support for complex and nested applications.",
  configurations: "Set of configuration properties that can be injected into the application components via envs, files and custom pluggable helper docker containers. Files of several standard formats like xml, properties, json, yaml and templates will be supported.",
  fileConfigs: "Set of file configurations that needs to be created and made available as a volume in an application component container.",
  userName: "Name of the user who launches the service."
};
