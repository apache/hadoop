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
package org.apache.hadoop.ozone.scm.pipelines;
/**
 Ozone supports the notion of different kind of pipelines.
 That means that we can have a replication pipeline build on
 Ratis, Standalone or some other protocol. All Pipeline managers
 the entities in charge of pipelines reside in the package.

 Here is the high level Arch.

 1. A pipeline selector class is instantiated in the Container manager class.

 2. A client when creating a container -- will specify what kind of
 replication type it wants to use. We support 2 types now, Ratis and StandAlone.

 3. Based on the replication type, the pipeline selector class asks the
 corresponding pipeline manager for a pipeline.

 4. We have supported the ability for clients to specify a set of nodes in
 the pipeline or rely in the pipeline manager to select the datanodes if they
 are not specified.
 */