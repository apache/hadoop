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
package org.apache.hadoop.ozone;

/**
 This package contains class that support ozone implementation on the datanode
 side.

 Main parts of ozone on datanode are:

 1. REST Interface - This code lives under the web directory and listens to the
 WebHDFS port.

 2. Datanode container classes: This support persistence of ozone objects on
 datanode. These classes live under container directory.

 3. Client and Shell: We also support a ozone REST client lib, they are under
 web/client and web/ozShell.

 */
