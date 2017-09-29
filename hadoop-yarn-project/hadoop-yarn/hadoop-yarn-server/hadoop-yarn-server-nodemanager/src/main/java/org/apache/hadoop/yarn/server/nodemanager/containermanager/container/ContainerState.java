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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

/**
 * States used by the container state machine.
 */
public enum ContainerState {
  // NOTE: In case of future additions / deletions / modifications to this
  //       enum, please ensure that the following are also correspondingly
  //       updated:
  //       1. ContainerImpl::getContainerSubState().
  //       2. the doc in the ContainerSubState class.
  //       3. the doc in the yarn_protos.proto file.
  NEW, LOCALIZING, LOCALIZATION_FAILED, SCHEDULED, RUNNING, RELAUNCHING,
  REINITIALIZING, REINITIALIZING_AWAITING_KILL,
  EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE, KILLING,
  CONTAINER_CLEANEDUP_AFTER_KILL, CONTAINER_RESOURCES_CLEANINGUP, DONE,
  PAUSING, PAUSED, RESUMING
}
