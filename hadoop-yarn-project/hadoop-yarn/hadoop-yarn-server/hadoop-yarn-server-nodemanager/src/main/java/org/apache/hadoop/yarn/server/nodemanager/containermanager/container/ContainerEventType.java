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

public enum ContainerEventType {

  // Producer: ContainerManager
  INIT_CONTAINER,
  KILL_CONTAINER,
  UPDATE_DIAGNOSTICS_MSG,
  CONTAINER_DONE,
  REINITIALIZE_CONTAINER,
  ROLLBACK_REINIT,
  PAUSE_CONTAINER,
  RESUME_CONTAINER,
  UPDATE_CONTAINER_TOKEN,

  // DownloadManager
  CONTAINER_INITED,
  RESOURCE_LOCALIZED,
  RESOURCE_FAILED,
  CONTAINER_RESOURCES_CLEANEDUP,

  // Producer: ContainersLauncher
  CONTAINER_LAUNCHED,
  CONTAINER_EXITED_WITH_SUCCESS,
  CONTAINER_EXITED_WITH_FAILURE,
  CONTAINER_KILLED_ON_REQUEST,
  CONTAINER_PAUSED,
  CONTAINER_RESUMED,

  // Producer: ContainerScheduler
  CONTAINER_TOKEN_UPDATED,

  RECOVER_PAUSED_CONTAINER
}
