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
package org.apache.hadoop.yarn.server.api.records;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public interface HeartbeatResponse {
  int getResponseId();
  boolean getReboot();
  
  List<ContainerId> getContainersToCleanupList();
  ContainerId getContainerToCleanup(int index);
  int getContainersToCleanupCount();
  
  List<ApplicationId> getApplicationsToCleanupList();
  ApplicationId getApplicationsToCleanup(int index);
  int getApplicationsToCleanupCount();
  
  void setResponseId(int responseId);
  void setReboot(boolean reboot);
  
  void addAllContainersToCleanup(List<ContainerId> containers);
  void addContainerToCleanup(ContainerId container);
  void removeContainerToCleanup(int index);
  void clearContainersToCleanup();
  
  void addAllApplicationsToCleanup(List<ApplicationId> applications);
  void addApplicationToCleanup(ApplicationId applicationId);
  void removeApplicationToCleanup(int index);
  void clearApplicationsToCleanup();
}
