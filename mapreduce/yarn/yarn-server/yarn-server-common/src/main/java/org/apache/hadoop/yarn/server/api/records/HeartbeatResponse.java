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
import org.apache.hadoop.yarn.api.records.Container;

public interface HeartbeatResponse {
  public abstract int getResponseId();
  public abstract boolean getReboot();
  
  public abstract List<Container> getContainersToCleanupList();
  public abstract Container getContainerToCleanup(int index);
  public abstract int getContainersToCleanupCount();
  
  public abstract List<ApplicationId> getApplicationsToCleanupList();
  public abstract ApplicationId getApplicationsToCleanup(int index);
  public abstract int getApplicationsToCleanupCount();
  
  public abstract void setResponseId(int responseId);
  public abstract void setReboot(boolean reboot);
  
  public abstract void addAllContainersToCleanup(List<Container> containers);
  public abstract void addContainerToCleanup(Container container);
  public abstract void removeContainerToCleanup(int index);
  public abstract void clearContainersToCleanup();
  
  public abstract void addAllApplicationsToCleanup(List<ApplicationId> applications);
  public abstract void addApplicationToCleanup(ApplicationId applicationId);
  public abstract void removeApplicationToCleanup(int index);
  public abstract void clearApplicationsToCleanup();
}
