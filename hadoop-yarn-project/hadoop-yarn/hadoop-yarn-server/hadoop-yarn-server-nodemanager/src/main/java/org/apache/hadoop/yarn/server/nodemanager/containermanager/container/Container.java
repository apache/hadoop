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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;

import java.util.List;
import java.util.Map;

public interface Container extends EventHandler<ContainerEvent> {

  ContainerId getContainerId();

  long getContainerStartTime();

  Resource getResource();

  ContainerTokenIdentifier getContainerTokenIdentifier();

  void setContainerTokenIdentifier(ContainerTokenIdentifier token);

  String getUser();
  
  ContainerState getContainerState();

  ContainerLaunchContext getLaunchContext();

  Credentials getCredentials();

  Map<Path,List<String>> getLocalizedResources();

  ContainerStatus cloneAndGetContainerStatus();

  NMContainerStatus getNMContainerStatus();

  boolean isRetryContextSet();

  boolean shouldRetry(int errorCode);

  String getWorkDir();

  void setWorkDir(String workDir);

  String getLogDir();

  void setLogDir(String logDir);

  void setIpAndHost(String[] ipAndHost);

  String toString();

  Priority getPriority();

  ResourceSet getResourceSet();

  boolean isRunning();

  void setIsReInitializing(boolean isReInitializing);

  boolean isReInitializing();

  boolean isMarkedForKilling();

  boolean canRollback();

  void commitUpgrade();

  void sendLaunchEvent();

  void sendKillEvent(int exitStatus, String description);

  boolean isRecovering();
}
