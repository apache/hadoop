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

package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface ContainerLaunchContext {
  ContainerId getContainerId();
  String getUser();
  Resource getResource();
  
  Map<String, LocalResource> getAllLocalResources();
  LocalResource getLocalResource(String key);
  
  
  ByteBuffer getContainerTokens();
  
  Map<String, ByteBuffer> getAllServiceData();
  ByteBuffer getServiceData(String key);
  
  Map<String, String> getAllEnv();
  String getEnv(String key);
  
  List<String> getCommandList();
  String getCommand(int index);
  int getCommandCount();
  
  void setContainerId(ContainerId containerId);
  void setUser(String user);
  void setResource(Resource resource);
  
  void addAllLocalResources(Map<String, LocalResource> localResources);
  void setLocalResource(String key, LocalResource value);
  void removeLocalResource(String key);
  void clearLocalResources();
  
  void setContainerTokens(ByteBuffer containerToken);
  
  void addAllServiceData(Map<String, ByteBuffer> serviceData);
  void setServiceData(String key, ByteBuffer value);
  void removeServiceData(String key);
  void clearServiceData();
  
  void addAllEnv(Map<String, String> env);
  void setEnv(String key, String value);
  void removeEnv(String key);
  void clearEnv();
  
  void addAllCommands(List<String> commands);
  void addCommand(String command);
  void removeCommand(int index);
  void clearCommands();
}
