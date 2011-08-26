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

public interface ApplicationSubmissionContext {
  public abstract ApplicationId getApplicationId();
  public abstract String getApplicationName();
  public abstract Resource getMasterCapability();
  
  public abstract Map<String, URL> getAllResources();
  public abstract URL getResource(String key);
  
  public abstract Map<String, LocalResource> getAllResourcesTodo();
  public abstract LocalResource getResourceTodo(String key);
  
  public abstract List<String> getFsTokenList();
  public abstract String getFsToken(int index);
  public abstract int getFsTokenCount();
  
  public abstract ByteBuffer getFsTokensTodo();
  
  public abstract Map<String, String> getAllEnvironment();
  public abstract String getEnvironment(String key);
  
  public abstract List<String> getCommandList();
  public abstract String getCommand(int index);
  public abstract int getCommandCount();
  
  public abstract String getQueue();
  public abstract Priority getPriority();
  public abstract String getUser();

  
  
  public abstract void setApplicationId(ApplicationId appplicationId);
  public abstract void setApplicationName(String applicationName);
  public abstract void setMasterCapability(Resource masterCapability);
  
  public abstract void addAllResources(Map<String, URL> resources);
  public abstract void setResource(String key, URL url);
  public abstract void removeResource(String key);
  public abstract void clearResources();
  
  public abstract void addAllResourcesTodo(Map<String, LocalResource> resourcesTodo);
  public abstract void setResourceTodo(String key, LocalResource localResource);
  public abstract void removeResourceTodo(String key);
  public abstract void clearResourcesTodo();
  
  public abstract void addAllFsTokens(List<String> fsTokens);
  public abstract void addFsToken(String fsToken);
  public abstract void removeFsToken(int index);
  public abstract void clearFsTokens();
  
  public abstract void setFsTokensTodo(ByteBuffer fsTokensTodo);
  
  public abstract void addAllEnvironment(Map<String, String> environment);
  public abstract void setEnvironment(String key, String env);
  public abstract void removeEnvironment(String key);
  public abstract void clearEnvironment();
  
  public abstract void addAllCommands(List<String> commands);
  public abstract void addCommand(String command);
  public abstract void removeCommand(int index);
  public abstract void clearCommands();
  
  public abstract void setQueue(String queue);
  public abstract void setPriority(Priority priority);
  public abstract void setUser(String user);
}