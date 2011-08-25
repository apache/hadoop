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

package org.apache.hadoop.yarn.api.records.impl.pb;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringLocalResourceMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringURLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;


    
public class ApplicationSubmissionContextPBImpl extends ProtoBase<ApplicationSubmissionContextProto> implements ApplicationSubmissionContext {
  ApplicationSubmissionContextProto proto = ApplicationSubmissionContextProto.getDefaultInstance();
  ApplicationSubmissionContextProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  private Resource masterCapability = null;
  private Map<String, URL> resources = null;
  private Map<String, LocalResource> resourcesTodo = null;
  private List<String> fsTokenList = null;
  private ByteBuffer fsTokenTodo = null;
  private Map<String, String> environment = null;
  private List<String> commandList = null;
  private Priority priority = null;
  
  
  
  public ApplicationSubmissionContextPBImpl() {
    builder = ApplicationSubmissionContextProto.newBuilder();
  }

  public ApplicationSubmissionContextPBImpl(ApplicationSubmissionContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationSubmissionContextProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.masterCapability != null) {
      builder.setMasterCapability(convertToProtoFormat(this.masterCapability));
    }
    if (this.resources != null) {
      addResourcesToProto();
    }
    if (this.resourcesTodo != null) {
      addResourcesTodoToProto();
    }
    if (this.fsTokenList != null) {
      addFsTokenListToProto();
    }
    if (this.fsTokenTodo != null) {
      builder.setFsTokensTodo(convertToProtoFormat(this.fsTokenTodo));
    }
    if (this.environment != null) {
      addEnvironmentToProto();
    }
    if (this.commandList != null) {
      addCommandsToProto();
    }
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationSubmissionContextProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Priority getPriority() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearPriority();
    this.priority = priority;
  }
  @Override
  public ApplicationId getApplicationId() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasApplicationId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }
  @Override
  public String getApplicationName() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationName()) {
      return null;
    }
    return (p.getApplicationName());
  }

  @Override
  public void setApplicationName(String applicationName) {
    maybeInitBuilder();
    if (applicationName == null) {
      builder.clearApplicationName();
      return;
    }
    builder.setApplicationName((applicationName));
  }
  @Override
  public Resource getMasterCapability() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.masterCapability != null) {
      return masterCapability;
    } // Else via proto
    if (!p.hasMasterCapability()) {
      return null;
    }
    masterCapability = convertFromProtoFormat(p.getMasterCapability());
    return this.masterCapability;
  }

  @Override
  public void setMasterCapability(Resource masterCapability) {
    maybeInitBuilder();
    if (masterCapability == null)
      builder.clearMasterCapability();
    this.masterCapability = masterCapability;
  }
  @Override
  public Map<String, URL> getAllResources() {
    initResources();
    return this.resources;
  }
  @Override
  public URL getResource(String key) {
    initResources();
    return this.resources.get(key);
  }
  
  private void initResources() {
    if (this.resources != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringURLMapProto> mapAsList = p.getResourcesList();
    this.resources = new HashMap<String, URL>();
    
    for (StringURLMapProto c : mapAsList) {
      this.resources.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllResources(final Map<String, URL> resources) {
    if (resources == null)
      return;
    initResources();
    this.resources.putAll(resources);
  }
  
  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResources();
    if (this.resources == null)
      return;
    Iterable<StringURLMapProto> iterable = new Iterable<StringURLMapProto>() {
      
      @Override
      public Iterator<StringURLMapProto> iterator() {
        return new Iterator<StringURLMapProto>() {
          
          Iterator<String> keyIter = resources.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringURLMapProto next() {
            String key = keyIter.next();
            return StringURLMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(resources.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllResources(iterable);
  }
  @Override
  public void setResource(String key, URL val) {
    initResources();
    this.resources.put(key, val);
  }
  @Override
  public void removeResource(String key) {
    initResources();
    this.resources.remove(key);
  }
  @Override
  public void clearResources() {
    initResources();
    this.resources.clear();
  }
  @Override
  public Map<String, LocalResource> getAllResourcesTodo() {
    initResourcesTodo();
    return this.resourcesTodo;
  }
  @Override
  public LocalResource getResourceTodo(String key) {
    initResourcesTodo();
    return this.resourcesTodo.get(key);
  }
  
  private void initResourcesTodo() {
    if (this.resourcesTodo != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringLocalResourceMapProto> mapAsList = p.getResourcesTodoList();
    this.resourcesTodo = new HashMap<String, LocalResource>();
    
    for (StringLocalResourceMapProto c : mapAsList) {
      this.resourcesTodo.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllResourcesTodo(final Map<String, LocalResource> resourcesTodo) {
    if (resourcesTodo == null) 
      return;
    initResourcesTodo();
    this.resourcesTodo.putAll(resourcesTodo);
  }
  
  private void addResourcesTodoToProto() {
    maybeInitBuilder();
    builder.clearResourcesTodo();
    if (resourcesTodo == null)
      return;
    Iterable<StringLocalResourceMapProto> iterable = new Iterable<StringLocalResourceMapProto>() {
      
      @Override
      public Iterator<StringLocalResourceMapProto> iterator() {
        return new Iterator<StringLocalResourceMapProto>() {
          
          Iterator<String> keyIter = resourcesTodo.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringLocalResourceMapProto next() {
            String key = keyIter.next();
            return StringLocalResourceMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(resourcesTodo.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllResourcesTodo(iterable);
  }
  @Override
  public void setResourceTodo(String key, LocalResource val) {
    initResourcesTodo();
    this.resourcesTodo.put(key, val);
  }
  @Override
  public void removeResourceTodo(String key) {
    initResourcesTodo();
    this.resourcesTodo.remove(key);
  }
  @Override
  public void clearResourcesTodo() {
    initResourcesTodo();
    this.resourcesTodo.clear();
  }
  @Override
  public List<String> getFsTokenList() {
    initFsTokenList();
    return this.fsTokenList;
  }
  @Override
  public String getFsToken(int index) {
    initFsTokenList();
    return this.fsTokenList.get(index);
  }
  @Override
  public int getFsTokenCount() {
    initFsTokenList();
    return this.fsTokenList.size();
  }
  
  private void initFsTokenList() {
    if (this.fsTokenList != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getFsTokensList();
    this.fsTokenList = new ArrayList<String>();

    for (String c : list) {
      this.fsTokenList.add(c);
    }
  }
  
  @Override
  public void addAllFsTokens(final List<String> fsTokens) {
    if (fsTokens == null) 
      return;
    initFsTokenList();
    this.fsTokenList.addAll(fsTokens);
  }
  
  private void addFsTokenListToProto() {
    maybeInitBuilder();
    builder.clearFsTokens();
    builder.addAllFsTokens(this.fsTokenList);
  }

  @Override
  public void addFsToken(String fsTokens) {
    initFsTokenList();
    this.fsTokenList.add(fsTokens);
  }
  @Override
  public void removeFsToken(int index) {
    initFsTokenList();
    this.fsTokenList.remove(index);
  }
  @Override
  public void clearFsTokens() {
    initFsTokenList();
    this.fsTokenList.clear();
  }
  @Override
  public ByteBuffer getFsTokensTodo() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.fsTokenTodo != null) {
      return this.fsTokenTodo;
    }
    if (!p.hasFsTokensTodo()) {
      return null;
    }
    this.fsTokenTodo = convertFromProtoFormat(p.getFsTokensTodo());
    return this.fsTokenTodo;
  }

  @Override
  public void setFsTokensTodo(ByteBuffer fsTokensTodo) {
    maybeInitBuilder();
    if (fsTokensTodo == null) 
      builder.clearFsTokensTodo();
    this.fsTokenTodo = fsTokensTodo;
  }
  @Override
  public Map<String, String> getAllEnvironment() {
    initEnvironment();
    return this.environment;
  }
  @Override
  public String getEnvironment(String key) {
    initEnvironment();
    return this.environment.get(key);
  }
  
  private void initEnvironment() {
    if (this.environment != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringStringMapProto> mapAsList = p.getEnvironmentList();
    this.environment = new HashMap<String, String>();
    
    for (StringStringMapProto c : mapAsList) {
      this.environment.put(c.getKey(), c.getValue());
    }
  }
  
  @Override
  public void addAllEnvironment(Map<String, String> environment) {
    if (environment == null)
      return;
    initEnvironment();
    this.environment.putAll(environment);
  }
  
  private void addEnvironmentToProto() {
    maybeInitBuilder();
    builder.clearEnvironment();
    if (environment == null)
      return;
    Iterable<StringStringMapProto> iterable = new Iterable<StringStringMapProto>() {
      
      @Override
      public Iterator<StringStringMapProto> iterator() {
        return new Iterator<StringStringMapProto>() {
          
          Iterator<String> keyIter = environment.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringStringMapProto next() {
            String key = keyIter.next();
            return StringStringMapProto.newBuilder().setKey(key).setValue((environment.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllEnvironment(iterable);
  }
  @Override
  public void setEnvironment(String key, String val) {
    initEnvironment();
    this.environment.put(key, val);
  }
  @Override
  public void removeEnvironment(String key) {
    initEnvironment();
    this.environment.remove(key);
  }
  @Override
  public void clearEnvironment() {
    initEnvironment();
    this.environment.clear();
  }
  @Override
  public List<String> getCommandList() {
    initCommandList();
    return this.commandList;
  }
  @Override
  public String getCommand(int index) {
    initCommandList();
    return this.commandList.get(index);
  }
  @Override
  public int getCommandCount() {
    initCommandList();
    return this.commandList.size();
  }
  
  private void initCommandList() {
    if (this.commandList != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getCommandList();
    this.commandList = new ArrayList<String>();

    for (String c : list) {
      this.commandList.add(c);
    }
  }
  
  @Override
  public void addAllCommands(final List<String> command) {
    if (command == null)
      return;
    initCommandList();
    this.commandList.addAll(command);
  }
  
  private void addCommandsToProto() {
    maybeInitBuilder();
    builder.clearCommand();
    if (this.commandList == null) 
      return;
    builder.addAllCommand(this.commandList);
  }
  @Override
  public void addCommand(String command) {
    initCommandList();
    this.commandList.add(command);
  }
  @Override
  public void removeCommand(int index) {
    initCommandList();
    this.commandList.remove(index);
  }
  @Override
  public void clearCommands() {
    initCommandList();
    this.commandList.clear();
  }
  @Override
  public String getQueue() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return (p.getQueue());
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue((queue));
  }
  @Override
  public String getUser() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return (p.getUser());
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser((user));
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }

  private URLPBImpl convertFromProtoFormat(URLProto p) {
    return new URLPBImpl(p);
  }

  private URLProto convertToProtoFormat(URL t) {
    return ((URLPBImpl)t).getProto();
  }

  private LocalResourcePBImpl convertFromProtoFormat(LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private LocalResourceProto convertToProtoFormat(LocalResource t) {
    return ((LocalResourcePBImpl)t).getProto();
  }

}  
