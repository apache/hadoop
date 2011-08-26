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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringLocalResourceMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;


    
public class ContainerLaunchContextPBImpl extends ProtoBase<ContainerLaunchContextProto> implements ContainerLaunchContext {
  ContainerLaunchContextProto proto = ContainerLaunchContextProto.getDefaultInstance();
  ContainerLaunchContextProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  private Resource resource = null;
  private Map<String, LocalResource> localResources = null;
  private ByteBuffer containerTokens = null;
  private Map<String, ByteBuffer> serviceData = null;
  private Map<String, String> env = null;
  private List<String> commands = null;
  
  
  public ContainerLaunchContextPBImpl() {
    builder = ContainerLaunchContextProto.newBuilder();
  }

  public ContainerLaunchContextPBImpl(ContainerLaunchContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ContainerLaunchContextProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToBuilder() {
    if (this.containerId != null && !((ContainerIdPBImpl)containerId).getProto().equals(builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.resource != null && !((ResourcePBImpl)this.resource).getProto().equals(builder.getResource())) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.localResources != null) {
      addLocalResourcesToProto();
    }
    if (this.containerTokens != null) {
      builder.setContainerTokens(convertToProtoFormat(this.containerTokens));
    }
    if (this.serviceData != null) {
      addServiceDataToProto();
    }
    if (this.env != null) {
      addEnvToProto();
    }
    if (this.commands != null) {
      addCommandsToProto();
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
      builder = ContainerLaunchContextProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Resource getResource() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) 
      builder.clearResource();
    this.resource = resource;
  }
  @Override
  public List<String> getCommandList() {
    initCommands();
    return this.commands;
  }
  @Override
  public String getCommand(int index) {
    initCommands();
    return this.commands.get(index);
  }
  @Override
  public int getCommandCount() {
    initCommands();
    return this.commands.size();
  }
  
  private void initCommands() {
    if (this.commands != null) {
      return;
    }
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getCommandList();
    this.commands = new ArrayList<String>();

    for (String c : list) {
      this.commands.add(c);
    }
  }
  
  @Override
  public void addAllCommands(final List<String> command) {
    if (command == null)
      return;
    initCommands();
    this.commands.addAll(command);
  }
  
  private void addCommandsToProto() {
    maybeInitBuilder();
    builder.clearCommand();
    if (this.commands == null) 
      return;
    builder.addAllCommand(this.commands);
  }
  @Override
  public void addCommand(String command) {
    initCommands();
    this.commands.add(command);
  }
  @Override
  public void removeCommand(int index) {
    initCommands();
    this.commands.remove(index);
  }
  @Override
  public void clearCommands() {
    initCommands();
    this.commands.clear();
  }
  @Override
  public String getUser() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
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
  @Override
  public ContainerId getContainerId() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) 
      builder.clearContainerId();
    this.containerId = containerId;
  }
  @Override
  public Map<String, LocalResource> getAllLocalResources() {
    initLocalResources();
    return this.localResources;
  }
  @Override
  public LocalResource getLocalResource(String key) {
    initLocalResources();
    return this.localResources.get(key);
  }
  
  private void initLocalResources() {
    if (this.localResources != null) {
      return;
    }
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringLocalResourceMapProto> list = p.getLocalResourcesList();
    this.localResources = new HashMap<String, LocalResource>();

    for (StringLocalResourceMapProto c : list) {
      this.localResources.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllLocalResources(final Map<String, LocalResource> localResources) {
    if (localResources == null)
      return;
    initLocalResources();
    this.localResources.putAll(localResources);
  }
  
  private void addLocalResourcesToProto() {
    maybeInitBuilder();
    builder.clearLocalResources();
    if (localResources == null)
      return;
    Iterable<StringLocalResourceMapProto> iterable = new Iterable<StringLocalResourceMapProto>() {
      
      @Override
      public Iterator<StringLocalResourceMapProto> iterator() {
        return new Iterator<StringLocalResourceMapProto>() {
          
          Iterator<String> keyIter = localResources.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringLocalResourceMapProto next() {
            String key = keyIter.next();
            return StringLocalResourceMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(localResources.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllLocalResources(iterable);
  }
  @Override
  public void setLocalResource(String key, LocalResource val) {
    initLocalResources();
    this.localResources.put(key, val);
  }
  @Override
  public void removeLocalResource(String key) {
    initLocalResources();
    this.localResources.remove(key);
  }
  @Override
  public void clearLocalResources() {
    initLocalResources();
    this.localResources.clear();
  }
  @Override
  public ByteBuffer getContainerTokens() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerTokens != null) {
      return this.containerTokens;
    }
    if (!p.hasContainerTokens()) {
      return null;
    }
    this.containerTokens =  convertFromProtoFormat(p.getContainerTokens());
    return this.containerTokens;
  }

  @Override
  public void setContainerTokens(ByteBuffer containerTokens) {
    maybeInitBuilder();
    if (containerTokens == null)
      builder.clearContainerTokens();
    this.containerTokens = containerTokens;
  }
  @Override
  public Map<String, ByteBuffer> getAllServiceData() {
    initServiceData();
    return this.serviceData;
  }
  @Override
  public ByteBuffer getServiceData(String key) {
    initServiceData();
    return this.serviceData.get(key);
  }
  
  private void initServiceData() {
    if (this.serviceData != null) {
      return;
    }
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringBytesMapProto> list = p.getServiceDataList();
    this.serviceData = new HashMap<String, ByteBuffer>();

    for (StringBytesMapProto c : list) {
      this.serviceData.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllServiceData(final Map<String, ByteBuffer> serviceData) {
    if (serviceData == null)
      return;
    initServiceData();
    this.serviceData.putAll(serviceData);
  }
  
  private void addServiceDataToProto() {
    maybeInitBuilder();
    builder.clearServiceData();
    if (serviceData == null)
      return;
    Iterable<StringBytesMapProto> iterable = new Iterable<StringBytesMapProto>() {
      
      @Override
      public Iterator<StringBytesMapProto> iterator() {
        return new Iterator<StringBytesMapProto>() {
          
          Iterator<String> keyIter = serviceData.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringBytesMapProto next() {
            String key = keyIter.next();
            return StringBytesMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(serviceData.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllServiceData(iterable);
  }
  @Override
  public void setServiceData(String key, ByteBuffer val) {
    initServiceData();
    this.serviceData.put(key, val);
  }
  @Override
  public void removeServiceData(String key) {
    initServiceData();
    this.serviceData.remove(key);
  }
  @Override
  public void clearServiceData() {
    initServiceData();
    this.serviceData.clear();
  }
  @Override
  public Map<String, String> getAllEnv() {
    initEnv();
    return this.env;
  }
  @Override
  public String getEnv(String key) {
    initEnv();
    return this.env.get(key);
  }
  
  private void initEnv() {
    if (this.env != null) {
      return;
    }
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringStringMapProto> list = p.getEnvList();
    this.env = new HashMap<String, String>();

    for (StringStringMapProto c : list) {
      this.env.put(c.getKey(), c.getValue());
    }
  }
  
  @Override
  public void addAllEnv(final Map<String, String> env) {
    if (env == null)
      return;
    initEnv();
    this.env.putAll(env);
  }
  
  private void addEnvToProto() {
    maybeInitBuilder();
    builder.clearEnv();
    if (env == null)
      return;
    Iterable<StringStringMapProto> iterable = new Iterable<StringStringMapProto>() {
      
      @Override
      public Iterator<StringStringMapProto> iterator() {
        return new Iterator<StringStringMapProto>() {
          
          Iterator<String> keyIter = env.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringStringMapProto next() {
            String key = keyIter.next();
            return StringStringMapProto.newBuilder().setKey(key).setValue((env.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllEnv(iterable);
  }
  @Override
  public void setEnv(String key, String val) {
    initEnv();
    this.env.put(key, val);
  }
  @Override
  public void removeEnv(String key) {
    initEnv();
    this.env.remove(key);
  }
  @Override
  public void clearEnv() {
    initEnv();
    this.env.clear();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

  private LocalResourcePBImpl convertFromProtoFormat(LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private LocalResourceProto convertToProtoFormat(LocalResource t) {
    return ((LocalResourcePBImpl)t).getProto();
  }
}  
