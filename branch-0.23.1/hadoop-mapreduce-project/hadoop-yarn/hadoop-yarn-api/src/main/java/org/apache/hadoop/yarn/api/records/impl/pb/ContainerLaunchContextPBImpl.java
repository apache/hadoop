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

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringLocalResourceMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ContainerLaunchContextPBImpl 
extends ProtoBase<ContainerLaunchContextProto> 
implements ContainerLaunchContext {
  ContainerLaunchContextProto proto = 
      ContainerLaunchContextProto.getDefaultInstance();
  ContainerLaunchContextProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  private Resource resource = null;
  private Map<String, LocalResource> localResources = null;
  private ByteBuffer containerTokens = null;
  private Map<String, ByteBuffer> serviceData = null;
  private Map<String, String> environment = null;
  private List<String> commands = null;
  private Map<ApplicationAccessType, String> applicationACLS = null;
  
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
    if (this.containerId != null && 
        !((ContainerIdPBImpl)containerId).getProto().equals(
            builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.resource != null && 
        !((ResourcePBImpl)this.resource).getProto().equals(
            builder.getResource())) {
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
    if (this.environment != null) {
      addEnvToProto();
    }
    if (this.commands != null) {
      addCommandsToProto();
    }
    if (this.applicationACLS != null) {
      addApplicationACLs();
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
  public List<String> getCommands() {
    initCommands();
    return this.commands;
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
  public void setCommands(final List<String> commands) {
    if (commands == null)
      return;
    initCommands();
    this.commands.clear();
    this.commands.addAll(commands);
  }
  
  private void addCommandsToProto() {
    maybeInitBuilder();
    builder.clearCommand();
    if (this.commands == null) 
      return;
    builder.addAllCommand(this.commands);
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
  public Map<String, LocalResource> getLocalResources() {
    initLocalResources();
    return this.localResources;
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
  public void setLocalResources(
      final Map<String, LocalResource> localResources) {
    if (localResources == null)
      return;
    initLocalResources();
    this.localResources.clear();
    this.localResources.putAll(localResources);
  }
  
  private void addLocalResourcesToProto() {
    maybeInitBuilder();
    builder.clearLocalResources();
    if (localResources == null)
      return;
    Iterable<StringLocalResourceMapProto> iterable = 
        new Iterable<StringLocalResourceMapProto>() {
      
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
            return StringLocalResourceMapProto.newBuilder().setKey(key).
                setValue(convertToProtoFormat(localResources.get(key))).build();
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
  public Map<String, ByteBuffer> getServiceData() {
    initServiceData();
    return this.serviceData;
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
  public void setServiceData(final Map<String, ByteBuffer> serviceData) {
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
    Iterable<StringBytesMapProto> iterable = 
        new Iterable<StringBytesMapProto>() {
      
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
            return StringBytesMapProto.newBuilder().setKey(key).setValue(
                convertToProtoFormat(serviceData.get(key))).build();
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
  public Map<String, String> getEnvironment() {
    initEnv();
    return this.environment;
  }
  
  private void initEnv() {
    if (this.environment != null) {
      return;
    }
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringStringMapProto> list = p.getEnvironmentList();
    this.environment = new HashMap<String, String>();

    for (StringStringMapProto c : list) {
      this.environment.put(c.getKey(), c.getValue());
    }
  }
  
  @Override
  public void setEnvironment(final Map<String, String> env) {
    if (env == null)
      return;
    initEnv();
    this.environment.clear();
    this.environment.putAll(env);
  }
  
  private void addEnvToProto() {
    maybeInitBuilder();
    builder.clearEnvironment();
    if (environment == null)
      return;
    Iterable<StringStringMapProto> iterable = 
        new Iterable<StringStringMapProto>() {
      
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
            return StringStringMapProto.newBuilder().setKey(key).setValue(
                (environment.get(key))).build();
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
  public Map<ApplicationAccessType, String> getApplicationACLs() {
    initApplicationACLs();
    return this.applicationACLS;
  }

  private void initApplicationACLs() {
    if (this.applicationACLS != null) {
      return;
    }
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationACLMapProto> list = p.getApplicationACLsList();
    this.applicationACLS = new HashMap<ApplicationAccessType, String>(list
        .size());

    for (ApplicationACLMapProto aclProto : list) {
      this.applicationACLS.put(ProtoUtils.convertFromProtoFormat(aclProto
          .getAccessType()), aclProto.getAcl());
    }
  }

  private void addApplicationACLs() {
    maybeInitBuilder();
    builder.clearApplicationACLs();
    if (applicationACLS == null) {
      return;
    }
    Iterable<? extends ApplicationACLMapProto> values
        = new Iterable<ApplicationACLMapProto>() {

      @Override
      public Iterator<ApplicationACLMapProto> iterator() {
        return new Iterator<ApplicationACLMapProto>() {
          Iterator<ApplicationAccessType> aclsIterator = applicationACLS
              .keySet().iterator();

          @Override
          public boolean hasNext() {
            return aclsIterator.hasNext();
          }

          @Override
          public ApplicationACLMapProto next() {
            ApplicationAccessType key = aclsIterator.next();
            return ApplicationACLMapProto.newBuilder().setAcl(
                applicationACLS.get(key)).setAccessType(
                ProtoUtils.convertToProtoFormat(key)).build();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    this.builder.addAllApplicationACLs(values);
  }

  @Override
  public void setApplicationACLs(
      final Map<ApplicationAccessType, String> appACLs) {
    if (appACLs == null)
      return;
    initApplicationACLs();
    this.applicationACLS.clear();
    this.applicationACLS.putAll(appACLs);
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
