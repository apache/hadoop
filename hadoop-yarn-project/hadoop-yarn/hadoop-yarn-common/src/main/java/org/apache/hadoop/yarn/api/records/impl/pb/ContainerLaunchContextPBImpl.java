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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerRetryContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringLocalResourceMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

@Private
@Unstable
public class ContainerLaunchContextPBImpl 
extends ContainerLaunchContext {
  ContainerLaunchContextProto proto = 
      ContainerLaunchContextProto.getDefaultInstance();
  ContainerLaunchContextProto.Builder builder = null;
  boolean viaProto = false;
  
  private Map<String, LocalResource> localResources = null;
  private ByteBuffer tokens = null;
  private ByteBuffer tokensConf = null;
  private Map<String, ByteBuffer> serviceData = null;
  private Map<String, String> environment = null;
  private List<String> commands = null;
  private Map<ApplicationAccessType, String> applicationACLS = null;
  private ContainerRetryContext containerRetryContext = null;

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

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }

  private void mergeLocalToBuilder() {
    if (this.localResources != null) {
      addLocalResourcesToProto();
    }
    if (this.tokens != null) {
      builder.setTokens(convertToProtoFormat(this.tokens));
    }
    if (this.tokensConf != null) {
      builder.setTokensConf(convertToProtoFormat(this.tokensConf));
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
    if (this.containerRetryContext != null) {
      builder.setContainerRetryContext(
          convertToProtoFormat(this.containerRetryContext));
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
    checkLocalResources(localResources);
    initLocalResources();
    this.localResources.clear();
    this.localResources.putAll(localResources);
  }
  
  private void checkLocalResources(Map<String, LocalResource> localResources) {
    for (Map.Entry<String, LocalResource> rsrcEntry : localResources
        .entrySet()) {
      if (rsrcEntry.getValue() == null
          || rsrcEntry.getValue().getResource() == null) {
        throw new NullPointerException(
            "Null resource URL for local resource " + rsrcEntry.getKey() + " : "
                + rsrcEntry.getValue());
      } else if (rsrcEntry.getValue().getType() == null) {
        throw new NullPointerException(
            "Null resource type for local resource " + rsrcEntry.getKey() + " : "
                + rsrcEntry.getValue());
      } else if (rsrcEntry.getValue().getVisibility() == null) {
          throw new NullPointerException(
            "Null resource visibility for local resource " + rsrcEntry.getKey() + " : "
                + rsrcEntry.getValue());
      }
    }
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
  public ByteBuffer getTokens() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.tokens != null) {
      return this.tokens;
    }
    if (!p.hasTokens()) {
      return null;
    }
    this.tokens =  convertFromProtoFormat(p.getTokens());
    return this.tokens;
  }

  @Override
  public void setTokens(ByteBuffer tokens) {
    maybeInitBuilder();
    if (tokens == null) {
      builder.clearTokens();
    }
    this.tokens = tokens;
  }

  @Override
  public ByteBuffer getTokensConf() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.tokensConf != null) {
      return this.tokensConf;
    }
    if (!p.hasTokensConf()) {
      return null;
    }
    this.tokensConf = convertFromProtoFormat(p.getTokensConf());
    return this.tokensConf;
  }

  @Override
  public void setTokensConf(ByteBuffer tokensConf) {
    maybeInitBuilder();
    if (tokensConf == null) {
      builder.clearTokensConf();
    }
    this.tokensConf = tokensConf;
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
      this.environment.put(StringInterner.weakIntern(c.getKey()),
          StringInterner.weakIntern(c.getValue()));
    }
  }
  
  @Override
  public void setEnvironment(final Map<String, String> env) {
    if (env == null)
      return;
    initEnv();
    this.environment.clear();
    for (Map.Entry<String, String> e : env.entrySet()) {
      this.environment.put(StringInterner.weakIntern(e.getKey()),
          StringInterner.weakIntern(e.getValue()));
    }
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
            String value = environment.get(key);

            if (value == null) {
              value = "";
            }

            return StringStringMapProto.newBuilder().setKey(key)
                .setValue((value)).build();
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
          .getAccessType()), StringInterner.weakIntern(aclProto.getAcl()));
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
    for (Map.Entry<ApplicationAccessType, String> e : appACLs.entrySet()) {
      this.applicationACLS.put(e.getKey(),
          StringInterner.weakIntern(e.getValue()));
    }
  }

  public ContainerRetryContext getContainerRetryContext() {
    ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerRetryContext != null) {
      return this.containerRetryContext;
    }
    if (!p.hasContainerRetryContext()) {
      return null;
    }
    this.containerRetryContext = convertFromProtoFormat(
        p.getContainerRetryContext());
    return this.containerRetryContext;
  }

  public void setContainerRetryContext(ContainerRetryContext retryContext) {
    maybeInitBuilder();
    if (retryContext == null) {
      builder.clearContainerRetryContext();
    }
    this.containerRetryContext = retryContext;
  }

  private LocalResourcePBImpl convertFromProtoFormat(LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private LocalResourceProto convertToProtoFormat(LocalResource t) {
    return ((LocalResourcePBImpl)t).getProto();
  }

  private ContainerRetryContextPBImpl convertFromProtoFormat(
      ContainerRetryContextProto p) {
    return new ContainerRetryContextPBImpl(p);
  }

  private ContainerRetryContextProto convertToProtoFormat(
      ContainerRetryContext t) {
    return ((ContainerRetryContextPBImpl)t).getProto();
  }
}
