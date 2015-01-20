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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NMTokenPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.NMTokenProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;


@Private
@Unstable
public class RegisterApplicationMasterResponsePBImpl extends
    RegisterApplicationMasterResponse {
  RegisterApplicationMasterResponseProto proto =
    RegisterApplicationMasterResponseProto.getDefaultInstance();
  RegisterApplicationMasterResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Resource maximumResourceCapability;
  private Map<ApplicationAccessType, String> applicationACLS = null;
  private List<Container> containersFromPreviousAttempts = null;
  private List<NMToken> nmTokens = null;
  private EnumSet<SchedulerResourceTypes> schedulerResourceTypes = null;

  public RegisterApplicationMasterResponsePBImpl() {
    builder = RegisterApplicationMasterResponseProto.newBuilder();
  }

  public RegisterApplicationMasterResponsePBImpl(RegisterApplicationMasterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public RegisterApplicationMasterResponseProto getProto() {
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

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.maximumResourceCapability != null) {
      builder.setMaximumCapability(
          convertToProtoFormat(this.maximumResourceCapability));
    }
    if (this.applicationACLS != null) {
      addApplicationACLs();
    }
    if (this.containersFromPreviousAttempts != null) {
      addContainersFromPreviousAttemptToProto();
    }
    if (nmTokens != null) {
      builder.clearNmTokensFromPreviousAttempts();
      Iterable<NMTokenProto> iterable = getTokenProtoIterable(nmTokens);
      builder.addAllNmTokensFromPreviousAttempts(iterable);
    }
    if(schedulerResourceTypes != null) {
      addSchedulerResourceTypes();
    }
  }


  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterApplicationMasterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    if (this.maximumResourceCapability != null) {
      return this.maximumResourceCapability;
    }

    RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaximumCapability()) {
      return null;
    }

    this.maximumResourceCapability = convertFromProtoFormat(p.getMaximumCapability());
    return this.maximumResourceCapability;
  }

  @Override
  public void setMaximumResourceCapability(Resource capability) {
    maybeInitBuilder();
    if(maximumResourceCapability == null) {
      builder.clearMaximumCapability();
    }
    this.maximumResourceCapability = capability;
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
    RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? proto
        : builder;
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
  
  @Override
  public void setClientToAMTokenMasterKey(ByteBuffer key) {
    maybeInitBuilder();
    if (key == null) {
      builder.clearClientToAmTokenMasterKey();
      return;
    }
    builder.setClientToAmTokenMasterKey(ByteString.copyFrom(key));
  }
  
  @Override
  public ByteBuffer getClientToAMTokenMasterKey() {
    maybeInitBuilder();
    ByteBuffer key =
        ByteBuffer.wrap(builder.getClientToAmTokenMasterKey().toByteArray());
    return key;
  }

  @Override
  public List<Container> getContainersFromPreviousAttempts() {
    if (this.containersFromPreviousAttempts != null) {
      return this.containersFromPreviousAttempts;
    }
    initContainersPreviousAttemptList();
    return this.containersFromPreviousAttempts;
  }

  @Override
  public void
      setContainersFromPreviousAttempts(final List<Container> containers) {
    if (containers == null) {
      return;
    }
    this.containersFromPreviousAttempts = new ArrayList<Container>();
    this.containersFromPreviousAttempts.addAll(containers);
  }
  
  @Override
  public String getQueue() {
    RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return p.getQueue();
  }
  
  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
    } else {
      builder.setQueue(queue);
    }
  }


  private void initContainersPreviousAttemptList() {
    RegisterApplicationMasterResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ContainerProto> list = p.getContainersFromPreviousAttemptsList();
    containersFromPreviousAttempts = new ArrayList<Container>();
    for (ContainerProto c : list) {
      containersFromPreviousAttempts.add(convertFromProtoFormat(c));
    }
  }

  private void addContainersFromPreviousAttemptToProto() {
    maybeInitBuilder();
    builder.clearContainersFromPreviousAttempts();
    List<ContainerProto> list = new ArrayList<ContainerProto>();
    for (Container c : containersFromPreviousAttempts) {
      list.add(convertToProtoFormat(c));
    }
    builder.addAllContainersFromPreviousAttempts(list);
  }


  @Override
  public List<NMToken> getNMTokensFromPreviousAttempts() {
    if (nmTokens != null) {
      return nmTokens;
    }
    initLocalNewNMTokenList();
    return nmTokens;
  }
  
  @Override
  public void setNMTokensFromPreviousAttempts(final List<NMToken> nmTokens) {
    maybeInitBuilder();
    if (nmTokens == null || nmTokens.isEmpty()) {
      if (this.nmTokens != null) {
        this.nmTokens.clear();
      }
      builder.clearNmTokensFromPreviousAttempts();
      return;
    }
    this.nmTokens = new ArrayList<NMToken>();
    this.nmTokens.addAll(nmTokens);
  }

  private synchronized void initLocalNewNMTokenList() {
    RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NMTokenProto> list = p.getNmTokensFromPreviousAttemptsList();
    nmTokens = new ArrayList<NMToken>();
    for (NMTokenProto t : list) {
      nmTokens.add(convertFromProtoFormat(t));
    }
  }

  private synchronized Iterable<NMTokenProto> getTokenProtoIterable(
      final List<NMToken> nmTokenList) {
    maybeInitBuilder();
    return new Iterable<NMTokenProto>() {
      @Override
      public synchronized Iterator<NMTokenProto> iterator() {
        return new Iterator<NMTokenProto>() {

          Iterator<NMToken> iter = nmTokenList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public NMTokenProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public EnumSet<SchedulerResourceTypes> getSchedulerResourceTypes() {
    initSchedulerResourceTypes();
    return this.schedulerResourceTypes;
  }

  private void initSchedulerResourceTypes() {
    if (this.schedulerResourceTypes != null) {
      return;
    }
    RegisterApplicationMasterResponseProtoOrBuilder p =
        viaProto ? proto : builder;

    List<SchedulerResourceTypes> list = p.getSchedulerResourceTypesList();
    if (list.isEmpty()) {
      this.schedulerResourceTypes =
          EnumSet.noneOf(SchedulerResourceTypes.class);
    } else {
      this.schedulerResourceTypes = EnumSet.copyOf(list);
    }
  }

  private void addSchedulerResourceTypes() {
    maybeInitBuilder();
    builder.clearSchedulerResourceTypes();
    if (schedulerResourceTypes == null) {
      return;
    }
    Iterable<? extends SchedulerResourceTypes> values =
        new Iterable<SchedulerResourceTypes>() {

          @Override
          public Iterator<SchedulerResourceTypes> iterator() {
            return new Iterator<SchedulerResourceTypes>() {
              Iterator<SchedulerResourceTypes> settingsIterator =
                  schedulerResourceTypes.iterator();

              @Override
              public boolean hasNext() {
                return settingsIterator.hasNext();
              }

              @Override
              public SchedulerResourceTypes next() {
                return settingsIterator.next();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    this.builder.addAllSchedulerResourceTypes(values);
  }

  @Override
  public void setSchedulerResourceTypes(EnumSet<SchedulerResourceTypes> types) {
    if (types == null) {
      return;
    }
    initSchedulerResourceTypes();
    this.schedulerResourceTypes.clear();
    this.schedulerResourceTypes.addAll(types);
  }

  private Resource convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  private ResourceProto convertToProtoFormat(Resource resource) {
    return ((ResourcePBImpl)resource).getProto();
  }

  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl) t).getProto();
  }

  private NMTokenProto convertToProtoFormat(NMToken token) {
    return ((NMTokenPBImpl) token).getProto();
  }

  private NMToken convertFromProtoFormat(NMTokenProto proto) {
    return new NMTokenPBImpl(proto);
  }
}
