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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.DistributedSchedulingAllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.DistributedSchedulingAllocateRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link DistributedSchedulingAllocateRequest}.
 */
public class DistributedSchedulingAllocateRequestPBImpl
    extends DistributedSchedulingAllocateRequest {
  private DistributedSchedulingAllocateRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private DistributedSchedulingAllocateRequestProto proto;
  private AllocateRequest allocateRequest;
  private List<Container> containers;

  public DistributedSchedulingAllocateRequestPBImpl() {
    builder = DistributedSchedulingAllocateRequestProto.newBuilder();
  }

  public DistributedSchedulingAllocateRequestPBImpl(
      DistributedSchedulingAllocateRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @Override
  public AllocateRequest getAllocateRequest() {
    DistributedSchedulingAllocateRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.allocateRequest != null) {
      return this.allocateRequest;
    }
    if (!p.hasAllocateRequest()) {
      return null;
    }
    this.allocateRequest = convertFromProtoFormat(p.getAllocateRequest());
    return this.allocateRequest;
  }

  @Override
  public void setAllocateRequest(AllocateRequest pAllocateRequest) {
    maybeInitBuilder();
    if (allocateRequest == null) {
      builder.clearAllocateRequest();
    }
    this.allocateRequest = pAllocateRequest;
  }

  @Override
  public List<Container> getAllocatedContainers() {
    if (this.containers != null) {
      return this.containers;
    }
    initAllocatedContainers();
    return containers;
  }

  private void initAllocatedContainers() {
    DistributedSchedulingAllocateRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ContainerProto> list = p.getAllocatedContainersList();
    this.containers = new ArrayList<Container>();
    for (ContainerProto c : list) {
      this.containers.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void setAllocatedContainers(List<Container> pContainers) {
    maybeInitBuilder();
    if (pContainers == null || pContainers.isEmpty()) {
      if (this.containers != null) {
        this.containers.clear();
      }
      builder.clearAllocatedContainers();
      return;
    }
    this.containers = new ArrayList<>();
    this.containers.addAll(pContainers);
  }

  public DistributedSchedulingAllocateRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DistributedSchedulingAllocateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.containers != null) {
      builder.clearAllocatedContainers();
      Iterable<ContainerProto> iterable =
          getContainerProtoIterable(this.containers);
      builder.addAllAllocatedContainers(iterable);
    }
    if (this.allocateRequest != null) {
      builder.setAllocateRequest(
          ((AllocateRequestPBImpl)this.allocateRequest).getProto());
    }
  }

  private Iterable<ContainerProto> getContainerProtoIterable(
      final List<Container> newContainersList) {
    maybeInitBuilder();
    return new Iterable<ContainerProto>() {
      @Override
      public synchronized Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {
          Iterator<Container> iter = newContainersList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized ContainerProto next() {
            return ProtoUtils.convertToProtoFormat(iter.next());
          }

          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();

          }
        };
      }
    };
  }

  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private AllocateRequestPBImpl convertFromProtoFormat(AllocateRequestProto p) {
    return new AllocateRequestPBImpl(p);
  }
}
