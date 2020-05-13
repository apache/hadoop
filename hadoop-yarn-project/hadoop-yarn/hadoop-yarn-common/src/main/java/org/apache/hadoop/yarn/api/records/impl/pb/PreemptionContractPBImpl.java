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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContractProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContractProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionResourceRequestProto;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class PreemptionContractPBImpl extends PreemptionContract {

  PreemptionContractProto proto = PreemptionContractProto.getDefaultInstance();
  PreemptionContractProto.Builder builder = null;

  boolean viaProto = false;
  private Set<PreemptionContainer> containers;
  private List<PreemptionResourceRequest> resources;

  public PreemptionContractPBImpl() {
    builder = PreemptionContractProto.newBuilder();
  }

  public PreemptionContractPBImpl(PreemptionContractProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized PreemptionContractProto getProto() {
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
    if (this.resources != null) {
      addResourcesToProto();
    }
    if (this.containers != null) {
      addContainersToProto();
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PreemptionContractProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized Set<PreemptionContainer> getContainers() {
    initPreemptionContainers();
    return containers;
  }

  @Override
  public synchronized void setContainers(
      final Set<PreemptionContainer> containers) {
    if (null == containers) {
      builder.clearContainer();
    }
    this.containers = containers;
  }

  @Override
  public synchronized List<PreemptionResourceRequest> getResourceRequest() {
    initPreemptionResourceRequests();
    return resources;
  }

  @Override
  public synchronized void setResourceRequest(
      final List<PreemptionResourceRequest> req) {
    if (null == resources) {
      builder.clearResource();
    }
    this.resources = req;
  }

  private void initPreemptionResourceRequests() {
    if (resources != null) {
      return;
    }
    PreemptionContractProtoOrBuilder p = viaProto ? proto : builder;
    List<PreemptionResourceRequestProto> list = p.getResourceList();
    resources = new ArrayList<PreemptionResourceRequest>();

    for (PreemptionResourceRequestProto rr : list) {
      resources.add(convertFromProtoFormat(rr));
    }
  }

  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResource();
    if (null == resources) {
      return;
    }
    Iterable<PreemptionResourceRequestProto> iterable =
      new Iterable<PreemptionResourceRequestProto>() {
      @Override
      public Iterator<PreemptionResourceRequestProto> iterator() {
        return new Iterator<PreemptionResourceRequestProto>() {

          Iterator<PreemptionResourceRequest> iter = resources.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public PreemptionResourceRequestProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllResource(iterable);
  }

  private void initPreemptionContainers() {
    if (containers != null) {
      return;
    }
    PreemptionContractProtoOrBuilder p = viaProto ? proto : builder;
    List<PreemptionContainerProto> list = p.getContainerList();
    containers = new HashSet<PreemptionContainer>();

    for (PreemptionContainerProto c : list) {
      containers.add(convertFromProtoFormat(c));
    }
  }

  private void addContainersToProto() {
    maybeInitBuilder();
    builder.clearContainer();
    if (null == containers) {
      return;
    }
    Iterable<PreemptionContainerProto> iterable =
      new Iterable<PreemptionContainerProto>() {
      @Override
      public Iterator<PreemptionContainerProto> iterator() {
        return new Iterator<PreemptionContainerProto>() {

          Iterator<PreemptionContainer> iter = containers.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public PreemptionContainerProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainer(iterable);
  }

  private PreemptionContainerPBImpl convertFromProtoFormat(PreemptionContainerProto p) {
    return new PreemptionContainerPBImpl(p);
  }

  private PreemptionContainerProto convertToProtoFormat(PreemptionContainer t) {
    return ((PreemptionContainerPBImpl)t).getProto();
  }

  private PreemptionResourceRequestPBImpl convertFromProtoFormat(PreemptionResourceRequestProto p) {
    return new PreemptionResourceRequestPBImpl(p);
  }

  private PreemptionResourceRequestProto convertToProtoFormat(PreemptionResourceRequest t) {
    return ((PreemptionResourceRequestPBImpl)t).getProto();
  }

}
