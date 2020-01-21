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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class GetContainersResponsePBImpl extends GetContainersResponse {

  GetContainersResponseProto proto = GetContainersResponseProto
    .getDefaultInstance();
  GetContainersResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<ContainerReport> containerList;

  public GetContainersResponsePBImpl() {
    builder = GetContainersResponseProto.newBuilder();
  }

  public GetContainersResponsePBImpl(GetContainersResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<ContainerReport> getContainerList() {
    initLocalContainerList();
    return this.containerList;
  }

  @Override
  public void setContainerList(List<ContainerReport> containers) {
    maybeInitBuilder();
    if (containers == null) {
      builder.clearContainers();
    }
    this.containerList = containers;
  }

  public GetContainersResponseProto getProto() {
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
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.containerList != null) {
      addLocalContainersToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetContainersResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // Once this is called. containerList will never be null - until a getProto
  // is called.
  private void initLocalContainerList() {
    if (this.containerList != null) {
      return;
    }
    GetContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerReportProto> list = p.getContainersList();
    containerList = new ArrayList<ContainerReport>();

    for (ContainerReportProto c : list) {
      containerList.add(convertFromProtoFormat(c));
    }
  }

  private void addLocalContainersToProto() {
    maybeInitBuilder();
    builder.clearContainers();
    if (containerList == null) {
      return;
    }
    Iterable<ContainerReportProto> iterable =
        new Iterable<ContainerReportProto>() {
          @Override
          public Iterator<ContainerReportProto> iterator() {
            return new Iterator<ContainerReportProto>() {

              Iterator<ContainerReport> iter = containerList.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ContainerReportProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();

              }
            };

          }
        };
    builder.addAllContainers(iterable);
  }

  private ContainerReportPBImpl convertFromProtoFormat(ContainerReportProto p) {
    return new ContainerReportPBImpl(p);
  }

  private ContainerReportProto convertToProtoFormat(ContainerReport t) {
    return ((ContainerReportPBImpl) t).getProto();
  }

}
