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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerExceptionMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class StopContainersResponsePBImpl extends StopContainersResponse {
  StopContainersResponseProto proto = StopContainersResponseProto
    .getDefaultInstance();
  StopContainersResponseProto.Builder builder = null;
  boolean viaProto = false;
  private List<ContainerId> succeededRequests = null;
  private Map<ContainerId, SerializedException> failedRequests = null;

  public StopContainersResponsePBImpl() {
    builder = StopContainersResponseProto.newBuilder();
  }

  public StopContainersResponsePBImpl(StopContainersResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public StopContainersResponseProto getProto() {
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
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StopContainersResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {

    if (this.succeededRequests != null) {
      addSucceededRequestsToProto();
    }
    if (this.failedRequests != null) {
      addFailedRequestsToProto();
    }
  }

  private void addSucceededRequestsToProto() {
    maybeInitBuilder();
    builder.clearSucceededRequests();
    if (this.succeededRequests == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = succeededRequests.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };
      }
    };
    builder.addAllSucceededRequests(iterable);
  }

  private void addFailedRequestsToProto() {
    maybeInitBuilder();
    builder.clearFailedRequests();
    if (this.failedRequests == null)
      return;
    List<ContainerExceptionMapProto> protoList =
        new ArrayList<ContainerExceptionMapProto>();

    for (Map.Entry<ContainerId, SerializedException> entry : this.failedRequests
      .entrySet()) {
      protoList.add(ContainerExceptionMapProto.newBuilder()
        .setContainerId(convertToProtoFormat(entry.getKey()))
        .setException(convertToProtoFormat(entry.getValue())).build());
    }
    builder.addAllFailedRequests(protoList);
  }

  private void initSucceededRequests() {
    if (this.succeededRequests != null)
      return;
    StopContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getSucceededRequestsList();
    this.succeededRequests = new ArrayList<ContainerId>();
    for (ContainerIdProto c : list) {
      this.succeededRequests.add(convertFromProtoFormat(c));
    }
  }

  private void initFailedRequests() {
    if (this.failedRequests != null) {
      return;
    }
    StopContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerExceptionMapProto> protoList = p.getFailedRequestsList();
    this.failedRequests = new HashMap<ContainerId, SerializedException>();
    for (ContainerExceptionMapProto ce : protoList) {
      this.failedRequests.put(convertFromProtoFormat(ce.getContainerId()),
        convertFromProtoFormat(ce.getException()));
    }
  }

  @Override
  public List<ContainerId> getSuccessfullyStoppedContainers() {
    initSucceededRequests();
    return this.succeededRequests;
  }

  @Override
  public void setSuccessfullyStoppedContainers(List<ContainerId> succeededRequests) {
    maybeInitBuilder();
    if (succeededRequests == null) {
      builder.clearSucceededRequests();
    }
    this.succeededRequests = succeededRequests;
  }

  @Override
  public Map<ContainerId, SerializedException> getFailedRequests() {
    initFailedRequests();
    return this.failedRequests;
  }

  @Override
  public void setFailedRequests(
      Map<ContainerId, SerializedException> failedRequests) {
    maybeInitBuilder();
    if (failedRequests == null)
      builder.clearFailedRequests();
    this.failedRequests = failedRequests;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private SerializedExceptionPBImpl convertFromProtoFormat(
      SerializedExceptionProto p) {
    return new SerializedExceptionPBImpl(p);
  }

  private SerializedExceptionProto convertToProtoFormat(SerializedException t) {
    return ((SerializedExceptionPBImpl) t).getProto();
  }
}
