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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerExceptionMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersResponseProtoOrBuilder;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

@Private
@Unstable
public class StartContainersResponsePBImpl extends StartContainersResponse {
  StartContainersResponseProto proto = StartContainersResponseProto
    .getDefaultInstance();
  StartContainersResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Map<String, ByteBuffer> servicesMetaData = null;
  private List<ContainerId> succeededContainers = null;
  private Map<ContainerId, SerializedException> failedContainers = null;

  public StartContainersResponsePBImpl() {
    builder = StartContainersResponseProto.newBuilder();
  }

  public StartContainersResponsePBImpl(StartContainersResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public StartContainersResponseProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.servicesMetaData != null) {
      addServicesMetaDataToProto();
    }
    if (this.succeededContainers != null) {
      addSucceededContainersToProto();
    }
    if (this.failedContainers != null) {
      addFailedContainersToProto();
    }
  }

  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
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
      builder = StartContainersResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Map<String, ByteBuffer> getAllServicesMetaData() {
    initServicesMetaData();
    return this.servicesMetaData;
  }

  @Override
  public void setAllServicesMetaData(Map<String, ByteBuffer> servicesMetaData) {
    if (servicesMetaData == null) {
      return;
    }
    initServicesMetaData();
    this.servicesMetaData.clear();
    this.servicesMetaData.putAll(servicesMetaData);
  }

  private void initServicesMetaData() {
    if (this.servicesMetaData != null) {
      return;
    }
    StartContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<StringBytesMapProto> list = p.getServicesMetaDataList();
    this.servicesMetaData = new HashMap<String, ByteBuffer>();

    for (StringBytesMapProto c : list) {
      this.servicesMetaData.put(c.getKey(),
        convertFromProtoFormat(c.getValue()));
    }
  }

  private void addServicesMetaDataToProto() {
    maybeInitBuilder();
    builder.clearServicesMetaData();
    if (servicesMetaData == null)
      return;
    Iterable<StringBytesMapProto> iterable =
        new Iterable<StringBytesMapProto>() {

          @Override
          public Iterator<StringBytesMapProto> iterator() {
            return new Iterator<StringBytesMapProto>() {

              Iterator<String> keyIter = servicesMetaData.keySet().iterator();

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public StringBytesMapProto next() {
                String key = keyIter.next();
                return StringBytesMapProto.newBuilder().setKey(key)
                  .setValue(convertToProtoFormat(servicesMetaData.get(key)))
                  .build();
              }

              @Override
              public boolean hasNext() {
                return keyIter.hasNext();
              }
            };
          }
        };
    builder.addAllServicesMetaData(iterable);
  }

  private void addFailedContainersToProto() {
    maybeInitBuilder();
    builder.clearFailedRequests();
    if (this.failedContainers == null)
      return;
    List<ContainerExceptionMapProto> protoList =
        new ArrayList<ContainerExceptionMapProto>();

    for (Map.Entry<ContainerId, SerializedException> entry : this.failedContainers
      .entrySet()) {
      protoList.add(ContainerExceptionMapProto.newBuilder()
        .setContainerId(convertToProtoFormat(entry.getKey()))
        .setException(convertToProtoFormat(entry.getValue())).build());
    }
    builder.addAllFailedRequests(protoList);
  }

  private void addSucceededContainersToProto() {
    maybeInitBuilder();
    builder.clearSucceededRequests();
    if (this.succeededContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = succeededContainers.iterator();

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

  private void initSucceededContainers() {
    if (this.succeededContainers != null)
      return;
    StartContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getSucceededRequestsList();
    this.succeededContainers = new ArrayList<ContainerId>();
    for (ContainerIdProto c : list) {
      this.succeededContainers.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public List<ContainerId> getSuccessfullyStartedContainers() {
    initSucceededContainers();
    return this.succeededContainers;
  }

  @Override
  public void setSuccessfullyStartedContainers(
      List<ContainerId> succeededContainers) {
    maybeInitBuilder();
    if (succeededContainers == null) {
      builder.clearSucceededRequests();
    }
    this.succeededContainers = succeededContainers;
  }

  private void initFailedContainers() {
    if (this.failedContainers != null) {
      return;
    }
    StartContainersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerExceptionMapProto> protoList = p.getFailedRequestsList();
    this.failedContainers = new HashMap<ContainerId, SerializedException>();
    for (ContainerExceptionMapProto ce : protoList) {
      this.failedContainers.put(convertFromProtoFormat(ce.getContainerId()),
        convertFromProtoFormat(ce.getException()));
    }
  }

  @Override
  public Map<ContainerId, SerializedException> getFailedRequests() {
    initFailedContainers();
    return this.failedContainers;
  }

  @Override
  public void setFailedRequests(
      Map<ContainerId, SerializedException> failedContainers) {
    maybeInitBuilder();
    if (failedContainers == null)
      builder.clearFailedRequests();
    this.failedContainers = failedContainers;
  }
}
