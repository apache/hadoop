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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceTypeInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceTypeInfoProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceTypeInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceTypeInfoResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Protobuf implementation class for the GetAllResourceTypeInfoResponse.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GetAllResourceTypeInfoResponsePBImpl
    extends
      GetAllResourceTypeInfoResponse {

  private GetAllResourceTypeInfoResponseProto proto = GetAllResourceTypeInfoResponseProto
      .getDefaultInstance();
  private GetAllResourceTypeInfoResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private List<ResourceTypeInfo> resourceTypeInfo;

  public GetAllResourceTypeInfoResponsePBImpl() {
    builder = GetAllResourceTypeInfoResponseProto.newBuilder();
  }

  public GetAllResourceTypeInfoResponsePBImpl(
      GetAllResourceTypeInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetAllResourceTypeInfoResponseProto getProto() {
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
  public void setResourceTypeInfo(List<ResourceTypeInfo> resourceTypes) {
    if (resourceTypeInfo == null) {
      builder.clearResourceTypeInfo();
    }
    this.resourceTypeInfo = resourceTypes;
  }

  @Override
  public List<ResourceTypeInfo> getResourceTypeInfo() {
    initResourceTypeInfosList();
    return this.resourceTypeInfo;
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
    if (this.resourceTypeInfo != null) {
      addResourceTypeInfosToProto();
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
      builder = GetAllResourceTypeInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // Once this is called. containerList will never be null - until a getProto
  // is called.
  private void initResourceTypeInfosList() {
    if (this.resourceTypeInfo != null) {
      return;
    }
    GetAllResourceTypeInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceTypeInfoProto> list = p.getResourceTypeInfoList();
    resourceTypeInfo = new ArrayList<ResourceTypeInfo>();

    for (ResourceTypeInfoProto a : list) {
      resourceTypeInfo.add(convertFromProtoFormat(a));
    }
  }

  private void addResourceTypeInfosToProto() {
    maybeInitBuilder();
    builder.clearResourceTypeInfo();
    if (resourceTypeInfo == null) {
      return;
    }
    Iterable<ResourceTypeInfoProto> iterable = new Iterable<ResourceTypeInfoProto>() {
      @Override
      public Iterator<ResourceTypeInfoProto> iterator() {
        return new Iterator<ResourceTypeInfoProto>() {

          Iterator<ResourceTypeInfo> iter = resourceTypeInfo.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceTypeInfoProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllResourceTypeInfo(iterable);
  }

  private ResourceTypeInfoPBImpl convertFromProtoFormat(
      ResourceTypeInfoProto p) {
    return new ResourceTypeInfoPBImpl(p);
  }

  private ResourceTypeInfoProto convertToProtoFormat(ResourceTypeInfo t) {
    return ((ResourceTypeInfoPBImpl) t).getProto();
  }
}
