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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder;

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
    if (key == null) {
      builder.clearClientToAmTokenMasterKey();
      return;
    }
    maybeInitBuilder();
    builder.setClientToAmTokenMasterKey(ByteString.copyFrom(key));
  }
  
  @Override
  public ByteBuffer getClientToAMTokenMasterKey() {
    maybeInitBuilder();
    ByteBuffer key =
        ByteBuffer.wrap(builder.getClientToAmTokenMasterKey().toByteArray());
    return key;
  }
  
  private Resource convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  private ResourceProto convertToProtoFormat(Resource resource) {
    return ((ResourcePBImpl)resource).getProto();
  }

}
