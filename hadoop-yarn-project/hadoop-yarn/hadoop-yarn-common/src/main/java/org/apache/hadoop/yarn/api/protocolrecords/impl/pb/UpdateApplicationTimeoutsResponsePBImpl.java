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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationUpdateTimeoutMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationTimeoutsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationTimeoutsResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class UpdateApplicationTimeoutsResponsePBImpl
    extends UpdateApplicationTimeoutsResponse {
  UpdateApplicationTimeoutsResponseProto proto =
      UpdateApplicationTimeoutsResponseProto.getDefaultInstance();
  UpdateApplicationTimeoutsResponseProto.Builder builder = null;
  boolean viaProto = false;
  private Map<ApplicationTimeoutType, String> applicationTimeouts = null;

  public UpdateApplicationTimeoutsResponsePBImpl() {
    builder = UpdateApplicationTimeoutsResponseProto.newBuilder();
  }

  public UpdateApplicationTimeoutsResponsePBImpl(
      UpdateApplicationTimeoutsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateApplicationTimeoutsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
      builder = UpdateApplicationTimeoutsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationTimeouts != null) {
      addApplicationTimeouts();
    }
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

  @Override
  public Map<ApplicationTimeoutType, String> getApplicationTimeouts() {
    initApplicationTimeout();
    return this.applicationTimeouts;
  }

  private void initApplicationTimeout() {
    if (this.applicationTimeouts != null) {
      return;
    }
    UpdateApplicationTimeoutsResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ApplicationUpdateTimeoutMapProto> lists =
        p.getApplicationTimeoutsList();
    this.applicationTimeouts =
        new HashMap<ApplicationTimeoutType, String>(lists.size());
    for (ApplicationUpdateTimeoutMapProto timeoutProto : lists) {
      this.applicationTimeouts.put(
          ProtoUtils
              .convertFromProtoFormat(timeoutProto.getApplicationTimeoutType()),
          timeoutProto.getExpireTime());
    }
  }

  @Override
  public void setApplicationTimeouts(
      Map<ApplicationTimeoutType, String> appTimeouts) {
    if (appTimeouts == null) {
      return;
    }
    initApplicationTimeout();
    this.applicationTimeouts.clear();
    this.applicationTimeouts.putAll(appTimeouts);
  }

  private void addApplicationTimeouts() {
    maybeInitBuilder();
    builder.clearApplicationTimeouts();
    if (applicationTimeouts == null) {
      return;
    }
    Iterable<? extends ApplicationUpdateTimeoutMapProto> values =
        new Iterable<ApplicationUpdateTimeoutMapProto>() {

          @Override
          public Iterator<ApplicationUpdateTimeoutMapProto> iterator() {
            return new Iterator<ApplicationUpdateTimeoutMapProto>() {
              private Iterator<ApplicationTimeoutType> iterator =
                  applicationTimeouts.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public ApplicationUpdateTimeoutMapProto next() {
                ApplicationTimeoutType key = iterator.next();
                return ApplicationUpdateTimeoutMapProto.newBuilder()
                    .setExpireTime(applicationTimeouts.get(key))
                    .setApplicationTimeoutType(
                        ProtoUtils.convertToProtoFormat(key))
                    .build();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    this.builder.addAllApplicationTimeouts(values);
  }
}
