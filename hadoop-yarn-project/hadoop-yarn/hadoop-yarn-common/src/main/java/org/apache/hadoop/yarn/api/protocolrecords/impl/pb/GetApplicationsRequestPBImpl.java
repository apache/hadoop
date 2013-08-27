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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetApplicationsRequestPBImpl extends GetApplicationsRequest {
  GetApplicationsRequestProto proto = GetApplicationsRequestProto.getDefaultInstance();
  GetApplicationsRequestProto.Builder builder = null;
  boolean viaProto = false;

  Set<String> applicationTypes = null;
  EnumSet<YarnApplicationState> applicationStates = null;

  public GetApplicationsRequestPBImpl() {
    builder = GetApplicationsRequestProto.newBuilder();
  }

  public GetApplicationsRequestPBImpl(GetApplicationsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetApplicationsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationTypes != null) {
      addLocalApplicationTypesToProto();
    }
    if (this.applicationStates != null) {
      maybeInitBuilder();
      builder.clearApplicationStates();
      Iterable<YarnApplicationStateProto> iterable =
          new Iterable<YarnApplicationStateProto>() {

            @Override
            public Iterator<YarnApplicationStateProto> iterator() {
              return new Iterator<YarnApplicationStateProto>() {

                Iterator<YarnApplicationState> iter = applicationStates
                    .iterator();

                @Override
                public boolean hasNext() {
                  return iter.hasNext();
                }

                @Override
                public YarnApplicationStateProto next() {
                  return ProtoUtils.convertToProtoFormat(iter.next());
                }

                @Override
                public void remove() {
                  throw new UnsupportedOperationException();

                }
              };

            }
          };
      builder.addAllApplicationStates(iterable);
    }
  }

  private void addLocalApplicationTypesToProto() {
    maybeInitBuilder();
    builder.clearApplicationTypes();
    if (this.applicationTypes == null)
      return;
    builder.addAllApplicationTypes(applicationTypes);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initApplicationTypes() {
    if (this.applicationTypes != null) {
      return;
    }
    GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> appTypeList = p.getApplicationTypesList();
    this.applicationTypes = new HashSet<String>();
    this.applicationTypes.addAll(appTypeList);
  }

  private void initApplicationStates() {
    if (this.applicationStates != null) {
      return;
    }
    GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnApplicationStateProto> appStatesList =
        p.getApplicationStatesList();
    this.applicationStates = EnumSet.noneOf(YarnApplicationState.class);

    for (YarnApplicationStateProto c : appStatesList) {
      this.applicationStates.add(ProtoUtils.convertFromProtoFormat(c));
    }
  }

  @Override
  public Set<String> getApplicationTypes() {
    initApplicationTypes();
    return this.applicationTypes;
  }

  @Override
  public void setApplicationTypes(Set<String> applicationTypes) {
    maybeInitBuilder();
    if (applicationTypes == null)
      builder.clearApplicationTypes();
    this.applicationTypes = applicationTypes;
  }

  @Override
  public EnumSet<YarnApplicationState> getApplicationStates() {
    initApplicationStates();
    return this.applicationStates;
  }

  @Override
  public void setApplicationStates(EnumSet<YarnApplicationState> applicationStates) {
    maybeInitBuilder();
    if (applicationStates == null) {
      builder.clearApplicationStates();
    }
    this.applicationStates = applicationStates;
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
}
