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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProtoOrBuilder;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetApplicationsRequestPBImpl extends GetApplicationsRequest {
  GetApplicationsRequestProto proto = GetApplicationsRequestProto.getDefaultInstance();
  GetApplicationsRequestProto.Builder builder = null;
  boolean viaProto = false;

  Set<String> applicationTypes = null;
  EnumSet<YarnApplicationState> applicationStates = null;
  Set<String> users = null;
  Set<String> queues = null;
  long limit = Long.MAX_VALUE;
  LongRange start = null;
  LongRange finish = null;
  private Set<String> applicationTags;
  private ApplicationsRequestScope scope;

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
    if (applicationTypes != null && !applicationTypes.isEmpty()) {
      builder.clearApplicationTypes();
      builder.addAllApplicationTypes(applicationTypes);
    }
    if (applicationStates != null && !applicationStates.isEmpty()) {
      builder.clearApplicationStates();
      builder.addAllApplicationStates(Iterables.transform(applicationStates,
          new Function<YarnApplicationState, YarnApplicationStateProto>() {
            @Override
            public YarnApplicationStateProto apply(YarnApplicationState input) {
              return ProtoUtils.convertToProtoFormat(input);
            }
          }));
    }
    if (applicationTags != null && !applicationTags.isEmpty()) {
      builder.clearApplicationTags();
      builder.addAllApplicationTags(this.applicationTags);
    }
    if (scope != null) {
      builder.setScope(ProtoUtils.convertToProtoFormat(scope));
    }
    if (start != null) {
      builder.setStartBegin(start.getMinimumLong());
      builder.setStartEnd(start.getMaximumLong());
    }
    if (finish != null) {
      builder.setFinishBegin(finish.getMinimumLong());
      builder.setFinishEnd(finish.getMaximumLong());
    }
    if (limit != Long.MAX_VALUE) {
      builder.setLimit(limit);
    }
    if (users != null && !users.isEmpty()) {
      builder.clearUsers();
      builder.addAllUsers(users);
    }
    if (queues != null && !queues.isEmpty()) {
      builder.clearQueues();
      builder.addAllQueues(queues);
    }
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

  private void initUsers() {
    if (this.users != null) {
      return;
    }
    GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> usersList = p.getUsersList();
    this.users = new HashSet<String>();
    this.users.addAll(usersList);
  }

  private void initQueues() {
    if (this.queues != null) {
      return;
    }
    GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> queuesList = p.getQueuesList();
    this.queues = new HashSet<String>();
    this.queues.addAll(queuesList);
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

  private void initApplicationTags() {
    if (this.applicationTags != null) {
      return;
    }
    GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.applicationTags = new HashSet<String>();
    this.applicationTags.addAll(p.getApplicationTagsList());
  }

  @Override
  public Set<String> getApplicationTags() {
    initApplicationTags();
    return this.applicationTags;
  }

  @Override
  public void setApplicationTags(Set<String> tags) {
    maybeInitBuilder();
    if (tags == null || tags.isEmpty()) {
      builder.clearApplicationTags();
      this.applicationTags = null;
      return;
    }
    // Convert applicationTags to lower case and add
    this.applicationTags = new HashSet<String>();
    for (String tag : tags) {
      this.applicationTags.add(tag.toLowerCase());
    }
  }

  @Override
  public EnumSet<YarnApplicationState> getApplicationStates() {
    initApplicationStates();
    return this.applicationStates;
  }

  private void initScope() {
    if (this.scope != null) {
      return;
    }
    GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.scope = ProtoUtils.convertFromProtoFormat(p.getScope());
  }

  @Override
  public ApplicationsRequestScope getScope() {
    initScope();
    return this.scope;
  }

  public void setScope(ApplicationsRequestScope scope) {
    maybeInitBuilder();
    if (scope == null) {
      builder.clearScope();
    }
    this.scope = scope;
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
  public void setApplicationStates(Set<String> applicationStates) {
    EnumSet<YarnApplicationState> appStates = null;
    for (YarnApplicationState state : YarnApplicationState.values()) {
      if (applicationStates.contains(state.name().toLowerCase())) {
        if (appStates == null) {
          appStates = EnumSet.of(state);
        } else {
          appStates.add(state);
        }
      }
    }
    setApplicationStates(appStates);
  }

  @Override
  public Set<String> getUsers() {
    initUsers();
    return this.users;
  }

  public void setUsers(Set<String> users) {
    maybeInitBuilder();
    if (users == null) {
      builder.clearUsers();
    }
    this.users = users;
  }

  @Override
  public Set<String> getQueues() {
    initQueues();
    return this.queues;
  }

  @Override
  public void setQueues(Set<String> queues) {
    maybeInitBuilder();
    if (queues == null) {
      builder.clearQueues();
    }
    this.queues = queues;
  }

  @Override
  public long getLimit() {
    if (this.limit == Long.MAX_VALUE) {
      GetApplicationsRequestProtoOrBuilder p = viaProto ? proto : builder;
      this.limit = p.hasLimit() ? p.getLimit() : Long.MAX_VALUE;
    }
    return this.limit;
  }

  @Override
  public void setLimit(long limit) {
    maybeInitBuilder();
    this.limit = limit;
  }

  @Override
  public LongRange getStartRange() {
    if (this.start == null) {
      GetApplicationsRequestProtoOrBuilder p = viaProto ? proto: builder;
      if (p.hasStartBegin() || p.hasStartEnd()) {
        long begin = p.hasStartBegin() ? p.getStartBegin() : 0L;
        long end = p.hasStartEnd() ? p.getStartEnd() : Long.MAX_VALUE;
        this.start = new LongRange(begin, end);
      }
    }
    return this.start;
  }

  @Override
  public void setStartRange(LongRange range) {
    this.start = range;
  }

  @Override
  public void setStartRange(long begin, long end)
      throws IllegalArgumentException {
    if (begin > end) {
      throw new IllegalArgumentException("begin > end in range (begin, " +
          "end): (" + begin + ", " + end + ")");
    }
    this.start = new LongRange(begin, end);
  }

  @Override
  public LongRange getFinishRange() {
    if (this.finish == null) {
      GetApplicationsRequestProtoOrBuilder p = viaProto ? proto: builder;
      if (p.hasFinishBegin() || p.hasFinishEnd()) {
        long begin = p.hasFinishBegin() ? p.getFinishBegin() : 0L;
        long end = p.hasFinishEnd() ? p.getFinishEnd() : Long.MAX_VALUE;
        this.finish = new LongRange(begin, end);
      }
    }
    return this.finish;
  }

  @Override
  public void setFinishRange(LongRange range) {
    this.finish = range;
  }

  @Override
  public void setFinishRange(long begin, long end) {
    if (begin > end) {
      throw new IllegalArgumentException("begin > end in range (begin, " +
          "end): (" + begin + ", " + end + ")");
    }
    this.finish = new LongRange(begin, end);
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
