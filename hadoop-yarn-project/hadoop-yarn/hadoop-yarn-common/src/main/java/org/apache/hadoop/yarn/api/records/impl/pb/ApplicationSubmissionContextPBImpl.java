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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class ApplicationSubmissionContextPBImpl 
extends ApplicationSubmissionContext {
  ApplicationSubmissionContextProto proto = 
      ApplicationSubmissionContextProto.getDefaultInstance();
  ApplicationSubmissionContextProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  private Priority priority = null;
  private ContainerLaunchContext amContainer = null;
  private Resource resource = null;
  private Set<String> applicationTags = null;
  private List<ResourceRequest> amResourceRequests = null;
  private LogAggregationContext logAggregationContext = null;
  private ReservationId reservationId = null;
  private Map<ApplicationTimeoutType, Long> applicationTimeouts = null;
  private Map<String, String> schedulingProperties = null;

  public ApplicationSubmissionContextPBImpl() {
    builder = ApplicationSubmissionContextProto.newBuilder();
  }

  public ApplicationSubmissionContextPBImpl(
      ApplicationSubmissionContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationSubmissionContextProto getProto() {
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
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.amContainer != null) {
      builder.setAmContainerSpec(convertToProtoFormat(this.amContainer));
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.applicationTags != null && !this.applicationTags.isEmpty()) {
      builder.clearApplicationTags();
      builder.addAllApplicationTags(this.applicationTags);
    }
    if (this.amResourceRequests != null) {
      builder.clearAmContainerResourceRequest();
      builder.addAllAmContainerResourceRequest(
          convertToProtoFormat(this.amResourceRequests));
    }
    if (this.logAggregationContext != null) {
      builder.setLogAggregationContext(
          convertToProtoFormat(this.logAggregationContext));
    }
    if (this.reservationId != null) {
      builder.setReservationId(convertToProtoFormat(this.reservationId));
    }
    if (this.applicationTimeouts != null) {
      addApplicationTimeouts();
    }
    if (this.schedulingProperties != null) {
      addApplicationSchedulingProperties();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationSubmissionContextProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Priority getPriority() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }
  
  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearPriority();
    this.priority = priority;
  }
  
  @Override
  public ApplicationId getApplicationId() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasApplicationId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }
  
  @Override
  public String getApplicationName() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationName()) {
      return null;
    }
    return (p.getApplicationName());
  }

  @Override
  public void setApplicationName(String applicationName) {
    maybeInitBuilder();
    if (applicationName == null) {
      builder.clearApplicationName();
      return;
    }
    builder.setApplicationName((applicationName));
  }

  @Override
  public String getQueue() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return (p.getQueue());
  }

  @Override
  public String getApplicationType() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationType()) {
      return null;
    }
    return (p.getApplicationType());
  }

  private void initApplicationTags() {
    if (this.applicationTags != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    this.applicationTags = new HashSet<String>();
    this.applicationTags.addAll(p.getApplicationTagsList());
  }

  @Override
  public Set<String> getApplicationTags() {
    initApplicationTags();
    return this.applicationTags;
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue((queue));
  }
  
  @Override
  public void setApplicationType(String applicationType) {
    maybeInitBuilder();
    if (applicationType == null) {
      builder.clearApplicationType();
      return;
    }
    builder.setApplicationType((applicationType));
  }

  private void checkTags(Set<String> tags) {
    if (tags.size() > YarnConfiguration.APPLICATION_MAX_TAGS) {
      throw new IllegalArgumentException("Too many applicationTags, a maximum of only "
          + YarnConfiguration.APPLICATION_MAX_TAGS + " are allowed!");
    }
    for (String tag : tags) {
      if (tag.length() > YarnConfiguration.APPLICATION_MAX_TAG_LENGTH) {
        throw new IllegalArgumentException("Tag " + tag + " is too long, " +
            "maximum allowed length of a tag is " +
            YarnConfiguration.APPLICATION_MAX_TAG_LENGTH);
      }
      if (!org.apache.commons.lang3.StringUtils.isAsciiPrintable(tag)) {
        throw new IllegalArgumentException("A tag can only have ASCII " +
            "characters! Invalid tag - " + tag);
      }
    }
  }

  @Override
  public void setApplicationTags(Set<String> tags) {
    maybeInitBuilder();
    if (tags == null || tags.isEmpty()) {
      builder.clearApplicationTags();
      this.applicationTags = null;
      return;
    }
    checkTags(tags);
    // Convert applicationTags to lower case and add
    this.applicationTags = new HashSet<String>();
    for (String tag : tags) {
      this.applicationTags.add(StringUtils.toLowerCase(tag));
    }
  }

  @Override
  public ContainerLaunchContext getAMContainerSpec() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.amContainer != null) {
      return amContainer;
    } // Else via proto
    if (!p.hasAmContainerSpec()) {
      return null;
    }
    amContainer = convertFromProtoFormat(p.getAmContainerSpec());
    return amContainer;
  }

  @Override
  public void setAMContainerSpec(ContainerLaunchContext amContainer) {
    maybeInitBuilder();
    if (amContainer == null) {
      builder.clearAmContainerSpec();
    }
    this.amContainer = amContainer;
  }
  
  @Override
  public boolean getUnmanagedAM() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    return p.getUnmanagedAm();
  }
  
  @Override
  public void setUnmanagedAM(boolean value) {
    maybeInitBuilder();
    builder.setUnmanagedAm(value);
  }
  
  @Override
  public boolean getCancelTokensWhenComplete() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    //There is a default so cancelTokens should never be null
    return p.getCancelTokensWhenComplete();
  }
  
  @Override
  public void setCancelTokensWhenComplete(boolean cancel) {
    maybeInitBuilder();
    builder.setCancelTokensWhenComplete(cancel);
  }

  @Override
  public int getMaxAppAttempts() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMaxAppAttempts();
  }

  @Override
  public void setMaxAppAttempts(int maxAppAttempts) {
    maybeInitBuilder();
    builder.setMaxAppAttempts(maxAppAttempts);
  }

  @Override
  public Resource getResource() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) {
      builder.clearResource();
    }
    this.resource = resource;
  }

  @Override
  public ReservationId getReservationID() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (reservationId != null) {
      return reservationId;
    }
    if (!p.hasReservationId()) {
      return null;
    }
    reservationId = convertFromProtoFormat(p.getReservationId());
    return reservationId;
  }

  @Override
  public void setReservationID(ReservationId reservationID) {
    maybeInitBuilder();
    if (reservationID == null) {
      builder.clearReservationId();
      return;
    }
    this.reservationId = reservationID;
  }

  @Override
  public void
      setKeepContainersAcrossApplicationAttempts(boolean keepContainers) {
    maybeInitBuilder();
    builder.setKeepContainersAcrossApplicationAttempts(keepContainers);
  }

  @Override
  public boolean getKeepContainersAcrossApplicationAttempts() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    return p.getKeepContainersAcrossApplicationAttempts();
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  private List<ResourceRequest> convertFromProtoFormat(
      List<ResourceRequestProto> ps) {
    List<ResourceRequest> rs = new ArrayList<>();
    for (ResourceRequestProto p : ps) {
      rs.add(new ResourceRequestPBImpl(p));
    }
    return rs;
  }

  private List<ResourceRequestProto> convertToProtoFormat(
      List<ResourceRequest> ts) {
    List<ResourceRequestProto> rs = new ArrayList<>(ts.size());
    for (ResourceRequest t : ts) {
      rs.add(((ResourceRequestPBImpl)t).getProto());
    }
    return rs;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  private ContainerLaunchContextPBImpl convertFromProtoFormat(
      ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(
      ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  @Override
  public String getNodeLabelExpression() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeLabelExpression()) {
      return null;
    }
    return p.getNodeLabelExpression();
  }

  @Override
  public void setNodeLabelExpression(String labelExpression) {
    maybeInitBuilder();
    if (labelExpression == null) {
      builder.clearNodeLabelExpression();
      return;
    }
    builder.setNodeLabelExpression(labelExpression);
  }
  
  @Override
  @Deprecated
  public ResourceRequest getAMContainerResourceRequest() {
    List<ResourceRequest> reqs = getAMContainerResourceRequests();
    if (reqs == null || reqs.isEmpty()) {
      return null;
    }
    return getAMContainerResourceRequests().get(0);
  }

  @Override
  public List<ResourceRequest> getAMContainerResourceRequests() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.amResourceRequests != null) {
      return amResourceRequests;
    } // Else via proto
    if (p.getAmContainerResourceRequestCount() == 0) {
      return null;
    }
    amResourceRequests =
        convertFromProtoFormat(p.getAmContainerResourceRequestList());
    return amResourceRequests;
  }

  @Override
  @Deprecated
  public void setAMContainerResourceRequest(ResourceRequest request) {
    maybeInitBuilder();
    if (request == null) {
      builder.clearAmContainerResourceRequest();
    }
    this.amResourceRequests = Collections.singletonList(request);
  }

  @Override
  public void setAMContainerResourceRequests(List<ResourceRequest> requests) {
    maybeInitBuilder();
    if (requests == null) {
      builder.clearAmContainerResourceRequest();
    }
    this.amResourceRequests = requests;
  }

  @Override
  public long getAttemptFailuresValidityInterval() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAttemptFailuresValidityInterval();
  }

  @Override
  public void setAttemptFailuresValidityInterval(
      long attemptFailuresValidityInterval) {
    maybeInitBuilder();
    builder.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
  }

  private LogAggregationContextPBImpl convertFromProtoFormat(
      LogAggregationContextProto p) {
    return new LogAggregationContextPBImpl(p);
  }

  private LogAggregationContextProto convertToProtoFormat(
      LogAggregationContext t) {
    return ((LogAggregationContextPBImpl) t).getProto();
  }

  @Override
  public LogAggregationContext getLogAggregationContext() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.logAggregationContext != null) {
      return this.logAggregationContext;
    } // Else via proto
    if (!p.hasLogAggregationContext()) {
      return null;
    }
    logAggregationContext = convertFromProtoFormat(p.getLogAggregationContext());
    return logAggregationContext;
  }

  @Override
  public void setLogAggregationContext(
      LogAggregationContext logAggregationContext) {
    maybeInitBuilder();
    if (logAggregationContext == null)
      builder.clearLogAggregationContext();
    this.logAggregationContext = logAggregationContext;
  }

  private ReservationIdPBImpl convertFromProtoFormat(ReservationIdProto p) {
    return new ReservationIdPBImpl(p);
  }

  private ReservationIdProto convertToProtoFormat(ReservationId t) {
    return ((ReservationIdPBImpl) t).getProto();
  }

  @Override
  public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
    initApplicationTimeout();
    return this.applicationTimeouts;
  }

  private void initApplicationTimeout() {
    if (this.applicationTimeouts != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationTimeoutMapProto> lists = p.getApplicationTimeoutsList();
    this.applicationTimeouts =
        new HashMap<ApplicationTimeoutType, Long>(lists.size());
    for (ApplicationTimeoutMapProto timeoutProto : lists) {
      this.applicationTimeouts.put(
          ProtoUtils
              .convertFromProtoFormat(timeoutProto.getApplicationTimeoutType()),
          timeoutProto.getTimeout());
    }
  }

  @Override
  public void setApplicationTimeouts(
      Map<ApplicationTimeoutType, Long> appTimeouts) {
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
    Iterable<? extends ApplicationTimeoutMapProto> values =
        new Iterable<ApplicationTimeoutMapProto>() {

          @Override
          public Iterator<ApplicationTimeoutMapProto> iterator() {
            return new Iterator<ApplicationTimeoutMapProto>() {
              private Iterator<ApplicationTimeoutType> iterator =
                  applicationTimeouts.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public ApplicationTimeoutMapProto next() {
                ApplicationTimeoutType key = iterator.next();
                return ApplicationTimeoutMapProto.newBuilder()
                    .setTimeout(applicationTimeouts.get(key))
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

  private void addApplicationSchedulingProperties() {
    maybeInitBuilder();
    builder.clearApplicationSchedulingProperties();
    if (this.schedulingProperties == null) {
      return;
    }
    Iterable<? extends StringStringMapProto> values =
        new Iterable<StringStringMapProto>() {

      @Override
      public Iterator<StringStringMapProto> iterator() {
        return new Iterator<StringStringMapProto>() {
          private Iterator<String> iterator = schedulingProperties.keySet()
              .iterator();

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public StringStringMapProto next() {
            String key = iterator.next();
            return StringStringMapProto.newBuilder()
                .setValue(schedulingProperties.get(key)).setKey(key).build();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    this.builder.addAllApplicationSchedulingProperties(values);
  }

  private void initApplicationSchedulingProperties() {
    if (this.schedulingProperties != null) {
      return;
    }
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    List<StringStringMapProto> properties = p
        .getApplicationSchedulingPropertiesList();
    this.schedulingProperties = new HashMap<String, String>(properties.size());
    for (StringStringMapProto envProto : properties) {
      this.schedulingProperties.put(envProto.getKey(), envProto.getValue());
    }
  }

  @Override
  public Map<String, String> getApplicationSchedulingPropertiesMap() {
    initApplicationSchedulingProperties();
    return this.schedulingProperties;
  }

  @Override
  public void setApplicationSchedulingPropertiesMap(
      Map<String, String> schedulingPropertyMap) {
    if (schedulingPropertyMap == null) {
      return;
    }
    initApplicationSchedulingProperties();
    this.schedulingProperties.clear();
    this.schedulingProperties.putAll(schedulingPropertyMap);
  }
}
