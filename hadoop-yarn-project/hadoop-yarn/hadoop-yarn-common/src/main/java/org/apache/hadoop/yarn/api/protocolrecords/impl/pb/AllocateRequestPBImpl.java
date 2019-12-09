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
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceBlacklistRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SchedulingRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.UpdateContainerRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceBlacklistRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SchedulingRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class AllocateRequestPBImpl extends AllocateRequest {
  AllocateRequestProto proto = AllocateRequestProto.getDefaultInstance();
  AllocateRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<ResourceRequest> ask = null;
  private List<ContainerId> release = null;
  private List<UpdateContainerRequest> updateRequests = null;
  private List<SchedulingRequest> schedulingRequests = null;
  private ResourceBlacklistRequest blacklistRequest = null;
  private String trackingUrl = null;
  
  public AllocateRequestPBImpl() {
    builder = AllocateRequestProto.newBuilder();
  }

  public AllocateRequestPBImpl(AllocateRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public AllocateRequestProto getProto() {
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
    if (this.ask != null) {
      addAsksToProto();
    }
    if (this.release != null) {
      addReleasesToProto();
    }
    if (this.updateRequests != null) {
      addUpdateRequestsToProto();
    }
    if (this.schedulingRequests != null) {
      addSchedulingRequestsToProto();
    }
    if (this.blacklistRequest != null) {
      builder.setBlacklistRequest(convertToProtoFormat(this.blacklistRequest));
    }
    if (this.trackingUrl != null) {
      builder.setTrackingUrl(this.trackingUrl);
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
      builder = AllocateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getResponseId() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getResponseId();
  }

  @Override
  public void setResponseId(int id) {
    maybeInitBuilder();
    builder.setResponseId(id);
  }

  @Override
  public float getProgress() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getProgress();
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress(progress);
  }

  @Override
  public List<ResourceRequest> getAskList() {
    initAsks();
    return this.ask;
  }
  
  @Override
  public void setAskList(final List<ResourceRequest> resourceRequests) {
    if(resourceRequests == null) {
      return;
    }
    initAsks();
    this.ask.clear();
    this.ask.addAll(resourceRequests);
  }
  
  @Override
  public List<UpdateContainerRequest> getUpdateRequests() {
    initUpdateRequests();
    return this.updateRequests;
  }

  @Override
  public void setUpdateRequests(List<UpdateContainerRequest> updateRequests) {
    if (updateRequests == null) {
      return;
    }
    initUpdateRequests();
    this.updateRequests.clear();
    this.updateRequests.addAll(updateRequests);
  }

  @Override
  public List<SchedulingRequest> getSchedulingRequests() {
    initSchedulingRequests();
    return this.schedulingRequests;
  }

  @Override
  public void setSchedulingRequests(
      List<SchedulingRequest> schedulingRequests) {
    if (schedulingRequests == null) {
      builder.clearSchedulingRequests();
      return;
    }
    initSchedulingRequests();
    this.schedulingRequests.clear();
    this.schedulingRequests.addAll(schedulingRequests);
  }

  @Override
  public ResourceBlacklistRequest getResourceBlacklistRequest() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.blacklistRequest != null) {
      return this.blacklistRequest;
    }
    if (!p.hasBlacklistRequest()) {
      return null;
    }
    this.blacklistRequest = convertFromProtoFormat(p.getBlacklistRequest());
    return this.blacklistRequest;
  }

  @Override
  public void setResourceBlacklistRequest(ResourceBlacklistRequest blacklistRequest) {
    maybeInitBuilder();
    if (blacklistRequest == null) {
      builder.clearBlacklistRequest();
    }
    this.blacklistRequest = blacklistRequest;
  }

  private void initAsks() {
    if (this.ask != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceRequestProto> list = p.getAskList();
    this.ask = new ArrayList<ResourceRequest>();

    for (ResourceRequestProto c : list) {
      this.ask.add(convertFromProtoFormat(c));
    }
  }
  
  private void addAsksToProto() {
    maybeInitBuilder();
    builder.clearAsk();
    if (ask == null)
      return;
    Iterable<ResourceRequestProto> iterable =
        new Iterable<ResourceRequestProto>() {
      @Override
      public Iterator<ResourceRequestProto> iterator() {
        return new Iterator<ResourceRequestProto>() {

          Iterator<ResourceRequest> iter = ask.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceRequestProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllAsk(iterable);
  }
  
  private void initUpdateRequests() {
    if (this.updateRequests != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<UpdateContainerRequestProto> list =
        p.getUpdateRequestsList();
    this.updateRequests = new ArrayList<>();

    for (UpdateContainerRequestProto c : list) {
      this.updateRequests.add(convertFromProtoFormat(c));
    }
  }

  private void initSchedulingRequests() {
    if (this.schedulingRequests != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<SchedulingRequestProto> list =
        p.getSchedulingRequestsList();
    this.schedulingRequests = new ArrayList<>();

    for (SchedulingRequestProto c : list) {
      this.schedulingRequests.add(convertFromProtoFormat(c));
    }
  }

  private void addUpdateRequestsToProto() {
    maybeInitBuilder();
    builder.clearUpdateRequests();
    if (updateRequests == null) {
      return;
    }
    Iterable<UpdateContainerRequestProto> iterable =
        new Iterable<UpdateContainerRequestProto>() {
          @Override
          public Iterator<UpdateContainerRequestProto> iterator() {
            return new Iterator<UpdateContainerRequestProto>() {

              private Iterator<UpdateContainerRequest> iter =
                  updateRequests.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public UpdateContainerRequestProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };

          }
        };
    builder.addAllUpdateRequests(iterable);
  }

  private void addSchedulingRequestsToProto() {
    maybeInitBuilder();
    builder.clearSchedulingRequests();
    if (schedulingRequests == null) {
      return;
    }
    Iterable<SchedulingRequestProto> iterable =
        new Iterable<SchedulingRequestProto>() {
          @Override
          public Iterator<SchedulingRequestProto> iterator() {
            return new Iterator<SchedulingRequestProto>() {

              private Iterator<SchedulingRequest> iter =
                  schedulingRequests.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public SchedulingRequestProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };

          }
        };
    builder.addAllSchedulingRequests(iterable);
  }
  @Override
  public List<ContainerId> getReleaseList() {
    initReleases();
    return this.release;
  }
  @Override
  public void setReleaseList(List<ContainerId> releaseContainers) {
    if(releaseContainers == null) {
      return;
    }
    initReleases();
    this.release.clear();
    this.release.addAll(releaseContainers);
  }
  
  private void initReleases() {
    if (this.release != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getReleaseList();
    this.release = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.release.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public String getTrackingUrl() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.trackingUrl != null) {
      return this.trackingUrl;
    }
    if (p.hasTrackingUrl()) {
      this.trackingUrl = p.getTrackingUrl();
    }
    return this.trackingUrl;
  }

  @Override
  public void setTrackingUrl(String trackingUrl) {
    maybeInitBuilder();
    if (trackingUrl == null) {
      builder.clearTrackingUrl();
    }
    this.trackingUrl = trackingUrl;
  }

  private void addReleasesToProto() {
    maybeInitBuilder();
    builder.clearRelease();
    if (release == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = release.iterator();

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
    builder.addAllRelease(iterable);
  }

  private ResourceRequestPBImpl convertFromProtoFormat(ResourceRequestProto p) {
    return new ResourceRequestPBImpl(p);
  }

  private ResourceRequestProto convertToProtoFormat(ResourceRequest t) {
    return ((ResourceRequestPBImpl)t).getProto();
  }
  
  private UpdateContainerRequestPBImpl convertFromProtoFormat(
      UpdateContainerRequestProto p) {
    return new UpdateContainerRequestPBImpl(p);
  }

  private UpdateContainerRequestProto convertToProtoFormat(
      UpdateContainerRequest t) {
    return ((UpdateContainerRequestPBImpl) t).getProto();
  }

  private SchedulingRequestPBImpl convertFromProtoFormat(
      SchedulingRequestProto p) {
    return new SchedulingRequestPBImpl(p);
  }

  private SchedulingRequestProto convertToProtoFormat(
      SchedulingRequest t) {
    return ((SchedulingRequestPBImpl) t).getProto();
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  private ResourceBlacklistRequestPBImpl convertFromProtoFormat(ResourceBlacklistRequestProto p) {
    return new ResourceBlacklistRequestPBImpl(p);
  }

  private ResourceBlacklistRequestProto convertToProtoFormat(ResourceBlacklistRequest t) {
    return ((ResourceBlacklistRequestPBImpl)t).getProto();
  }
}  
