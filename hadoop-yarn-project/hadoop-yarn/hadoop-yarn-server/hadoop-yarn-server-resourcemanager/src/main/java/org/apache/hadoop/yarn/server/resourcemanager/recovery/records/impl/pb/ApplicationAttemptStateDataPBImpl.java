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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.RMAppAttemptStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

import com.google.protobuf.TextFormat;

public class ApplicationAttemptStateDataPBImpl extends
    ApplicationAttemptStateData {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationAttemptStateDataPBImpl.class);
  ApplicationAttemptStateDataProto proto = 
      ApplicationAttemptStateDataProto.getDefaultInstance();
  ApplicationAttemptStateDataProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId attemptId = null;
  private Container masterContainer = null;
  private ByteBuffer appAttemptTokens = null;

  private Map<String, Long> resourceSecondsMap;
  private Map<String, Long> preemptedResourceSecondsMap;

  public ApplicationAttemptStateDataPBImpl() {
    builder = ApplicationAttemptStateDataProto.newBuilder();
  }

  public ApplicationAttemptStateDataPBImpl(
      ApplicationAttemptStateDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationAttemptStateDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.attemptId != null) {
      builder.setAttemptId(((ApplicationAttemptIdPBImpl)attemptId).getProto());
    }
    if(this.masterContainer != null) {
      builder.setMasterContainer(((ContainerPBImpl)masterContainer).getProto());
    }
    if(this.appAttemptTokens != null) {
      builder.setAppAttemptTokens(ProtoUtils.convertToProtoFormat(
          this.appAttemptTokens));
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
      builder = ApplicationAttemptStateDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationAttemptId getAttemptId() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(attemptId != null) {
      return attemptId;
    }
    if (!p.hasAttemptId()) {
      return null;
    }
    attemptId = new ApplicationAttemptIdPBImpl(p.getAttemptId());
    return attemptId;
  }

  @Override
  public void setAttemptId(ApplicationAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null) {
      builder.clearAttemptId();
    }
    this.attemptId = attemptId;
  }

  @Override
  public Container getMasterContainer() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(masterContainer != null) {
      return masterContainer;
    }
    if (!p.hasMasterContainer()) {
      return null;
    }
    masterContainer = new ContainerPBImpl(p.getMasterContainer());
    return masterContainer;
  }

  @Override
  public void setMasterContainer(Container container) {
    maybeInitBuilder();
    if (container == null) {
      builder.clearMasterContainer();
    }
    this.masterContainer = container;
  }

  @Override
  public Credentials getAppAttemptTokens() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(appAttemptTokens != null) {
      return convertCredentialsFromByteBuffer(appAttemptTokens);
    }
    if(!p.hasAppAttemptTokens()) {
      return null;
    }
    this.appAttemptTokens = ProtoUtils.convertFromProtoFormat(
        p.getAppAttemptTokens());
    return convertCredentialsFromByteBuffer(appAttemptTokens);
  }

  @Override
  public void setAppAttemptTokens(Credentials attemptTokens) {
    maybeInitBuilder();
    if(attemptTokens == null) {
      builder.clearAppAttemptTokens();
      return;
    }
    this.appAttemptTokens = convertCredentialsToByteBuffer(attemptTokens);
  }

  @Override
  public RMAppAttemptState getState() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppAttemptState()) {
      return null;
    }
    return convertFromProtoFormat(p.getAppAttemptState());
  }

  @Override
  public void setState(RMAppAttemptState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearAppAttemptState();
      return;
    }
    builder.setAppAttemptState(convertToProtoFormat(state));
  }

  @Override
  public String getFinalTrackingUrl() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalTrackingUrl()) {
      return null;
    }
    return p.getFinalTrackingUrl();
  }

  @Override
  public void setFinalTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearFinalTrackingUrl();
      return;
    }
    builder.setFinalTrackingUrl(url);
  }

  @Override
  public String getDiagnostics() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public long getStartTime() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  @Override
  public long getMemorySeconds() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMemorySeconds();
  }
 
  @Override
  public long getVcoreSeconds() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getVcoreSeconds();
  }

  @Override
  public void setMemorySeconds(long memorySeconds) {
    maybeInitBuilder();
    builder.setMemorySeconds(memorySeconds);
  }
 
  @Override
  public void setVcoreSeconds(long vcoreSeconds) {
    maybeInitBuilder();
    builder.setVcoreSeconds(vcoreSeconds);
  }

  @Override
  public long getPreemptedMemorySeconds() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPreemptedMemorySeconds();
  }

  @Override
  public long getPreemptedVcoreSeconds() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPreemptedVcoreSeconds();
  }

  @Override
  public void setPreemptedMemorySeconds(long memorySeconds) {
    maybeInitBuilder();
    builder.setPreemptedMemorySeconds(memorySeconds);
  }

  @Override
  public void setPreemptedVcoreSeconds(long vcoreSeconds) {
    maybeInitBuilder();
    builder.setPreemptedVcoreSeconds(vcoreSeconds);
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalApplicationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getFinalApplicationStatus());
  }

  @Override
  public void setFinalApplicationStatus(FinalApplicationStatus finishState) {
    maybeInitBuilder();
    if (finishState == null) {
      builder.clearFinalApplicationStatus();
      return;
    }
    builder.setFinalApplicationStatus(convertToProtoFormat(finishState));
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public int getAMContainerExitStatus() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAmContainerExitStatus();
  }

  @Override
  public void setAMContainerExitStatus(int exitStatus) {
    maybeInitBuilder();
    builder.setAmContainerExitStatus(exitStatus);
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
  
  private static String RM_APP_ATTEMPT_PREFIX = "RMATTEMPT_";
  public static RMAppAttemptStateProto convertToProtoFormat(RMAppAttemptState e) {
    return RMAppAttemptStateProto.valueOf(RM_APP_ATTEMPT_PREFIX + e.name());
  }
  public static RMAppAttemptState convertFromProtoFormat(RMAppAttemptStateProto e) {
    return RMAppAttemptState.valueOf(e.name().replace(RM_APP_ATTEMPT_PREFIX, ""));
  }

  private FinalApplicationStatusProto convertToProtoFormat(FinalApplicationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }
  private FinalApplicationStatus convertFromProtoFormat(FinalApplicationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  @Override
  public long getFinishTime() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  private static ByteBuffer convertCredentialsToByteBuffer(
      Credentials credentials) {
    ByteBuffer appAttemptTokens = null;
    DataOutputBuffer dob = new DataOutputBuffer();
    try {
      if (credentials != null) {
        credentials.writeTokenStorageToStream(dob);
        appAttemptTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      }
      return appAttemptTokens;
    } catch (IOException e) {
      LOG.error("Failed to convert Credentials to ByteBuffer.");
      assert false;
      return null;
    } finally {
      IOUtils.closeStream(dob);
    }
  }

  private static Credentials convertCredentialsFromByteBuffer(
      ByteBuffer appAttemptTokens) {
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    try {
      Credentials credentials = null;
      if (appAttemptTokens != null) {
        credentials = new Credentials();
        appAttemptTokens.rewind();
        dibb.reset(appAttemptTokens);
        credentials.readTokenStorageStream(dibb);
      }
      return credentials;
    } catch (IOException e) {
      LOG.error("Failed to convert Credentials from ByteBuffer.");
      assert false;
      return null;
    } finally {
      IOUtils.closeStream(dibb);
    }
  }

  @Override
  public Map<String, Long> getResourceSecondsMap() {
    if (this.resourceSecondsMap != null) {
      return this.resourceSecondsMap;
    }
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    this.resourceSecondsMap = ProtoUtils.convertStringLongMapProtoListToMap(
        p.getApplicationResourceUsageMapList());
    return this.resourceSecondsMap;
  }

  @Override
  public void setResourceSecondsMap(Map<String, Long> resourceSecondsMap) {
    maybeInitBuilder();
    builder.clearApplicationResourceUsageMap();
    this.resourceSecondsMap = resourceSecondsMap;
    if (resourceSecondsMap != null) {
      builder.addAllApplicationResourceUsageMap(
          ProtoUtils.convertMapToStringLongMapProtoList(resourceSecondsMap));
    }
  }

  @Override
  public Map<String, Long> getPreemptedResourceSecondsMap() {
    if (this.preemptedResourceSecondsMap != null) {
      return this.preemptedResourceSecondsMap;
    }
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    this.preemptedResourceSecondsMap = ProtoUtils
        .convertStringLongMapProtoListToMap(
            p.getApplicationResourceUsageMapList());
    return this.preemptedResourceSecondsMap;
  }

  @Override
  public void setPreemptedResourceSecondsMap(
      Map<String, Long> preemptedResourceSecondsMap) {
    maybeInitBuilder();
    builder.clearPreemptedResourceUsageMap();
    this.preemptedResourceSecondsMap = preemptedResourceSecondsMap;
    if (preemptedResourceSecondsMap != null) {
      builder.addAllPreemptedResourceUsageMap(ProtoUtils
          .convertMapToStringLongMapProtoList(preemptedResourceSecondsMap));
    }
  }
}
