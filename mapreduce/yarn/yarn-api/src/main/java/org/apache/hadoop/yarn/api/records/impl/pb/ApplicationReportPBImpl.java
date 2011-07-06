package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ApplicationReportPBImpl extends ProtoBase<ApplicationReportProto> 
implements ApplicationReport {
  ApplicationReportProto proto = ApplicationReportProto.getDefaultInstance();
  ApplicationReportProto.Builder builder = null;
  boolean viaProto = false;

  ApplicationId applicationId;
  ApplicationStatus status;
  Container masterContainer = null;

  public ApplicationReportPBImpl() {
    builder = ApplicationReportProto.newBuilder();
  }
  
  public ApplicationReportPBImpl(ApplicationReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }

    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }
  
  @Override
  public Container getMasterContainer() {
    if (this.masterContainer != null) {
      return this.masterContainer;
    }

    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMasterContainer()) {
      return null;
    }
    this.masterContainer = convertFromProtoFormat(p.getMasterContainer());
    return this.masterContainer;
  }

  @Override
  public String getTrackingUrl() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrackingUrl()) {
      return null;
    }
    return p.getTrackingUrl();
  }

  @Override
  public String getName() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return p.getName();
  }

  @Override
  public String getQueue() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return p.getQueue();
  }

  @Override
  public ApplicationState getState() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public String getHost() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return (p.getHost());
  }

  @Override
  public int getRpcPort() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getRpcPort());
  }

  @Override
  public String getClientToken() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasClientToken()) {
      return null;
    }
    return (p.getClientToken());
  }

  @Override
  public ApplicationStatus getStatus() {
    if (this.status != null) {
      return this.status;
    }

    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStatus()) {
      return null;
    }
    this.status = convertFromProtoFormat(p.getStatus());
    return this.status;
  }

  @Override
  public String getUser() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return p.getUser();
  }


  @Override
  public String getDiagnostics() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearStatus();
    this.applicationId = applicationId;
  }

  @Override
  public void setMasterContainer(Container container) {
    maybeInitBuilder();
    if (container == null)
      builder.clearMasterContainer();
    this.masterContainer = container;
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearTrackingUrl();
      return;
    }
    builder.setTrackingUrl(url);
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);
  }

  @Override
  public void setState(ApplicationState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) {
      builder.clearHost();
      return;
    }
    builder.setHost((host));
  }

  @Override
  public void setRpcPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort((rpcPort));
  }

  @Override
  public void setClientToken(String clientToken) {
    maybeInitBuilder();
    if (clientToken == null) {
      builder.clearClientToken();
      return;
    }
    builder.setClientToken((clientToken));
  }

  @Override
  public void setStatus(ApplicationStatus status) {
    maybeInitBuilder();
    if (status == null)
      builder.clearStatus();
    this.status = status;
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser((user));
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
  public ApplicationReportProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
            builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.status != null
        && !((ApplicationStatusPBImpl) this.status).getProto().equals(
            builder.getStatus())) {
      builder.setStatus(convertToProtoFormat(this.status));
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
      builder = ApplicationReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private ApplicationStatusPBImpl convertFromProtoFormat(
      ApplicationStatusProto p) {
    return new ApplicationStatusPBImpl(p);
  }

  private ApplicationStatusProto convertToProtoFormat(ApplicationStatus t) {
    return ((ApplicationStatusPBImpl) t).getProto();
  }

  private ApplicationState convertFromProtoFormat(ApplicationStateProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private ApplicationStateProto convertToProtoFormat(ApplicationState s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }

  private ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl) t).getProto();
  }

  private Container convertFromProtoFormat(ContainerProto c) {
    return new ContainerPBImpl(c);
  }

}
