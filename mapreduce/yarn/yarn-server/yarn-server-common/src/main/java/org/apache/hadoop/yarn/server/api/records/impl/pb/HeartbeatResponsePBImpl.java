package org.apache.hadoop.yarn.server.api.records.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.HeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.HeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;


    
public class HeartbeatResponsePBImpl extends ProtoBase<HeartbeatResponseProto> implements HeartbeatResponse {
  HeartbeatResponseProto proto = HeartbeatResponseProto.getDefaultInstance();
  HeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<Container> containersToCleanup = null;
  
  private List<ApplicationId> applicationsToCleanup = null;
  
  
  public HeartbeatResponsePBImpl() {
    builder = HeartbeatResponseProto.newBuilder();
  }

  public HeartbeatResponsePBImpl(HeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public HeartbeatResponseProto getProto() {

      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containersToCleanup != null) {
      addContainersToCleanupToProto();
    }
    if (this.applicationsToCleanup != null) {
      addApplicationsToCleanupToProto();
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
      builder = HeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getResponseId() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }
  @Override
  public boolean getReboot() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getReboot());
  }

  @Override
  public void setReboot(boolean reboot) {
    maybeInitBuilder();
    builder.setReboot((reboot));
  }
  @Override
  public List<Container> getContainersToCleanupList() {
    initContainersToCleanup();
    return this.containersToCleanup;
  }
  @Override
  public Container getContainerToCleanup(int index) {
    initContainersToCleanup();
    return this.containersToCleanup.get(index);
  }
  @Override
  public int getContainersToCleanupCount() {
    initContainersToCleanup();
    return this.containersToCleanup.size();
  }
  
  private void initContainersToCleanup() {
    if (this.containersToCleanup != null) {
      return;
    }
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getContainersToCleanupList();
    this.containersToCleanup = new ArrayList<Container>();

    for (ContainerProto c : list) {
      this.containersToCleanup.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllContainersToCleanup(final List<Container> containersToCleanup) {
    if (containersToCleanup == null)
      return;
    initContainersToCleanup();
    this.containersToCleanup.addAll(containersToCleanup);
  }
  
  private void addContainersToCleanupToProto() {
    maybeInitBuilder();
    builder.clearContainersToCleanup();
    if (containersToCleanup == null)
      return;
    Iterable<ContainerProto> iterable = new Iterable<ContainerProto>() {
      @Override
      public Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {

          Iterator<Container> iter = containersToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainersToCleanup(iterable);
  }
  @Override
  public void addContainerToCleanup(Container containersToCleanup) {
    initContainersToCleanup();
    this.containersToCleanup.add(containersToCleanup);
  }
  @Override
  public void removeContainerToCleanup(int index) {
    initContainersToCleanup();
    this.containersToCleanup.remove(index);
  }
  @Override
  public void clearContainersToCleanup() {
    initContainersToCleanup();
    this.containersToCleanup.clear();
  }
  @Override
  public List<ApplicationId> getApplicationsToCleanupList() {
    initApplicationsToCleanup();
    return this.applicationsToCleanup;
  }
  @Override
  public ApplicationId getApplicationsToCleanup(int index) {
    initApplicationsToCleanup();
    return this.applicationsToCleanup.get(index);
  }
  @Override
  public int getApplicationsToCleanupCount() {
    initApplicationsToCleanup();
    return this.applicationsToCleanup.size();
  }
  
  private void initApplicationsToCleanup() {
    if (this.applicationsToCleanup != null) {
      return;
    }
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdProto> list = p.getApplicationsToCleanupList();
    this.applicationsToCleanup = new ArrayList<ApplicationId>();

    for (ApplicationIdProto c : list) {
      this.applicationsToCleanup.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllApplicationsToCleanup(final List<ApplicationId> applicationsToCleanup) {
    if (applicationsToCleanup == null)
      return;
    initApplicationsToCleanup();
    this.applicationsToCleanup.addAll(applicationsToCleanup);
  }
  
  private void addApplicationsToCleanupToProto() {
    maybeInitBuilder();
    builder.clearApplicationsToCleanup();
    if (applicationsToCleanup == null)
      return;
    Iterable<ApplicationIdProto> iterable = new Iterable<ApplicationIdProto>() {
      @Override
      public Iterator<ApplicationIdProto> iterator() {
        return new Iterator<ApplicationIdProto>() {

          Iterator<ApplicationId> iter = applicationsToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ApplicationIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllApplicationsToCleanup(iterable);
  }
  @Override
  public void addApplicationToCleanup(ApplicationId applicationsToCleanup) {
    initApplicationsToCleanup();
    this.applicationsToCleanup.add(applicationsToCleanup);
  }
  @Override
  public void removeApplicationToCleanup(int index) {
    initApplicationsToCleanup();
    this.applicationsToCleanup.remove(index);
  }
  @Override
  public void clearApplicationsToCleanup() {
    initApplicationsToCleanup();
    this.applicationsToCleanup.clear();
  }

  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }



}  
