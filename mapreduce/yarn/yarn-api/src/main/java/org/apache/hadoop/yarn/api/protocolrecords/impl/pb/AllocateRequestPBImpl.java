package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProtoOrBuilder;


    
public class AllocateRequestPBImpl extends ProtoBase<AllocateRequestProto> implements AllocateRequest {
  AllocateRequestProto proto = AllocateRequestProto.getDefaultInstance();
  AllocateRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationStatus applicationStatus = null;
  private List<ResourceRequest> ask = null;
  private List<Container> release = null;
  
  
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

  private void mergeLocalToBuilder() {
    if (this.applicationStatus != null) {
      builder.setApplicationStatus(convertToProtoFormat(this.applicationStatus));
    }
    if (this.ask != null) {
      addAsksToProto();
    }
    if (this.release != null) {
      addReleasesToProto();
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
  public ApplicationStatus getApplicationStatus() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationStatus != null) {
      return this.applicationStatus;
    }
    if (!p.hasApplicationStatus()) {
      return null;
    }
    this.applicationStatus = convertFromProtoFormat(p.getApplicationStatus());
    return this.applicationStatus;
  }

  @Override
  public void setApplicationStatus(ApplicationStatus applicationStatus) {
    maybeInitBuilder();
    if (applicationStatus == null) 
      builder.clearApplicationStatus();
    this.applicationStatus = applicationStatus;
  }
  @Override
  public List<ResourceRequest> getAskList() {
    initAsks();
    return this.ask;
  }
  @Override
  public ResourceRequest getAsk(int index) {
    initAsks();
    return this.ask.get(index);
  }
  @Override
  public int getAskCount() {
    initAsks();
    return this.ask.size();
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
  
  @Override
  public void addAllAsks(final List<ResourceRequest> ask) {
    if (ask == null)
      return;
    initAsks();
    this.ask.addAll(ask);
  }
  
  private void addAsksToProto() {
    maybeInitBuilder();
    builder.clearAsk();
    if (ask == null)
      return;
    Iterable<ResourceRequestProto> iterable = new Iterable<ResourceRequestProto>() {
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
  @Override
  public void addAsk(ResourceRequest ask) {
    initAsks();
    this.ask.add(ask);
  }
  @Override
  public void removeAsk(int index) {
    initAsks();
    this.ask.remove(index);
  }
  @Override
  public void clearAsks() {
    initAsks();
    this.ask.clear();
  }
  @Override
  public List<Container> getReleaseList() {
    initReleases();
    return this.release;
  }
  @Override
  public Container getRelease(int index) {
    initReleases();
    return this.release.get(index);
  }
  @Override
  public int getReleaseCount() {
    initReleases();
    return this.release.size();
  }
  
  private void initReleases() {
    if (this.release != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getReleaseList();
    this.release = new ArrayList<Container>();

    for (ContainerProto c : list) {
      this.release.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllReleases(final List<Container> release) {
    if (release == null)
      return;
    initReleases();
    this.release.addAll(release);
  }
  
  private void addReleasesToProto() {
    maybeInitBuilder();
    builder.clearRelease();
    if (release == null)
      return;
    Iterable<ContainerProto> iterable = new Iterable<ContainerProto>() {
      @Override
      public Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {

          Iterator<Container> iter = release.iterator();

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
    builder.addAllRelease(iterable);
  }
  @Override
  public void addRelease(Container release) {
    initReleases();
    this.release.add(release);
  }
  @Override
  public void removeRelease(int index) {
    initReleases();
    this.release.remove(index);
  }
  @Override
  public void clearReleases() {
    initReleases();
    this.release.clear();
  }

  private ApplicationStatusPBImpl convertFromProtoFormat(ApplicationStatusProto p) {
    return new ApplicationStatusPBImpl(p);
  }

  private ApplicationStatusProto convertToProtoFormat(ApplicationStatus t) {
    return ((ApplicationStatusPBImpl)t).getProto();
  }

  private ResourceRequestPBImpl convertFromProtoFormat(ResourceRequestProto p) {
    return new ResourceRequestPBImpl(p);
  }

  private ResourceRequestProto convertToProtoFormat(ResourceRequest t) {
    return ((ResourceRequestPBImpl)t).getProto();
  }

  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl)t).getProto();
  }



}  
