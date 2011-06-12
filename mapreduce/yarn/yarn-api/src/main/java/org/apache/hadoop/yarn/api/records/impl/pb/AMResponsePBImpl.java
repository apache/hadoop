package org.apache.hadoop.yarn.api.records.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.AMResponseProto;
import org.apache.hadoop.yarn.proto.YarnProtos.AMResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeManagerInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;


    
public class AMResponsePBImpl extends ProtoBase<AMResponseProto> implements AMResponse {
  AMResponseProto proto = AMResponseProto.getDefaultInstance();
  AMResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  Resource limit;

  private List<Container> containerList = null;
//  private boolean hasLocalContainerList = false;
  
  
  public AMResponsePBImpl() {
    builder = AMResponseProto.newBuilder();
  }

  public AMResponsePBImpl(AMResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public AMResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToBuilder() {
    if (this.containerList != null) {
      addLocalContainersToProto();
    }
    if (this.limit != null) {
      builder.setLimit(convertToProtoFormat(this.limit));
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
      builder = AMResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public boolean getReboot() {
    AMResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getReboot());
  }

  @Override
  public void setReboot(boolean reboot) {
    maybeInitBuilder();
    builder.setReboot((reboot));
  }
  @Override
  public int getResponseId() {
    AMResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }
  @Override
  public Resource getAvailableResources() {
    if (this.limit != null) {
      return this.limit;
    }

    AMResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLimit()) {
      return null;
    }
    this.limit = convertFromProtoFormat(p.getLimit());
    return this.limit;
  }

  @Override
  public void setAvailableResources(Resource limit) {
    maybeInitBuilder();
    if (limit == null)
      builder.clearLimit();
    this.limit = limit;
  }

  @Override
  public List<Container> getContainerList() {
    initLocalContainerList();
    return this.containerList;
  }
  
  @Override
  public Container getContainer(int index) {
    initLocalContainerList();
    return this.containerList.get(index);
  }
  @Override
  public int getContainerCount() {
    initLocalContainerList();
    return this.containerList.size();
  }
  
  //Once this is called. containerList will never be null - untill a getProto is called.
  private void initLocalContainerList() {
    if (this.containerList != null) {
      return;
    }
    AMResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getContainersList();
    containerList = new ArrayList<Container>();

    for (ContainerProto c : list) {
      containerList.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllContainers(final List<Container> containers) {
    if (containers == null) 
      return;
    initLocalContainerList();
    containerList.addAll(containers);
  }

  private void addLocalContainersToProto() {
    maybeInitBuilder();
    builder.clearContainers();
    if (containerList == null)
      return;
    Iterable<ContainerProto> iterable = new Iterable<ContainerProto>() {
      @Override
      public Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {

          Iterator<Container> iter = containerList.iterator();

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
    builder.addAllContainers(iterable);
  }
  
  @Override
  public void addContainer(Container containers) {
    initLocalContainerList();
    if (containers == null) 
      return;
    this.containerList.add(containers);
  }
  
  @Override
  public void removeContainer(int index) {
    initLocalContainerList();
    this.containerList.remove(index);
  }
  @Override
  public void clearContainers() {
    initLocalContainerList();
    this.containerList.clear();
  }

  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource r) {
    return ((ResourcePBImpl) r).getProto();
  }

}  
