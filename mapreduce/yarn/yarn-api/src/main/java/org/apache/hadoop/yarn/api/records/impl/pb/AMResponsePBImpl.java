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
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;


    
public class AMResponsePBImpl extends ProtoBase<AMResponseProto> implements AMResponse {
  AMResponseProto proto = AMResponseProto.getDefaultInstance();
  AMResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  Resource limit;

  private List<Container> newContainersList = null;
  private List<Container> finishedContainersList = null;
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
    if (this.newContainersList != null) {
      builder.clearNewContainers();
      Iterable<ContainerProto> iterable = getProtoIterable(this.newContainersList);
      builder.addAllNewContainers(iterable);
    }
    if (this.finishedContainersList != null) {
      builder.clearFinishedContainers();
      Iterable<ContainerProto> iterable = getProtoIterable(this.finishedContainersList);
      builder.addAllFinishedContainers(iterable);
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
  public List<Container> getNewContainerList() {
    initLocalNewContainerList();
    return this.newContainersList;
  }
  
  @Override
  public Container getNewContainer(int index) {
    initLocalNewContainerList();
    return this.newContainersList.get(index);
  }
  @Override
  public int getNewContainerCount() {
    initLocalNewContainerList();
    return this.newContainersList.size();
  }
  
  //Once this is called. containerList will never be null - untill a getProto is called.
  private void initLocalNewContainerList() {
    if (this.newContainersList != null) {
      return;
    }
    AMResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getNewContainersList();
    newContainersList = new ArrayList<Container>();

    for (ContainerProto c : list) {
      newContainersList.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllNewContainers(final List<Container> containers) {
    if (containers == null) 
      return;
    initLocalNewContainerList();
    newContainersList.addAll(containers);
  }

  private Iterable<ContainerProto> getProtoIterable(
      final List<Container> newContainersList) {
    maybeInitBuilder();
    return new Iterable<ContainerProto>() {
      @Override
      public Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {

          Iterator<Container> iter = newContainersList.iterator();

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
  }
  
  @Override
  public void addNewContainer(Container containers) {
    initLocalNewContainerList();
    if (containers == null) 
      return;
    this.newContainersList.add(containers);
  }
  
  @Override
  public void removeNewContainer(int index) {
    initLocalNewContainerList();
    this.newContainersList.remove(index);
  }
  @Override
  public void clearNewContainers() {
    initLocalNewContainerList();
    this.newContainersList.clear();
  }

  //// Finished containers
  @Override
  public List<Container> getFinishedContainerList() {
    initLocalFinishedContainerList();
    return this.finishedContainersList;
  }
  
  @Override
  public Container getFinishedContainer(int index) {
    initLocalFinishedContainerList();
    return this.finishedContainersList.get(index);
  }
  @Override
  public int getFinishedContainerCount() {
    initLocalFinishedContainerList();
    return this.finishedContainersList.size();
  }
  
  //Once this is called. containerList will never be null - untill a getProto is called.
  private void initLocalFinishedContainerList() {
    if (this.finishedContainersList != null) {
      return;
    }
    AMResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getFinishedContainersList();
    finishedContainersList = new ArrayList<Container>();

    for (ContainerProto c : list) {
      finishedContainersList.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllFinishedContainers(final List<Container> containers) {
    if (containers == null) 
      return;
    initLocalFinishedContainerList();
    finishedContainersList.addAll(containers);
  }
  
  @Override
  public void addFinishedContainer(Container containers) {
    initLocalFinishedContainerList();
    if (containers == null) 
      return;
    this.finishedContainersList.add(containers);
  }
  
  @Override
  public void removeFinishedContainer(int index) {
    initLocalFinishedContainerList();
    this.finishedContainersList.remove(index);
  }
  @Override
  public void clearFinishedContainers() {
    initLocalFinishedContainerList();
    this.finishedContainersList.clear();
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
