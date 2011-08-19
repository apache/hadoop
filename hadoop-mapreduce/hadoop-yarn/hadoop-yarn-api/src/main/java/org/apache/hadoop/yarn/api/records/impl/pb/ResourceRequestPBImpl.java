package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProtoOrBuilder;


    
public class ResourceRequestPBImpl extends ProtoBase<ResourceRequestProto> implements  ResourceRequest {
  ResourceRequestProto proto = ResourceRequestProto.getDefaultInstance();
  ResourceRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private Priority priority = null;
  private Resource capability = null;
  
  
  public ResourceRequestPBImpl() {
    builder = ResourceRequestProto.newBuilder();
  }

  public ResourceRequestPBImpl(ResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResourceRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.capability != null) {
      builder.setCapability(convertToProtoFormat(this.capability));
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
      builder = ResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Priority getPriority() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getHostName() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHostName()) {
      return null;
    }
    return (p.getHostName());
  }

  @Override
  public void setHostName(String hostName) {
    maybeInitBuilder();
    if (hostName == null) {
      builder.clearHostName();
      return;
    }
    builder.setHostName((hostName));
  }
  @Override
  public Resource getCapability() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.capability != null) {
      return this.capability;
    }
    if (!p.hasCapability()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getCapability());
    return this.capability;
  }

  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null) 
      builder.clearCapability();
    this.capability = capability;
  }
  @Override
  public int getNumContainers() {
    ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumContainers());
  }

  @Override
  public void setNumContainers(int numContainers) {
    maybeInitBuilder();
    builder.setNumContainers((numContainers));
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }

  @Override
  public int compareTo(ResourceRequest other) {
    if (this.getPriority().compareTo(other.getPriority()) == 0) {
      if (this.getHostName().equals(other.getHostName())) {
        if (this.getCapability().equals(other.getCapability())) {
          if (this.getNumContainers() == other.getNumContainers()) {
            return 0;
          } else {
            return this.getNumContainers() - other.getNumContainers();
          }
        } else {
          return this.getCapability().compareTo(other.getCapability());
        }
      } else {
        return this.getHostName().compareTo(other.getHostName());
      }
    } else {
      return this.getPriority().compareTo(other.getPriority());
    }
  }

}  
