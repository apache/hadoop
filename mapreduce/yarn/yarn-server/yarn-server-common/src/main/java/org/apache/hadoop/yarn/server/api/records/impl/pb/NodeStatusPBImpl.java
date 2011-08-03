package org.apache.hadoop.yarn.server.api.records.impl.pb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeHealthStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.ApplicationIdContainerListMapProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.ContainerListProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.mortbay.util.ajax.JSON.Convertor;
    
public class NodeStatusPBImpl extends ProtoBase<NodeStatusProto> implements NodeStatus {
  NodeStatusProto proto = NodeStatusProto.getDefaultInstance();
  NodeStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeId nodeId = null;
  private Map<ApplicationIdProto, List<Container>> containers = null;
  private NodeHealthStatus nodeHealthStatus = null;
  
  public NodeStatusPBImpl() {
    builder = NodeStatusProto.newBuilder();
  }

  public NodeStatusPBImpl(NodeStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeStatusProto getProto() {

      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.containers != null) {
      addContainersToProto();
    }
    if (this.nodeHealthStatus != null) {
      builder.setNodeHealthStatus(convertToProtoFormat(this.nodeHealthStatus));
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
      builder = NodeStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getResponseId() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getResponseId();
  }
  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId(responseId);
  }
  @Override
  public NodeId getNodeId() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    
    return this.nodeId;
  }
  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
    
  }
  
  @Override
  public Map<ApplicationId, List<Container>> getAllContainers() {
    initContainers();
    HashMap<ApplicationId, List<Container>> returnMap = new HashMap<ApplicationId, List<Container>>(
        this.containers.size());
    for (Entry<ApplicationIdProto, List<Container>> entry : this.containers.entrySet()) {
      returnMap.put(convertFromProtoFormat(entry.getKey()), entry.getValue());
    }
    return returnMap;
  }

  @Override
  public List<Container> getContainers(ApplicationId applicationId) {
    initContainers();
    ApplicationIdProto applicationIdProto = convertToProtoFormat(applicationId);
    if (this.containers.get(applicationIdProto) == null) {
      this.containers.put(applicationIdProto, new ArrayList<Container>());
    }
    return this.containers.get(applicationIdProto);
  }

  private void initContainers() {
    if (this.containers != null) {
      return;
    }
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdContainerListMapProto> list = p.getContainersList();
    this.containers = new HashMap<ApplicationIdProto, List<Container>>();

    for (ApplicationIdContainerListMapProto c : list) {
      this.containers.put(c.getApplicationId(), convertFromProtoFormat(c.getValue()));
    }
    
  }
  
  @Override
  public void addAllContainers(final Map<ApplicationId, List<Container>> containers) {
    if (containers == null)
      return;
    initContainers();
    for (Entry<ApplicationId, List<Container>> entry : containers.entrySet()) {
      this.containers.put(convertToProtoFormat(entry.getKey()), entry.getValue());
    }
  }
  
  private void addContainersToProto() {
    maybeInitBuilder();
    builder.clearContainers();
    viaProto = false;
    Iterable<ApplicationIdContainerListMapProto> iterable = new Iterable<ApplicationIdContainerListMapProto>() {

      @Override
      public Iterator<ApplicationIdContainerListMapProto> iterator() {
        return new Iterator<ApplicationIdContainerListMapProto>() {

          Iterator<ApplicationIdProto> keyIter = containers.keySet().iterator();
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }

          @Override
          public ApplicationIdContainerListMapProto next() {
            ApplicationIdProto applicationIdProto = keyIter.next();
            return ApplicationIdContainerListMapProto.newBuilder().setApplicationId(applicationIdProto).setValue(convertToProtoFormat(containers.get(applicationIdProto))).build();
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
  public NodeHealthStatus getNodeHealthStatus() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (nodeHealthStatus != null) {
      return nodeHealthStatus;
    }
    if (!p.hasNodeHealthStatus()) {
      return null;
    }
    nodeHealthStatus = convertFromProtoFormat(p.getNodeHealthStatus());
    return nodeHealthStatus;
  }

  @Override
  public void setNodeHealthStatus(NodeHealthStatus healthStatus) {
    maybeInitBuilder();
    if (healthStatus == null) {
      builder.clearNodeHealthStatus();
    }
    this.nodeHealthStatus = healthStatus;
  }

  /*
   * 
   * @Override
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
  */
  
  private ContainerListProto convertToProtoFormat(List<Container> src) {
    ContainerListProto.Builder ret = ContainerListProto.newBuilder();
    for (Container c : src) {
      ret.addContainer(((ContainerPBImpl)c).getProto());
    }
    return ret.build();
  }
  
  private List<Container> convertFromProtoFormat(ContainerListProto src) {
    List<Container> ret = new ArrayList<Container>();
    for (ContainerProto c : src.getContainerList()) {
      ret.add(convertFromProtoFormat(c));
    }
    return ret;
  }

  private Container convertFromProtoFormat(ContainerProto src) {
    return new ContainerPBImpl(src);
  }
  
  @Override
  public void setContainers(ApplicationId applicationId, List<Container> containers) {
    initContainers();
    this.containers.put(convertToProtoFormat(applicationId), containers);
  }

  @Override
  public void removeContainers(ApplicationId applicationId) {
    initContainers();
    this.containers.remove(applicationId);
  }
  
  @Override
  public void clearContainers() {
    initContainers();
    this.containers.clear();
  }

  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl)nodeId).getProto();
  }
  
  private NodeId convertFromProtoFormat(NodeIdProto proto) {
    return new NodeIdPBImpl(proto);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId applicationId) {
    return ((ApplicationIdPBImpl)applicationId).getProto();
  }
  
  private ApplicationId convertFromProtoFormat(ApplicationIdProto proto) {
    return new ApplicationIdPBImpl(proto);
  }

  private NodeHealthStatusProto convertToProtoFormat(
      NodeHealthStatus healthStatus) {
    return ((NodeHealthStatusPBImpl) healthStatus).getProto();
  }

  private NodeHealthStatus convertFromProtoFormat(NodeHealthStatusProto proto) {
    return new NodeHealthStatusPBImpl(proto);
  }
}  
