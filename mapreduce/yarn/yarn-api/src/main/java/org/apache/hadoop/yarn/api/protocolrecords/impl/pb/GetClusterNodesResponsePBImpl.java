package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeManagerInfo;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeManagerInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeManagerInfoProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProtoOrBuilder;

public class GetClusterNodesResponsePBImpl extends
    ProtoBase<GetClusterNodesResponseProto> implements GetClusterNodesResponse {

  GetClusterNodesResponseProto proto = 
    GetClusterNodesResponseProto.getDefaultInstance();
  GetClusterNodesResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<NodeManagerInfo> nodeManagerInfoList;
  
  public GetClusterNodesResponsePBImpl() {
    builder = GetClusterNodesResponseProto.newBuilder();
  }
  
  public GetClusterNodesResponsePBImpl(GetClusterNodesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<NodeManagerInfo> getNodeManagerList() {    
    initLocalNodeManagerInfosList();
    return this.nodeManagerInfoList;
  }
  
  @Override
  public void setNodeManagerList(List<NodeManagerInfo> nodeManagers) {
    if (nodeManagers == null) {
      builder.clearNodeManagers();
    }
    this.nodeManagerInfoList = nodeManagers;
  }

  @Override
  public GetClusterNodesResponseProto getProto() {    
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeManagerInfoList != null) {
      addLocalNodeManagerInfosToProto();
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
      builder = GetClusterNodesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  //Once this is called. containerList will never be null - untill a getProto is called.
  private void initLocalNodeManagerInfosList() {
    if (this.nodeManagerInfoList != null) {
      return;
    }
    GetClusterNodesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeManagerInfoProto> list = p.getNodeManagersList();
    nodeManagerInfoList = new ArrayList<NodeManagerInfo>();

    for (NodeManagerInfoProto a : list) {
      nodeManagerInfoList.add(convertFromProtoFormat(a));
    }
  }

  private void addLocalNodeManagerInfosToProto() {
    maybeInitBuilder();
    builder.clearNodeManagers();
    if (nodeManagerInfoList == null)
      return;
    Iterable<NodeManagerInfoProto> iterable = new Iterable<NodeManagerInfoProto>() {
      @Override
      public Iterator<NodeManagerInfoProto> iterator() {
        return new Iterator<NodeManagerInfoProto>() {

          Iterator<NodeManagerInfo> iter = nodeManagerInfoList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public NodeManagerInfoProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllNodeManagers(iterable);
  }

  private NodeManagerInfoPBImpl convertFromProtoFormat(NodeManagerInfoProto p) {
    return new NodeManagerInfoPBImpl(p);
  }

  private NodeManagerInfoProto convertToProtoFormat(NodeManagerInfo t) {
    return ((NodeManagerInfoPBImpl)t).getProto();
  }

}
