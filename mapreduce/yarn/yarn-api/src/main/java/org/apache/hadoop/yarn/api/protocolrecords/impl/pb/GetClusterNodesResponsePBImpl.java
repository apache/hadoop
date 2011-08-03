package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProtoOrBuilder;

public class GetClusterNodesResponsePBImpl extends
    ProtoBase<GetClusterNodesResponseProto> implements GetClusterNodesResponse {

  GetClusterNodesResponseProto proto = 
    GetClusterNodesResponseProto.getDefaultInstance();
  GetClusterNodesResponseProto.Builder builder = null;
  boolean viaProto = false;

  List<NodeReport> nodeManagerInfoList;
  
  public GetClusterNodesResponsePBImpl() {
    builder = GetClusterNodesResponseProto.newBuilder();
  }
  
  public GetClusterNodesResponsePBImpl(GetClusterNodesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<NodeReport> getNodeReports() {    
    initLocalNodeManagerInfosList();
    return this.nodeManagerInfoList;
  }
  
  @Override
  public void setNodeReports(List<NodeReport> nodeManagers) {
    if (nodeManagers == null) {
      builder.clearNodeReports();
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
    List<NodeReportProto> list = p.getNodeReportsList();
    nodeManagerInfoList = new ArrayList<NodeReport>();

    for (NodeReportProto a : list) {
      nodeManagerInfoList.add(convertFromProtoFormat(a));
    }
  }

  private void addLocalNodeManagerInfosToProto() {
    maybeInitBuilder();
    builder.clearNodeReports();
    if (nodeManagerInfoList == null)
      return;
    Iterable<NodeReportProto> iterable = new Iterable<NodeReportProto>() {
      @Override
      public Iterator<NodeReportProto> iterator() {
        return new Iterator<NodeReportProto>() {

          Iterator<NodeReport> iter = nodeManagerInfoList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public NodeReportProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllNodeReports(iterable);
  }

  private NodeReportPBImpl convertFromProtoFormat(NodeReportProto p) {
    return new NodeReportPBImpl(p);
  }

  private NodeReportProto convertToProtoFormat(NodeReport t) {
    return ((NodeReportPBImpl)t).getProto();
  }

}
