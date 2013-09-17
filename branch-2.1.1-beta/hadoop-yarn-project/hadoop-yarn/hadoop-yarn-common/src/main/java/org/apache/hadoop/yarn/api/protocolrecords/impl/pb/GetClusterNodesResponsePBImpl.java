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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetClusterNodesResponsePBImpl extends GetClusterNodesResponse {

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

  public GetClusterNodesResponseProto getProto() {    
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
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

  // Once this is called. containerList will never be null - until a getProto
  // is called.
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
