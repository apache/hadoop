/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import com.google.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link GetSubClustersInfoResponse}.
 */
@Private
@Unstable
public class GetSubClustersInfoResponsePBImpl
    extends GetSubClustersInfoResponse {

  private GetSubClustersInfoResponseProto proto =
      GetSubClustersInfoResponseProto.getDefaultInstance();
  private GetSubClustersInfoResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private List<SubClusterInfo> subClusterInfos;

  public GetSubClustersInfoResponsePBImpl() {
    builder = GetSubClustersInfoResponseProto.newBuilder();
  }

  public GetSubClustersInfoResponsePBImpl(
      GetSubClustersInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetSubClustersInfoResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.subClusterInfos != null) {
      addSubClusterInfosToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClustersInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<SubClusterInfo> getSubClusters() {
    initSubClustersInfoList();
    return subClusterInfos;
  }

  @Override
  public void setSubClusters(List<SubClusterInfo> subClusters) {
    if (subClusters == null) {
      builder.clearSubClusterInfos();
      return;
    }
    this.subClusterInfos = subClusters;
  }

  private void initSubClustersInfoList() {
    if (this.subClusterInfos != null) {
      return;
    }
    GetSubClustersInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<SubClusterInfoProto> subClusterInfosList = p.getSubClusterInfosList();
    subClusterInfos = new ArrayList<SubClusterInfo>();

    for (SubClusterInfoProto r : subClusterInfosList) {
      subClusterInfos.add(convertFromProtoFormat(r));
    }
  }

  private void addSubClusterInfosToProto() {
    maybeInitBuilder();
    builder.clearSubClusterInfos();
    if (subClusterInfos == null) {
      return;
    }
    Iterable<SubClusterInfoProto> iterable =
        new Iterable<SubClusterInfoProto>() {
          @Override
          public Iterator<SubClusterInfoProto> iterator() {
            return new Iterator<SubClusterInfoProto>() {

              private Iterator<SubClusterInfo> iter =
                  subClusterInfos.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public SubClusterInfoProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllSubClusterInfos(iterable);
  }

  private SubClusterInfoProto convertToProtoFormat(SubClusterInfo r) {
    return ((SubClusterInfoPBImpl) r).getProto();
  }

  private SubClusterInfoPBImpl convertFromProtoFormat(SubClusterInfoProto r) {
    return new SubClusterInfoPBImpl(r);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

}
