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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AttributeMappingOperationTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributesProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodesToAttributesMappingRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodesToAttributesMappingRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;

/**
 * Proto class for node to attributes mapping request.
 */
public class NodesToAttributesMappingRequestPBImpl
    extends NodesToAttributesMappingRequest {
  private NodesToAttributesMappingRequestProto proto =
      NodesToAttributesMappingRequestProto.getDefaultInstance();
  private NodesToAttributesMappingRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private List<NodeToAttributes> nodeAttributesMapping = null;

  public NodesToAttributesMappingRequestPBImpl() {
    builder = NodesToAttributesMappingRequestProto.newBuilder();
  }

  public NodesToAttributesMappingRequestPBImpl(
      NodesToAttributesMappingRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    if (this.nodeAttributesMapping != null) {
      for (NodeToAttributes nodeAttributes : nodeAttributesMapping) {
        builder.addNodeToAttributes(
            ((NodeToAttributesPBImpl) nodeAttributes).getProto());
      }
    }
    proto = builder.build();
    viaProto = true;
  }

  public NodesToAttributesMappingRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodesToAttributesMappingRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setNodesToAttributes(List<NodeToAttributes> nodesToAttributes) {
    if (nodeAttributesMapping == null) {
      nodeAttributesMapping = new ArrayList<>();
    }
    if(nodesToAttributes == null) {
      throw new IllegalArgumentException("nodesToAttributes cannot be null");
    }
    nodeAttributesMapping.clear();
    nodeAttributesMapping.addAll(nodesToAttributes);
  }

  private void initNodeAttributesMapping() {
    if (this.nodeAttributesMapping != null) {
      return;
    }

    NodesToAttributesMappingRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<NodeToAttributesProto> nodeAttributesProtoList =
        p.getNodeToAttributesList();
    List<NodeToAttributes> attributes = new ArrayList<>();
    if (nodeAttributesProtoList == null
        || nodeAttributesProtoList.size() == 0) {
      this.nodeAttributesMapping = attributes;
      return;
    }
    for (NodeToAttributesProto nodeAttributeProto : nodeAttributesProtoList) {
      attributes.add(new NodeToAttributesPBImpl(nodeAttributeProto));
    }
    this.nodeAttributesMapping = attributes;
  }

  @Override
  public List<NodeToAttributes> getNodesToAttributes() {
    initNodeAttributesMapping();
    return this.nodeAttributesMapping;
  }

  @Override
  public void setFailOnUnknownNodes(boolean failOnUnknownNodes) {
    maybeInitBuilder();
    builder.setFailOnUnknownNodes(failOnUnknownNodes);
  }

  @Override
  public boolean getFailOnUnknownNodes() {
    NodesToAttributesMappingRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getFailOnUnknownNodes();
  }

  @Override
  public void setOperation(AttributeMappingOperationType operation) {
    maybeInitBuilder();
    builder.setOperation(convertToProtoFormat(operation));
  }

  private AttributeMappingOperationTypeProto convertToProtoFormat(
      AttributeMappingOperationType operation) {
    return AttributeMappingOperationTypeProto.valueOf(operation.name());
  }

  private AttributeMappingOperationType convertFromProtoFormat(
      AttributeMappingOperationTypeProto operationTypeProto) {
    return AttributeMappingOperationType.valueOf(operationTypeProto.name());
  }

  @Override
  public AttributeMappingOperationType getOperation() {
    NodesToAttributesMappingRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasOperation()) {
      return null;
    }
    return convertFromProtoFormat(p.getOperation());
  }

  @Override
  public String getSubClusterId() {
    NodesToAttributesMappingRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasSubClusterId()) ? p.getSubClusterId() : null;
  }

  @Override
  public void setSubClusterId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    builder.setSubClusterId(subClusterId);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof NodesToAttributesMappingRequest) {
      NodesToAttributesMappingRequest other =
          (NodesToAttributesMappingRequest) obj;
      if (getNodesToAttributes() == null) {
        if (other.getNodesToAttributes() != null) {
          return false;
        }
      } else if (!getNodesToAttributes()
          .containsAll(other.getNodesToAttributes())) {
        return false;
      }

      if (getOperation() == null) {
        if (other.getOperation() != null) {
          return false;
        }
      } else if (!getOperation().equals(other.getOperation())) {
        return false;
      }

      return getFailOnUnknownNodes() == other.getFailOnUnknownNodes();
    }
    return false;
  }
}
