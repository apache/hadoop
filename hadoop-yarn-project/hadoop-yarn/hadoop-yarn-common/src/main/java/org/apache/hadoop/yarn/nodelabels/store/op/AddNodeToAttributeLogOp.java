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
package org.apache.hadoop.yarn.nodelabels.store.op;

import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodesToAttributesMappingRequestPBImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * File system Add Node to attribute mapping.
 */
public class AddNodeToAttributeLogOp
    extends FSNodeStoreLogOp<NodeAttributesManager> {

  private List<NodeToAttributes> attributes;

  public static final int OPCODE = 0;

  @Override
  public void write(OutputStream os, NodeAttributesManager mgr)
      throws IOException {
    ((NodesToAttributesMappingRequestPBImpl) NodesToAttributesMappingRequest
        .newInstance(AttributeMappingOperationType.ADD, attributes, false))
        .getProto().writeDelimitedTo(os);
  }

  @Override
  public void recover(InputStream is, NodeAttributesManager mgr)
      throws IOException {
    NodesToAttributesMappingRequest request =
        new NodesToAttributesMappingRequestPBImpl(
            YarnServerResourceManagerServiceProtos
                .NodesToAttributesMappingRequestProto
                .parseDelimitedFrom(is));
    mgr.addNodeAttributes(getNodeToAttributesMap(request));
  }

  public AddNodeToAttributeLogOp setAttributes(
      List<NodeToAttributes> attributesList) {
    this.attributes = attributesList;
    return this;
  }

  @Override
  public int getOpCode() {
    return OPCODE;
  }
}