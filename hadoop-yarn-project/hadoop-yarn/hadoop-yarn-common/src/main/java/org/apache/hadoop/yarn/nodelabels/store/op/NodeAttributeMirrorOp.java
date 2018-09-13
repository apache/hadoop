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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodesToAttributesMappingRequestPBImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * File System Node Attribute Mirror read and write operation.
 */
public class NodeAttributeMirrorOp
    extends FSNodeStoreLogOp<NodeAttributesManager> {

  @Override
  public void write(OutputStream os, NodeAttributesManager mgr)
      throws IOException {
    ((NodesToAttributesMappingRequestPBImpl) NodesToAttributesMappingRequest
        .newInstance(AttributeMappingOperationType.REPLACE,
            mgr.getNodeToAttributes(
                ImmutableSet.of(NodeAttribute.PREFIX_CENTRALIZED)), false))
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
    mgr.replaceNodeAttributes(NodeAttribute.PREFIX_CENTRALIZED,
        getNodeToAttributesMap(request));
  }

  @Override
  public int getOpCode() {
    return -1;
  }
}