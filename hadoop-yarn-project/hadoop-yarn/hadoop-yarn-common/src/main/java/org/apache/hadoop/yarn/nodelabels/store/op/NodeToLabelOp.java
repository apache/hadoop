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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords
    .ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb
    .ReplaceLabelsOnNodeRequestPBImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

/**
 * Node to label mapping store operation for label.
 */
public class NodeToLabelOp
    extends FSNodeStoreLogOp<CommonNodeLabelsManager> {

  private Map<NodeId, Set<String>> nodeToLabels;
  public static final int OPCODE = 1;

  @Override
  public void write(OutputStream os, CommonNodeLabelsManager mgr)
      throws IOException {
    ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest
        .newInstance(nodeToLabels)).getProto().writeDelimitedTo(os);
  }

  @Override
  public void recover(InputStream is, CommonNodeLabelsManager mgr)
      throws IOException {
    nodeToLabels = new ReplaceLabelsOnNodeRequestPBImpl(
        YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
            .parseDelimitedFrom(is)).getNodeToLabels();
    if (mgr.isCentralizedConfiguration()) {
      mgr.replaceLabelsOnNode(nodeToLabels);
    }
  }

  public NodeToLabelOp setNodeToLabels(
      Map<NodeId, Set<String>> nodeToLabelsList) {
    this.nodeToLabels = nodeToLabelsList;
    return this;
  }

  public Map<NodeId, Set<String>> getNodeToLabels() {
    return nodeToLabels;
  }

  @Override
  public int getOpCode() {
    return OPCODE;
  }
}