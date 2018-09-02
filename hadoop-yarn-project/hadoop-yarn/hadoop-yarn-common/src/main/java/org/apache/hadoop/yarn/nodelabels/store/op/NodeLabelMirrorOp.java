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
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords
    .ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb
    .AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb
    .ReplaceLabelsOnNodeRequestPBImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * NodeLabel Mirror Op class.
 */
public class NodeLabelMirrorOp
    extends FSNodeStoreLogOp<CommonNodeLabelsManager> {

  public NodeLabelMirrorOp() {
    super();
  }

  @Override
  public void write(OutputStream os, CommonNodeLabelsManager mgr)
      throws IOException {
    ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequestPBImpl
        .newInstance(mgr.getClusterNodeLabels())).getProto()
        .writeDelimitedTo(os);
    if (mgr.isCentralizedConfiguration()) {
      ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest
          .newInstance(mgr.getNodeLabels())).getProto().writeDelimitedTo(os);
    }
  }

  @Override
  public void recover(InputStream is, CommonNodeLabelsManager mgr)
      throws IOException {
    List<NodeLabel> labels = new AddToClusterNodeLabelsRequestPBImpl(
        YarnServerResourceManagerServiceProtos
            .AddToClusterNodeLabelsRequestProto
            .parseDelimitedFrom(is)).getNodeLabels();
    mgr.addToCluserNodeLabels(labels);

    if (mgr.isCentralizedConfiguration()) {
      // Only load node to labels mapping while using centralized
      // configuration
      Map<NodeId, Set<String>> nodeToLabels =
          new ReplaceLabelsOnNodeRequestPBImpl(
              YarnServerResourceManagerServiceProtos
                  .ReplaceLabelsOnNodeRequestProto
                  .parseDelimitedFrom(is)).getNodeToLabels();
      mgr.replaceLabelsOnNode(nodeToLabels);
    }
  }

  @Override
  public int getOpCode() {
    return -1;
  }
}