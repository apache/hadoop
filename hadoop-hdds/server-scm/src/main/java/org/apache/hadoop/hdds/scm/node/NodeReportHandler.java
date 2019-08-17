/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Node Reports from datanode.
 */
public class NodeReportHandler implements EventHandler<NodeReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(NodeReportHandler.class);
  private final NodeManager nodeManager;

  public NodeReportHandler(NodeManager nodeManager) {
    Preconditions.checkNotNull(nodeManager);
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(NodeReportFromDatanode nodeReportFromDatanode,
      EventPublisher publisher) {
    Preconditions.checkNotNull(nodeReportFromDatanode);
    DatanodeDetails dn = nodeReportFromDatanode.getDatanodeDetails();
    Preconditions.checkNotNull(dn, "NodeReport is "
        + "missing DatanodeDetails.");
    LOGGER.trace("Processing node report for dn: {}", dn);
    nodeManager
        .processNodeReport(dn, nodeReportFromDatanode.getReport());
  }
}
