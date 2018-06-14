/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.server.report;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles Datanode Node Report.
 */
public class SCMDatanodeNodeReportHandler extends
    SCMDatanodeReportHandler<NodeReportProto> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SCMDatanodeNodeReportHandler.class);

  @Override
  public void processReport(DatanodeDetails datanodeDetails,
                            NodeReportProto report) throws IOException {
    LOG.debug("Processing node report from {}.", datanodeDetails);
    //TODO: add logic to process node report.
  }
}
