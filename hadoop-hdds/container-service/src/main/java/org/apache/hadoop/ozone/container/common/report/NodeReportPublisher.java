/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.report;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.HddsServerUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_NODE_REPORT_INTERVAL_DEFAULT;

/**
 * Publishes NodeReport which will be sent to SCM as part of heartbeat.
 * NodeReport consist of:
 *   - NodeIOStats
 *   - VolumeReports
 */
public class NodeReportPublisher extends ReportPublisher<NodeReportProto> {

  private Long nodeReportInterval;

  @Override
  protected long getReportFrequency() {
    if (nodeReportInterval == null) {
      nodeReportInterval = getConf().getTimeDuration(
          HDDS_NODE_REPORT_INTERVAL,
          HDDS_NODE_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);

      long heartbeatFrequency = HddsServerUtil.getScmHeartbeatInterval(
          getConf());

      Preconditions.checkState(
          heartbeatFrequency <= nodeReportInterval,
          HDDS_NODE_REPORT_INTERVAL +
              " cannot be configured lower than heartbeat frequency.");
    }
    return nodeReportInterval;
  }

  @Override
  protected NodeReportProto getReport() throws IOException {
    return getContext().getParent().getContainer().getNodeReport();
  }
}
