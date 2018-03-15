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

package org.apache.hadoop.ozone.scm.node;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * This class represents the item in SCM heartbeat queue.
 */
public class HeartbeatQueueItem {
  private DatanodeID datanodeID;
  private long recvTimestamp;
  private SCMNodeReport nodeReport;
  private ReportState containerReportState;

  /**
   *
   * @param datanodeID - datanode ID of the heartbeat.
   * @param recvTimestamp - heartbeat receive timestamp.
   * @param nodeReport - node report associated with the heartbeat if any.
   * @param containerReportState - container report state.
   */
  HeartbeatQueueItem(DatanodeID datanodeID, long recvTimestamp,
      SCMNodeReport nodeReport, ReportState containerReportState) {
    this.datanodeID = datanodeID;
    this.recvTimestamp = recvTimestamp;
    this.nodeReport = nodeReport;
    this.containerReportState = containerReportState;
  }

  /**
   * @return datanode ID.
   */
  public DatanodeID getDatanodeID() {
    return datanodeID;
  }

  /**
   * @return node report.
   */
  public SCMNodeReport getNodeReport() {
    return nodeReport;
  }

  /**
   * @return container report state.
   */
  public ReportState getContainerReportState() {
    return containerReportState;
  }

  /**
   * @return heartbeat receive timestamp.
   */
  public long getRecvTimestamp() {
    return recvTimestamp;
  }

  /**
   * Builder for HeartbeatQueueItem.
   */
  public static class Builder {
    private DatanodeID datanodeID;
    private SCMNodeReport nodeReport;
    private ReportState containerReportState;
    private long recvTimestamp = monotonicNow();

    public Builder setDatanodeID(DatanodeID datanodeId) {
      this.datanodeID = datanodeId;
      return this;
    }

    public Builder setNodeReport(SCMNodeReport scmNodeReport) {
      this.nodeReport = scmNodeReport;
      return this;
    }

    public Builder setContainerReportState(ReportState crs) {
      this.containerReportState = crs;
      return this;
    }

    @VisibleForTesting
    public Builder setRecvTimestamp(long recvTime) {
      this.recvTimestamp = recvTime;
      return this;
    }

    public HeartbeatQueueItem build() {
      return new HeartbeatQueueItem(datanodeID, recvTimestamp, nodeReport,
          containerReportState);
    }
  }
}