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
package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code NodeHealthStatus} is a summary of the health status of the node.
 * <p>
 * It includes information such as:
 * <ul>
 *   <li>
 *     An indicator of whether the node is healthy, as determined by the
 *     health-check script.
 *   </li>
 *   <li>The previous time at which the health status was reported.</li>
 *   <li>A diagnostic report on the health status.</li>
 * </ul>
 * 
 * @see NodeReport
 * @see ApplicationClientProtocol#getClusterNodes(org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest)
 */
@Public
@Stable
public abstract class NodeHealthStatus {

  @Private
  public static NodeHealthStatus newInstance(boolean isNodeHealthy,
      String healthReport, long lastHealthReport) {
    NodeHealthStatus status = Records.newRecord(NodeHealthStatus.class);
    status.setIsNodeHealthy(isNodeHealthy);
    status.setHealthReport(healthReport);
    status.setLastHealthReportTime(lastHealthReport);
    return status;
  }

  /**
   * Is the node healthy?
   * @return <code>true</code> if the node is healthy, else <code>false</code>
   */
  @Public
  @Stable
  public abstract boolean getIsNodeHealthy();

  @Private
  @Unstable
  public abstract void setIsNodeHealthy(boolean isNodeHealthy);

  /**
   * Get the <em>diagnostic health report</em> of the node.
   * @return <em>diagnostic health report</em> of the node
   */
  @Public
  @Stable
  public abstract String getHealthReport();

  @Private
  @Unstable
  public abstract void setHealthReport(String healthReport);

  /**
   * Get the <em>last timestamp</em> at which the health report was received.
   * @return <em>last timestamp</em> at which the health report was received
   */
  @Public
  @Stable
  public abstract long getLastHealthReportTime();

  @Private
  @Unstable
  public abstract void setLastHealthReportTime(long lastHealthReport);
}