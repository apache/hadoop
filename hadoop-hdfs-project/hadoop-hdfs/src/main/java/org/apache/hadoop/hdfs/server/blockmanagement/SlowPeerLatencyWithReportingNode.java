/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class represents the reporting node and the slow node's latency as observed by the
 * reporting node. This class is used by SlowPeerJsonReport class.
 */
@InterfaceAudience.Private
final class SlowPeerLatencyWithReportingNode
    implements Comparable<SlowPeerLatencyWithReportingNode> {

  @JsonProperty("ReportingNode")
  private final String reportingNode;

  @JsonProperty("ReportedLatency")
  private final Double reportedLatency;

  SlowPeerLatencyWithReportingNode(
      @JsonProperty("ReportingNode")
          String reportingNode,
      @JsonProperty("ReportedLatency")
          Double reportedLatency) {
    this.reportingNode = reportingNode;
    this.reportedLatency = reportedLatency;
  }

  public String getReportingNode() {
    return reportingNode;
  }

  public Double getReportedLatency() {
    return reportedLatency;
  }

  @Override
  public int compareTo(SlowPeerLatencyWithReportingNode o) {
    return this.reportingNode.compareTo(o.getReportingNode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SlowPeerLatencyWithReportingNode that = (SlowPeerLatencyWithReportingNode) o;

    return new EqualsBuilder()
        .append(reportingNode, that.reportingNode)
        .append(reportedLatency, that.reportedLatency)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(reportingNode)
        .append(reportedLatency)
        .toHashCode();
  }
}
