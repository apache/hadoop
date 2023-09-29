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

  @JsonProperty("MedianLatency")
  private final Double medianLatency;

  @JsonProperty("MadLatency")
  private final Double madLatency;

  @JsonProperty("UpperLimitLatency")
  private final Double upperLimitLatency;

  SlowPeerLatencyWithReportingNode(
      @JsonProperty("ReportingNode")
          String reportingNode,
      @JsonProperty("ReportedLatency")
          Double reportedLatency,
      @JsonProperty("MedianLatency")
          Double medianLatency,
      @JsonProperty("MadLatency")
          Double madLatency,
      @JsonProperty("UpperLimitLatency")
          Double upperLimitLatency) {
    this.reportingNode = reportingNode;
    this.reportedLatency = reportedLatency;
    this.medianLatency = medianLatency;
    this.madLatency = madLatency;
    this.upperLimitLatency = upperLimitLatency;
  }

  public String getReportingNode() {
    return reportingNode;
  }

  public Double getReportedLatency() {
    return reportedLatency;
  }

  public Double getMedianLatency() {
    return medianLatency;
  }

  public Double getMadLatency() {
    return madLatency;
  }

  public Double getUpperLimitLatency() {
    return upperLimitLatency;
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
        .append(medianLatency, that.medianLatency)
        .append(madLatency, that.madLatency)
        .append(upperLimitLatency, that.upperLimitLatency)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(reportingNode)
        .append(reportedLatency)
        .append(medianLatency)
        .append(madLatency)
        .append(upperLimitLatency)
        .toHashCode();
  }
}
