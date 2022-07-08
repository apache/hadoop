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

import java.util.SortedSet;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This structure is a thin wrapper over slow peer reports to make Json
 * [de]serialization easy.
 */
@InterfaceAudience.Private
final class SlowPeerJsonReport {

  @JsonProperty("SlowNode")
  private final String slowNode;

  @JsonProperty("SlowPeerLatencyWithReportingNodes")
  private final SortedSet<SlowPeerLatencyWithReportingNode> slowPeerLatencyWithReportingNodes;

  SlowPeerJsonReport(
      @JsonProperty("SlowNode")
          String slowNode,
      @JsonProperty("SlowPeerLatencyWithReportingNodes")
          SortedSet<SlowPeerLatencyWithReportingNode> slowPeerLatencyWithReportingNodes) {
    this.slowNode = slowNode;
    this.slowPeerLatencyWithReportingNodes = slowPeerLatencyWithReportingNodes;
  }

  public String getSlowNode() {
    return slowNode;
  }

  public SortedSet<SlowPeerLatencyWithReportingNode> getSlowPeerLatencyWithReportingNodes() {
    return slowPeerLatencyWithReportingNodes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SlowPeerJsonReport that = (SlowPeerJsonReport) o;

    return new EqualsBuilder()
        .append(slowNode, that.slowNode)
        .append(slowPeerLatencyWithReportingNodes, that.slowPeerLatencyWithReportingNodes)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(slowNode)
        .append(slowPeerLatencyWithReportingNodes)
        .toHashCode();
  }
}
