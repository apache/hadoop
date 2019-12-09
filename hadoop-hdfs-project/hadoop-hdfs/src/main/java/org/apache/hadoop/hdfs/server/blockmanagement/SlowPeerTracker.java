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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;


/**
 * This class aggregates information from {@link SlowPeerReports} received via
 * heartbeats.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SlowPeerTracker {
  public static final Logger LOG =
      LoggerFactory.getLogger(SlowPeerTracker.class);

  /**
   * Time duration after which a report is considered stale. This is
   * set to DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY * 3 i.e.
   * maintained for at least two successive reports.
   */
  private final long reportValidityMs;

  /**
   * Timer object for querying the current time. Separated out for
   * unit testing.
   */
  private final Timer timer;

  /**
   * ObjectWriter to convert JSON reports to String.
   */
  private static final ObjectWriter WRITER = new ObjectMapper().writer();
  /**
   * Number of nodes to include in JSON report. We will return nodes with
   * the highest number of votes from peers.
   */
  private static final int MAX_NODES_TO_REPORT = 5;

  /**
   * Information about peers that have reported a node as being slow.
   * Each outer map entry is a map of (DatanodeId) -> (timestamp),
   * mapping reporting nodes to the timestamp of the last report from
   * that node.
   *
   * DatanodeId could be the DataNodeId or its address. We
   * don't care as long as the caller uses it consistently.
   *
   * Stale reports are not evicted proactively and can potentially
   * hang around forever.
   */
  private final ConcurrentMap<String, ConcurrentMap<String, Long>>
      allReports;

  public SlowPeerTracker(Configuration conf, Timer timer) {
    this.timer = timer;
    this.allReports = new ConcurrentHashMap<>();
    this.reportValidityMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS) * 3;
  }

  /**
   * Add a new report. DatanodeIds can be the DataNodeIds or addresses
   * We don't care as long as the caller is consistent.
   *
   * @param reportingNode DataNodeId of the node reporting on its peer.
   * @param slowNode DataNodeId of the peer suspected to be slow.
   */
  public void addReport(String slowNode,
                        String reportingNode) {
    ConcurrentMap<String, Long> nodeEntries = allReports.get(slowNode);

    if (nodeEntries == null) {
      // putIfAbsent guards against multiple writers.
      allReports.putIfAbsent(slowNode, new ConcurrentHashMap<>());
      nodeEntries = allReports.get(slowNode);
    }

    // Replace the existing entry from this node, if any.
    nodeEntries.put(reportingNode, timer.monotonicNow());
  }

  /**
   * Retrieve the non-expired reports that mark a given DataNode
   * as slow. Stale reports are excluded.
   *
   * @param slowNode target node Id.
   * @return set of reports which implicate the target node as being slow.
   */
  public Set<String> getReportsForNode(String slowNode) {
    final ConcurrentMap<String, Long> nodeEntries =
        allReports.get(slowNode);

    if (nodeEntries == null || nodeEntries.isEmpty()) {
      return Collections.emptySet();
    }

    return filterNodeReports(nodeEntries, timer.monotonicNow());
  }

  /**
   * Retrieve all reports for all nodes. Stale reports are excluded.
   *
   * @return map from SlowNodeId -> (set of nodes reporting peers).
   */
  public Map<String, SortedSet<String>> getReportsForAllDataNodes() {
    if (allReports.isEmpty()) {
      return ImmutableMap.of();
    }

    final Map<String, SortedSet<String>> allNodesValidReports = new HashMap<>();
    final long now = timer.monotonicNow();

    for (Map.Entry<String, ConcurrentMap<String, Long>> entry :
        allReports.entrySet()) {
      SortedSet<String> validReports = filterNodeReports(entry.getValue(), now);
      if (!validReports.isEmpty()) {
        allNodesValidReports.put(entry.getKey(), validReports);
      }
    }
    return allNodesValidReports;
  }

  /**
   * Filter the given reports to return just the valid ones.
   *
   * @param reports
   * @param now
   * @return
   */
  private SortedSet<String> filterNodeReports(
      ConcurrentMap<String, Long> reports, long now) {
    final SortedSet<String> validReports = new TreeSet<>();

    for (Map.Entry<String, Long> entry : reports.entrySet()) {
      if (now - entry.getValue() < reportValidityMs) {
        validReports.add(entry.getKey());
      }
    }
    return validReports;
  }

  /**
   * Retrieve all valid reports as a JSON string.
   * @return serialized representation of valid reports. null if
   *         serialization failed.
   */
  public String getJson() {
    Collection<ReportForJson> validReports = getJsonReports(
        MAX_NODES_TO_REPORT);
    try {
      return WRITER.writeValueAsString(validReports);
    } catch (JsonProcessingException e) {
      // Failed to serialize. Don't log the exception call stack.
      LOG.debug("Failed to serialize statistics" + e);
      return null;
    }
  }

  /**
   * This structure is a thin wrapper over reports to make Json
   * [de]serialization easy.
   */
  public static class ReportForJson {
    @JsonProperty("SlowNode")
    final private String slowNode;

    @JsonProperty("ReportingNodes")
    final private SortedSet<String> reportingNodes;

    public ReportForJson(
        @JsonProperty("SlowNode") String slowNode,
        @JsonProperty("ReportingNodes") SortedSet<String> reportingNodes) {
      this.slowNode = slowNode;
      this.reportingNodes = reportingNodes;
    }

    public String getSlowNode() {
      return slowNode;
    }

    public SortedSet<String> getReportingNodes() {
      return reportingNodes;
    }
  }

  /**
   * Retrieve reports in a structure for generating JSON, limiting the
   * output to the top numNodes nodes i.e nodes with the most reports.
   * @param numNodes number of nodes to return. This is to limit the
   *                 size of the generated JSON.
   */
  private Collection<ReportForJson> getJsonReports(int numNodes) {
    if (allReports.isEmpty()) {
      return Collections.emptyList();
    }

    final PriorityQueue<ReportForJson> topNReports =
        new PriorityQueue<>(allReports.size(),
            new Comparator<ReportForJson>() {
          @Override
          public int compare(ReportForJson o1, ReportForJson o2) {
            return Ints.compare(o1.reportingNodes.size(),
                o2.reportingNodes.size());
          }
        });

    final long now = timer.monotonicNow();

    for (Map.Entry<String, ConcurrentMap<String, Long>> entry :
        allReports.entrySet()) {
      SortedSet<String> validReports = filterNodeReports(
          entry.getValue(), now);
      if (!validReports.isEmpty()) {
        if (topNReports.size() < numNodes) {
          topNReports.add(new ReportForJson(entry.getKey(), validReports));
        } else if (topNReports.peek().getReportingNodes().size() <
            validReports.size()){
          // Remove the lowest element
          topNReports.poll();
          topNReports.add(new ReportForJson(entry.getKey(), validReports));
        }
      }
    }
    return topNReports;
  }

  @VisibleForTesting
  long getReportValidityMs() {
    return reportValidityMs;
  }
}
