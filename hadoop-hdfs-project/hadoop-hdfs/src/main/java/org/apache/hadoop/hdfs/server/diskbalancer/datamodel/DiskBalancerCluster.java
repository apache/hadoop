/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * DiskBalancerCluster represents the nodes that we are working against.
 * <p/>
 * Please Note :
 * <p/>
 * Semantics of inclusionList and exclusionLists.
 * <p/>
 * If a non-empty inclusionList is specified then the diskBalancer assumes that
 * the user is only interested in processing that list of nodes. This node list
 * is checked against the exclusionList and only the nodes in inclusionList but
 * not in exclusionList is processed.
 * <p/>
 * if inclusionList is empty, then we assume that all live nodes in the nodes is
 * to be processed by diskBalancer. In that case diskBalancer will avoid any
 * nodes specified in the exclusionList but will process all nodes in the
 * cluster.
 * <p/>
 * In other words, an empty inclusionList is means all the nodes otherwise
 * only a given list is processed and ExclusionList is always honored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiskBalancerCluster {

  static final Log LOG = LogFactory.getLog(DiskBalancerCluster.class);
  private final Set<String> exclusionList;
  private final Set<String> inclusionList;
  private ClusterConnector clusterConnector;
  private List<DiskBalancerDataNode> nodes;
  private String outputpath;

  @JsonIgnore
  private List<DiskBalancerDataNode> nodesToProcess;
  private float threshold;

  /**
   * Empty Constructor needed by Jackson.
   */
  public DiskBalancerCluster() {
    nodes = new LinkedList<>();
    exclusionList = new TreeSet<>();
    inclusionList = new TreeSet<>();

  }

  /**
   * Constructs a DiskBalancerCluster.
   *
   * @param connector - ClusterConnector
   * @throws IOException
   */
  public DiskBalancerCluster(ClusterConnector connector) throws IOException {
    Preconditions.checkNotNull(connector);
    clusterConnector = connector;
    exclusionList = new TreeSet<>();
    inclusionList = new TreeSet<>();
  }

  /**
   * Parses a Json string and converts to DiskBalancerCluster.
   *
   * @param json - Json String
   * @return DiskBalancerCluster
   * @throws IOException
   */
  public static DiskBalancerCluster parseJson(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(json, DiskBalancerCluster.class);
  }

  /**
   * readClusterInfo connects to the cluster and reads the node's data.  This
   * data is used as basis of rest of computation in DiskBalancerCluster
   */
  public void readClusterInfo() throws Exception {
    Preconditions.checkNotNull(clusterConnector);
    LOG.info("Using connector : " + clusterConnector.getConnectorInfo());
    nodes = clusterConnector.getNodes();
  }

  /**
   * Gets all DataNodes in the Cluster.
   *
   * @return Array of DisKBalancerDataNodes
   */
  public List<DiskBalancerDataNode> getNodes() {
    return nodes;
  }

  /**
   * Sets the list of nodes of this cluster.
   *
   * @param clusterNodes List of Nodes
   */
  public void setNodes(List<DiskBalancerDataNode> clusterNodes) {
    this.nodes = clusterNodes;
  }

  /**
   * Returns the current ExclusionList.
   *
   * @return List of Nodes that are excluded from diskBalancer right now.
   */
  public Set<String> getExclusionList() {
    return exclusionList;
  }

  /**
   * sets the list of nodes to exclude from process of diskBalancer.
   *
   * @param excludedNodes - exclusionList of nodes.
   */
  public void setExclusionList(Set<String> excludedNodes) {
    this.exclusionList.addAll(excludedNodes);
  }

  /**
   * Returns the threshold value. This is used for indicating how much skew is
   * acceptable, This is expressed as a percentage. For example to say 20% skew
   * between volumes is acceptable set this value to 20.
   *
   * @return float
   */
  public float getThreshold() {
    return threshold;
  }

  /**
   * Sets the threshold value.
   *
   * @param thresholdPercent - float - in percentage
   */
  public void setThreshold(float thresholdPercent) {
    Preconditions.checkState((thresholdPercent >= 0.0f) &&
        (thresholdPercent <= 100.0f),  "A percentage value expected.");
    this.threshold = thresholdPercent;
  }

  /**
   * Gets the Inclusion list.
   *
   * @return List of machine to be processed by diskBalancer.
   */
  public Set<String> getInclusionList() {
    return inclusionList;
  }

  /**
   * Sets the inclusionList.
   *
   * @param includeNodes - set of machines to be processed by diskBalancer.
   */
  public void setInclusionList(Set<String> includeNodes) {
    this.inclusionList.addAll(includeNodes);
  }

  /**
   * returns a serialized json string.
   *
   * @return String - json
   * @throws IOException
   */
  public String toJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }

  /**
   * Returns the Nodes to Process which is the real list of nodes processed by
   * diskBalancer.
   *
   * @return List of DiskBalancerDataNodes
   */
  @JsonIgnore
  public List<DiskBalancerDataNode> getNodesToProcess() {
    return nodesToProcess;
  }

  /**
   * Sets the nodes to process.
   *
   * @param dnNodesToProcess - List of DataNodes to process
   */
  @JsonIgnore
  public void setNodesToProcess(List<DiskBalancerDataNode> dnNodesToProcess) {
    this.nodesToProcess = dnNodesToProcess;
  }

  /**
   * Returns th output path for this cluster.
   */
  public String getOutput() {
    return outputpath;
  }

  /**
   * Sets the output path for this run.
   *
   * @param output - Path
   */
  public void setOutput(String output) {
    this.outputpath = output;
  }

  /**
   * Writes a snapshot of the cluster to the specified directory.
   *
   * @param snapShotName - name of the snapshot
   */
  public void createSnapshot(String snapShotName) throws IOException {
    String json = this.toJson();
    File outFile = new File(getOutput() + "/" + snapShotName);
    FileUtils.writeStringToFile(outFile, json);
  }
}
