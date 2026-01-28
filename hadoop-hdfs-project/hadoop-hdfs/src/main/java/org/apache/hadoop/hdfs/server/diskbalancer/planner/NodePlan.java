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

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.SUPPORTED_PACKAGES_CONFIG_NAME;

/**
 * NodePlan is a set of volumeSetPlans.
 */
public class NodePlan {
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
      include = JsonTypeInfo.As.PROPERTY, property = "@class")
  private List<Step> volumeSetPlans;
  private String nodeName;
  private String nodeUUID;
  private int port;
  private long timeStamp;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(NodePlan.class);
  private static final ObjectWriter WRITER = MAPPER.writerFor(
      MAPPER.constructType(NodePlan.class));
  private static final Configuration CONFIGURATION = new HdfsConfiguration();
  private static final Collection<String> SUPPORTED_PACKAGES = getAllowedPackages();

  /**
   * returns timestamp when this plan was created.
   *
   * @return long
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * Sets the timestamp when this plan was created.
   *
   * @param timeStamp
   */
  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  /**
   * Constructs an Empty Node Plan.
   */
  public NodePlan() {
    volumeSetPlans = new LinkedList<>();
  }

  /**
   * Constructs an empty NodePlan.
   */
  public NodePlan(String datanodeName, int rpcPort) {
    volumeSetPlans = new LinkedList<>();
    this.nodeName = datanodeName;
    this.port = rpcPort;
  }

  /**
   * Returns a Map of  VolumeSetIDs and volumeSetPlans.
   *
   * @return List of Steps
   */
  public List<Step> getVolumeSetPlans() {
    return volumeSetPlans;
  }

  /**
   * Adds a step to the existing Plan.
   *
   * @param nextStep - nextStep
   */
  void addStep(Step nextStep) {
    Preconditions.checkNotNull(nextStep);
    volumeSetPlans.add(nextStep);
  }

  /**
   * Sets Node Name.
   *
   * @param nodeName - Name
   */
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Sets a volume List plan.
   *
   * @param volumeSetPlans - List of plans.
   */
  public void setVolumeSetPlans(List<Step> volumeSetPlans) {
    this.volumeSetPlans = volumeSetPlans;
  }

  /**
   * Returns the DataNode URI.
   *
   * @return URI
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Sets the DataNodeURI.
   *
   * @param dataNodeName - String
   */
  public void setURI(String dataNodeName) {
    this.nodeName = dataNodeName;
  }

  /**
   * Gets the DataNode RPC Port.
   *
   * @return port
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the DataNode RPC Port.
   *
   * @param port - int
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Parses a JSON string and converts to NodePlan.
   *
   * @param json - JSON String
   * @return NodePlan
   * @throws IOException
   */
  public static NodePlan parseJson(String json) throws IOException {
    JsonNode tree = READER.readTree(json);
    checkNodes(tree);
    return READER.readValue(tree);
  }

  /**
   * Iterate through the tree structure beginning at the input `node`. This includes
   * checking arrays and within JSON object structures (allowing for nested structures)
   *
   * @param node a node representing the root of tree structure
   * @throws IOException if any unexpected `@class` values are found - this is the
   * pre-existing exception type exposed by the calling code
   */
  private static void checkNodes(JsonNode node) throws IOException {
    if (node == null) {
      return;
    }

    // Check Node and Recurse into child nodes
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
      while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> entry = fieldsIterator.next();
        if ("@class".equals(entry.getKey())) {
          String textValue = entry.getValue().asText();
          if (textValue != null && !textValue.isBlank() && !stepClassIsAllowed(textValue)) {
            throw new IOException("Invalid @class value in NodePlan JSON: " + textValue);
          }
        }
        checkNodes(entry.getValue());
      }
    } else if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        checkNodes(node.get(i));
      }
    }
  }

  /**
   * Returns a JSON representation of NodePlan.
   *
   * @return - JSON String
   * @throws IOException
   */
  public String toJson() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * gets the Node UUID.
   *
   * @return Node UUID.
   */
  public String getNodeUUID() {
    return nodeUUID;
  }

  /**
   * Sets the Node UUID.
   *
   * @param nodeUUID - UUID of the node.
   */
  public void setNodeUUID(String nodeUUID) {
    this.nodeUUID = nodeUUID;
  }

  private static boolean stepClassIsAllowed(String className) {
    for (String pkg : SUPPORTED_PACKAGES) {
      if (className.startsWith(pkg)) {
        return true;
      }
    }
    return false;
  }

  private static Collection<String> getAllowedPackages() {
    return CONFIGURATION.getStringCollection(SUPPORTED_PACKAGES_CONFIG_NAME)
        .stream()
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }
}
