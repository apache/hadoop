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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
   * @return Map
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
   * Parses a Json string and converts to NodePlan.
   *
   * @param json - Json String
   * @return NodePlan
   * @throws IOException
   */
  public static NodePlan parseJson(String json) throws IOException {
    return READER.readValue(json);
  }

  /**
   * Returns a Json representation of NodePlan.
   *
   * @return - json String
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
}
