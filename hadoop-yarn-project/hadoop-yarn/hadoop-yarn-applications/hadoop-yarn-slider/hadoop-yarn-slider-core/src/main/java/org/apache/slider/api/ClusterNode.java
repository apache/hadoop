/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.api;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.slider.api.proto.Messages;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Describe a specific node in the cluster
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL )
public final class ClusterNode implements Cloneable {
  protected static final Logger
    LOG = LoggerFactory.getLogger(ClusterNode.class);
  
  @JsonIgnore
  public ContainerId containerId;

  /**
   * server name
   */
  public String name;


  /**
   * UUID of container used in Slider RPC to refer to instances
   */
  public String id;
  
  public String role;
  
  public int roleId;

  public long createTime;
  public long startTime;
  /**
   * flag set when it is released, to know if it has
   * already been targeted for termination
   */
  public boolean released;
  public String host;
  public String ip;
  public String hostname;
  public String hostUrl;

  /**
   * state from {@link ClusterDescription}
   */
  public int state;

  /**
   * Exit code: only valid if the state >= STOPPED
   */
  public int exitCode;

  /**
   * what was the command executed?
   */
  public String command;

  /**
   * Any diagnostics
   */
  public String diagnostics;

  /**
   * What is the tail output from the executed process (or [] if not started
   * or the log cannot be picked up
   */
  public String[] output;

  /**
   * Any environment details
   */
  public String[] environment;

  /**
   * server-side ctor takes the container ID and builds the name from it
   * @param containerId container ID; can be null
   */
  public ClusterNode(ContainerId containerId) {
    if (containerId != null) {
      this.containerId = containerId;
      this.name = containerId.toString();
    }
  }

  /**
   * ctor for deserialization
   */
  public ClusterNode() {
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name).append(": ");
    builder.append(state).append("\n");
    builder.append("state: ").append(state).append("\n");
    builder.append("role: ").append(role).append("\n");
    append(builder, "host", host);
    append(builder, "hostURL", hostUrl);
    append(builder, "command", command);
    if (output != null) {
      for (String line : output) {
        builder.append(line).append("\n");
      }
    }
    append(builder, "diagnostics", diagnostics);
    return builder.toString();
  }

  private void append(StringBuilder builder, String key, Object val) {
    if (val != null) {
      builder.append(key).append(": ").append(val.toString()).append("\n");
    }
  }
  
  /**
   * Convert to a JSON string
   * @return a JSON string description
   * @throws IOException Problems mapping/writing the object
   */
  public String toJsonString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }


  /**
   * Convert from JSON
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO
   */
  public static ClusterNode fromJson(String json)
    throws IOException, JsonParseException, JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, ClusterNode.class);
    } catch (IOException e) {
      LOG.error("Exception while parsing json : {}\n{}", e , json, e);
      throw e;
    }
  }

  /**
   * Build from a protobuf response
   * @param message
   * @return the deserialized node
   */
  public static ClusterNode fromProtobuf(Messages.RoleInstanceState message) {
    ClusterNode node = new ClusterNode();
    node.name = message.getName();
    node.command = message.getCommand();
    node.diagnostics = message.getDiagnostics();
    String[] arr;
    int environmentCount = message.getEnvironmentCount();
    if (environmentCount > 0) {
      arr = new String[environmentCount];
      node.environment = message.getEnvironmentList().toArray(arr);
    }
    node.exitCode = message.getExitCode();
    int outputCount = message.getOutputCount();
    if (outputCount > 0) {
      arr = new String[outputCount];
      node.output = message.getOutputList().toArray(arr);
    }
    node.role = message.getRole();
    node.roleId = message.getRoleId();
    node.state = message.getState();
    node.host = message.getHost();
    node.hostUrl = message.getHostURL();
    node.createTime = message.getCreateTime();
    node.startTime = message.getStartTime();
    node.released = message.getReleased();
    return node;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
  
  public ClusterNode doClone() {
    try {
      return (ClusterNode)clone();
    } catch (CloneNotSupportedException e) {
      //not going to happen. This is a final class
      return null;
    }
  }
}
