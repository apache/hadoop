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

package org.apache.slider.server.appmaster.state;

import com.google.common.base.Preconditions;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.common.tools.SliderUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tracking information about a container
 */
public final class RoleInstance implements Cloneable {

  public Container container;
  /**
   * Container ID
   */
  public final String id;
  public long createTime;
  public long startTime;
  /**
   * flag set when it is released, to know if it has
   * already been targeted for termination
   */
  public boolean released;

  /**
   * Name of the role
   */
  public String role;
  public String group;

  /**
   * Version of the app
   */
  public String appVersion;

  /**
   * Role Id; matches priority in resources.json
   */
  public int roleId;

  /**
   * state from StateValues
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
  
  public String ip;
  public String hostname;
  public String host;
  public String hostURL;
  public ContainerAllocationOutcome placement;


  /**
   * A list of registered endpoints.
   */
  private List<Endpoint> endpoints =
      new ArrayList<>(2);

  public RoleInstance(ContainerAssignment assignment) {
    this(assignment.container);
    placement = assignment.placement;
  }
  /**
   * Create an instance to track an allocated container
   * @param container a container which must be non null, and have a non-null Id field.
   */
  public RoleInstance(Container container) {
    Preconditions.checkNotNull(container, "Null container");
    Preconditions.checkState(container.getId() != null, 
      "Null container ID");

    this.container = container;
    id = container.getId().toString();
    if (container.getNodeId() != null) {
      host = container.getNodeId().getHost();
    }
    if (container.getNodeHttpAddress() != null) {
      hostURL = "http://" + container.getNodeHttpAddress();
    }
  }

  public ContainerId getId() {
    return container.getId();
  }
  
  public NodeId getHost() {
    return container.getNodeId();
  }

  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("RoleInstance{");
    sb.append("role='").append(role).append('\'');
    sb.append(", id='").append(id).append('\'');
    sb.append(", container=").append(SliderUtils.containerToString(container));
    sb.append(", createTime=").append(createTime);
    sb.append(", startTime=").append(startTime);
    sb.append(", released=").append(released);
    sb.append(", roleId=").append(roleId);
    sb.append(", host=").append(host);
    sb.append(", hostURL=").append(hostURL);
    sb.append(", state=").append(state);
    sb.append(", placement=").append(placement);
    sb.append(", exitCode=").append(exitCode);
    sb.append(", command='").append(command).append('\'');
    sb.append(", diagnostics='").append(diagnostics).append('\'');
    sb.append(", output=").append(Arrays.toString(output));
    sb.append(", environment=").append(Arrays.toString(environment));
    sb.append('}');
    return sb.toString();
  }

  public ContainerId getContainerId() {
    return container != null ? container.getId() : null;
  }

  /**
   * Generate the protobuf format of a request
   * @return protobuf format. This excludes the Container info
   */
  public Messages.RoleInstanceState toProtobuf() {
    Messages.RoleInstanceState.Builder builder =
      Messages.RoleInstanceState.newBuilder();
    if (container != null) {
      builder.setName(container.getId().toString());
    } else {
      builder.setName("unallocated instance");
    }
    if (command != null) {
      builder.setCommand(command);
    }
    if (environment != null) {
      builder.addAllEnvironment(Arrays.asList(environment));
    }
    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
    builder.setExitCode(exitCode);

    if (output != null) {
      builder.addAllOutput(Arrays.asList(output));
    }
    if (role != null) {
      builder.setRole(role);
    }
    builder.setRoleId(roleId);
    builder.setState(state);

    builder.setReleased(released);
    builder.setCreateTime(createTime);
    builder.setStartTime(startTime);
    builder.setHost(host);
    builder.setHostURL(hostURL);
    if (appVersion != null) {
      builder.setAppVersion(appVersion);
    }
    return builder.build();
  }

  /**
   * Build a serializable ClusterNode structure from this instance.
   * This operation is unsynchronized.
   * @return a serialized value.
   */
  public ClusterNode toClusterNode() {
    ClusterNode node;
    if (container != null) {
      node = new ClusterNode(container.getId());
    } else {
      node = new ClusterNode();
      node.name = "unallocated instance";
    }
    node.command = command;
    node.createTime = createTime;
    node.diagnostics = diagnostics;
    if (environment != null) {
      node.environment = Arrays.copyOf(environment, environment.length);
    }
    node.exitCode = exitCode;
    node.ip = ip;
    node.hostname = hostname;
    node.host = host;
    node.hostUrl = hostURL;
    if (output != null) {
      node.output = Arrays.copyOf(output, output.length);
    }
    node.released = released;
    node.role = role;
    node.roleId = roleId;
    node.startTime = startTime ;
    node.state = state;
    
    return node;
  }
  
  /**
   * Clone operation clones all the simple values but shares the 
   * Container object into the cloned copy -same with the output,
   * diagnostics and env arrays.
   * @return a clone of the object
   * @throws CloneNotSupportedException
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    RoleInstance cloned = (RoleInstance) super.clone();
    // clone the endpoint list, but not the values
    cloned.endpoints = new ArrayList<Endpoint>(this.endpoints);
    return cloned;
  }

  /**
   * Get the list of endpoints. 
   * @return the endpoint list.
   */
  public List<Endpoint> getEndpoints() {
    return endpoints;
  }

  /**
   * Add an endpoint registration
   * @param endpoint endpoint (non-null)
   */
  public void addEndpoint(Endpoint endpoint) {
    Preconditions.checkArgument(endpoint != null);
    endpoints.add(endpoint);
  }

  /**
   * Register a port endpoint as an inet-addr formatted endpoint, using the
   * hostname as the first part of the address
   * @param port port port
   * @param api  API API name
   */
  public void registerPortEndpoint(int port, String api) {
    Endpoint epr =
        RegistryTypeUtils.inetAddrEndpoint(api,
            ProtocolTypes.PROTOCOL_TCP, host, port);
    addEndpoint(epr);
  }

  /**
   * Serialize. Some data structures (e.g output)
   * may be shared
   * @return a serialized form for marshalling as JSON
   */
  public ContainerInformation serialize() {
    ContainerInformation info = new ContainerInformation();
    info.containerId = id;
    info.component = role;
    info.appVersion = appVersion;
    info.startTime = startTime;
    info.createTime = createTime;
    info.diagnostics = diagnostics;
    info.state = state;
    info.host = host;
    info.hostURL = hostURL;
    info.released = released ? Boolean.TRUE : null;
    if (placement != null) {
      info.placement = placement.toString();
    }
    if (output != null) {
      info.output = output;
    }
    return info;
  }
}
