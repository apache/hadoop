/*
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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;

/**
 * Mock container.
 */
public class MockContainer extends Container {

  private ContainerId id;
  private NodeId nodeId;
  private String nodeHttpAddress;
  private Resource resource;
  private Priority priority;
  private Token containerToken;

  @Override
  public int compareTo(Container other) {
    if (this.getId().compareTo(other.getId()) == 0) {
      if (this.getNodeId().compareTo(other.getNodeId()) == 0) {
        return this.getResource().compareTo(other.getResource());
      } else {
        return this.getNodeId().compareTo(other.getNodeId());
      }
    } else {
      return this.getId().compareTo(other.getId());
    }
  }

  @Override
  public String toString() {
    return "MockContainer{ id=" + id +
           ", nodeHttpAddress='" + nodeHttpAddress + "'," +
           " priority=" + priority + " }";
  }

  @Override
  public ContainerId getId() {
    return id;
  }

  @Override
  public void setId(ContainerId id) {
    this.id = id;
  }

  @Override
  public NodeId getNodeId() {
    return nodeId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    this.nodeId = nodeId;
  }

  @Override
  public String getNodeHttpAddress() {
    return nodeHttpAddress;
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    this.nodeHttpAddress = nodeHttpAddress;
  }

  @Override
  public Resource getResource() {
    return resource;
  }

  @Override
  public void setResource(Resource resource) {
    this.resource = resource;
  }

  @Override
  public Priority getPriority() {
    return priority;
  }

  @Override
  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  @Override
  public Token getContainerToken() {
    return containerToken;
  }

  @Override
  public void setContainerToken(Token containerToken) {
    this.containerToken = containerToken;
  }

  @Override
  public ExecutionType getExecutionType() {
    return null;
  }

  @Override
  public void setExecutionType(ExecutionType executionType) {

  }

}
