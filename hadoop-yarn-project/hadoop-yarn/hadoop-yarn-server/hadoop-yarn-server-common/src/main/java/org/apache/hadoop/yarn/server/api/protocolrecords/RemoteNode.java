/**
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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

/**
 * This class is used to encapsulate the {@link NodeId} as well as the HTTP
 * address that can be used to communicate with the Node.
 */
@Private
@Unstable
public abstract class RemoteNode implements Comparable<RemoteNode> {

  /**
   * Create new Instance.
   * @param nodeId NodeId.
   * @param httpAddress Http address.
   * @return RemoteNode instance.
   */
  @Private
  @Unstable
  public static RemoteNode newInstance(NodeId nodeId, String httpAddress) {
    RemoteNode remoteNode = Records.newRecord(RemoteNode.class);
    remoteNode.setNodeId(nodeId);
    remoteNode.setHttpAddress(httpAddress);
    return remoteNode;
  }

  /**
   * Create new Instance.
   * @param nodeId NodeId.
   * @param httpAddress Http address.
   * @param rackName Rack Name.
   * @return RemoteNode instance.
   */
  @Private
  @Unstable
  public static RemoteNode newInstance(NodeId nodeId, String httpAddress,
      String rackName) {
    RemoteNode remoteNode = Records.newRecord(RemoteNode.class);
    remoteNode.setNodeId(nodeId);
    remoteNode.setHttpAddress(httpAddress);
    remoteNode.setRackName(rackName);
    return remoteNode;
  }

  /**
   * Create new Instance.
   * @param nodeId NodeId.
   * @param httpAddress Http address.
   * @param rackName Rack Name.
   * @param nodePartition Node Partition.
   * @return RemoteNode Instance.
   */
  @Private
  @Unstable
  public static RemoteNode newInstance(NodeId nodeId, String httpAddress,
      String rackName, String nodePartition) {
    RemoteNode remoteNode = Records.newRecord(RemoteNode.class);
    remoteNode.setNodeId(nodeId);
    remoteNode.setHttpAddress(httpAddress);
    remoteNode.setRackName(rackName);
    remoteNode.setNodePartition(nodePartition);
    return remoteNode;
  }

  /**
   * Get {@link NodeId}.
   * @return NodeId.
   */
  @Private
  @Unstable
  public abstract NodeId getNodeId();

  /**
   * Set {@link NodeId}.
   * @param nodeId NodeId.
   */
  @Private
  @Unstable
  public abstract void setNodeId(NodeId nodeId);

  /**
   * Get HTTP address.
   * @return Http Address.
   */
  @Private
  @Unstable
  public abstract String getHttpAddress();

  /**
   * Set HTTP address.
   * @param httpAddress HTTP address.
   */
  @Private
  @Unstable
  public abstract void setHttpAddress(String httpAddress);

  /**
   * Get Rack Name.
   * @return Rack Name.
   */
  @Private
  @Unstable
  public abstract String getRackName();

  /**
   * Set Rack Name.
   * @param rackName Rack Name.
   */
  @Private
  @Unstable
  public abstract void setRackName(String rackName);

  /**
   * Use the underlying {@link NodeId} comparator.
   * @param other RemoteNode.
   * @return Comparison.
   */

  /**
   * Get Node Partition.
   * @return Node Partition.
   */
  @Private
  @Unstable
  public  abstract String getNodePartition();

  /**
   * Set Node Partition.
   * @param nodePartition
   */
  @Private
  @Unstable
  public abstract void setNodePartition(String nodePartition);

  @Override
  public int compareTo(RemoteNode other) {
    return this.getNodeId().compareTo(other.getNodeId());
  }

  @Override
  public String toString() {
    return "RemoteNode{" +
        "nodeId=" + getNodeId() + ", " +
        "rackName=" + getRackName() + ", " +
        "httpAddress=" + getHttpAddress() + ", " +
        "partition=" + getNodePartition() + "}";
  }
}
