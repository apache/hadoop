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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>NodeId</code> is the unique identifier for a node.</p>
 * 
 * <p>It includes the <em>hostname</em> and <em>port</em> to uniquely 
 * identify the node. Thus, it is unique across restarts of any 
 * <code>NodeManager</code>.</p>
 */
@Public
@Stable
public abstract class NodeId implements Comparable<NodeId> {

  @Public
  @Stable
  public static NodeId newInstance(String host, int port) {
    NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(host);
    nodeId.setPort(port);
    nodeId.build();
    return nodeId;
  }

  /**
   * Get the <em>hostname</em> of the node.
   * @return <em>hostname</em> of the node
   */ 
  @Public
  @Stable
  public abstract String getHost();
  
  @Private
  @Unstable
  protected abstract void setHost(String host);

  /**
   * Get the <em>port</em> for communicating with the node.
   * @return <em>port</em> for communicating with the node
   */
  @Public
  @Stable
  public abstract int getPort();
  
  @Private
  @Unstable
  protected abstract void setPort(int port);

  @Override
  public String toString() {
    return this.getHost() + ":" + this.getPort();
  }

  @Override
  public int hashCode() {
    final int prime = 493217;
    int result = 8501;
    result = prime * result + this.getHost().hashCode();
    result = prime * result + this.getPort();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NodeId other = (NodeId) obj;
    if (!this.getHost().equals(other.getHost()))
      return false;
    if (this.getPort() != other.getPort())
      return false;
    return true;
  }

  @Override
  public int compareTo(NodeId other) {
    int hostCompare = this.getHost().compareTo(other.getHost());
    if (hostCompare == 0) {
      if (this.getPort() > other.getPort()) {
        return 1;
      } else if (this.getPort() < other.getPort()) {
        return -1;
      }
      return 0;
    }
    return hostCompare;
  }
  
  @Public
  @Stable
  public static NodeId fromString(String nodeIdStr) {
    String[] parts = nodeIdStr.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid NodeId [" + nodeIdStr
          + "]. Expected host:port");
    }
    try {
      NodeId nodeId =
          NodeId.newInstance(parts[0].trim(), Integer.parseInt(parts[1]));
      return nodeId;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port: " + parts[1], e);
    }
  }

  protected abstract void build();
}
