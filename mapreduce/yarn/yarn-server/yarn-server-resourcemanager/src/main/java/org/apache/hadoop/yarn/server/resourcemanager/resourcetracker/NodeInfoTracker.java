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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;

/**
 * Track the node info and heart beat responses for this node.
 * This should be package private. It does not need to be public.
 *
 */
public class NodeInfoTracker {
  private final NodeManager node;
  HeartbeatResponse lastHeartBeatResponse;
  private NodeHeartbeatStatus heartBeatStatus;
  
  class NodeHeartbeatStatus {
    private long lastSeen = 0;
   
    public NodeHeartbeatStatus(long lastSeen) {
      this.lastSeen = lastSeen;
    }
    public NodeId getNodeId() {
     return node.getNodeID();
    }
    
    public long getLastSeen() {
      return  lastSeen;
    }
  }
  public NodeInfoTracker(NodeManager node, HeartbeatResponse lastHeartBeatResponse) {
    this.node = node;
    this.lastHeartBeatResponse = lastHeartBeatResponse;
    this.heartBeatStatus = new NodeHeartbeatStatus(System.currentTimeMillis());
  }

  public synchronized NodeManager getNodeManager() {
    return this.node;
  }


  public synchronized NodeHeartbeatStatus getlastHeartBeat() {
    return this.heartBeatStatus;
  }

  public synchronized HeartbeatResponse getLastHeartBeatResponse() {
    return this.lastHeartBeatResponse;
  }

  public synchronized void setLastHeartBeatResponse(HeartbeatResponse heartBeatResponse) {
    this.lastHeartBeatResponse = heartBeatResponse;
  }
  
  public synchronized void setLastHeartBeatTime() {
   this.heartBeatStatus = new NodeHeartbeatStatus(System.currentTimeMillis()); 
  }
}