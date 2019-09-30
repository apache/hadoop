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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import org.apache.hadoop.yarn.api.records.NodeState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "DecommissionCandidateNode")
@XmlAccessorType(XmlAccessType.NONE)
public class DecommissionCandidateNodeInfo {

  @XmlElement
  protected int amCount;

  @XmlElement
  protected int runningAppCount;

  @XmlElement
  protected int decommissionTimeout;

  @XmlElement
  protected String nodeId;

  protected boolean recommendFlag;

  @XmlElement
  protected NodeState nodeState;

  public DecommissionCandidateNodeInfo() {}

  public DecommissionCandidateNodeInfo(int a, int r, int de, NodeState s,
      String n, boolean recommend) {
    this.amCount = a;
    this.decommissionTimeout = de;
    this.runningAppCount = r;
    this.nodeState = s;
    this.nodeId = n;
    this.recommendFlag = recommend;
  }

  public String getNodeId() {
    return nodeId;
  }

  public NodeState getNodeState() {
    return nodeState;
  }
  public int getAmCount() {
    return amCount;
  }

  public int getRunningAppCount() {
    return runningAppCount;
  }

  public int getDecommissionTimeout() {
    return decommissionTimeout;
  }

}
