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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * XML element uses to represent NodeIds' list.
 */
@XmlRootElement(name = "nodeIDsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeIDsInfo {

  /**
   * Set doesn't support default no arg constructor which is req by JAXB
   */
  @XmlElement(name="nodes")
  protected ArrayList<String> nodeIDsList = new ArrayList<String>();

  @XmlElement(name = "partitionInfo")
  private PartitionInfo partitionInfo;

  public NodeIDsInfo() {
  } // JAXB needs this

  public NodeIDsInfo(List<String> nodeIdsList) {
    this.nodeIDsList.addAll(nodeIdsList);
  }

  public NodeIDsInfo(List<String> nodeIdsList, Resource resource) {
    this(nodeIdsList);
    this.partitionInfo = new PartitionInfo(new ResourceInfo(resource));
  }

  public NodeIDsInfo(Collection<String> nodeIdsList, PartitionInfo partitionInfo) {
    this.nodeIDsList.addAll(nodeIdsList);
    this.partitionInfo = partitionInfo;
  }

  public ArrayList<String> getNodeIDs() {
    return nodeIDsList;
  }

  public PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  /**
   * This method will generate a new NodeIDsInfo object based on the two NodeIDsInfo objects.
   * The information to be combined includes the node list (removed duplicate node)
   * and partitionInfo object.
   *
   * @param left left NodeIDsInfo Object.
   * @param right right NodeIDsInfo Object.
   * @return new NodeIDsInfo Object.
   */
  public static NodeIDsInfo add(NodeIDsInfo left, NodeIDsInfo right) {
    Set<String> nodes = new HashSet<>();
    if (left != null && left.nodeIDsList != null) {
      nodes.addAll(left.nodeIDsList);
    }
    if (right != null && right.nodeIDsList != null) {
      nodes.addAll(right.nodeIDsList);
    }

    PartitionInfo leftPartitionInfo = null;
    if (left != null) {
      leftPartitionInfo = left.getPartitionInfo();
    }

    PartitionInfo rightPartitionInfo = null;
    if (right != null) {
      rightPartitionInfo = right.getPartitionInfo();
    }

    PartitionInfo info = PartitionInfo.addTo(leftPartitionInfo, rightPartitionInfo);
    return new NodeIDsInfo(nodes, info);
  }
}
