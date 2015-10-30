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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeLabel;

@XmlRootElement(name = "nodeLabelsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeLabelsInfo {

  @XmlElement(name = "nodeLabelInfo")
  private ArrayList<NodeLabelInfo> nodeLabelsInfo =
    new ArrayList<NodeLabelInfo>();

  public NodeLabelsInfo() {
    // JAXB needs this
  }

  public NodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabels) {
    this.nodeLabelsInfo = nodeLabels;
  }

  public NodeLabelsInfo(List<NodeLabel> nodeLabels) {
    this.nodeLabelsInfo = new ArrayList<NodeLabelInfo>();
    for (NodeLabel label : nodeLabels) {
      this.nodeLabelsInfo.add(new NodeLabelInfo(label));
    }
  }
  
  public NodeLabelsInfo(Set<String> nodeLabelsName) {
    this.nodeLabelsInfo = new ArrayList<NodeLabelInfo>();
    for (String labelName : nodeLabelsName) {
      this.nodeLabelsInfo.add(new NodeLabelInfo(labelName));
    }
  }

  public ArrayList<NodeLabelInfo> getNodeLabelsInfo() {
    return nodeLabelsInfo;
  }

  public Set<NodeLabel> getNodeLabels() {
    Set<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    for (NodeLabelInfo label : nodeLabelsInfo) {
      nodeLabels.add(NodeLabel.newInstance(label.getName(),
          label.getExclusivity()));
    }
    return nodeLabels;
  }
  
  public List<String> getNodeLabelsName() {
    ArrayList<String> nodeLabelsName = new ArrayList<String>();
    for (NodeLabelInfo label : nodeLabelsInfo) {
      nodeLabelsName.add(label.getName());
    }
    return nodeLabelsName;
  }

  public void setNodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabelInfo) {
    this.nodeLabelsInfo = nodeLabelInfo;
  }
}
