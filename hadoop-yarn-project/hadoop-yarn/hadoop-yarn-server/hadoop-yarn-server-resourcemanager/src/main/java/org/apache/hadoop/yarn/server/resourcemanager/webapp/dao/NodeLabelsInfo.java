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
  private ArrayList<NodeLabelInfo> nodeLabelInfo = new ArrayList<>();

  public NodeLabelsInfo() {
    // JAXB needs this
  }

  public NodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabels) {
    this.nodeLabelInfo = nodeLabels;
  }

  public NodeLabelsInfo(List<NodeLabel> nodeLabels) {
    this.nodeLabelInfo = new ArrayList<>();
    for (NodeLabel label : nodeLabels) {
      this.nodeLabelInfo.add(new NodeLabelInfo(label));
    }
  }

  public NodeLabelsInfo(Set<String> nodeLabelsName) {
    this.nodeLabelInfo = new ArrayList<>();
    for (String labelName : nodeLabelsName) {
      this.nodeLabelInfo.add(new NodeLabelInfo(labelName));
    }
  }

  public NodeLabelsInfo(Collection<NodeLabel> nodeLabels) {
    this.nodeLabelInfo = new ArrayList<>();
    nodeLabels.stream().forEach(nodeLabel -> {
      this.nodeLabelInfo.add(new NodeLabelInfo(nodeLabel));
    });
  }

  public ArrayList<NodeLabelInfo> getNodeLabelsInfo() {
    return nodeLabelInfo;
  }

  public Set<NodeLabel> getNodeLabels() {
    Set<NodeLabel> nodeLabels = new HashSet<>();
    for (NodeLabelInfo label : nodeLabelInfo) {
      nodeLabels.add(NodeLabel.newInstance(label.getName(),
          label.getExclusivity()));
    }
    return nodeLabels;
  }

  public List<String> getNodeLabelsName() {
    ArrayList<String> nodeLabelsName = new ArrayList<>();
    for (NodeLabelInfo label : nodeLabelInfo) {
      nodeLabelsName.add(label.getName());
    }
    return nodeLabelsName;
  }

  public void setNodeLabelsInfo(ArrayList<NodeLabelInfo> nodeLabelInfo) {
    this.nodeLabelInfo = nodeLabelInfo;
  }

  public void setNodeLabelInfo(ArrayList<NodeLabelInfo> nodeLabelsInfo) {
    this.nodeLabelInfo = nodeLabelsInfo;
  }
}
