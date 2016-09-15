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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/*
 * DAO object to display node information in allocation tree.
 * It corresponds to "ActivityNode" class.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ActivityNodeInfo {
  protected String name;  // The name for activity node
  protected String appPriority;
  protected String requestPriority;
  protected String allocationState;
  protected String diagnostic;

  protected List<ActivityNodeInfo> children;

  ActivityNodeInfo() {
  }

  ActivityNodeInfo(ActivityNode node) {
    this.name = node.getName();
    getPriority(node);
    this.allocationState = node.getState().name();
    this.diagnostic = node.getDiagnostic();
    this.children = new ArrayList<>();

    for (ActivityNode child : node.getChildren()) {
      ActivityNodeInfo containerInfo = new ActivityNodeInfo(child);
      this.children.add(containerInfo);
    }
  }

  private void getPriority(ActivityNode node) {
    if (node.getType()) {
      this.appPriority = node.getAppPriority();
    } else {
      this.requestPriority = node.getRequestPriority();
    }
  }
}
