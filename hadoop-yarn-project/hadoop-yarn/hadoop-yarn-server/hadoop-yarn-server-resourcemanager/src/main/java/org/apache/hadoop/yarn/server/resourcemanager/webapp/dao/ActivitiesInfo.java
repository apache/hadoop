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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.NodeAllocation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

/*
 * DAO object to display node allocation activity.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ActivitiesInfo {
  protected String nodeId;
  protected String timeStamp;
  protected String diagnostic = null;
  protected List<NodeAllocationInfo> allocations;

  private static final Logger LOG =
      LoggerFactory.getLogger(ActivitiesInfo.class);

  public ActivitiesInfo() {
  }

  public ActivitiesInfo(String errorMessage, String nodeId) {
    this.diagnostic = errorMessage;
    this.nodeId = nodeId;
  }

  public ActivitiesInfo(List<NodeAllocation> nodeAllocations, String nodeId) {
    this.nodeId = nodeId;
    this.allocations = new ArrayList<>();

    if (nodeAllocations == null) {
      diagnostic = (nodeId != null ?
          "waiting for display" :
          "waiting for next allocation");
    } else {
      if (nodeAllocations.size() == 0) {
        diagnostic = "do not have available resources";
      } else {
        this.nodeId = nodeAllocations.get(0).getNodeId();

        Date date = new Date();
        date.setTime(nodeAllocations.get(0).getTimeStamp());
        this.timeStamp = date.toString();

        for (int i = 0; i < nodeAllocations.size(); i++) {
          NodeAllocation nodeAllocation = nodeAllocations.get(i);
          NodeAllocationInfo allocationInfo = new NodeAllocationInfo(
              nodeAllocation);
          this.allocations.add(allocationInfo);
        }
      }
    }
  }
}
