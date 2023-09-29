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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * It records an activity operation in allocation,
 * which can be classified as queue, application or container activity.
 * Other information include state, diagnostic, priority.
 */
public class AllocationActivity {
  private String childName = null;
  private String parentName = null;
  private Integer appPriority = null;
  private Integer requestPriority = null;
  private ActivityState state;
  private String diagnostic = null;
  private NodeId nodeId;
  private Long allocationRequestId;
  private ActivityLevel level;

  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationActivity.class);

  public AllocationActivity(String parentName, String queueName,
      Integer priority, ActivityState state, String diagnostic,
      ActivityLevel level, NodeId nodeId, Long allocationRequestId) {
    this.childName = queueName;
    this.parentName = parentName;
    if (level != null) {
      this.level = level;
      switch (level) {
      case APP:
        this.appPriority = priority;
        break;
      case REQUEST:
        this.requestPriority = priority;
        this.allocationRequestId = allocationRequestId;
        break;
      case NODE:
        this.nodeId = nodeId;
        break;
      default:
        break;
      }
    }
    this.state = state;
    this.diagnostic = diagnostic;
  }

  public ActivityNode createTreeNode() {
    return new ActivityNode(this.childName, this.parentName,
        this.level == ActivityLevel.APP ?
            this.appPriority : this.requestPriority,
        this.state, this.diagnostic, this.level,
        this.nodeId, this.allocationRequestId);
  }

  public String getName() {
    return this.childName;
  }

  public String getState() {
    return this.state.toString();
  }
}
