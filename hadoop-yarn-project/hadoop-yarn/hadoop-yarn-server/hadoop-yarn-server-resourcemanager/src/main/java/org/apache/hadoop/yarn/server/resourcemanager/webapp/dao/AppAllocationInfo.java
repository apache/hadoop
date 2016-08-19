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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AppAllocation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*
 * DAO object to display application allocation detailed information.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAllocationInfo {
  protected String nodeId;
  protected String queueName;
  protected String appPriority;
  protected String allocatedContainerId;
  protected String allocationState;
  protected String diagnostic;
  protected String timeStamp;
  protected List<ActivityNodeInfo> allocationAttempt;

  private static final Log LOG = LogFactory.getLog(AppAllocationInfo.class);

  AppAllocationInfo() {
  }

  AppAllocationInfo(AppAllocation allocation) {
    this.allocationAttempt = new ArrayList<>();

    this.nodeId = allocation.getNodeId();
    this.queueName = allocation.getQueueName();
    this.appPriority = allocation.getPriority();
    this.allocatedContainerId = allocation.getContainerId();
    this.allocationState = allocation.getAppState().name();
    this.diagnostic = allocation.getDiagnostic();

    Date date = new Date();
    date.setTime(allocation.getTime());
    this.timeStamp = date.toString();

    for (ActivityNode attempt : allocation.getAllocationAttempts()) {
      ActivityNodeInfo containerInfo = new ActivityNodeInfo(attempt);
      this.allocationAttempt.add(containerInfo);
    }
  }
}
