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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

@XmlRootElement(name = "fairScheduler")
@XmlType(name = "fairScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class FairSchedulerInfo extends SchedulerInfo {
  private FairSchedulerQueueInfo rootQueue;
  
  @XmlTransient
  private FairScheduler scheduler;
  
  public FairSchedulerInfo() {
  } // JAXB needs this
  
  public FairSchedulerInfo(FairScheduler fs) {
    scheduler = fs;
    rootQueue = new FairSchedulerQueueInfo(scheduler.getQueueManager().
        getRootQueue(), scheduler);
  }
  
  public int getAppFairShare(ApplicationAttemptId appAttemptId) {
    return scheduler.getSchedulerApp(appAttemptId).getFairShare().getMemory();
  }
  
  public FairSchedulerQueueInfo getRootQueueInfo() {
    return rootQueue;
  }
}
