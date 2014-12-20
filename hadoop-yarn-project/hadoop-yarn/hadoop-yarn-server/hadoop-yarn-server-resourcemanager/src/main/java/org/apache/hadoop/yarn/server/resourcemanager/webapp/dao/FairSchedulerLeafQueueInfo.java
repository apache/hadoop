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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FairSchedulerLeafQueueInfo extends FairSchedulerQueueInfo {
  private int numPendingApps;
  private int numActiveApps;
  
  public FairSchedulerLeafQueueInfo() {
  }
  
  public FairSchedulerLeafQueueInfo(FSLeafQueue queue, FairScheduler scheduler) {
    super(queue, scheduler);
    numPendingApps = queue.getNumPendingApps();
    numActiveApps = queue.getNumActiveApps();
  }
  
  public int getNumActiveApplications() {
    return numActiveApps;
  }
  
  public int getNumPendingApplications() {
    return numPendingApps;
  }
}
