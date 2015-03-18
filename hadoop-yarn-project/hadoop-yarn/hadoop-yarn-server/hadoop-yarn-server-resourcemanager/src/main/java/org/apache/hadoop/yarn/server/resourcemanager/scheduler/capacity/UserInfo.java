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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class UserInfo {
  protected String  username;
  protected ResourceInfo resourcesUsed;
  protected int numPendingApplications;
  protected int numActiveApplications;
  protected ResourceInfo AMResourceUsed;
  protected ResourceInfo userResourceLimit;

  UserInfo() {}

  UserInfo(String username, Resource resUsed, int activeApps, int pendingApps,
      Resource amResUsed, Resource resourceLimit) {
    this.username = username;
    this.resourcesUsed = new ResourceInfo(resUsed);
    this.numActiveApplications = activeApps;
    this.numPendingApplications = pendingApps;
    this.AMResourceUsed = new ResourceInfo(amResUsed);
    this.userResourceLimit = new ResourceInfo(resourceLimit);
  }

  public String getUsername() {
    return username;
  }

  public ResourceInfo getResourcesUsed() {
    return resourcesUsed;
  }

  public int getNumPendingApplications() {
    return numPendingApplications;
  }

  public int getNumActiveApplications() {
    return numActiveApplications;
  }

  public ResourceInfo getAMResourcesUsed() {
    return AMResourceUsed;
  }

  public ResourceInfo getUserResourceLimit() {
    return userResourceLimit;
  }
}
