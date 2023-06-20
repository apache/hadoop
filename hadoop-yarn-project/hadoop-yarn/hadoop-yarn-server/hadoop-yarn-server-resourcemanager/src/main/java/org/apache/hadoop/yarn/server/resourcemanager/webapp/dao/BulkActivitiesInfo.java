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
import java.util.List;
import java.util.ArrayList;

/**
 * DAO object to display allocation activities.
 */
@XmlRootElement(name = "bulkActivities")
@XmlAccessorType(XmlAccessType.FIELD)
public class BulkActivitiesInfo {

  private ArrayList<ActivitiesInfo> activities = new ArrayList<>();

  private String subClusterId;

  public BulkActivitiesInfo() {
    // JAXB needs this
  }

  public void add(ActivitiesInfo activitiesInfo) {
    activities.add(activitiesInfo);
  }

  public ArrayList<ActivitiesInfo> getActivities() {
    return activities;
  }

  public void addAll(List<ActivitiesInfo> activitiesInfoList) {
    activities.addAll(activitiesInfoList);
  }

  public String getSubClusterId() {
    return subClusterId;
  }

  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}
