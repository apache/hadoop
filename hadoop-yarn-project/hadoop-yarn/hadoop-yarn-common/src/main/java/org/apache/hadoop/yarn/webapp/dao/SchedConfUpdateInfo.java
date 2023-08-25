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

package org.apache.hadoop.yarn.webapp.dao;

import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Information for making scheduler configuration changes (supports adding,
 * removing, or updating a queue, as well as global scheduler conf changes).
 */
@XmlRootElement(name = "sched-conf")
@XmlAccessorType(XmlAccessType.FIELD)
public class SchedConfUpdateInfo {

  @XmlElement(name = "add-queue")
  private ArrayList<QueueConfigInfo> addQueueInfo = new ArrayList<>();

  @XmlElement(name = "remove-queue")
  private ArrayList<String> removeQueueInfo = new ArrayList<>();

  @XmlElement(name = "update-queue")
  private ArrayList<QueueConfigInfo> updateQueueInfo = new ArrayList<>();

  @XmlElement(name = "subClusterId")
  private String subClusterId = "";

  private HashMap<String, String> global = new HashMap<>();

  public SchedConfUpdateInfo() {
    // JAXB needs this
  }

  public ArrayList<QueueConfigInfo> getAddQueueInfo() {
    return addQueueInfo;
  }

  public void setAddQueueInfo(ArrayList<QueueConfigInfo> addQueueInfo) {
    this.addQueueInfo = addQueueInfo;
  }

  public ArrayList<String> getRemoveQueueInfo() {
    return removeQueueInfo;
  }

  public void setRemoveQueueInfo(ArrayList<String> removeQueueInfo) {
    this.removeQueueInfo = removeQueueInfo;
  }

  public ArrayList<QueueConfigInfo> getUpdateQueueInfo() {
    return updateQueueInfo;
  }

  public void setUpdateQueueInfo(ArrayList<QueueConfigInfo> updateQueueInfo) {
    this.updateQueueInfo = updateQueueInfo;
  }

  @XmlElementWrapper(name = "global-updates")
  public HashMap<String, String> getGlobalParams() {
    return global;
  }

  public void setGlobalParams(HashMap<String, String> globalInfo) {
    this.global = globalInfo;
  }

  public String getSubClusterId() {
    return subClusterId;
  }

  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}
