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

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerQueueInfo {

  @XmlTransient
  protected String queuePath;
  @XmlTransient
  protected Boolean isParent = false;

  // bit odd to store this but makes html easier for now
  @XmlTransient
  protected CSQueue queue;

  protected float capacity;
  protected float usedCapacity;
  protected float maxCapacity;
  protected String queueName;
  protected String state;
  protected ArrayList<CapacitySchedulerQueueInfo> subQueues;

  CapacitySchedulerQueueInfo() {
  };

  CapacitySchedulerQueueInfo(float cap, float used, float max, String name,
      String state, String path) {
    this.capacity = cap;
    this.usedCapacity = used;
    this.maxCapacity = max;
    this.queueName = name;
    this.state = state;
    this.queuePath = path;
  }

  public Boolean isParent() {
    return this.isParent;
  }

  public CSQueue getQueue() {
    return this.queue;
  }

  public float getCapacity() {
    return this.capacity;
  }

  public float getUsedCapacity() {
    return this.usedCapacity;
  }

  public float getMaxCapacity() {
    return this.maxCapacity;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public String getQueueState() {
    return this.state;
  }

  public String getQueuePath() {
    return this.queuePath;
  }

  public ArrayList<CapacitySchedulerQueueInfo> getSubQueues() {
    return this.subQueues;
  }

}
