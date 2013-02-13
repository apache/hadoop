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
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({CapacitySchedulerLeafQueueInfo.class})
public class CapacitySchedulerQueueInfo {

  @XmlTransient
  static final float EPSILON = 1e-8f;

  @XmlTransient
  protected String queuePath;

  protected float capacity;
  protected float usedCapacity;
  protected float maxCapacity;
  protected float absoluteCapacity;
  protected float absoluteMaxCapacity;
  protected float absoluteUsedCapacity;
  protected int numApplications;
  protected String usedResources;
  protected String queueName;
  protected QueueState state;
  protected CapacitySchedulerQueueInfoList queues;
  protected ResourceInfo resourcesUsed;

  CapacitySchedulerQueueInfo() {
  };

  CapacitySchedulerQueueInfo(CSQueue q) {
    queuePath = q.getQueuePath();
    capacity = q.getCapacity() * 100;
    usedCapacity = q.getUsedCapacity() * 100;

    maxCapacity = q.getMaximumCapacity();
    if (maxCapacity < EPSILON || maxCapacity > 1f)
      maxCapacity = 1f;
    maxCapacity *= 100;

    absoluteCapacity = cap(q.getAbsoluteCapacity(), 0f, 1f) * 100;
    absoluteMaxCapacity = cap(q.getAbsoluteMaximumCapacity(), 0f, 1f) * 100;
    absoluteUsedCapacity = cap(q.getAbsoluteUsedCapacity(), 0f, 1f) * 100;
    numApplications = q.getNumApplications();
    usedResources = q.getUsedResources().toString();
    queueName = q.getQueueName();
    state = q.getState();
    resourcesUsed = new ResourceInfo(q.getUsedResources());
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

  public float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  public float getAbsoluteMaxCapacity() {
    return absoluteMaxCapacity;
  }

  public float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  public int getNumApplications() {
    return numApplications;
  }

  public String getUsedResources() {
    return usedResources;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public String getQueueState() {
    return this.state.toString();
  }

  public String getQueuePath() {
    return this.queuePath;
  }

  public CapacitySchedulerQueueInfoList getQueues() {
    return this.queues;
  }

  public ResourceInfo getResourcesUsed() {
    return resourcesUsed;
  }

  /**
   * Limit a value to a specified range.
   * @param val the value to be capped
   * @param low the lower bound of the range (inclusive)
   * @param hi the upper bound of the range (inclusive)
   * @return the capped value
   */
  static float cap(float val, float low, float hi) {
    return Math.min(Math.max(val, low), hi);
  }
}
