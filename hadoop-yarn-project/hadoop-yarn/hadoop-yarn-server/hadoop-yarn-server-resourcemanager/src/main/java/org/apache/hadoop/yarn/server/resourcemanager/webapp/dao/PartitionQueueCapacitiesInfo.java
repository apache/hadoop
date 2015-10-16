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

/**
 * This class represents queue capacities for a given partition
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionQueueCapacitiesInfo {
  private String partitionName;

  private float capacity;
  private float usedCapacity;
  private float maxCapacity = 100;
  private float absoluteCapacity;
  private float absoluteUsedCapacity;
  private float absoluteMaxCapacity  = 100;

  public PartitionQueueCapacitiesInfo() {
  }

  public PartitionQueueCapacitiesInfo(String partitionName, float capacity,
      float usedCapacity, float maxCapacity, float absCapacity,
      float absUsedCapacity, float absMaxCapacity) {
    super();
    this.partitionName = partitionName;
    this.capacity = capacity;
    this.usedCapacity = usedCapacity;
    this.maxCapacity = maxCapacity;
    this.absoluteCapacity = absCapacity;
    this.absoluteUsedCapacity = absUsedCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
  }

  public float getCapacity() {
    return capacity;
  }

  public void setCapacity(float capacity) {
    this.capacity = capacity;
  }

  public float getUsedCapacity() {
    return usedCapacity;
  }

  public void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }

  public float getMaxCapacity() {
    return maxCapacity;
  }

  public void setMaxCapacity(float maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  public void setAbsoluteCapacity(float absoluteCapacity) {
    this.absoluteCapacity = absoluteCapacity;
  }

  public float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  public void setAbsoluteUsedCapacity(float absoluteUsedCapacity) {
    this.absoluteUsedCapacity = absoluteUsedCapacity;
  }

  public float getAbsoluteMaxCapacity() {
    return absoluteMaxCapacity;
  }

  public void setAbsoluteMaxCapacity(float absoluteMaxCapacity) {
    this.absoluteMaxCapacity = absoluteMaxCapacity;
  }
}