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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

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
  private float maxAMLimitPercentage;
  private ResourceInfo configuredMinResource;
  private ResourceInfo configuredMaxResource;
  private ResourceInfo effectiveMinResource;
  private ResourceInfo effectiveMaxResource;

  public PartitionQueueCapacitiesInfo() {
  }

  public PartitionQueueCapacitiesInfo(String partitionName, float capacity,
      float usedCapacity, float maxCapacity, float absCapacity,
      float absUsedCapacity, float absMaxCapacity, float maxAMLimitPercentage,
      Resource confMinRes, Resource confMaxRes, Resource effMinRes,
      Resource effMaxRes) {
    super();
    this.partitionName = partitionName;
    this.capacity = capacity;
    this.usedCapacity = usedCapacity;
    this.maxCapacity = maxCapacity;
    this.absoluteCapacity = absCapacity;
    this.absoluteUsedCapacity = absUsedCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
    this.maxAMLimitPercentage = maxAMLimitPercentage;
    this.configuredMinResource = new ResourceInfo(confMinRes);
    this.configuredMaxResource = new ResourceInfo(confMaxRes);
    this.effectiveMinResource = new ResourceInfo(effMinRes);
    this.effectiveMaxResource = new ResourceInfo(effMaxRes);
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

  public float getMaxAMLimitPercentage() {
    return maxAMLimitPercentage;
  }

  public void setMaxAMLimitPercentage(float maxAMLimitPercentage) {
    this.maxAMLimitPercentage = maxAMLimitPercentage;
  }

  public ResourceInfo getConfiguredMinResource() {
    return configuredMinResource;
  }

  public ResourceInfo getConfiguredMaxResource() {
    if (configuredMaxResource == null
        || configuredMaxResource.getResource().equals(Resources.none())) {
      return null;
    }
    return configuredMaxResource;
  }

  public ResourceInfo getEffectiveMinResource() {
    return effectiveMinResource;
  }

  public ResourceInfo getEffectiveMaxResource() {
    return effectiveMaxResource;
  }
}