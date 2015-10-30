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
 * This class represents queue/user resource usage info for a given partition
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionResourceUsageInfo {
  private String partitionName;
  private ResourceInfo used = new ResourceInfo();
  private ResourceInfo reserved;
  private ResourceInfo pending;
  private ResourceInfo amUsed;

  public PartitionResourceUsageInfo() {
  }

  public PartitionResourceUsageInfo(String partitionName, ResourceInfo used,
      ResourceInfo reserved, ResourceInfo pending,
      ResourceInfo amResourceUsed) {
    super();
    this.partitionName = partitionName;
    this.used = used;
    this.reserved = reserved;
    this.pending = pending;
    this.amUsed = amResourceUsed;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public ResourceInfo getUsed() {
    return used;
  }

  public void setUsed(ResourceInfo used) {
    this.used = used;
  }

  public ResourceInfo getReserved() {
    return reserved;
  }

  public void setReserved(ResourceInfo reserved) {
    this.reserved = reserved;
  }

  public ResourceInfo getPending() {
    return pending;
  }

  public void setPending(ResourceInfo pending) {
    this.pending = pending;
  }

  public ResourceInfo getAmUsed() {
    return amUsed;
  }

  public void setAmUsed(ResourceInfo amResourceUsed) {
    this.amUsed = amResourceUsed;
  }
}
