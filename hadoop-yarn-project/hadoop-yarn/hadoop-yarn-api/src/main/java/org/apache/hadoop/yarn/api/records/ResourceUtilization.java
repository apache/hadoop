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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * <code>ResourceUtilization</code> models the utilization of a set of computer
 * resources in the cluster.
 * </p>
 */
@Public
@Unstable
public abstract class ResourceUtilization implements
    Comparable<ResourceUtilization> {

  @Public
  @Unstable
  public static ResourceUtilization newInstance(int pmem, int vmem, float cpu) {
    ResourceUtilization utilization =
        Records.newRecord(ResourceUtilization.class);
    utilization.setPhysicalMemory(pmem);
    utilization.setVirtualMemory(vmem);
    utilization.setCPU(cpu);
    return utilization;
  }

  @Public
  @Unstable
  public static ResourceUtilization newInstance(
      ResourceUtilization resourceUtil) {
    return newInstance(resourceUtil.getPhysicalMemory(),
        resourceUtil.getVirtualMemory(), resourceUtil.getCPU());
  }

  /**
   * Get used <em>virtual memory</em>.
   *
   * @return <em>virtual memory</em> in MB
   */
  @Public
  @Unstable
  public abstract int getVirtualMemory();

  /**
   * Set used <em>virtual memory</em>.
   *
   * @param vmem <em>virtual memory</em> in MB
   */
  @Public
  @Unstable
  public abstract void setVirtualMemory(int vmem);

  /**
   * Get <em>physical memory</em>.
   *
   * @return <em>physical memory</em> in MB
   */
  @Public
  @Unstable
  public abstract int getPhysicalMemory();

  /**
   * Set <em>physical memory</em>.
   *
   * @param pmem <em>physical memory</em> in MB
   */
  @Public
  @Unstable
  public abstract void setPhysicalMemory(int pmem);

  /**
   * Get <em>CPU</em> utilization.
   *
   * @return <em>CPU utilization</em> normalized to 1 CPU
   */
  @Public
  @Unstable
  public abstract float getCPU();

  /**
   * Set <em>CPU</em> utilization.
   *
   * @param cpu <em>CPU utilization</em> normalized to 1 CPU
   */
  @Public
  @Unstable
  public abstract void setCPU(float cpu);

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result = 3571;
    result = prime * result + getVirtualMemory();
    result = prime * result + getPhysicalMemory();
    result = 31 * result + Float.valueOf(getCPU()).hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ResourceUtilization)) {
      return false;
    }
    ResourceUtilization other = (ResourceUtilization) obj;
    if (getVirtualMemory() != other.getVirtualMemory()
        || getPhysicalMemory() != other.getPhysicalMemory()
        || getCPU() != other.getCPU()) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "<pmem:" + getPhysicalMemory() + ", vmem:" + getVirtualMemory()
        + ", vCores:" + getCPU() + ">";
  }

  /**
   * Add utilization to the current one.
   * @param pmem Physical memory used to add.
   * @param vmem Virtual memory used to add.
   * @param cpu CPU utilization to add.
   */
  @Public
  @Unstable
  public void addTo(int pmem, int vmem, float cpu) {
    this.setPhysicalMemory(this.getPhysicalMemory() + pmem);
    this.setVirtualMemory(this.getVirtualMemory() + vmem);
    this.setCPU(this.getCPU() + cpu);
  }

  /**
   * Subtract utilization from the current one.
   * @param pmem Physical memory to be subtracted.
   * @param vmem Virtual memory to be subtracted.
   * @param cpu CPU utilization to be subtracted.
   */
  @Public
  @Unstable
  public void subtractFrom(int pmem, int vmem, float cpu) {
    this.setPhysicalMemory(this.getPhysicalMemory() - pmem);
    this.setVirtualMemory(this.getVirtualMemory() - vmem);
    this.setCPU(this.getCPU() - cpu);
  }
}