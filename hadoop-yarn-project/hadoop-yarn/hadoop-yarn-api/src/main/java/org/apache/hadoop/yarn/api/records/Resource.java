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
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>Resource</code> models a set of computer resources in the 
 * cluster.</p>
 * 
 * <p>Currently it models both <em>memory</em> and <em>CPU</em>.</p>
 * 
 * <p>The unit for memory is megabytes. CPU is modeled with virtual cores
 * (vcores), a unit for expressing parallelism. A node's capacity should
 * be configured with virtual cores equal to its number of physical cores. A
 * container should be requested with the number of cores it can saturate, i.e.
 * the average number of threads it expects to have runnable at a time.</p>
 * 
 * <p>Virtual cores take integer values and thus currently CPU-scheduling is
 * very coarse.  A complementary axis for CPU requests that represents processing
 * power will likely be added in the future to enable finer-grained resource
 * configuration.</p>
 * 
 * <p>Typically, applications request <code>Resource</code> of suitable
 * capability to run their component tasks.</p>
 * 
 * @see ResourceRequest
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */
@Public
@Stable
public abstract class Resource implements Comparable<Resource> {

  @Public
  @Stable
  public static Resource newInstance(int memory, int vCores) {
    return newInstance(memory, vCores, 0, 0);
  }

  @Public
  @Stable
  public static Resource newInstance(int memory, int vCores, int GPUs) {
    return newInstance(memory, vCores, GPUs, 0);
  }

  @Public
  @Stable
  public static Resource newInstance(int memory, int vCores, int GPUs, long GPUAttribute) {
    return newInstance(memory, vCores, GPUs, GPUAttribute, null);
  }

  @Public
  @Stable
  public static Resource newInstance(int memory, int vCores, int GPUs, long GPUAttribute, ValueRanges ports) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    resource.setVirtualCores(vCores);
    resource.setGPUs(GPUs);
    resource.setGPUAttribute(GPUAttribute);
    resource.setPorts(ports);
    return resource;
  }

  /**
   * Get <em>memory</em> of the resource.
   * @return <em>memory</em> of the resource
   */
  @Public
  @Stable
  public abstract int getMemory();
  
  /**
   * Set <em>memory</em> of the resource.
   * @param memory <em>memory</em> of the resource
   */
  @Public
  @Stable
  public abstract void setMemory(int memory);


  /**
   * Get <em>number of virtual cpu cores</em> of the resource.
   * 
   * Virtual cores are a unit for expressing CPU parallelism. A node's capacity
   * should be configured with virtual cores equal to its number of physical cores.
   * A container should be requested with the number of cores it can saturate, i.e.
   * the average number of threads it expects to have runnable at a time.
   *   
   * @return <em>num of virtual cpu cores</em> of the resource
   */
  @Public
  @Evolving
  public abstract int getVirtualCores();
  
  /**
   * Set <em>number of virtual cpu cores</em> of the resource.
   * 
   * Virtual cores are a unit for expressing CPU parallelism. A node's capacity
   * should be configured with virtual cores equal to its number of physical cores.
   * A container should be requested with the number of cores it can saturate, i.e.
   * the average number of threads it expects to have runnable at a time.
   *    
   * @param vCores <em>number of virtual cpu cores</em> of the resource
   */
  @Public
  @Evolving
  public abstract void setVirtualCores(int vCores);

  /**
   * Get <em>number of GPUs</em> of the resource.
   *
   * GPUs are a unit for expressing GPU parallelism. A node's capacity
   * should be configured with GPUs equal to its number of GPUs.
   * A container should be requested with the number of GPUs it can saturate, i.e.
   * the average number of GPU parallelism it expects to have runnable at a time.
   *
   * @return <em>number of GPUs</em> of the resource
   */
  @Public
  @Evolving
  public abstract int getGPUs();

  /**
   * Set <em>number of GPUs</em> of the resource.
   *
   * GPUs are a unit for expressing GPU parallelism. A node's capacity
   * should be configured with GPUs equal to its number of GPUs.
   * A container should be requested with the number of GPUs it can saturate, i.e.
   * the average number of GPU parallelism it expects to have runnable at a time.
   *
   * @param GPUs <em>number of GPUs</em> of the resource
   */
  @Public
  @Evolving
  public abstract void setGPUs(int GPUs);

  /**
   * Get <em> GPU locality preference information </em>.
   *
   * This abstracts GPU locality preference. Now, we have two types supported.
   * 0 means that GPUs can be placed anywhere in the machine, and
   * 1 means that GPUs are preferred to be placed in the same socket of the machine.
   *
   * @return <em>GPU locality preference information</em>
   */
  @Public
  @Evolving
  public abstract long getGPUAttribute();

  /**
   * Set <em>GPU allocation information</em>.
   *
   * This represents where assigned GPUs are placed using bit vector. Each bit indicates GPU id.
   * Bits set as 1 mean that corresponding GPUs are assigned, and
   * Bits set as 0 mean that corresponding GPUs are not unassigned.
   * The sum of 1s should equal to the number of GPUs.
   *
   * @param GPUAttribute <em>GPU locality preference information</em>
   */
  @Public
  @Evolving
  public abstract void setGPUAttribute(long GPUAttribute);


  /**
   * Get <em>ports</em> of the resource.
   * @return <em>ports</em> of the resource
   */
  @Public
  @Stable
  public abstract ValueRanges getPorts();

  /**
   * Set <em>ports</em> of the resource.
   * @param ports <em>ports</em> of the resource
   */
  @Public
  @Stable
  public abstract void setPorts(ValueRanges ports);

  /**
   * Get <em>portsCount</em> of the resource.
   * @return <em>portsCount</em> of the resource
   */

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result = 3571;
    result = 939769357 + getMemory(); // prime * result = 939769357 initially
    result = prime * result + getVirtualCores();
    result = prime * result + getGPUs();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Resource))
      return false;
    Resource other = (Resource) obj;
    if (getMemory() != other.getMemory() ||
        getVirtualCores() != other.getVirtualCores() ||
        getGPUs() != other.getGPUs() ||
        getPortsCount() != other.getPortsCount()) {
      return false;
    }
    return true;
  }

  public boolean equalsWithGPUAttribute(Object obj) {
    if (!this.equals(obj)) {
      return false;
    } else {
      Resource other = (Resource) obj;
      return this.getGPUAttribute() == other.getGPUAttribute();
    }
  }

  public boolean equalsWithPorts(Object obj) {
    if (!this.equalsWithGPUAttribute(obj)) {
      return false;
    } else {
      Resource other = (Resource) obj;
      ValueRanges lPorts = this.getPorts();
      ValueRanges rPorts = other.getPorts();
      if (lPorts == null) {
        return rPorts == null;
      } else {
        return lPorts.equals(rPorts);
      }
    }
  }

  @Override
  public String toString() {
    return "<memory:" + getMemory() + ", vCores:" + getVirtualCores() + ", GPUs:" + getGPUs() + ", GPUAttribute:" + getGPUAttribute() + ", ports: " + getPorts() + ">";
  }
}
