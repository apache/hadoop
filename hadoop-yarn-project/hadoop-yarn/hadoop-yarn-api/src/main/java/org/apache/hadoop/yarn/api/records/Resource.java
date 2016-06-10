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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
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
  public static Resource newInstance(long memory, long vCores) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    resource.setVirtualCores(vCores);
    return resource;
  }

  /**
   * This method is DEPRECATED:
   * Use {@link Resource#getMemorySize()} instead
   *
   * Get <em>memory</em> of the resource.
   * @return <em>memory</em> of the resource
   */
  @Public
  @Deprecated
  public abstract int getMemory();

  /**
   * Get <em>memory</em> of the resource.
   * @return <em>memory</em> of the resource
   */
  @Private
  @Unstable
  public abstract long getMemorySize();
  
  /**
   * Set <em>memory</em> of the resource.
   * @param memory <em>memory</em> of the resource
   */
  @Public
  @Stable
  public abstract void setMemory(long memory);


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

  @Public
  @Unstable
  public abstract long getVirtualCoresSize();
  
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
  public abstract void setVirtualCores(long vCores);

  @Override
  public int hashCode() {
    final int prime = 263167;

    int result = (int) (939769357
        + getMemorySize()); // prime * result = 939769357 initially
    result = prime * result + getVirtualCores();
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
    if (getMemorySize() != other.getMemorySize() ||
        getVirtualCores() != other.getVirtualCores()) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "<memory:" + getMemorySize() + ", vCores:" + getVirtualCores() + ">";
  }
}
