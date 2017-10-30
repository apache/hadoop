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

package org.apache.hadoop.yarn.api.records.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_MB;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;

/**
 * <p>
 * <code>LightResource</code> extends Resource to handle base resources such
 * as memory and CPU.
 * TODO: We have a long term plan to use AbstractResource when additional
 * resource types are to be handled as well.
 * This will be used to speed up internal calculation to avoid creating
 * costly PB-backed Resource object: <code>ResourcePBImpl</code>
 * </p>
 *
 * <p>
 * Currently it models both <em>memory</em> and <em>CPU</em>.
 * </p>
 *
 * <p>
 * The unit for memory is megabytes. CPU is modeled with virtual cores (vcores),
 * a unit for expressing parallelism. A node's capacity should be configured
 * with virtual cores equal to its number of physical cores. A container should
 * be requested with the number of cores it can saturate, i.e. the average
 * number of threads it expects to have runnable at a time.
 * </p>
 *
 * <p>
 * Virtual cores take integer values and thus currently CPU-scheduling is very
 * coarse. A complementary axis for CPU requests that represents processing
 * power will likely be added in the future to enable finer-grained resource
 * configuration.
 * </p>
 *
 * @see Resource
 */
@InterfaceAudience.Private
@Unstable
public class LightWeightResource extends Resource {

  private ResourceInformation memoryResInfo;
  private ResourceInformation vcoresResInfo;

  public LightWeightResource(long memory, long vcores) {
    this.memoryResInfo = LightWeightResource.newDefaultInformation(MEMORY_URI,
        MEMORY_MB.getUnits(), memory);
    this.vcoresResInfo = LightWeightResource.newDefaultInformation(VCORES_URI,
        "", vcores);

    resources = new ResourceInformation[NUM_MANDATORY_RESOURCES];
    resources[MEMORY_INDEX] = memoryResInfo;
    resources[VCORES_INDEX] = vcoresResInfo;
  }

  private static ResourceInformation newDefaultInformation(String name,
      String unit, long value) {
    ResourceInformation ri = new ResourceInformation();
    ri.setName(name);
    ri.setValue(value);
    ri.setResourceType(ResourceTypes.COUNTABLE);
    ri.setUnitsWithoutValidation(unit);
    ri.setMinimumAllocation(0);
    ri.setMaximumAllocation(Long.MAX_VALUE);
    return ri;
  }

  @Override
  @SuppressWarnings("deprecation")
  public int getMemory() {
    return castToIntSafely(memoryResInfo.getValue());
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setMemory(int memory) {
    this.memoryResInfo.setValue(memory);
  }

  @Override
  public long getMemorySize() {
    return memoryResInfo.getValue();
  }

  @Override
  public void setMemorySize(long memory) {
    this.memoryResInfo.setValue(memory);
  }

  @Override
  public int getVirtualCores() {
    return castToIntSafely(vcoresResInfo.getValue());
  }

  @Override
  public void setVirtualCores(int vcores) {
    this.vcoresResInfo.setValue(vcores);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !(obj instanceof Resource)) {
      return false;
    }
    Resource other = (Resource) obj;
    if (getMemorySize() != other.getMemorySize()
        || getVirtualCores() != other.getVirtualCores()) {
      return false;
    }

    return true;
  }

  @Override
  public int compareTo(Resource other) {
    // compare memory and vcores first(in that order) to preserve
    // existing behaviour
    long diff = this.getMemorySize() - other.getMemorySize();
    if (diff == 0) {
      return this.getVirtualCores() - other.getVirtualCores();
    } else if (diff > 0){
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    return prime * (prime + Long.hashCode(getMemorySize())) + getVirtualCores();
  }
}
