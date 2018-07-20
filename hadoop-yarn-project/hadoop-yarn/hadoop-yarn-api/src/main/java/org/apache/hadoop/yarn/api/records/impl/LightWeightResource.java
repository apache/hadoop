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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.*;

/**
 * <p>
 * <code>LightWeightResource</code> extends Resource to handle base resources such
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
@Private
@Unstable
public class LightWeightResource extends Resource {

  private ResourceInformation memoryResInfo;
  private ResourceInformation vcoresResInfo;

  public LightWeightResource(long memory, int vcores) {
    int numberOfKnownResourceTypes = ResourceUtils
        .getNumberOfKnownResourceTypes();
    initResourceInformations(memory, vcores, numberOfKnownResourceTypes);

    if (numberOfKnownResourceTypes > 2) {
      ResourceInformation[] types = ResourceUtils.getResourceTypesArray();
      for (int i = 2; i < numberOfKnownResourceTypes; i++) {
        resources[i] = new ResourceInformation();
        ResourceInformation.copy(types[i], resources[i]);
      }
    }
  }

  public LightWeightResource(long memory, int vcores,
      ResourceInformation[] source) {
    int numberOfKnownResourceTypes = ResourceUtils
        .getNumberOfKnownResourceTypes();
    initResourceInformations(memory, vcores, numberOfKnownResourceTypes);

    for (int i = 2; i < numberOfKnownResourceTypes; i++) {
      resources[i] = new ResourceInformation();
      ResourceInformation.copy(source[i], resources[i]);
    }
  }

  private void initResourceInformations(long memory, long vcores,
      int numberOfKnownResourceTypes) {
    this.memoryResInfo = newDefaultInformation(MEMORY_URI, MEMORY_MB.getUnits(),
        memory);
    this.vcoresResInfo = newDefaultInformation(VCORES_URI, VCORES.getUnits(),
        vcores);

    resources = new ResourceInformation[numberOfKnownResourceTypes];
    resources[MEMORY_INDEX] = memoryResInfo;
    resources[VCORES_INDEX] = vcoresResInfo;
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

    if (resources.length > 2) {
      ResourceInformation[] otherVectors = other.getResources();

      if (resources.length != otherVectors.length) {
        return false;
      }

      for (int i = 2; i < resources.length; i++) {
        ResourceInformation a = resources[i];
        ResourceInformation b = otherVectors[i];
        if ((a != b) && ((a == null) || !a.equals(b))) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public int compareTo(Resource other) {
    // compare memory and vcores first(in that order) to preserve
    // existing behavior.
    if (resources.length <= 2) {
      long diff = this.getMemorySize() - other.getMemorySize();
      if (diff == 0) {
        return this.getVirtualCores() - other.getVirtualCores();
      } else if (diff > 0) {
        return 1;
      } else {
        return -1;
      }
    }

    return super.compareTo(other);
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    return prime * (prime + Long.hashCode(getMemorySize())) + getVirtualCores();
  }
}
