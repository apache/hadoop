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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;

import java.util.Arrays;

/**
 * <p>
 * <code>BaseResource</code> extends Resource to handle base resources such
 * as memory and CPU.
 * TODO: We have a long term plan to use AbstractResource when additional
 * resource types are to be handled as well.
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
@Public
@Unstable
public class BaseResource extends Resource {

  private ResourceInformation memoryResInfo;
  private ResourceInformation vcoresResInfo;
  protected ResourceInformation[] resources = null;
  protected ResourceInformation[] readOnlyResources = null;

  // Number of mandatory resources, this is added to avoid invoke
  // MandatoryResources.values().length, since values() internally will
  // copy array, etc.
  private static final int NUM_MANDATORY_RESOURCES = 2;

  protected enum MandatoryResources {
    MEMORY(0), VCORES(1);

    private final int id;

    MandatoryResources(int id) {
      this.id = id;
    }

    public int getId() {
      return this.id;
    }
  }

  public BaseResource() {
    // Base constructor.
  }

  public BaseResource(long memory, long vcores) {
    this.memoryResInfo = ResourceInformation.newInstance(MEMORY,
        ResourceInformation.MEMORY_MB.getUnits(), memory);
    this.vcoresResInfo = ResourceInformation.newInstance(VCORES, "", vcores);

    resources = new ResourceInformation[NUM_MANDATORY_RESOURCES];
    readOnlyResources = new ResourceInformation[NUM_MANDATORY_RESOURCES];
    resources[MandatoryResources.MEMORY.id] = memoryResInfo;
    resources[MandatoryResources.VCORES.id] = vcoresResInfo;
    readOnlyResources = Arrays.copyOf(resources, resources.length);
  }

  @Override
  @SuppressWarnings("deprecation")
  public int getMemory() {
    return (int) memoryResInfo.getValue();
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
    return (int) vcoresResInfo.getValue();
  }

  @Override
  public void setVirtualCores(int vcores) {
    this.vcoresResInfo.setValue(vcores);
  }

  @Override
  public ResourceInformation[] getResources() {
    return readOnlyResources;
  }
}
