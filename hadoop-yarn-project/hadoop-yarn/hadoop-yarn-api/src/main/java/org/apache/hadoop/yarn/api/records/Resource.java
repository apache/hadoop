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

import java.util.Arrays;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.impl.BaseResource;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

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
 * very coarse.  A complementary axis for CPU requests that represents
 * processing power will likely be added in the future to enable finer-grained
 * resource configuration.</p>
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

  protected static final String MEMORY = ResourceInformation.MEMORY_MB.getName();
  protected static final String VCORES = ResourceInformation.VCORES.getName();

  @Public
  @Stable
  public static Resource newInstance(int memory, int vCores) {
    if (ResourceUtils.getResourceTypesArray().length > 2) {
      Resource ret = Records.newRecord(Resource.class);
      ret.setMemorySize(memory);
      ret.setVirtualCores(vCores);
      return ret;
    }
    return new BaseResource(memory, vCores);
  }

  @Public
  @Stable
  public static Resource newInstance(long memory, int vCores) {
    if (ResourceUtils.getResourceTypesArray().length > 2) {
      Resource ret = Records.newRecord(Resource.class);
      ret.setMemorySize(memory);
      ret.setVirtualCores(vCores);
      return ret;
    }
    return new BaseResource(memory, vCores);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static Resource newInstance(Resource resource) {
    Resource ret = Resource.newInstance(resource.getMemorySize(),
        resource.getVirtualCores());
    if (ResourceUtils.getResourceTypesArray().length > 2) {
      Resource.copy(resource, ret);
    }
    return ret;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static void copy(Resource source, Resource dest) {
    for (ResourceInformation entry : source.getResources()) {
      dest.setResourceInformation(entry.getName(), entry);
    }
  }

  /**
   * This method is DEPRECATED:
   * Use {@link Resource#getMemorySize()} instead
   *
   * Get <em>memory</em> of the resource. Note - while memory has
   * never had a unit specified, all YARN configurations have specified memory
   * in MB. The assumption has been that the daemons and applications are always
   * using the same units. With the introduction of the ResourceInformation
   * class we have support for units - so this function will continue to return
   * memory but in the units of MB
   *
   * @return <em>memory</em>(in MB) of the resource
   */
  @Public
  @Deprecated
  public abstract int getMemory();

  /**
   * Get <em>memory</em> of the resource. Note - while memory has
   * never had a unit specified, all YARN configurations have specified memory
   * in MB. The assumption has been that the daemons and applications are always
   * using the same units. With the introduction of the ResourceInformation
   * class we have support for units - so this function will continue to return
   * memory but in the units of MB
   *
   * @return <em>memory</em> of the resource
   */
  @Public
  @Stable
  public long getMemorySize() {
    throw new NotImplementedException(
        "This method is implemented by ResourcePBImpl");
  }

  /**
   * Set <em>memory</em> of the resource. Note - while memory has
   * never had a unit specified, all YARN configurations have specified memory
   * in MB. The assumption has been that the daemons and applications are always
   * using the same units. With the introduction of the ResourceInformation
   * class we have support for units - so this function will continue to set
   * memory but the assumption is that the value passed is in units of MB.
   *
   * @param memory <em>memory</em>(in MB) of the resource
   */
  @Public
  @Deprecated
  public abstract void setMemory(int memory);

  /**
   * Set <em>memory</em> of the resource.
   * @param memory <em>memory</em> of the resource
   */
  @Public
  @Stable
  public void setMemorySize(long memory) {
    throw new NotImplementedException(
        "This method is implemented by ResourcePBImpl");
  }


  /**
   * Get <em>number of virtual cpu cores</em> of the resource.
   * 
   * Virtual cores are a unit for expressing CPU parallelism. A node's capacity
   * should be configured with virtual cores equal to its number of physical
   * cores. A container should be requested with the number of cores it can
   * saturate, i.e. the average number of threads it expects to have runnable
   * at a time.
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
   * should be configured with virtual cores equal to its number of physical
   * cores. A container should be requested with the number of cores it can
   * saturate, i.e. the average number of threads it expects to have runnable
   * at a time.
   *
   * @param vCores <em>number of virtual cpu cores</em> of the resource
   */
  @Public
  @Evolving
  public abstract void setVirtualCores(int vCores);

  /**
   * Get ResourceInformation for all resources.
   *
   * @return Map of resource name to ResourceInformation
   */
  @Public
  @Evolving
  public abstract ResourceInformation[] getResources();

  /**
   * Get ResourceInformation for a specified resource.
   *
   * @param resource name of the resource
   * @return the ResourceInformation object for the resource
   * @throws ResourceNotFoundException if the resource can't be found
   */
  @Public
  @Evolving
  public ResourceInformation getResourceInformation(String resource)
      throws ResourceNotFoundException {
    Integer index = ResourceUtils.getResourceTypeIndex().get(resource);
    ResourceInformation[] resources = getResources();
    if (index != null) {
      return resources[index];
    }
    throw new ResourceNotFoundException("Unknown resource '" + resource
        + "'. Known resources are " + Arrays.toString(resources));
  }

  /**
   * Get the value for a specified resource. No information about the units is
   * returned.
   *
   * @param resource name of the resource
   * @return the value for the resource
   * @throws ResourceNotFoundException if the resource can't be found
   */
  @Public
  @Evolving
  public long getResourceValue(String resource)
      throws ResourceNotFoundException {
    return getResourceInformation(resource).getValue();
  }

  /**
   * Set the ResourceInformation object for a particular resource.
   *
   * @param resource the resource for which the ResourceInformation is provided
   * @param resourceInformation ResourceInformation object
   * @throws ResourceNotFoundException if the resource is not found
   */
  @Public
  @Evolving
  public void setResourceInformation(String resource,
      ResourceInformation resourceInformation)
      throws ResourceNotFoundException {
    if (resource.equals(MEMORY)) {
      this.setMemorySize(resourceInformation.getValue());
      return;
    }
    if (resource.equals(VCORES)) {
      this.setVirtualCores((int) resourceInformation.getValue());
      return;
    }
    ResourceInformation storedResourceInfo = getResourceInformation(resource);
    ResourceInformation.copy(resourceInformation, storedResourceInfo);
  }

  /**
   * Set the value of a resource in the ResourceInformation object. The unit of
   * the value is assumed to be the one in the ResourceInformation object.
   *
   * @param resource the resource for which the value is provided.
   * @param value    the value to set
   * @throws ResourceNotFoundException if the resource is not found
   */
  @Public
  @Evolving
  public void setResourceValue(String resource, long value)
      throws ResourceNotFoundException {
    if (resource.equals(MEMORY)) {
      this.setMemorySize(value);
      return;
    }
    if (resource.equals(VCORES)) {
      this.setVirtualCores((int)value);
      return;
    }

    ResourceInformation storedResourceInfo = getResourceInformation(resource);
    storedResourceInfo.setValue(value);
  }

  @Override
  public int hashCode() {
    final int prime = 263167;

    int result = (int) (939769357
        + getMemorySize()); // prime * result = 939769357 initially
    result = prime * result + getVirtualCores();
    for (ResourceInformation entry : getResources()) {
      if (!entry.getName().equals(MEMORY) && !entry.getName().equals(VCORES)) {
        result = prime * result + entry.hashCode();
      }
    }
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
    if (!(obj instanceof Resource)) {
      return false;
    }
    Resource other = (Resource) obj;
    if (getMemorySize() != other.getMemorySize()
        || getVirtualCores() != other.getVirtualCores()) {
      return false;
    }

    ResourceInformation[] myVectors = getResources();
    ResourceInformation[] otherVectors = other.getResources();

    if (myVectors.length != otherVectors.length) {
      return false;
    }

    for (int i = 0; i < myVectors.length; i++) {
      ResourceInformation a = myVectors[i];
      ResourceInformation b = otherVectors[i];
      if ((a != b) && ((a == null) || !a.equals(b))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<memory:").append(getMemorySize()).append(", vCores:")
        .append(getVirtualCores());
    for (ResourceInformation entry : getResources()) {
      if (entry.getName().equals(MEMORY)
          && entry.getUnits()
          .equals(ResourceInformation.MEMORY_MB.getUnits())) {
        continue;
      }
      if (entry.getName().equals(VCORES)
          && entry.getUnits()
          .equals(ResourceInformation.VCORES.getUnits())) {
        continue;
      }
      sb.append(", ").append(entry.getName()).append(": ")
          .append(entry.getValue())
          .append(entry.getUnits());
    }
    sb.append(">");
    return sb.toString();
  }

  @Override
  public int compareTo(Resource other) {
    ResourceInformation[] thisResources = this.getResources();
    ResourceInformation[] otherResources = other.getResources();

    // compare memory and vcores first(in that order) to preserve
    // existing behaviour
    long diff = this.getMemorySize() - other.getMemorySize();
    if (diff == 0) {
      diff = this.getVirtualCores() - other.getVirtualCores();
    }
    if (diff == 0) {
      diff = thisResources.length - otherResources.length;
      if (diff == 0) {
        int maxLength = ResourceUtils.getResourceTypesArray().length;
        for (int i = 0; i < maxLength; i++) {
          // For memory and vcores, we can skip the loop as it's already
          // compared.
          if (i < 2) {
            continue;
          }

          ResourceInformation entry = thisResources[i];
          ResourceInformation otherEntry = otherResources[i];
          if (entry.getName().equals(otherEntry.getName())) {
            diff = entry.compareTo(otherEntry);
            if (diff != 0) {
              break;
            }
          }
        }
      }
    }
    return Long.compare(diff, 0);
  }

  /**
   * Convert long to int for a resource value safely. This method assumes
   * resource value is positive.
   *
   * @param value long resource value
   * @return int resource value
   */
  protected static int castToIntSafely(long value) {
    if (value > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return Long.valueOf(value).intValue();
  }
}
