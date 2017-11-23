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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.shaded.com.google.common.reflect.ClassPath;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.impl.LightWeightResource;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
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

  protected ResourceInformation[] resources = null;

  // Number of mandatory resources, this is added to avoid invoke
  // MandatoryResources.values().length, since values() internally will
  // copy array, etc.
  protected static final int NUM_MANDATORY_RESOURCES = 2;

  @Private
  public static final int MEMORY_INDEX = 0;
  @Private
  public static final int VCORES_INDEX = 1;

  @Public
  @Stable
  public static Resource newInstance(int memory, int vCores) {
    return new LightWeightResource(memory, vCores);
  }

  @Public
  @Stable
  public static Resource newInstance(long memory, int vCores) {
    return new LightWeightResource(memory, vCores);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static Resource newInstance(Resource resource) {
    Resource ret;
    int numberOfKnownResourceTypes = ResourceUtils
        .getNumberOfKnownResourceTypes();
    if (numberOfKnownResourceTypes > 2) {
      ret = new LightWeightResource(resource.getMemorySize(),
          resource.getVirtualCores(), resource.getResources());
    } else {
      ret = new LightWeightResource(resource.getMemorySize(),
          resource.getVirtualCores());
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
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public ResourceInformation[] getResources() {
    return resources;
  }

  /**
   * Get list of resource information, this will be used by JAXB.
   * @return list of resources copy.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public List<ResourceInformation> getAllResourcesListCopy() {
    List<ResourceInformation> list = new ArrayList<>();
    for (ResourceInformation i : resources) {
      ResourceInformation ri = new ResourceInformation();
      ResourceInformation.copy(i, ri);
      list.add(ri);
    }
    return list;
  }

  /**
   * Get ResourceInformation for a specified resource.
   *
   * @param resource name of the resource
   * @return the ResourceInformation object for the resource
   * @throws ResourceNotFoundException if the resource can't be found
   */
  @Public
  @InterfaceStability.Unstable
  public ResourceInformation getResourceInformation(String resource)
      throws ResourceNotFoundException {
    Integer index = ResourceUtils.getResourceTypeIndex().get(resource);
    if (index != null) {
      return resources[index];
    }
    throw new ResourceNotFoundException("Unknown resource '" + resource
        + "'. Known resources are " + Arrays.toString(resources));
  }

  /**
   * Get ResourceInformation for a specified resource from a given index.
   *
   * @param index
   *          of the resource
   * @return the ResourceInformation object for the resource
   * @throws ResourceNotFoundException
   *           if the resource can't be found
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public ResourceInformation getResourceInformation(int index)
      throws ResourceNotFoundException {
    ResourceInformation ri = null;
    try {
      ri = resources[index];
    } catch (ArrayIndexOutOfBoundsException e) {
      throwExceptionWhenArrayOutOfBound(index);
    }
    return ri;
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
  @InterfaceStability.Unstable
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
  @InterfaceStability.Unstable
  public void setResourceInformation(String resource,
      ResourceInformation resourceInformation)
      throws ResourceNotFoundException {
    if (resource.equals(ResourceInformation.MEMORY_URI)) {
      this.setMemorySize(resourceInformation.getValue());
      return;
    }
    if (resource.equals(ResourceInformation.VCORES_URI)) {
      this.setVirtualCores((int) resourceInformation.getValue());
      return;
    }
    ResourceInformation storedResourceInfo = getResourceInformation(resource);
    ResourceInformation.copy(resourceInformation, storedResourceInfo);
  }

  /**
   * Set the ResourceInformation object for a particular resource.
   *
   * @param index
   *          the resource index for which the ResourceInformation is provided
   * @param resourceInformation
   *          ResourceInformation object
   * @throws ResourceNotFoundException
   *           if the resource is not found
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public void setResourceInformation(int index,
      ResourceInformation resourceInformation)
      throws ResourceNotFoundException {
    if (index < 0 || index >= resources.length) {
      throw new ResourceNotFoundException("Unknown resource at index '" + index
          + "'. Valid resources are " + Arrays.toString(resources));
    }
    ResourceInformation.copy(resourceInformation, resources[index]);
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
  @InterfaceStability.Unstable
  public void setResourceValue(String resource, long value)
      throws ResourceNotFoundException {
    if (resource.equals(ResourceInformation.MEMORY_URI)) {
      this.setMemorySize(value);
      return;
    }
    if (resource.equals(ResourceInformation.VCORES_URI)) {
      this.setVirtualCores((int)value);
      return;
    }

    ResourceInformation storedResourceInfo = getResourceInformation(resource);
    storedResourceInfo.setValue(value);
  }

  /**
   * Set the value of a resource in the ResourceInformation object. The unit of
   * the value is assumed to be the one in the ResourceInformation object.
   *
   * @param index
   *          the resource index for which the value is provided.
   * @param value
   *          the value to set
   * @throws ResourceNotFoundException
   *           if the resource is not found
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public void setResourceValue(int index, long value)
      throws ResourceNotFoundException {
    try {
      resources[index].setValue(value);
    } catch (ArrayIndexOutOfBoundsException e) {
      throwExceptionWhenArrayOutOfBound(index);
    }
  }

  protected void throwExceptionWhenArrayOutOfBound(int index) {
    String exceptionMsg = String.format(
        "Trying to access ResourceInformation for given index=%d. "
            + "Acceptable index range is [0,%d), please check double check "
            + "configured resources in resource-types.xml",
        index, ResourceUtils.getNumberOfKnownResourceTypes());

    throw new ResourceNotFoundException(exceptionMsg);
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

    ResourceInformation[] otherVectors = other.getResources();

    if (resources.length != otherVectors.length) {
      return false;
    }

    for (int i = 0; i < resources.length; i++) {
      ResourceInformation a = resources[i];
      ResourceInformation b = otherVectors[i];
      if ((a != b) && ((a == null) || !a.equals(b))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(Resource other) {
    ResourceInformation[] otherResources = other.getResources();

    int arrLenThis = this.resources.length;
    int arrLenOther = otherResources.length;

    // compare memory and vcores first(in that order) to preserve
    // existing behavior.
    for (int i = 0; i < arrLenThis; i++) {
      ResourceInformation otherEntry;
      try {
        otherEntry = otherResources[i];
      } catch (ArrayIndexOutOfBoundsException e) {
        // For two vectors with different size and same prefix. Shorter vector
        // goes first.
        return 1;
      }
      ResourceInformation entry = resources[i];

      long diff = entry.compareTo(otherEntry);
      if (diff > 0) {
        return 1;
      } else if (diff < 0) {
        return -1;
      }
    }

    if (arrLenThis < arrLenOther) {
      return -1;
    }

    return 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("<memory:").append(getMemorySize()).append(", vCores:")
        .append(getVirtualCores());

    for (int i = 2; i < resources.length; i++) {
      ResourceInformation ri = resources[i];
      if (ri.getValue() == 0) {
        continue;
      }
      sb.append(", ");
      sb.append(ri.getName()).append(": ")
          .append(ri.getValue());
      sb.append(ri.getUnits());
    }

    sb.append(">");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    int result = 0;
    for (ResourceInformation entry : resources) {
      result = prime * result + entry.hashCode();
    }
    return result;
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

  /**
   * Create ResourceInformation with basic fields.
   * @param name Resource Type Name
   * @param unit Default unit of provided resource type
   * @param value Value associated with giveb resource
   * @return ResourceInformation object
   */
  protected static ResourceInformation newDefaultInformation(String name,
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
}
