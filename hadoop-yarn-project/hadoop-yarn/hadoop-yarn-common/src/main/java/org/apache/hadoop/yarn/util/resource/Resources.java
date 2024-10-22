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

package org.apache.hadoop.yarn.util.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;

/**
 * Resources is a computation class which provides a set of apis to do
 * mathematical operations on Resource object.
 */
@LimitedPrivate({ "YARN", "MapReduce" })
@Unstable
public class Resources {

  private enum RoundingDirection { UP, DOWN }

  private static final Logger LOG =
      LoggerFactory.getLogger(Resources.class);

  /**
   * Helper class to create a resource with a fixed value for all resource
   * types. For example, a NONE resource which returns 0 for any resource type.
   */
  @Private
  @Unstable
  static class FixedValueResource extends Resource {

    private final long resourceValue;
    private String name;

    /**
     * Constructor for a fixed value resource.
     * @param rName the name of the resource
     * @param value the fixed value to be returned for all resource types
     */
    FixedValueResource(String rName, long value) {
      this.resourceValue = value;
      this.name = rName;
      initResourceMap();
    }

    @Override
    @SuppressWarnings("deprecation")
    public int getMemory() {
      return castToIntSafely(resourceValue);
    }

    @Override
    public long getMemorySize() {
      return this.resourceValue;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setMemory(int memory) {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public void setMemorySize(long memory) {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return castToIntSafely(resourceValue);
    }

    @Override
    public void setVirtualCores(int virtualCores) {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public void setResourceInformation(int index,
        ResourceInformation resourceInformation)
        throws ResourceNotFoundException {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public void setResourceValue(int index, long value)
        throws ResourceNotFoundException {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public void setResourceInformation(String resource,
        ResourceInformation resourceInformation) {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public void setResourceValue(String resource, long value) {
      throw new RuntimeException(name + " cannot be modified!");
    }

    /*
     *  FixedValueResource cannot be updated when any resource types refresh
     *  by using approach introduced by YARN-7307 and do operations like
     *  Resources.compare(resource_x, Resources.none()) will throw exceptions.
     *
     *  That's why we do reinitialize resource maps for following methods.
     */

    @Override
    public ResourceInformation getResourceInformation(int index)
        throws ResourceNotFoundException {
      ResourceInformation ri = null;
      try {
        ri = super.getResourceInformation(index);
      } catch (ResourceNotFoundException e) {
        // Retry once to reinitialize resource information.
        initResourceMap();
        try {
          return super.getResourceInformation(index);
        } catch (ResourceNotFoundException ee) {
          throwExceptionWhenArrayOutOfBound(index);
        }
      }
      return ri;
    }

    @Override
    public ResourceInformation getResourceInformation(String resource)
        throws ResourceNotFoundException {
      ResourceInformation ri;
      try {
        ri = super.getResourceInformation(resource);
      } catch (ResourceNotFoundException e) {
        // Retry once to reinitialize resource information.
        initResourceMap();
        try {
          return super.getResourceInformation(resource);
        } catch (ResourceNotFoundException ee) {
          throw ee;
        }
      }
      return ri;
    }

    @Override
    public ResourceInformation[] getResources() {
      if (resources.length != ResourceUtils.getNumberOfKnownResourceTypes()) {
        // Retry once to reinitialize resource information.
        initResourceMap();
        if (resources.length != ResourceUtils.getNumberOfKnownResourceTypes()) {
          throw new ResourceNotFoundException("Failed to reinitialize "
              + "FixedValueResource to get number of resource types same "
              + "as configured");
        }
      }

      return resources;
    }

    private void initResourceMap() {
      ResourceInformation[] types = ResourceUtils.getResourceTypesArray();
      if (types != null) {
        resources = new ResourceInformation[types.length];
        for (int index = 0; index < types.length; index++) {
          resources[index] = ResourceInformation.newInstance(types[index]);
          resources[index].setValue(resourceValue);
        }
      }
    }
  }

  public static Resource createResource(int memory) {
    return createResource(memory, (memory > 0) ? 1 : 0);
  }

  public static Resource createResource(int memory, int cores) {
    return Resource.newInstance(memory, cores);
  }

  private static final Resource UNBOUNDED =
      new FixedValueResource("UNBOUNDED", Long.MAX_VALUE);

  private static final Resource NONE = new FixedValueResource("NONE", 0L);

  public static Resource createResource(long memory) {
    return createResource(memory, (memory > 0) ? 1 : 0);
  }

  public static Resource createResource(long memory, int cores) {
    return Resource.newInstance(memory, cores);
  }

  public static Resource none() {
    return NONE;
  }

  /**
   * Check whether a resource object is empty (0 memory and 0 virtual cores).
   * @param other The resource to check
   * @return {@code true} if {@code other} has 0 memory and 0 virtual cores,
   * {@code false} otherwise
   */
  public static boolean isNone(Resource other) {
    return NONE.equals(other);
  }

  public static Resource unbounded() {
    return UNBOUNDED;
  }

  public static Resource clone(Resource res) {
    return Resource.newInstance(res);
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(i);
        ResourceInformation lhsValue = lhs.getResourceInformation(i);
        lhs.setResourceValue(i, lhsValue.getValue() + rhsValue.getValue());
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
        continue;
      }
    }
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(i);
        ResourceInformation lhsValue = lhs.getResourceInformation(i);
        lhs.setResourceValue(i, lhsValue.getValue() - rhsValue.getValue());
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
        continue;
      }
    }
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  /**
   * Subtract {@code rhs} from {@code lhs} and reset any negative values to
   * zero. This call will modify {@code lhs}.
   *
   * @param lhs {@link Resource} to subtract from
   * @param rhs {@link Resource} to subtract
   * @return the value of lhs after subtraction
   */
  public static Resource subtractFromNonNegative(Resource lhs, Resource rhs) {
    subtractFrom(lhs, rhs);
    if (lhs.getMemorySize() < 0) {
      lhs.setMemorySize(0);
    }
    if (lhs.getVirtualCores() < 0) {
      lhs.setVirtualCores(0);
    }
    return lhs;
  }

  /**
   * Subtract {@code rhs} from {@code lhs} and reset any negative values to
   * zero. This call will operate on a copy of {@code lhs}, leaving {@code lhs}
   * unmodified.
   *
   * @param lhs {@link Resource} to subtract from
   * @param rhs {@link Resource} to subtract
   * @return the value of lhs after subtraction
   */
  public static Resource subtractNonNegative(Resource lhs, Resource rhs) {
    return subtractFromNonNegative(clone(lhs), rhs);
  }

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, double by) {
    return multiplyAndRound(lhs, by, RoundingDirection.DOWN);
  }

  public static Resource multiply(Resource lhs, double by) {
    return multiplyTo(clone(lhs), by);
  }

  /**
   * Multiply {@code rhs} by {@code by}, and add the result to {@code lhs}
   * without creating any new {@link Resource} object.
   * @param lhs {@link Resource} to subtract from.
   * @param rhs {@link Resource} to subtract.
   * @param by multiplier.
   * @return instance of Resource.
   */
  public static Resource multiplyAndAddTo(
      Resource lhs, Resource rhs, double by) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(i);
        ResourceInformation lhsValue = lhs.getResourceInformation(i);

        long convertedRhs = (long) (rhsValue.getValue() * by);
        lhs.setResourceValue(i, lhsValue.getValue() + convertedRhs);
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
      }
    }
    return lhs;
  }

  public static Resource multiplyAndNormalizeUp(ResourceCalculator calculator,
      Resource lhs, double[] by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }

  public static Resource multiplyAndNormalizeUp(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }
  
  public static Resource multiplyAndNormalizeDown(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeDown(lhs, by, factor);
  }

  /**
   * Multiply {@code lhs} by {@code by}, and set the result rounded down into a
   * cloned version of {@code lhs} Resource object.
   * @param lhs Resource object
   * @param by Multiply values by this value
   * @return A cloned version of {@code lhs} with updated values
   */
  public static Resource multiplyAndRoundDown(Resource lhs, double by) {
    return multiplyAndRound(clone(lhs), by, RoundingDirection.DOWN);
  }

  /**
   * Multiply {@code lhs} by {@code by}, and set the result rounded up into a
   * cloned version of {@code lhs} Resource object.
   * @param lhs Resource object
   * @param by Multiply values by this value
   * @return A cloned version of {@code lhs} with updated values
   */
  public static Resource multiplyAndRoundUp(Resource lhs, double by) {
    return multiplyAndRound(clone(lhs), by, RoundingDirection.UP);
  }

  /**
   * Multiply {@code lhs} by {@code by}, and set the result according to
   * the rounding direction to {@code lhs}
   * without creating any new {@link Resource} object.
   * @param lhs Resource object
   * @param by Multiply values by this value
   * @return Returns {@code lhs} itself (without cloning) with updated values
   */
  private static Resource multiplyAndRound(Resource lhs, double by,
      RoundingDirection roundingDirection) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation lhsValue = lhs.getResourceInformation(i);

        final long value;
        if (roundingDirection == RoundingDirection.DOWN) {
          value = (long) (lhsValue.getValue() * by);
        } else {
          value = (long) Math.ceil(lhsValue.getValue() * by);
        }
        lhs.setResourceValue(i, value);
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
      }
    }
    return lhs;
  }

  public static Resource normalize(
      ResourceCalculator calculator, Resource lhs, Resource min,
      Resource max, Resource increment) {
    return calculator.normalize(lhs, min, max, increment);
  }
  
  public static Resource roundUp(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundUp(lhs, factor);
  }
  
  public static Resource roundDown(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundDown(lhs, factor);
  }
  
  public static boolean isInvalidDivisor(
      ResourceCalculator resourceCalculator, Resource divisor) {
    return resourceCalculator.isInvalidDivisor(divisor);
  }

  public static float ratio(
      ResourceCalculator resourceCalculator, Resource lhs, Resource rhs) {
    return resourceCalculator.ratio(lhs, rhs);
  }
  
  public static float divide(
      ResourceCalculator resourceCalculator,
      Resource clusterResource, Resource lhs, Resource rhs) {
    return resourceCalculator.divide(clusterResource, lhs, rhs);
  }
  
  public static Resource divideAndCeil(
      ResourceCalculator resourceCalculator, Resource lhs, int rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }

  public static Resource divideAndCeil(
      ResourceCalculator resourceCalculator, Resource lhs, float rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }
  
  public static boolean equals(Resource lhs, Resource rhs) {
    return lhs.equals(rhs);
  }

  public static boolean lessThan(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) < 0);
  }

  public static boolean lessThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) <= 0);
  }

  public static boolean greaterThan(
      ResourceCalculator resourceCalculator,
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) > 0;
  }

  public static boolean greaterThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0;
  }
  
  public static Resource min(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) <= 0 ? lhs : rhs;
  }

  public static Resource max(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
  }
  
  public static boolean fitsIn(Resource smaller, Resource bigger) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation rhsValue = bigger.getResourceInformation(i);
        ResourceInformation lhsValue = smaller.getResourceInformation(i);
        if (lhsValue.getValue() > rhsValue.getValue()) {
          return false;
        }
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
        continue;
      }
    }
    return true;
  }

  public static boolean fitsIn(ResourceCalculator rc,
      Resource smaller, Resource bigger) {
    return rc.fitsIn(smaller, bigger);
  }
  
  public static boolean fitsInMultiplyMemory(
      Resource smaller, Resource bigger, float by) {
    // below 0 means no limit
    ResourceInformation rhsValue = bigger.getResourceInformation(Resource.MEMORY_INDEX);
    ResourceInformation lhsValue = smaller.getResourceInformation(Resource.MEMORY_INDEX);
    return (int)(rhsValue.getValue() * by) < 0 ||
        lhsValue.getValue() <= (int)(rhsValue.getValue() * by);
  }

  public static Resource componentwiseMin(Resource lhs, Resource rhs) {
    Resource ret = createResource(0);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(i);
        ResourceInformation lhsValue = lhs.getResourceInformation(i);
        ResourceInformation outInfo = lhsValue.getValue() < rhsValue.getValue()
            ? lhsValue
            : rhsValue;
        ret.setResourceInformation(i, outInfo);
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
        continue;
      }
    }
    return ret;
  }
  
  public static Resource componentwiseMax(Resource lhs, Resource rhs) {
    Resource ret = createResource(0);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(i);
        ResourceInformation lhsValue = lhs.getResourceInformation(i);
        ResourceInformation outInfo = lhsValue.getValue() > rhsValue.getValue()
            ? lhsValue
            : rhsValue;
        ret.setResourceInformation(i, outInfo);
      } catch (ResourceNotFoundException ye) {
        LOG.warn("Resource is missing:" + ye.getMessage());
        continue;
      }
    }
    return ret;
  }

  public static Resource normalizeDown(ResourceCalculator calculator,
      Resource resource, Resource factor) {
    return calculator.normalizeDown(resource, factor);
  }
}
