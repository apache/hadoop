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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.LimitedPrivate({ "YARN", "MapReduce" })
@Unstable
public class Resources {

  /**
   * Helper class to create a resource with a fixed value for all resource
   * types. For example, a NONE resource which returns 0 for any resource type.
   */
  static class FixedValueResource extends Resource {

    private Map<String, ResourceInformation> resources;
    private Long resourceValue;
    private String name;

    /**
     * Constructor for a fixed value resource
     * @param rName the name of the resource
     * @param value the fixed value to be returned for all resource types
     */
    FixedValueResource(String rName, Long value) {
      this.resourceValue = value;
      this.name = rName;
      resources = initResourceMap();
    }

    private int resourceValueToInt() {
      if(this.resourceValue > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      return this.resourceValue.intValue();
    }

    @Override
    @SuppressWarnings("deprecation")
    public int getMemory() {
      return resourceValueToInt();
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
      return resourceValueToInt();
    }

    @Override
    public void setVirtualCores(int virtualCores) {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public Map<String, ResourceInformation> getResources() {
      return Collections.unmodifiableMap(this.resources);
    }

    @Override
    public ResourceInformation getResourceInformation(String resource)
        throws YarnException {
      if (resources.containsKey(resource)) {
        ResourceInformation value = this.resources.get(resource);
        ResourceInformation ret = ResourceInformation.newInstance(value);
        ret.setValue(resourceValue);
        return ret;
      }
      throw new YarnException("" + resource + " not found");
    }

    @Override
    public Long getResourceValue(String resource) throws YarnException {
      if (resources.containsKey(resource)) {
        return resourceValue;
      }
      throw new YarnException("" + resource + " not found");
    }

    @Override
    public void setResourceInformation(String resource,
        ResourceInformation resourceInformation)
        throws ResourceNotFoundException {
      throw new RuntimeException(name + " cannot be modified!");
    }

    @Override
    public void setResourceValue(String resource, Long value)
        throws ResourceNotFoundException {
      throw new RuntimeException(name + " cannot be modified!");
    }

    private Map<String, ResourceInformation> initResourceMap() {
      Map<String, ResourceInformation> tmp = new HashMap<>();
      Map<String, ResourceInformation> types = ResourceUtils.getResourceTypes();
      if (types != null) {
        for (Map.Entry<String, ResourceInformation> entry : types.entrySet()) {
          tmp.put(entry.getKey(),
              ResourceInformation.newInstance(entry.getValue()));
          tmp.get(entry.getKey()).setValue(resourceValue);
        }
      }
      // this is a fix for getVirtualCores returning an int
      if (resourceValue > Integer.MAX_VALUE) {
        tmp.get(ResourceInformation.VCORES.getName())
            .setValue((long) Integer.MAX_VALUE);
      }
      return tmp;
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
    for (Map.Entry<String, ResourceInformation> entry : lhs.getResources()
        .entrySet()) {
      String name = entry.getKey();
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(name);
        ResourceInformation lhsValue = entry.getValue();
        Long convertedRhs = UnitsConversionUtil
            .convert(rhsValue.getUnits(), lhsValue.getUnits(),
                rhsValue.getValue());
        lhs.setResourceValue(name, lhsValue.getValue() + convertedRhs);
      } catch (YarnException ye) {
        continue;
      }
    }
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    for (Map.Entry<String, ResourceInformation> entry : lhs.getResources()
        .entrySet()) {
      String name = entry.getKey();
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(name);
        ResourceInformation lhsValue = entry.getValue();
        Long convertedRhs = UnitsConversionUtil
            .convert(rhsValue.getUnits(), lhsValue.getUnits(),
                rhsValue.getValue());
        lhs.setResourceValue(name, lhsValue.getValue() - convertedRhs);
      } catch (YarnException ye) {
        continue;
      }
    }
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  /**
   * Subtract <code>rhs</code> from <code>lhs</code> and reset any negative
   * values to zero.
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

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, double by) {
    for (Map.Entry<String, ResourceInformation> entry : lhs.getResources()
        .entrySet()) {
      String name = entry.getKey();
      ResourceInformation lhsValue = entry.getValue();
      lhs.setResourceValue(name, (long) (lhsValue.getValue() * by));
    }
    return lhs;
  }

  public static Resource multiply(Resource lhs, double by) {
    return multiplyTo(clone(lhs), by);
  }

  /**
   * Multiply {@code rhs} by {@code by}, and add the result to {@code lhs}
   * without creating any new {@link Resource} object
   */
  public static Resource multiplyAndAddTo(
      Resource lhs, Resource rhs, double by) {
    for (Map.Entry<String, ResourceInformation> entry : lhs.getResources()
        .entrySet()) {
      String name = entry.getKey();
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(name);
        ResourceInformation lhsValue = entry.getValue();
        Long convertedRhs = (long) (UnitsConversionUtil
            .convert(rhsValue.getUnits(), lhsValue.getUnits(),
                rhsValue.getValue()) * by);
        lhs.setResourceValue(name, lhsValue.getValue() + convertedRhs);
      } catch (YarnException ye) {
        continue;
      }
    }
    return lhs;
  }

  public static Resource multiplyAndNormalizeUp(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }
  
  public static Resource multiplyAndNormalizeDown(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeDown(lhs, by, factor);
  }
  
  public static Resource multiplyAndRoundDown(Resource lhs, double by) {
    Resource out = clone(lhs);
    for (Map.Entry<String, ResourceInformation> entry : out.getResources()
        .entrySet()) {
      String name = entry.getKey();
      ResourceInformation lhsValue = entry.getValue();
      out.setResourceValue(name, (long) (lhsValue.getValue() * by));
    }
    return out;
  }

  public static Resource multiplyAndRoundUp(Resource lhs, double by) {
    Resource out = clone(lhs);
    out.setMemorySize((long)Math.ceil(lhs.getMemorySize() * by));
    out.setVirtualCores((int)Math.ceil(lhs.getVirtualCores() * by));
    return out;
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
    for (Map.Entry<String, ResourceInformation> entry : smaller.getResources()
        .entrySet()) {
      String name = entry.getKey();
      try {
        ResourceInformation rhsValue = bigger.getResourceInformation(name);
        ResourceInformation lhsValue = entry.getValue();
        Long convertedRhs = UnitsConversionUtil
            .convert(rhsValue.getUnits(), lhsValue.getUnits(),
                rhsValue.getValue());
        if(lhsValue.getValue() > convertedRhs) {
          return false;
        }
      } catch (YarnException ye) {
        return false;
      }
    }
    return true;
  }

  public static boolean fitsIn(ResourceCalculator rc, Resource cluster,
      Resource smaller, Resource bigger) {
    return rc.fitsIn(cluster, smaller, bigger);
  }
  
  public static Resource componentwiseMin(Resource lhs, Resource rhs) {
    Resource ret = createResource(0);
    for (Map.Entry<String, ResourceInformation> entry : lhs.getResources()
        .entrySet()) {
      String name = entry.getKey();
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(name);
        ResourceInformation lhsValue = entry.getValue();
        Long convertedRhs = UnitsConversionUtil
            .convert(rhsValue.getUnits(), lhsValue.getUnits(),
                rhsValue.getValue());
        ResourceInformation outInfo =
            lhsValue.getValue() < convertedRhs ? lhsValue : rhsValue;
        ret.setResourceInformation(name, outInfo);
      } catch (YarnException ye) {
        continue;
      }
    }
    return ret;
  }
  
  public static Resource componentwiseMax(Resource lhs, Resource rhs) {
    Resource ret = createResource(0);
    for (Map.Entry<String, ResourceInformation> entry : lhs.getResources()
        .entrySet()) {
      String name = entry.getKey();
      try {
        ResourceInformation rhsValue = rhs.getResourceInformation(name);
        ResourceInformation lhsValue = entry.getValue();
        Long convertedRhs = UnitsConversionUtil
            .convert(rhsValue.getUnits(), lhsValue.getUnits(),
                rhsValue.getValue());
        ResourceInformation outInfo =
            lhsValue.getValue() > convertedRhs ? lhsValue : rhsValue;
        ret.setResourceInformation(name, outInfo);
      } catch (YarnException ye) {
        continue;
      }
    }
    return ret;
  }

  public static boolean isAnyMajorResourceZero(ResourceCalculator rc,
      Resource resource) {
    return rc.isAnyMajorResourceZero(resource);
  }
}
