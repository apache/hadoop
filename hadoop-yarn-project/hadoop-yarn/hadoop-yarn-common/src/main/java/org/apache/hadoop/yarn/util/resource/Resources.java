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
import org.apache.hadoop.yarn.api.records.*;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
@Unstable
public class Resources {
  
  // Java doesn't have const :(
  private static final Resource NONE = new Resource() {

    @Override
    @SuppressWarnings("deprecation")
    public int getMemory() {
      return 0;
    }

    @Override
    public long getMemorySize() {
      return 0;
    }

    @Override
    public void setMemorySize(long memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return 0;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      long diff = 0 - o.getMemorySize();
      if (diff == 0) {
        diff = 0 - o.getVirtualCores();
      }
      return Long.signum(diff);
    }
    
  };
  
  private static final Resource UNBOUNDED = new Resource() {

    @Override
    @SuppressWarnings("deprecation")
    public int getMemory() {
      return Integer.MAX_VALUE;
    }

    @Override
    public long getMemorySize() {
      return Long.MAX_VALUE;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setMemory(int memory) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public void setMemorySize(long memory) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      long diff = Long.MAX_VALUE - o.getMemorySize();
      if (diff == 0) {
        diff = Integer.MAX_VALUE - o.getVirtualCores();
      }
      return Long.signum(diff);
    }
    
  };

  public static Resource createResource(int memory) {
    return createResource(memory, (memory > 0) ? 1 : 0);
  }

  public static Resource createResource(int memory, int cores) {
    return Resource.newInstance(memory, cores);
  }

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
    return createResource(res.getMemorySize(), res.getVirtualCores());
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    lhs.setMemorySize(lhs.getMemorySize() + rhs.getMemorySize());
    lhs.setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores());
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    lhs.setMemorySize(lhs.getMemorySize() - rhs.getMemorySize());
    lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
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
    lhs.setMemorySize((long)(lhs.getMemorySize() * by));
    lhs.setVirtualCores((int)(lhs.getVirtualCores() * by));
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
    lhs.setMemorySize(lhs.getMemorySize() + (long)(rhs.getMemorySize() * by));
    lhs.setVirtualCores(lhs.getVirtualCores()
        + (int)(rhs.getVirtualCores() * by));
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
    out.setMemorySize((long)(lhs.getMemorySize() * by));
    out.setVirtualCores((int)(lhs.getVirtualCores() * by));
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
    return smaller.getMemorySize() <= bigger.getMemorySize() &&
        smaller.getVirtualCores() <= bigger.getVirtualCores();
  }

  public static boolean fitsIn(ResourceCalculator rc, Resource cluster,
      Resource smaller, Resource bigger) {
    return rc.fitsIn(cluster, smaller, bigger);
  }
  
  public static Resource componentwiseMin(Resource lhs, Resource rhs) {
    return createResource(Math.min(lhs.getMemorySize(), rhs.getMemorySize()),
        Math.min(lhs.getVirtualCores(), rhs.getVirtualCores()));
  }
  
  public static Resource componentwiseMax(Resource lhs, Resource rhs) {
    return createResource(Math.max(lhs.getMemorySize(), rhs.getMemorySize()),
        Math.max(lhs.getVirtualCores(), rhs.getVirtualCores()));
  }

  public static boolean isAnyMajorResourceZero(ResourceCalculator rc,
      Resource resource) {
    return rc.isAnyMajorResourceZero(resource);
  }
}
