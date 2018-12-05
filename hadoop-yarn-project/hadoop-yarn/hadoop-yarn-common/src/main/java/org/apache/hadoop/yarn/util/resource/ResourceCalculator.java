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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * A set of {@link Resource} comparison and manipulation interfaces.
 */
@Private
@Unstable
public abstract class ResourceCalculator {

  /**
   * On a cluster with capacity {@code clusterResource}, compare {@code lhs}
   * and {@code rhs}. Consider all resources unless {@code singleType} is set
   * to true. When {@code singleType} is set to true, consider only one
   * resource as per the {@link ResourceCalculator} implementation; the
   * {@link DefaultResourceCalculator} considers memory and
   * {@link DominantResourceCalculator} considers the dominant resource.
   *
   * @param clusterResource cluster capacity
   * @param lhs First {@link Resource} to compare
   * @param rhs Second {@link Resource} to compare
   * @param singleType Whether to consider a single resource type or all
   *                   resource types
   * @return -1 if {@code lhs} is smaller, 0 if equal and 1 if it is larger
   */
  public abstract int compare(
      Resource clusterResource, Resource lhs, Resource rhs, boolean singleType);

  /**
   * On a cluster with capacity {@code clusterResource}, compare {@code lhs}
   * and {@code rhs} considering all resources.
   *
   * @param clusterResource cluster capacity
   * @param lhs First {@link Resource} to compare
   * @param rhs Second {@link Resource} to compare
   * @return -1 if {@code lhs} is smaller, 0 if equal and 1 if it is larger
   */
  public int compare(Resource clusterResource, Resource lhs, Resource rhs) {
    return compare(clusterResource, lhs, rhs, false);
  }

  public static int divideAndCeil(int a, int b) {
    if (b == 0) {
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  public static int divideAndCeil(int a, float b) {
    if (b == 0) {
      return 0;
    }
    return (int) Math.ceil(a / b);
  }
  
  public static long divideAndCeil(long a, long b) {
    if (b == 0) {
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  public static long divideAndCeil(long a, float b) {
    if (b == 0) {
      return 0;
    }
    return (long) Math.ceil(a/b);
  }

  /**
   * Divides lhs by rhs.
   * If both lhs and rhs are having a value of 0, then we return 0.
   * This is to avoid division by zero and return NaN as a result.
   * If lhs is zero but rhs is not, Float.infinity will be returned
   * as the result.
   * @param lhs
   * @param rhs
   * @return
   */
  public static float divideSafelyAsFloat(long lhs, long rhs) {
    if (lhs == 0 && rhs == 0) {
      return 0;
    } else {
      return (float) lhs / (float) rhs;
    }
  }

  public static int roundUp(int a, int b) {
    return divideAndCeil(a, b) * b;
  }

  public static long roundUp(long a, long b) {
    return divideAndCeil(a, b) * b;
  }

  public static long roundDown(long a, long b) {
    return (a / b) * b;
  }

  public static int roundDown(int a, int b) {
    return (a / b) * b;
  }

  /**
   * Compute the number of containers which can be allocated given
   * <code>available</code> and <code>required</code> resources.
   * 
   * @param available available resources
   * @param required required resources
   * @return number of containers which can be allocated
   */
  public abstract long computeAvailableContainers(
      Resource available, Resource required);

  /**
   * Multiply resource <code>r</code> by factor <code>by</code> 
   * and normalize up using step-factor <code>stepFactor</code>.
   * 
   * @param r resource to be multiplied
   * @param by multiplier
   * @param stepFactor factor by which to normalize up 
   * @return resulting normalized resource
   */
  public abstract Resource multiplyAndNormalizeUp(
      Resource r, double by, Resource stepFactor);

  /**
   * Multiply resource <code>r</code> by factor <code>by</code>
   * and normalize up using step-factor <code>stepFactor</code>.
   *
   * @param r resource to be multiplied
   * @param by multiplier array for all resource types
   * @param stepFactor factor by which to normalize up
   * @return resulting normalized resource
   */
  public abstract Resource multiplyAndNormalizeUp(
      Resource r, double[] by, Resource stepFactor);

  /**
   * Multiply resource <code>r</code> by factor <code>by</code> 
   * and normalize down using step-factor <code>stepFactor</code>.
   * 
   * @param r resource to be multiplied
   * @param by multiplier
   * @param stepFactor factor by which to normalize down 
   * @return resulting normalized resource
   */
  public abstract Resource multiplyAndNormalizeDown(
      Resource r, double by, Resource stepFactor);

  /**
   * Normalize resource <code>r</code> given the base 
   * <code>minimumResource</code> and verify against max allowed
   * <code>maximumResource</code> using a step factor for the normalization.
   *
   * @param r resource
   * @param minimumResource minimum value
   * @param maximumResource the upper bound of the resource to be allocated
   * @param stepFactor the increment for resources to be allocated
   * @return normalized resource
   */
  public abstract Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor);

  /**
   * Round-up resource <code>r</code> given factor <code>stepFactor</code>.
   * 
   * @param r resource
   * @param stepFactor step-factor
   * @return rounded resource
   */
  public abstract Resource roundUp(Resource r, Resource stepFactor);
  
  /**
   * Round-down resource <code>r</code> given factor <code>stepFactor</code>.
   * 
   * @param r resource
   * @param stepFactor step-factor
   * @return rounded resource
   */
  public abstract Resource roundDown(Resource r, Resource stepFactor);
  
  /**
   * Divide resource <code>numerator</code> by resource <code>denominator</code>
   * using specified policy (domination, average, fairness etc.); hence overall
   * <code>clusterResource</code> is provided for context.
   *  
   * @param clusterResource cluster resources
   * @param numerator numerator
   * @param denominator denominator
   * @return <code>numerator</code>/<code>denominator</code> 
   *         using specific policy
   */
  public abstract float divide(
      Resource clusterResource, Resource numerator, Resource denominator);
  
  /**
   * Determine if a resource is not suitable for use as a divisor
   * (will result in divide by 0, etc)
   *
   * @param r resource
   * @return true if divisor is invalid (should not be used), false else
   */
  public abstract boolean isInvalidDivisor(Resource r);

  /**
   * Ratio of resource <code>a</code> to resource <code>b</code>.
   * 
   * @param a resource 
   * @param b resource
   * @return ratio of resource <code>a</code> to resource <code>b</code>
   */
  public abstract float ratio(Resource a, Resource b);

  /**
   * Divide-and-ceil <code>numerator</code> by <code>denominator</code>.
   * 
   * @param numerator numerator resource
   * @param denominator denominator
   * @return resultant resource
   */
  public abstract Resource divideAndCeil(Resource numerator, int denominator);

  /**
   * Divide-and-ceil <code>numerator</code> by <code>denominator</code>.
   *
   * @param numerator numerator resource
   * @param denominator denominator
   * @return resultant resource
   */
  public abstract Resource divideAndCeil(Resource numerator, float denominator);
  
  /**
   * Check if a smaller resource can be contained by bigger resource.
   */
  public abstract boolean fitsIn(Resource smaller, Resource bigger);

  /**
   * Check if resource has any major resource types (which are all NodeManagers
   * included) a zero value or negative value.
   *
   * @param resource resource
   * @return returns true if any resource is zero.
   */
  public abstract boolean isAnyMajorResourceZeroOrNegative(Resource resource);

  /**
   * Get resource <code>r</code>and normalize down using step-factor
   * <code>stepFactor</code>.
   *
   * @param r
   *          resource to be multiplied
   * @param stepFactor
   *          factor by which to normalize down
   * @return resulting normalized resource
   */
  public abstract Resource normalizeDown(Resource r, Resource stepFactor);

  /**
   * Check if resource has any major resource types (which are all NodeManagers
   * included) has a {@literal >} 0 value.
   *
   * @param resource resource
   * @return returns true if any resource is {@literal >} 0
   */
  public abstract boolean isAnyMajorResourceAboveZero(Resource resource);
}
