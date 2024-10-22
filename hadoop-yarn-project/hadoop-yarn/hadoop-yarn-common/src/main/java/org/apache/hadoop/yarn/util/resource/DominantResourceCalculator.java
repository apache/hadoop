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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@link ResourceCalculator} which uses the concept of
 * <em>dominant resource</em> to compare multi-dimensional resources.
 *
 * Essentially the idea is that the in a multi-resource environment,
 * the resource allocation should be determined by the dominant share
 * of an entity (user or queue), which is the maximum share that the
 * entity has been allocated of any resource.
 *
 * In a nutshell, it seeks to maximize the minimum dominant share across
 * all entities.
 *
 * For example, if user A runs CPU-heavy tasks and user B runs
 * memory-heavy tasks, it attempts to equalize CPU share of user A
 * with Memory-share of user B.
 *
 * In the single resource case, it reduces to max-min fairness for that resource.
 *
 * See the Dominant Resource Fairness paper for more details:
 * www.cs.berkeley.edu/~matei/papers/2011/nsdi_drf.pdf
 */
@Private
@Unstable
public class DominantResourceCalculator extends ResourceCalculator {
  private static final Logger LOG =
      LoggerFactory.getLogger(DominantResourceCalculator.class);

  public DominantResourceCalculator() {
  }

  /**
   * Compare two resources - if the value for every resource type for the lhs
   * is greater than that of the rhs, return 1. If the value for every resource
   * type in the lhs is less than the rhs, return -1. Otherwise, return 0
   *
   * @param lhs resource to be compared
   * @param rhs resource to be compared
   * @return 0, 1, or -1
   */
  private int compare(Resource lhs, Resource rhs) {
    boolean lhsGreater = false;
    boolean rhsGreater = false;
    int ret = 0;

    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation lhsResourceInformation = lhs
          .getResourceInformation(i);
      ResourceInformation rhsResourceInformation = rhs
          .getResourceInformation(i);
      int diff = lhsResourceInformation.compareTo(rhsResourceInformation);
      if (diff >= 1) {
        lhsGreater = true;
      } else if (diff <= -1) {
        rhsGreater = true;
      }
    }
    if (lhsGreater && rhsGreater) {
      ret = 0;
    } else if (lhsGreater) {
      ret = 1;
    } else if (rhsGreater) {
      ret = -1;
    }
    return ret;
  }

  @Override
  public int compare(Resource clusterResource, Resource lhs, Resource rhs,
      boolean singleType) {
    if (lhs.equals(rhs)) {
      return 0;
    }

    if (isAllInvalidDivisor(clusterResource)) {
      return this.compare(lhs, rhs);
    }

    // We have to calculate the shares for all resource types for both
    // resources and then look for which resource has the biggest
    // share overall.
    ResourceInformation[] clusterRes = clusterResource.getResources();
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();

    // If array creation shows up as a time sink, these arrays could be cached
    // because they're always the same length.
    double[] lhsShares = new double[maxLength];
    double[] rhsShares = new double[maxLength];
    double diff;

    try {
      if (singleType) {
        double[] max = new double[2];

        calculateShares(clusterRes, lhs, rhs, lhsShares, rhsShares, max);

        diff = max[0] - max[1];
      } else if (maxLength == 2) {
        // Special case to handle the common scenario of only CPU and memory
        // so that we can optimize for performance
        diff = calculateSharesForTwoMandatoryResources(clusterRes, lhs, rhs,
            lhsShares, rhsShares);
      } else {
        calculateShares(clusterRes, lhs, rhs, lhsShares, rhsShares);

        Arrays.sort(lhsShares);
        Arrays.sort(rhsShares);

        diff = compareShares(lhsShares, rhsShares);
      }
    } catch (ArrayIndexOutOfBoundsException ex) {
      StringWriter out = new StringWriter(); // No need to close a StringWriter
      ex.printStackTrace(new PrintWriter(out));

      LOG.error("A problem was encountered while calculating resource "
          + "availability that should not occur under normal circumstances. "
          + "Please report this error to the Hadoop community by opening a "
          + "JIRA ticket at http://issues.apache.org/jira and including the "
          + "following information:\n* Exception encountered: " + out + "* "
          + "Cluster resources: " + Arrays.toString(clusterRes) + "\n* "
          + "LHS resource: " + Arrays.toString(lhs.getResources()) + "\n* "
          + "RHS resource: " + Arrays.toString(rhs.getResources()));
      LOG.error("The resource manager is in an inconsistent state. It is safe "
          + "for the resource manager to be restarted as the error encountered "
          + "should be transitive. If high availability is enabled, failing "
          + "over to a standby resource manager is also safe.");
      throw new YarnRuntimeException("A problem was encountered while "
          + "calculating resource availability that should not occur under "
          + "normal circumstances. Please see the log for more information.",
          ex);
    }

    return (int) Math.signum(diff);
  }

  /**
   * Calculate the shares for {@code first} and {@code second} according to
   * {@code clusterRes}, and store the results in {@code firstShares} and
   * {@code secondShares}, respectively. All parameters must be non-null.
   * @param clusterRes the array of ResourceInformation instances that
   * represents the cluster's maximum resources
   * @param first the first resource to compare
   * @param second the second resource to compare
   * @param firstShares an array to store the shares for the first resource
   * @param secondShares an array to store the shares for the second resource
   * @return -1.0, 0.0, or 1.0, depending on whether the max share of the first
   * resource is less than, equal to, or greater than the max share of the
   * second resource, respectively
   * @throws NullPointerException if any parameter is null
   */
  private void calculateShares(ResourceInformation[] clusterRes, Resource first,
      Resource second, double[] firstShares, double[] secondShares) {
    ResourceInformation[] firstRes = first.getResources();
    ResourceInformation[] secondRes = second.getResources();

    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      firstShares[i] = calculateShare(clusterRes[i], firstRes[i]);
      secondShares[i] = calculateShare(clusterRes[i], secondRes[i]);
    }
  }

  /**
   * Calculate the shares for {@code first} and {@code second} according to
   * {@code clusterRes}, and store the results in {@code firstShares} and
   * {@code secondShares}, respectively. All parameters must be non-null.
   * This method assumes that the length of {@code clusterRes} is exactly 2 and
   * makes performance optimizations based on that assumption.
   * @param clusterRes the array of ResourceInformation instances that
   * represents the cluster's maximum resources
   * @param first the first resource to compare
   * @param second the second resource to compare
   * @param firstShares an array to store the shares for the first resource
   * @param secondShares an array to store the shares for the second resource
   * @return -1.0, 0.0, or 1.0, depending on whether the max share of the first
   * resource is less than, equal to, or greater than the max share of the
   * second resource, respectively
   * @throws NullPointerException if any parameter is null
   */
  private int calculateSharesForTwoMandatoryResources(
      ResourceInformation[] clusterRes, Resource first, Resource second,
      double[] firstShares, double[] secondShares) {
    ResourceInformation[] firstRes = first.getResources();
    ResourceInformation[] secondRes = second.getResources();
    firstShares[0] = calculateShare(clusterRes[0], firstRes[0]);
    secondShares[0] = calculateShare(clusterRes[0], secondRes[0]);
    firstShares[1] = calculateShare(clusterRes[1], firstRes[1]);
    secondShares[1] = calculateShare(clusterRes[1], secondRes[1]);

    int firstDom = 0;
    int firstSub = 1;
    if (firstShares[1] > firstShares[0]) {
      firstDom = 1;
      firstSub = 0;
    }
    int secondDom = 0;
    int secondSub = 1;
    if (secondShares[1] > secondShares[0]) {
      secondDom = 1;
      secondSub = 0;
    }

    if (firstShares[firstDom] > secondShares[secondDom]) {
      return 1;
    } else if (firstShares[firstDom] < secondShares[secondDom]) {
      return -1;
    } else if (firstShares[firstSub] > secondShares[secondSub]) {
      return 1;
    } else if (firstShares[firstSub] < secondShares[secondSub]) {
      return -1;
    } else {
      return 0;
    }
  }

  /**
   * Calculate the shares for {@code first} and {@code second} according to
   * {@code clusterRes}, and store the results in {@code firstShares} and
   * {@code secondShares}, respectively. {@code max} will be populated with
   * the max shares from {@code firstShare} and {@code secondShare} in the
   * first and second indices, respectively. All parameters must be non-null,
   * and {@code max} must have a length of at least 2.
   * @param clusterRes the array of ResourceInformation instances that
   * represents the cluster's maximum resources
   * @param first the first resource to compare
   * @param second the second resource to compare
   * @param firstShares an array to store the shares for the first resource
   * @param secondShares an array to store the shares for the second resource
   * @param max an array to store the max shares of the first and second
   * resources
   * @return -1.0, 0.0, or 1.0, depending on whether the max share of the first
   * resource is less than, equal to, or greater than the max share of the
   * second resource, respectively
   * @throws NullPointerException if any parameter is null
   * @throws ArrayIndexOutOfBoundsException if the length of {@code max} is
   * less than 2
   */
  private void calculateShares(ResourceInformation[] clusterRes, Resource first,
      Resource second, double[] firstShares, double[] secondShares,
      double[] max) {
    ResourceInformation[] firstRes = first.getResources();
    ResourceInformation[] secondRes = second.getResources();

    max[0] = 0.0;
    max[1] = 0.0;

    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      firstShares[i] = calculateShare(clusterRes[i], firstRes[i]);
      secondShares[i] = calculateShare(clusterRes[i], secondRes[i]);

      if (firstShares[i] == Float.POSITIVE_INFINITY ||
              secondShares[i] == Float.POSITIVE_INFINITY) {
        continue;
      }

      if (firstShares[i] > max[0]) {
        max[0] = firstShares[i];
      }

      if (secondShares[i] > max[1]) {
        max[1] = secondShares[i];
      }
    }
  }

  /**
   * Calculate the share for a resource type.
   * @param clusterRes the resource type for the cluster maximum
   * @param res the resource type for which to calculate the share
   * @return the share
   */
  private double calculateShare(ResourceInformation clusterRes,
      ResourceInformation res) {
    if(clusterRes.getValue() == 0) {
      return Float.POSITIVE_INFINITY;
    }
    return (double) res.getValue() / clusterRes.getValue();
  }

  /**
   * Compare the two shares arrays by comparing the largest elements, then the
   * next largest if the previous were equal, etc. The share arrays must be
   * sorted in ascending order.
   * @param lhsShares the first share array to compare
   * @param rhsShares the second share array to compare
   * @return a number that is less than 0 if the first array is less than the
   * second, equal to 0 if the arrays are equal, and greater than 0 if the
   * first array is greater than the second
   */
  private double compareShares(double[] lhsShares, double[] rhsShares) {
    double diff = 0.0;

    // lhsShares and rhsShares must necessarily have the same length, because
    // everyone uses the same master resource list.
    for (int i = lhsShares.length - 1; i >= 0; i--) {
      if (lhsShares[i] == Float.POSITIVE_INFINITY ||
              rhsShares[i] == Float.POSITIVE_INFINITY) {
        continue;
      }
      diff = lhsShares[i] - rhsShares[i];

      if (diff != 0.0) {
        break;
      }
    }

    return diff;
  }

  @Override
  public long computeAvailableContainers(Resource available,
      Resource required) {
    long min = Long.MAX_VALUE;
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation availableResource = available
          .getResourceInformation(i);
      ResourceInformation requiredResource = required.getResourceInformation(i);
      if (requiredResource.getValue() != 0) {
        long tmp = availableResource.getValue() / requiredResource.getValue();
        min = min < tmp ? min : tmp;
      }
    }
    return min > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) min;
  }

  @Override
  public float divide(Resource clusterResource,
      Resource numerator, Resource denominator) {
    int nKnownResourceTypes = ResourceUtils.getNumberOfCountableResourceTypes();
    ResourceInformation[] clusterRes = clusterResource.getResources();
    // We have to provide the calculateShares() method with somewhere to store
    // the shares. We don't actually need these shares afterwards.
    double[] numeratorShares = new double[nKnownResourceTypes];
    double[] denominatorShares = new double[nKnownResourceTypes];
    // We also have to provide a place for calculateShares() to store the max
    // shares so that we can use them.
    double[] max = new double[2];

    calculateShares(clusterRes, numerator, denominator, numeratorShares,
        denominatorShares, max);

    return (float) (max[0] / max[1]);
  }

  @Override
  public boolean isInvalidDivisor(Resource r) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      if (r.getResourceInformation(i).getValue() == 0L) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isAllInvalidDivisor(Resource r) {
    boolean flag = true;
    for (ResourceInformation res : r.getResources()) {
      if (flag == true && res.getValue() == 0L) {
        flag = true;
        continue;
      }
      flag = false;
    }
    return flag;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    return ratio(a, b, true);
  }

  /**
   * Computes the ratio of resource a over resource b,
   * where the boolean flag {@literal isDominantShare} allows
   * specification of whether the max- or min-share should be computed.
   * @param a the numerator resource.
   * @param b the denominator resource.
   * @param isDominantShare whether the dominant (max) share should be computed,
   *                        computes the min-share if false.
   * @return the max- or min-share ratio of the resources.
   */
  private float ratio(Resource a, Resource b, boolean isDominantShare) {
    float ratio = isDominantShare ? 0.0f : 1.0f;
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation aResourceInformation = a.getResourceInformation(i);
      ResourceInformation bResourceInformation = b.getResourceInformation(i);
      final float tmp = divideSafelyAsFloat(aResourceInformation.getValue(),
          bResourceInformation.getValue());
      if (isDominantShare) {
        ratio = Math.max(ratio, tmp);
      } else {
        ratio = Math.min(ratio, tmp);
      }
    }
    return ratio;
  }

  /**
   * Computes the ratio of resource a over resource b.
   * However, different from ratio(Resource, Resource),
   * this returns the min-share of the resources.
   * For example, ratio(Resource(10, 50), Resource(100, 100)) would return 0.5,
   * whereas minRatio(Resource(10, 50), Resource(100, 100)) would return 0.1.
   * @param a the numerator resource.
   * @param b the denominator resource.
   * @return the min-share ratio of the resources.
   */
  @Unstable
  public float minRatio(Resource a, Resource b) {
    return ratio(a, b, false);
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return divideAndCeil(numerator, (long) denominator);
  }

  public Resource divideAndCeil(Resource numerator, long denominator) {
    Resource ret = Resource.newInstance(numerator);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation = ret.getResourceInformation(i);
      resourceInformation
          .setValue(divideAndCeil(resourceInformation.getValue(), denominator));
    }
    return ret;
  }

  @Override
  public Resource divideAndCeil(Resource numerator, float denominator) {
    Resource ret = Resource.newInstance(numerator);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation = ret.getResourceInformation(i);
      resourceInformation
          .setValue(divideAndCeil(resourceInformation.getValue(), denominator));
    }
    return ret;
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    Resource ret = Resource.newInstance(r);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation rResourceInformation = r.getResourceInformation(i);
      ResourceInformation minimumResourceInformation = minimumResource
          .getResourceInformation(i);
      ResourceInformation maximumResourceInformation = maximumResource
          .getResourceInformation(i);
      ResourceInformation stepFactorResourceInformation = stepFactor
          .getResourceInformation(i);
      ResourceInformation tmp = ret.getResourceInformation(i);

      long rValue = rResourceInformation.getValue();
      long value = Math.max(rValue, minimumResourceInformation.getValue());
      if (stepFactorResourceInformation.getValue() != 0) {
        value = roundUp(value, stepFactorResourceInformation.getValue());
      }
      tmp.setValue(Math.min(value, maximumResourceInformation.getValue()));
      ret.setResourceInformation(i, tmp);
    }
    return ret;
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return this.rounding(r, stepFactor, true);
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return this.rounding(r, stepFactor, false);
  }

  private Resource rounding(Resource r, Resource stepFactor, boolean roundUp) {
    Resource ret = Resource.newInstance(r);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation rResourceInformation = r.getResourceInformation(i);
      ResourceInformation stepFactorResourceInformation = stepFactor
          .getResourceInformation(i);

      long rValue = rResourceInformation.getValue();
      long stepFactorValue = stepFactorResourceInformation.getValue();
      long value = rValue;
      if (stepFactorValue != 0) {
        value = roundUp
            ? roundUp(rValue, stepFactorValue)
            : roundDown(rValue, stepFactorValue);
      }
      ResourceInformation.copy(rResourceInformation,
          ret.getResourceInformation(i));
      ret.getResourceInformation(i).setValue(value);
    }
    return ret;
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double[] by,
      Resource stepFactor) {
    Resource ret = Resource.newInstance(r);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation rResourceInformation = r.getResourceInformation(i);
      ResourceInformation stepFactorResourceInformation = stepFactor
          .getResourceInformation(i);

      long rValue = rResourceInformation.getValue();
      long stepFactorValue = stepFactorResourceInformation.getValue();
      ret.setResourceValue(i, ResourceCalculator
          .roundUp((long) Math.ceil(rValue * by[i]), stepFactorValue));
    }
    return ret;
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return this.multiplyAndNormalize(r, by, stepFactor, true);
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return this.multiplyAndNormalize(r, by, stepFactor, false);
  }

  private Resource multiplyAndNormalize(Resource r, double by,
      Resource stepFactor, boolean roundUp) {
    Resource ret = Resource.newInstance(r);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation rResourceInformation = r.getResourceInformation(i);
      ResourceInformation stepFactorResourceInformation = stepFactor
          .getResourceInformation(i);
      ResourceInformation tmp = ret.getResourceInformation(i);

      long rValue = rResourceInformation.getValue();
      long stepFactorValue = stepFactorResourceInformation.getValue();
      long value;
      if (stepFactorValue != 0) {
        value = roundUp
            ? roundUp((long) Math.ceil((float) (rValue * by)), stepFactorValue)
            : roundDown((long) (rValue * by), stepFactorValue);
      } else {
        value = roundUp
            ? (long) Math.ceil((float) (rValue * by))
            : (long) (rValue * by);
      }
      tmp.setValue(value);
    }
    return ret;
  }

  @Override
  public boolean fitsIn(Resource smaller, Resource bigger) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation sResourceInformation = smaller
          .getResourceInformation(i);
      ResourceInformation bResourceInformation = bigger
          .getResourceInformation(i);
      if (sResourceInformation.getValue() > bResourceInformation.getValue()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Resource normalizeDown(Resource r, Resource stepFactor) {
    Resource ret = Resource.newInstance(r);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation rResourceInformation = r.getResourceInformation(i);
      ResourceInformation stepFactorResourceInformation = stepFactor
          .getResourceInformation(i);
      ResourceInformation tmp = ret.getResourceInformation(i);

      long rValue = rResourceInformation.getValue();
      long stepFactorValue = stepFactorResourceInformation.getValue();
      long value = rValue;
      if (stepFactorValue != 0) {
        value = roundDown(rValue, stepFactorValue);
      }
      tmp.setValue(value);
    }
    return ret;
  }

  @Override
  public boolean isAnyMajorResourceZeroOrNegative(Resource resource) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation = resource.getResourceInformation(
          i);
      if (resourceInformation.getValue() <= 0L) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isAnyRequestedResourceZeroOrNegative(Resource available, Resource resource) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation = available.getResourceInformation(
          i);
      if (resourceInformation.getValue() != 0L) {
        ResourceInformation ri2 = resource.getResourceInformation(i);
        if (ri2.getValue() <= 0L) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isAnyMajorResourceAboveZero(Resource resource) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation = resource.getResourceInformation(
          i);
      if (resourceInformation.getValue() > 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> getInsufficientResourceNames(Resource required,
      Resource available) {
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    return IntStream.range(0, maxLength).filter(
        i -> required.getResourceInformation(i).getValue() > available
            .getResourceInformation(i).getValue())
        .mapToObj(i -> ResourceUtils.getResourceTypesArray()[i].getName())
        .collect(Collectors.toSet());
  }
}
