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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * Makes scheduling decisions by trying to equalize dominant resource usage.
 * A schedulable's dominant resource usage is the largest ratio of resource
 * usage to capacity among the resource types it is using.
 */
@Private
@Unstable
public class DominantResourceFairnessPolicy extends SchedulingPolicy {

  public static final String NAME = "DRF";

  private static final int NUM_RESOURCES =
      ResourceUtils.getNumberOfKnownResourceTypes();
  private static final DominantResourceFairnessComparator COMPARATORN =
      new DominantResourceFairnessComparatorN();
  private static final DominantResourceFairnessComparator COMPARATOR2 =
      new DominantResourceFairnessComparator2();
  private static final DominantResourceCalculator CALCULATOR =
      new DominantResourceCalculator();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    if (NUM_RESOURCES == 2) {
      // To improve performance, if we know we're dealing with the common
      // case of only CPU and memory, then handle CPU and memory explicitly.
      return COMPARATOR2;
    } else {
      // Otherwise, do it the generic way.
      return COMPARATORN;
    }

  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return CALCULATOR;
  }

  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
      ComputeFairShares.computeShares(schedulables, totalResources,
          info.getName());
    }
  }

  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
      ComputeFairShares.computeSteadyShares(queues, totalResources,
          info.getName());
    }
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    return !Resources.fitsIn(usage, fairShare);
  }

  @Override
  public Resource getHeadroom(Resource queueFairShare, Resource queueUsage,
                              Resource maxAvailable) {
    long queueAvailableMemory =
        Math.max(queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
    int queueAvailableCPU =
        Math.max(queueFairShare.getVirtualCores() - queueUsage
            .getVirtualCores(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
        Math.min(maxAvailable.getVirtualCores(),
            queueAvailableCPU));
    return headroom;
  }

  @Override
  public void initialize(FSContext fsContext) {
    COMPARATORN.setFSContext(fsContext);
    COMPARATOR2.setFSContext(fsContext);
  }

  /**
   * This class compares two {@link Schedulable} instances according to the
   * DRF policy. If neither instance is below min share, approximate fair share
   * ratios are compared. Subclasses of this class will do the actual work of
   * the comparison, specialized for the number of configured resource types.
   */
  public abstract static class DominantResourceFairnessComparator
      implements Comparator<Schedulable> {
    protected FSContext fsContext;

    public void setFSContext(FSContext fsContext) {
      this.fsContext = fsContext;
    }

    /**
     * This method is used when apps are tied in fairness ratio. It breaks
     * the tie by submit time and job name to get a deterministic ordering,
     * which is useful for unit tests.
     *
     * @param s1 the first item to compare
     * @param s2 the second item to compare
     * @return &lt; 0, 0, or &gt; 0 if the first item is less than, equal to,
     * or greater than the second item, respectively
     */
    protected int compareAttribrutes(Schedulable s1, Schedulable s2) {
      int res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());

      if (res == 0) {
        res = s1.getName().compareTo(s2.getName());
      }

      return res;
    }
  }

  /**
   * This class compares two {@link Schedulable} instances according to the
   * DRF policy. If neither instance is below min share, approximate fair share
   * ratios are compared. This class makes no assumptions about the number of
   * resource types.
   */
  @VisibleForTesting
  static class DominantResourceFairnessComparatorN
      extends DominantResourceFairnessComparator {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      Resource usage1 = s1.getResourceUsage();
      Resource usage2 = s2.getResourceUsage();
      Resource minShare1 = s1.getMinShare();
      Resource minShare2 = s2.getMinShare();
      Resource clusterCapacity = fsContext.getClusterResource();

      // These arrays hold the usage, fair, and min share ratios for each
      // resource type. ratios[0][x] are the usage ratios, ratios[1][x] are
      // the fair share ratios, and ratios[2][x] are the min share ratios.
      float[][] ratios1 = new float[NUM_RESOURCES][3];
      float[][] ratios2 = new float[NUM_RESOURCES][3];

      // Calculate cluster shares and approximate fair shares for each
      // resource type of both schedulables.
      int dominant1 = calculateClusterAndFairRatios(usage1, clusterCapacity,
          ratios1, s1.getWeight());
      int dominant2 = calculateClusterAndFairRatios(usage2, clusterCapacity,
          ratios2, s2.getWeight());

      // A queue is needy for its min share if its dominant resource
      // (with respect to the cluster capacity) is below its configured min
      // share for that resource
      boolean s1Needy =
          usage1.getResources()[dominant1].getValue() <
          minShare1.getResources()[dominant1].getValue();
      boolean s2Needy =
          usage2.getResources()[dominant2].getValue() <
          minShare2.getResources()[dominant2].getValue();
      
      int res;

      if (!s2Needy && !s1Needy) {
        // Sort shares by usage ratio and compare them by approximate fair share
        // ratio
        sortRatios(ratios1, ratios2);
        res = compareRatios(ratios1, ratios2, 1);
      } else if (s1Needy && !s2Needy) {
        res = -1;
      } else if (s2Needy && !s1Needy) {
        res = 1;
      } else { // both are needy below min share
        // Calculate the min share ratios, then sort by usage ratio, and compare
        // by min share ratio
        calculateMinShareRatios(usage1, minShare1, ratios1);
        calculateMinShareRatios(usage2, minShare2, ratios2);
        sortRatios(ratios1, ratios2);
        res = compareRatios(ratios1, ratios2, 2);
      }

      if (res == 0) {
        res = compareAttribrutes(s1, s2);
      }

      return res;
    }

    /**
     * Sort both ratios arrays according to the usage ratios (the
     * first index of the inner arrays, e.g. {@code ratios1[x][0]}).
     *
     * @param ratios1 the first ratios array
     * @param ratios2 the second ratios array
     */
    @VisibleForTesting
    void sortRatios(float[][] ratios1, float[][]ratios2) {
      // sort order descending by resource share
      Arrays.sort(ratios1, (float[] o1, float[] o2) ->
          (int) Math.signum(o2[0] - o1[0]));
      Arrays.sort(ratios2, (float[] o1, float[] o2) ->
          (int) Math.signum(o2[0] - o1[0]));
    }

    /**
     * Calculate a resource's usage ratio and approximate fair share ratio.
     * The {@code ratios} array will be populated with both the usage ratio
     * and the approximate fair share ratio for each resource type. The usage
     * ratio is calculated as {@code resource} divided by {@code cluster}.
     * The approximate fair share ratio is calculated as the usage ratio
     * divided by {@code weight}. If the cluster's resources are 100MB and
     * 10 vcores, and the usage ({@code resource}) is 10 MB and 5 CPU, the
     * usage ratios will be 0.1 and 0.5. If the weights are 2, the fair
     * share ratios will be 0.05 and 0.25.
     *
     * The approximate fair share ratio is the usage divided by the
     * approximate fair share, i.e. the cluster resources times the weight.
     * The approximate fair share is an acceptable proxy for the fair share
     * because when comparing resources, the resource with the higher weight
     * will be assigned by the scheduler a proportionally higher fair share.
     *
     * The {@code ratios} array must be at least <i>n</i> x 2, where <i>n</i>
     * is the number of resource types. Only the first and second indices of
     * the inner arrays in the {@code ratios} array will be used, e.g.
     * {@code ratios[x][0]} and {@code ratios[x][1]}.
     *
     * The return value will be the index of the dominant resource type in the
     * {@code ratios} array. The dominant resource is the resource type for
     * which {@code resource} has the largest usage ratio.
     *
     * @param resource the resource for which to calculate ratios
     * @param cluster the total cluster resources
     * @param ratios the share ratios array to populate
     * @param weight the resource weight
     * @return the index of the resource type with the largest cluster share
     */
    @VisibleForTesting
    int calculateClusterAndFairRatios(Resource resource, Resource cluster,
        float[][] ratios, float weight) {
      ResourceInformation[] resourceInfo = resource.getResources();
      ResourceInformation[] clusterInfo = cluster.getResources();
      int max = 0;

      for (int i = 0; i < clusterInfo.length; i++) {
        // First calculate the cluster share
        ratios[i][0] =
            resourceInfo[i].getValue() / (float) clusterInfo[i].getValue();

        // Use the cluster share to find the dominant resource
        if (ratios[i][0] > ratios[max][0]) {
          max = i;
        }

        // Now divide by the weight to get the approximate fair share.
        // It's OK if the weight is zero, because the floating point division
        // will yield Infinity, i.e. this Schedulable will lose out to any
        // other Schedulable with non-zero weight.
        ratios[i][1] = ratios[i][0] / weight;
      }

      return max;
    }
    
    /**
     * Calculate a resource's min share ratios. The {@code ratios} array will be
     * populated with the {@code resource} divided by {@code minShare} for each
     * resource type. If the min shares are 5 MB and 10 vcores, and the usage
     * ({@code resource}) is 10 MB and 5 CPU, the ratios will be 2 and 0.5.
     *
     * The {@code ratios} array must be <i>n</i> x 3, where <i>n</i> is the
     * number of resource types. Only the third index of the inner arrays in
     * the {@code ratios} array will be used, e.g. {@code ratios[x][2]}.
     *
     * @param resource the resource for which to calculate min shares
     * @param minShare the min share
     * @param ratios the share ratios array to populate
     */
    @VisibleForTesting
    void calculateMinShareRatios(Resource resource, Resource minShare,
        float[][] ratios) {
      ResourceInformation[] resourceInfo = resource.getResources();
      ResourceInformation[] minShareInfo = minShare.getResources();

      for (int i = 0; i < minShareInfo.length; i++) {
        ratios[i][2] =
            resourceInfo[i].getValue() / (float) minShareInfo[i].getValue();
      }
    }

    /**
     * Compare the two ratios arrays and return -1, 0, or 1 if the first array
     * is less than, equal to, or greater than the second array, respectively.
     * The {@code index} parameter determines which index of the inner arrays
     * will be used for the comparisons. 0 is for usage ratios, 1 is for
     * fair share ratios, and 2 is for the min share ratios. The ratios arrays
     * are assumed to be sorted in descending order by usage ratio.
     *
     * @param ratios1 the first shares array
     * @param ratios2 the second shares array
     * @param index the outer index of the ratios arrays to compare. 0 is for
     * usage ratio, 1 is for approximate fair share ratios, and 1 is for min
     * share ratios
     * @return -1, 0, or 1 if the first array is less than, equal to, or
     * greater than the second array, respectively
     */
    @VisibleForTesting
    int compareRatios(float[][] ratios1, float[][] ratios2, int index) {
      int ret = 0;

      for (int i = 0; i < ratios1.length; i++) {
        ret = (int) Math.signum(ratios1[i][index] - ratios2[i][index]);

        if (ret != 0) {
          break;
        }
      }

      return ret;
    }
  }

  /**
   * This class compares two {@link Schedulable} instances according to the
   * DRF policy in the special case that only CPU and memory are configured.
   * If neither instance is below min share, approximate fair share
   * ratios are compared.
   */
  @VisibleForTesting
  static class DominantResourceFairnessComparator2
      extends DominantResourceFairnessComparator {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      ResourceInformation[] resourceInfo1 =
          s1.getResourceUsage().getResources();
      ResourceInformation[] resourceInfo2 =
          s2.getResourceUsage().getResources();
      ResourceInformation[] minShareInfo1 = s1.getMinShare().getResources();
      ResourceInformation[] minShareInfo2 = s2.getMinShare().getResources();
      ResourceInformation[] clusterInfo =
          fsContext.getClusterResource().getResources();
      double[] shares1 = new double[2];
      double[] shares2 = new double[2];

      int dominant1 = calculateClusterAndFairRatios(resourceInfo1,
          s1.getWeight(), clusterInfo, shares1);
      int dominant2 = calculateClusterAndFairRatios(resourceInfo2,
          s2.getWeight(), clusterInfo, shares2);

      // A queue is needy for its min share if its dominant resource
      // (with respect to the cluster capacity) is below its configured min
      // share for that resource
      boolean s1Needy = resourceInfo1[dominant1].getValue() <
          minShareInfo1[dominant1].getValue();
      boolean s2Needy = resourceInfo1[dominant2].getValue() <
          minShareInfo2[dominant2].getValue();

      int res;

      if (!s2Needy && !s1Needy) {
        res = (int) Math.signum(shares1[dominant1] - shares2[dominant2]);

        if (res == 0) {
          // Because memory and CPU are indices 0 and 1, we can find the
          // non-dominant index by subtracting the dominant index from 1.
          res = (int) Math.signum(shares1[1 - dominant1] -
              shares2[1 - dominant2]);
        }
      } else if (s1Needy && !s2Needy) {
        res = -1;
      } else if (s2Needy && !s1Needy) {
        res = 1;
      } else {
        double[] minShares1 =
            calculateMinShareRatios(resourceInfo1, minShareInfo1);
        double[] minShares2 =
            calculateMinShareRatios(resourceInfo2, minShareInfo2);

        res = (int) Math.signum(minShares1[dominant1] - minShares2[dominant2]);

        if (res == 0) {
          res = (int) Math.signum(minShares1[1 - dominant1] -
              minShares2[1 - dominant2]);
        }
      }

      if (res == 0) {
        res = compareAttribrutes(s1, s2);
      }

      return res;
    }

    /**
     * Calculate a resource's usage ratio and approximate fair share ratio
     * assuming that CPU and memory are the only configured resource types.
     * The {@code shares} array will be populated with the approximate fair
     * share ratio for each resource type. The approximate fair share ratio
     * is calculated as {@code resourceInfo} divided by {@code cluster} and
     * the {@code weight}. If the cluster's resources are 100MB and
     * 10 vcores, the usage ({@code resourceInfo}) is 10 MB and 5 CPU, and the
     * weights are 2, the fair share ratios will be 0.05 and 0.25.
     *
     * The approximate fair share ratio is the usage divided by the
     * approximate fair share, i.e. the cluster resources times the weight.
     * The approximate fair share is an acceptable proxy for the fair share
     * because when comparing resources, the resource with the higher weight
     * will be assigned by the scheduler a proportionally higher fair share.
     *
     * The length of the {@code shares} array must be at least 2.
     *
     * The return value will be the index of the dominant resource type in the
     * {@code shares} array. The dominant resource is the resource type for
     * which {@code resourceInfo} has the largest usage ratio.
     *
     * @param resourceInfo the resource for which to calculate ratios
     * @param weight the resource weight
     * @param clusterInfo the total cluster resources
     * @param shares the share ratios array to populate
     * @return the index of the resource type with the largest cluster share
     */
    @VisibleForTesting
    int calculateClusterAndFairRatios(ResourceInformation[] resourceInfo,
        float weight, ResourceInformation[] clusterInfo, double[] shares) {
      int dominant;

      shares[Resource.MEMORY_INDEX] =
          ((double) resourceInfo[Resource.MEMORY_INDEX].getValue()) /
          clusterInfo[Resource.MEMORY_INDEX].getValue();
      shares[Resource.VCORES_INDEX] =
          ((double) resourceInfo[Resource.VCORES_INDEX].getValue()) /
          clusterInfo[Resource.VCORES_INDEX].getValue();
      dominant =
          shares[Resource.VCORES_INDEX] > shares[Resource.MEMORY_INDEX] ?
          Resource.VCORES_INDEX : Resource.MEMORY_INDEX;

      shares[Resource.MEMORY_INDEX] /= weight;
      shares[Resource.VCORES_INDEX] /= weight;

      return dominant;
    }

    /**
     * Calculate a resource's min share ratios assuming that CPU and memory
     * are the only configured resource types. The return array will be
     * populated with the {@code resourceInfo} divided by {@code minShareInfo}
     * for each resource type. If the min shares are 5 MB and 10 vcores, and
     * the usage ({@code resourceInfo}) is 10 MB and 5 CPU, the ratios will
     * be 2 and 0.5.
     *
     * The length of the {@code ratios} array must be 2.
     *
     * @param resourceInfo the resource for which to calculate min shares
     * @param minShareInfo the min share
     * @return the share ratios
     */
    @VisibleForTesting
    double[] calculateMinShareRatios(ResourceInformation[] resourceInfo,
        ResourceInformation[] minShareInfo) {
      double[] minShares1 = new double[2];

      // both are needy below min share
      minShares1[Resource.MEMORY_INDEX] =
          ((double) resourceInfo[Resource.MEMORY_INDEX].getValue()) /
          minShareInfo[Resource.MEMORY_INDEX].getValue();
      minShares1[Resource.VCORES_INDEX] =
          ((double) resourceInfo[Resource.VCORES_INDEX].getValue()) /
          minShareInfo[Resource.VCORES_INDEX].getValue();

      return minShares1;
    }
  }
}
