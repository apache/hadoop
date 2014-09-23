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

import java.util.Collection;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType.*;

/**
 * Makes scheduling decisions by trying to equalize dominant resource usage.
 * A schedulable's dominant resource usage is the largest ratio of resource
 * usage to capacity among the resource types it is using.
 */
@Private
@Unstable
public class DominantResourceFairnessPolicy extends SchedulingPolicy {

  public static final String NAME = "DRF";

  private DominantResourceFairnessComparator comparator =
      new DominantResourceFairnessComparator();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public byte getApplicableDepth() {
    return SchedulingPolicy.DEPTH_ANY;
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    return comparator;
  }
  
  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    for (ResourceType type : ResourceType.values()) {
      ComputeFairShares.computeShares(schedulables, totalResources, type);
    }
  }

  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    for (ResourceType type : ResourceType.values()) {
      ComputeFairShares.computeSteadyShares(queues, totalResources, type);
    }
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    return !Resources.fitsIn(usage, fairShare);
  }

  @Override
  public boolean checkIfAMResourceUsageOverLimit(Resource usage, Resource maxAMResource) {
    return !Resources.fitsIn(usage, maxAMResource);
  }

  @Override
  public Resource getHeadroom(Resource queueFairShare, Resource queueUsage,
                              Resource clusterAvailable) {
    int queueAvailableMemory =
        Math.max(queueFairShare.getMemory() - queueUsage.getMemory(), 0);
    int queueAvailableCPU =
        Math.max(queueFairShare.getVirtualCores() - queueUsage
            .getVirtualCores(), 0);
    Resource headroom = Resources.createResource(
        Math.min(clusterAvailable.getMemory(), queueAvailableMemory),
        Math.min(clusterAvailable.getVirtualCores(),
            queueAvailableCPU));
    return headroom;
  }

  @Override
  public void initialize(Resource clusterCapacity) {
    comparator.setClusterCapacity(clusterCapacity);
  }

  public static class DominantResourceFairnessComparator implements Comparator<Schedulable> {
    private static final int NUM_RESOURCES = ResourceType.values().length;
    
    private Resource clusterCapacity;

    public void setClusterCapacity(Resource clusterCapacity) {
      this.clusterCapacity = clusterCapacity;
    }

    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      ResourceWeights sharesOfCluster1 = new ResourceWeights();
      ResourceWeights sharesOfCluster2 = new ResourceWeights();
      ResourceWeights sharesOfMinShare1 = new ResourceWeights();
      ResourceWeights sharesOfMinShare2 = new ResourceWeights();
      ResourceType[] resourceOrder1 = new ResourceType[NUM_RESOURCES];
      ResourceType[] resourceOrder2 = new ResourceType[NUM_RESOURCES];
      
      // Calculate shares of the cluster for each resource both schedulables.
      calculateShares(s1.getResourceUsage(),
          clusterCapacity, sharesOfCluster1, resourceOrder1, s1.getWeights());
      calculateShares(s1.getResourceUsage(),
          s1.getMinShare(), sharesOfMinShare1, null, ResourceWeights.NEUTRAL);
      calculateShares(s2.getResourceUsage(),
          clusterCapacity, sharesOfCluster2, resourceOrder2, s2.getWeights());
      calculateShares(s2.getResourceUsage(),
          s2.getMinShare(), sharesOfMinShare2, null, ResourceWeights.NEUTRAL);
      
      // A queue is needy for its min share if its dominant resource
      // (with respect to the cluster capacity) is below its configured min share
      // for that resource
      boolean s1Needy = sharesOfMinShare1.getWeight(resourceOrder1[0]) < 1.0f;
      boolean s2Needy = sharesOfMinShare2.getWeight(resourceOrder2[0]) < 1.0f;
      
      int res = 0;
      if (!s2Needy && !s1Needy) {
        res = compareShares(sharesOfCluster1, sharesOfCluster2,
            resourceOrder1, resourceOrder2);
      } else if (s1Needy && !s2Needy) {
        res = -1;
      } else if (s2Needy && !s1Needy) {
        res = 1;
      } else { // both are needy below min share
        res = compareShares(sharesOfMinShare1, sharesOfMinShare2,
            resourceOrder1, resourceOrder2);
      }
      if (res == 0) {
        // Apps are tied in fairness ratio. Break the tie by submit time.
        res = (int)(s1.getStartTime() - s2.getStartTime());
      }
      return res;
    }
    
    /**
     * Calculates and orders a resource's share of a pool in terms of two vectors.
     * The shares vector contains, for each resource, the fraction of the pool that
     * it takes up.  The resourceOrder vector contains an ordering of resources
     * by largest share.  So if resource=<10 MB, 5 CPU>, and pool=<100 MB, 10 CPU>,
     * shares will be [.1, .5] and resourceOrder will be [CPU, MEMORY].
     */
    void calculateShares(Resource resource, Resource pool,
        ResourceWeights shares, ResourceType[] resourceOrder, ResourceWeights weights) {
      shares.setWeight(MEMORY, (float)resource.getMemory() /
          (pool.getMemory() * weights.getWeight(MEMORY)));
      shares.setWeight(CPU, (float)resource.getVirtualCores() /
          (pool.getVirtualCores() * weights.getWeight(CPU)));
      // sort order vector by resource share
      if (resourceOrder != null) {
        if (shares.getWeight(MEMORY) > shares.getWeight(CPU)) {
          resourceOrder[0] = MEMORY;
          resourceOrder[1] = CPU;
        } else  {
          resourceOrder[0] = CPU;
          resourceOrder[1] = MEMORY;
        }
      }
    }
    
    private int compareShares(ResourceWeights shares1, ResourceWeights shares2,
        ResourceType[] resourceOrder1, ResourceType[] resourceOrder2) {
      for (int i = 0; i < resourceOrder1.length; i++) {
        int ret = (int)Math.signum(shares1.getWeight(resourceOrder1[i])
            - shares2.getWeight(resourceOrder2[i]));
        if (ret != 0) {
          return ret;
        }
      }
      return 0;
    }
  }
}
