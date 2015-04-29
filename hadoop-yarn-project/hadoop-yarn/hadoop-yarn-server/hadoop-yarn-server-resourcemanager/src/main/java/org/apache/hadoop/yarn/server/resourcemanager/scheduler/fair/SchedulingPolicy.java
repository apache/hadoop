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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

@Public
@Evolving
public abstract class SchedulingPolicy {
  private static final ConcurrentHashMap<Class<? extends SchedulingPolicy>, SchedulingPolicy> instances =
      new ConcurrentHashMap<Class<? extends SchedulingPolicy>, SchedulingPolicy>();

  public static final SchedulingPolicy DEFAULT_POLICY =
      getInstance(FairSharePolicy.class);
  
  public static final byte DEPTH_LEAF = (byte) 1;
  public static final byte DEPTH_INTERMEDIATE = (byte) 2;
  public static final byte DEPTH_ROOT = (byte) 4;
  public static final byte DEPTH_PARENT = (byte) 6; // Root and Intermediate
  public static final byte DEPTH_ANY = (byte) 7;

  /**
   * Returns a {@link SchedulingPolicy} instance corresponding to the passed clazz
   */
  public static SchedulingPolicy getInstance(Class<? extends SchedulingPolicy> clazz) {
    SchedulingPolicy policy = ReflectionUtils.newInstance(clazz, null);
    SchedulingPolicy policyRet = instances.putIfAbsent(clazz, policy);
    if(policyRet != null) {
      return policyRet;
    }
    return policy;
  }

  /**
   * Returns {@link SchedulingPolicy} instance corresponding to the
   * {@link SchedulingPolicy} passed as a string. The policy can be "fair" for
   * FairSharePolicy, "fifo" for FifoPolicy, or "drf" for
   * DominantResourceFairnessPolicy. For a custom
   * {@link SchedulingPolicy}s in the RM classpath, the policy should be
   * canonical class name of the {@link SchedulingPolicy}.
   * 
   * @param policy canonical class name or "drf" or "fair" or "fifo"
   * @throws AllocationConfigurationException
   */
  @SuppressWarnings("unchecked")
  public static SchedulingPolicy parse(String policy)
      throws AllocationConfigurationException {
    @SuppressWarnings("rawtypes")
    Class clazz;
    String text = StringUtils.toLowerCase(policy);
    if (text.equalsIgnoreCase(FairSharePolicy.NAME)) {
      clazz = FairSharePolicy.class;
    } else if (text.equalsIgnoreCase(FifoPolicy.NAME)) {
      clazz = FifoPolicy.class;
    } else if (text.equalsIgnoreCase(DominantResourceFairnessPolicy.NAME)) {
      clazz = DominantResourceFairnessPolicy.class;
    } else {
      try {
        clazz = Class.forName(policy);
      } catch (ClassNotFoundException cnfe) {
        throw new AllocationConfigurationException(policy
            + " SchedulingPolicy class not found!");
      }
    }
    if (!SchedulingPolicy.class.isAssignableFrom(clazz)) {
      throw new AllocationConfigurationException(policy
          + " does not extend SchedulingPolicy");
    }
    return getInstance(clazz);
  }
  
  public void initialize(Resource clusterCapacity) {}

  /**
   * @return returns the name of {@link SchedulingPolicy}
   */
  public abstract String getName();

  /**
   * Specifies the depths in the hierarchy, this {@link SchedulingPolicy}
   * applies to
   * 
   * @return depth equal to one of fields {@link SchedulingPolicy}#DEPTH_*
   */
  public abstract byte getApplicableDepth();

  /**
   * Checks if the specified {@link SchedulingPolicy} can be used for a queue at
   * the specified depth in the hierarchy
   * 
   * @param policy {@link SchedulingPolicy} we are checking the
   *          depth-applicability for
   * @param depth queue's depth in the hierarchy
   * @return true if policy is applicable to passed depth, false otherwise
   */
  public static boolean isApplicableTo(SchedulingPolicy policy, byte depth) {
    return ((policy.getApplicableDepth() & depth) == depth) ? true : false;
  }

  /**
   * The comparator returned by this method is to be used for sorting the
   * {@link Schedulable}s in that queue.
   * 
   * @return the comparator to sort by
   */
  public abstract Comparator<Schedulable> getComparator();

  /**
   * Computes and updates the shares of {@link Schedulable}s as per
   * the {@link SchedulingPolicy}, to be used later for scheduling decisions.
   * The shares computed are instantaneous and only consider queues with
   * running applications.
   * 
   * @param schedulables {@link Schedulable}s whose shares are to be updated
   * @param totalResources Total {@link Resource}s in the cluster
   */
  public abstract void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources);

  /**
   * Computes and updates the steady shares of {@link FSQueue}s as per the
   * {@link SchedulingPolicy}. The steady share does not differentiate
   * between queues with and without running applications under them. The
   * steady share is not used for scheduling, it is displayed on the Web UI
   * for better visibility.
   *
   * @param queues {@link FSQueue}s whose shares are to be updated
   * @param totalResources Total {@link Resource}s in the cluster
   */
  public abstract void computeSteadyShares(
      Collection<? extends FSQueue> queues, Resource totalResources);

  /**
   * Check if the resource usage is over the fair share under this policy
   *
   * @param usage {@link Resource} the resource usage
   * @param fairShare {@link Resource} the fair share
   * @return true if check passes (is over) or false otherwise
   */
  public abstract boolean checkIfUsageOverFairShare(
      Resource usage, Resource fairShare);

  /**
   * Check if a leaf queue's AM resource usage over its limit under this policy
   *
   * @param usage {@link Resource} the resource used by application masters
   * @param maxAMResource {@link Resource} the maximum allowed resource for
   *                                      application masters
   * @return true if AM resource usage is over the limit
   */
  public abstract boolean checkIfAMResourceUsageOverLimit(
      Resource usage, Resource maxAMResource);

  /**
   * Get headroom by calculating the min of <code>clusterAvailable</code> and
   * (<code>queueFairShare</code> - <code>queueUsage</code>) resources that are
   * applicable to this policy. For eg if only memory then leave other
   * resources such as CPU to same as clusterAvailable.
   *
   * @param queueFairShare fairshare in the queue
   * @param queueUsage resources used in the queue
   * @param maxAvailable available resource in cluster for this queue
   * @return calculated headroom
   */
  public abstract Resource getHeadroom(Resource queueFairShare,
      Resource queueUsage, Resource maxAvailable);

}
