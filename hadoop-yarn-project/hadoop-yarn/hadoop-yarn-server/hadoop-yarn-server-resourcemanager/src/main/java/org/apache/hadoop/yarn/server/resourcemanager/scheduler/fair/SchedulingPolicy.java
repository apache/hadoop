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

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;

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
    SchedulingPolicy policy = instances.get(clazz);
    if (policy == null) {
      policy = ReflectionUtils.newInstance(clazz, null);
      instances.put(clazz, policy);
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
    String text = policy.toLowerCase();
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
   * Computes and updates the shares of {@link Schedulable}s as per the
   * {@link SchedulingPolicy}, to be used later at schedule time.
   * 
   * @param schedulables {@link Schedulable}s whose shares are to be updated
   * @param totalResources Total {@link Resource}s in the cluster
   */
  public abstract void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources);
}
