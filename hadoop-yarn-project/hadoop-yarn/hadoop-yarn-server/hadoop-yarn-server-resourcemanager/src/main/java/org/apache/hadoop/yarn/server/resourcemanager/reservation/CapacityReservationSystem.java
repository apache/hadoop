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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the implementation of {@link ReservationSystem} based on the
 * {@link CapacityScheduler}
 */
@LimitedPrivate("yarn")
@Unstable
public class CapacityReservationSystem extends AbstractReservationSystem {

  private static final Logger LOG = LoggerFactory
      .getLogger(CapacityReservationSystem.class);

  private CapacityScheduler capScheduler;

  public CapacityReservationSystem() {
    super(CapacityReservationSystem.class.getName());
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException {
    // Validate if the scheduler is capacity based
    ResourceScheduler scheduler = rmContext.getScheduler();
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class "
          + scheduler.getClass().getCanonicalName() + " not instance of "
          + CapacityScheduler.class.getCanonicalName());
    }
    capScheduler = (CapacityScheduler) scheduler;
    this.conf = conf;
    super.reinitialize(conf, rmContext);
  }

  @Override
  protected Plan initializePlan(String planQueueName) throws YarnException {
    SharingPolicy adPolicy = getAdmissionPolicy(planQueueName);
    String planQueuePath = capScheduler.getQueue(planQueueName).getQueuePath();
    adPolicy.init(planQueuePath, capScheduler.getConfiguration());
    CSQueue planQueue = capScheduler.getQueue(planQueueName);
    // Calculate the max plan capacity
    Resource minAllocation = capScheduler.getMinimumResourceCapability();
    ResourceCalculator rescCalc = capScheduler.getResourceCalculator();
    Resource totCap =
        rescCalc.multiplyAndNormalizeDown(capScheduler.getClusterResource(),
            planQueue.getAbsoluteCapacity(), minAllocation);
    Plan plan =
        new InMemoryPlan(capScheduler.getRootQueueMetrics(), adPolicy,
            getAgent(planQueuePath), totCap, planStepSize, rescCalc,
            minAllocation, capScheduler.getMaximumResourceCapability(),
            planQueueName, getReplanner(planQueuePath), capScheduler
                .getConfiguration().getMoveOnExpiry(planQueuePath));
    LOG.info("Intialized plan {0} based on reservable queue {1}",
        plan.toString(), planQueueName);
    return plan;
  }

  @Override
  protected Planner getReplanner(String planQueueName) {
    CapacitySchedulerConfiguration capSchedulerConfig =
        capScheduler.getConfiguration();
    String plannerClassName = capSchedulerConfig.getReplanner(planQueueName);
    LOG.info("Using Replanner: " + plannerClassName + " for queue: "
        + planQueueName);
    try {
      Class<?> plannerClazz =
          capSchedulerConfig.getClassByName(plannerClassName);
      if (Planner.class.isAssignableFrom(plannerClazz)) {
        Planner planner =
            (Planner) ReflectionUtils.newInstance(plannerClazz, conf);
        planner.init(planQueueName, capSchedulerConfig);
        return planner;
      } else {
        throw new YarnRuntimeException("Class: " + plannerClazz
            + " not instance of " + Planner.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Planner: "
          + plannerClassName + " for queue: " + planQueueName, e);
    }
  }

  @Override
  protected ReservationAgent getAgent(String queueName) {
    CapacitySchedulerConfiguration capSchedulerConfig =
        capScheduler.getConfiguration();
    String agentClassName = capSchedulerConfig.getReservationAgent(queueName);
    LOG.info("Using Agent: " + agentClassName + " for queue: " + queueName);
    try {
      Class<?> agentClazz = capSchedulerConfig.getClassByName(agentClassName);
      if (ReservationAgent.class.isAssignableFrom(agentClazz)) {
        return (ReservationAgent) ReflectionUtils.newInstance(agentClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + agentClassName
            + " not instance of " + ReservationAgent.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Agent: "
          + agentClassName + " for queue: " + queueName, e);
    }
  }

  @Override
  protected SharingPolicy getAdmissionPolicy(String queueName) {
    CapacitySchedulerConfiguration capSchedulerConfig =
        capScheduler.getConfiguration();
    String admissionPolicyClassName =
        capSchedulerConfig.getReservationAdmissionPolicy(queueName);
    LOG.info("Using AdmissionPolicy: " + admissionPolicyClassName
        + " for queue: " + queueName);
    try {
      Class<?> admissionPolicyClazz =
          capSchedulerConfig.getClassByName(admissionPolicyClassName);
      if (SharingPolicy.class.isAssignableFrom(admissionPolicyClazz)) {
        return (SharingPolicy) ReflectionUtils.newInstance(
            admissionPolicyClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + admissionPolicyClassName
            + " not instance of " + SharingPolicy.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate AdmissionPolicy: "
          + admissionPolicyClassName + " for queue: " + queueName, e);
    }
  }

}
