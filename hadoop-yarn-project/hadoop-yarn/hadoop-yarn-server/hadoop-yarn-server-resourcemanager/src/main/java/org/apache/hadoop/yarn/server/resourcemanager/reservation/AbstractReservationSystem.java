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
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ReservationsACLsManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is the implementation of {@link ReservationSystem} based on the
 * {@link ResourceScheduler}
 */
@LimitedPrivate("yarn")
@Unstable
public abstract class AbstractReservationSystem extends AbstractService
    implements ReservationSystem {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbstractReservationSystem.class);

  // private static final String DEFAULT_CAPACITY_SCHEDULER_PLAN

  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock(true);
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  private boolean initialized = false;

  private final Clock clock = new UTCClock();

  private AtomicLong resCounter = new AtomicLong();

  private Map<String, Plan> plans = new HashMap<String, Plan>();

  private Map<ReservationId, String> resQMap =
      new HashMap<ReservationId, String>();

  private RMContext rmContext;

  private ResourceScheduler scheduler;

  private ScheduledExecutorService scheduledExecutorService;

  protected Configuration conf;

  protected long planStepSize;

  private PlanFollower planFollower;

  private ReservationsACLsManager reservationsACLsManager;

  private boolean isRecoveryEnabled = false;

  /**
   * Construct the service.
   * 
   * @param name service name
   */
  public AbstractReservationSystem(String name) {
    super(name);
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    writeLock.lock();
    try {
      this.rmContext = rmContext;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException {
    writeLock.lock();
    try {
      if (!initialized) {
        initialize(conf);
        initialized = true;
      } else {
        initializeNewPlans(conf);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void initialize(Configuration conf) throws YarnException {
    LOG.info("Initializing Reservation system");
    this.conf = conf;
    scheduler = rmContext.getScheduler();
    // Get the plan step size
    planStepSize =
        conf.getTimeDuration(
            YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP,
            YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP,
            TimeUnit.MILLISECONDS);
    if (planStepSize < 0) {
      planStepSize =
          YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP;
    }
    // Create a plan corresponding to every reservable queue
    Set<String> planQueueNames = scheduler.getPlanQueues();
    for (String planQueueName : planQueueNames) {
      Plan plan = initializePlan(planQueueName);
      plans.put(planQueueName, plan);
    }
    isRecoveryEnabled = conf.getBoolean(
        YarnConfiguration.RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

    if (conf.getBoolean(YarnConfiguration.YARN_RESERVATION_ACL_ENABLE,
            YarnConfiguration.DEFAULT_YARN_RESERVATION_ACL_ENABLE) &&
                    conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
                            YarnConfiguration.DEFAULT_YARN_ACL_ENABLE)) {
      reservationsACLsManager = new ReservationsACLsManager(scheduler, conf);
    }
  }

  private void loadPlan(String planName,
      Map<ReservationId, ReservationAllocationStateProto> reservations)
          throws PlanningException {
    Plan plan = plans.get(planName);
    Resource minAllocation = getMinAllocation();
    ResourceCalculator rescCalculator = getResourceCalculator();
    for (Entry<ReservationId, ReservationAllocationStateProto> currentReservation : reservations
        .entrySet()) {
      plan.addReservation(ReservationSystemUtil.toInMemoryAllocation(planName,
          currentReservation.getKey(), currentReservation.getValue(),
          minAllocation, rescCalculator), true);
      resQMap.put(currentReservation.getKey(), planName);
    }
    LOG.info("Recovered reservations for Plan: {}", planName);
  }

  @Override
  public void recover(RMState state) throws Exception {
    LOG.info("Recovering Reservation system");
    writeLock.lock();
    try {
      Map<String, Map<ReservationId, ReservationAllocationStateProto>> reservationSystemState =
          state.getReservationState();
      if (planFollower != null) {
        for (String plan : plans.keySet()) {
          // recover reservations if any from state store
          if (reservationSystemState.containsKey(plan)) {
            loadPlan(plan, reservationSystemState.get(plan));
          }
          synchronizePlan(plan, false);
        }
        startPlanFollower(conf.getLong(
            YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
            YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void initializeNewPlans(Configuration conf) {
    LOG.info("Refreshing Reservation system");
    writeLock.lock();
    try {
      // Create a plan corresponding to every new reservable queue
      Set<String> planQueueNames = scheduler.getPlanQueues();
      for (String planQueueName : planQueueNames) {
        if (!plans.containsKey(planQueueName)) {
          Plan plan = initializePlan(planQueueName);
          plans.put(planQueueName, plan);
        } else {
          LOG.warn("Plan based on reservation queue {} already exists.",
              planQueueName);
        }
      }
      // Update the plan follower with the active plans
      if (planFollower != null) {
        planFollower.setPlans(plans.values());
      }
    } catch (YarnException e) {
      LOG.warn("Exception while trying to refresh reservable queues", e);
    } finally {
      writeLock.unlock();
    }
  }

  private PlanFollower createPlanFollower() {
    String planFollowerPolicyClassName =
        conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER,
            getDefaultPlanFollower());
    if (planFollowerPolicyClassName == null) {
      return null;
    }
    LOG.info("Using PlanFollowerPolicy: " + planFollowerPolicyClassName);
    try {
      Class<?> planFollowerPolicyClazz =
          conf.getClassByName(planFollowerPolicyClassName);
      if (PlanFollower.class.isAssignableFrom(planFollowerPolicyClazz)) {
        return (PlanFollower) ReflectionUtils.newInstance(
            planFollowerPolicyClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + planFollowerPolicyClassName
            + " not instance of " + PlanFollower.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate PlanFollowerPolicy: "
              + planFollowerPolicyClassName, e);
    }
  }

  private String getDefaultPlanFollower() {
    // currently only capacity scheduler is supported
    if (scheduler instanceof CapacityScheduler) {
      return CapacitySchedulerPlanFollower.class.getName();
    } else if (scheduler instanceof FairScheduler) {
      return FairSchedulerPlanFollower.class.getName();
    }
    return null;
  }

  @Override
  public Plan getPlan(String planName) {
    readLock.lock();
    try {
      return plans.get(planName);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @return the planStepSize
   */
  @Override
  public long getPlanFollowerTimeStep() {
    readLock.lock();
    try {
      return planStepSize;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void synchronizePlan(String planName, boolean shouldReplan) {
    writeLock.lock();
    try {
      Plan plan = plans.get(planName);
      if (plan != null) {
        planFollower.synchronizePlan(plan, shouldReplan);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void startPlanFollower(long initialDelay) {
    if (planFollower != null) {
      scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
      scheduledExecutorService.scheduleWithFixedDelay(planFollower,
          initialDelay, planStepSize, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    reinitialize(configuration, rmContext);
    // Create the plan follower with the active plans
    planFollower = createPlanFollower();
    if (planFollower != null) {
      planFollower.init(clock, scheduler, plans.values());
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    if (!isRecoveryEnabled) {
      startPlanFollower(planStepSize);
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() {
    // Stop the plan follower
    if (scheduledExecutorService != null
        && !scheduledExecutorService.isShutdown()) {
      scheduledExecutorService.shutdown();
    }
    // Clear the plans
    plans.clear();
  }

  @Override
  public String getQueueForReservation(ReservationId reservationId) {
    readLock.lock();
    try {
      return resQMap.get(reservationId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setQueueForReservation(ReservationId reservationId,
      String queueName) {
    writeLock.lock();
    try {
      resQMap.put(reservationId, queueName);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public ReservationId getNewReservationId() {
    writeLock.lock();
    try {
      ReservationId resId =
          ReservationId.newInstance(ResourceManager.getClusterTimeStamp(),
              resCounter.incrementAndGet());
      LOG.info("Allocated new reservationId: " + resId);
      return resId;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Map<String, Plan> getAllPlans() {
    return plans;
  }

  /**
   * Get the default reservation system corresponding to the scheduler
   * 
   * @param scheduler the scheduler for which the reservation system is required
   */
  public static String getDefaultReservationSystem(ResourceScheduler scheduler) {
    if (scheduler instanceof CapacityScheduler) {
      return CapacityReservationSystem.class.getName();
    } else if (scheduler instanceof FairScheduler) {
      return FairReservationSystem.class.getName();
    }
    return null;
  }

  protected Plan initializePlan(String planQueueName) throws YarnException {
    String planQueuePath = getPlanQueuePath(planQueueName);
    SharingPolicy adPolicy = getAdmissionPolicy(planQueuePath);
    adPolicy.init(planQueuePath, getReservationSchedulerConfiguration());
    // Calculate the max plan capacity
    Resource minAllocation = getMinAllocation();
    Resource maxAllocation = getMaxAllocation();
    ResourceCalculator rescCalc = getResourceCalculator();
    Resource totCap = getPlanQueueCapacity(planQueueName);
    Plan plan =
        new InMemoryPlan(getRootQueueMetrics(), adPolicy,
            getAgent(planQueuePath), totCap, planStepSize, rescCalc,
            minAllocation, maxAllocation, planQueueName,
            getReplanner(planQueuePath), getReservationSchedulerConfiguration()
            .getMoveOnExpiry(planQueuePath), rmContext);
    LOG.info("Intialized plan {} based on reservable queue {}",
        plan.toString(), planQueueName);
    return plan;
  }

  protected Planner getReplanner(String planQueueName) {
    ReservationSchedulerConfiguration reservationConfig =
        getReservationSchedulerConfiguration();
    String plannerClassName = reservationConfig.getReplanner(planQueueName);
    LOG.info("Using Replanner: " + plannerClassName + " for queue: "
        + planQueueName);
    try {
      Class<?> plannerClazz = conf.getClassByName(plannerClassName);
      if (Planner.class.isAssignableFrom(plannerClazz)) {
        Planner planner =
            (Planner) ReflectionUtils.newInstance(plannerClazz, conf);
        planner.init(planQueueName, reservationConfig);
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

  protected ReservationAgent getAgent(String queueName) {
    ReservationSchedulerConfiguration reservationConfig =
        getReservationSchedulerConfiguration();
    String agentClassName = reservationConfig.getReservationAgent(queueName);
    LOG.info("Using Agent: " + agentClassName + " for queue: " + queueName);
    try {
      Class<?> agentClazz = conf.getClassByName(agentClassName);
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

  protected SharingPolicy getAdmissionPolicy(String queueName) {
    ReservationSchedulerConfiguration reservationConfig =
        getReservationSchedulerConfiguration();
    String admissionPolicyClassName =
        reservationConfig.getReservationAdmissionPolicy(queueName);
    LOG.info("Using AdmissionPolicy: " + admissionPolicyClassName
        + " for queue: " + queueName);
    try {
      Class<?> admissionPolicyClazz =
          conf.getClassByName(admissionPolicyClassName);
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

  public ReservationsACLsManager getReservationsACLsManager() {
    return this.reservationsACLsManager;
  }

  protected abstract ReservationSchedulerConfiguration
      getReservationSchedulerConfiguration();

  protected abstract String getPlanQueuePath(String planQueueName);

  protected abstract Resource getPlanQueueCapacity(String planQueueName);

  protected abstract Resource getMinAllocation();

  protected abstract Resource getMaxAllocation();

  protected abstract ResourceCalculator getResourceCalculator();

  protected abstract QueueMetrics getRootQueueMetrics();
}
