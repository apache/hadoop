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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs.DynamicQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueTree;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.TestQueueConfigResolver;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Parity between the resolver and trunk behavior.
 * <ul>
 * <li>Every {@code CapacitySchedulerConfiguration} per-queue getter returns
 * the resolved value, or throws the resolved failure, for every queue of a
 * set of configurations.</li>
 * <li>Inherited and derived values equal the values of the queues a MockRM
 * builds from the same configuration.</li>
 * <li>For dynamic v1 and v2 queues created in a MockRM, the getters of the
 * queue context configuration, which holds the template writes, return the
 * values {@link QueueConfigResolver#resolveDynamicQueue} resolves.</li>
 * </ul>
 * For configurations trunk rejects at startup, the resolver must hold a
 * failed value with the exception type and message that failed queue
 * initialization.
 */
public class TestQueueConfigResolverParity {
  /**
   * Configurations that trunk rejects with a check (mixed capacity
   * modes of siblings) rather than a parse failure, so the resolver has no
   * failed value for them.
   */
  private static final Set<String> CHECK_REJECTIONS = new HashSet<>(
      Arrays.asList("absolute with weight units", "absolute before percentage"));
  private static final String P = CapacitySchedulerConfiguration.PREFIX;
  private static final int GB = 1024;

  /** Reads the value a getter returns for a queue and label. */
  private interface Getter {
    Object get(CapacitySchedulerConfiguration conf, QueuePath queue,
        String label, ResolutionInputs inputs);
  }

  /** Properties a queue may inherit from its parent queue. */
  private static final Set<QueueProperty<?>> INHERITED =
      new HashSet<>(Arrays.<QueueProperty<?>>asList(USER_LIMIT,
          USER_LIMIT_FACTOR, USER_WEIGHTS, STATE, ACCESSIBLE_NODE_LABELS,
          DEFAULT_NODE_LABEL_EXPRESSION, QUEUE_ORDERING_POLICY,
          MAXIMUM_ALLOCATION, PREEMPTION_DISABLED,
          INTRA_QUEUE_PREEMPTION_DISABLED, MAXIMUM_APPLICATION_LIFETIME,
          DEFAULT_APPLICATION_LIFETIME));
  /** Properties templates do not write for dynamic queues. */
  private static final Set<QueueProperty<?>> NOT_TEMPLATED =
      new HashSet<>(Arrays.<QueueProperty<?>>asList(QUEUES, USER_WEIGHTS,
          ACL_SUBMIT_RESERVATIONS, ACL_LIST_RESERVATIONS,
          ACL_ADMINISTER_RESERVATIONS, ALLOW_ZERO_CAPACITY_SUM,
          AUTO_CREATE_CHILD_QUEUE_ENABLED, AUTO_CREATE_CHILD_QUEUE_MAX_QUEUES,
          AUTO_CREATE_CHILD_QUEUE_FAIL_ON_EXCEEDING_CAPACITY,
          AUTO_CREATED_QUEUE_MANAGEMENT_POLICY_NAME, LEAF_QUEUE_TEMPLATE,
          AUTO_QUEUE_CREATION_V2_ENABLED, AUTO_QUEUE_CREATION_V2_MAX_DEPTH,
          AQC_V2_TEMPLATE, AQC_V2_LEAF_TEMPLATE, AQC_V2_PARENT_TEMPLATE,
          RESERVABLE, RESERVATION_WINDOW_MS, AVERAGE_CAPACITY_PERCENT,
          INSTANTANEOUS_MAX_CAPACITY_PERCENT, RESERVATION_ADMISSION_POLICY_NAME,
          RESERVATION_AGENT, RESERVATION_PLANNER,
          RESERVATION_MOVE_ON_EXPIRY_ENABLED, RESERVATION_ENFORCEMENT_WINDOW_MS,
          SHOW_RESERVATIONS_AS_QUEUES));

  private static final Map<QueueProperty<?>, Getter> GETTERS =
      new LinkedHashMap<>();

  static {
    GETTERS.put(QUEUES, (c, q, l, i) -> c.getQueues(q));
    GETTERS.put(CAPACITY, (c, q, l, i) -> c.getNonLabeledQueueCapacity(q));
    GETTERS.put(CAPACITY_WEIGHT, (c, q, l, i) -> l.equals(NO_LABEL)
        ? c.getNonLabeledQueueWeight(q) : c.getLabeledQueueWeight(q, l));
    GETTERS.put(CAPACITY_IS_ABSOLUTE_RESOURCE, (c, q, l, i) ->
        c.checkConfigTypeIsAbsoluteResource(l, q, QUEUE_RESOURCE_TYPES));
    GETTERS.put(MINIMUM_RESOURCE, (c, q, l, i) ->
        c.getMinimumResourceRequirement(l, q, QUEUE_RESOURCE_TYPES));
    GETTERS.put(CAPACITY_VECTOR, (c, q, l, i) -> c
        .parseConfiguredResourceVector(q, Collections.singleton(l)).get(l));
    GETTERS.put(MAXIMUM_CAPACITY,
        (c, q, l, i) -> c.getNonLabeledQueueMaximumCapacity(q));
    GETTERS.put(MAXIMUM_RESOURCE, (c, q, l, i) ->
        c.getMaximumResourceRequirement(l, q, QUEUE_RESOURCE_TYPES));
    GETTERS.put(MAXIMUM_CAPACITY_VECTOR, (c, q, l, i) -> c
        .parseConfiguredMaximumCapacityVector(q, Collections.singleton(l),
            QueueCapacityVector.newInstance()).get(l));
    GETTERS.put(LABELED_CAPACITY,
        (c, q, l, i) -> c.getLabeledQueueCapacity(q, l));
    GETTERS.put(LABELED_MAXIMUM_CAPACITY,
        (c, q, l, i) -> c.getLabeledQueueMaximumCapacity(q, l));
    GETTERS.put(MAXIMUM_AM_RESOURCE_PERCENT, (c, q, l, i) ->
        c.getMaximumApplicationMasterResourcePerQueuePercent(q));
    GETTERS.put(LABELED_MAXIMUM_AM_RESOURCE_PERCENT,
        (c, q, l, i) -> c.getMaximumAMResourcePercentPerPartition(q, l));
    GETTERS.put(MAXIMUM_APPLICATIONS,
        (c, q, l, i) -> c.getMaximumApplicationsPerQueue(q));
    GETTERS.put(USER_LIMIT, (c, q, l, i) -> c.getUserLimit(q));
    GETTERS.put(USER_LIMIT_FACTOR, (c, q, l, i) -> c.getUserLimitFactor(q));
    GETTERS.put(STATE, (c, q, l, i) -> c.getConfiguredState(q));
    GETTERS.put(ACCESSIBLE_NODE_LABELS,
        (c, q, l, i) -> c.getAccessibleNodeLabels(q));
    GETTERS.put(DEFAULT_NODE_LABEL_EXPRESSION,
        (c, q, l, i) -> c.getDefaultNodeLabelExpression(q));
    GETTERS.put(ACL_SUBMIT_APPLICATIONS,
        (c, q, l, i) -> c.getAcl(q, QueueACL.SUBMIT_APPLICATIONS));
    GETTERS.put(ACL_ADMINISTER_QUEUE,
        (c, q, l, i) -> c.getAcl(q, QueueACL.ADMINISTER_QUEUE));
    GETTERS.put(ACL_APPLICATION_MAX_PRIORITY, (c, q, l, i) ->
        c.getPriorityAcls(q, i.getClusterMaximumApplicationPriority()));
    GETTERS.put(ACL_SUBMIT_RESERVATIONS, (c, q, l, i) ->
        c.getReservationAcls(q).get(ReservationACL.SUBMIT_RESERVATIONS));
    GETTERS.put(ACL_LIST_RESERVATIONS, (c, q, l, i) ->
        c.getReservationAcls(q).get(ReservationACL.LIST_RESERVATIONS));
    GETTERS.put(ACL_ADMINISTER_RESERVATIONS, (c, q, l, i) ->
        c.getReservationAcls(q).get(ReservationACL.ADMINISTER_RESERVATIONS));
    GETTERS.put(QUEUE_ORDERING_POLICY, (c, q, l, i) ->
        c.getQueueOrderingPolicy(q, null).getConfigName());
    GETTERS.put(PRIORITY, (c, q, l, i) -> c.getQueuePriority(q).getPriority());
    GETTERS.put(DEFAULT_APPLICATION_PRIORITY,
        (c, q, l, i) -> c.getDefaultApplicationPriorityConfPerQueue(q));
    GETTERS.put(MAXIMUM_ALLOCATION_MB,
        (c, q, l, i) -> c.getQueueMaximumAllocationMb(q));
    GETTERS.put(MAXIMUM_ALLOCATION_VCORES,
        (c, q, l, i) -> c.getQueueMaximumAllocationVcores(q));
    GETTERS.put(PREEMPTION_DISABLED,
        (c, q, l, i) -> c.getPreemptionDisabled(q, false));
    GETTERS.put(INTRA_QUEUE_PREEMPTION_DISABLED,
        (c, q, l, i) -> c.getIntraQueuePreemptionDisabled(q, false));
    GETTERS.put(MAXIMUM_APPLICATION_LIFETIME,
        (c, q, l, i) -> c.getMaximumLifetimePerQueue(q));
    GETTERS.put(DEFAULT_APPLICATION_LIFETIME,
        (c, q, l, i) -> c.getDefaultLifetimePerQueue(q));
    GETTERS.put(MAX_PARALLEL_APPS,
        (c, q, l, i) -> c.getMaxParallelAppsForQueue(q));
    GETTERS.put(MULTI_NODE_SORTING_POLICY,
        (c, q, l, i) -> c.getMultiNodesSortingAlgorithmPolicy(q));
    GETTERS.put(ALLOW_ZERO_CAPACITY_SUM,
        (c, q, l, i) -> c.getAllowZeroCapacitySum(q));
    GETTERS.put(AUTO_CREATE_CHILD_QUEUE_ENABLED,
        (c, q, l, i) -> c.isAutoCreateChildQueueEnabled(q));
    GETTERS.put(AUTO_CREATE_CHILD_QUEUE_MAX_QUEUES,
        (c, q, l, i) -> c.getAutoCreatedQueuesMaxChildQueuesLimit(q));
    GETTERS.put(AUTO_CREATE_CHILD_QUEUE_FAIL_ON_EXCEEDING_CAPACITY,
        (c, q, l, i) ->
            c.getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(q));
    GETTERS.put(AUTO_CREATED_QUEUE_MANAGEMENT_POLICY_NAME,
        (c, q, l, i) -> c.getAutoCreatedQueueManagementPolicy(q));
    GETTERS.put(AUTO_QUEUE_CREATION_V2_ENABLED,
        (c, q, l, i) -> c.isAutoQueueCreationV2Enabled(q));
    GETTERS.put(AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
        (c, q, l, i) -> c.getAutoCreatedQueuesV2MaxChildQueuesLimit(q));
    GETTERS.put(AUTO_QUEUE_CREATION_V2_MAX_DEPTH,
        (c, q, l, i) -> c.getMaximumAutoCreatedQueueDepth(q));
    GETTERS.put(AUTO_QUEUE_AUTO_REMOVAL_ENABLED,
        (c, q, l, i) -> c.isAutoExpiredDeletionEnabled(q));
    GETTERS.put(AQC_V2_TEMPLATE, (c, q, l, i) ->
        new AutoCreatedQueueTemplate(c, q).getTemplateProperties());
    GETTERS.put(AQC_V2_LEAF_TEMPLATE, (c, q, l, i) ->
        new AutoCreatedQueueTemplate(c, q).getLeafOnlyProperties());
    GETTERS.put(AQC_V2_PARENT_TEMPLATE, (c, q, l, i) ->
        new AutoCreatedQueueTemplate(c, q).getParentOnlyProperties());
    GETTERS.put(RESERVABLE, (c, q, l, i) -> c.isReservable(q));
    GETTERS.put(RESERVATION_WINDOW_MS,
        (c, q, l, i) -> c.getReservationWindow(q));
    GETTERS.put(AVERAGE_CAPACITY_PERCENT,
        (c, q, l, i) -> c.getAverageCapacity(q));
    GETTERS.put(INSTANTANEOUS_MAX_CAPACITY_PERCENT,
        (c, q, l, i) -> c.getInstantaneousMaxCapacity(q));
    GETTERS.put(RESERVATION_ADMISSION_POLICY_NAME,
        (c, q, l, i) -> c.getReservationAdmissionPolicy(q));
    GETTERS.put(RESERVATION_AGENT, (c, q, l, i) -> c.getReservationAgent(q));
    GETTERS.put(RESERVATION_PLANNER, (c, q, l, i) -> c.getReplanner(q));
    GETTERS.put(RESERVATION_MOVE_ON_EXPIRY_ENABLED,
        (c, q, l, i) -> c.getMoveOnExpiry(q));
    GETTERS.put(RESERVATION_ENFORCEMENT_WINDOW_MS,
        (c, q, l, i) -> c.getEnforcementWindow(q));
    GETTERS.put(SHOW_RESERVATIONS_AS_QUEUES,
        (c, q, l, i) -> c.getShowReservationAsQueues(q));
  }

  /**
   * Properties without a plain getter, and why: user weights, the maximum
   * allocation, the ordering policy parameters and the v1 template are
   * compared against live queues or their template writes instead; the
   * application ordering policy getter builds the policy object.
   */
  private static final Set<QueueProperty<?>> WITHOUT_GETTER =
      new HashSet<>(Arrays.<QueueProperty<?>>asList(USER_WEIGHTS,
          MAXIMUM_ALLOCATION, ORDERING_POLICY_PARAMETERS, LEAF_QUEUE_TEMPLATE,
          APP_ORDERING_POLICY));

  @Test
  public void testEveryPropertyIsCompared() {
    for (QueueProperty<?> property : ALL_PROPERTIES) {
      assertTrue(GETTERS.containsKey(property)
          || WITHOUT_GETTER.contains(property), property.toString());
    }
  }

  @Test
  public void testGettersMatchResolvedValues() throws Exception {
    int compared = 0;
    for (Map.Entry<String, Configuration> entry : corpus().entrySet()) {
      CapacitySchedulerConfiguration conf =
          new CapacitySchedulerConfiguration(entry.getValue(), false);
      ResolutionInputs inputs = TestQueueConfigResolver.inputs(conf,
          Collections.<DynamicQueue>emptyList());
      ResolvedQueueTree tree =
          QueueConfigResolver.resolve(conf.getConfigSnapshot(), inputs);
      ConfiguredNodeLabels labels = new ConfiguredNodeLabels(conf);
      for (ResolvedQueueConfig queue : tree.getQueues()) {
        if (queue.isDynamic()) {
          // Without a scheduler nothing writes the template entries
          continue;
        }
        compared += assertGetterParity(entry.getKey(), conf, inputs, queue);
        Set<String> expectedLabels = queue.getQueuePath().isRoot()
            ? labels.getAllConfiguredLabels()
            : labels.getLabelsByQueue(queue.getQueuePath().getFullPath());
        assertEquals(expectedLabels,
            queue.get(CONFIGURED_NODE_LABELS).getValue(), entry.getKey());
      }
    }
    assertTrue(compared > 1000, "compared " + compared + " values");
  }

  @Test
  public void testRejectedConfigurationsHoldFailedValues() throws Exception {
    for (Map.Entry<String, Configuration> entry : rejections().entrySet()) {
      String name = entry.getKey();
      CapacitySchedulerConfiguration conf =
          new CapacitySchedulerConfiguration(entry.getValue(), false);
      ResolutionInputs inputs = TestQueueConfigResolver.inputs(conf,
          Collections.<DynamicQueue>emptyList());
      ResolvedQueueTree tree =
          QueueConfigResolver.resolve(conf.getConfigSnapshot(), inputs);
      for (ResolvedQueueConfig queue : tree.getQueues()) {
        assertGetterParity(name, conf, inputs, queue);
      }
      Throwable rootCause = null;
      MockRM rm = null;
      try {
        // Queues are parsed when the RM is initialized
        rm = startRM(entry.getValue());
      } catch (Exception e) {
        rootCause = e;
        while (rootCause.getCause() != null) {
          rootCause = rootCause.getCause();
        }
      } finally {
        if (rm != null) {
          rm.stop();
        }
      }
      assertNotNull(rootCause, name + " was accepted");
      assertRejectionResolved(name, tree, rootCause);
    }
  }

  /** The existing dynamic queues of a live hierarchy. */
  private static List<DynamicQueue> dynamicQueues(CSQueue queue,
      List<DynamicQueue> dynamic) {
    if (queue instanceof AutoCreatedLeafQueue) {
      dynamic.add(new DynamicQueue(queue.getQueuePathObject(), true, true));
    } else if (!(queue instanceof ReservationQueue)
        && ((AbstractCSQueue) queue).isDynamicQueue()) {
      dynamic.add(new DynamicQueue(queue.getQueuePathObject(), false,
          queue instanceof AbstractLeafQueue));
    }
    if (queue.getChildQueues() != null) {
      for (CSQueue child : queue.getChildQueues()) {
        dynamicQueues(child, dynamic);
      }
    }
    return dynamic;
  }

  /**
   * The exception that failed queue initialization must be a resolved
   * failure, with the same type and message.
   */
  private static void assertRejectionResolved(String name,
      ResolvedQueueTree tree, Throwable rootCause) {
    List<String> failures = new ArrayList<>();
    for (ResolvedQueueConfig queue : tree.getQueues()) {
      for (Resolved<?> value : queue.explain().values()) {
        if (value.isFailed()) {
          try {
            value.getValue();
          } catch (RuntimeException e) {
            failures.add(e.getClass().getName() + ": " + e.getMessage());
          }
        }
      }
    }
    String expected = rootCause.getClass().getName() + ": "
        + rootCause.getMessage();
    if (CHECK_REJECTIONS.contains(name)) {
      assertTrue(!failures.contains(expected), name);
    } else {
      assertTrue(failures.contains(expected),
          name + ": " + expected + " not among " + failures);
    }
  }

  /**
   * Getter parity on the configuration of a running scheduler, including
   * the template writes of the dynamic queues it creates, and live parity
   * of every queue it built.
   */
  @Test
  public void testEffectiveValuesMatchLiveQueues() throws Exception {
    int values = 0;
    for (Map.Entry<String, Configuration> entry : liveCorpus().entrySet()) {
      String name = entry.getKey();
      MockRM rm = startRM(entry.getValue());
      try {
        CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
        rm.registerNode("h1:1234", 100 * GB, 100);
        for (String path : DYNAMIC_QUEUES.getOrDefault(name,
            Collections.<String>emptyList())) {
          cs.getCapacitySchedulerQueueManager().createQueue(
              new QueuePath(path));
        }
        CapacitySchedulerConfiguration conf =
            cs.getQueueContext().getConfiguration();
        ResolutionInputs inputs = TestQueueConfigResolver.inputs(conf,
            dynamicQueues(cs.getRootQueue(), new ArrayList<DynamicQueue>()));
        ResolvedQueueTree tree = QueueConfigResolver.resolve(
            cs.getQueueContext().getConfigSnapshot(), inputs);
        for (ResolvedQueueConfig queue : tree.getQueues()) {
          values += queue.isDynamic()
              ? assertDynamicParity(name, conf, inputs, queue)
              : assertGetterParity(name, conf, inputs, queue);
          assertLiveParity(name, cs, conf, tree, queue, !queue.isDynamic());
        }
      } finally {
        rm.stop();
      }
    }
    assertTrue(values > 1000, "compared " + values + " values");
  }

  @Test
  public void testDynamicQueuesMatchTemplateWrites() throws Exception {
    for (boolean legacyMode : new boolean[] {true, false}) {
      Configuration base = dynamicConfiguration();
      base.setBoolean(P + "legacy-queue-mode.enabled", legacyMode);
      MockRM rm = startRM(base);
      try {
        CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
        rm.registerNode("h1:1234", 100 * GB, 100);
        CapacitySchedulerConfiguration conf =
            cs.getQueueContext().getConfiguration();
        ConfigSnapshot snapshot = cs.getQueueContext().getConfigSnapshot();
        ResolvedQueueTree before = QueueConfigResolver.resolve(snapshot,
            TestQueueConfigResolver.inputs(conf,
                Collections.<DynamicQueue>emptyList()));

        List<DynamicQueue> dynamicQueues = new ArrayList<>();
        String[] leaves = {"root.a.d1", "root.a.dp.dl", "root.m.u1"};
        for (String leaf : leaves) {
          cs.getCapacitySchedulerQueueManager().createQueue(
              new QueuePath(leaf));
        }
        dynamicQueues.add(new DynamicQueue(new QueuePath("root.a.d1"), false,
            true));
        dynamicQueues.add(new DynamicQueue(new QueuePath("root.a.dp"), false,
            false));
        dynamicQueues.add(new DynamicQueue(new QueuePath("root.a.dp.dl"),
            false, true));
        dynamicQueues.add(new DynamicQueue(new QueuePath("root.m.u1"), true,
            true));
        ResolvedQueueTree tree = QueueConfigResolver.resolve(snapshot,
            TestQueueConfigResolver.inputs(conf, dynamicQueues));

        String name = "dynamic, legacy mode " + legacyMode;
        int templated = 0;
        for (DynamicQueue dynamic : dynamicQueues) {
          ResolvedQueueConfig queue = tree.get(dynamic.getPath());
          assertNotNull(queue, dynamic.getPath().getFullPath());
          assertTrue(queue.isDynamic());
          templated += assertDynamicParity(name, conf, tree.getInputs(),
              queue);
          assertLiveParity(name, cs, conf, tree, queue, false);
          if (before.get(dynamic.getPath().getParentObject()) != null) {
            ResolvedQueueConfig created = QueueConfigResolver
                .resolveDynamicQueue(before, dynamic.getPath(),
                    dynamic.isLeaf());
            for (QueueProperty<?> property : GETTERS.keySet()) {
              assertEquals(String.valueOf(queue.get(property)),
                  String.valueOf(created.get(property)),
                  name + " " + queue + " " + property);
            }
          }
        }
        assertTrue(templated >= 20, "templated values " + templated);
        // Static queues are unaffected by the template writes
        for (ResolvedQueueConfig queue : tree.getQueues()) {
          if (!queue.isDynamic()) {
            assertGetterParity(name, conf, tree.getInputs(), queue);
          }
        }
      } finally {
        rm.stop();
      }
    }
  }

  /**
   * A template value may reference a key trunk writes for the same dynamic
   * queue: another template entry or a dynamic leaf default. Trunk expands
   * the variable when it reads the key, after every entry is written.
   */
  @Test
  public void testTemplateVariablesReadDynamicQueueKeys() throws Exception {
    String v2 = "root.a.auto-queue-creation-v2.leaf-template.";
    for (boolean legacyMode : new boolean[] {true, false}) {
      Configuration base = conf("root.queues", "a,m",
          "root.a.capacity", "50", "root.m.capacity", "50",
          "root.a.queues", "a1", "root.a.a1.capacity", "1w",
          "root.a.auto-queue-creation-v2.enabled", "true",
          v2 + "user-limit-factor", "2",
          v2 + "maximum-applications",
          "${" + P + "root.a.d.user-limit-factor}",
          v2 + "minimum-user-limit-percent",
          "${" + P + "root.a.d.maximum-am-resource-percent}",
          "root.m.auto-create-child-queue.enabled", "true",
          "root.m.leaf-queue-template.capacity", "10",
          "root.m.leaf-queue-template.user-limit-factor", "3",
          "root.m.leaf-queue-template.maximum-applications",
          "${" + P + "root.m.u1.user-limit-factor}");
      base.setBoolean(P + "legacy-queue-mode.enabled", legacyMode);
      MockRM rm = startRM(base);
      try {
        CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
        rm.registerNode("h1:1234", 100 * GB, 100);
        CapacitySchedulerConfiguration conf =
            cs.getQueueContext().getConfiguration();
        ConfigSnapshot snapshot = cs.getQueueContext().getConfigSnapshot();
        cs.getCapacitySchedulerQueueManager().createQueue(
            new QueuePath("root.a.d"));
        cs.getCapacitySchedulerQueueManager().createQueue(
            new QueuePath("root.m.u1"));
        ResolvedQueueTree tree = QueueConfigResolver.resolve(snapshot,
            TestQueueConfigResolver.inputs(conf, Arrays.asList(
                new DynamicQueue(new QueuePath("root.a.d"), false, true),
                new DynamicQueue(new QueuePath("root.m.u1"), true, true))));

        String name = "template variables, legacy mode " + legacyMode;
        for (String path : new String[] {"root.a.d", "root.m.u1"}) {
          ResolvedQueueConfig queue = tree.get(new QueuePath(path));
          assertDynamicParity(name, conf, tree.getInputs(), queue);
          assertLiveParity(name, cs, conf, tree, queue, false);
        }
        ResolvedQueueConfig d = tree.get(new QueuePath("root.a.d"));
        assertEquals(2, (int) d.get(MAXIMUM_APPLICATIONS).getValue(), name);
        assertEquals(1f, d.get(USER_LIMIT).getValue(), name);
        assertEquals(2, ((AbstractLeafQueue) cs.getQueue("root.a.d"))
            .getMaxApplications(), name);
        assertEquals(3, (int) tree.get(new QueuePath("root.m.u1"))
            .get(MAXIMUM_APPLICATIONS).getValue(), name);
      } finally {
        rm.stop();
      }
    }
  }

  /** Records the parameters of every ordering policy it configures. */
  public static class RecordingOrderingPolicy
      extends FifoOrderingPolicy<FiCaSchedulerApp> {
    private static final List<Map<String, String>> CONFIGURED =
        Collections.synchronizedList(new ArrayList<Map<String, String>>());

    @Override
    public void configure(Map<String, String> conf) {
      CONFIGURED.add(new HashMap<>(conf));
      super.configure(conf);
    }
  }

  /**
   * A custom application ordering policy receives the keys below its
   * ordering-policy key, without the policy name itself, for static queues
   * and template entries alike.
   */
  @Test
  public void testCustomOrderingPolicyParameters() throws Exception {
    String policy = RecordingOrderingPolicy.class.getName();
    String v2 = "root.a.auto-queue-creation-v2.leaf-template.";
    Configuration base = conf("root.queues", "a,b",
        "root.a.capacity", "1w", "root.b.capacity", "1w",
        "root.b.ordering-policy", policy,
        "root.b.ordering-policy.static-param", "1",
        "root.a.auto-queue-creation-v2.enabled", "true",
        v2 + "ordering-policy", policy,
        v2 + "ordering-policy.template-param", "2");
    base.setBoolean(P + "legacy-queue-mode.enabled", false);
    RecordingOrderingPolicy.CONFIGURED.clear();
    MockRM rm = startRM(base);
    try {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      CapacitySchedulerConfiguration conf =
          cs.getQueueContext().getConfiguration();
      cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.a.d"));
      ResolvedQueueTree tree = QueueConfigResolver.resolve(
          cs.getQueueContext().getConfigSnapshot(),
          TestQueueConfigResolver.inputs(conf, Arrays.asList(
              new DynamicQueue(new QueuePath("root.a.d"), false, true))));

      Map<String, String> configured = Collections.singletonMap(
          "static-param", "1");
      Map<String, String> templated = Collections.singletonMap(
          "template-param", "2");
      assertEquals(configured, tree.get(new QueuePath("root.b"))
          .get(ORDERING_POLICY_PARAMETERS).getValue());
      assertEquals(templated, tree.get(new QueuePath("root.a.d"))
          .get(ORDERING_POLICY_PARAMETERS).getValue());
      List<Map<String, String>> recorded =
          new ArrayList<>(RecordingOrderingPolicy.CONFIGURED);
      assertTrue(recorded.contains(configured), recorded.toString());
      assertTrue(recorded.contains(templated), recorded.toString());
      for (Map<String, String> parameters : recorded) {
        assertFalse(parameters.containsKey(""), recorded.toString());
      }
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testResolutionTime() {
    // 50 parents with 100 leaves each
    Configuration conf = new Configuration(false);
    StringBuilder parents = new StringBuilder();
    for (int p = 0; p < 50; p++) {
      parents.append(p == 0 ? "" : ",").append("p").append(p);
      StringBuilder leaves = new StringBuilder();
      for (int l = 0; l < 100; l++) {
        leaves.append(l == 0 ? "" : ",").append("l").append(l);
        String leaf = P + "root.p" + p + ".l" + l + ".";
        conf.set(leaf + "capacity", "1");
        conf.set(leaf + "user-limit-factor", "2");
        conf.set(leaf + "acl_submit_applications", "user" + l);
      }
      conf.set(P + "root.p" + p + ".queues", leaves.toString());
      conf.set(P + "root.p" + p + ".capacity", "2");
    }
    conf.set(P + "root.queues", parents.toString());
    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);
    ResolutionInputs inputs = TestQueueConfigResolver.inputs(conf,
        Collections.<DynamicQueue>emptyList());

    long best = Long.MAX_VALUE;
    ResolvedQueueTree tree = null;
    for (int i = 0; i < 10; i++) {
      long start = System.nanoTime();
      tree = QueueConfigResolver.resolve(snapshot, inputs);
      best = Math.min(best, System.nanoTime() - start);
    }
    assertEquals(5051, tree.getQueues().size());
    System.out.println("Resolved " + tree.getQueues().size()
        + " queues in " + best / 1000000 + " ms (best of 10)");
  }

  private static int assertGetterParity(String name,
      CapacitySchedulerConfiguration conf, ResolutionInputs inputs,
      ResolvedQueueConfig queue) {
    int compared = 0;
    for (Map.Entry<QueueProperty<?>, Getter> getter : GETTERS.entrySet()) {
      for (String label : queue.getConfiguredNodeLabels()) {
        if (assertGetter(name, conf, inputs, queue, getter.getKey(),
            getter.getValue(), label)) {
          compared++;
        }
        if (!getter.getKey().isLabeled()) {
          break;
        }
      }
    }
    return compared;
  }

  /**
   * For a dynamic queue, the getters of the live configuration see the
   * template writes; only properties templates can set are compared.
   */
  private static int assertDynamicParity(String name,
      CapacitySchedulerConfiguration conf, ResolutionInputs inputs,
      ResolvedQueueConfig queue) {
    int templated = 0;
    for (Map.Entry<QueueProperty<?>, Getter> getter : GETTERS.entrySet()) {
      if (NOT_TEMPLATED.contains(getter.getKey())
          || getter.getKey().getName().startsWith("acl_")) {
        continue;
      }
      for (String label : queue.getConfiguredNodeLabels()) {
        assertGetter(name, conf, inputs, queue, getter.getKey(),
            getter.getValue(), label);
        Resolved<?> resolved = queue.get(getter.getKey(), label);
        if (resolved != null && (resolved.getSource() == ValueSource.TEMPLATE_V1
            || resolved.getSource() == ValueSource.TEMPLATE_V2)) {
          templated++;
        }
        if (!getter.getKey().isLabeled()) {
          break;
        }
      }
    }
    return templated;
  }

  private static boolean assertGetter(String name,
      CapacitySchedulerConfiguration conf, ResolutionInputs inputs,
      ResolvedQueueConfig queue, QueueProperty<?> property, Getter getter,
      String label) {
    Resolved<?> resolved = queue.get(property, label);
    if (resolved == null || !comparable(property, resolved)) {
      return false;
    }
    String message = name + ": " + queue + " " + property + " [" + label
        + "] " + resolved;
    Object expected;
    try {
      expected = getter.get(conf, queue.getQueuePath(), label, inputs);
    } catch (RuntimeException e) {
      assertTrue(resolved.isFailed(), message + " getter threw " + e);
      try {
        resolved.getValue();
      } catch (RuntimeException failure) {
        assertEquals(e.getClass(), failure.getClass(), message);
        assertEquals(e.getMessage(), failure.getMessage(), message);
      }
      return true;
    }
    if (resolved.isFailed()) {
      fail(message + " getter returned " + expected);
    }
    Object value = resolved.getValue();
    if (expected instanceof AccessControlList) {
      value = new AccessControlList((String) value);
    }
    assertEquals(normalize(expected), normalize(value), message);
    return true;
  }

  /**
   * Values a getter cannot return: inherited and derived values, and the
   * defaults of properties that inherit, which the getters leave unset.
   */
  private static boolean comparable(QueueProperty<?> property,
      Resolved<?> resolved) {
    ValueSource source = resolved.getSource();
    if (source == ValueSource.PARENT || source == ValueSource.DERIVED
        || "dynamic queue default".equals(resolved.getSourceDetail())) {
      return false;
    }
    return !INHERITED.contains(property)
        || source == ValueSource.QUEUE || source == ValueSource.TEMPLATE_V1
        || source == ValueSource.TEMPLATE_V2;
  }

  private static Object normalize(Object value) {
    if (value instanceof QueueCapacityVector) {
      return value.toString();
    }
    if (value instanceof AccessControlList) {
      return ((AccessControlList) value).getAclString();
    }
    if (value instanceof List && !((List<?>) value).isEmpty()
        && ((List<?>) value).get(0) instanceof AppPriorityACLGroup) {
      // AppPriorityACLGroup.equals compares the ACL objects by identity
      List<String> groups = new ArrayList<>();
      for (Object item : (List<?>) value) {
        AppPriorityACLGroup group = (AppPriorityACLGroup) item;
        groups.add(group.getMaxPriority() + "/" + group.getDefaultPriority()
            + "/" + group.getACLList().getAclString());
      }
      return groups;
    }
    return value;
  }

  /** Inherited and derived values against the live queue objects. */
  private static void assertLiveParity(String name, CapacityScheduler cs,
      CapacitySchedulerConfiguration conf, ResolvedQueueTree tree,
      ResolvedQueueConfig queue, boolean derived) {
    QueuePath path = queue.getQueuePath();
    CSQueue live = cs.getQueue(path.getFullPath());
    assertNotNull(live, name + " " + path);
    String message = name + ": " + queue;
    assertEquals(live.getAccessibleNodeLabels(),
        queue.get(ACCESSIBLE_NODE_LABELS).getValue(), message);
    assertEquals(live.getDefaultNodeLabelExpression(),
        queue.get(DEFAULT_NODE_LABEL_EXPRESSION).getValue(), message);
    assertEquals(live.getPreemptionDisabled(),
        queue.get(PREEMPTION_DISABLED).getValue(), message);
    assertEquals(live.getIntraQueuePreemptionDisabledInHierarchy(),
        queue.get(INTRA_QUEUE_PREEMPTION_DISABLED).getValue(), message);
    assertEquals(live.getMaximumApplicationLifetime(),
        (long) queue.get(MAXIMUM_APPLICATION_LIFETIME).getValue(), message);
    assertEquals(live.getDefaultApplicationLifetime(),
        (long) queue.get(DEFAULT_APPLICATION_LIFETIME).getValue(), message);
    assertEquals(live.getMaximumAllocation(),
        queue.get(MAXIMUM_ALLOCATION).getValue(), message);
    assertEquals(live.getState(), queue.get(STATE).getValue(), message);
    assertEquals(live.getPriority().getPriority(),
        (int) queue.get(PRIORITY).getValue(), message);
    assertEquals(live.getConfiguredNodeLabels(),
        queue.get(CONFIGURED_NODE_LABELS).getValue(), message);
    for (Map.Entry<String, Float> weight
        : queue.get(USER_WEIGHTS).getValue().entrySet()) {
      assertEquals(weight.getValue(),
          live.getUserWeights().getByUser(weight.getKey()), message);
    }
    AbstractCSQueue csQueue = (AbstractCSQueue) live;
    assertEquals(csQueue.getMaxParallelApps(),
        (int) queue.get(MAX_PARALLEL_APPS).getValue(), message);
    assertEquals(csQueue.getACLs().get(AccessType.SUBMIT_APP).getAclString(),
        new AccessControlList(queue.get(ACL_SUBMIT_APPLICATIONS).getValue())
            .getAclString(), message);
    assertEquals(csQueue.getACLs().get(AccessType.ADMINISTER_QUEUE)
        .getAclString(), new AccessControlList(
            queue.get(ACL_ADMINISTER_QUEUE).getValue()).getAclString(),
        message);
    if (queue.getKind() == Kind.RESERVATION) {
      AbstractLeafQueue reservation = (AbstractLeafQueue) live;
      assertEquals(reservation.getUserLimit(),
          queue.get(USER_LIMIT).getValue(), message);
      assertEquals(reservation.getUserLimitFactor(),
          queue.get(USER_LIMIT_FACTOR).getValue(), message);
      assertEquals(reservation.getMaxApplications(),
          (int) queue.get(EFFECTIVE_MAXIMUM_APPLICATIONS).getValue(), message);
      assertEquals(reservation.getMaxApplicationsPerUser(),
          (int) queue.get(MAXIMUM_APPLICATIONS_PER_USER).getValue(), message);
      assertEquals(reservation.getMaxAMResourcePerQueuePercent(),
          queue.get(MAXIMUM_AM_RESOURCE_PERCENT).getValue(), message);
    }
    if (live instanceof AbstractParentQueue) {
      assertEquals(((AbstractParentQueue) live).getQueueOrderingPolicy()
          .getConfigName(), queue.get(QUEUE_ORDERING_POLICY).getValue(),
          message);
    }
    if (!derived || !conf.isLegacyQueueMode() || hasAbsoluteAncestor(tree,
        queue)) {
      return;
    }
    for (String label : queue.getConfiguredNodeLabels()) {
      assertEquals(live.getQueueCapacities().getAbsoluteCapacity(label),
          queue.get(ABSOLUTE_CAPACITY, label).getValue(), 1e-6,
          message + " " + label);
      assertEquals(live.getQueueCapacities().getAbsoluteMaximumCapacity(label),
          queue.get(ABSOLUTE_MAXIMUM_CAPACITY, label).getValue(), 1e-6,
          message + " " + label);
    }
    if (live instanceof AbstractLeafQueue && queue.getKind() == Kind.LEAF) {
      AbstractLeafQueue leaf = (AbstractLeafQueue) live;
      assertEquals(leaf.getMaxApplications(),
          (int) queue.get(EFFECTIVE_MAXIMUM_APPLICATIONS).getValue(), message);
      assertEquals(leaf.getMaxApplicationsPerUser(),
          (int) queue.get(MAXIMUM_APPLICATIONS_PER_USER).getValue(), message);
    }
  }

  private static boolean hasAbsoluteAncestor(ResolvedQueueTree tree,
      ResolvedQueueConfig queue) {
    for (QueuePath path = queue.getQueuePath(); path != null;
        path = path.isRoot() ? null : path.getParentObject()) {
      ResolvedQueueConfig config = tree.get(path);
      for (String label : config.getConfiguredNodeLabels()) {
        if (config.get(CAPACITY_IS_ABSOLUTE_RESOURCE, label).getValue()) {
          return true;
        }
      }
    }
    return false;
  }

  private static MockRM startRM(Configuration base) throws Exception {
    return startRM(base, base);
  }

  /**
   * Starts a MockRM on a configuration whose cluster has the node labels
   * another configuration names.
   */
  static MockRM startRM(Configuration base, Configuration labelsOf)
      throws Exception {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(base, false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    // Read from the keys, so that a malformed label key fails the queue
    // initialization rather than this method
    Set<String> labels = new HashSet<>();
    String marker = ".accessible-node-labels";
    for (Map.Entry<String, String> e : labelsOf) {
      int at = e.getKey().indexOf(marker);
      if (!e.getKey().startsWith(P) || at < 0) {
        continue;
      }
      String rest = e.getKey().substring(at + marker.length());
      if (rest.isEmpty()) {
        for (String label : e.getValue().split(",")) {
          labels.add(label.trim());
        }
      } else if (rest.startsWith(".")) {
        labels.add(rest.substring(1).split("\\.")[0]);
      }
    }
    labels.remove(NO_LABEL);
    labels.remove(RMNodeLabelsManager.ANY);
    if (!labels.isEmpty()) {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(labels);
    }
    MockRM rm = new MockRM(conf) {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.start();
    return rm;
  }

  /** Configurations trunk accepts or rejects at startup. */
  static Map<String, Configuration> corpus() throws Exception {
    Map<String, Configuration> corpus = liveCorpus();
    corpus.put("quirks", quirksConfiguration());
    corpus.put("invalid values", invalidConfiguration());
    corpus.put("queue labels", TestUtils.getConfigurationWithQueueLabels(
        new Configuration(false)));
    corpus.put("complex queue labels",
        TestUtils.getComplexConfigurationWithQueueLabels(
            new Configuration(false)));
    corpus.put("default queue labels",
        TestUtils.getConfigurationWithDefaultQueueLabels(
            new Configuration(false)));
    corpus.put("dynamic", dynamicConfiguration());
    return corpus;
  }

  /** The dynamic queues created in a running scheduler, by configuration. */
  private static final Map<String, List<String>> DYNAMIC_QUEUES =
      Collections.singletonMap("managed parent",
          Arrays.asList("root.m.user1", "root.m.user2"));

  /** Configurations that a MockRM can start with. */
  private static Map<String, Configuration> liveCorpus() {
    Map<String, Configuration> corpus = new LinkedHashMap<>();
    corpus.put("queue helpers",
        CapacitySchedulerQueueHelpers.setupQueueConfiguration(
            new CapacitySchedulerConfiguration(new Configuration(false),
                false)));
    corpus.put("inheritance", inheritanceConfiguration());
    corpus.put("weights", weightConfiguration(true));
    corpus.put("weights, non-legacy", weightConfiguration(false));
    corpus.put("absolute", absoluteConfiguration());
    corpus.put("node labels", labelConfiguration());
    corpus.put("mixed modes, non-legacy", mixedModeConfiguration());
    corpus.put("reservation plan", planConfiguration());
    corpus.put("managed parent", managedParentConfiguration());
    corpus.put("user weights", userWeightConfiguration());
    corpus.put("ordering, priority and lifetime", orderingConfiguration());
    return corpus;
  }

  /**
   * Configurations trunk rejects when it initializes the queues, each with
   * one invalid value.
   */
  static Map<String, Configuration> rejections() {
    Map<String, Configuration> rejected = new LinkedHashMap<>();
    rejected.put("percent suffix capacity", conf("root.queues", "a,b",
        "root.a.capacity", "50%", "root.b.capacity", "50"));
    rejected.put("uppercase weight suffix", conf("root.queues", "a,b",
        "root.a.capacity", "1W", "root.b.capacity", "1w"));
    rejected.put("weight maximum capacity", conf("root.queues", "a",
        "root.a.capacity", "1w", "root.a.maximum-capacity", "2w"));
    rejected.put("untrimmed max parallel apps", conf("root.queues", "a",
        "root.a.capacity", "100", "root.a.max-parallel-apps", " 5"));
    rejected.put("label key without property", conf("root.queues", "a",
        "root.a.capacity", "100", "root.a.accessible-node-labels.x", "50"));
    rejected.put("labeled maximum capacity -1", conf("root.queues", "a",
        "root.accessible-node-labels", "x", "root.a.capacity", "100",
        "root.a.accessible-node-labels", "x",
        "root.a.accessible-node-labels.x.capacity", "100",
        "root.a.accessible-node-labels.x.maximum-capacity", "-1"));
    rejected.put("absolute with weight units", conf("root.queues", "a,c",
        "root.a.capacity", "[memory=40960,vcores=40]",
        "root.c.capacity", "[memory=1w,vcores=1w]"));
    rejected.put("absolute before percentage", conf("root.queues", "a,b",
        "root.a.capacity", "[memory=1024,vcores=1]", "root.b.capacity", "50"));
    return rejected;
  }

  private static Configuration conf(String... keyValues) {
    Configuration conf = new Configuration(false);
    for (int i = 0; i < keyValues.length; i += 2) {
      conf.set(keyValues[i].startsWith("yarn.") ? keyValues[i]
          : P + keyValues[i], keyValues[i + 1]);
    }
    return conf;
  }

  private static Configuration inheritanceConfiguration() {
    return conf("root.queues", "a,b,c",
        "root.a.capacity", "50", "root.b.capacity", "30",
        "root.c.capacity", "20", "root.maximum-capacity", "80",
        "root.a.queues", "a1,a2", "root.a.a1.capacity", "40",
        "root.a.a2.capacity", "60",
        "root.a.maximum-application-lifetime", "1000",
        "root.a.default-application-lifetime", "100",
        "root.a.a1.maximum-application-lifetime", "50",
        "root.a.a2.default-application-lifetime", "-1",
        "root.b.maximum-application-lifetime", "300",
        "root.b.default-application-lifetime", "0",
        "yarn.resourcemanager.scheduler.monitor.enable", "true",
        "yarn.resourcemanager.monitor.capacity.preemption"
            + ".intra-queue-preemption.enabled", "true",
        "root.a.disable_preemption", "true",
        "root.a.a2.disable_preemption", "false",
        "root.b.intra-queue-preemption.disable_preemption", "true",
        "root.a.ordering-policy", "priority-utilization",
        "root.c.state", "STOPPED",
        "root.a.maximum-allocation-mb", "4096",
        "root.a.a1.maximum-allocation-vcores", "2",
        "root.b.maximum-allocation", "memory-mb=2048,vcores=3",
        "root.a.user-settings.alice.weight", "0.5",
        "root.a.a1.user-settings.alice.weight", "0.8",
        "root.a.a1.user-settings.bob.weight", "${yarn.test.weight}",
        "yarn.test.weight", "0.6",
        "root.acl_submit_applications", "admin",
        "root.a.acl_submit_applications", "alice,bob ops",
        "root.a.a1.acl_administer_queue", "*",
        "root.a.a1.priority", "3", "root.b.max-parallel-apps", "4",
        "max-parallel-apps", "10",
        "root.a.a2.maximum-applications", "77",
        "global-queue-max-application", "-1",
        "root.b.minimum-user-limit-percent", "25",
        "root.b.user-limit-factor", "3",
        "root.a.a1.user-limit-factor", "-1");
  }

  private static Configuration weightConfiguration(boolean legacyMode) {
    Configuration conf = conf("root.queues", "a,b",
        "root.a.capacity", "1w", "root.b.capacity", "3w",
        "root.a.queues", "a1,a2,a3", "root.a.a1.capacity", "2w",
        "root.a.a2.capacity", "2w", "root.a.a3.capacity", "4w",
        "root.b.maximum-capacity", "50", "maximum-applications", "5000",
        "root.a.a3.user-limit-factor", "0.5");
    conf.setBoolean(P + "legacy-queue-mode.enabled", legacyMode);
    return conf;
  }

  private static Configuration absoluteConfiguration() {
    return conf("root.queues", "a,b",
        "root.a.capacity", "[memory=20480,vcores=20]",
        "root.a.maximum-capacity", "[memory=40960,vcores=40]",
        "root.b.capacity", "[memory=10240,vcores=10]",
        "root.a.queues", "a1", "root.a.a1.capacity", "[memory=10240,vcores=5]",
        "global-queue-max-application", "200");
  }

  /**
   * Labeled capacities, label inheritance, default label expressions, a
   * wildcard and a blank label list.
   */
  private static Configuration labelConfiguration() {
    String a = "root.a.accessible-node-labels";
    return conf("root.queues", "a,b,c", "root.accessible-node-labels", "x",
        "root.a.capacity", "50", a, "x,y", a + ".x.capacity", "60",
        a + ".x.maximum-capacity", "80", a + ".y.capacity", "100",
        a + ".x.maximum-am-resource-percent", "0.3",
        "root.a.default-node-label-expression", "x",
        "root.a.queues", "a1,a2", "root.a.a1.capacity", "50",
        "root.a.a1.accessible-node-labels.x.capacity", "100",
        "root.a.a1.accessible-node-labels.y.capacity", "50",
        "root.a.a2.capacity", "50", "root.a.a2.accessible-node-labels", "y",
        "root.a.a2.accessible-node-labels.y.capacity", "50",
        "root.a.a2.default-node-label-expression", "y",
        "root.b.capacity", "30", "root.b.accessible-node-labels", "*",
        "root.b.accessible-node-labels.x.capacity", "40",
        "root.c.capacity", "20", "root.c.accessible-node-labels", " ");
  }

  /** Percentage, weight and absolute siblings and mixed vectors. */
  private static Configuration mixedModeConfiguration() {
    Configuration conf = conf("root.queues", "a,b,c,d",
        "root.a.capacity", "[memory=20480,vcores=10]",
        "root.a.maximum-capacity", "[memory=30720,vcores=200]",
        "root.b.capacity", "40", "root.c.capacity", "2w",
        "root.d.capacity", "[memory=50%,vcores=1w]",
        "root.c.queues", "c1,c2,c3",
        "root.c.c1.capacity", "[memory=81920,vcores=90]",
        "root.c.c2.capacity", "10", "root.c.c3.capacity", "1w",
        "root.c.c3.maximum-capacity", "[memory=0,vcores=0]");
    conf.setBoolean(P + "legacy-queue-mode.enabled", false);
    return conf;
  }

  /** A reservable plan queue with every reservation setting. */
  private static Configuration planConfiguration() {
    String plan = "root.plan.";
    String reservation = "org.apache.hadoop.yarn.server.resourcemanager"
        + ".reservation.";
    return conf("maximum-applications", "4000", "root.queues", "plan,b",
        plan + "capacity", "40", plan + "reservable", "true",
        plan + "reservation-window", "7200000",
        plan + "average-capacity", "50",
        plan + "instantaneous-max-capacity", "80",
        plan + "reservation-policy", reservation + "CapacityOverTimePolicy",
        plan + "reservation-agent",
        reservation + "planning.AlignedPlannerWithGreedy",
        plan + "reservation-planner",
        reservation + "planning.SimpleCapacityReplanner",
        plan + "reservation-move-on-expiry", "false",
        plan + "reservation-enforcement-window", "60000",
        plan + "show-reservations-as-queues", "true",
        plan + "acl_submit_reservations", "planuser",
        plan + "acl_list_reservations", "planlister",
        plan + "acl_administer_reservations", "",
        plan + "minimum-user-limit-percent", "40",
        plan + "user-limit-factor", "3",
        plan + "leaf-queue-template.acl_submit_applications", "resuser",
        "root.b.capacity", "60", "root.b.reservable", "false");
  }

  /**
   * An AQC v1 managed parent with labeled template capacities and explicit
   * keys on the paths of its dynamic leaves.
   */
  private static Configuration managedParentConfiguration() {
    String template = "root.m.leaf-queue-template.";
    return conf("root.queues", "m,b", "root.accessible-node-labels", "x",
        "root.m.capacity", "50", "root.m.accessible-node-labels", "x",
        "root.m.accessible-node-labels.x.capacity", "100",
        "root.m.auto-create-child-queue.enabled", "true",
        "root.m.auto-create-child-queue.max-queues", "3",
        "root.m.auto-create-child-queue.fail-on-exceeding-parent-capacity",
        "true",
        template + "capacity", "20", template + "maximum-capacity", "60",
        template + "user-limit-factor", "2",
        template + "maximum-applications", "13",
        template + "acl_submit_applications", "v1user",
        template + "accessible-node-labels", "x",
        template + "accessible-node-labels.x.capacity", "20",
        template + "accessible-node-labels.x.maximum-capacity", "50",
        "root.m.user1.user-limit-factor", "7", "root.m.user1.priority", "4",
        "root.b.capacity", "50");
  }

  /** User weights on several levels, with an extra key segment. */
  private static Configuration userWeightConfiguration() {
    return conf("root.queues", "a,b",
        "root.acl_administer_queue", "admin",
        "root.user-settings.alice.weight", "1",
        "root.user-settings.bob.weight", "0.5",
        "root.a.capacity", "50", "root.a.queues", "a1",
        "root.a.user-settings.alice.weight", "1.5",
        "root.a.a1.capacity", "100",
        "root.a.a1.minimum-user-limit-percent", "50",
        "root.a.a1.user-settings.carol.weight", "1",
        "root.a.a1.user-settings.dave.weight.x", "1.25",
        "root.b.capacity", "50", "root.b.acl_administer_queue", "",
        "root.b.acl_application_max_priority", "*");
  }

  /**
   * Ordering policies and their parameters, priorities, priority ACLs,
   * lifetimes and preemption flags.
   */
  private static Configuration orderingConfiguration() {
    return conf("yarn.resourcemanager.scheduler.monitor.enable", "true",
        "yarn.resourcemanager.monitor.capacity.preemption"
            + ".intra-queue-preemption.enabled", "true",
        "yarn.cluster.max-application-priority", "5",
        "root.queues", "a,b", "root.ordering-policy", "priority-utilization",
        "root.default-application-lifetime", "50",
        "root.maximum-application-lifetime", "1000",
        "root.a.capacity", "60", "root.a.priority", "3",
        "root.a.queues", "a1,a2", "root.a.disable_preemption", "true",
        "root.a.a1.capacity", "50", "root.a.a1.ordering-policy", "fair",
        "root.a.a1.ordering-policy.fair.enable-size-based-weight", "TRUE",
        "root.a.a1.default-application-priority", "4",
        "root.a.a1.priority", "1",
        "root.a.a1.acl_application_max_priority",
        "[user=alice max_priority=9 default_priority=-2]"
            + " [group=g1 max_priority=2 default_priority=1]",
        "root.a.a1.maximum-application-lifetime", "100",
        "root.a.a1.default-application-lifetime", "60",
        "root.a.a2.capacity", "50",
        "root.a.a2.ordering-policy", "fifo-with-partitions",
        "root.a.a2.ordering-policy.exclusive-enforced-partitions", " x , ,y",
        "root.a.a2.disable_preemption", "false",
        "root.a.a2.intra-queue-preemption.disable_preemption", "true",
        "root.b.capacity", "40", "root.b.ordering-policy",
        "fifo-for-pending-apps",
        "root.b.default-application-lifetime", "0",
        "root.b.maximum-application-lifetime", "0",
        "root.b.disable_preemption", "yes");
  }

  /** Values that exercise the parse quirks, all of them accepted by trunk. */
  private static Configuration quirksConfiguration() {
    Configuration conf = conf("root.queues", " a , b,c,,p",
        "root.a.capacity", "50f", "root.b.capacity", "${yarn.test.cap}",
        "yarn.test.cap", "[memory=1024,vcores=1]",
        "root.c.capacity", "[memory=1w,vcores=1w]",
        "root.p.capacity", "1w", "root.p.reservable", "true",
        "root.p.reservation-window", "0x10", "root.p.average-capacity", "20",
        "root.p.show-reservations-as-queues", "TRUE",
        "root.a.maximum-capacity", "-1", "root.b.maximum-capacity", "150",
        "root.c.maximum-capacity", "[memory=1024,vcores=1]",
        "root.a.accessible-node-labels", " x, ,*",
        "root.a.accessible-node-labels.x.capacity", "30",
        "root.a.accessible-node-labels.x.maximum-capacity", "[memory=2048]",
        "root.a.accessible-node-labels.x.maximum-am-resource-percent", "0.6",
        "root.b.accessible-node-labels", "",
        "root.a.default-node-label-expression", " x ",
        "root.a.state", "running", "root.a.max-parallel-apps", "7",
        "root.a.ordering-policy", " fair ",
        "root.a.ordering-policy.fair.enable-size-based-weight", "true",
        "root.a.acl_application_max_priority",
        "[user=alice max_priority=5 default_priority=2]",
        "root.a.disable_preemption", "maybe",
        "root.a.multi-node-sorting.policy", "res",
        "multi-node-sorting.policy.res.class", "org.apache.hadoop.yarn"
            + ".server.resourcemanager.scheduler.placement"
            + ".ResourceUsageMultiNodeLookupPolicy",
        "root.a.maximum-applications", "0x20",
        "root.a.auto-queue-creation-v2.maximum-queue-depth", "3",
        "auto-queue-creation-v2.maximum-queue-depth", "1",
        "root.auto-queue-creation-v2.enabled", "true",
        "root.auto-queue-creation-v2.template.capacity", "1w",
        "root.*.auto-queue-creation-v2.leaf-template.priority", "2",
        "root.a.auto-queue-creation-v2.parent-template.ordering-policy",
        "fair",
        "root.c.auto-create-child-queue.enabled", "true",
        "root.c.auto-create-child-queue.max-queues", "3",
        "root.c.leaf-queue-template.capacity", "5",
        "yarn.cluster.max-application-priority", "3");
    return conf;
  }

  /** Values each getter rejects, so resolution must keep the failures. */
  private static Configuration invalidConfiguration() {
    return conf("root.queues", "a,b,c,d",
        "root.a.capacity", "abc", "root.b.capacity", "20000w",
        "root.c.capacity", "50%", "root.d.capacity", "120",
        "root.a.maximum-capacity", "2w",
        "root.a.accessible-node-labels.x.capacity", "-1",
        "root.b.accessible-node-labels.x.maximum-capacity", "abc",
        "root.a.user-limit-factor", "", "root.b.minimum-user-limit-percent",
        "x", "root.a.state", "BOGUS", "root.b.max-parallel-apps", " 3",
        "root.c.maximum-allocation", "memory-mb=1x",
        "root.d.maximum-allocation-mb", "big",
        "root.a.priority", "high", "root.b.maximum-application-lifetime", "x",
        "root.c.multi-node-sorting.policy", "missing",
        "root.d.acl_application_max_priority", "[user=a max_priority=x]",
        "root.a.maximum-am-resource-percent", "x",
        "root.b.auto-queue-creation-v2.maximum-queue-depth", "deep",
        "root.d.user-settings.bob.weight", "heavy");
  }

  /**
   * v2 dynamic queues under root.a, with templates on exact and wildcard
   * paths and explicit keys on dynamic paths, and a v1 managed parent.
   */
  static Configuration dynamicConfiguration() {
    String v2 = "root.a.auto-queue-creation-v2.";
    return conf("root.queues", "a,b,m",
        "root.a.capacity", "50", "root.b.capacity", "25",
        "root.m.capacity", "25",
        "root.a.queues", "a1", "root.a.a1.capacity", "1w",
        "root.a.auto-queue-creation-v2.enabled", "true",
        v2 + "template.maximum-applications", "111",
        v2 + "template.user-limit-factor", "3",
        v2 + "template.capacity", "2w",
        v2 + "template.acl_administer_queue", "admin",
        v2 + "template.maximum-application-lifetime", "${yarn.test.lifetime}",
        "yarn.test.lifetime", "500",
        v2 + "template.priority", "2",
        v2 + "leaf-template.ordering-policy", "fair",
        v2 + "leaf-template.ordering-policy.fair.enable-size-based-weight",
        "true",
        v2 + "leaf-template.minimum-user-limit-percent", "50",
        v2 + "parent-template.maximum-am-resource-percent", "0.4",
        v2 + "parent-template.acl_submit_applications", "alice",
        v2 + "parent-template.allow-zero-capacity-sum", "true",
        "root.*.auto-queue-creation-v2.leaf-template.maximum-applications",
        "222",
        "root.a.*.auto-queue-creation-v2.template.priority", "4",
        "root.a.*.auto-queue-creation-v2.leaf-template.default-application"
            + "-priority", "3",
        "root.a.d1.user-limit-factor", "7",
        "root.a.d1.maximum-am-resource-percent", "0.3",
        "root.a.d1.default-application-priority", "5",
        "root.a.d1.user-settings.carol.weight", "0.5",
        "root.a.user-settings.alice.weight", "0.25",
        "root.m.auto-create-child-queue.enabled", "true",
        "root.m.leaf-queue-template.capacity", "10",
        "root.m.leaf-queue-template.maximum-capacity", "50",
        "root.m.leaf-queue-template.user-limit-factor", "2",
        "root.m.leaf-queue-template.acl_submit_applications", "bob",
        "root.m.leaf-queue-template.ordering-policy", "fair",
        "root.m.u1.user-limit-factor", "9",
        "root.m.u1.priority", "6");
  }
}
