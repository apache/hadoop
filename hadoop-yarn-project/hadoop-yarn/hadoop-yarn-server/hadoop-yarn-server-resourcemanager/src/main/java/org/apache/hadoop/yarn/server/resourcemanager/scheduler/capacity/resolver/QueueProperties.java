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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLConfigurationParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLGroup;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.QueueCapacityConfigParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.*;

/**
 * One {@link QueueProperty} per per-queue configuration key of the Capacity
 * Scheduler, and the parse rules they share with the
 * {@code CapacitySchedulerConfiguration} getters.
 * <p>
 * A property that takes an argument documents it; {@code null} selects the
 * default the queues use. Readers mirror the getters exactly, including
 * which fallback keys are read before the queue key, so that the getters can
 * delegate to them without changing values, exception types or messages.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueProperties {
  /**
   * The readers log under the loggers of the code they replace, so log
   * output and log based tests stay the same.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerConfiguration.class);
  private static final Logger CONF_LOG =
      LoggerFactory.getLogger(Configuration.class);

  private static final String WEIGHT_SUFFIX = "w";
  private static final QueueCapacityConfigParser CAPACITY_PARSER =
      CapacitySchedulerConfiguration.getQueueCapacityConfigParser();
  private static final AppPriorityACLConfigurationParser PRIORITY_ACL_PARSER =
      new AppPriorityACLConfigurationParser();
  /** The resource types the queues pass to the absolute resource parser. */
  public static final Set<String> QUEUE_RESOURCE_TYPES =
      ImmutableSet.of("memory", "vcores");
  /** Key of the scheduler-wide maximum-queue-depth fallback. */
  public static final String GLOBAL_MAXIMUM_QUEUE_DEPTH =
      PREFIX + MAXIMUM_QUEUE_DEPTH;
  /** Key of the scheduler-wide multi-node sorting policy fallback. */
  public static final String GLOBAL_MULTI_NODE_SORTING_POLICY =
      MULTI_NODE_SORTING_POLICY_NAME;

  /** The kinds of configured queues. */
  private static final Set<Kind> ALL =
      EnumSet.of(Kind.ROOT, Kind.PARENT, Kind.LEAF, Kind.PLAN);
  /**
   * Every kind, ReservationQueue included; a ReservationQueue sets up every
   * value except its capacities, which come from its entitlement.
   */
  private static final Set<Kind> SET_UP = EnumSet.allOf(Kind.class);
  private static final Set<Kind> PARENTS = EnumSet.of(Kind.ROOT, Kind.PARENT);
  /** Queues set up as parent queue objects, PlanQueue included. */
  private static final Set<Kind> PARENT_OBJECTS =
      EnumSet.of(Kind.ROOT, Kind.PARENT, Kind.PLAN);
  private static final Set<Kind> LEAVES =
      EnumSet.of(Kind.LEAF, Kind.PLAN, Kind.RESERVATION);
  /** Queues set up as leaf queue objects. */
  private static final Set<Kind> LEAF =
      EnumSet.of(Kind.LEAF, Kind.RESERVATION);
  private static final Set<Kind> PLANS = EnumSet.of(Kind.PLAN);

  private QueueProperties() {
  }

  // P01
  public static final QueueProperty<List<String>> QUEUES = property(
      CapacitySchedulerConfiguration.QUEUES, false, ALL,
      (conf, key, queue, label, arg) -> {
        String[] queues = StringUtils.getStrings(conf.apply(key));
        List<String> trimmedQueueNames = new ArrayList<>();
        if (queues != null) {
          for (String s : queues) {
            trimmedQueueNames.add(s.trim());
          }
        }
        return trimmedQueueNames;
      });

  // P02, percentage reading
  public static final QueueProperty<Float> CAPACITY = property(
      CapacitySchedulerConfiguration.CAPACITY, false, ALL,
      (conf, key, queue, label, arg) -> {
        String configuredCapacity = conf.apply(key);
        if (isAbsoluteResource(configuredCapacity)
            || isWeight(configuredCapacity)
            || CAPACITY_PARSER.isCapacityVectorFormat(configuredCapacity)) {
          // Absolute resources, weights and vectors are parsed separately
          return queue.isRoot() ? 100.0f : 0f;
        }
        float capacity = queue.isRoot() ? 100.0f
            : (configuredCapacity == null) ? 0f
            : Float.parseFloat(configuredCapacity);
        if (capacity < MINIMUM_CAPACITY_VALUE
            || capacity > MAXIMUM_CAPACITY_VALUE) {
          throw new IllegalArgumentException("Illegal " + "capacity of "
              + capacity + " for queue " + queue.getFullPath());
        }
        return capacity;
      });

  // P02 and P04, weight reading; -1 means not set
  public static final QueueProperty<Float> CAPACITY_WEIGHT = property(
      CapacitySchedulerConfiguration.CAPACITY, true, ALL,
      (conf, key, queue, label, arg) -> {
        String configuredValue = conf.apply(key);
        float weight = isWeight(configuredValue) ? Float.parseFloat(
            configuredValue.substring(0, configuredValue.indexOf(WEIGHT_SUFFIX)))
            : -1f;
        if ((weight < -1e-6 && Math.abs(weight + 1) > 1e-6) || weight > 10000) {
          throw new IllegalArgumentException("Illegal " + "weight=" + weight
              + " for queue=" + queue.getFullPath() + "label=" + label
              + ". Acceptable values: [0, 10000], -1 is same as not set");
        }
        return weight;
      });

  // P02 and P04, legacy absolute resource classification
  public static final QueueProperty<Boolean> CAPACITY_IS_ABSOLUTE_RESOURCE =
      property(CapacitySchedulerConfiguration.CAPACITY, true, ALL,
          (conf, key, queue, label, arg) -> {
            String resourceString = conf.apply(key);
            return resourceString != null && !resourceString.isEmpty()
                && isAbsoluteResource(resourceString);
          });

  /**
   * P02 and P04, legacy absolute minimum resource. The argument is the set of
   * resource type names known to the caller.
   */
  public static final QueueProperty<Resource> MINIMUM_RESOURCE = property(
      CapacitySchedulerConfiguration.CAPACITY, true, ALL,
      QueueProperties::readAbsoluteResource);

  // P02 and P04, capacity vector
  public static final QueueProperty<QueueCapacityVector> CAPACITY_VECTOR =
      property(CapacitySchedulerConfiguration.CAPACITY, true, ALL,
          (conf, key, queue, label, arg) ->
              CAPACITY_PARSER.parse(conf.apply(key), queue));

  // P03, percentage reading
  public static final QueueProperty<Float> MAXIMUM_CAPACITY = property(
      CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, false, ALL,
      (conf, key, queue, label, arg) -> {
        String configuredCapacity = conf.apply(key);
        if (isAbsoluteResource(configuredCapacity)
            || CAPACITY_PARSER.isCapacityVectorFormat(configuredCapacity)) {
          return 100.0f;
        }
        float maxCapacity = (configuredCapacity == null)
            ? MAXIMUM_CAPACITY_VALUE : Float.parseFloat(configuredCapacity);
        return (maxCapacity == DEFAULT_MAXIMUM_CAPACITY_VALUE)
            ? MAXIMUM_CAPACITY_VALUE : maxCapacity;
      });

  /**
   * P03 and P05, legacy absolute maximum resource. The argument is the set of
   * resource type names known to the caller.
   */
  public static final QueueProperty<Resource> MAXIMUM_RESOURCE = property(
      CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, true, ALL,
      QueueProperties::readAbsoluteResource);

  /**
   * P03 and P05, maximum capacity vector. The argument is the vector used
   * when nothing is configured; by default an empty vector, which means the
   * parent's maximum.
   */
  public static final QueueProperty<QueueCapacityVector>
      MAXIMUM_CAPACITY_VECTOR = property(
          CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, true, ALL,
          (conf, key, queue, label, arg) -> {
            QueueCapacityVector capacityVector =
                CAPACITY_PARSER.parse(conf.apply(key), queue);
            if (capacityVector.isEmpty()) {
              capacityVector = arg != null ? (QueueCapacityVector) arg
                  : QueueCapacityVector.newInstance();
            }
            return capacityVector;
          });

  // P04, percentage reading
  public static final QueueProperty<Float> LABELED_CAPACITY = property(
      CapacitySchedulerConfiguration.CAPACITY, true, ALL,
      (conf, key, queue, label, arg) ->
          readLabeledCapacity(conf, key, queue, label, 0f));

  // P05, percentage reading
  public static final QueueProperty<Float> LABELED_MAXIMUM_CAPACITY = property(
      CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, true, ALL,
      (conf, key, queue, label, arg) ->
          readLabeledCapacity(conf, key, queue, label, 100f));

  // P06
  public static final QueueProperty<Float> MAXIMUM_AM_RESOURCE_PERCENT =
      property(MAXIMUM_AM_RESOURCE_SUFFIX, false, SET_UP,
          (conf, key, queue, label, arg) -> {
            float fallback = readFloat(conf,
                MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
                DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT);
            return readFloat(conf, key, fallback);
          });

  // P07; the empty label reads the P06 key a second time
  public static final QueueProperty<Float>
      LABELED_MAXIMUM_AM_RESOURCE_PERCENT = property(
          MAXIMUM_AM_RESOURCE_SUFFIX, true, ALL,
          (conf, key, queue, label, arg) -> readFloat(conf, key,
              MAXIMUM_AM_RESOURCE_PERCENT.read(conf, queue)));

  // P08, -1 when not set
  public static final QueueProperty<Integer> MAXIMUM_APPLICATIONS = property(
      MAXIMUM_APPLICATIONS_SUFFIX, false, LEAVES,
      (conf, key, queue, label, arg) -> readInt(conf, key, (int) UNDEFINED));

  // P09
  public static final QueueProperty<Float> USER_LIMIT = property(
      CapacitySchedulerConfiguration.USER_LIMIT, false, LEAVES,
      (conf, key, queue, label, arg) -> readFloat(conf, key,
          readFloat(conf, PREFIX + CapacitySchedulerConfiguration.USER_LIMIT,
              DEFAULT_USER_LIMIT)));

  // P10
  public static final QueueProperty<Float> USER_LIMIT_FACTOR = property(
      CapacitySchedulerConfiguration.USER_LIMIT_FACTOR, false, LEAVES,
      (conf, key, queue, label, arg) -> readFloat(conf, key,
          readFloat(conf, PREFIX + CapacitySchedulerConfiguration
              .USER_LIMIT_FACTOR, DEFAULT_USER_LIMIT_FACTOR)));

  /**
   * P11, the weights of the queue's own user-settings keys. The argument is
   * the map of keys under {@code user-settings.} (prefix removed) to their raw
   * values, and the function maps a raw value to the value to parse. The
   * resolver adds the PARENT step, the union with the ancestors' weights.
   */
  public static final QueueProperty<Map<String, Float>> USER_WEIGHTS =
      property(USER_SETTINGS + DOT, false, SET_UP,
        (conf, key, queue, label, arg) -> {
            @SuppressWarnings("unchecked")
            Map<String, String> entries = (Map<String, String>) arg;
            Map<String, Float> weights = new HashMap<>();
            for (Map.Entry<String, String> item : entries.entrySet()) {
              Matcher m = USER_WEIGHT_PATTERN.matcher(item.getKey());
              if (m.find()) {
                String userName =
                    item.getKey().replaceFirst("\\." + USER_WEIGHT, "");
                if (!userName.isEmpty()) {
                  weights.put(userName,
                      Float.valueOf(conf.apply(item.getValue())));
                }
              }
            }
            return weights;
          });

  /**
   * P12, the configured state, {@code null} when not set. The resolver adds
   * the PARENT step of the initial state.
   */
  public static final QueueProperty<QueueState> STATE = property(
      CapacitySchedulerConfiguration.STATE, false, SET_UP,
      (conf, key, queue, label, arg) -> {
        String state = conf.apply(key);
        return state == null ? null
            : QueueState.valueOf(StringUtils.toUpperCase(state));
      });

  /**
   * P13, {@code null} when not set on a non-root queue. The resolver adds the
   * PARENT step.
   */
  public static final QueueProperty<Set<String>> ACCESSIBLE_NODE_LABELS =
      property(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, false,
          SET_UP, (conf, key, queue, label, arg) -> {
            String accessibleLabelStr = conf.apply(key);
            if (accessibleLabelStr == null) {
              if (!queue.isRoot()) {
                return null;
              }
            } else if (queue.isRoot()) {
              LOG.warn("Accessible node labels for root queue will be ignored,"
                  + " it will be automatically set to \"*\".");
            }
            if (queue.isRoot()) {
              return ImmutableSet.of(RMNodeLabelsManager.ANY);
            }
            Set<String> set = new HashSet<>();
            for (String str : accessibleLabelStr.split(",")) {
              if (!str.trim().isEmpty()) {
                set.add(str.trim());
              }
            }
            if (set.contains(RMNodeLabelsManager.ANY)) {
              set.clear();
              set.add(RMNodeLabelsManager.ANY);
            }
            return Collections.unmodifiableSet(set);
          });

  /**
   * P14, {@code null} when not set. The resolver adds the PARENT step.
   */
  public static final QueueProperty<String> DEFAULT_NODE_LABEL_EXPRESSION =
      property(CapacitySchedulerConfiguration.DEFAULT_NODE_LABEL_EXPRESSION,
          false, SET_UP, (conf, key, queue, label, arg) -> {
            String defaultLabelExpression = conf.apply(key);
            return defaultLabelExpression == null ? null
                : defaultLabelExpression.trim();
          });

  /**
   * P15, the ACL text. Dynamic queues take it from their templates only,
   * see {@link QueueConfigResolver}.
   */
  public static final QueueProperty<String> ACL_SUBMIT_APPLICATIONS =
      queueAcl("acl_submit_applications");

  // P16, as P15
  public static final QueueProperty<String> ACL_ADMINISTER_QUEUE =
      queueAcl("acl_administer_queue");

  /**
   * P17, parsed and capped priority ACL groups. The argument is the cluster
   * maximum application priority.
   */
  public static final QueueProperty<List<AppPriorityACLGroup>>
      ACL_APPLICATION_MAX_PRIORITY = property("acl_application_max_priority",
          false, LEAF, (conf, key, queue, label, arg) -> {
            String aclString = conf.apply(key);
            return PRIORITY_ACL_PARSER.getPriorityAcl((Priority) arg,
                aclString == null ? ALL_ACL : aclString);
          });

  // P18 to P20, the ACL text; every queue defaults to all access
  public static final QueueProperty<String> ACL_SUBMIT_RESERVATIONS =
      string("acl_submit_reservations", PLANS, ALL_ACL);
  public static final QueueProperty<String> ACL_LIST_RESERVATIONS =
      string("acl_list_reservations", PLANS, ALL_ACL);
  public static final QueueProperty<String> ACL_ADMINISTER_RESERVATIONS =
      string("acl_administer_reservations", PLANS, ALL_ACL);

  // P21, leaf semantics: the untrimmed application ordering policy name
  public static final QueueProperty<String> APP_ORDERING_POLICY = property(
      ORDERING_POLICY, false, LEAF,
      (conf, key, queue, label, arg) -> {
        String value = conf.apply(key);
        return value == null ? DEFAULT_APP_ORDERING_POLICY : value;
      });

  /**
   * P21, parent semantics: the trimmed queue ordering policy name. The
   * argument is the parent's policy name, used when the key is not set.
   */
  public static final QueueProperty<String> QUEUE_ORDERING_POLICY = property(
      ORDERING_POLICY, false, PARENT_OBJECTS,
      (conf, key, queue, label, arg) -> {
        String defaultPolicy = arg != null ? (String) arg
            : DEFAULT_QUEUE_ORDERING_POLICY;
        String value = conf.apply(key);
        return (value == null ? defaultPolicy : value).trim();
      });

  /**
   * P22 to P24, the ordering policy parameters passed to the policy, which
   * parses them. The argument is the map of keys under
   * {@code ordering-policy.} (prefix removed) to their raw values.
   */
  public static final QueueProperty<Map<String, String>>
      ORDERING_POLICY_PARAMETERS = entries(ORDERING_POLICY + DOT, LEAF);

  // P25
  public static final QueueProperty<Integer> PRIORITY =
      integer("priority", SET_UP, 0);

  // P26
  public static final QueueProperty<Integer> DEFAULT_APPLICATION_PRIORITY =
      integer(CapacitySchedulerConfiguration.DEFAULT_APPLICATION_PRIORITY,
          LEAF, DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);

  /**
   * P27, {@link Resources#none()} when not set. The resolver adds the
   * fallback to P28 and P29 and the PARENT and GLOBAL steps.
   */
  public static final QueueProperty<Resource> MAXIMUM_ALLOCATION = property(
      CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION, false, SET_UP,
      (conf, key, queue, label, arg) -> {
        String rawQueueMaxAllocation = conf.apply(key);
        if (rawQueueMaxAllocation == null || rawQueueMaxAllocation.isEmpty()) {
          return Resources.none();
        }
        return ResourceUtils.createResourceFromString(rawQueueMaxAllocation,
            ResourceUtils.getResourcesTypeInfo());
      });

  // P28, -1 when not set
  public static final QueueProperty<Long> MAXIMUM_ALLOCATION_MB = property(
      CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB, false, SET_UP,
      (conf, key, queue, label, arg) ->
          (long) readInt(conf, key, (int) UNDEFINED));

  // P29, -1 when not set
  public static final QueueProperty<Integer> MAXIMUM_ALLOCATION_VCORES =
      integer(CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES, SET_UP,
          (int) UNDEFINED);

  /**
   * P30. The argument is the default, which is the parent's value below the
   * root. The resolver adds the scheduler-wide monitor gate.
   */
  public static final QueueProperty<Boolean> PREEMPTION_DISABLED =
      flagWithDefaultArgument(QUEUE_PREEMPTION_DISABLED);

  /**
   * P31, the in-hierarchy flag. The argument is the default, which is the
   * parent's in-hierarchy value below the root. The resolver adds the
   * scheduler-wide intra-queue preemption gate.
   */
  public static final QueueProperty<Boolean> INTRA_QUEUE_PREEMPTION_DISABLED =
      flagWithDefaultArgument("intra-queue-preemption."
          + QUEUE_PREEMPTION_DISABLED);

  /**
   * P32, -1 when not set. The resolver adds the PARENT step.
   */
  public static final QueueProperty<Long> MAXIMUM_APPLICATION_LIFETIME =
      longValue(MAXIMUM_LIFETIME_SUFFIX, SET_UP, (long) UNDEFINED);

  /**
   * P33, -1 when not set. The resolver adds the PARENT step and the fallback
   * to the maximum lifetime.
   */
  public static final QueueProperty<Long> DEFAULT_APPLICATION_LIFETIME =
      longValue(DEFAULT_LIFETIME_SUFFIX, SET_UP, (long) UNDEFINED);

  // P34; the queue value is not trimmed, unlike every other number
  public static final QueueProperty<Integer> MAX_PARALLEL_APPS = property(
      MAX_PARALLEL_APPLICATIONS, false, SET_UP,
      (conf, key, queue, label, arg) -> {
        String maxParallelAppsForQueue = conf.apply(key);
        return (maxParallelAppsForQueue != null)
            ? Integer.parseInt(maxParallelAppsForQueue)
            : readInt(conf, PREFIX + MAX_PARALLEL_APPLICATIONS,
                DEFAULT_MAX_PARALLEL_APPLICATIONS);
      });

  /**
   * P35, the configured class name of the queue's multi-node sorting policy,
   * {@code null} when multi-node lookup is off. The getter also checks that
   * the class can be loaded, which the resolver does not.
   */
  public static final QueueProperty<String> MULTI_NODE_SORTING_POLICY =
      property("multi-node-sorting.policy", false, SET_UP,
          (conf, key, queue, label, arg) -> {
            String policyName = conf.apply(key);
            if (policyName == null) {
              policyName = conf.apply(MULTI_NODE_SORTING_POLICY_NAME);
            }
            if (policyName == null || policyName.isEmpty()) {
              return null;
            }
            String policyClassName = conf.apply(MULTI_NODE_SORTING_POLICY_NAME
                + DOT + policyName.trim() + DOT + "class");
            if (policyClassName == null || policyClassName.isEmpty()) {
              throw new YarnRuntimeException(policyName.trim()
                  + " Class is not configured or not an instance of "
                  + "org.apache.hadoop.yarn.server.resourcemanager.scheduler"
                  + ".placement.MultiNodeLookupPolicy");
            }
            return policyClassName.trim();
          });

  // P36
  public static final QueueProperty<Boolean> ALLOW_ZERO_CAPACITY_SUM =
      bool(CapacitySchedulerConfiguration.ALLOW_ZERO_CAPACITY_SUM,
          PARENT_OBJECTS,
          DEFAULT_ALLOW_ZERO_CAPACITY_SUM);

  // P37
  public static final QueueProperty<Boolean> AUTO_CREATE_CHILD_QUEUE_ENABLED =
      bool(CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED,
          ALL, DEFAULT_AUTO_CREATE_CHILD_QUEUE_ENABLED);

  // P38
  public static final QueueProperty<Integer> AUTO_CREATE_CHILD_QUEUE_MAX_QUEUES =
      integer(AUTO_CREATE_QUEUE_MAX_QUEUES, PARENTS,
          DEFAULT_AUTO_CREATE_QUEUE_MAX_QUEUES);

  // P39
  public static final QueueProperty<Boolean>
      AUTO_CREATE_CHILD_QUEUE_FAIL_ON_EXCEEDING_CAPACITY = bool(
          FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY, PARENTS,
          DEFAULT_FAIL_AUTO_CREATION_ON_EXCEEDING_CAPACITY);

  // P40, the class name
  public static final QueueProperty<String>
      AUTO_CREATED_QUEUE_MANAGEMENT_POLICY_NAME = string(
          AUTO_CREATED_QUEUE_MANAGEMENT_POLICY, PARENTS,
          DEFAULT_AUTO_CREATED_QUEUE_MANAGEMENT_POLICY);

  /**
   * P41, the v1 leaf queue template of a managed parent: full template keys
   * to raw values. The argument is that map.
   */
  public static final QueueProperty<Map<String, String>> LEAF_QUEUE_TEMPLATE =
      entries(AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX + DOT, PARENT_OBJECTS);

  // P42
  public static final QueueProperty<Boolean> AUTO_QUEUE_CREATION_V2_ENABLED =
      bool(CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_ENABLED, ALL,
          DEFAULT_AUTO_QUEUE_CREATION_ENABLED);

  // P43
  public static final QueueProperty<Integer>
      AUTO_QUEUE_CREATION_V2_MAX_QUEUES = integer(
          CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
          PARENTS, DEFAULT_AUTO_QUEUE_CREATION_V2_MAX_QUEUES);

  // P44
  public static final QueueProperty<Integer> AUTO_QUEUE_CREATION_V2_MAX_DEPTH =
      property(MAXIMUM_QUEUE_DEPTH, false, PARENT_OBJECTS,
          (conf, key, queue, label, arg) -> readInt(conf, key, readInt(conf,
              GLOBAL_MAXIMUM_QUEUE_DEPTH, DEFAULT_MAXIMUM_QUEUE_DEPTH)));

  // P45
  public static final QueueProperty<Boolean> AUTO_QUEUE_AUTO_REMOVAL_ENABLED =
      property(AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE, false, ALL,
        (conf, key, queue, label, arg) -> readBoolean(conf, key,
              DEFAULT_AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE));

  /**
   * P46, the v2 {@code template} entries a child of the queue receives, with
   * the most specific wildcard path first. The argument is that map.
   */
  public static final QueueProperty<Map<String, String>> AQC_V2_TEMPLATE =
      entries(AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX,
          PARENT_OBJECTS);

  // P47, as P46 for {@code leaf-template}
  public static final QueueProperty<Map<String, String>> AQC_V2_LEAF_TEMPLATE =
      entries(AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX,
          PARENT_OBJECTS);

  // P48, as P46 for {@code parent-template}
  public static final QueueProperty<Map<String, String>>
      AQC_V2_PARENT_TEMPLATE = entries(
          AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX,
          PARENT_OBJECTS);

  // P49
  public static final QueueProperty<Boolean> RESERVABLE =
      bool(IS_RESERVABLE, ALL, false);

  // P50
  public static final QueueProperty<Long> RESERVATION_WINDOW_MS = longValue(
      RESERVATION_WINDOW, PLANS, DEFAULT_RESERVATION_WINDOW);

  // P51
  public static final QueueProperty<Float> AVERAGE_CAPACITY_PERCENT =
      property(AVERAGE_CAPACITY, false, PLANS,
          (conf, key, queue, label, arg) ->
              readFloat(conf, key, MAXIMUM_CAPACITY_VALUE));

  // P52
  public static final QueueProperty<Float> INSTANTANEOUS_MAX_CAPACITY_PERCENT =
      property(INSTANTANEOUS_MAX_CAPACITY, false, PLANS,
          (conf, key, queue, label, arg) ->
              readFloat(conf, key, MAXIMUM_CAPACITY_VALUE));

  // P53
  public static final QueueProperty<String> RESERVATION_ADMISSION_POLICY_NAME =
      string(RESERVATION_ADMISSION_POLICY, PLANS,
          DEFAULT_RESERVATION_ADMISSION_POLICY);

  // P54
  public static final QueueProperty<String> RESERVATION_AGENT =
      string(RESERVATION_AGENT_NAME, PLANS, DEFAULT_RESERVATION_AGENT_NAME);

  // P55
  public static final QueueProperty<String> RESERVATION_PLANNER =
      string(RESERVATION_PLANNER_NAME, PLANS, DEFAULT_RESERVATION_PLANNER_NAME);

  // P56
  public static final QueueProperty<Boolean> RESERVATION_MOVE_ON_EXPIRY_ENABLED =
      bool(RESERVATION_MOVE_ON_EXPIRY, PLANS,
          DEFAULT_RESERVATION_MOVE_ON_EXPIRY);

  // P57
  public static final QueueProperty<Long> RESERVATION_ENFORCEMENT_WINDOW_MS =
      longValue(RESERVATION_ENFORCEMENT_WINDOW, PLANS,
          DEFAULT_RESERVATION_ENFORCEMENT_WINDOW);

  // P58
  public static final QueueProperty<Boolean> SHOW_RESERVATIONS_AS_QUEUES =
      bool(RESERVATION_SHOW_RESERVATION_AS_QUEUE, PLANS,
          DEFAULT_SHOW_RESERVATIONS_AS_QUEUES);

  // Values the resolver derives from other resolved values; they have no
  // key of their own and cannot be read from a configuration

  /** The labels labeled properties are resolved for (Q11). */
  public static final QueueProperty<Set<String>> CONFIGURED_NODE_LABELS =
      derived("configured-node-labels", false, SET_UP);

  /** Configured absolute capacity per label, a fraction. */
  public static final QueueProperty<Float> ABSOLUTE_CAPACITY =
      derived("absolute-capacity", true, SET_UP);

  /** Configured absolute maximum capacity per label, a fraction. */
  public static final QueueProperty<Float> ABSOLUTE_MAXIMUM_CAPACITY =
      derived("absolute-maximum-capacity", true, ALL);

  /**
   * Maximum applications of a leaf in a cluster that has resources: the
   * explicit value, the global per-queue value, or derived from the absolute
   * capacity.
   */
  public static final QueueProperty<Integer> EFFECTIVE_MAXIMUM_APPLICATIONS =
      derived("effective-maximum-applications", false, LEAVES);

  /** Maximum applications per user of a leaf. */
  public static final QueueProperty<Integer> MAXIMUM_APPLICATIONS_PER_USER =
      derived("maximum-applications-per-user", false, LEAVES);

  /** Every configured property, in key table order. */
  public static final List<QueueProperty<?>> ALL_PROPERTIES =
      Collections.unmodifiableList(java.util.Arrays.<QueueProperty<?>>asList(
          QUEUES, CAPACITY, CAPACITY_WEIGHT, CAPACITY_IS_ABSOLUTE_RESOURCE,
          MINIMUM_RESOURCE, CAPACITY_VECTOR, MAXIMUM_CAPACITY,
          MAXIMUM_RESOURCE, MAXIMUM_CAPACITY_VECTOR, LABELED_CAPACITY,
          LABELED_MAXIMUM_CAPACITY, MAXIMUM_AM_RESOURCE_PERCENT,
          LABELED_MAXIMUM_AM_RESOURCE_PERCENT, MAXIMUM_APPLICATIONS,
          USER_LIMIT, USER_LIMIT_FACTOR, USER_WEIGHTS, STATE,
          ACCESSIBLE_NODE_LABELS, DEFAULT_NODE_LABEL_EXPRESSION,
          ACL_SUBMIT_APPLICATIONS, ACL_ADMINISTER_QUEUE,
          ACL_APPLICATION_MAX_PRIORITY, ACL_SUBMIT_RESERVATIONS,
          ACL_LIST_RESERVATIONS, ACL_ADMINISTER_RESERVATIONS,
          APP_ORDERING_POLICY, QUEUE_ORDERING_POLICY,
          ORDERING_POLICY_PARAMETERS, PRIORITY, DEFAULT_APPLICATION_PRIORITY,
          MAXIMUM_ALLOCATION, MAXIMUM_ALLOCATION_MB, MAXIMUM_ALLOCATION_VCORES,
          PREEMPTION_DISABLED, INTRA_QUEUE_PREEMPTION_DISABLED,
          MAXIMUM_APPLICATION_LIFETIME, DEFAULT_APPLICATION_LIFETIME,
          MAX_PARALLEL_APPS, MULTI_NODE_SORTING_POLICY,
          ALLOW_ZERO_CAPACITY_SUM, AUTO_CREATE_CHILD_QUEUE_ENABLED,
          AUTO_CREATE_CHILD_QUEUE_MAX_QUEUES,
          AUTO_CREATE_CHILD_QUEUE_FAIL_ON_EXCEEDING_CAPACITY,
          AUTO_CREATED_QUEUE_MANAGEMENT_POLICY_NAME, LEAF_QUEUE_TEMPLATE,
          AUTO_QUEUE_CREATION_V2_ENABLED, AUTO_QUEUE_CREATION_V2_MAX_QUEUES,
          AUTO_QUEUE_CREATION_V2_MAX_DEPTH, AUTO_QUEUE_AUTO_REMOVAL_ENABLED,
          AQC_V2_TEMPLATE, AQC_V2_LEAF_TEMPLATE, AQC_V2_PARENT_TEMPLATE,
          RESERVABLE, RESERVATION_WINDOW_MS, AVERAGE_CAPACITY_PERCENT,
          INSTANTANEOUS_MAX_CAPACITY_PERCENT,
          RESERVATION_ADMISSION_POLICY_NAME, RESERVATION_AGENT,
          RESERVATION_PLANNER, RESERVATION_MOVE_ON_EXPIRY_ENABLED,
          RESERVATION_ENFORCEMENT_WINDOW_MS, SHOW_RESERVATIONS_AS_QUEUES));

  /**
   * Whether a capacity value is a legacy absolute resource, matched on the
   * substituted value.
   * @param value the capacity value, may be {@code null}
   * @return true if it matches {@code RESOURCE_PATTERN}
   */
  public static boolean isAbsoluteResource(String value) {
    return value != null && RESOURCE_PATTERN.matcher(value).find();
  }

  private static boolean isWeight(String value) {
    return value != null && value.endsWith(WEIGHT_SUFFIX);
  }

  /**
   * Parses a legacy absolute resource, {@code [memory=..,vcores=..]}.
   * @param resourceString the configured value, may be {@code null}
   * @param resourceTypes the resource type names known to the caller
   * @return the resource, or {@link Resources#none()} when the value is not
   *         set or configures no memory
   */
  public static Resource parseAbsoluteResource(String resourceString,
      Set<String> resourceTypes) {
    if (resourceString == null || resourceString.isEmpty()) {
      return Resources.none();
    }
    Resource resource = Resource.newInstance(0L, 0);
    Matcher matcher = RESOURCE_PATTERN.matcher(resourceString);
    // An absolute resource is grouped by "[]", for example
    // "[memory=4Gi,vcores=2]"
    if (matcher.find()) {
      String subGroup = matcher.group(0);
      if (subGroup.trim().isEmpty()) {
        return Resources.none();
      }
      subGroup = subGroup.substring(1, subGroup.length() - 1);
      for (String kvPair : subGroup.trim().split(",")) {
        String[] splits = kvPair.split("=");
        if (splits != null && splits.length > 1) {
          updateResourceValue(resourceTypes, resource, splits);
        }
      }
    }
    // Memory has to be configured always
    if (resource.getMemorySize() == 0L) {
      return Resources.none();
    }
    return resource;
  }

  private static void updateResourceValue(Set<String> resourceTypes,
      Resource resource, String[] splits) {
    String resourceName = splits[0].trim();
    if (!resourceTypes.contains(resourceName)
        && !ResourceUtils.getResourceTypes().containsKey(resourceName)) {
      LOG.error(resourceName + " not supported.");
      return;
    }
    String units = getUnits(splits[1]);
    if (!UnitsConversionUtil.KNOWN_UNITS.contains(units)) {
      return;
    }
    Long resourceValue = Long.valueOf(
        splits[1].substring(0, splits[1].length() - units.length()));
    // Convert all incoming units to MB if units is configured
    if (!units.isEmpty()) {
      resourceValue = UnitsConversionUtil.convert(units, "Mi", resourceValue);
    }
    if (!resourceTypes.contains(resourceName)) {
      // Custom resource type, such as GPU or FPGA
      resource.setResourceInformation(resourceName,
          ResourceInformation.newInstance(resourceName, units, resourceValue));
      return;
    }
    AbsoluteResourceType resType =
        AbsoluteResourceType.valueOf(StringUtils.toUpperCase(resourceName));
    if (resType == AbsoluteResourceType.MEMORY) {
      resource.setMemorySize(resourceValue);
    } else {
      resource.setVirtualCores(resourceValue.intValue());
    }
  }

  @SuppressWarnings("unchecked")
  private static Resource readAbsoluteResource(Function<String, String> conf,
      String key, QueuePath queue, String label, Object arg) {
    return parseAbsoluteResource(conf.apply(key),
        arg != null ? (Set<String>) arg : QUEUE_RESOURCE_TYPES);
  }

  private static float readLabeledCapacity(Function<String, String> conf,
      String key, QueuePath queue, String label, float defaultValue) {
    String configuredCapacity = conf.apply(key);
    if (isAbsoluteResource(configuredCapacity) || isWeight(configuredCapacity)
        || CAPACITY_PARSER.isCapacityVectorFormat(configuredCapacity)) {
      // Absolute resources, weights and vectors are parsed separately
      return queue.isRoot() ? 100.0f : defaultValue;
    }
    float capacity = queue.isRoot() ? 100.0f
        : readFloat(conf, key, defaultValue);
    if (capacity < MINIMUM_CAPACITY_VALUE
        || capacity > MAXIMUM_CAPACITY_VALUE) {
      throw new IllegalArgumentException("Illegal capacity of " + capacity
          + " for node-label=" + label + " in queue=" + queue
          + ", valid capacity should in range of [0, 100].");
    }
    return capacity;
  }

  // Mirrors of the Configuration typed getters, so that values parse the
  // same way from the snapshot and from a live configuration.

  static float readFloat(Function<String, String> conf, String key,
      float defaultValue) {
    String value = conf.apply(key);
    return value == null ? defaultValue : Float.parseFloat(value.trim());
  }

  static int readInt(Function<String, String> conf, String key,
      int defaultValue) {
    String value = conf.apply(key);
    if (value == null) {
      return defaultValue;
    }
    String trimmed = value.trim();
    String hex = hexDigits(trimmed);
    return hex == null ? Integer.parseInt(trimmed) : Integer.parseInt(hex, 16);
  }

  static long readLong(Function<String, String> conf, String key,
      long defaultValue) {
    String value = conf.apply(key);
    if (value == null) {
      return defaultValue;
    }
    String trimmed = value.trim();
    String hex = hexDigits(trimmed);
    return hex == null ? Long.parseLong(trimmed) : Long.parseLong(hex, 16);
  }

  static boolean readBoolean(Function<String, String> conf, String key,
      boolean defaultValue) {
    String value = conf.apply(key);
    if (value == null) {
      return defaultValue;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return defaultValue;
    }
    if ("true".equalsIgnoreCase(trimmed)) {
      return true;
    } else if ("false".equalsIgnoreCase(trimmed)) {
      return false;
    }
    CONF_LOG.warn("Invalid value for boolean: " + trimmed
        + ", choose default value: " + defaultValue + " for " + key);
    return defaultValue;
  }

  private static String hexDigits(String value) {
    boolean negative = value.startsWith("-");
    String unsigned = negative ? value.substring(1) : value;
    if (!unsigned.startsWith("0x") && !unsigned.startsWith("0X")) {
      return null;
    }
    return negative ? "-" + unsigned.substring(2) : unsigned.substring(2);
  }

  private static <T> QueueProperty<T> property(String name, boolean labeled,
      Set<Kind> appliesTo, QueueProperty.Reader<T> reader) {
    return new QueueProperty<>(name, labeled, appliesTo, reader);
  }

  private static <T> QueueProperty<T> derived(String name, boolean labeled,
      Set<Kind> appliesTo) {
    return property(name, labeled, appliesTo,
        (conf, key, queue, label, arg) -> {
          throw new UnsupportedOperationException(
              name + " is derived by the resolver");
        });
  }

  private static QueueProperty<Integer> integer(String name,
      Set<Kind> appliesTo, int defaultValue) {
    return property(name, false, appliesTo,
        (conf, key, queue, label, arg) -> readInt(conf, key, defaultValue));
  }

  private static QueueProperty<Long> longValue(String name,
      Set<Kind> appliesTo, long defaultValue) {
    return property(name, false, appliesTo,
        (conf, key, queue, label, arg) -> readLong(conf, key, defaultValue));
  }

  private static QueueProperty<Boolean> bool(String name, Set<Kind> appliesTo,
      boolean defaultValue) {
    return property(name, false, appliesTo,
        (conf, key, queue, label, arg) ->
            readBoolean(conf, key, defaultValue));
  }

  private static QueueProperty<Boolean> flagWithDefaultArgument(String name) {
    return property(name, false, SET_UP,
        (conf, key, queue, label, arg) ->
            readBoolean(conf, key, arg != null && (Boolean) arg));
  }

  private static QueueProperty<String> string(String name, Set<Kind> appliesTo,
      String defaultValue) {
    return property(name, false, appliesTo,
        (conf, key, queue, label, arg) -> {
          String value = conf.apply(key);
          return value == null ? defaultValue : value;
        });
  }

  private static QueueProperty<String> queueAcl(String name) {
    return property(name, false, SET_UP,
        (conf, key, queue, label, arg) -> {
          String value = conf.apply(key);
          if (value == null) {
            // The root queue defaults to all access, other queues to none
            return queue.isRoot() ? ALL_ACL : NONE_ACL;
          }
          return value;
        });
  }

  private static QueueProperty<Map<String, String>> entries(String prefix,
      Set<Kind> appliesTo) {
    return property(prefix, false, appliesTo,
        (conf, key, queue, label, arg) -> {
          @SuppressWarnings("unchecked")
          Map<String, String> entries = (Map<String, String>) arg;
          Map<String, String> values = new HashMap<>();
          for (Map.Entry<String, String> entry : entries.entrySet()) {
            values.put(entry.getKey(), conf.apply(entry.getValue()));
          }
          return values;
        });
  }
}
