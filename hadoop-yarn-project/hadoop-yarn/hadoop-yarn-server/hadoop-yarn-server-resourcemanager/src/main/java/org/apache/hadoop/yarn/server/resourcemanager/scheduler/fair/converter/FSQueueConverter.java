/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Converts a Fair Schedule queue hierarchy to Capacity Scheduler
 * configuration.
 *
 */
public class FSQueueConverter {
  public static final float QUEUE_MAX_AM_SHARE_DISABLED = -1.0f;
  private static final int MAX_RUNNING_APPS_UNSET = Integer.MAX_VALUE;
  private static final String FAIR_POLICY = "fair";
  private static final String FIFO_POLICY = "fifo";

  private final FSConfigToCSConfigRuleHandler ruleHandler;
  private Configuration capacitySchedulerConfig;
  private final boolean preemptionEnabled;
  private final boolean sizeBasedWeight;
  @SuppressWarnings("unused")
  private final Resource clusterResource;
  private final float queueMaxAMShareDefault;
  private final boolean autoCreateChildQueues;
  private final int queueMaxAppsDefault;
  private final boolean drfUsed;

  private ConversionOptions conversionOptions;

  public FSQueueConverter(FSQueueConverterBuilder builder) {
    this.ruleHandler = builder.ruleHandler;
    this.capacitySchedulerConfig = builder.capacitySchedulerConfig;
    this.preemptionEnabled = builder.preemptionEnabled;
    this.sizeBasedWeight = builder.sizeBasedWeight;
    this.clusterResource = builder.clusterResource;
    this.queueMaxAMShareDefault = builder.queueMaxAMShareDefault;
    this.autoCreateChildQueues = builder.autoCreateChildQueues;
    this.queueMaxAppsDefault = builder.queueMaxAppsDefault;
    this.conversionOptions = builder.conversionOptions;
    this.drfUsed = builder.drfUsed;
  }

  public void convertQueueHierarchy(FSQueue queue) {
    List<FSQueue> children = queue.getChildQueues();
    final String queueName = queue.getName();

    emitChildQueues(queueName, children);
    emitMaxAMShare(queueName, queue);
    emitMaxParallelApps(queueName, queue);
    emitMaxAllocations(queueName, queue);
    emitPreemptionDisabled(queueName, queue);

    emitChildCapacity(queue);
    emitMaximumCapacity(queueName, queue);
    emitAutoCreateChildQueue(queueName, queue);
    emitSizeBasedWeight(queueName);
    emitOrderingPolicy(queueName, queue);
    checkMaxChildCapacitySetting(queue);

    for (FSQueue childQueue : children) {
      convertQueueHierarchy(childQueue);
    }
  }

  /**
   * Generates yarn.scheduler.capacity.&lt;queue-name&gt;.queues.
   * @param queueName
   * @param children
   */
  private void emitChildQueues(String queueName, List<FSQueue> children) {
    ruleHandler.handleChildQueueCount(queueName, children.size());

    if (children.size() > 0) {
      String childQueues = children.stream()
          .map(child -> getQueueShortName(child.getName()))
          .collect(Collectors.joining(","));

      capacitySchedulerConfig.set(PREFIX + queueName + ".queues", childQueues);
    }
  }

  /**
   * &lt;maxAMShare&gt; 
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-am-resource-percent.
   * @param queueName
   * @param queue
   */
  private void emitMaxAMShare(String queueName, FSQueue queue) {
    float queueMaxAmShare = queue.getMaxAMShare();

    // Direct floating point comparison is OK here
    if (queueMaxAmShare != 0.0f
        && queueMaxAmShare != queueMaxAMShareDefault
        && queueMaxAmShare != QUEUE_MAX_AM_SHARE_DISABLED) {
      capacitySchedulerConfig.setFloat(PREFIX + queueName +
          ".maximum-am-resource-percent", queueMaxAmShare);
    }

    if (queueMaxAmShare == QUEUE_MAX_AM_SHARE_DISABLED
        && queueMaxAmShare != queueMaxAMShareDefault) {
      capacitySchedulerConfig.setFloat(PREFIX + queueName +
          ".maximum-am-resource-percent", 1.0f);
    }
  }

  /**
   * &lt;maxRunningApps&gt;
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.max-parallel-apps.
   * @param queueName
   * @param queue
   */
  private void emitMaxParallelApps(String queueName, FSQueue queue) {
    if (queue.getMaxRunningApps() != MAX_RUNNING_APPS_UNSET
        && queue.getMaxRunningApps() != queueMaxAppsDefault) {
      capacitySchedulerConfig.set(PREFIX + queueName + ".max-parallel-apps",
          String.valueOf(queue.getMaxRunningApps()));
    }
  }

  /**
   * &lt;maxResources&gt;
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-capacity.
   * @param queueName
   * @param queue
   */
  private void emitMaximumCapacity(String queueName, FSQueue queue) {
    ConfigurableResource rawMaxShare = queue.getRawMaxShare();
    final Resource maxResource = rawMaxShare.getResource();

    if ((maxResource == null && rawMaxShare.getPercentages() != null)
        || isNotUnboundedResource(maxResource)) {
      ruleHandler.handleMaxResources();
    }

    capacitySchedulerConfig.set(PREFIX + queueName + ".maximum-capacity",
        "100");
  }

  /**
   * &lt;maxContainerAllocation&gt;
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-allocation-mb
   * / vcores.
   * @param queueName
   * @param queue
   */
  private void emitMaxAllocations(String queueName, FSQueue queue) {
    Resource maxAllocation = queue.getMaximumContainerAllocation();

    if (isNotUnboundedResource(maxAllocation)) {
      long parentMaxVcores = Integer.MIN_VALUE;
      long parentMaxMemory = Integer.MIN_VALUE;

      if (queue.getParent() != null) {
        FSQueue parent = queue.getParent();
        Resource parentMaxAllocation = parent.getMaximumContainerAllocation();
        if (isNotUnboundedResource(parentMaxAllocation)) {
          parentMaxVcores = parentMaxAllocation.getVirtualCores();
          parentMaxMemory = parentMaxAllocation.getMemorySize();
        }
      }

      long maxVcores = maxAllocation.getVirtualCores();
      long maxMemory = maxAllocation.getMemorySize();

      // only emit max allocation if it differs from the parent's setting
      if (maxVcores != parentMaxVcores || maxMemory != parentMaxMemory) {
        capacitySchedulerConfig.set(PREFIX + queueName +
            ".maximum-allocation-mb", String.valueOf(maxMemory));

        capacitySchedulerConfig.set(PREFIX + queueName +
            ".maximum-allocation-vcores", String.valueOf(maxVcores));
      }
    }
  }

  /**
   * &lt;allowPreemptionFrom&gt;
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.disable_preemption.
   * @param queueName
   * @param queue
   */
  private void emitPreemptionDisabled(String queueName, FSQueue queue) {
    if (preemptionEnabled && !queue.isPreemptable()) {
      capacitySchedulerConfig.set(PREFIX + queueName + ".disable_preemption",
          "true");
    }
  }

  /**
   * yarn.scheduler.fair.allow-undeclared-pools
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;
   * .auto-create-child-queue.enabled.
   * @param queueName
   */
  private void emitAutoCreateChildQueue(String queueName, FSQueue queue) {
    if (autoCreateChildQueues && !queue.getChildQueues().isEmpty()
        && !queueName.equals(CapacitySchedulerConfiguration.ROOT)) {
      capacitySchedulerConfig.setBoolean(PREFIX + queueName +
          ".auto-create-child-queue.enabled", true);
    }
  }

  /**
   * yarn.scheduler.fair.sizebasedweight ==>
   * yarn.scheduler.capacity.&lt;queue-path&gt;
   * .ordering-policy.fair.enable-size-based-weight.
   * @param queueName
   */
  private void emitSizeBasedWeight(String queueName) {
    if (sizeBasedWeight) {
      capacitySchedulerConfig.setBoolean(PREFIX + queueName +
          ".ordering-policy.fair.enable-size-based-weight", true);
    }
  }

  /**
   * &lt;schedulingPolicy&gt;
   * ==> yarn.scheduler.capacity.&lt;queue-path&gt;.ordering-policy.
   * @param queueName
   * @param queue
   */
  private void emitOrderingPolicy(String queueName, FSQueue queue) {
    if (queue instanceof FSLeafQueue) {
      String policy = queue.getPolicy().getName();

      switch (policy) {
      case DominantResourceFairnessPolicy.NAME:
        capacitySchedulerConfig.set(PREFIX + queueName
            + ".ordering-policy", FAIR_POLICY);
        break;
      case FairSharePolicy.NAME:
        capacitySchedulerConfig.set(PREFIX + queueName
            + ".ordering-policy", FAIR_POLICY);
        if (drfUsed) {
          ruleHandler.handleFairAsDrf(queueName);
        }
        break;
      case FifoPolicy.NAME:
        capacitySchedulerConfig.set(PREFIX + queueName
            + ".ordering-policy", FIFO_POLICY);
        break;
      default:
        String msg = String.format("Unexpected ordering policy " +
            "on queue %s: %s", queue, policy);
        conversionOptions.handleConversionError(msg);
      }
    }
  }

  /**
   * weight + minResources
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.capacity.
   * @param queue
   */
  private void emitChildCapacity(FSQueue queue) {
    List<FSQueue> children = queue.getChildQueues();

    int totalWeight = getTotalWeight(children);
    Map<String, BigDecimal> capacities = getCapacities(totalWeight, children);
    capacities
        .forEach((key, value) -> capacitySchedulerConfig.set(PREFIX + key +
                ".capacity", value.toString()));
  }

  /**
   * Missing feature, "leaf-queue-template.capacity" only accepts a single
   * pct value.
   * @param queue
   */
  private void checkMaxChildCapacitySetting(FSQueue queue) {
    if (queue.getMaxChildQueueResource() != null) {
      Resource resource = queue.getMaxChildQueueResource().getResource();

      if ((resource != null && isNotUnboundedResource(resource))
          || queue.getMaxChildQueueResource().getPercentages() != null) {
        // Maximum child resource is defined
        ruleHandler.handleMaxChildCapacity();
      }
    }
  }

  private Map<String, BigDecimal> getCapacities(int totalWeight,
      List<FSQueue> children) {
    final BigDecimal hundred = new BigDecimal(100).setScale(3);

    if (children.size() == 0) {
      return new HashMap<>();
    } else if (children.size() == 1) {
      Map<String, BigDecimal> capacity = new HashMap<>();
      String queueName = children.get(0).getName();
      capacity.put(queueName, hundred);

      return capacity;
    } else {
      Map<String, BigDecimal> capacities = new HashMap<>();

      children
          .stream()
          .forEach(queue -> {
            BigDecimal total = new BigDecimal(totalWeight);
            BigDecimal weight = new BigDecimal(queue.getWeight());
            BigDecimal pct = weight
                              .setScale(5)
                              .divide(total, RoundingMode.HALF_UP)
                              .multiply(hundred)
                              .setScale(3);

            if (Resources.none().compareTo(queue.getMinShare()) != 0) {
              ruleHandler.handleMinResources();
            }

            capacities.put(queue.getName(), pct);
          });

      BigDecimal totalPct = new BigDecimal(0);
      for (Map.Entry<String, BigDecimal> entry : capacities.entrySet()) {
        totalPct = totalPct.add(entry.getValue());
      }

      // fix last value if total != 100.000
      if (!totalPct.equals(hundred)) {
        BigDecimal tmp = new BigDecimal(0);
        for (int i = 0; i < children.size() - 1; i++) {
          tmp = tmp.add(capacities.get(children.get(i).getQueueName()));
        }

        String lastQueue = children.get(children.size() - 1).getName();
        BigDecimal corrected = hundred.subtract(tmp);
        capacities.put(lastQueue, corrected);
      }

      return capacities;
    }
  }

  private int getTotalWeight(List<FSQueue> children) {
    double sum = children
                  .stream()
                  .mapToDouble(c -> c.getWeight())
                  .sum();
    return (int) sum;
  }

  private String getQueueShortName(String queueName) {
    int lastDot = queueName.lastIndexOf(".");
    return queueName.substring(lastDot + 1);
  }

  private boolean isNotUnboundedResource(Resource res) {
    return Resources.unbounded().compareTo(res) != 0;
  }
}