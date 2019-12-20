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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Converts a Fair Schedule queue hierarchy to Capacity Scheduler
 * configuration.
 *
 */
public class FSQueueConverter {
  private static final int MAX_RUNNING_APPS_UNSET = Integer.MIN_VALUE;
  private final Set<String> leafQueueNames;
  private final FSConfigToCSConfigRuleHandler ruleHandler;
  private Configuration capacitySchedulerConfig;
  private final boolean preemptionEnabled;
  private final boolean sizeBasedWeight;
  private final Resource clusterResource;
  private final float queueMaxAMShareDefault;
  private final boolean autoCreateChildQueues;
  private final int queueMaxAppsDefault;

  private boolean fifoOrFairSharePolicyUsed;
  private boolean drfPolicyUsedOnQueueLevel;

  @SuppressWarnings("checkstyle:parameternumber")
  public FSQueueConverter(FSConfigToCSConfigRuleHandler ruleHandler,
      Configuration capacitySchedulerConfig,
      boolean preemptionEnabled,
      boolean sizeBasedWeight,
      boolean autoCreateChildQueues,
      Resource clusterResource,
      float queueMaxAMShareDefault,
      int queueMaxAppsDefault) {
    this.leafQueueNames = new HashSet<>();
    this.ruleHandler = ruleHandler;
    this.capacitySchedulerConfig = capacitySchedulerConfig;
    this.preemptionEnabled = preemptionEnabled;
    this.sizeBasedWeight = sizeBasedWeight;
    this.clusterResource = clusterResource;
    this.queueMaxAMShareDefault = queueMaxAMShareDefault;
    this.autoCreateChildQueues = autoCreateChildQueues;
    this.queueMaxAppsDefault = queueMaxAppsDefault;
  }

  @SuppressWarnings("checkstyle:linelength")
  public void convertQueueHierarchy(FSQueue queue) {
    List<FSQueue> children = queue.getChildQueues();
    final String queueName = queue.getName();

    if (queue instanceof FSLeafQueue) {
      String shortName = getQueueShortName(queueName);
      if (!leafQueueNames.add(shortName)) {
        throw new ConversionException(
            "Leaf queues must be unique, "
                + shortName + " is defined at least twice");
      }
    }

    emitChildQueues(queueName, children);
    emitMaxAMShare(queueName, queue);
    emitMaxRunningApps(queueName, queue);
    emitMaxAllocations(queueName, queue);
    emitPreemptionDisabled(queueName, queue);

    // TODO: COULD BE incorrect! Needs further clarifications
    emitChildCapacity(queue);
    emitMaximumCapacity(queueName, queue);
    emitAutoCreateChildQueue(queueName);
    emitSizeBasedWeight(queueName);
    emitOrderingPolicy(queueName, queue);
    checkMaxChildCapacitySetting(queue);

    for (FSQueue childQueue : children) {
      convertQueueHierarchy(childQueue);
    }
  }

  public boolean isFifoOrFairSharePolicyUsed() {
    return fifoOrFairSharePolicyUsed;
  }

  public boolean isDrfPolicyUsedOnQueueLevel() {
    return drfPolicyUsedOnQueueLevel;
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
        && queueMaxAmShare != -1.0f) {
      capacitySchedulerConfig.set(PREFIX + queueName +
          ".maximum-am-resource-percent", String.valueOf(queueMaxAmShare));
    }

    if (queueMaxAmShare == -1.0f) {
      capacitySchedulerConfig.set(PREFIX + queueName +
          ".maximum-am-resource-percent", "1.0");
    }
  }

  /**
   * &lt;maxRunningApps&gt;
   * ==> yarn.scheduler.capacity.&lt;queue-name&gt;.maximum-applications.
   * @param queueName
   * @param queue
   */
  private void emitMaxRunningApps(String queueName, FSQueue queue) {
    if (queue.getMaxRunningApps() != MAX_RUNNING_APPS_UNSET
        && queue.getMaxRunningApps() != queueMaxAppsDefault) {
      capacitySchedulerConfig.set(PREFIX + queueName + ".maximum-applications",
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

    long memSize = 0;
    long vCores = 0;
    boolean defined = false;

    if (maxResource == null) {
      if (rawMaxShare.getPercentages() != null) {
        if (clusterResource == null) {
          throw new ConversionException(
              String.format("<maxResources> defined in percentages for" +
                  " queue %s, but cluster resource parameter is not" +
                  " defined via CLI!", queueName));
        }

        ruleHandler.handleMaxCapacityPercentage(queueName);

        double[] percentages = rawMaxShare.getPercentages();
        int memIndex = ResourceUtils.getResourceTypeIndex().get("memory-mb");
        int vcoreIndex = ResourceUtils.getResourceTypeIndex().get("vcores");

        memSize = (long) (percentages[memIndex] *
            clusterResource.getMemorySize());
        vCores = (long) (percentages[vcoreIndex] *
            clusterResource.getVirtualCores());
        defined = true;
      } else {
        throw new PreconditionException(
            "Illegal ConfigurableResource = " + rawMaxShare);
      }
    } else if (isNotUnboundedResource(maxResource)) {
      memSize = maxResource.getMemorySize();
      vCores = maxResource.getVirtualCores();
      defined = true;
    }

    if (defined) {
      capacitySchedulerConfig.set(PREFIX + queueName + ".maximum-capacity",
          String.format("[memory=%d, vcores=%d]", memSize, vCores));
    }
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
  private void emitAutoCreateChildQueue(String queueName) {
    if (autoCreateChildQueues) {
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
    String policy = queue.getPolicy().getName();

    switch (policy) {
    case FairSharePolicy.NAME:
      capacitySchedulerConfig.set(PREFIX + queueName
          + ".ordering-policy", FairSharePolicy.NAME);
      fifoOrFairSharePolicyUsed = true;
      break;
    case FifoPolicy.NAME:
      capacitySchedulerConfig.set(PREFIX + queueName
          + ".ordering-policy", FifoPolicy.NAME);
      fifoOrFairSharePolicyUsed = true;
      break;
    case DominantResourceFairnessPolicy.NAME:
      // DRF is not supported on a queue level,
      // it has to be global
      drfPolicyUsedOnQueueLevel = true;
      break;
    default:
      throw new ConversionException("Unexpected ordering policy " +
          "on queue " + queueName + ": " + policy);
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
    Map<String, Capacity> capacities = getCapacities(totalWeight, children);
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

  private Map<String, Capacity> getCapacities(int totalWeight,
      List<FSQueue> children) {
    final BigDecimal hundred = new BigDecimal(100).setScale(3);

    if (children.size() == 0) {
      return new HashMap<>();
    } else if (children.size() == 1) {
      Map<String, Capacity> capacity = new HashMap<>();
      String queueName = children.get(0).getName();
      capacity.put(queueName, Capacity.newCapacity(hundred));

      return capacity;
    } else {
      Map<String, Capacity> capacities = new HashMap<>();
      Map<String, BigDecimal> bdCapacities = new HashMap<>();

      MutableBoolean needVerifySum = new MutableBoolean(true);
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

            // <minResources> defined?
            if (Resources.none().compareTo(queue.getMinShare()) != 0) {
              needVerifySum.setFalse();

              /* TODO: Needs discussion.
               *
               * Probably it's not entirely correct this way!
               * Eg. root.queue1 in FS translates to 33%
               * capacity, but minResources is defined as 1vcore,8GB
               * which is less than 33%.
               *
               * Therefore max(calculatedCapacity, minResource) is
               * more sound.
               */
              Resource minShare = queue.getMinShare();
              // TODO: in Phase-2, we have to deal with other resources as well
              String capacity = String.format("[memory=%d,vcores=%d]",
                  minShare.getMemorySize(), minShare.getVirtualCores());
              capacities.put(queue.getName(), Capacity.newCapacity(capacity));
            } else {
              capacities.put(queue.getName(), Capacity.newCapacity(pct));
              bdCapacities.put(queue.getName(), pct);
            }
          });

      if (needVerifySum.isTrue()) {
        BigDecimal totalPct = new BigDecimal(0);
        for (Map.Entry<String, BigDecimal> entry : bdCapacities.entrySet()) {
          totalPct = totalPct.add(entry.getValue());
        }

        // fix last value if total != 100.000
        if (!totalPct.equals(hundred)) {
          BigDecimal tmp = new BigDecimal(0);
          for (int i = 0; i < children.size() - 2; i++) {
            tmp = tmp.add(bdCapacities.get(children.get(i).getQueueName()));
          }

          String lastQueue = children.get(children.size() - 1).getName();
          BigDecimal corrected = hundred.subtract(tmp);
          capacities.put(lastQueue, Capacity.newCapacity(corrected));
        }
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

  /*
   * Represents a queue capacity in either percentage
   * or in absolute resources
   */
  private static class Capacity {
    private BigDecimal percentage;
    private String absoluteResource;

    public static Capacity newCapacity(BigDecimal pct) {
      Capacity capacity = new Capacity();
      capacity.percentage = pct;
      capacity.absoluteResource = null;

      return capacity;
    }

    public static Capacity newCapacity(String absoluteResource) {
      Capacity capacity = new Capacity();
      capacity.percentage = null;
      capacity.absoluteResource = absoluteResource;

      return capacity;
    }

    @Override
    public String toString() {
      if (percentage != null) {
        return percentage.toString();
      } else {
        return absoluteResource;
      }
    }
  }

}
