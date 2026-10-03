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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Capacity checks run while a queue is built from configuration (legacy queue
 * mode type and sum checks, and configured min/max resource checks).
 * Every method returns null when the check passes, or the exact message of the
 * exception the caller throws when it fails, so a validator working on resolved
 * configuration can share the same implementation as the queue constructors.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueCapacityChecks {
  /**
   * Tolerance used when comparing the sum of children capacities with 0 or 1.
   */
  public static final float CAPACITY_SUM_PRECISION = 0.0005f;

  private QueueCapacityChecks() {}

  /**
   * The capacity configuration type of one label in legacy queue mode.
   * @param absoluteResource whether the capacity of the label is configured
   *                         as an absolute resource
   * @return ABSOLUTE_RESOURCE or PERCENTAGE
   */
  public static CapacityConfigType capacityConfigTypeOf(
      boolean absoluteResource) {
    return absoluteResource ? CapacityConfigType.ABSOLUTE_RESOURCE
        : CapacityConfigType.PERCENTAGE;
  }

  /**
   * The capacity configuration type of one label outside legacy queue mode:
   * ABSOLUTE_RESOURCE when every resource is absolute, otherwise PERCENTAGE
   * (percentage, weight and mixed vectors).
   * @param capacityVector the configured capacity vector of the label
   * @return ABSOLUTE_RESOURCE or PERCENTAGE
   */
  public static CapacityConfigType capacityConfigTypeOf(
      QueueCapacityVector capacityVector) {
    Set<ResourceUnitCapacityType> definedCapacityTypes =
        capacityVector.getDefinedCapacityTypes();
    if (definedCapacityTypes.size() == 1 && definedCapacityTypes.iterator()
        .next() == ResourceUnitCapacityType.ABSOLUTE) {
      return CapacityConfigType.ABSOLUTE_RESOURCE;
    }
    return CapacityConfigType.PERCENTAGE;
  }

  /**
   * The weight a capacity vector defines when it only has weights and every
   * resource has the same one, for example 5w == [memory=5w, vcores=5w].
   * Queue setup stores this weight in place of the configured weight.
   * @param capacityVector the configured capacity vector of the label
   * @return the common weight, or null if the vector does not define one
   */
  public static Float uniformWeightOf(QueueCapacityVector capacityVector) {
    Set<ResourceUnitCapacityType> definedCapacityTypes =
        capacityVector.getDefinedCapacityTypes();
    if (definedCapacityTypes.size() != 1 || definedCapacityTypes.iterator()
        .next() != ResourceUnitCapacityType.WEIGHT) {
      return null;
    }
    Set<Double> weights = new HashSet<>();
    for (String resourceName : capacityVector.getResourceNames()) {
      weights.add(capacityVector.getResource(resourceName).getResourceValue());
    }
    return weights.size() == 1 ? weights.iterator().next().floatValue() : null;
  }

  /**
   * Capacity related values of one queue for one node label, as seen by the
   * legacy queue mode capacity type checks.
   */
  public static final class LabelCapacity {
    /** Values reported by QueueCapacities for a label it does not know. */
    public static final LabelCapacity UNSET = new LabelCapacity(0f, -1f, false);

    private final float capacity;
    private final float weight;
    private final boolean absolute;

    /**
     * @param capacity relative capacity in the range [0, 1]
     * @param weight configured weight, -1 when no weight is configured
     * @param absolute whether the capacity of the label is configured as an
     *                 absolute resource
     */
    public LabelCapacity(float capacity, float weight, boolean absolute) {
      this.capacity = capacity;
      this.weight = weight;
      this.absolute = absolute;
    }

    public float getCapacity() {
      return capacity;
    }

    public float getWeight() {
      return weight;
    }

    public boolean isAbsolute() {
      return absolute;
    }
  }

  /**
   * Per label capacity values of one queue.
   */
  public static final class QueueCapacityInput {
    private final String queuePath;
    private final Map<String, LabelCapacity> byLabel;

    public QueueCapacityInput(String queuePath,
        Map<String, LabelCapacity> byLabel) {
      this.queuePath = queuePath;
      this.byLabel = Collections.unmodifiableMap(new HashMap<>(byLabel));
    }

    public String getQueuePath() {
      return queuePath;
    }

    /**
     * @param label node label
     * @return the values of the label, or {@link LabelCapacity#UNSET}
     */
    public LabelCapacity get(String label) {
      LabelCapacity labelCapacity = byLabel.get(label);
      return labelCapacity == null ? LabelCapacity.UNSET : labelCapacity;
    }
  }

  /**
   * C05: the raw capacity of the root queue must be 100.
   *
   * @param queueName short name of the queue
   * @param isRoot whether the queue is the root queue
   * @param rawCapacity unlabeled configured capacity
   * @return null if valid, otherwise the error message
   */
  public static String checkRootCapacity(String queueName, boolean isRoot,
      float rawCapacity) {
    if (isRoot &&
        (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      return "Illegal " +
          "capacity of " + rawCapacity + " for queue " + queueName +
          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE;
    }
    return null;
  }

  /**
   * C06: the configured max resource of a queue must not be greater than the
   * configured max resource of its parent, if the latter is set.
   *
   * @param queuePath full path of the queue
   * @param maxResource configured max resource of the queue for a label
   * @param parentMaxResource configured max resource of the parent for the
   *                          same label
   * @param clusterResource cluster resource used for the comparison
   * @param resourceCalculator scheduler resource calculator
   * @return null if valid, otherwise the error message
   */
  public static String checkMaxResourceWithinParent(String queuePath,
      Resource maxResource, Resource parentMaxResource,
      Resource clusterResource, ResourceCalculator resourceCalculator) {
    if (isGreaterThanSetLimit(resourceCalculator, clusterResource,
        maxResource, parentMaxResource)) {
      return "Max resource configuration "
          + maxResource + " is greater than parents max value:"
          + parentMaxResource + " in queue:" + queuePath;
    }
    return null;
  }

  /**
   * If the max resource of a queue is not set, but its min resource and the
   * max resource of its parent are, the queue inherits the max resource of its
   * parent before C07 is evaluated.
   *
   * @param minResource configured min resource of the queue
   * @param maxResource configured max resource of the queue
   * @param parentMaxResource configured max resource of the parent
   * @return the max resource C07 compares with
   */
  public static Resource inheritMaxResourceFromParent(Resource minResource,
      Resource maxResource, Resource parentMaxResource) {
    if (maxResource.equals(Resources.none()) &&
        !minResource.equals(Resources.none()) &&
        !parentMaxResource.equals(Resources.none())) {
      return Resources.clone(parentMaxResource);
    }
    return maxResource;
  }

  /**
   * C07: the configured min resource of a queue must not be greater than its
   * max resource, if the latter is set.
   *
   * @param queuePath full path of the queue
   * @param minResource configured min resource of the queue for a label
   * @param maxResource max resource of the queue for the same label, after
   *                    {@link #inheritMaxResourceFromParent}
   * @param clusterResource cluster resource used for the comparison
   * @param resourceCalculator scheduler resource calculator
   * @return null if valid, otherwise the error message
   */
  public static String checkMinResourceWithinMax(String queuePath,
      Resource minResource, Resource maxResource, Resource clusterResource,
      ResourceCalculator resourceCalculator) {
    if (isGreaterThanSetLimit(resourceCalculator, clusterResource,
        minResource, maxResource)) {
      return "Min resource configuration "
          + minResource + " is greater than its max value:" + maxResource
          + " in queue:" + queuePath;
    }
    return null;
  }

  private static boolean isGreaterThanSetLimit(
      ResourceCalculator resourceCalculator, Resource clusterResource,
      Resource resource, Resource limit) {
    return !limit.equals(Resources.none()) && Resources.greaterThan(
        resourceCalculator, clusterResource, resource, limit);
  }

  /**
   * C08: in legacy queue mode a non-root queue must use the same capacity
   * config type for all its configured labels.
   *
   * @param queuePath full path of the queue
   * @param isRoot whether the queue is the root queue
   * @param legacyQueueMode whether legacy queue mode is enabled
   * @param establishedType type of the first configured label
   * @param labelType type of the label being checked
   * @return null if valid, otherwise the error message
   */
  public static String checkCapacityConfigTypeConsistent(String queuePath,
      boolean isRoot, boolean legacyQueueMode,
      CapacityConfigType establishedType, CapacityConfigType labelType) {
    if (!isRoot
        && !establishedType.equals(labelType) &&
        legacyQueueMode) {
      return "Queue '" + queuePath
          + "' should use either percentage based capacity"
          + " configuration or absolute resource.";
    }
    return null;
  }

  /**
   * C09: in legacy queue mode the given queues must not mix percentage,
   * weight and absolute resource capacities. The check is order dependent:
   * an absolute resource capacity clears the percentage flag collected from
   * the previously visited labels and queues, a 0% capacity does not count as
   * percentage, and it is skipped when the first queue is root.
   *
   * @param parentQueuePath full path of the parent the check runs for
   * @param labels labels of the parent, in iteration order
   * @param queues the checked queues, in order
   * @return null if valid, otherwise the error message
   */
  public static String checkCapacityTypesNotMixed(String parentQueuePath,
      Collection<String> labels, List<QueueCapacityInput> queues) {
    boolean percentageIsSet = false;
    boolean weightIsSet = false;
    boolean absoluteMinResSet = false;

    StringBuilder diagMsg = new StringBuilder();

    for (QueueCapacityInput queue : queues) {
      for (String nodeLabel : labels) {
        LabelCapacity labelCapacity = queue.get(nodeLabel);
        if (labelCapacity.getCapacity() > 0) {
          percentageIsSet = true;
        }
        // By default weight is set to -1, so >= 0 is enough.
        if (labelCapacity.getWeight() >= 0) {
          weightIsSet = true;
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses weight mode}. ");
        }
        if (labelCapacity.isAbsolute()) {
          absoluteMinResSet = true;
          // When absolute resource is configured, capacity is calculated for
          // UI/metrics purposes, so the percentage flag is unset.
          percentageIsSet = false;
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses absolute mode}. ");
        }
        if (percentageIsSet) {
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses percentage mode}. ");
        }
      }
    }
    // Root always reports 100 as capacity, so its config is not checked.
    if (!queues.isEmpty() &&
        !queues.get(0).getQueuePath().equals(
            CapacitySchedulerConfiguration.ROOT) &&
        (percentageIsSet ? 1 : 0) + (weightIsSet ? 1 : 0)
            + (absoluteMinResSet ? 1 : 0) > 1) {
      return "Parent queue '" + parentQueuePath
          + "' have children queue used mixed of "
          + " weight mode, percentage and absolute mode, it is not allowed, please "
          + "double check, details:" + diagMsg.toString();
    }
    return null;
  }

  /**
   * Capacity type of the given queues as a group, as used by the legacy queue
   * mode checks; only meaningful if {@link #checkCapacityTypesNotMixed}
   * passes.
   *
   * @param labels labels of the parent
   * @param queues the classified queues
   * @return WEIGHT if any queue uses weights or there are no queues,
   *         ABSOLUTE_RESOURCE if any queue uses absolute resources, otherwise
   *         PERCENT
   */
  public static QueueCapacityType getCapacityConfigurationType(
      Collection<String> labels, List<QueueCapacityInput> queues) {
    boolean weightIsSet = false;
    boolean absoluteMinResSet = false;
    for (QueueCapacityInput queue : queues) {
      for (String nodeLabel : labels) {
        LabelCapacity labelCapacity = queue.get(nodeLabel);
        if (labelCapacity.getWeight() >= 0) {
          weightIsSet = true;
        }
        if (labelCapacity.isAbsolute()) {
          absoluteMinResSet = true;
        }
      }
    }

    if (weightIsSet || queues.isEmpty()) {
      return QueueCapacityType.WEIGHT;
    } else if (absoluteMinResSet) {
      return QueueCapacityType.ABSOLUTE_RESOURCE;
    } else {
      return QueueCapacityType.PERCENT;
    }
  }

  /**
   * C10: in legacy queue mode, when either a non-root parent or its children
   * use absolute resources, both must.
   *
   * @param parentQueuePath full path of the parent
   * @param parentType capacity type of the parent
   * @param childrenType capacity type of the children
   * @return null if valid, otherwise the error message
   */
  public static String checkAbsoluteResourceUsedByParentAndChildren(
      String parentQueuePath, QueueCapacityType parentType,
      QueueCapacityType childrenType) {
    if ((childrenType == QueueCapacityType.ABSOLUTE_RESOURCE
        || parentType == QueueCapacityType.ABSOLUTE_RESOURCE)
        && childrenType != parentType
        && !parentQueuePath.equals(CapacitySchedulerConfiguration.ROOT)) {
      return "Parent=" + parentQueuePath
          + ": When absolute minResource is used, we must make sure both "
          + "parent and child all use absolute minResource";
    }
    return null;
  }

  /**
   * C11: in legacy queue mode with absolute resources, the configured min
   * resource of a parent, if set, must not be less than the sum of the
   * configured min resources of its children.
   *
   * @param parentQueueName short name of the parent
   * @param parentMinResource configured min resource of the parent for a label
   * @param childrenMinResources configured min resources of the children for
   *                             the same label
   * @param resourceByLabel resource of the label, used for the comparison
   * @param resourceCalculator scheduler resource calculator
   * @return null if valid, otherwise the error message
   */
  public static String checkChildrenMinResourceWithinParent(
      String parentQueueName, Resource parentMinResource,
      List<Resource> childrenMinResources, Resource resourceByLabel,
      ResourceCalculator resourceCalculator) {
    Resource minRes = Resources.createResource(0, 0);
    for (Resource childMinResource : childrenMinResources) {
      Resources.addTo(minRes, childMinResource);
    }
    if (!parentMinResource.equals(Resources.none()) && Resources.lessThan(
        resourceCalculator, resourceByLabel, parentMinResource, minRes)) {
      return "Parent Queues" + " capacity: " + parentMinResource
          + " is less than" + " to its children:" + minRes
          + " for queue:" + parentQueueName;
    }
    return null;
  }

  /**
   * C12, C13, C14: in legacy queue mode with percentage children, the sum of
   * the children capacities for a label must be 1, or 0; 0 is only allowed
   * under a percentage parent with zero capacity for the label or with
   * allow-zero-capacity-sum, and 1 is not allowed under a percentage parent
   * with zero capacity unless allow-zero-capacity-sum is set.
   *
   * @param parentQueueName short name of the parent
   * @param label node label of the parent
   * @param parentType capacity type of the parent
   * @param parentCapacity relative capacity of the parent for the label
   * @param allowZeroCapacitySum allow-zero-capacity-sum of the parent
   * @param children capacity values of the children
   * @return null if valid, otherwise the error message
   */
  public static String checkChildrenCapacitySum(String parentQueueName,
      String label, QueueCapacityType parentType, float parentCapacity,
      boolean allowZeroCapacitySum, List<QueueCapacityInput> children) {
    float childrenPctSum = 0;
    for (QueueCapacityInput child : children) {
      childrenPctSum += child.get(label).getCapacity();
    }

    if (Math.abs(1 - childrenPctSum) > CAPACITY_SUM_PRECISION) {
      if (Math.abs(childrenPctSum) > CAPACITY_SUM_PRECISION) {
        // C12: the sum is neither 0 nor 1
        return "Illegal" + " capacity sum of " + childrenPctSum
            + " for children of queue " + parentQueueName + " for label="
            + label + ". It should be either 0 or 1.0";
      } else if (parentType == QueueCapacityType.PERCENT
          && (Math.abs(parentCapacity) > CAPACITY_SUM_PRECISION)
          && (!allowZeroCapacitySum)) {
        // C13
        return "Illegal" + " capacity sum of " + childrenPctSum
            + " for children of queue " + parentQueueName
            + " for label=" + label
            + ". It is set to 0, but parent percent != 0, and "
            + "doesn't allow children capacity to set to 0";
      }
    } else if (parentType == QueueCapacityType.PERCENT
        && Math.abs(parentCapacity) <= 0f
        && !allowZeroCapacitySum) {
      // C14
      return "Illegal" + " capacity sum of " + childrenPctSum
          + " for children of queue " + parentQueueName + " for label="
          + label + ". queue=" + parentQueueName
          + " has zero capacity, but child"
          + "queues have positive capacities";
    }
    return null;
  }
}
