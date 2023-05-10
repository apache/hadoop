/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

/**
 * Provides a fluent API to assert resource and capacity attributes of queues.
 */
class QueueAssertionBuilder {
  private static final String EFFECTIVE_MAX_RES_INFO = "Effective Maximum Resource";
  private static final BiFunction<QueueResourceQuotas, String, Resource> EFFECTIVE_MAX_RES =
      QueueResourceQuotas::getEffectiveMaxResource;

  private static final String EFFECTIVE_MIN_RES_INFO = "Effective Minimum Resource";
  private static final BiFunction<QueueResourceQuotas, String, Resource> EFFECTIVE_MIN_RES =
      QueueResourceQuotas::getEffectiveMinResource;

  private static final String CAPACITY_INFO = "Capacity";
  private static final BiFunction<QueueCapacities, String, Float> CAPACITY =
      QueueCapacities::getCapacity;

  private static final String ABS_CAPACITY_INFO = "Absolute Capacity";
  private static final BiFunction<QueueCapacities, String, Float> ABS_CAPACITY =
      QueueCapacities::getAbsoluteCapacity;

  private static final String ASSERTION_ERROR_MESSAGE =
      "'%s' of queue '%s' does not match %f for label %s";
  private static final String RESOURCE_ASSERTION_ERROR_MESSAGE =
      "'%s' of queue '%s' does not match %s for label %s";
  private final CapacityScheduler cs;

  QueueAssertionBuilder(CapacityScheduler cs) {
    this.cs = cs;
  }

  public class QueueAssertion {
    private final String queuePath;
    private final List<QueueAssertion.ValueAssertion> assertions = new ArrayList<>();

    QueueAssertion(String queuePath) {
      this.queuePath = queuePath;
    }


    public QueueAssertion withQueue(String queuePath) {
      return QueueAssertionBuilder.this.withQueue(queuePath);
    }

    public QueueAssertionBuilder build() {
      return QueueAssertionBuilder.this.build();
    }

    public QueueAssertion assertEffectiveMaxResource(Resource expected) {
      ValueAssertion valueAssertion = new ValueAssertion(expected);
      valueAssertion.withResourceSupplier(EFFECTIVE_MAX_RES, EFFECTIVE_MAX_RES_INFO);
      assertions.add(valueAssertion);

      return this;
    }

    public QueueAssertion assertEffectiveMinResource(Resource expected, String label) {
      ValueAssertion valueAssertion = new ValueAssertion(expected);
      valueAssertion.withResourceSupplier(EFFECTIVE_MIN_RES, EFFECTIVE_MIN_RES_INFO);
      assertions.add(valueAssertion);
      valueAssertion.label = label;

      return this;
    }

    public QueueAssertion assertEffectiveMinResource(Resource expected) {
      return assertEffectiveMinResource(expected, NO_LABEL);
    }

    public QueueAssertion assertCapacity(double expected) {
      ValueAssertion valueAssertion = new ValueAssertion(expected);
      valueAssertion.withCapacitySupplier(CAPACITY, CAPACITY_INFO);
      assertions.add(valueAssertion);

      return this;
    }

    public QueueAssertion assertAbsoluteCapacity(double expected) {
      ValueAssertion valueAssertion = new ValueAssertion(expected);
      valueAssertion.withCapacitySupplier(ABS_CAPACITY, ABS_CAPACITY_INFO);
      assertions.add(valueAssertion);

      return this;
    }

    private class ValueAssertion {
      private double expectedValue = 0;
      private Resource expectedResource = null;
      private String assertionType;
      private Supplier<Float> valueSupplier;
      private Supplier<Resource> resourceSupplier;
      private String label = "";

      ValueAssertion(double expectedValue) {
        this.expectedValue = expectedValue;
      }

      ValueAssertion(Resource expectedResource) {
        this.expectedResource = expectedResource;
      }

      public void setLabel(String label) {
        this.label = label;
      }

      public void withResourceSupplier(
          BiFunction<QueueResourceQuotas, String, Resource> assertion, String messageInfo) {
        CSQueue queue = cs.getQueue(queuePath);
        if (queue == null) {
          Assert.fail("Queue " + queuePath + " is not found");
        }

        assertionType = messageInfo;
        resourceSupplier = () -> assertion.apply(queue.getQueueResourceQuotas(), label);
      }

      public void withCapacitySupplier(
          BiFunction<QueueCapacities, String, Float> assertion, String messageInfo) {
        CSQueue queue = cs.getQueue(queuePath);
        if (queue == null) {
          Assert.fail("Queue " + queuePath + " is not found");
        }
        assertionType = messageInfo;
        valueSupplier = () -> assertion.apply(queue.getQueueCapacities(), label);
      }
    }

  }

  private final Map<String, QueueAssertion> assertions = new LinkedHashMap<>();

  public QueueAssertionBuilder build() {
    return this;
  }

  /**
   * Creates a new assertion group for a specific queue.
   * @param queuePath path of the queue
   * @return queue assertion group
   */
  public QueueAssertion withQueue(String queuePath) {
    assertions.putIfAbsent(queuePath, new QueueAssertion(queuePath));
    return assertions.get(queuePath);
  }

  /**
   * Executes assertions created for all queues.
   */
  public void finishAssertion() {
    for (Map.Entry<String, QueueAssertion> assertionEntry : assertions.entrySet()) {
      for (QueueAssertion.ValueAssertion assertion : assertionEntry.getValue().assertions) {
        if (assertion.resourceSupplier != null) {
          String errorMessage = String.format(RESOURCE_ASSERTION_ERROR_MESSAGE,
              assertion.assertionType, assertionEntry.getKey(),
              assertion.expectedResource.toString(), assertion.label);
          Assert.assertEquals(errorMessage, assertion.expectedResource,
              assertion.resourceSupplier.get());
        } else {
          String errorMessage = String.format(ASSERTION_ERROR_MESSAGE,
              assertion.assertionType, assertionEntry.getKey(), assertion.expectedValue,
              assertion.label);
          Assert.assertEquals(errorMessage, assertion.expectedValue,
              assertion.valueSupplier.get(), EPSILON);
        }
      }
    }
  }

  /**
   * Returns all queues that have defined assertions.
   * @return queue paths
   */
  public Set<String> getQueues() {
    return assertions.keySet();
  }
}
