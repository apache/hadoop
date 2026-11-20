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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperty.Kind;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * The resolved configuration of one queue: every property that applies to
 * the queue's kind, for every configured node label of labeled properties,
 * with the source of each value. Created by {@link QueueConfigResolver}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ResolvedQueueConfig {
  private final QueuePath queuePath;
  private final Kind kind;
  private final boolean dynamic;
  private final Set<String> configuredNodeLabels;
  private final Map<QueueProperty<?>, Resolved<?>> values =
      new LinkedHashMap<>();
  private final Map<QueueProperty<?>, Map<String, Resolved<?>>> labeledValues =
      new LinkedHashMap<>();
  /** Whether a default application lifetime is set here or above. */
  private boolean defaultLifetimeSpecified;

  ResolvedQueueConfig(QueuePath queuePath, Kind kind, boolean dynamic,
      Set<String> configuredNodeLabels) {
    this.queuePath = queuePath;
    this.kind = kind;
    this.dynamic = dynamic;
    this.configuredNodeLabels = configuredNodeLabels;
  }

  public QueuePath getQueuePath() {
    return queuePath;
  }

  public Kind getKind() {
    return kind;
  }

  /**
   * Whether the queue is a dynamic queue, which receives template entries.
   * @return true for v1 auto-created and v2 dynamic queues
   */
  public boolean isDynamic() {
    return dynamic;
  }

  /**
   * Returns the node labels of the queue's labeled keys, always including
   * the empty label; the labels labeled properties are resolved for.
   * @return the configured node labels
   */
  public Set<String> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  /**
   * Returns an unlabeled value, or the empty label's value of a labeled
   * property.
   * @param property the property
   * @param <T> type of the value
   * @return the resolved value, or {@code null} when the property does not
   *         apply to this queue
   */
  public <T> Resolved<T> get(QueueProperty<T> property) {
    return get(property, CommonNodeLabelsManager.NO_LABEL);
  }

  /**
   * Returns the value of a property for a node label.
   * @param property the property
   * @param label the node label, ignored for unlabeled properties
   * @param <T> type of the value
   * @return the resolved value, or {@code null} when the property does not
   *         apply to this queue or the label is not configured for it
   */
  @SuppressWarnings("unchecked")
  public <T> Resolved<T> get(QueueProperty<T> property, String label) {
    if (!property.isLabeled()) {
      return (Resolved<T>) values.get(property);
    }
    Map<String, Resolved<?>> byLabel = labeledValues.get(property);
    return byLabel == null ? null : (Resolved<T>) byLabel.get(label);
  }

  /**
   * Returns every resolved value by its full key, one entry per key. Several
   * properties read the same key; the entry holds the reading a user wrote:
   * the percentage, weight ({@code <n>w}), absolute resource or capacity
   * vector of a capacity or maximum capacity, never an internal reading such
   * as the absolute classification. Derived values and template maps, which
   * have no single key, use the queue prefix followed by the property name.
   * @return full key to resolved value
   */
  public Map<String, Resolved<?>> explain() {
    Map<String, Resolved<?>> explained = new LinkedHashMap<>();
    for (Map.Entry<QueueProperty<?>, Resolved<?>> entry : values.entrySet()) {
      String key = entry.getKey().getKey(queuePath,
          CommonNodeLabelsManager.NO_LABEL);
      if (entry.getKey() == QueueProperties.CAPACITY) {
        putReading(explained, key, capacityReading(
            CommonNodeLabelsManager.NO_LABEL));
      } else if (entry.getKey() == QueueProperties.MAXIMUM_CAPACITY) {
        putReading(explained, key, maximumCapacityReading(
            CommonNodeLabelsManager.NO_LABEL));
      }
      if (!explained.containsKey(key)) {
        explained.put(key, entry.getValue());
      }
    }
    for (String label : configuredNodeLabels) {
      if (!label.equals(CommonNodeLabelsManager.NO_LABEL)) {
        putReading(explained,
            QueueProperties.LABELED_CAPACITY.getKey(queuePath, label),
            capacityReading(label));
        putReading(explained,
            QueueProperties.LABELED_MAXIMUM_CAPACITY.getKey(queuePath, label),
            maximumCapacityReading(label));
      }
    }
    for (Map.Entry<QueueProperty<?>, Map<String, Resolved<?>>> entry
        : labeledValues.entrySet()) {
      for (Map.Entry<String, Resolved<?>> value
          : entry.getValue().entrySet()) {
        String key = entry.getKey().getKey(queuePath, value.getKey());
        if (!explained.containsKey(key)) {
          explained.put(key, value.getValue());
        }
      }
    }
    return Collections.unmodifiableMap(explained);
  }

  private static void putReading(Map<String, Resolved<?>> explained,
      String key, Resolved<?> reading) {
    if (reading != null && !explained.containsKey(key)) {
      explained.put(key, reading);
    }
  }

  /** The capacity as configured: percentage, weight, absolute or vector. */
  private Resolved<?> capacityReading(String label) {
    boolean noLabel = label.equals(CommonNodeLabelsManager.NO_LABEL);
    Resolved<Float> percentage = get(noLabel ? QueueProperties.CAPACITY
        : QueueProperties.LABELED_CAPACITY, label);
    Resolved<QueueCapacityVector> vector =
        get(QueueProperties.CAPACITY_VECTOR, label);
    if (vector == null || vector.isFailed() || vector.getValue().isEmpty()
        || percentage == null || percentage.isFailed()) {
      return percentage != null ? percentage : vector;
    }
    Set<ResourceUnitCapacityType> types =
        vector.getValue().getDefinedCapacityTypes();
    ResourceUnitCapacityType type =
        types.size() == 1 ? types.iterator().next() : null;
    if (type == ResourceUnitCapacityType.ABSOLUTE) {
      Resolved<Resource> minimum = get(QueueProperties.MINIMUM_RESOURCE, label);
      if (minimum == null || minimum.isFailed()
          || Resources.none().equals(minimum.getValue())) {
        return percentage;
      }
      return minimum;
    }
    if (type == ResourceUnitCapacityType.PERCENTAGE) {
      return samePercentage(vector, percentage) ? percentage : vector;
    }
    if (type == ResourceUnitCapacityType.WEIGHT) {
      Resolved<Float> weight = get(QueueProperties.CAPACITY_WEIGHT, label);
      if (weight != null && !weight.isFailed() && weight.getValue() >= 0) {
        return Resolved.of(weight.getValue() + "w", weight.getSource(),
            weight.getSourceDetail());
      }
    }
    return vector;
  }

  /**
   * Whether a plain percentage was configured: a vector written in vector
   * format reads as 0% or 100% through the percentage reader.
   */
  private static boolean samePercentage(Resolved<QueueCapacityVector> vector,
      Resolved<Float> percentage) {
    return Math.abs(vector.getValue().getMemory() - percentage.getValue())
        < 1e-4;
  }

  /** The maximum capacity as configured: percentage, absolute or vector. */
  private Resolved<?> maximumCapacityReading(String label) {
    boolean noLabel = label.equals(CommonNodeLabelsManager.NO_LABEL);
    Resolved<Float> percentage = get(noLabel
        ? QueueProperties.MAXIMUM_CAPACITY
        : QueueProperties.LABELED_MAXIMUM_CAPACITY, label);
    Resolved<QueueCapacityVector> vector =
        get(QueueProperties.MAXIMUM_CAPACITY_VECTOR, label);
    if (vector == null || vector.isFailed() || vector.getValue().isEmpty()
        || percentage == null || percentage.isFailed()) {
      return percentage != null ? percentage : vector;
    }
    Set<ResourceUnitCapacityType> types =
        vector.getValue().getDefinedCapacityTypes();
    ResourceUnitCapacityType type =
        types.size() == 1 ? types.iterator().next() : null;
    if (type == ResourceUnitCapacityType.ABSOLUTE) {
      Resolved<Resource> maximum = get(QueueProperties.MAXIMUM_RESOURCE, label);
      if (maximum == null || maximum.isFailed()
          || Resources.none().equals(maximum.getValue())) {
        // An unset maximum is an empty absolute vector
        return percentage;
      }
      return maximum;
    }
    if (type == ResourceUnitCapacityType.PERCENTAGE
        && samePercentage(vector, percentage)) {
      return percentage;
    }
    return vector;
  }

  <T> void put(QueueProperty<T> property, String label, Resolved<T> value) {
    if (!property.isLabeled()) {
      values.put(property, value);
      return;
    }
    Map<String, Resolved<?>> byLabel = labeledValues.get(property);
    if (byLabel == null) {
      byLabel = new LinkedHashMap<>();
      labeledValues.put(property, byLabel);
    }
    byLabel.put(label, value);
  }

  /**
   * Whether a default application lifetime is set for this queue or for one
   * of its ancestors; such a default is inherited by the queue.
   * @return true if a default application lifetime is set in the hierarchy
   */
  public boolean isDefaultLifetimeSpecified() {
    return defaultLifetimeSpecified;
  }

  void setDefaultLifetimeSpecified(boolean specified) {
    this.defaultLifetimeSpecified = specified;
  }

  @Override
  public String toString() {
    return queuePath + " " + kind + (dynamic ? " (dynamic)" : "");
  }
}
