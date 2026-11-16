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
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;

/**
 * The single definition of a per-queue configuration property: its key, the
 * queue kinds it applies to, and how its value is parsed and defaulted.
 * <p>
 * {@link #read(Function, QueuePath, String, Object)} is exactly what the
 * corresponding {@code CapacitySchedulerConfiguration} getter does, given a
 * function that returns the (variable substituted) value of a key. The
 * getters call it with their own configuration, and the resolver calls it
 * with the configuration snapshot, overlaid with the template entries a
 * dynamic queue receives.
 * <p>
 * Properties whose value is a map of the keys under a prefix, for example
 * user weights or template entries, take those entries as the argument of
 * {@code read} and apply the function to each value.
 * @param <T> type of the value
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueProperty<T> {

  /** Queue kinds, as decided when the queue hierarchy is parsed. */
  public enum Kind {
    ROOT,
    /** A parent queue, including a managed (v1 auto-creation) parent. */
    PARENT,
    LEAF,
    /** A reservable leaf in the configuration, which becomes a PlanQueue. */
    PLAN,
    /** The default ReservationQueue of a PlanQueue. */
    RESERVATION
  }

  /** Reads a value the way the corresponding getter does. */
  interface Reader<T> {
    T read(Function<String, String> conf, String key, QueuePath queue,
        String label, Object argument);
  }

  private final String name;
  private final boolean labeled;
  private final Set<Kind> appliesTo;
  private final Reader<T> reader;

  QueueProperty(String name, boolean labeled, Set<Kind> appliesTo,
      Reader<T> reader) {
    this.name = name;
    this.labeled = labeled;
    this.appliesTo = Collections.unmodifiableSet(EnumSet.copyOf(appliesTo));
    this.reader = reader;
  }

  /**
   * Returns the key suffix, relative to the queue prefix or, for a labeled
   * property, to the node label prefix of the queue.
   * @return the key suffix
   */
  public String getName() {
    return name;
  }

  /**
   * Whether the property is set per node label. A labeled property read with
   * the empty label uses the key under the queue prefix.
   * @return true for node label scoped properties
   */
  public boolean isLabeled() {
    return labeled;
  }

  public Set<Kind> getAppliesTo() {
    return appliesTo;
  }

  /**
   * Returns the full key of the property.
   * @param queue the queue path
   * @param label the node label, ignored for unlabeled properties
   * @return the full key
   */
  public String getKey(QueuePath queue, String label) {
    return getKey(QueuePrefixes.getQueuePrefix(queue), label);
  }

  String getKey(String queuePrefix, String label) {
    if (!labeled || label.equals(CommonNodeLabelsManager.NO_LABEL)) {
      return queuePrefix + name;
    }
    return queuePrefix + CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS
        + CapacitySchedulerConfiguration.DOT + label
        + CapacitySchedulerConfiguration.DOT + name;
  }

  /**
   * Reads the unlabeled value without an argument.
   * @param conf returns the value of a key, or {@code null} when absent
   * @param queue the queue path
   * @return the value
   */
  public T read(Function<String, String> conf, QueuePath queue) {
    return read(conf, queue, CommonNodeLabelsManager.NO_LABEL, null);
  }

  /**
   * Reads a value.
   * @param conf returns the value of a key, or {@code null} when absent
   * @param queue the queue path
   * @param label the node label, ignored for unlabeled properties
   * @param argument the property specific argument documented in
   *                 {@link QueueProperties}, or {@code null} for its default
   * @return the value
   */
  public T read(Function<String, String> conf, QueuePath queue, String label,
      Object argument) {
    return reader.read(conf, getKey(queue, label), queue, label, argument);
  }

  T read(Function<String, String> conf, String queuePrefix, QueuePath queue,
      String label, Object argument) {
    return reader.read(conf, getKey(queuePrefix, label), queue, label,
        argument);
  }

  @Override
  public String toString() {
    return labeled ? "accessible-node-labels.<label>." + name : name;
  }
}
