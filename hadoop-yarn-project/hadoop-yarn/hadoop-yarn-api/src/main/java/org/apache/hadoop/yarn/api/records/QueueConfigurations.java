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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * QueueConfigurations contain information about the configuration percentages
 * of a queue.
 * <p>
 * It includes information such as:
 * <ul>
 *   <li>Capacity of the queue.</li>
 *   <li>Absolute capacity of the queue.</li>
 *   <li>Maximum capacity of the queue.</li>
 *   <li>Absolute maximum capacity of the queue.</li>
 *   <li>Maximum ApplicationMaster resource percentage of the queue.</li>
 * </ul>
 */
public abstract class QueueConfigurations {

  @Public
  @Unstable
  public static QueueConfigurations newInstance(float capacity,
      float absoluteCapacity, float maxCapacity, float absoluteMaxCapacity,
      float maxAMPercentage) {
    QueueConfigurations queueConfigurations =
        Records.newRecord(QueueConfigurations.class);
    queueConfigurations.setCapacity(capacity);
    queueConfigurations.setAbsoluteCapacity(absoluteCapacity);
    queueConfigurations.setMaxCapacity(maxCapacity);
    queueConfigurations.setAbsoluteMaxCapacity(absoluteMaxCapacity);
    queueConfigurations.setMaxAMPercentage(maxAMPercentage);
    return queueConfigurations;
  }

  /**
   * Get the queue capacity.
   *
   * @return the queue capacity
   */
  @Public
  @Unstable
  public abstract float getCapacity();

  /**
   * Set the queue capacity.
   *
   * @param capacity
   *          the queue capacity.
   */
  @Private
  @Unstable
  public abstract void setCapacity(float capacity);

  /**
   * Get the absolute capacity.
   *
   * @return the absolute capacity
   */
  @Public
  @Unstable
  public abstract float getAbsoluteCapacity();

  /**
   * Set the absolute capacity.
   *
   * @param absoluteCapacity
   *          the absolute capacity
   */
  @Private
  @Unstable
  public abstract void setAbsoluteCapacity(float absoluteCapacity);

  /**
   * Get the maximum capacity.
   *
   * @return the maximum capacity
   */
  @Public
  @Unstable
  public abstract float getMaxCapacity();

  /**
   * Set the maximum capacity.
   *
   * @param maxCapacity
   *          the maximum capacity
   */
  @Private
  @Unstable
  public abstract void setMaxCapacity(float maxCapacity);

  /**
   * Get the absolute maximum capacity.
   *
   * @return the absolute maximum capacity
   */
  @Public
  @Unstable
  public abstract float getAbsoluteMaxCapacity();

  /**
   * Set the absolute maximum capacity.
   *
   * @param absoluteMaxCapacity
   *          the absolute maximum capacity
   */
  @Private
  @Unstable
  public abstract void setAbsoluteMaxCapacity(float absoluteMaxCapacity);

  /**
   * Get the maximum AM resource percentage.
   *
   * @return the maximum AM resource percentage
   */
  @Public
  @Unstable
  public abstract float getMaxAMPercentage();

  /**
   * Set the maximum AM resource percentage.
   *
   * @param maxAMPercentage
   *          the maximum AM resource percentage
   */
  @Private
  @Unstable
  public abstract void setMaxAMPercentage(float maxAMPercentage);

  /**
   * Get the effective minimum capacity of queue (from absolute resource).
   *
   * @return minimum resource capability
   */
  @Public
  @Unstable
  public abstract Resource getEffectiveMinCapacity();

  /**
   * Set the effective minimum capacity of queue (from absolute resource).
   *
   * @param capacity
   *          minimum resource capability
   */
  @Private
  @Unstable
  public abstract void setEffectiveMinCapacity(Resource capacity);

  /**
   * Get the effective maximum capacity of queue (from absolute resource).
   *
   * @return maximum resource capability
   */
  @Public
  @Unstable
  public abstract Resource getEffectiveMaxCapacity();

  /**
   * Set the effective maximum capacity of queue (from absolute resource).
   *
   * @param capacity
   *          maximum resource capability
   */
  @Private
  @Unstable
  public abstract void setEffectiveMaxCapacity(Resource capacity);

  /**
   * Get the configured minimum capacity of queue (from absolute resource).
   *
   * @return minimum resource capability
   */
  @Public
  @Unstable
  public abstract Resource getConfiguredMinCapacity();

  /**
   * Set the configured minimum capacity of queue (from absolute resource).
   *
   * @param configuredMinResource
   *          minimum resource capability
   */
  @Public
  @Unstable
  public abstract void setConfiguredMinCapacity(Resource configuredMinResource);

  /**
   * Get the configured maximum capacity of queue (from absolute resource).
   *
   * @return maximum resource capability
   */
  @Public
  @Unstable
  public abstract Resource getConfiguredMaxCapacity();

  /**
   * Set the configured maximum capacity of queue (from absolute resource).
   *
   * @param configuredMaxResource
   *          maximum resource capability
   */
  @Public
  @Unstable
  public abstract void setConfiguredMaxCapacity(Resource configuredMaxResource);
}
