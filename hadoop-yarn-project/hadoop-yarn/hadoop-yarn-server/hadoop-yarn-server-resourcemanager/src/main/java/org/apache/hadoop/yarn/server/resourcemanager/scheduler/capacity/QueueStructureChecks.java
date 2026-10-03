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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Structural checks of the queue hierarchy as parsed from the configuration. Every check
 * returns null when it passes, or the error message when it fails; the caller decides which
 * exception to throw.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueStructureChecks {

  private QueueStructureChecks() {
  }

  /**
   * Decides whether a queue is parsed as a parent queue: it has configured child queues or it
   * is eligible for auto queue creation.
   * @param input the structure of the queue
   * @return true if the queue is parsed as a parent queue
   */
  public static boolean isParent(QueueStructureInput input) {
    return input.getChildCount() != 0 || input.isAutoQueueCreationEnabledParent();
  }

  /**
   * Checks that the root queue is parsed as a parent queue.
   * @param input the structure of the queue
   * @return null if the check passes, otherwise the error message
   */
  public static String checkRootHasChildQueues(QueueStructureInput input) {
    if (input.isRoot() && !isParent(input)) {
      return "Queue configuration missing child queue names for " + input.getQueueName();
    }
    return null;
  }

  /**
   * Checks that only leaf queues are reservable.
   * @param input the structure of the queue
   * @return null if the check passes, otherwise the error message
   */
  public static String checkReservable(QueueStructureInput input) {
    if (input.isReservable() && isParent(input)) {
      return "Only Leaf Queues can be reservable for " + input.getQueuePath();
    }
    return null;
  }

  /**
   * Checks that a newly parsed reservable queue has only the default reservation queue as a
   * child.
   * @param childCount the number of child queues of the newly parsed reservable queue
   * @return null if the check passes, otherwise the error message
   */
  public static String checkPlanQueueChildren(int childCount) {
    if (childCount != 1) {
      return "Reservable Queue should not have sub-queues in the"
          + "configuration expect the default reservation queue";
    }
    return null;
  }

  /**
   * Checks that the leaf queue template of an auto create enabled parent queue (auto queue
   * creation v1) does not configure an absolute minimum resource while the parent queue uses
   * percentage capacities.
   * @param queuePath the path of the managed parent queue
   * @param parentCapacityConfigType the capacity configuration type of the parent queue
   * @param templateMinResource the configured minimum resource of the template for one label
   * @return null if the check passes, otherwise the error message
   */
  public static String checkLeafQueueTemplateConfigType(String queuePath,
      AbstractCSQueue.CapacityConfigType parentCapacityConfigType, Resource templateMinResource) {
    if (parentCapacityConfigType.equals(AbstractCSQueue.CapacityConfigType.PERCENTAGE)
        && !templateMinResource.equals(Resources.none())) {
      return "Managed Parent Queue " + queuePath
          + " config type is different from leaf queue template config type";
    }
    return null;
  }

  /**
   * Input of the queue structure checks.
   */
  public static final class QueueStructureInput {
    private final String queuePath;
    private final String queueName;
    private final boolean root;
    private final int childCount;
    private final boolean reservable;
    private final boolean autoQueueCreationEnabledParent;

    /**
     * @param queuePath the full path of the queue
     * @param queueName the name of the queue as listed in its parent ("root" for root)
     * @param root whether the queue is parsed without a parent
     * @param childCount the number of configured child queues
     * @param reservable whether the queue is configured as reservable
     * @param autoQueueCreationEnabledParent whether auto queue creation v1 or v2 is enabled on
     *                                       the queue, or the existing queue on this path is a
     *                                       dynamic parent queue
     */
    public QueueStructureInput(String queuePath, String queueName, boolean root,
        int childCount, boolean reservable, boolean autoQueueCreationEnabledParent) {
      this.queuePath = queuePath;
      this.queueName = queueName;
      this.root = root;
      this.childCount = childCount;
      this.reservable = reservable;
      this.autoQueueCreationEnabledParent = autoQueueCreationEnabledParent;
    }

    public String getQueuePath() {
      return queuePath;
    }

    public String getQueueName() {
      return queueName;
    }

    public boolean isRoot() {
      return root;
    }

    public int getChildCount() {
      return childCount;
    }

    public boolean isReservable() {
      return reservable;
    }

    public boolean isAutoQueueCreationEnabledParent() {
      return autoQueueCreationEnabledParent;
    }
  }
}
