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

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;

/**
 * Node label checks of a queue configuration. Every check returns null when it passes, or the
 * error message when it fails; the caller decides which exception to throw.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class QueueLabelChecks {

  private QueueLabelChecks() {
  }

  /**
   * Checks that the accessible node labels of a non-root queue are a subset of the accessible
   * node labels of its parent, unless the parent can access any label.
   * @param root whether the queue is the root queue
   * @param ownLabels the accessible labels of the queue, after inheritance
   * @param parentLabels the accessible labels of the parent queue
   * @return null if the check passes, otherwise the error message
   */
  public static String checkAccessibleLabelsSubset(boolean root, Set<String> ownLabels,
      Set<String> parentLabels) {
    if (root) {
      return null;
    }
    if (parentLabels == null || parentLabels.contains(RMNodeLabelsManager.ANY)) {
      return null;
    }
    // If parent isn't "*", child shouldn't be "*" too
    if (ownLabels.contains(RMNodeLabelsManager.ANY)) {
      return "Parent's accessible queue is not ANY(*), "
          + "but child's accessible queue is " + RMNodeLabelsManager.ANY;
    }
    Set<String> diff = Sets.difference(ownLabels, parentLabels);
    if (!diff.isEmpty()) {
      return String.format(
          "Some labels of child queue is not a subset of parent queue, these labels=[%s]",
          StringUtils.join(diff, ","));
    }
    return null;
  }

  /**
   * Checks that every label of the default node label expression of a leaf queue is accessible
   * by the queue.
   * @param queuePath the full path of the leaf queue
   * @param accessibleLabels the accessible labels of the queue, after inheritance
   * @param defaultLabelExpression the default label expression, after inheritance
   * @return null if the check passes, otherwise the error message
   */
  public static String checkDefaultLabelExpression(String queuePath,
      Set<String> accessibleLabels, String defaultLabelExpression) {
    if (SchedulerUtils.checkQueueLabelExpression(accessibleLabels, defaultLabelExpression,
        null)) {
      return null;
    }
    return "Invalid default label expression of " + " queue=" + queuePath
        + " doesn't have permission to access all labels "
        + "in default label expression. labelExpression of resource request="
        + (defaultLabelExpression == null ? "" : defaultLabelExpression)
        + ". Queue labels=" + (accessibleLabels == null ?
        "" : StringUtils.join(accessibleLabels.iterator(), ','));
  }

  /**
   * Checks that every node label of the leaf queue template of an auto create enabled parent
   * queue (auto queue creation v1) is in the label set of the parent queue.
   * @param parentQueuePath the path of the managed parent queue
   * @param templateLabels the node labels of the leaf queue template, in iteration order
   * @param parentLabels the node labels of the parent queue; when the parent can access any
   *                     label this depends on the labels used by the parent at runtime
   * @return null if the check passes, otherwise the error message for the first invalid label
   */
  public static String checkLeafQueueTemplateLabels(String parentQueuePath,
      Set<String> templateLabels, Set<String> parentLabels) {
    for (String nodeLabel : templateLabels) {
      if (!parentLabels.contains(nodeLabel)) {
        return "Invalid node label " + nodeLabel
            + " on configured leaf template on parent" + " queue " + parentQueuePath;
      }
    }
    return null;
  }
}
