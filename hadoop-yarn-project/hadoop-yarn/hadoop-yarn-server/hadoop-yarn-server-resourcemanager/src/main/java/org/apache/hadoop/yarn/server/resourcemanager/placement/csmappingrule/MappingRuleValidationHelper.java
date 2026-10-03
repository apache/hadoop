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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueIndex;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlacementRuleChecks.QueueRef;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * This class' functionality needs to be merged into CapacityScheduler
 * or CapacitySchedulerQueueManager, but that will include a lot of testcase
 * changes, so temporarily the logic is extracted to this class.
 */
public final class MappingRuleValidationHelper {
  public enum ValidationResult {
    CREATABLE,
    QUEUE_EXISTS,
    NO_PARENT_PROVIDED,
    NO_DYNAMIC_PARENT,
    AMBIGUOUS_PARENT,
    AMBIGUOUS_QUEUE,
    EMPTY_PATH
  }

  /**
   * Utility class hidden constructor.
   */
  private MappingRuleValidationHelper() {

  }

  public static String normalizeQueuePathRoot(
      CapacitySchedulerQueueManager queueManager, String fullPath)
      throws YarnException {
    return normalizeQueuePathRoot(
        PlacementRuleChecks.queueIndexOf(queueManager), fullPath);
  }

  @InterfaceAudience.Private
  public static String normalizeQueuePathRoot(
      QueueIndex queueIndex, String fullPath) throws YarnException {
    //Normalizing the root of the path
    ArrayList<String> parts = new ArrayList<>();
    Collections.addAll(parts, fullPath.split("\\."));

    //the first element of the path is the path root
    String pathRoot = parts.get(0);
    QueueRef pathRootQueue = queueIndex.getQueue(pathRoot);
    if (pathRootQueue == null) {
      if (queueIndex.isAmbiguous(pathRoot)) {
        throw new YarnException("Path root '" + pathRoot +
            "' is ambiguous. Path '" + fullPath + "' is invalid");
      } else {
        throw new YarnException("Path root '" + pathRoot +
            "' does not exist. Path '" + fullPath + "' is invalid");
      }
    }

    //Normalizing the root
    parts.set(0, pathRootQueue.getQueuePath());
    return String.join(DOT, parts);
  }

  public static ValidationResult validateQueuePathAutoCreation(
      CapacitySchedulerQueueManager queueManager, String path) {
    return validateQueuePathAutoCreation(
        PlacementRuleChecks.queueIndexOf(queueManager), path);
  }

  @InterfaceAudience.Private
  public static ValidationResult validateQueuePathAutoCreation(
      QueueIndex queueIndex, String path) {
    //Some sanity checks, the empty path and existing queue can be checked easy
    if (path == null || path.isEmpty()) {
      return ValidationResult.EMPTY_PATH;
    }

    if (queueIndex.getQueue(path) != null) {
      return ValidationResult.QUEUE_EXISTS;
    }

    if (queueIndex.isAmbiguous(path)) {
      return ValidationResult.AMBIGUOUS_QUEUE;
    }

    //Creating the path of the parent queue and grand parent queue
    ArrayList<String> parts = new ArrayList<>();
    Collections.addAll(parts, path.split("\\."));

    //dropping leaf name part of the path
    parts.remove(parts.size() - 1);
    String parentPath = parts.size() >= 1 ? String.join(".", parts) : "";
    //dropping parent name part of the path
    parts.remove(parts.size() - 1);
    String grandParentPath = parts.size() >= 1 ? String.join(".", parts) : "";

    if (parentPath.isEmpty()) {
      return ValidationResult.NO_PARENT_PROVIDED;
    }

    if (queueIndex.isAmbiguous(parentPath)) {
      return ValidationResult.AMBIGUOUS_PARENT;
    }
    QueueRef parentQueue = queueIndex.getQueue(parentPath);
    if (parentQueue == null) {
      if (grandParentPath.isEmpty()) {
        return ValidationResult.NO_PARENT_PROVIDED;
      }

      if (queueIndex.isAmbiguous(grandParentPath)) {
        return ValidationResult.AMBIGUOUS_PARENT;
      }
      //if we don't have a valid parent queue, we need to check the grandparent
      //if the grandparent allows new dynamic creation, the dynamic parent and
      //the dynamic leaf queue can be created as well
      QueueRef grandParentQueue = queueIndex.getQueue(grandParentPath);
      if (grandParentQueue != null && grandParentQueue.isParent() &&
          grandParentQueue.isEligibleForAutoQueueCreation()) {
        //Grandparent is a new dynamic parent queue, which allows deep queue
        //creation
        return ValidationResult.CREATABLE;
      }

      return ValidationResult.NO_DYNAMIC_PARENT;
    }

    //at this point we know we have a parent queue we just need to make sure
    //it allows queue creation
    if (parentQueue.getKind() == QueueKind.MANAGED_PARENT) {
      //Managed parent is the legacy way, so it will allow creation
      return ValidationResult.CREATABLE;
    }
    if (parentQueue.getKind() == QueueKind.PARENT) {
      //the new way of dynamic queue creation uses ParentQueues so we need to
      //check if those queues allow dynamic queue creation
      if (parentQueue.isEligibleForAutoQueueCreation()) {
        return ValidationResult.CREATABLE;
      }
    }
    //at this point we can be sure the parent does not support auto queue
    //creation it's either being a leaf queue or a non-dynamic parent queue
    return ValidationResult.NO_DYNAMIC_PARENT;
  }
}
