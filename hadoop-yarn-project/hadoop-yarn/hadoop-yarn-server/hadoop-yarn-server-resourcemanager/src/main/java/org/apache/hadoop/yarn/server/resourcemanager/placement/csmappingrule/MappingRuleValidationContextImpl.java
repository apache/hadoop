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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;

import java.util.*;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public class MappingRuleValidationContextImpl
    implements MappingRuleValidationContext {
  /**
   * We store all known variables in this set.
   */
  private Set<String> knownVariables = Sets.newHashSet();

  /**
   * This set is to determine which variables are immutable.
   */
  private Set<String> immutableVariables = Sets.newHashSet();

  /**
   * For queue path validations we need an instance of the queue manager
   * to look up queues and their parents.
   */
  private final CapacitySchedulerQueueManager queueManager;

  public MappingRuleValidationContextImpl(CapacitySchedulerQueueManager qm) {
    queueManager = qm;
  }

  /**
   * This method will determine if a static queue path is valid.
   * We consider a path static (in the target path validation context)
   * If non if it's parts contain any substitutable variables.
   * eg. root.groups.bob is static, while root.groups.%user is dynamic
   * @param path The static path of the queue
   * @return true if the path is valid
   * @throws YarnException if the path is invalid
   */
  private boolean validateStaticQueuePath(QueuePath path)
      throws YarnException {
    String normalizedPath = MappingRuleValidationHelper.normalizeQueuePathRoot(
        queueManager, path.getFullPath());
    MappingRuleValidationHelper.ValidationResult validity =
        MappingRuleValidationHelper.validateQueuePathAutoCreation(
            queueManager, normalizedPath);

    switch (validity) {
    case AMBIGUOUS_PARENT:
      throw new YarnException("Target queue path '" + path +
          "' contains an ambiguous parent queue '" +
          path.getParent() + "' reference.");
    case AMBIGUOUS_QUEUE:
      throw new YarnException("Target queue is an ambiguous leaf queue '" +
              path.getFullPath() + "'.");
    case EMPTY_PATH:
      throw new YarnException("Mapping rule did not specify a target queue.");
    case NO_PARENT_PROVIDED:
      throw new YarnException(
          "Target queue does not exist and has no parent defined '" +
              path.getFullPath() + "'.");
    case NO_DYNAMIC_PARENT:
      throw new YarnException("Mapping rule specified a parent queue '" +
          path.getParent() + "', but it is not a dynamic parent queue, " +
          "and no queue exists with name '" + path.getLeafName() +
          "' under it.");
    case QUEUE_EXISTS:
      CSQueue queue = queueManager.getQueue(normalizedPath);
      if (!(queue instanceof AbstractLeafQueue)) {
        throw new YarnException("Target queue '" + path.getFullPath() +
            "' but it's not a leaf queue.");
      }
      break;
    case CREATABLE:
      break;
    default:
      //Probably the QueueCreationValidation have
      //new items, which are not handled here
      throw new YarnException("Unknown queue path validation result. '" +
          validity + "'.");
    }

    return true;
  }

  /**
   * This method will determine if a dynamic queue path (a path which contains
   * variables) is valid.
   * @param path The dynamic path of the queue
   * @return true of the path is valid
   * @throws YarnException if the path is invalid
   */
  private boolean validateDynamicQueuePath(QueuePath path)
      throws YarnException{
    ArrayList<String> parts = new ArrayList<>();
    Collections.addAll(parts, path.getFullPath().split("\\."));
    //How deep is the path to be created after the root element

    Iterator<String> pointer = parts.iterator();
    if (!pointer.hasNext()) {
      //This should not happen since we only call validateDynamicQueuePath
      //if we have found at least ONE dynamic part, which implies the path is
      //not empty, so if we get here, I'm really curious what the path was,
      //that's the reason we give back a theoretically "empty" path
      throw new YarnException("Empty queue path provided '" + path + "'");
    }
    StringBuilder staticPartBuffer = new StringBuilder(pointer.next());
    String staticPartParent = null;

    //If not even the root of the reference is static we cannot validate
    if (!isPathStatic(staticPartBuffer.toString())) {
      return true;
    }

    //getting the static part of the queue, we can only validate that
    while (pointer.hasNext()) {
      String nextPart = pointer.next();
      if (isPathStatic(nextPart)) {
        staticPartParent = staticPartBuffer.toString();
        staticPartBuffer.append(DOT).append(nextPart);
      } else {
        //when we find the first dynamic part, we stop the search
        break;
      }
    }
    String staticPart = staticPartBuffer.toString();

    String normalizedStaticPart =
        MappingRuleValidationHelper.normalizeQueuePathRoot(
            queueManager, staticPart);
    CSQueue queue = queueManager.getQueue(normalizedStaticPart);
    //if the static part of our queue exists, and it's not a leaf queue,
    //we cannot do any deeper validation
    if (queue != null) {
      if (queue instanceof AbstractLeafQueue) {
        throw new YarnException("Queue path '" + path +"' is invalid " +
            "because '" + normalizedStaticPart + "' is a leaf queue, " +
            "which can have no other queues under it.");
      }
      return true;
    }

    if (staticPartParent != null) {
      String normalizedStaticPartParent
          = MappingRuleValidationHelper.normalizeQueuePathRoot(
              queueManager, staticPartParent);
      queue = queueManager.getQueue(normalizedStaticPartParent);
      //if the parent of our static part is eligible for creation, we validate
      //this rule
      if (isDynamicParent(queue)) {
        return true;
      }
    }

    //at this point we cannot find any parent which is eligible for creating
    //this path
    throw new YarnException("No eligible parent found on path '" + path + "'.");
  }

  /**
   * This method determines if a queue is eligible for being a parent queue.
   * Since YARN-10506 not only managed parent queues can have child queues.
   * @param queue The queue object
   * @return true if queues can be created under this queue otherwise false
   */
  private boolean isDynamicParent(CSQueue queue) {
    if (queue == null) {
      return false;
    }

    if (queue instanceof ManagedParentQueue) {
      return true;
    }

    if (queue instanceof ParentQueue) {
      return ((ParentQueue)queue).isEligibleForAutoQueueCreation();
    }

    return false;
  }


  /**
   * This method should determine if the provided queue path can result in
   * a possible placement. It should fail if the provided path cannot be placed
   * into any of the known queues regardless of the variable context.
   * @param queuePath The path to check
   * @return true if the validation was successful
   * @throws YarnException if the provided queue path is invalid
   */
  public boolean validateQueuePath(String queuePath) throws YarnException {
    if (queuePath == null || queuePath.isEmpty()) {
      throw new YarnException("Queue path is empty.");
    }
    QueuePath path = new QueuePath(queuePath);

    if (isPathStatic(queuePath)) {
      return validateStaticQueuePath(path);
    } else {
      return validateDynamicQueuePath(path);
    }
  }

  /**
   * Method to determine if the provided queue path contains any dynamic parts
   * A part is dynamic if a known variable is referenced in it.
   * @param queuePath The path to check
   * @return true if no dynamic parts were found
   * @throws YarnException if a path part is invalid (eg. empty)
   */
  public boolean isPathStatic(String queuePath) throws YarnException {
    String[] parts = queuePath.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].isEmpty()) {
        throw new YarnException("Path segment cannot be empty '" +
            queuePath + "'.");
      }

      if (!isPathPartStatic(parts[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * Method to determine if the provided queue path part is dynamic.
   * A part is dynamic if a known variable is referenced in it.
   * @param pathPart The path part to check
   * @return true if part is not dynamic
   */
  private boolean isPathPartStatic(String pathPart) {
    if (knownVariables.contains(pathPart)) {
      return false;
    }

    return true;
  }

  /**
   * This method will add a known variable to the validation context, known
   * variables can be used to determine if a path is static or dynamic.
   * @param variable Name of the variable
   * @throws YarnException If the variable to be added has already added as an
   * immutable one, an exception is thrown
   */
  public void addVariable(String variable) throws YarnException {
    if (immutableVariables.contains(variable)) {
      throw new YarnException("Variable '" + variable + "' is immutable " +
          "cannot add to the modified variable list.");
    }
    knownVariables.add(variable);
  }

  /**
   * This method will add a known immutable variable to the validation context,
   * known variables can be used to determine if a path is static or dynamic.
   * @param variable Name of the immutable variable
   * @throws YarnException If the variable to be added has already added as a
   * regular, mutable variable an exception is thrown
   */
  public void addImmutableVariable(String variable) throws YarnException {
    if (knownVariables.contains(variable) &&
        !immutableVariables.contains(variable)) {
      throw new YarnException("Variable '" + variable + "' already " +
          "added as a mutable variable cannot set it to immutable.");
    }
    knownVariables.add(variable);
    immutableVariables.add(variable);
  }

  /**
   * This method will return all the known variables.
   * @return Set of the known variables
   */
  public Set<String> getVariables() {
    return ImmutableSet.copyOf(knownVariables);
  }
}
