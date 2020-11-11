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
package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;

import java.util.Set;

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

  MappingRuleValidationContextImpl(CapacitySchedulerQueueManager qm) {
    queueManager = qm;
  }

  /**
   * This method will determine if a static queue path is valid.
   * @param path The static path of the queue
   * @return true of the path is valid
   * @throws YarnException if the path is invalid
   */
  private boolean validateStaticQueuePath(MappingQueuePath path)
      throws YarnException {
    //Try getting queue by its full path name, if it exists it is a static
    //leaf queue indeed, without any auto creation magic
    CSQueue queue = queueManager.getQueue(path.getFullPath());
    if (queue == null) {
      //We might not be able to find the queue, because the reference was
      // ambiguous this should only happen if the queue was referenced by
      // leaf name only
      if (queueManager.isAmbiguous(path.getFullPath())) {
        throw new YarnException(
            "Target queue is an ambiguous leaf queue '" +
            path.getFullPath() + "'");
      }

      //if leaf queue does not exist,
      //we need to check if the parent exists and is a managed parent
      if (!path.hasParent()) {
        throw new YarnException(
            "Target queue does not exist and has no parent defined '" +
            path.getFullPath() + "'");
      }

      CSQueue parentQueue = queueManager.getQueue(path.getParent());
      if (parentQueue == null) {
        if (queueManager.isAmbiguous(path.getParent())) {
          throw new YarnException("Target queue path '" + path +
              "' contains an ambiguous parent queue '" +
              path.getParent() + "' reference");
        } else {
          throw new YarnException("Target queue path '" + path + "' " +
              "contains an invalid parent queue '" + path.getParent() + "'.");
        }
      }

      if (!(parentQueue instanceof ManagedParentQueue)) {
        //If the parent path was referenced by short name, and it is not
        // managed, we look up if there is a queue under it with the leaf
        // queue's name
        String normalizedParentPath = parentQueue.getQueuePath() + "."
            + path.getLeafName();
        CSQueue normalizedQueue = queueManager.getQueue(normalizedParentPath);
        if (normalizedQueue instanceof LeafQueue) {
          return true;
        }

        if (normalizedQueue == null) {
          throw new YarnException(
              "Target queue '" + path.getFullPath() + "' does not exist" +
              " and has a non-managed parent queue defined.");
        } else {
          throw new YarnException("Target queue '" + path + "' references" +
              "a non-leaf queue, target queues must always be " +
              "leaf queues.");
        }

      }

    } else {
      // if queue exists, validate if its an instance of leaf queue
      if (!(queue instanceof LeafQueue)) {
        throw new YarnException("Target queue '" + path + "' references" +
            "a non-leaf queue, target queues must always be " +
            "leaf queues.");
      }
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
  private boolean validateDynamicQueuePath(MappingQueuePath path)
      throws YarnException{
    //if the queue is dynamic and we don't have a parent path, we cannot do
    //any validation, since the dynamic part can be substituted to anything
    //and that is the only part
    if (!path.hasParent()) {
      return true;
    }

    String parent = path.getParent();
    //if the parent path has dynamic parts, we cannot do any more validations
    if (!isPathStatic(parent)) {
      return true;
    }

    //We check if the parent queue exists
    CSQueue parentQueue = queueManager.getQueue(parent);
    if (parentQueue == null) {
      throw new YarnException("Target queue path '" + path + "' contains an " +
          "invalid parent queue");
    }

    if (!(parentQueue instanceof ManagedParentQueue)) {
      if (parentQueue.getChildQueues() != null) {
        for (CSQueue queue : parentQueue.getChildQueues()) {
          if (queue instanceof LeafQueue) {
            //if a non managed parent queue has at least one leaf queue, this
            //mapping can be valid, we cannot do any more checks
            return true;
          }
        }
      }

      //There is no way we can place anything into the queue referenced by the
      // rule, because we cannot auto create, and we don't have any leaf queues
      //Actually this branch is not accessible with the current queue hierarchy,
      //there should be no parents without any leaf queues. This condition says
      //for sanity checks
      throw new YarnException("Target queue path '" + path + "' has " +
          "a non-managed parent queue which has no LeafQueues either.");
    }

    return true;
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
    MappingQueuePath path = new MappingQueuePath(queuePath);

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
   */
  public boolean isPathStatic(String queuePath) {
    String[] parts = queuePath.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      if (knownVariables.contains(parts[i])) {
        return false;
      }
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
