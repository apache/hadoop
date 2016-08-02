/*
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

package org.apache.slider.server.appmaster.actions;

/**
 * Access for queue operations
 */
public interface QueueAccess {
  /**
   * Put an action on the immediate queue -to be executed when the queue
   * reaches it.
   * @param action action to queue
   */
  void put(AsyncAction action);

  /**
   * Put a delayed action: this will only be added to the main queue
   * after its action time has been reached
   * @param action action to queue
   */
  void schedule(AsyncAction action);

  /**
   * Remove an action from the queues.
   * @param action action to remove
   * @return true if the action was removed
   */
  boolean remove(AsyncAction action);

  /**
   * Add a named renewing action
   * @param name name
   * @param renewingAction wrapped action
   */
  void renewing(String name,
      RenewingAction<? extends AsyncAction> renewingAction);

  /**
   * Look up a renewing action
   * @param name name of the action
   * @return the action or null if none was found
   */
  RenewingAction<? extends AsyncAction> lookupRenewingAction(String name);

  /**
   * Remove a renewing action
   * @param name action name name of the action
   * @return true if the action was found and removed.
   */
  boolean removeRenewingAction(String name);

  /**
   * Look in the immediate queue for any actions of a specific attribute
   */
  boolean hasQueuedActionWithAttribute(int attr);
}
