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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.util.Time;

/**
 * Namenode base state to implement state machine pattern.
 */
@InterfaceAudience.Private
abstract public class HAState {
  protected final HAServiceState state;
  private long lastHATransitionTime;

  /**
   * Constructor
   * @param state HA service state.
   */
  public HAState(HAServiceState state) {
    this.state = state;
  }

  /**
   * @return the generic service state
   */
  public HAServiceState getServiceState() {
    return state;
  }

  /**
   * Internal method to move from the existing state to a new state.
   * @param context HA context
   * @param s new state
   * @throws ServiceFailedException on failure to transition to new state.
   */
  protected final void setStateInternal(final HAContext context, final HAState s)
      throws ServiceFailedException {
    prepareToExitState(context);
    s.prepareToEnterState(context);
    context.writeLock();
    try {
      exitState(context);
      context.setState(s);
      s.enterState(context);
      s.updateLastHATransitionTime();
    } finally {
      context.writeUnlock();
    }
  }

  /**
   * Gets the most recent HA transition time in milliseconds from the epoch.
   *
   * @return the most recent HA transition time in milliseconds from the epoch.
   */
  public long getLastHATransitionTime() {
    return lastHATransitionTime;
  }

  private void updateLastHATransitionTime() {
    lastHATransitionTime = Time.now();
  }

  /**
   * Method to be overridden by subclasses to prepare to enter a state.
   * This method is called <em>without</em> the context being locked,
   * and after {@link #prepareToExitState(HAContext)} has been called
   * for the previous state, but before {@link #exitState(HAContext)}
   * has been called for the previous state.
   * @param context HA context
   * @throws ServiceFailedException on precondition failure
   */
  public void prepareToEnterState(final HAContext context)
      throws ServiceFailedException {}

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * entering a state.
   * @param context HA context
   * @throws ServiceFailedException on failure to enter the state.
   */
  public abstract void enterState(final HAContext context)
      throws ServiceFailedException;

  /**
   * Method to be overridden by subclasses to prepare to exit a state.
   * This method is called <em>without</em> the context being locked.
   * This is used by the standby state to cancel any checkpoints
   * that are going on. It can also be used to check any preconditions
   * for the state transition.
   * 
   * This method should not make any destructive changes to the state
   * (eg stopping threads) since {@link #prepareToEnterState(HAContext)}
   * may subsequently cancel the state transition.
   * @param context HA context
   * @throws ServiceFailedException on precondition failure
   */
  public void prepareToExitState(final HAContext context)
      throws ServiceFailedException {}

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * exiting a state.
   * @param context HA context
   * @throws ServiceFailedException on failure to enter the state.
   */
  public abstract void exitState(final HAContext context)
      throws ServiceFailedException;

  /**
   * Move from the existing state to a new state
   * @param context HA context
   * @param s new state
   * @throws ServiceFailedException on failure to transition to new state.
   */
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (this == s) { // Already in the new state
      return;
    }
    throw new ServiceFailedException("Transition from state " + this + " to "
        + s + " is not allowed.");
  }
  
  /**
   * Check if an operation is supported in a given state.
   * @param context HA context
   * @param op Type of the operation.
   * @throws StandbyException if a given type of operation is not
   *           supported in standby state
   */
  public abstract void checkOperation(final HAContext context, final OperationCategory op)
      throws StandbyException;

  public abstract boolean shouldPopulateReplQueues();

  /**
   * @return String representation of the service state.
   */
  @Override
  public String toString() {
    return state.toString();
  }
}
