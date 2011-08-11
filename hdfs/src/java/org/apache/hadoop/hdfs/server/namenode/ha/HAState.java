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
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;

/**
 * Namenode base state to implement state machine pattern.
 */
@InterfaceAudience.Private
abstract public class HAState {
  protected final String name;

  /**
   * Constructor
   * @param name Name of the state.
   */
  public HAState(String name) {
    this.name = name;
  }

  /**
   * Internal method to transition the state of a given namenode to a new state.
   * @param nn Namenode
   * @param s new state
   * @throws ServiceFailedException on failure to transition to new state.
   */
  protected final void setStateInternal(final NameNode nn, final HAState s)
      throws ServiceFailedException {
    exitState(nn);
    nn.setState(s);
    s.enterState(nn);
  }

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * entering a state.
   * @param nn Namenode
   * @throws ServiceFailedException on failure to enter the state.
   */
  protected abstract void enterState(final NameNode nn)
      throws ServiceFailedException;

  /**
   * Method to be overridden by subclasses to perform steps necessary for
   * exiting a state.
   * @param nn Namenode
   * @throws ServiceFailedException on failure to enter the state.
   */
  protected abstract void exitState(final NameNode nn)
      throws ServiceFailedException;

  /**
   * Move from the existing state to a new state
   * @param nn Namenode
   * @param s new state
   * @throws ServiceFailedException on failure to transition to new state.
   */
  public void setState(NameNode nn, HAState s) throws ServiceFailedException {
    if (this == s) { // Aleady in the new state
      return;
    }
    throw new ServiceFailedException("Transtion from state " + this + " to "
        + s + " is not allowed.");
  }
  
  /**
   * Check if an operation is supported in a given state.
   * @param nn Namenode
   * @param op Type of the operation.
   * @throws UnsupportedActionException if a given type of operation is not
   *           supported in this state.
   */
  public void checkOperation(final NameNode nn, final OperationCategory op)
      throws UnsupportedActionException {
    String msg = "Operation category " + op + " is not supported in state "
        + nn.getState();
    throw new UnsupportedActionException(msg);
  }
  
  @Override
  public String toString() {
    return super.toString();
  }
}