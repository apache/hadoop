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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.StandbyException;

/**
 * Namenode standby state. In this state the namenode acts as warm standby and
 * keeps the following updated:
 * <ul>
 * <li>Namespace by getting the edits.</li>
 * <li>Block location information by receiving block reports and blocks
 * received from the datanodes.</li>
 * </ul>
 * 
 * It does not handle read/write/checkpoint operations.
 */
@InterfaceAudience.Private
public class StandbyState extends HAState {
  // TODO: consider implementing a ObserverState instead of using the flag.
  private final boolean isObserver;

  public StandbyState() {
    this(false);
  }

  public StandbyState(boolean isObserver) {
    super(isObserver ? HAServiceState.OBSERVER : HAServiceState.STANDBY);
    this.isObserver = isObserver;
  }

  @Override
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (s == NameNode.ACTIVE_STATE ||
        (!isObserver && s == NameNode.OBSERVER_STATE) ||
        (isObserver && s == NameNode.STANDBY_STATE)) {
      setStateInternal(context, s);
      return;
    }
    super.setState(context, s);
  }

  @Override
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startStandbyServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start standby services", e);
    }
  }

  @Override
  public void prepareToExitState(HAContext context) throws ServiceFailedException {
    context.prepareToStopStandbyServices();
  }

  @Override
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopStandbyServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop standby services", e);
    }
  }

  @Override
  public void checkOperation(HAContext context, OperationCategory op)
      throws StandbyException {
    if (op == OperationCategory.UNCHECKED ||
        (op == OperationCategory.READ && context.allowStaleReads())) {
      return;
    }
    String faq = ". Visit https://s.apache.org/sbnn-error";
    String msg = "Operation category " + op + " is not supported in state "
        + context.getState() + faq;
    if (op == OperationCategory.WRITE && isObserver) {
      // If observer receives a write call, return active retry
      // exception to inform client to retry on active.
      // A write should never happen on Observer. Except that,
      // if access time is enabled. A open call can transition
      // to a write operation. In this case, Observer
      // should inform the client to retry this open on Active.
      throw new ObserverRetryOnActiveException(msg);
    } else {
      throw new StandbyException(msg);
    }
  }

  @Override
  public boolean shouldPopulateReplQueues() {
    return false;
  }

  @Override
  public String toString() {
    return isObserver ? "observer" : "standby";
  }
}

