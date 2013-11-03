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
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;

/**
 * Active state of the namenode. In this state, namenode provides the namenode
 * service and handles operations of type {@link OperationCategory#WRITE} and
 * {@link OperationCategory#READ}.
 */
@InterfaceAudience.Private
public class ActiveState extends HAState {
  public ActiveState() {
    super(HAServiceState.ACTIVE);
  }

  @Override
  public void checkOperation(HAContext context, OperationCategory op) {
    return; // All operations are allowed in active state
  }
  
  @Override
  public boolean shouldPopulateReplQueues() {
    return true;
  }
  
  @Override
  public void setState(HAContext context, HAState s) throws ServiceFailedException {
    if (s == NameNode.STANDBY_STATE) {
      setStateInternal(context, s);
      return;
    }
    super.setState(context, s);
  }

  @Override
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start active services", e);
    }
  }

  @Override
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop active services", e);
    }
  }

}
