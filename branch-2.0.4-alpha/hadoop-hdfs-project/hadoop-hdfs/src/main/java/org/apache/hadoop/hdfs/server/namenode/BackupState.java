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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.ipc.StandbyException;

@InterfaceAudience.Private
public class BackupState extends HAState {

  public BackupState() {
    super(HAServiceState.STANDBY);
  }

  @Override // HAState
  public void checkOperation(HAContext context, OperationCategory op)
      throws StandbyException {
    context.checkOperation(op);
  }

  @Override // HAState
  public boolean shouldPopulateReplQueues() {
    return false;
  }

  @Override // HAState
  public void enterState(HAContext context) throws ServiceFailedException {
    try {
      context.startActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start backup services", e);
    }
  }

  @Override // HAState
  public void exitState(HAContext context) throws ServiceFailedException {
    try {
      context.stopActiveServices();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop backup services", e);
    }
  }

  @Override // HAState
  public void prepareToExitState(HAContext context) throws ServiceFailedException {
    context.prepareToStopStandbyServices();
  }
}
