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
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.StandbyException;

/**
 * Context that is to be used by {@link HAState} for getting/setting the
 * current state and performing required operations.
 */
@InterfaceAudience.Private
public interface HAContext {
  /** Set the state of the context to given {@code state} */
  public void setState(HAState state);
  
  /** Get the state from the context */
  public HAState getState();
  
  /** Start the services required in active state */
  public void startActiveServices() throws IOException;
  
  /** Stop the services when exiting active state */
  public void stopActiveServices() throws IOException;
  
  /** Start the services required in standby state */
  public void startStandbyServices() throws IOException;

  /** Prepare to exit the standby state */
  public void prepareToStopStandbyServices() throws ServiceFailedException;

  /** Stop the services when exiting standby state */
  public void stopStandbyServices() throws IOException;

  /**
   * Take a write-lock on the underlying namesystem
   * so that no concurrent state transitions or edits
   * can be made.
   */
  void writeLock();

  /**
   * Unlock the lock taken by {@link #writeLock()}
   */
  void writeUnlock();

  /**
   * Verify that the given operation category is allowed in the current state.
   * This is to allow NN implementations (eg BackupNode) to override it with
   * node-specific handling.
   * 
   * If the operation which is being checked will be taking the FSNS lock, it's
   * advisable to check the operation category both immediately before and after
   * taking the lock. This is because clients rely on the StandbyException
   * thrown by this method in order to trigger client failover, and if a client
   * first tries to contact the Standby NN, it could block for a long time if
   * the Standby is holding the lock for a while, e.g. when performing a
   * checkpoint. See HDFS-4591 for more details.
   */
  void checkOperation(OperationCategory op) throws StandbyException;

  /**
   * @return true if the node should allow stale reads (ie reads
   * while the namespace is not up to date)
   */
  boolean allowStaleReads();
}
