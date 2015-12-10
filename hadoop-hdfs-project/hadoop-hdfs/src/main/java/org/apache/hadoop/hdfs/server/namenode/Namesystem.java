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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;

/** Namesystem operations. */
@InterfaceAudience.Private
public interface Namesystem extends RwLock, SafeMode {
  /** Is this name system running? */
  boolean isRunning();

  /** Check if the user has superuser privilege. */
  void checkSuperuserPrivilege() throws AccessControlException;

  /** @return the block pool ID */
  String getBlockPoolId();

  boolean isInStandbyState();

  boolean isGenStampInFuture(Block block);

  BlockCollection getBlockCollection(long id);

  void checkOperation(OperationCategory read) throws StandbyException;

  void startSecretManagerIfNecessary();

  /**
   * Gets the erasure coding policy for the path
   * @param src
   *          - path
   * @return {@link ErasureCodingPolicy}
   * @throws IOException
   */
  ErasureCodingPolicy getErasureCodingPolicyForPath(String src)
      throws IOException;

  boolean isInSnapshot(long blockCollectionID);

  CacheManager getCacheManager();

  HAContext getHAContext();

  /**
   * @return true if the HA is enabled else false
   */
  boolean isHaEnabled();

  /**
   * @return Whether the namenode is transitioning to active state and is in the
   *         middle of the starting active services.
   */
  boolean inTransitionToActive();
}
