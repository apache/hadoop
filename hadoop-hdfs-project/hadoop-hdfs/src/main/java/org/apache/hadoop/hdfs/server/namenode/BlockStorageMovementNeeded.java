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

import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A Class to track the block collection IDs for which physical storage movement
 * needed as per the Namespace and StorageReports from DN.
 */
@InterfaceAudience.Private
public class BlockStorageMovementNeeded {
  private final Queue<Long> storageMovementNeeded = new LinkedList<Long>();

  /**
   * Add the block collection id to tracking list for which storage movement
   * expected if necessary.
   *
   * @param blockCollectionID
   *          - block collection id, which is nothing but inode id.
   */
  public synchronized void add(Long blockCollectionID) {
    storageMovementNeeded.add(blockCollectionID);
  }

  /**
   * Gets the block collection id for which storage movements check necessary
   * and make the movement if required.
   *
   * @return block collection ID
   */
  public synchronized Long get() {
    return storageMovementNeeded.poll();
  }

  public synchronized void clearAll() {
    storageMovementNeeded.clear();
  }
}
