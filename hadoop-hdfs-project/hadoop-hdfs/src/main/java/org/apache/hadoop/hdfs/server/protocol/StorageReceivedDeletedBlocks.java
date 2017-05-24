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

package org.apache.hadoop.hdfs.server.protocol;

import java.util.Arrays;

/**
 * Report of block received and deleted per Datanode
 * storage.
 */
public class StorageReceivedDeletedBlocks {
  final DatanodeStorage storage;
  private final ReceivedDeletedBlockInfo[] blocks;

  @Deprecated
  public String getStorageID() {
    return storage.getStorageID();
  }

  public DatanodeStorage getStorage() {
    return storage;
  }

  public ReceivedDeletedBlockInfo[] getBlocks() {
    return blocks;
  }

  @Deprecated
  public StorageReceivedDeletedBlocks(final String storageID,
      final ReceivedDeletedBlockInfo[] blocks) {
    this.storage = new DatanodeStorage(storageID);
    this.blocks = blocks;
  }

  public StorageReceivedDeletedBlocks(final DatanodeStorage storage,
      final ReceivedDeletedBlockInfo[] blocks) {
    this.storage = storage;
    this.blocks = blocks;
  }

  @Override
  public String toString() {
    return storage + Arrays.toString(blocks);
  }
}
