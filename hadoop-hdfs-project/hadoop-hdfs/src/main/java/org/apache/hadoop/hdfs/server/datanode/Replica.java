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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/** 
 * This represents block replicas which are stored in DataNode.
 */
@InterfaceAudience.Private
public interface Replica {
  /** Get the block ID  */
  public long getBlockId();

  /** Get the generation stamp */
  public long getGenerationStamp();

  /**
   * Get the replica state
   * @return the replica state
   */
  public ReplicaState getState();

  /**
   * Get the number of bytes received
   * @return the number of bytes that have been received
   */
  public long getNumBytes();
  
  /**
   * Get the number of bytes that have written to disk
   * @return the number of bytes that have written to disk
   */
  public long getBytesOnDisk();

  /**
   * Get the number of bytes that are visible to readers
   * @return the number of bytes that are visible to readers
   */
  public long getVisibleLength();

  /**
   * Return the storageUuid of the volume that stores this replica.
   */
  public String getStorageUuid();

  /**
   * Return true if the target volume is backed by RAM.
   */
  public boolean isOnTransientStorage();
}
