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
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

/** 
 * This represents block replicas which are stored in DataNode.
 */
@InterfaceAudience.Private
public interface Replica {
  /** Get the block ID  */
  long getBlockId();

  /** Get the generation stamp */
  long getGenerationStamp();

  /**
   * Get the replica state
   * @return the replica state
   */
  ReplicaState getState();

  /**
   * Get the number of bytes received
   * @return the number of bytes that have been received
   */
  long getNumBytes();
  
  /**
   * Get the number of bytes that have written to disk
   * @return the number of bytes that have written to disk
   */
  long getBytesOnDisk();

  /**
   * Get the number of bytes that are visible to readers
   * @return the number of bytes that are visible to readers
   */
  long getVisibleLength();

  /**
   * Return the storageUuid of the volume that stores this replica.
   */
  String getStorageUuid();

  /**
   * Return true if the target volume is backed by RAM.
   */
  boolean isOnTransientStorage();

  /**
   * Get the volume of replica.
   * @return the volume of replica
   */
  FsVolumeSpi getVolume();
}
