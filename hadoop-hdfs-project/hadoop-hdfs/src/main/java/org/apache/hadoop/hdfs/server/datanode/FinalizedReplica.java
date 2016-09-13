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

import java.io.File;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

/**
 * This class describes a replica that has been finalized.
 */
public class FinalizedReplica extends LocalReplica {

  /**
   * Constructor
   * @param blockId block id
   * @param len replica length
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  public FinalizedReplica(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir) {
    super(blockId, len, genStamp, vol, dir);
  }
  
  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  public FinalizedReplica(Block block, FsVolumeSpi vol, File dir) {
    super(block, vol, dir);
  }

  /**
   * Copy constructor.
   * @param from where to copy construct from
   */
  public FinalizedReplica(FinalizedReplica from) {
    super(from);
  }

  @Override  // ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.FINALIZED;
  }
  
  @Override
  public long getVisibleLength() {
    return getNumBytes();       // all bytes are visible
  }

  @Override
  public long getBytesOnDisk() {
    return getNumBytes();
  }

  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
  
  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getOriginalReplica");
  }

  @Override
  public long getRecoveryID() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getRecoveryID");
  }

  @Override
  public void setRecoveryID(long recoveryId) {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support setRecoveryID");
  }

  @Override
  public ReplicaRecoveryInfo createInfo() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support createInfo");
  }
}
