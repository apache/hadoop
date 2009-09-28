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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

/**
 * This class represents replicas that are under block recovery
 * It has a recovery id that is equal to the generation stamp 
 * that the replica will be bumped to after recovery
 * The recovery id is used to handle multiple concurrent block recoveries.
 * A recovery with higher recovery id preempts recoveries with a lower id.
 *
 */
class ReplicaUnderRecovery extends ReplicaInfo {
  private ReplicaInfo original; // the original replica that needs to be recovered
  private long recoveryId; // recovery id; it is also the generation stamp 
                           // that the replica will be bumped to after recovery

  ReplicaUnderRecovery(ReplicaInfo replica, long recoveryId) {
    super(replica.getBlockId(), replica.getNumBytes(), replica.getGenerationStamp(),
        replica.getVolume(), replica.getDir());
    if ( replica.getState() != ReplicaState.FINALIZED &&
         replica.getState() != ReplicaState.RBW &&
         replica.getState() != ReplicaState.RWR ) {
      throw new IllegalArgumentException("Cannot recover replica: " + replica);
    }
    this.original = replica;
    this.recoveryId = recoveryId;
  }

  /** 
   * Get the recovery id
   * @return the generation stamp that the replica will be bumped to 
   */
  long getRecoveryID() {
    return recoveryId;
  }

  /** 
   * Set the recovery id
   * @param recoveryId the new recoveryId
   */
  void setRecoveryID(long recoveryId) {
    if (recoveryId > this.recoveryId) {
      this.recoveryId = recoveryId;
    } else {
      throw new IllegalArgumentException("The new rcovery id: " + recoveryId
          + " must be greater than the current one: " + this.recoveryId);
    }
  }

  /**
   * Get the original replica that's under recovery
   * @return the original replica under recovery
   */
  ReplicaInfo getOriginalReplica() {
    return original;
  }
  
  /**
   * Get the original replica's state
   * @return the original replica's state
   */
  ReplicaState getOrignalReplicaState() {
    return original.getState();
  }

  @Override //ReplicaInfo
  boolean isUnlinked() {
    return original.isUnlinked();
  }

  @Override //ReplicaInfo
  void setUnlinked() {
    original.setUnlinked();
  }
  
  @Override //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.RUR;
  }
  
  @Override
  public long getVisibleLength() {
    return original.getVisibleLength();
  }

  @Override
  public long getBytesOnDisk() {
    return original.getBytesOnDisk();
  }

  @Override  //org.apache.hadoop.hdfs.protocol.Block
  public void setBlockId(long blockId) {
    super.setBlockId(blockId);
    original.setBlockId(blockId);
  }

  @Override //org.apache.hadoop.hdfs.protocol.Block
  public void setGenerationStamp(long gs) {
    super.setGenerationStamp(gs);
    original.setGenerationStamp(gs);
  }
  
  @Override //org.apache.hadoop.hdfs.protocol.Block
  public void setNumBytes(long numBytes) {
    super.setNumBytes(numBytes);
    original.setNumBytes(numBytes);
  }
  
  @Override //ReplicaInfo
  void setDir(File dir) {
    super.setDir(dir);
    original.setDir(dir);
  }
  
  @Override //ReplicaInfo
  void setVolume(FSVolume vol) {
    super.setVolume(vol);
    original.setVolume(vol);
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
    return super.toString()
        + "\n  recoveryId=" + recoveryId
        + "\n  original=" + original;
  }

  Info createInfo() {
    return new Info(this); 
  }

  /** Replica recovery information. */
  static class Info extends Block{
    private ReplicaState originalState;

    private Info(ReplicaUnderRecovery rur) {
      super(rur);
      originalState = rur.getOrignalReplicaState();
    }

    ReplicaState getOriginalReplicaState() {
      return originalState;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      originalState = ReplicaState.read(in); 
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      originalState.write(out);
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
