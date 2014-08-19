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

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

/**
 * This class represents replicas that are under block recovery
 * It has a recovery id that is equal to the generation stamp 
 * that the replica will be bumped to after recovery
 * The recovery id is used to handle multiple concurrent block recoveries.
 * A recovery with higher recovery id preempts recoveries with a lower id.
 *
 */
public class ReplicaUnderRecovery extends ReplicaInfo {
  private ReplicaInfo original; // the original replica that needs to be recovered
  private long recoveryId; // recovery id; it is also the generation stamp 
                           // that the replica will be bumped to after recovery

  public ReplicaUnderRecovery(ReplicaInfo replica, long recoveryId) {
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
   * Copy constructor.
   * @param from where to copy from
   */
  public ReplicaUnderRecovery(ReplicaUnderRecovery from) {
    super(from);
    this.original = from.getOriginalReplica();
    this.recoveryId = from.getRecoveryID();
  }

  /** 
   * Get the recovery id
   * @return the generation stamp that the replica will be bumped to 
   */
  public long getRecoveryID() {
    return recoveryId;
  }

  /** 
   * Set the recovery id
   * @param recoveryId the new recoveryId
   */
  public void setRecoveryID(long recoveryId) {
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
  public ReplicaInfo getOriginalReplica() {
    return original;
  }

  @Override //ReplicaInfo
  public boolean isUnlinked() {
    return original.isUnlinked();
  }

  @Override //ReplicaInfo
  public void setUnlinked() {
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
  public void setDir(File dir) {
    super.setDir(dir);
    original.setDir(dir);
  }
  
  @Override //ReplicaInfo
  void setVolume(FsVolumeSpi vol) {
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

  public ReplicaRecoveryInfo createInfo() {
    return new ReplicaRecoveryInfo(original.getBlockId(), 
        original.getBytesOnDisk(), original.getGenerationStamp(),
        original.getState()); 
  }
}
