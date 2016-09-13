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

import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/**
 * This class is to be used as a builder for {@link ReplicaInfo} objects.
 * The state of the replica is used to determine which object is instantiated.
 */
public class ReplicaBuilder {

  private ReplicaState state;
  private long blockId;
  private long genStamp;
  private long length;
  private FsVolumeSpi volume;
  private File directoryUsed;
  private long bytesToReserve;
  private Thread writer;
  private long recoveryId;
  private Block block;

  private ReplicaInfo fromReplica;

  public ReplicaBuilder(ReplicaState state) {
    volume = null;
    writer = null;
    block = null;
    length = -1;
    this.state = state;
  }

  public ReplicaBuilder setState(ReplicaState state) {
    this.state = state;
    return this;
  }

  public ReplicaBuilder setBlockId(long blockId) {
    this.blockId = blockId;
    return this;
  }

  public ReplicaBuilder setGenerationStamp(long genStamp) {
    this.genStamp = genStamp;
    return this;
  }

  public ReplicaBuilder setLength(long length) {
    this.length = length;
    return this;
  }

  public ReplicaBuilder setFsVolume(FsVolumeSpi volume) {
    this.volume = volume;
    return this;
  }

  public ReplicaBuilder setDirectoryToUse(File dir) {
    this.directoryUsed = dir;
    return this;
  }

  public ReplicaBuilder setBytesToReserve(long bytesToReserve) {
    this.bytesToReserve = bytesToReserve;
    return this;
  }

  public ReplicaBuilder setWriterThread(Thread writer) {
    this.writer = writer;
    return this;
  }

  public ReplicaBuilder from(ReplicaInfo fromReplica) {
    this.fromReplica = fromReplica;
    return this;
  }

  public ReplicaBuilder setRecoveryId(long recoveryId) {
    this.recoveryId = recoveryId;
    return this;
  }

  public ReplicaBuilder setBlock(Block block) {
    this.block = block;
    return this;
  }

  public LocalReplicaInPipeline buildLocalReplicaInPipeline()
      throws IllegalArgumentException {
    LocalReplicaInPipeline info = null;
    switch(state) {
    case RBW:
      info = buildRBW();
      break;
    case TEMPORARY:
      info = buildTemporaryReplica();
      break;
    default:
      throw new IllegalArgumentException("Unknown replica state " + state);
    }
    return info;
  }

  private LocalReplicaInPipeline buildRBW() throws IllegalArgumentException {
    if (null != fromReplica && fromReplica.getState() == ReplicaState.RBW) {
      return new ReplicaBeingWritten((ReplicaBeingWritten) fromReplica);
    } else if (null != fromReplica) {
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      if (null != block) {
        if (null == writer) {
          throw new IllegalArgumentException("A valid writer is "
              + "required for constructing a RBW from block "
              + block.getBlockId());
        }
        return new ReplicaBeingWritten(block, volume, directoryUsed, writer);
      } else {
        if (length != -1) {
          return new ReplicaBeingWritten(blockId, length, genStamp,
              volume, directoryUsed, writer, bytesToReserve);
        } else {
          return new ReplicaBeingWritten(blockId, genStamp, volume,
              directoryUsed, bytesToReserve);
        }
      }
    }
  }

  private LocalReplicaInPipeline buildTemporaryReplica()
      throws IllegalArgumentException {
    if (null != fromReplica &&
        fromReplica.getState() == ReplicaState.TEMPORARY) {
      return new LocalReplicaInPipeline((LocalReplicaInPipeline) fromReplica);
    } else if (null != fromReplica) {
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      if (null != block) {
        if (null == writer) {
          throw new IllegalArgumentException("A valid writer is "
              + "required for constructing a Replica from block "
              + block.getBlockId());
        }
        return new LocalReplicaInPipeline(block, volume, directoryUsed,
            writer);
      } else {
        if (length != -1) {
          return new LocalReplicaInPipeline(blockId, length, genStamp,
              volume, directoryUsed, writer, bytesToReserve);
        } else {
          return new LocalReplicaInPipeline(blockId, genStamp, volume,
              directoryUsed, bytesToReserve);
        }
      }
    }
  }

  private ReplicaInfo buildFinalizedReplica() throws IllegalArgumentException {
    if (null != fromReplica &&
        fromReplica.getState() == ReplicaState.FINALIZED) {
      return new FinalizedReplica((FinalizedReplica)fromReplica);
    } else if (null != this.fromReplica) {
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      if (null != block) {
        return new FinalizedReplica(block, volume, directoryUsed);
      } else {
        return new FinalizedReplica(blockId, length, genStamp, volume,
            directoryUsed);
      }
    }
  }

  private ReplicaInfo buildRWR() throws IllegalArgumentException {

    if (null != fromReplica && fromReplica.getState() == ReplicaState.RWR) {
      return new ReplicaWaitingToBeRecovered(
          (ReplicaWaitingToBeRecovered) fromReplica);
    } else if (null != fromReplica){
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      if (null != block) {
        return new ReplicaWaitingToBeRecovered(block, volume, directoryUsed);
      } else {
        return new ReplicaWaitingToBeRecovered(blockId, length, genStamp,
            volume, directoryUsed);
      }
    }
  }

  private ReplicaInfo buildRUR() throws IllegalArgumentException {
    if (null == fromReplica) {
      throw new IllegalArgumentException(
          "Missing a valid replica to recover from");
    }
    if (null != writer || null != block) {
      throw new IllegalArgumentException("Invalid state for "
          + "recovering from replica with blk id "
          + fromReplica.getBlockId());
    }
    if (fromReplica.getState() == ReplicaState.RUR) {
      return new ReplicaUnderRecovery((ReplicaUnderRecovery) fromReplica);
    } else {
      return new ReplicaUnderRecovery(fromReplica, recoveryId);
    }
  }

  public ReplicaInfo build() throws IllegalArgumentException {
    ReplicaInfo info = null;
    switch(this.state) {
    case FINALIZED:
      info = buildFinalizedReplica();
      break;
    case RWR:
      info = buildRWR();
      break;
    case RUR:
      info = buildRUR();
      break;
    case RBW:
    case TEMPORARY:
      info = buildLocalReplicaInPipeline();
      break;
    default:
      throw new IllegalArgumentException("Unknown replica state " + state);
    }
    return info;
  }
}
