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

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This class handles the block recovery work commands.
 */
@InterfaceAudience.Private
public class BlockRecoveryWorker {
  public static final Log LOG = DataNode.LOG;

  private final DataNode datanode;
  private final Configuration conf;
  private final DNConf dnConf;

  BlockRecoveryWorker(DataNode datanode) {
    this.datanode = datanode;
    conf = datanode.getConf();
    dnConf = datanode.getDnConf();
  }

  /** A convenient class used in block recovery. */
  static class BlockRecord {
    private final DatanodeID id;
    private final InterDatanodeProtocol datanode;
    private final ReplicaRecoveryInfo rInfo;

    private String storageID;

    BlockRecord(DatanodeID id, InterDatanodeProtocol datanode,
        ReplicaRecoveryInfo rInfo) {
      this.id = id;
      this.datanode = datanode;
      this.rInfo = rInfo;
    }

    private void updateReplicaUnderRecovery(String bpid, long recoveryId,
        long newBlockId, long newLength) throws IOException {
      final ExtendedBlock b = new ExtendedBlock(bpid, rInfo);
      storageID = datanode.updateReplicaUnderRecovery(b, recoveryId, newBlockId,
          newLength);
    }

    @Override
    public String toString() {
      return "block:" + rInfo + " node:" + id;
    }
  }

  /** A block recovery task for a contiguous block. */
  class RecoveryTaskContiguous {
    private final RecoveringBlock rBlock;
    private final ExtendedBlock block;
    private final String bpid;
    private final DatanodeInfo[] locs;
    private final long recoveryId;

    RecoveryTaskContiguous(RecoveringBlock rBlock) {
      this.rBlock = rBlock;
      block = rBlock.getBlock();
      bpid = block.getBlockPoolId();
      locs = rBlock.getLocations();
      recoveryId = rBlock.getNewGenerationStamp();
    }

    protected void recover() throws IOException {
      List<BlockRecord> syncList = new ArrayList<>(locs.length);
      int errorCount = 0;
      int candidateReplicaCnt = 0;

      // Check generation stamps, replica size and state. Replica must satisfy
      // the following criteria to be included in syncList for recovery:
      // - Valid generation stamp
      // - Non-zero length
      // - Original state is RWR or better
      for(DatanodeID id : locs) {
        try {
          DatanodeID bpReg = new DatanodeID(
              datanode.getBPOfferService(bpid).bpRegistration);
          InterDatanodeProtocol proxyDN = bpReg.equals(id)?
              datanode: DataNode.createInterDataNodeProtocolProxy(id, conf,
              dnConf.socketTimeout, dnConf.connectToDnViaHostname);
          ReplicaRecoveryInfo info = callInitReplicaRecovery(proxyDN, rBlock);
          if (info != null &&
              info.getGenerationStamp() >= block.getGenerationStamp() &&
              info.getNumBytes() > 0) {
            // Count the number of candidate replicas received.
            ++candidateReplicaCnt;
            if (info.getOriginalReplicaState().getValue() <=
                ReplicaState.RWR.getValue()) {
              syncList.add(new BlockRecord(id, proxyDN, info));
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Block recovery: Ignored replica with invalid " +
                    "original state: " + info + " from DataNode: " + id);
              }
            }
          } else {
            if (LOG.isDebugEnabled()) {
              if (info == null) {
                LOG.debug("Block recovery: DataNode: " + id + " does not have "
                    + "replica for block: " + block);
              } else {
                LOG.debug("Block recovery: Ignored replica with invalid "
                    + "generation stamp or length: " + info + " from " +
                    "DataNode: " + id);
              }
            }
          }
        } catch (RecoveryInProgressException ripE) {
          InterDatanodeProtocol.LOG.warn(
              "Recovery for replica " + block + " on data-node " + id
                  + " is already in progress. Recovery id = "
                  + rBlock.getNewGenerationStamp() + " is aborted.", ripE);
          return;
        } catch (IOException e) {
          ++errorCount;
          InterDatanodeProtocol.LOG.warn(
              "Failed to obtain replica info for block (=" + block
                  + ") from datanode (=" + id + ")", e);
        }
      }

      if (errorCount == locs.length) {
        throw new IOException("All datanodes failed: block=" + block
            + ", datanodeids=" + Arrays.asList(locs));
      }

      // None of the replicas reported by DataNodes has the required original
      // state, report the error.
      if (candidateReplicaCnt > 0 && syncList.isEmpty()) {
        throw new IOException("Found " + candidateReplicaCnt +
            " replica(s) for block " + block + " but none is in " +
            ReplicaState.RWR.name() + " or better state. datanodeids=" +
            Arrays.asList(locs));
      }

      syncBlock(syncList);
    }

    /** Block synchronization. */
    void syncBlock(List<BlockRecord> syncList) throws IOException {
      DatanodeProtocolClientSideTranslatorPB nn =
          getActiveNamenodeForBP(block.getBlockPoolId());

      boolean isTruncateRecovery = rBlock.getNewBlock() != null;
      long blockId = (isTruncateRecovery) ?
          rBlock.getNewBlock().getBlockId() : block.getBlockId();

      if (LOG.isDebugEnabled()) {
        LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
            + "), syncList=" + syncList);
      }

      // syncList.isEmpty() means that all data-nodes do not have the block
      // or their replicas have 0 length.
      // The block can be deleted.
      if (syncList.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("syncBlock for block " + block + ", all datanodes don't " +
              "have the block or their replicas have 0 length. The block can " +
              "be deleted.");
        }
        nn.commitBlockSynchronization(block, recoveryId, 0,
            true, true, DatanodeID.EMPTY_ARRAY, null);
        return;
      }

      // Calculate the best available replica state.
      ReplicaState bestState = ReplicaState.RWR;
      long finalizedLength = -1;
      for (BlockRecord r : syncList) {
        assert r.rInfo.getNumBytes() > 0 : "zero length replica";
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if (rState.getValue() < bestState.getValue()) {
          bestState = rState;
        }
        if(rState == ReplicaState.FINALIZED) {
          if (finalizedLength > 0 && finalizedLength != r.rInfo.getNumBytes()) {
            throw new IOException("Inconsistent size of finalized replicas. " +
                "Replica " + r.rInfo + " expected size: " + finalizedLength);
          }
          finalizedLength = r.rInfo.getNumBytes();
        }
      }

      // Calculate list of nodes that will participate in the recovery
      // and the new block size
      List<BlockRecord> participatingList = new ArrayList<>();
      final ExtendedBlock newBlock = new ExtendedBlock(bpid, blockId,
          -1, recoveryId);
      switch(bestState) {
      case FINALIZED:
        assert finalizedLength > 0 : "finalizedLength is not positive";
        for(BlockRecord r : syncList) {
          ReplicaState rState = r.rInfo.getOriginalReplicaState();
          if (rState == ReplicaState.FINALIZED ||
              rState == ReplicaState.RBW &&
                  r.rInfo.getNumBytes() == finalizedLength) {
            participatingList.add(r);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("syncBlock replicaInfo: block=" + block +
                ", from datanode " + r.id + ", receivedState=" + rState.name() +
                ", receivedLength=" + r.rInfo.getNumBytes() +
                ", bestState=FINALIZED, finalizedLength=" + finalizedLength);
          }
        }
        newBlock.setNumBytes(finalizedLength);
        break;
      case RBW:
      case RWR:
        long minLength = Long.MAX_VALUE;
        for(BlockRecord r : syncList) {
          ReplicaState rState = r.rInfo.getOriginalReplicaState();
          if(rState == bestState) {
            minLength = Math.min(minLength, r.rInfo.getNumBytes());
            participatingList.add(r);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("syncBlock replicaInfo: block=" + block +
                ", from datanode " + r.id + ", receivedState=" + rState.name() +
                ", receivedLength=" + r.rInfo.getNumBytes() + ", bestState=" +
                bestState.name());
          }
        }
        // recover() guarantees syncList will have at least one replica with RWR
        // or better state.
        assert minLength != Long.MAX_VALUE : "wrong minLength";
        newBlock.setNumBytes(minLength);
        break;
      case RUR:
      case TEMPORARY:
        assert false : "bad replica state: " + bestState;
      default:
        break; // we have 'case' all enum values
      }
      if (isTruncateRecovery) {
        newBlock.setNumBytes(rBlock.getNewBlock().getNumBytes());
      }

      List<DatanodeID> failedList = new ArrayList<>();
      final List<BlockRecord> successList = new ArrayList<>();
      for (BlockRecord r : participatingList) {
        try {
          r.updateReplicaUnderRecovery(bpid, recoveryId, blockId,
              newBlock.getNumBytes());
          successList.add(r);
        } catch (IOException e) {
          InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
              + newBlock + ", datanode=" + r.id + ")", e);
          failedList.add(r.id);
        }
      }

      // If any of the data-nodes failed, the recovery fails, because
      // we never know the actual state of the replica on failed data-nodes.
      // The recovery should be started over.
      if (!failedList.isEmpty()) {
        StringBuilder b = new StringBuilder();
        for(DatanodeID id : failedList) {
          b.append("\n  " + id);
        }
        throw new IOException("Cannot recover " + block + ", the following "
            + failedList.size() + " data-nodes failed {" + b + "\n}");
      }

      // Notify the name-node about successfully recovered replicas.
      final DatanodeID[] datanodes = new DatanodeID[successList.size()];
      final String[] storages = new String[datanodes.length];
      for (int i = 0; i < datanodes.length; i++) {
        final BlockRecord r = successList.get(i);
        datanodes[i] = r.id;
        storages[i] = r.storageID;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Datanode triggering commitBlockSynchronization, block=" +
            block + ", newGs=" + newBlock.getGenerationStamp() +
            ", newLength=" + newBlock.getNumBytes());
      }

      nn.commitBlockSynchronization(block,
          newBlock.getGenerationStamp(), newBlock.getNumBytes(), true, false,
          datanodes, storages);
    }
  }

  private static void logRecoverBlock(String who, RecoveringBlock rb) {
    ExtendedBlock block = rb.getBlock();
    DatanodeInfo[] targets = rb.getLocations();

    LOG.info(who + " calls recoverBlock(" + block
        + ", targets=[" + Joiner.on(", ").join(targets) + "]"
        + ", newGenerationStamp=" + rb.getNewGenerationStamp()
        + ", newBlock=" + rb.getNewBlock()
        + ", isStriped=" + rb.isStriped()
        + ")");
  }

  /**
   * Convenience method, which unwraps RemoteException.
   * @throws IOException not a RemoteException.
   */
  private static ReplicaRecoveryInfo callInitReplicaRecovery(
      InterDatanodeProtocol datanode, RecoveringBlock rBlock)
      throws IOException {
    try {
      return datanode.initReplicaRecovery(rBlock);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  /**
   * Get the NameNode corresponding to the given block pool.
   *
   * @param bpid Block pool Id
   * @return Namenode corresponding to the bpid
   * @throws IOException if unable to get the corresponding NameNode
   */
  DatanodeProtocolClientSideTranslatorPB getActiveNamenodeForBP(
      String bpid) throws IOException {
    BPOfferService bpos = datanode.getBPOfferService(bpid);
    if (bpos == null) {
      throw new IOException("No block pool offer service for bpid=" + bpid);
    }

    DatanodeProtocolClientSideTranslatorPB activeNN = bpos.getActiveNN();
    if (activeNN == null) {
      throw new IOException(
          "Block pool " + bpid + " has not recognized an active NN");
    }
    return activeNN;
  }

  public Daemon recoverBlocks(final String who,
      final Collection<RecoveringBlock> blocks) {
    Daemon d = new Daemon(datanode.threadGroup, new Runnable() {
      @Override
      public void run() {
        for(RecoveringBlock b : blocks) {
          try {
            logRecoverBlock(who, b);
            RecoveryTaskContiguous task = new RecoveryTaskContiguous(b);
            task.recover();
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED: " + b, e);
          }
        }
      }
    });
    d.start();
    return d;
  }
}
