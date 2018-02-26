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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringStripedBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BLOCK_GROUP_INDEX_MASK;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;

/**
 * This class handles the block recovery work commands.
 */
@InterfaceAudience.Private
public class BlockRecoveryWorker {
  public static final Logger LOG = DataNode.LOG;

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

    public ReplicaRecoveryInfo getReplicaRecoveryInfo(){
      return rInfo;
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
          DatanodeID bpReg = getDatanodeID(bpid);
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
          InterDatanodeProtocol.LOG.warn("Failed to recover block (block="
              + block + ", datanode=" + id + ")", e);
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

      LOG.info("BlockRecoveryWorker: block={} (length={}),"
              + " isTruncateRecovery={}, syncList={}", block,
          block.getNumBytes(), isTruncateRecovery, syncList);

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

      LOG.info("BlockRecoveryWorker: block={} (length={}), bestState={},"
              + " newBlock={} (length={}), participatingList={}",
          block, block.getNumBytes(), bestState.name(), newBlock,
          newBlock.getNumBytes(), participatingList);

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

      // Abort if all failed.
      if (successList.isEmpty()) {
        throw new IOException("Cannot recover " + block
            + ", the following datanodes failed: " + failedList);
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

  /**
   * blk_0  blk_1  blk_2  blk_3  blk_4  blk_5  blk_6  blk_7  blk_8
   *  64k    64k    64k    64k    64k    64k    64k    64k    64k   <-- stripe_0
   *  64k    64k    64k    64k    64k    64k    64k    64k    64k
   *  64k    64k    64k    64k    64k    64k    64k    61k    <-- startStripeIdx
   *  64k    64k    64k    64k    64k    64k    64k
   *  64k    64k    64k    64k    64k    64k    59k
   *  64k    64k    64k    64k    64k    64k
   *  64k    64k    64k    64k    64k    64k                <-- last full stripe
   *  64k    64k    13k    64k    55k     3k              <-- target last stripe
   *  64k    64k           64k     1k
   *  64k    64k           58k
   *  64k    64k
   *  64k    19k
   *  64k                                               <-- total visible stripe
   *
   *  Due to different speed of streamers, the internal blocks in a block group
   *  could have different lengths when the block group isn't ended normally.
   *  The purpose of this class is to recover the UnderConstruction block group,
   *  so all internal blocks end at the same stripe.
   *
   * The steps:
   * 1. get all blocks lengths from DataNodes.
   * 2. calculate safe length, which is at the target last stripe.
   * 3. decode and feed blk_6~8, make them end at last full stripe. (the last
   * full stripe means the last decodable stripe.)
   * 4. encode the target last stripe, with the remaining sequential data. In
   * this case, the sequential data is 64k+64k+13k. Feed blk_6~8 the parity cells.
   * Overwrite the parity cell if have to.
   * 5. truncate the stripes from visible stripe, to target last stripe.
   * TODO: implement step 3,4
   */
  public class RecoveryTaskStriped {
    private final RecoveringBlock rBlock;
    private final ExtendedBlock block;
    private final String bpid;
    private final DatanodeInfo[] locs;
    private final long recoveryId;

    private final byte[] blockIndices;
    private final ErasureCodingPolicy ecPolicy;

    RecoveryTaskStriped(RecoveringStripedBlock rBlock) {
      this.rBlock = rBlock;
      // TODO: support truncate
      Preconditions.checkArgument(rBlock.getNewBlock() == null);

      block = rBlock.getBlock();
      bpid = block.getBlockPoolId();
      locs = rBlock.getLocations();
      recoveryId = rBlock.getNewGenerationStamp();
      blockIndices = rBlock.getBlockIndices();
      ecPolicy = rBlock.getErasureCodingPolicy();
    }

    protected void recover() throws IOException {
      checkLocations(locs.length);

      Map<Long, BlockRecord> syncBlocks = new HashMap<>(locs.length);
      final int dataBlkNum = ecPolicy.getNumDataUnits();
      final int totalBlkNum = dataBlkNum + ecPolicy.getNumParityUnits();
      //check generation stamps
      for (int i = 0; i < locs.length; i++) {
        DatanodeID id = locs[i];
        try {
          DatanodeID bpReg = getDatanodeID(bpid);
          InterDatanodeProtocol proxyDN = bpReg.equals(id) ?
              datanode : DataNode.createInterDataNodeProtocolProxy(id, conf,
              dnConf.socketTimeout, dnConf.connectToDnViaHostname);
          ExtendedBlock internalBlk = new ExtendedBlock(block);
          final long blockId = block.getBlockId() + blockIndices[i];
          internalBlk.setBlockId(blockId);
          ReplicaRecoveryInfo info = callInitReplicaRecovery(proxyDN,
              new RecoveringBlock(internalBlk, null, recoveryId));

          if (info != null &&
              info.getGenerationStamp() >= block.getGenerationStamp() &&
              info.getNumBytes() > 0) {
            final BlockRecord existing = syncBlocks.get(blockId);
            if (existing == null ||
                info.getNumBytes() > existing.rInfo.getNumBytes()) {
              // if we have >1 replicas for the same internal block, we
              // simply choose the one with larger length.
              // TODO: better usage of redundant replicas
              syncBlocks.put(blockId, new BlockRecord(id, proxyDN, info));
            }
          }
        } catch (RecoveryInProgressException ripE) {
          InterDatanodeProtocol.LOG.warn(
              "Recovery for replica " + block + " on data-node " + id
                  + " is already in progress. Recovery id = "
                  + rBlock.getNewGenerationStamp() + " is aborted.", ripE);
          return;
        } catch (IOException e) {
          InterDatanodeProtocol.LOG.warn("Failed to recover block (block="
              + block + ", datanode=" + id + ")", e);
        }
      }
      checkLocations(syncBlocks.size());

      final long safeLength = getSafeLength(syncBlocks);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Recovering block " + block
            + ", length=" + block.getNumBytes() + ", safeLength=" + safeLength
            + ", syncList=" + syncBlocks);
      }

      // If some internal blocks reach the safe length, convert them to RUR
      List<BlockRecord> rurList = new ArrayList<>(locs.length);
      for (BlockRecord r : syncBlocks.values()) {
        int blockIndex = (int) (r.rInfo.getBlockId() & BLOCK_GROUP_INDEX_MASK);
        long newSize = getInternalBlockLength(safeLength, ecPolicy.getCellSize(),
            dataBlkNum, blockIndex);
        if (r.rInfo.getNumBytes() >= newSize) {
          rurList.add(r);
        }
      }
      assert rurList.size() >= dataBlkNum : "incorrect safe length";

      // Recovery the striped block by truncating internal blocks to the safe
      // length. Abort if there is any failure in this step.
      truncatePartialBlock(rurList, safeLength);

      // notify Namenode the new size and locations
      final DatanodeID[] newLocs = new DatanodeID[totalBlkNum];
      final String[] newStorages = new String[totalBlkNum];
      for (int i = 0; i < totalBlkNum; i++) {
        newLocs[blockIndices[i]] = DatanodeID.EMPTY_DATANODE_ID;
        newStorages[blockIndices[i]] = "";
      }
      for (BlockRecord r : rurList) {
        int index = (int) (r.rInfo.getBlockId() &
            HdfsServerConstants.BLOCK_GROUP_INDEX_MASK);
        newLocs[index] = r.id;
        newStorages[index] = r.storageID;
      }
      ExtendedBlock newBlock = new ExtendedBlock(bpid, block.getBlockId(),
          safeLength, recoveryId);
      DatanodeProtocolClientSideTranslatorPB nn = getActiveNamenodeForBP(bpid);
      nn.commitBlockSynchronization(block, newBlock.getGenerationStamp(),
          newBlock.getNumBytes(), true, false, newLocs, newStorages);
    }

    private void truncatePartialBlock(List<BlockRecord> rurList,
        long safeLength) throws IOException {
      int cellSize = ecPolicy.getCellSize();
      int dataBlkNum = ecPolicy.getNumDataUnits();
      List<DatanodeID> failedList = new ArrayList<>();
      for (BlockRecord r : rurList) {
        int blockIndex = (int) (r.rInfo.getBlockId() & BLOCK_GROUP_INDEX_MASK);
        long newSize = getInternalBlockLength(safeLength, cellSize, dataBlkNum,
            blockIndex);
        try {
          r.updateReplicaUnderRecovery(bpid, recoveryId, r.rInfo.getBlockId(),
              newSize);
        } catch (IOException e) {
          InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
              + ", datanode=" + r.id + ")", e);
          failedList.add(r.id);
        }
      }

      // If any of the data-nodes failed, the recovery fails, because
      // we never know the actual state of the replica on failed data-nodes.
      // The recovery should be started over.
      if (!failedList.isEmpty()) {
        throw new IOException("Cannot recover " + block
            + ", the following datanodes failed: " + failedList);
      }
    }

    /**
     * TODO: the current implementation depends on the assumption that the
     * parity cells are only generated based on the full stripe. This is not
     * true after we support hflush.
     */
    @VisibleForTesting
    long getSafeLength(Map<Long, BlockRecord> syncBlocks) {
      final int dataBlkNum = ecPolicy.getNumDataUnits();
      Preconditions.checkArgument(syncBlocks.size() >= dataBlkNum);
      long[] blockLengths = new long[syncBlocks.size()];
      int i = 0;
      for (BlockRecord r : syncBlocks.values()) {
        ReplicaRecoveryInfo rInfo = r.getReplicaRecoveryInfo();
        blockLengths[i++] = rInfo.getNumBytes();
      }
      return StripedBlockUtil.getSafeLength(ecPolicy, blockLengths);
    }

    private void checkLocations(int locationCount)
        throws IOException {
      if (locationCount < ecPolicy.getNumDataUnits()) {
        throw new IOException(block + " has no enough internal blocks" +
            ", unable to start recovery. Locations=" + Arrays.asList(locs));
      }
    }
  }

  private DatanodeID getDatanodeID(String bpid) throws IOException {
    BPOfferService bpos = datanode.getBPOfferService(bpid);
    if (bpos == null) {
      throw new IOException("No block pool offer service for bpid=" + bpid);
    }
    return new DatanodeID(bpos.bpRegistration);
  }

  private static void logRecoverBlock(String who, RecoveringBlock rb) {
    ExtendedBlock block = rb.getBlock();
    DatanodeInfo[] targets = rb.getLocations();

    LOG.info("BlockRecoveryWorker: " + who + " calls recoverBlock(" + block
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
            if (b.isStriped()) {
              new RecoveryTaskStriped((RecoveringStripedBlock) b).recover();
            } else {
              new RecoveryTaskContiguous(b).recover();
            }
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
