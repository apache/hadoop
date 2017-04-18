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

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockPinningException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * StoragePolicySatisfyWorker handles the storage policy satisfier commands.
 * These commands would be issued from NameNode as part of Datanode's heart beat
 * response. BPOfferService delegates the work to this class for handling
 * BlockStorageMovement commands.
 */
@InterfaceAudience.Private
public class StoragePolicySatisfyWorker {

  private static final Logger LOG = LoggerFactory
      .getLogger(StoragePolicySatisfyWorker.class);

  private final DataNode datanode;
  private final int ioFileBufferSize;

  private final int moverThreads;
  private final ExecutorService moveExecutor;
  private final CompletionService<BlockMovementResult> moverCompletionService;
  private final BlocksMovementsStatusHandler handler;
  private final BlockStorageMovementTracker movementTracker;
  private Daemon movementTrackerThread;

  private long inprogressTrackIdsCheckInterval = 30 * 1000; // 30seconds.
  private long nextInprogressRecheckTime;

  public StoragePolicySatisfyWorker(Configuration conf, DataNode datanode) {
    this.datanode = datanode;
    this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);

    moverThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    moveExecutor = initializeBlockMoverThreadPool(moverThreads);
    moverCompletionService = new ExecutorCompletionService<>(moveExecutor);
    handler = new BlocksMovementsStatusHandler();
    movementTracker = new BlockStorageMovementTracker(moverCompletionService,
        handler);
    movementTrackerThread = new Daemon(movementTracker);
    movementTrackerThread.setName("BlockStorageMovementTracker");

    // Interval to check that the inprogress trackIds. The time interval is
    // proportional o the heart beat interval time period.
    final long heartbeatIntervalSeconds = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    inprogressTrackIdsCheckInterval = 5 * heartbeatIntervalSeconds;
    // update first inprogress recheck time to a future time stamp.
    nextInprogressRecheckTime = monotonicNow()
        + inprogressTrackIdsCheckInterval;

    // TODO: Needs to manage the number of concurrent moves per DataNode.
  }

  /**
   * Start StoragePolicySatisfyWorker, which will start block movement tracker
   * thread to track the completion of block movements.
   */
  void start() {
    movementTrackerThread.start();
  }

  /**
   * Stop StoragePolicySatisfyWorker, which will stop block movement tracker
   * thread.
   */
  void stop() {
    movementTrackerThread.interrupt();
    movementTracker.stopTracking();
  }

  /**
   * Timed wait to stop BlockStorageMovement tracker daemon thread.
   */
  void waitToFinishWorkerThread() {
    try {
      movementTrackerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  private ThreadPoolExecutor initializeBlockMoverThreadPool(int num) {
    LOG.debug("Block mover to satisfy storage policy; pool threads={}", num);

    ThreadPoolExecutor moverThreadPool = new ThreadPoolExecutor(1, num, 60,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);
          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("BlockMoverTask-" + threadIndex.getAndIncrement());
            return t;
          }
        }, new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
              ThreadPoolExecutor e) {
            LOG.info("Execution for block movement to satisfy storage policy"
                + " got rejected, Executing in current thread");
            // will run in the current thread.
            super.rejectedExecution(runnable, e);
          }
        });

    moverThreadPool.allowCoreThreadTimeOut(true);
    return moverThreadPool;
  }

  /**
   * Handles the given set of block movement tasks. This will iterate over the
   * block movement list and submit each block movement task asynchronously in a
   * separate thread. Each task will move the block replica to the target node &
   * wait for the completion.
   *
   * @param trackID
   *          unique tracking identifier
   * @param blockPoolID
   *          block pool ID
   * @param blockMovingInfos
   *          list of blocks to be moved
   */
  public void processBlockMovingTasks(long trackID, String blockPoolID,
      Collection<BlockMovingInfo> blockMovingInfos) {
    LOG.debug("Received BlockMovingTasks {}", blockMovingInfos);
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      assert blkMovingInfo.getSources().length == blkMovingInfo
          .getTargets().length;
      for (int i = 0; i < blkMovingInfo.getSources().length; i++) {
        DatanodeInfo target = blkMovingInfo.getTargets()[i];
        BlockMovingTask blockMovingTask = new BlockMovingTask(
            trackID, blockPoolID, blkMovingInfo.getBlock(),
            blkMovingInfo.getSources()[i], target,
            blkMovingInfo.getSourceStorageTypes()[i],
            blkMovingInfo.getTargetStorageTypes()[i]);
        Future<BlockMovementResult> moveCallable = moverCompletionService
            .submit(blockMovingTask);
        movementTracker.addBlock(trackID, moveCallable);
      }
    }
  }

  /**
   * This class encapsulates the process of moving the block replica to the
   * given target and wait for the response.
   */
  private class BlockMovingTask implements Callable<BlockMovementResult> {
    private final long trackID;
    private final String blockPoolID;
    private final Block block;
    private final DatanodeInfo source;
    private final DatanodeInfo target;
    private final StorageType srcStorageType;
    private final StorageType targetStorageType;

    BlockMovingTask(long trackID, String blockPoolID, Block block,
        DatanodeInfo source, DatanodeInfo target,
        StorageType srcStorageType, StorageType targetStorageType) {
      this.trackID = trackID;
      this.blockPoolID = blockPoolID;
      this.block = block;
      this.source = source;
      this.target = target;
      this.srcStorageType = srcStorageType;
      this.targetStorageType = targetStorageType;
    }

    @Override
    public BlockMovementResult call() {
      BlockMovementStatus status = moveBlock();
      return new BlockMovementResult(trackID, block.getBlockId(), target,
          status);
    }

    private BlockMovementStatus moveBlock() {
      LOG.info("Start moving block:{} from src:{} to destin:{} to satisfy "
              + "storageType, sourceStoragetype:{} and destinStoragetype:{}",
          block, source, target, srcStorageType, targetStorageType);
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        ExtendedBlock extendedBlock = new ExtendedBlock(blockPoolID, block);
        DNConf dnConf = datanode.getDnConf();
        String dnAddr = target.getXferAddr(dnConf.getConnectToDnViaHostname());
        sock = datanode.newSocket();
        NetUtils.connect(sock, NetUtils.createSocketAddr(dnAddr),
            dnConf.getSocketTimeout());
        sock.setSoTimeout(2 * dnConf.getSocketTimeout());
        LOG.debug("Connecting to datanode {}", dnAddr);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        Token<BlockTokenIdentifier> accessToken = datanode.getBlockAccessToken(
            extendedBlock, EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE),
            new StorageType[]{targetStorageType}, new String[0]);

        DataEncryptionKeyFactory keyFactory = datanode
            .getDataEncryptionKeyFactoryForBlock(extendedBlock);
        IOStreamPair saslStreams = datanode.getSaslClient().socketSend(sock,
            unbufOut, unbufIn, keyFactory, accessToken, target);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(
            new BufferedOutputStream(unbufOut, ioFileBufferSize));
        in = new DataInputStream(
            new BufferedInputStream(unbufIn, ioFileBufferSize));
        sendRequest(out, extendedBlock, accessToken, source, targetStorageType);
        receiveResponse(in);

        LOG.info(
            "Successfully moved block:{} from src:{} to destin:{} for"
                + " satisfying storageType:{}",
            block, source, target, targetStorageType);
        return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_SUCCESS;
      } catch (BlockPinningException e) {
        // Pinned block won't be able to move to a different node. So, its not
        // required to do retries, just marked as SUCCESS.
        LOG.debug("Pinned block can't be moved, so skipping block:{}", block,
            e);
        return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_SUCCESS;
      } catch (IOException e) {
        // TODO: handle failure retries
        LOG.warn(
            "Failed to move block:{} from src:{} to destin:{} to satisfy "
                + "storageType:{}",
            block, source, target, targetStorageType, e);
        return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_FAILURE;
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }

    /** Send a reportedBlock replace request to the output stream. */
    private void sendRequest(DataOutputStream out, ExtendedBlock eb,
        Token<BlockTokenIdentifier> accessToken, DatanodeInfo srcDn,
        StorageType destinStorageType) throws IOException {
      new Sender(out).replaceBlock(eb, destinStorageType, accessToken,
          srcDn.getDatanodeUuid(), srcDn, null);
    }

    /** Receive a reportedBlock copy response from the input stream. */
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response = BlockOpResponseProto
          .parseFrom(vintPrefixed(in));
      while (response.getStatus() == Status.IN_PROGRESS) {
        // read intermediate responses
        response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      }
      String logInfo = "reportedBlock move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo, true);
    }
  }

  /**
   * Block movement status code.
   */
  public static enum BlockMovementStatus {
    /** Success. */
    DN_BLK_STORAGE_MOVEMENT_SUCCESS(0),
    /**
     * Failure due to generation time stamp mismatches or network errors
     * or no available space.
     */
    DN_BLK_STORAGE_MOVEMENT_FAILURE(-1);

    // TODO: need to support different type of failures. Failure due to network
    // errors, block pinned, no space available etc.

    private final int code;

    private BlockMovementStatus(int code) {
      this.code = code;
    }

    /**
     * @return the status code.
     */
    int getStatusCode() {
      return code;
    }
  }

  /**
   * This class represents result from a block movement task. This will have the
   * information of the task which was successful or failed due to errors.
   */
  static class BlockMovementResult {
    private final long trackId;
    private final long blockId;
    private final DatanodeInfo target;
    private final BlockMovementStatus status;

    public BlockMovementResult(long trackId, long blockId,
        DatanodeInfo target, BlockMovementStatus status) {
      this.trackId = trackId;
      this.blockId = blockId;
      this.target = target;
      this.status = status;
    }

    long getTrackId() {
      return trackId;
    }

    long getBlockId() {
      return blockId;
    }

    BlockMovementStatus getStatus() {
      return status;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("Block movement result(\n  ")
          .append("track id: ").append(trackId).append(" block id: ")
          .append(blockId).append(" target node: ").append(target)
          .append(" movement status: ").append(status).append(")").toString();
    }
  }

  /**
   * Blocks movements status handler, which is used to collect details of the
   * completed or inprogress list of block movements and this status(success or
   * failure or inprogress) will be send to the namenode via heartbeat.
   */
  class BlocksMovementsStatusHandler {
    private final List<BlocksStorageMovementResult> trackIdVsMovementStatus =
        new ArrayList<>();

    /**
     * Collect all the block movement results. Later this will be send to
     * namenode via heart beat.
     *
     * @param results
     *          result of all the block movements per trackId
     */
    void handle(List<BlockMovementResult> resultsPerTrackId) {
      BlocksStorageMovementResult.Status status =
          BlocksStorageMovementResult.Status.SUCCESS;
      long trackId = -1;
      for (BlockMovementResult blockMovementResult : resultsPerTrackId) {
        trackId = blockMovementResult.getTrackId();
        if (blockMovementResult.status ==
            BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_FAILURE) {
          status = BlocksStorageMovementResult.Status.FAILURE;
          // If any of the block movement is failed, then mark as failure so
          // that namenode can take a decision to retry the blocks associated to
          // the given trackId.
          break;
        }
      }

      // Adding to the tracking results list. Later this will be send to
      // namenode via datanode heartbeat.
      synchronized (trackIdVsMovementStatus) {
        trackIdVsMovementStatus.add(
            new BlocksStorageMovementResult(trackId, status));
      }
    }

    /**
     * @return unmodifiable list of blocks storage movement results.
     */
    List<BlocksStorageMovementResult> getBlksMovementResults() {
      List<BlocksStorageMovementResult> movementResults = new ArrayList<>();
      // 1. Adding all the completed trackids.
      synchronized (trackIdVsMovementStatus) {
        if (trackIdVsMovementStatus.size() > 0) {
          movementResults = Collections
              .unmodifiableList(trackIdVsMovementStatus);
        }
      }
      // 2. Adding the in progress track ids after those which are completed.
      Set<Long> inProgressTrackIds = getInProgressTrackIds();
      for (Long trackId : inProgressTrackIds) {
        movementResults.add(new BlocksStorageMovementResult(trackId,
            BlocksStorageMovementResult.Status.IN_PROGRESS));
      }
      return movementResults;
    }

    /**
     * Remove the blocks storage movement results.
     *
     * @param results
     *          set of blocks storage movement results
     */
    void remove(BlocksStorageMovementResult[] results) {
      if (results != null) {
        synchronized (trackIdVsMovementStatus) {
          for (BlocksStorageMovementResult blocksMovementResult : results) {
            trackIdVsMovementStatus.remove(blocksMovementResult);
          }
        }
      }
    }

    /**
     * Clear the trackID vs movement status tracking map.
     */
    void removeAll() {
      synchronized (trackIdVsMovementStatus) {
        trackIdVsMovementStatus.clear();
      }
    }

  }

  @VisibleForTesting
  BlocksMovementsStatusHandler getBlocksMovementsStatusHandler() {
    return handler;
  }

  /**
   * Drop the in-progress SPS work queues.
   */
  public void dropSPSWork() {
    LOG.info("Received request to drop StoragePolicySatisfierWorker queues. "
        + "So, none of the SPS Worker queued block movements will"
        + " be scheduled.");
    movementTracker.removeAll();
    handler.removeAll();
  }

  /**
   * Gets list of trackids which are inprogress. Will do collection periodically
   * on 'dfs.datanode.storage.policy.satisfier.worker.inprogress.recheck.time.
   * millis' interval.
   *
   * @return collection of trackids which are inprogress
   */
  private Set<Long> getInProgressTrackIds() {
    Set<Long> trackIds = new HashSet<>();
    long now = monotonicNow();
    if (nextInprogressRecheckTime >= now) {
      trackIds = movementTracker.getInProgressTrackIds();

      // schedule next re-check interval
      nextInprogressRecheckTime = now + inprogressTrackIdsCheckInterval;
    }
    return trackIds;
  }
}
