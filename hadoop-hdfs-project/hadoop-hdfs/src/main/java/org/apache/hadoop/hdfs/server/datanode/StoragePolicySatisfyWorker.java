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

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.sps.BlockDispatcher;
import org.apache.hadoop.hdfs.server.common.sps.BlockMovementAttemptFinished;
import org.apache.hadoop.hdfs.server.common.sps.BlockMovementStatus;
import org.apache.hadoop.hdfs.server.common.sps.BlockStorageMovementTracker;
import org.apache.hadoop.hdfs.server.common.sps.BlocksMovementsStatusHandler;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final int moverThreads;
  private final ExecutorService moveExecutor;
  private final CompletionService<BlockMovementAttemptFinished>
      moverCompletionService;
  private final BlockStorageMovementTracker movementTracker;
  private Daemon movementTrackerThread;
  private final BlockDispatcher blkDispatcher;

  public StoragePolicySatisfyWorker(Configuration conf, DataNode datanode,
      BlocksMovementsStatusHandler handler) {
    this.datanode = datanode;
    // Defaulting to 10. This is to minimize the number of move ops.
    moverThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY, 10);
    moveExecutor = initializeBlockMoverThreadPool(moverThreads);
    moverCompletionService = new ExecutorCompletionService<>(moveExecutor);
    movementTracker = new BlockStorageMovementTracker(moverCompletionService,
        handler);
    movementTrackerThread = new Daemon(movementTracker);
    movementTrackerThread.setName("BlockStorageMovementTracker");
    DNConf dnConf = datanode.getDnConf();
    int ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);
    blkDispatcher = new BlockDispatcher(dnConf.getSocketTimeout(),
        ioFileBufferSize, dnConf.getConnectToDnViaHostname());
  }

  /**
   * Start StoragePolicySatisfyWorker, which will start block movement tracker
   * thread to track the completion of block movements.
   */
  void start() {
    movementTrackerThread.start();
  }

  /**
   * Stop StoragePolicySatisfyWorker, which will terminate executor service and
   * stop block movement tracker thread.
   */
  void stop() {
    movementTracker.stopTracking();
    movementTrackerThread.interrupt();
    moveExecutor.shutdown();
    try {
      moveExecutor.awaitTermination(500, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for mover thread to terminate", e);
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
   * @param blockPoolID block pool identifier
   *
   * @param blockMovingInfos
   *          list of blocks to be moved
   */
  public void processBlockMovingTasks(final String blockPoolID,
      final Collection<BlockMovingInfo> blockMovingInfos) {
    LOG.debug("Received BlockMovingTasks {}", blockMovingInfos);
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      StorageType sourceStorageType = blkMovingInfo.getSourceStorageType();
      StorageType targetStorageType = blkMovingInfo.getTargetStorageType();
      assert sourceStorageType != targetStorageType
          : "Source and Target storage type shouldn't be same!";
      BlockMovingTask blockMovingTask = new BlockMovingTask(blockPoolID,
          blkMovingInfo);
      moverCompletionService.submit(blockMovingTask);
    }
  }

  /**
   * This class encapsulates the process of moving the block replica to the
   * given target and wait for the response.
   */
  private class BlockMovingTask implements
      Callable<BlockMovementAttemptFinished> {
    private final String blockPoolID;
    private final BlockMovingInfo blkMovingInfo;

    BlockMovingTask(String blockPoolID, BlockMovingInfo blkMovInfo) {
      this.blockPoolID = blockPoolID;
      this.blkMovingInfo = blkMovInfo;
    }

    @Override
    public BlockMovementAttemptFinished call() {
      BlockMovementStatus status = moveBlock();
      return new BlockMovementAttemptFinished(blkMovingInfo.getBlock(),
          blkMovingInfo.getSource(), blkMovingInfo.getTarget(),
          blkMovingInfo.getTargetStorageType(), status);
    }

    private BlockMovementStatus moveBlock() {
      datanode.incrementXmitsInProgress();
      ExtendedBlock eb = new ExtendedBlock(blockPoolID,
          blkMovingInfo.getBlock());
      try {
        Token<BlockTokenIdentifier> accessToken = datanode.getBlockAccessToken(
            eb, EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE),
            new StorageType[]{blkMovingInfo.getTargetStorageType()},
            new String[0]);
        DataEncryptionKeyFactory keyFactory = datanode
            .getDataEncryptionKeyFactoryForBlock(eb);

        return blkDispatcher.moveBlock(blkMovingInfo,
            datanode.getSaslClient(), eb, datanode.newSocket(),
            keyFactory, accessToken);
      } catch (IOException e) {
        // TODO: handle failure retries
        LOG.warn(
            "Failed to move block:{} from src:{} to destin:{} to satisfy "
                + "storageType:{}",
            blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
            blkMovingInfo.getTarget(), blkMovingInfo.getTargetStorageType(), e);
        return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_FAILURE;
      } finally {
        datanode.decrementXmitsInProgress();
      }
    }
  }

  /**
   * Drop the in-progress SPS work queues.
   */
  public void dropSPSWork() {
    LOG.info("Received request to drop StoragePolicySatisfierWorker queues. "
        + "So, none of the SPS Worker queued block movements will"
        + " be scheduled.");
  }
}
