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

package org.apache.hadoop.hdfs.server.sps;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.sps.BlockDispatcher;
import org.apache.hadoop.hdfs.server.common.sps.BlockMovementAttemptFinished;
import org.apache.hadoop.hdfs.server.common.sps.BlockMovementStatus;
import org.apache.hadoop.hdfs.server.common.sps.BlockStorageMovementTracker;
import org.apache.hadoop.hdfs.server.common.sps.BlocksMovementsStatusHandler;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMoveTaskHandler;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the external SPS block movements. This will move the
 * given block to a target datanode by directly establishing socket connection
 * to it and invokes function
 * {@link Sender#replaceBlock(ExtendedBlock, StorageType, Token, String,
 * DatanodeInfo, String)}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExternalSPSBlockMoveTaskHandler implements BlockMoveTaskHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(ExternalSPSBlockMoveTaskHandler.class);

  private final ExecutorService moveExecutor;
  private final CompletionService<BlockMovementAttemptFinished> mCompletionServ;
  private final NameNodeConnector nnc;
  private final SaslDataTransferClient saslClient;
  private final BlockStorageMovementTracker blkMovementTracker;
  private Daemon movementTrackerThread;
  private final SPSService service;
  private final BlockDispatcher blkDispatcher;

  public ExternalSPSBlockMoveTaskHandler(Configuration conf,
      NameNodeConnector nnc, SPSService spsService) {
    int moverThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    moveExecutor = initializeBlockMoverThreadPool(moverThreads);
    mCompletionServ = new ExecutorCompletionService<>(moveExecutor);
    this.nnc = nnc;
    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf),
        nnc.getFallbackToSimpleAuth());
    this.blkMovementTracker = new BlockStorageMovementTracker(
        mCompletionServ, new ExternalBlocksMovementsStatusHandler());
    this.service = spsService;

    boolean connectToDnViaHostname = conf.getBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,
        HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    int ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);
    blkDispatcher = new BlockDispatcher(HdfsConstants.READ_TIMEOUT,
        ioFileBufferSize, connectToDnViaHostname);

    startMovementTracker();
  }

  /**
   * Initializes block movement tracker daemon and starts the thread.
   */
  private void startMovementTracker() {
    movementTrackerThread = new Daemon(this.blkMovementTracker);
    movementTrackerThread.setName("BlockStorageMovementTracker");
    movementTrackerThread.start();
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

  @Override
  public void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException {
    // TODO: Need to increment scheduled block size on the target node. This
    // count will be used to calculate the remaining space of target datanode
    // during block movement assignment logic. In the internal movement,
    // remaining space is bookkeeping at the DatanodeDescriptor, please refer
    // IntraSPSNameNodeBlockMoveTaskHandler#submitMoveTask implementation and
    // updating via the funcation call -
    // dn.incrementBlocksScheduled(blkMovingInfo.getTargetStorageType());
    LOG.debug("Received BlockMovingTask {}", blkMovingInfo);
    BlockMovingTask blockMovingTask = new BlockMovingTask(blkMovingInfo);
    mCompletionServ.submit(blockMovingTask);
  }

  private class ExternalBlocksMovementsStatusHandler
      implements BlocksMovementsStatusHandler {
    @Override
    public void handle(BlockMovementAttemptFinished attemptedMove) {
      service.notifyStorageMovementAttemptFinishedBlk(
          attemptedMove.getTargetDatanode(), attemptedMove.getTargetType(),
          attemptedMove.getBlock());
    }
  }

  /**
   * This class encapsulates the process of moving the block replica to the
   * given target.
   */
  private class BlockMovingTask
      implements Callable<BlockMovementAttemptFinished> {
    private final BlockMovingInfo blkMovingInfo;

    BlockMovingTask(BlockMovingInfo blkMovingInfo) {
      this.blkMovingInfo = blkMovingInfo;
    }

    @Override
    public BlockMovementAttemptFinished call() {
      BlockMovementStatus blkMovementStatus = moveBlock();
      return new BlockMovementAttemptFinished(blkMovingInfo.getBlock(),
          blkMovingInfo.getSource(), blkMovingInfo.getTarget(),
          blkMovingInfo.getTargetStorageType(),
          blkMovementStatus);
    }

    private BlockMovementStatus moveBlock() {
      ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
          blkMovingInfo.getBlock());

      final KeyManager km = nnc.getKeyManager();
      Token<BlockTokenIdentifier> accessToken;
      try {
        accessToken = km.getAccessToken(eb,
            new StorageType[]{blkMovingInfo.getTargetStorageType()},
            new String[0]);
      } catch (IOException e) {
        // TODO: handle failure retries
        LOG.warn(
            "Failed to move block:{} from src:{} to destin:{} to satisfy "
                + "storageType:{}",
            blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
            blkMovingInfo.getTarget(), blkMovingInfo.getTargetStorageType(), e);
        return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_FAILURE;
      }
      return blkDispatcher.moveBlock(blkMovingInfo, saslClient, eb,
          new Socket(), km, accessToken);
    }
  }

  /**
   * Cleanup the resources.
   */
  void cleanUp() {
    blkMovementTracker.stopTracking();
    if (movementTrackerThread != null) {
      movementTrackerThread.interrupt();
    }
  }
}
