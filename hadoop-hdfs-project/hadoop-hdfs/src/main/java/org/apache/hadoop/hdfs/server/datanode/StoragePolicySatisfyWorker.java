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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
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
  private final int ioFileBufferSize;

  private final int moverThreads;
  private final ExecutorService moveExecutor;
  private final CompletionService<Void> moverExecutorCompletionService;
  private final List<Future<Void>> moverTaskFutures;

  public StoragePolicySatisfyWorker(Configuration conf, DataNode datanode) {
    this.datanode = datanode;
    this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);

    moverThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    moveExecutor = initializeBlockMoverThreadPool(moverThreads);
    moverExecutorCompletionService = new ExecutorCompletionService<>(
        moveExecutor);
    moverTaskFutures = new ArrayList<>();
    // TODO: Needs to manage the number of concurrent moves per DataNode.
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

  public void processBlockMovingTasks(long trackID,
      List<BlockMovingInfo> blockMovingInfos) {
    Future<Void> moveCallable = null;
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      assert blkMovingInfo
          .getSources().length == blkMovingInfo.getTargets().length;

      for (int i = 0; i < blkMovingInfo.getSources().length; i++) {
        BlockMovingTask blockMovingTask =
            new BlockMovingTask(blkMovingInfo.getBlock(),
            blkMovingInfo.getSources()[i],
            blkMovingInfo.getTargets()[i],
            blkMovingInfo.getTargetStorageTypes()[i]);
        moveCallable = moverExecutorCompletionService
            .submit(blockMovingTask);
        moverTaskFutures.add(moveCallable);
      }
    }

    // TODO: Presently this function act as a blocking call, this has to be
    // refined by moving the tracking logic to another tracker thread.
    for (int i = 0; i < moverTaskFutures.size(); i++) {
      try {
        moveCallable = moverExecutorCompletionService.take();
        moveCallable.get();
      } catch (InterruptedException | ExecutionException e) {
        // TODO: Failure retries and report back the error to NameNode.
        LOG.error("Exception while moving block replica to target storage type",
            e);
      }
    }
  }

  /**
   * This class encapsulates the process of moving the block replica to the
   * given target.
   */
  private class BlockMovingTask implements Callable<Void> {
    private final ExtendedBlock block;
    private final DatanodeInfo source;
    private final DatanodeInfo target;
    private final StorageType targetStorageType;

    BlockMovingTask(ExtendedBlock block, DatanodeInfo source,
        DatanodeInfo target, StorageType targetStorageType) {
      this.block = block;
      this.source = source;
      this.target = target;
      this.targetStorageType = targetStorageType;
    }

    @Override
    public Void call() {
      moveBlock();
      return null;
    }

    private void moveBlock() {
      LOG.info("Start moving block {}", block);

      LOG.debug("Start moving block:{} from src:{} to destin:{} to satisfy "
          + "storageType:{}", block, source, target, targetStorageType);
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
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
            block, EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE));

        DataEncryptionKeyFactory keyFactory = datanode
            .getDataEncryptionKeyFactoryForBlock(block);
        IOStreamPair saslStreams = datanode.getSaslClient().socketSend(sock,
            unbufOut, unbufIn, keyFactory, accessToken, target);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(
            new BufferedOutputStream(unbufOut, ioFileBufferSize));
        in = new DataInputStream(
            new BufferedInputStream(unbufIn, ioFileBufferSize));
        sendRequest(out, block, accessToken, source, targetStorageType);
        receiveResponse(in);

        LOG.debug(
            "Successfully moved block:{} from src:{} to destin:{} for"
                + " satisfying storageType:{}",
            block, source, target, targetStorageType);
      } catch (IOException e) {
        // TODO: handle failure retries
        LOG.warn(
            "Failed to move block:{} from src:{} to destin:{} to satisfy "
                + "storageType:{}",
            block, source, target, targetStorageType, e);
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
          srcDn.getDatanodeUuid(), srcDn);
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
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
    }
  }
}
