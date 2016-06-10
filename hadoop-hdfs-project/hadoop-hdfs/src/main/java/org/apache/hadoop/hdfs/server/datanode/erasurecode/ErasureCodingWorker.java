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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ErasureCodingWorker handles the erasure coding reconstruction work commands.
 * These commands would be issued from Namenode as part of Datanode's heart beat
 * response. BPOfferService delegates the work to this class for handling EC
 * commands.
 */
@InterfaceAudience.Private
public final class ErasureCodingWorker {
  private static final Logger LOG = DataNode.LOG;

  private final DataNode datanode;
  private final Configuration conf;

  private ThreadPoolExecutor stripedReconstructionPool;
  private ThreadPoolExecutor stripedReadPool;

  public ErasureCodingWorker(Configuration conf, DataNode datanode) {
    this.datanode = datanode;
    this.conf = conf;

    initializeStripedReadThreadPool(conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_THREADS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_THREADS_DEFAULT));
    initializeStripedBlkReconstructionThreadPool(conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_BLK_THREADS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_BLK_THREADS_DEFAULT));
  }

  private void initializeStripedReadThreadPool(int num) {
    LOG.debug("Using striped reads; pool threads={}", num);

    stripedReadPool = new ThreadPoolExecutor(1, num, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("stripedRead-" + threadIndex.getAndIncrement());
            return t;
          }
        },
        new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
                                        ThreadPoolExecutor e) {
            LOG.info("Execution for striped reading rejected, "
                + "Executing in current thread");
            // will run in the current thread
            super.rejectedExecution(runnable, e);
          }
        });

    stripedReadPool.allowCoreThreadTimeOut(true);
  }

  private void initializeStripedBlkReconstructionThreadPool(int num) {
    LOG.debug("Using striped block reconstruction; pool threads={}", num);
    stripedReconstructionPool = new ThreadPoolExecutor(2, num, 60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIdx = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("stripedBlockReconstruction-"
                + threadIdx.getAndIncrement());
            return t;
          }
        });
    stripedReconstructionPool.allowCoreThreadTimeOut(true);
  }

  /**
   * Handles the Erasure Coding reconstruction work commands.
   *
   * @param ecTasks BlockECReconstructionInfo
   *
   */
  public void processErasureCodingTasks(
      Collection<BlockECReconstructionInfo> ecTasks) {
    for (BlockECReconstructionInfo reconInfo : ecTasks) {
      try {
        StripedReconstructionInfo stripedReconInfo =
            new StripedReconstructionInfo(
            reconInfo.getExtendedBlock(), reconInfo.getErasureCodingPolicy(),
            reconInfo.getLiveBlockIndices(), reconInfo.getSourceDnInfos(),
            reconInfo.getTargetDnInfos(), reconInfo.getTargetStorageTypes());
        final StripedBlockReconstructor task =
            new StripedBlockReconstructor(this, stripedReconInfo);
        if (task.hasValidTargets()) {
          stripedReconstructionPool.submit(task);
        } else {
          LOG.warn("No missing internal block. Skip reconstruction for task:{}",
              reconInfo);
        }
      } catch (Throwable e) {
        LOG.warn("Failed to reconstruct striped block {}",
            reconInfo.getExtendedBlock().getLocalBlock(), e);
      }
    }
  }

  DataNode getDatanode() {
    return datanode;
  }

  Configuration getConf() {
    return conf;
  }

  ThreadPoolExecutor getStripedReadPool() {
    return stripedReadPool;
  }
}
