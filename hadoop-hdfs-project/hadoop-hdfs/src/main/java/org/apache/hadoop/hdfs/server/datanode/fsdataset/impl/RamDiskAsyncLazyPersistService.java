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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is a container of multiple thread pools, one for each non-RamDisk
 * volume with a maximum thread count of 1 so that we can schedule async lazy
 * persist operations easily with volume arrival and departure handled.
 *
 * This class and {@link org.apache.hadoop.util.AsyncDiskService} are similar.
 * They should be combined.
 */
class RamDiskAsyncLazyPersistService {
  public static final Logger LOG =
      LoggerFactory.getLogger(RamDiskAsyncLazyPersistService.class);

  // ThreadPool core pool size
  private static final int CORE_THREADS_PER_VOLUME = 1;
  // ThreadPool maximum pool size
  private static final int MAXIMUM_THREADS_PER_VOLUME = 1;
  // ThreadPool keep-alive time for threads over core pool size
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;

  private final DataNode datanode;
  private final Configuration conf;

  private final ThreadGroup threadGroup;
  private Map<String, ThreadPoolExecutor> executors
      = new HashMap<String, ThreadPoolExecutor>();
  private final static HdfsConfiguration EMPTY_HDFS_CONF = new HdfsConfiguration();

  /**
   * Create a RamDiskAsyncLazyPersistService with a set of volumes (specified by their
   * root directories).
   *
   * The RamDiskAsyncLazyPersistService uses one ThreadPool per volume to do the async
   * disk operations.
   */
  RamDiskAsyncLazyPersistService(DataNode datanode, Configuration conf) {
    this.datanode = datanode;
    this.conf = conf;
    this.threadGroup = new ThreadGroup(getClass().getSimpleName());
  }

  private void addExecutorForVolume(final String storageId) {
    ThreadFactory threadFactory = new ThreadFactory() {

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(threadGroup, r);
        t.setName("Async RamDisk lazy persist worker " +
            " for volume with id " + storageId);
        return t;
      }
    };

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        CORE_THREADS_PER_VOLUME, MAXIMUM_THREADS_PER_VOLUME,
        THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), threadFactory);

    // This can reduce the number of running threads
    executor.allowCoreThreadTimeOut(true);
    executors.put(storageId, executor);
  }

  /**
   * Starts AsyncLazyPersistService for a new volume
   * @param volume the root of the new data volume.
   */
  synchronized void addVolume(FsVolumeImpl volume) {
    String storageId = volume.getStorageID();
    if (executors == null) {
      throw new RuntimeException("AsyncLazyPersistService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    if (executor != null) {
      throw new RuntimeException("Volume " + volume + " is already existed.");
    }
    addExecutorForVolume(storageId);
  }

  /**
   * Stops AsyncLazyPersistService for a volume.
   * @param volume the root of the volume.
   */
  synchronized void removeVolume(FsVolumeImpl volume) {
    String storageId = volume.getStorageID();
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    if (executor == null) {
      throw new RuntimeException("Can not find volume with storage id " +
          storageId + " to remove.");
    } else {
      executor.shutdown();
      executors.remove(storageId);
    }
  }

  /**
   * Query if the thread pool exist for the volume
   * @param volume the root of a volume
   * @return true if there is one thread pool for the volume
   *         false otherwise
   */
  synchronized boolean queryVolume(FsVolumeImpl volume) {
    String storageId = volume.getStorageID();
    if (executors == null) {
      throw new RuntimeException(
          "AsyncLazyPersistService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    return (executor != null);
  }

  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  synchronized void execute(String storageId, Runnable task) {
    try {
      if (executors == null) {
        throw new RuntimeException(
            "AsyncLazyPersistService is already shutdown");
      }
      ThreadPoolExecutor executor = executors.get(storageId);
      if (executor == null) {
        throw new RuntimeException("Cannot find root storage volume with id " +
            storageId + " for execution of task " + task);
      } else {
        executor.execute(task);
      }
    } catch (RuntimeException re) {
      if (task instanceof ReplicaLazyPersistTask) {
        IOUtils.cleanupWithLogger(null,
            ((ReplicaLazyPersistTask) task).targetVolume);
      }
      throw re;
    }
  }

  /**
   * Gracefully shut down all ThreadPool. Will wait for all lazy persist
   * tasks to finish.
   */
  synchronized void shutdown() {
    if (executors == null) {
      LOG.warn("AsyncLazyPersistService has already shut down.");
    } else {
      LOG.info("Shutting down all async lazy persist service threads");

      for (Map.Entry<String, ThreadPoolExecutor> e : executors.entrySet()) {
        e.getValue().shutdown();
      }
      // clear the executor map so that calling execute again will fail.
      executors = null;
      LOG.info("All async lazy persist service threads have been shut down");
    }
  }

  /**
   * Asynchronously lazy persist the block from the RamDisk to Disk.
   */
  void submitLazyPersistTask(String bpId, long blockId,
      long genStamp, long creationTime,
      ReplicaInfo replica, FsVolumeReference target) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("LazyWriter schedule async task to persist RamDisk block pool id: "
          + bpId + " block id: " + blockId);
    }

    ReplicaLazyPersistTask lazyPersistTask = new ReplicaLazyPersistTask(
        bpId, blockId, genStamp, creationTime, replica, target);

    FsVolumeImpl volume = (FsVolumeImpl)target.getVolume();
    execute(volume.getStorageID(), lazyPersistTask);
  }

  class ReplicaLazyPersistTask implements Runnable {
    private final String bpId;
    private final long blockId;
    private final long genStamp;
    private final long creationTime;
    private final ReplicaInfo replicaInfo;
    private final FsVolumeReference targetVolume;

    ReplicaLazyPersistTask(String bpId, long blockId,
        long genStamp, long creationTime,
        ReplicaInfo replicaInfo,
        FsVolumeReference targetVolume) {
      this.bpId = bpId;
      this.blockId = blockId;
      this.genStamp = genStamp;
      this.creationTime = creationTime;
      this.replicaInfo = replicaInfo;
      this.targetVolume = targetVolume;
    }

    @Override
    public String toString() {
      // Called in AsyncLazyPersistService.execute for displaying error messages.
      return "LazyWriter async task of persist RamDisk block pool id:"
          + bpId + " block pool id: "
          + blockId + " with block file " + replicaInfo.getBlockURI()
          + " and meta file " + replicaInfo.getMetadataURI()
          + " to target volume " + targetVolume;
    }

    @Override
    public void run() {
      boolean succeeded = false;
      final FsDatasetImpl dataset = (FsDatasetImpl)datanode.getFSDataset();
      try (FsVolumeReference ref = this.targetVolume) {
        int smallBufferSize = DFSUtilClient.getSmallBufferSize(EMPTY_HDFS_CONF);

        FsVolumeImpl volume = (FsVolumeImpl)ref.getVolume();
        File[] targetFiles = volume.copyBlockToLazyPersistLocation(bpId,
            blockId, genStamp, replicaInfo, smallBufferSize, conf);

        // Lock FsDataSetImpl during onCompleteLazyPersist callback
        dataset.onCompleteLazyPersist(bpId, blockId,
                creationTime, targetFiles, volume);
        succeeded = true;
      } catch (Exception e){
        FsDatasetImpl.LOG.warn(
            "LazyWriter failed to async persist RamDisk block pool id: "
            + bpId + "block Id: " + blockId, e);
      } finally {
        if (!succeeded) {
          dataset.onFailLazyPersist(bpId, blockId);
        }
      }
    }
  }
}
