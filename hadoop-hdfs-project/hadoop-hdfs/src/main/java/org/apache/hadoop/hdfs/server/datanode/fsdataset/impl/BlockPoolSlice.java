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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Timer;

import com.google.common.annotations.VisibleForTesting;

/**
 * A block pool slice represents a portion of a block pool stored on a volume.
 * Taken together, all BlockPoolSlices sharing a block pool ID across a
 * cluster represent a single block pool.
 *
 * This class is synchronized by {@link FsVolumeImpl}.
 */
class BlockPoolSlice {
  static final Log LOG = LogFactory.getLog(BlockPoolSlice.class);

  private final String bpid;
  private final FsVolumeImpl volume; // volume to which this BlockPool belongs to
  private final File currentDir; // StorageDirectory/current/bpid/current
  // directory where finalized replicas are stored
  private final File finalizedDir;
  private final File lazypersistDir;
  private final File rbwDir; // directory store RBW replica
  private final File tmpDir; // directory store Temporary replica
  private final int ioFileBufferSize;
  @VisibleForTesting
  public static final String DU_CACHE_FILE = "dfsUsed";
  private final Runnable shutdownHook;
  private volatile boolean dfsUsedSaved = false;
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private final boolean deleteDuplicateReplicas;
  private static final String REPLICA_CACHE_FILE = "replicas";
  private final long replicaCacheExpiry = 5*60*1000;
  private AtomicLong numOfBlocks = new AtomicLong();
  private final long cachedDfsUsedCheckTime;
  private final Timer timer;
  private final int maxDataLength;
  private final FileIoProvider fileIoProvider;

  private static ForkJoinPool addReplicaThreadPool = null;
  private static final int VOLUMES_REPLICA_ADD_THREADPOOL_SIZE = Runtime
      .getRuntime().availableProcessors();
  private static final Comparator<File> FILE_COMPARATOR =
      new Comparator<File>() {
    @Override
    public int compare(File f1, File f2) {
      return f1.getName().compareTo(f2.getName());
    }
  };

  // TODO:FEDERATION scalability issue - a thread per DU is needed
  private final GetSpaceUsed dfsUsage;

  /**
   * Create a blook pool slice
   * @param bpid Block pool Id
   * @param volume {@link FsVolumeImpl} to which this BlockPool belongs to
   * @param bpDir directory corresponding to the BlockPool
   * @param conf configuration
   * @param timer include methods for getting time
   * @throws IOException
   */
  BlockPoolSlice(String bpid, FsVolumeImpl volume, File bpDir,
      Configuration conf, Timer timer) throws IOException {
    this.bpid = bpid;
    this.volume = volume;
    this.fileIoProvider = volume.getFileIoProvider();
    this.currentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    this.finalizedDir = new File(
        currentDir, DataStorage.STORAGE_DIR_FINALIZED);
    this.lazypersistDir = new File(currentDir, DataStorage.STORAGE_DIR_LAZY_PERSIST);
    if (!this.finalizedDir.exists()) {
      if (!this.finalizedDir.mkdirs()) {
        throw new IOException("Failed to mkdirs " + this.finalizedDir);
      }
    }

    this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);

    this.deleteDuplicateReplicas = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION,
        DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION_DEFAULT);

    this.cachedDfsUsedCheckTime =
        conf.getLong(
            DFSConfigKeys.DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS,
            DFSConfigKeys.DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_DEFAULT_MS);

    this.maxDataLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);

    this.timer = timer;

    // Files that were being written when the datanode was last shutdown
    // are now moved back to the data directory. It is possible that
    // in the future, we might want to do some sort of datanode-local
    // recovery for these blocks. For example, crc validation.
    //
    this.tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
    if (tmpDir.exists()) {
      fileIoProvider.fullyDelete(volume, tmpDir);
    }
    this.rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);

    // create the rbw and tmp directories if they don't exist.
    fileIoProvider.mkdirs(volume, rbwDir);
    fileIoProvider.mkdirs(volume, tmpDir);

    // Use cached value initially if available. Or the following call will
    // block until the initial du command completes.
    this.dfsUsage = new CachingGetSpaceUsed.Builder().setPath(bpDir)
                                                     .setConf(conf)
                                                     .setInitialUsed(loadDfsUsed())
                                                     .build();
    if (addReplicaThreadPool == null) {
      // initialize add replica fork join pool
      initializeAddReplicaPool(conf);
    }
    // Make the dfs usage to be saved during shutdown.
    shutdownHook = new Runnable() {
      @Override
      public void run() {
        if (!dfsUsedSaved) {
          saveDfsUsed();
          addReplicaThreadPool.shutdownNow();
        }
      }
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
  }

  private synchronized void initializeAddReplicaPool(Configuration conf) {
    if (addReplicaThreadPool == null) {
      FsDatasetImpl dataset = (FsDatasetImpl) volume.getDataset();
      int numberOfBlockPoolSlice = dataset.getVolumeCount()
          * dataset.getBPServiceCount();
      int poolsize = Math.max(numberOfBlockPoolSlice,
          VOLUMES_REPLICA_ADD_THREADPOOL_SIZE);
      // Default pool sizes is max of (volume * number of bp_service) and
      // number of processor.
      addReplicaThreadPool = new ForkJoinPool(conf.getInt(
          DFSConfigKeys.DFS_DATANODE_VOLUMES_REPLICA_ADD_THREADPOOL_SIZE_KEY,
          poolsize));
    }
  }

  File getDirectory() {
    return currentDir.getParentFile();
  }

  File getFinalizedDir() {
    return finalizedDir;
  }

  File getLazypersistDir() {
    return lazypersistDir;
  }

  File getRbwDir() {
    return rbwDir;
  }

  File getTmpDir() {
    return tmpDir;
  }

  /** Run DU on local drives.  It must be synchronized from caller. */
  void decDfsUsed(long value) {
    if (dfsUsage instanceof CachingGetSpaceUsed) {
      ((CachingGetSpaceUsed)dfsUsage).incDfsUsed(-value);
    }
  }

  long getDfsUsed() throws IOException {
    return dfsUsage.getUsed();
  }

  void incDfsUsed(long value) {
    if (dfsUsage instanceof CachingGetSpaceUsed) {
      ((CachingGetSpaceUsed)dfsUsage).incDfsUsed(value);
    }
  }

  /**
   * Read in the cached DU value and return it if it is less than
   * cachedDfsUsedCheckTime which is set by
   * dfs.datanode.cached-dfsused.check.interval.ms parameter. Slight imprecision
   * of dfsUsed is not critical and skipping DU can significantly shorten the
   * startup time. If the cached value is not available or too old, -1 is
   * returned.
   */
  long loadDfsUsed() {
    long cachedDfsUsed;
    long mtime;
    Scanner sc;

    try {
      sc = new Scanner(new File(currentDir, DU_CACHE_FILE), "UTF-8");
    } catch (FileNotFoundException fnfe) {
      return -1;
    }

    try {
      // Get the recorded dfsUsed from the file.
      if (sc.hasNextLong()) {
        cachedDfsUsed = sc.nextLong();
      } else {
        return -1;
      }
      // Get the recorded mtime from the file.
      if (sc.hasNextLong()) {
        mtime = sc.nextLong();
      } else {
        return -1;
      }

      // Return the cached value if mtime is okay.
      if (mtime > 0 && (timer.now() - mtime < cachedDfsUsedCheckTime)) {
        FsDatasetImpl.LOG.info("Cached dfsUsed found for " + currentDir + ": " +
            cachedDfsUsed);
        return cachedDfsUsed;
      }
      return -1;
    } finally {
      sc.close();
    }
  }

  /**
   * Write the current dfsUsed to the cache file.
   */
  void saveDfsUsed() {
    File outFile = new File(currentDir, DU_CACHE_FILE);
    if (!fileIoProvider.deleteWithExistsCheck(volume, outFile)) {
      FsDatasetImpl.LOG.warn("Failed to delete old dfsUsed file in " +
        outFile.getParent());
    }

    try {
      long used = getDfsUsed();
      try (Writer out = new OutputStreamWriter(
          new FileOutputStream(outFile), "UTF-8")) {
        // mtime is written last, so that truncated writes won't be valid.
        out.write(Long.toString(used) + " " + Long.toString(timer.now()));
        // This is only called as part of the volume shutdown.
        // We explicitly avoid calling flush with fileIoProvider which triggers
        // volume check upon io exception to avoid cyclic volume checks.
        out.flush();
      }
    } catch (IOException ioe) {
      // If write failed, the volume might be bad. Since the cache file is
      // not critical, log the error and continue.
      FsDatasetImpl.LOG.warn("Failed to write dfsUsed to " + outFile, ioe);
    }
  }

  /**
   * Temporary files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createTmpFile(Block b) throws IOException {
    File f = new File(tmpDir, b.getBlockName());
    File tmpFile = DatanodeUtil.createFileWithExistsCheck(
        volume, b, f, fileIoProvider);
    // If any exception during creation, its expected that counter will not be
    // incremented, So no need to decrement
    incrNumBlocks();
    return tmpFile;
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(Block b) throws IOException {
    File f = new File(rbwDir, b.getBlockName());
    File rbwFile = DatanodeUtil.createFileWithExistsCheck(
        volume, b, f, fileIoProvider);
    // If any exception during creation, its expected that counter will not be
    // incremented, So no need to decrement
    incrNumBlocks();
    return rbwFile;
  }

  File addFinalizedBlock(Block b, ReplicaInfo replicaInfo) throws IOException {
    File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, b.getBlockId());
    fileIoProvider.mkdirsWithExistsCheck(volume, blockDir);
    File blockFile = FsDatasetImpl.moveBlockFiles(b, replicaInfo, blockDir);
    File metaFile = FsDatasetUtil.getMetaFile(blockFile, b.getGenerationStamp());
    if (dfsUsage instanceof CachingGetSpaceUsed) {
      ((CachingGetSpaceUsed) dfsUsage).incDfsUsed(
          b.getNumBytes() + metaFile.length());
    }
    return blockFile;
  }

  /**
   * Move a persisted replica from lazypersist directory to a subdirectory
   * under finalized.
   */
  ReplicaInfo activateSavedReplica(ReplicaInfo replicaInfo,
      RamDiskReplica replicaState) throws IOException {
    File metaFile = replicaState.getSavedMetaFile();
    File blockFile = replicaState.getSavedBlockFile();
    final long blockId = replicaInfo.getBlockId();
    final File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, blockId);
    final File targetBlockFile = new File(blockDir, blockFile.getName());
    final File targetMetaFile = new File(blockDir, metaFile.getName());
    fileIoProvider.moveFile(volume, blockFile, targetBlockFile);
    FsDatasetImpl.LOG.info("Moved " + blockFile + " to " + targetBlockFile);
    fileIoProvider.moveFile(volume, metaFile, targetMetaFile);
    FsDatasetImpl.LOG.info("Moved " + metaFile + " to " + targetMetaFile);

    ReplicaInfo newReplicaInfo =
        new ReplicaBuilder(ReplicaState.FINALIZED)
        .setBlockId(blockId)
        .setLength(replicaInfo.getBytesOnDisk())
        .setGenerationStamp(replicaInfo.getGenerationStamp())
        .setFsVolume(replicaState.getLazyPersistVolume())
        .setDirectoryToUse(targetBlockFile.getParentFile())
        .build();
    return newReplicaInfo;
  }

  void checkDirs() throws DiskErrorException {
    DiskChecker.checkDir(finalizedDir);
    DiskChecker.checkDir(tmpDir);
    DiskChecker.checkDir(rbwDir);
  }



  void getVolumeMap(ReplicaMap volumeMap,
                    final RamDiskReplicaTracker lazyWriteReplicaMap)
      throws IOException {
    // Recover lazy persist replicas, they will be added to the volumeMap
    // when we scan the finalized directory.
    if (lazypersistDir.exists()) {
      int numRecovered = moveLazyPersistReplicasToFinalized(lazypersistDir);
      FsDatasetImpl.LOG.info(
          "Recovered " + numRecovered + " replicas from " + lazypersistDir);
    }

    boolean  success = readReplicasFromCache(volumeMap, lazyWriteReplicaMap);
    if (!success) {
      List<IOException> exceptions = Collections
          .synchronizedList(new ArrayList<IOException>());
      Queue<RecursiveAction> subTaskQueue =
          new ConcurrentLinkedQueue<RecursiveAction>();

      // add finalized replicas
      AddReplicaProcessor task = new AddReplicaProcessor(volumeMap,
          finalizedDir, lazyWriteReplicaMap, true, exceptions, subTaskQueue);
      ForkJoinTask<Void> finalizedTask = addReplicaThreadPool.submit(task);

      // add rbw replicas
      task = new AddReplicaProcessor(volumeMap, rbwDir, lazyWriteReplicaMap,
          false, exceptions, subTaskQueue);
      ForkJoinTask<Void> rbwTask = addReplicaThreadPool.submit(task);

      try {
        finalizedTask.get();
        rbwTask.get();
      } catch (InterruptedException | ExecutionException e) {
        exceptions.add(new IOException(
            "Failed to start sub tasks to add replica in replica map :"
                + e.getMessage()));
      }

      //wait for all the tasks to finish.
      waitForSubTaskToFinish(subTaskQueue, exceptions);
    }
  }

  /**
   * Wait till all the recursive task for add replica to volume completed.
   *
   * @param subTaskQueue
   *          {@link AddReplicaProcessor} tasks list.
   * @param exceptions
   *          exceptions occurred in sub tasks.
   * @throws IOException
   *           throw if any sub task or multiple sub tasks failed.
   */
  private void waitForSubTaskToFinish(Queue<RecursiveAction> subTaskQueue,
      List<IOException> exceptions) throws IOException {
    while (!subTaskQueue.isEmpty()) {
      RecursiveAction task = subTaskQueue.poll();
      if (task != null) {
        task.join();
      }
    }
    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }

  /**
   * Recover an unlinked tmp file on datanode restart. If the original block
   * does not exist, then the tmp file is renamed to be the
   * original file name and the original name is returned; otherwise the tmp
   * file is deleted and null is returned.
   */
  File recoverTempUnlinkedBlock(File unlinkedTmp) throws IOException {
    File blockFile = FsDatasetUtil.getOrigFile(unlinkedTmp);
    if (blockFile.exists()) {
      // If the original block file still exists, then no recovery is needed.
      if (!fileIoProvider.delete(volume, unlinkedTmp)) {
        throw new IOException("Unable to cleanup unlinked tmp file " +
            unlinkedTmp);
      }
      return null;
    } else {
      fileIoProvider.rename(volume, unlinkedTmp, blockFile);
      return blockFile;
    }
  }


  /**
   * Move replicas in the lazy persist directory to their corresponding locations
   * in the finalized directory.
   * @return number of replicas recovered.
   */
  private int moveLazyPersistReplicasToFinalized(File source)
      throws IOException {
    File[] files = fileIoProvider.listFiles(volume, source);
    int numRecovered = 0;
    for (File file : files) {
      if (file.isDirectory()) {
        numRecovered += moveLazyPersistReplicasToFinalized(file);
      }

      if (Block.isMetaFilename(file.getName())) {
        File metaFile = file;
        File blockFile = Block.metaToBlockFile(metaFile);
        long blockId = Block.filename2id(blockFile.getName());
        File targetDir = DatanodeUtil.idToBlockDir(finalizedDir, blockId);

        if (blockFile.exists()) {

          try {
            fileIoProvider.mkdirsWithExistsCheck(volume, targetDir);
          } catch(IOException ioe) {
            LOG.warn("Failed to mkdirs " + targetDir);
            continue;
          }

          final File targetMetaFile = new File(targetDir, metaFile.getName());
          try {
            fileIoProvider.rename(volume, metaFile, targetMetaFile);
          } catch (IOException e) {
            LOG.warn("Failed to move meta file from "
                + metaFile + " to " + targetMetaFile, e);
            continue;
          }

          final File targetBlockFile = new File(targetDir, blockFile.getName());
          try {
            fileIoProvider.rename(volume, blockFile, targetBlockFile);
          } catch (IOException e) {
            LOG.warn("Failed to move block file from "
                + blockFile + " to " + targetBlockFile, e);
            continue;
          }

          if (targetBlockFile.exists() && targetMetaFile.exists()) {
            ++numRecovered;
          } else {
            // Failure should be rare.
            LOG.warn("Failed to move " + blockFile + " to " + targetDir);
          }
        }
      }
    }

    fileIoProvider.fullyDelete(volume, source);
    return numRecovered;
  }

  private void addReplicaToReplicasMap(Block block, ReplicaMap volumeMap,
      final RamDiskReplicaTracker lazyWriteReplicaMap,boolean isFinalized)
      throws IOException {
    ReplicaInfo newReplica = null;
    long blockId = block.getBlockId();
    long genStamp = block.getGenerationStamp();
    if (isFinalized) {
      newReplica = new ReplicaBuilder(ReplicaState.FINALIZED)
          .setBlockId(blockId)
          .setLength(block.getNumBytes())
          .setGenerationStamp(genStamp)
          .setFsVolume(volume)
          .setDirectoryToUse(DatanodeUtil.idToBlockDir(finalizedDir, blockId))
          .build();
    } else {
      File file = new File(rbwDir, block.getBlockName());
      boolean loadRwr = true;
      File restartMeta = new File(file.getParent()  +
          File.pathSeparator + "." + file.getName() + ".restart");
      Scanner sc = null;
      try {
        sc = new Scanner(restartMeta, "UTF-8");
        // The restart meta file exists
        if (sc.hasNextLong() && (sc.nextLong() > timer.now())) {
          // It didn't expire. Load the replica as a RBW.
          // We don't know the expected block length, so just use 0
          // and don't reserve any more space for writes.
          newReplica = new ReplicaBuilder(ReplicaState.RBW)
              .setBlockId(blockId)
              .setLength(validateIntegrityAndSetLength(file, genStamp))
              .setGenerationStamp(genStamp)
              .setFsVolume(volume)
              .setDirectoryToUse(file.getParentFile())
              .setWriterThread(null)
              .setBytesToReserve(0)
              .build();
          loadRwr = false;
        }
        sc.close();
        if (!fileIoProvider.delete(volume, restartMeta)) {
          FsDatasetImpl.LOG.warn("Failed to delete restart meta file: " +
              restartMeta.getPath());
        }
      } catch (FileNotFoundException fnfe) {
        // nothing to do hereFile dir =
      } finally {
        if (sc != null) {
          sc.close();
        }
      }
      // Restart meta doesn't exist or expired.
      if (loadRwr) {
        ReplicaBuilder builder = new ReplicaBuilder(ReplicaState.RWR)
            .setBlockId(blockId)
            .setLength(validateIntegrityAndSetLength(file, genStamp))
            .setGenerationStamp(genStamp)
            .setFsVolume(volume)
            .setDirectoryToUse(file.getParentFile());
        newReplica = builder.build();
      }
    }

    ReplicaInfo tmpReplicaInfo = volumeMap.addAndGet(bpid, newReplica);
    ReplicaInfo oldReplica = (tmpReplicaInfo == newReplica) ? null
        : tmpReplicaInfo;
    if (oldReplica != null) {
      // We have multiple replicas of the same block so decide which one
      // to keep.
      newReplica = resolveDuplicateReplicas(newReplica, oldReplica, volumeMap);
    }

    // If we are retaining a replica on transient storage make sure
    // it is in the lazyWriteReplicaMap so it can be persisted
    // eventually.
    if (newReplica.getVolume().isTransientStorage()) {
      lazyWriteReplicaMap.addReplica(bpid, blockId,
          (FsVolumeImpl) newReplica.getVolume(), 0);
    } else {
      lazyWriteReplicaMap.discardReplica(bpid, blockId, false);
    }
    if (oldReplica == null) {
      incrNumBlocks();
    }
  }


  /**
   * Add replicas under the given directory to the volume map
   * @param volumeMap the replicas map
   * @param dir an input directory
   * @param lazyWriteReplicaMap Map of replicas on transient
   *                                storage.
   * @param isFinalized true if the directory has finalized replicas;
   *                    false if the directory has rbw replicas
   * @param exceptions list of exception which need to return to parent thread.
   * @param subTaskQueue queue of sub tasks
   */
  void addToReplicasMap(ReplicaMap volumeMap, File dir,
      final RamDiskReplicaTracker lazyWriteReplicaMap, boolean isFinalized,
      List<IOException> exceptions, Queue<RecursiveAction> subTaskQueue)
      throws IOException {
    File[] files = fileIoProvider.listFiles(volume, dir);
    Arrays.sort(files, FILE_COMPARATOR);
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      if (file.isDirectory()) {
        // Launch new sub task.
        AddReplicaProcessor subTask = new AddReplicaProcessor(volumeMap, file,
            lazyWriteReplicaMap, isFinalized, exceptions, subTaskQueue);
        subTask.fork();
        subTaskQueue.add(subTask);
      }

      if (isFinalized && FsDatasetUtil.isUnlinkTmpFile(file)) {
        file = recoverTempUnlinkedBlock(file);
        if (file == null) { // the original block still exists, so we cover it
          // in another iteration and can continue here
          continue;
        }
      }
      if (!Block.isBlockFilename(file)) {
        continue;
      }

      long genStamp = FsDatasetUtil.getGenerationStampFromFile(
          files, file, i);
      long blockId = Block.filename2id(file.getName());
      Block block = new Block(blockId, file.length(), genStamp);
      addReplicaToReplicasMap(block, volumeMap, lazyWriteReplicaMap,
          isFinalized);
    }
  }

  /**
   * This method is invoked during DN startup when volumes are scanned to
   * build up the volumeMap.
   *
   * Given two replicas, decide which one to keep. The preference is as
   * follows:
   *   1. Prefer the replica with the higher generation stamp.
   *   2. If generation stamps are equal, prefer the replica with the
   *      larger on-disk length.
   *   3. If on-disk length is the same, prefer the replica on persistent
   *      storage volume.
   *   4. All other factors being equal, keep replica1.
   *
   * The other replica is removed from the volumeMap and is deleted from
   * its storage volume.
   *
   * @param replica1
   * @param replica2
   * @param volumeMap
   * @return the replica that is retained.
   * @throws IOException
   */
  ReplicaInfo resolveDuplicateReplicas(
      final ReplicaInfo replica1, final ReplicaInfo replica2,
      final ReplicaMap volumeMap) throws IOException {

    if (!deleteDuplicateReplicas) {
      // Leave both block replicas in place.
      return replica1;
    }
    final ReplicaInfo replicaToDelete =
        selectReplicaToDelete(replica1, replica2);
    final ReplicaInfo replicaToKeep =
        (replicaToDelete != replica1) ? replica1 : replica2;
    // Update volumeMap and delete the replica
    volumeMap.add(bpid, replicaToKeep);
    if (replicaToDelete != null) {
      deleteReplica(replicaToDelete);
    }
    return replicaToKeep;
  }

  @VisibleForTesting
  static ReplicaInfo selectReplicaToDelete(final ReplicaInfo replica1,
      final ReplicaInfo replica2) {
    ReplicaInfo replicaToKeep;
    ReplicaInfo replicaToDelete;

    // it's the same block so don't ever delete it, even if GS or size
    // differs.  caller should keep the one it just discovered on disk
    if (replica1.getBlockURI().equals(replica2.getBlockURI())) {
      return null;
    }
    if (replica1.getGenerationStamp() != replica2.getGenerationStamp()) {
      replicaToKeep = replica1.getGenerationStamp() > replica2.getGenerationStamp()
          ? replica1 : replica2;
    } else if (replica1.getNumBytes() != replica2.getNumBytes()) {
      replicaToKeep = replica1.getNumBytes() > replica2.getNumBytes() ?
          replica1 : replica2;
    } else if (replica1.getVolume().isTransientStorage() &&
               !replica2.getVolume().isTransientStorage()) {
      replicaToKeep = replica2;
    } else {
      replicaToKeep = replica1;
    }

    replicaToDelete = (replicaToKeep == replica1) ? replica2 : replica1;

    if (LOG.isDebugEnabled()) {
      LOG.debug("resolveDuplicateReplicas decide to keep " + replicaToKeep
          + ".  Will try to delete " + replicaToDelete);
    }
    return replicaToDelete;
  }

  private void deleteReplica(final ReplicaInfo replicaToDelete) {
    // Delete the files on disk. Failure here is okay.
    if (!replicaToDelete.deleteBlockData()) {
      LOG.warn("Failed to delete block file for replica " + replicaToDelete);
    }
    if (!replicaToDelete.deleteMetadata()) {
      LOG.warn("Failed to delete meta file for replica " + replicaToDelete);
    }
  }

  /**
   * Find out the number of bytes in the block that match its crc.
   *
   * This algorithm assumes that data corruption caused by unexpected
   * datanode shutdown occurs only in the last crc chunk. So it checks
   * only the last chunk.
   *
   * @param blockFile the block file
   * @param genStamp generation stamp of the block
   * @return the number of valid bytes
   */
  private long validateIntegrityAndSetLength(File blockFile, long genStamp) {
    try {
      final File metaFile = FsDatasetUtil.getMetaFile(blockFile, genStamp);
      long blockFileLen = blockFile.length();
      long metaFileLen = metaFile.length();
      int crcHeaderLen = DataChecksum.getChecksumHeaderSize();
      if (!blockFile.exists() || blockFileLen == 0 ||
          !metaFile.exists() || metaFileLen < crcHeaderLen) {
        return 0;
      }
      try (DataInputStream checksumIn = new DataInputStream(
          new BufferedInputStream(
              fileIoProvider.getFileInputStream(volume, metaFile),
              ioFileBufferSize))) {
        // read and handle the common header here. For now just a version
        final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(
            checksumIn, metaFile);
        if (Type.NULL.equals(checksum.getChecksumType())) {
          // in case of NULL checksum type consider full file as valid
          return blockFileLen;
        }
        int bytesPerChecksum = checksum.getBytesPerChecksum();
        int checksumSize = checksum.getChecksumSize();
        long numChunks = Math.min(
            (blockFileLen + bytesPerChecksum - 1) / bytesPerChecksum,
            (metaFileLen - crcHeaderLen) / checksumSize);
        if (numChunks == 0) {
          return 0;
        }
        try (InputStream blockIn = fileIoProvider.getFileInputStream(
                 volume, blockFile);
             ReplicaInputStreams ris = new ReplicaInputStreams(blockIn,
                 checksumIn, volume.obtainReference(), fileIoProvider)) {
          ris.skipChecksumFully((numChunks - 1) * checksumSize);
          long lastChunkStartPos = (numChunks - 1) * bytesPerChecksum;
          ris.skipDataFully(lastChunkStartPos);
          int lastChunkSize = (int) Math.min(
              bytesPerChecksum, blockFileLen - lastChunkStartPos);
          byte[] buf = new byte[lastChunkSize + checksumSize];
          ris.readChecksumFully(buf, lastChunkSize, checksumSize);
          ris.readDataFully(buf, 0, lastChunkSize);
          checksum.update(buf, 0, lastChunkSize);
          long validFileLength;
          if (checksum.compare(buf, lastChunkSize)) { // last chunk matches crc
            validFileLength = lastChunkStartPos + lastChunkSize;
          } else { // last chunk is corrupt
            validFileLength = lastChunkStartPos;
          }
          // truncate if extra bytes are present without CRC
          if (blockFile.length() > validFileLength) {
            try (RandomAccessFile blockRAF =
                     fileIoProvider.getRandomAccessFile(
                         volume, blockFile, "rw")) {
              // truncate blockFile
              blockRAF.setLength(validFileLength);
            }
          }
          return validFileLength;
        }
      }
    } catch (IOException e) {
      FsDatasetImpl.LOG.warn("Getting exception while validating integrity " +
              "and setting length for blockFile", e);
      return 0;
    }
  }

  @Override
  public String toString() {
    return currentDir.getAbsolutePath();
  }

  void shutdown(BlockListAsLongs blocksListToPersist) {
    saveReplicas(blocksListToPersist);
    saveDfsUsed();
    dfsUsedSaved = true;

    // Remove the shutdown hook to avoid any memory leak
    if (shutdownHook != null) {
      ShutdownHookManager.get().removeShutdownHook(shutdownHook);
    }

    if (dfsUsage instanceof CachingGetSpaceUsed) {
      IOUtils.cleanup(LOG, ((CachingGetSpaceUsed) dfsUsage));
    }
  }

  private boolean readReplicasFromCache(ReplicaMap volumeMap,
      final RamDiskReplicaTracker lazyWriteReplicaMap) {
    ReplicaMap tmpReplicaMap = new ReplicaMap(new AutoCloseableLock());
    File replicaFile = new File(currentDir, REPLICA_CACHE_FILE);
    // Check whether the file exists or not.
    if (!replicaFile.exists()) {
      LOG.info("Replica Cache file: "+  replicaFile.getPath() +
          " doesn't exist ");
      return false;
    }
    long fileLastModifiedTime = replicaFile.lastModified();
    if (System.currentTimeMillis() > fileLastModifiedTime + replicaCacheExpiry) {
      LOG.info("Replica Cache file: " + replicaFile.getPath() +
          " has gone stale");
      // Just to make findbugs happy
      if (!replicaFile.delete()) {
        LOG.info("Replica Cache file: " + replicaFile.getPath() +
            " cannot be deleted");
      }
      return false;
    }
    FileInputStream inputStream = null;
    try {
      inputStream = fileIoProvider.getFileInputStream(volume, replicaFile);
      BlockListAsLongs blocksList =
          BlockListAsLongs.readFrom(inputStream, maxDataLength);
      if (blocksList == null) {
        return false;
      }

      for (BlockReportReplica replica : blocksList) {
        switch (replica.getState()) {
        case FINALIZED:
          addReplicaToReplicasMap(replica, tmpReplicaMap, lazyWriteReplicaMap, true);
          break;
        case RUR:
        case RBW:
        case RWR:
          addReplicaToReplicasMap(replica, tmpReplicaMap, lazyWriteReplicaMap, false);
          break;
        default:
          break;
        }
      }
      // Now it is safe to add the replica into volumeMap
      // In case of any exception during parsing this cache file, fall back
      // to scan all the files on disk.
      for (Iterator<ReplicaInfo> iter =
          tmpReplicaMap.replicas(bpid).iterator(); iter.hasNext(); ) {
        ReplicaInfo info = iter.next();
        // We use a lightweight GSet to store replicaInfo, we need to remove
        // it from one GSet before adding to another.
        iter.remove();
        volumeMap.add(bpid, info);
      }
      LOG.info("Successfully read replica from cache file : "
          + replicaFile.getPath());
      return true;
    } catch (Exception e) {
      // Any exception we need to revert back to read from disk
      // Log the error and return false
      LOG.info("Exception occurred while reading the replicas cache file: "
          + replicaFile.getPath(), e );
      return false;
    }
    finally {
      // close the inputStream
      IOUtils.closeStream(inputStream);

      if (!fileIoProvider.delete(volume, replicaFile)) {
        LOG.info("Failed to delete replica cache file: " +
            replicaFile.getPath());
      }
    }
  }

  private void saveReplicas(BlockListAsLongs blocksListToPersist) {
    if (blocksListToPersist == null ||
        blocksListToPersist.getNumberOfBlocks()== 0) {
      return;
    }
    final File tmpFile = new File(currentDir, REPLICA_CACHE_FILE + ".tmp");
    final File replicaCacheFile = new File(currentDir, REPLICA_CACHE_FILE);
    if (!fileIoProvider.deleteWithExistsCheck(volume, tmpFile) ||
        !fileIoProvider.deleteWithExistsCheck(volume, replicaCacheFile)) {
      return;
    }

    FileOutputStream out = null;
    try {
      out = fileIoProvider.getFileOutputStream(volume, tmpFile);
      blocksListToPersist.writeTo(out);
      out.close();
      // Renaming the tmp file to replicas
      fileIoProvider.moveFile(volume, tmpFile, replicaCacheFile);
    } catch (Exception e) {
      // If write failed, the volume might be bad. Since the cache file is
      // not critical, log the error, delete both the files (tmp and cache)
      // and continue.
      LOG.warn("Failed to write replicas to cache ", e);
      fileIoProvider.deleteWithExistsCheck(volume, replicaCacheFile);
    } finally {
      IOUtils.closeStream(out);
      fileIoProvider.deleteWithExistsCheck(volume, tmpFile);
    }
  }

  void incrNumBlocks() {
    numOfBlocks.incrementAndGet();
  }

  void decrNumBlocks() {
    numOfBlocks.decrementAndGet();
  }

  public long getNumOfBlocks() {
    return numOfBlocks.get();
  }

  /**
   * Recursive action for add replica in map.
   */
  class AddReplicaProcessor extends RecursiveAction {

    private ReplicaMap volumeMap;
    private File dir;
    private RamDiskReplicaTracker lazyWriteReplicaMap;
    private boolean isFinalized;
    private List<IOException> exceptions;
    private Queue<RecursiveAction> subTaskQueue;

    /**
     * @param volumeMap
     *          the replicas map
     * @param dir
     *          an input directory
     * @param lazyWriteReplicaMap
     *          Map of replicas on transient storage.
     * @param isFinalized
     *          true if the directory has finalized replicas; false if the
     *          directory has rbw replicas
     * @param exceptions
     *          List of exception which need to return to parent thread.
     * @param subTaskQueue
     *          queue of sub tasks
     */
    AddReplicaProcessor(ReplicaMap volumeMap, File dir,
        RamDiskReplicaTracker lazyWriteReplicaMap, boolean isFinalized,
        List<IOException> exceptions, Queue<RecursiveAction> subTaskQueue) {
      this.volumeMap = volumeMap;
      this.dir = dir;
      this.lazyWriteReplicaMap = lazyWriteReplicaMap;
      this.isFinalized = isFinalized;
      this.exceptions = exceptions;
      this.subTaskQueue = subTaskQueue;
    }

    @Override
    protected void compute() {
      try {
        addToReplicasMap(volumeMap, dir, lazyWriteReplicaMap, isFinalized,
            exceptions, subTaskQueue);
      } catch (IOException e) {
        LOG.warn("Caught exception while adding replicas from " + volume
            + " in subtask. Will throw later.", e);
        exceptions.add(e);
      }
    }
  }

  /**
   * Return the size of fork pool used for adding replica in map.
   */
  @VisibleForTesting
  public static int getAddReplicaForkPoolSize() {
    return addReplicaThreadPool.getPoolSize();
  }
}
