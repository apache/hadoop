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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Time;

/**
 * A block pool slice represents a portion of a block pool stored on a volume.  
 * Taken together, all BlockPoolSlices sharing a block pool ID across a 
 * cluster represent a single block pool.
 * 
 * This class is synchronized by {@link FsVolumeImpl}.
 */
class BlockPoolSlice {
  private final String bpid;
  private final FsVolumeImpl volume; // volume to which this BlockPool belongs to
  private final File currentDir; // StorageDirectory/current/bpid/current
  // directory where finalized replicas are stored
  private final File finalizedDir;
  private final File rbwDir; // directory store RBW replica
  private final File tmpDir; // directory store Temporary replica
  private static final String DU_CACHE_FILE = "dfsUsed";
  private volatile boolean dfsUsedSaved = false;
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  
  // TODO:FEDERATION scalability issue - a thread per DU is needed
  private final DU dfsUsage;

  /**
   * Create a blook pool slice 
   * @param bpid Block pool Id
   * @param volume {@link FsVolumeImpl} to which this BlockPool belongs to
   * @param bpDir directory corresponding to the BlockPool
   * @param conf configuration
   * @throws IOException
   */
  BlockPoolSlice(String bpid, FsVolumeImpl volume, File bpDir,
      Configuration conf) throws IOException {
    this.bpid = bpid;
    this.volume = volume;
    this.currentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT); 
    this.finalizedDir = new File(
        currentDir, DataStorage.STORAGE_DIR_FINALIZED);
    if (!this.finalizedDir.exists()) {
      if (!this.finalizedDir.mkdirs()) {
        throw new IOException("Failed to mkdirs " + this.finalizedDir);
      }
    }

    // Files that were being written when the datanode was last shutdown
    // are now moved back to the data directory. It is possible that
    // in the future, we might want to do some sort of datanode-local
    // recovery for these blocks. For example, crc validation.
    //
    this.tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
    if (tmpDir.exists()) {
      FileUtil.fullyDelete(tmpDir);
    }
    this.rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);
    if (!rbwDir.mkdirs()) {  // create rbw directory if not exist
      if (!rbwDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + rbwDir.toString());
      }
    }
    if (!tmpDir.mkdirs()) {
      if (!tmpDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + tmpDir.toString());
      }
    }
    // Use cached value initially if available. Or the following call will
    // block until the initial du command completes.
    this.dfsUsage = new DU(bpDir, conf, loadDfsUsed());
    this.dfsUsage.start();

    // Make the dfs usage to be saved during shutdown.
    ShutdownHookManager.get().addShutdownHook(
      new Runnable() {
        @Override
        public void run() {
          if (!dfsUsedSaved) {
            saveDfsUsed();
          }
        }
      }, SHUTDOWN_HOOK_PRIORITY);
  }

  File getDirectory() {
    return currentDir.getParentFile();
  }

  File getFinalizedDir() {
    return finalizedDir;
  }
  
  File getRbwDir() {
    return rbwDir;
  }

  /** Run DU on local drives.  It must be synchronized from caller. */
  void decDfsUsed(long value) {
    dfsUsage.decDfsUsed(value);
  }
  
  long getDfsUsed() throws IOException {
    return dfsUsage.getUsed();
  }
  
   /**
   * Read in the cached DU value and return it if it is less than 600 seconds
   * old (DU update interval). Slight imprecision of dfsUsed is not critical
   * and skipping DU can significantly shorten the startup time.
   * If the cached value is not available or too old, -1 is returned.
   */
  long loadDfsUsed() {
    long cachedDfsUsed;
    long mtime;
    Scanner sc;

    try {
      sc = new Scanner(new File(currentDir, DU_CACHE_FILE));
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
      if (mtime > 0 && (Time.now() - mtime < 600000L)) {
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
    if (outFile.exists() && !outFile.delete()) {
      FsDatasetImpl.LOG.warn("Failed to delete old dfsUsed file in " +
        outFile.getParent());
    }

    FileWriter out = null;
    try {
      long used = getDfsUsed();
      if (used > 0) {
        out = new FileWriter(outFile);
        // mtime is written last, so that truncated writes won't be valid.
        out.write(Long.toString(used) + " " + Long.toString(Time.now()));
        out.flush();
        out.close();
        out = null;
      }
    } catch (IOException ioe) {
      // If write failed, the volume might be bad. Since the cache file is
      // not critical, log the error and continue.
      FsDatasetImpl.LOG.warn("Failed to write dfsUsed to " + outFile, ioe);
    } finally {
      IOUtils.cleanup(null, out);
    }
  }

  /**
   * Temporary files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createTmpFile(Block b) throws IOException {
    File f = new File(tmpDir, b.getBlockName());
    return DatanodeUtil.createTmpFile(b, f);
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(Block b) throws IOException {
    File f = new File(rbwDir, b.getBlockName());
    return DatanodeUtil.createTmpFile(b, f);
  }

  File addBlock(Block b, File f) throws IOException {
    File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, b.getBlockId());
    if (!blockDir.exists()) {
      if (!blockDir.mkdirs()) {
        throw new IOException("Failed to mkdirs " + blockDir);
      }
    }
    File blockFile = FsDatasetImpl.moveBlockFiles(b, f, blockDir);
    File metaFile = FsDatasetUtil.getMetaFile(blockFile, b.getGenerationStamp());
    dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
    return blockFile;
  }
    
  void checkDirs() throws DiskErrorException {
    DiskChecker.checkDirs(finalizedDir);
    DiskChecker.checkDir(tmpDir);
    DiskChecker.checkDir(rbwDir);
  }
    
  void getVolumeMap(ReplicaMap volumeMap) throws IOException {
    // add finalized replicas
    addToReplicasMap(volumeMap, finalizedDir, true);
    // add rbw replicas
    addToReplicasMap(volumeMap, rbwDir, false);
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
      if (!unlinkedTmp.delete()) {
        throw new IOException("Unable to cleanup unlinked tmp file " +
            unlinkedTmp);
      }
      return null;
    } else {
      if (!unlinkedTmp.renameTo(blockFile)) {
        throw new IOException("Unable to rename unlinked tmp file " +
            unlinkedTmp);
      }
      return blockFile;
    }
  }


  /**
   * Add replicas under the given directory to the volume map
   * @param volumeMap the replicas map
   * @param dir an input directory
   * @param isFinalized true if the directory has finalized replicas;
   *                    false if the directory has rbw replicas
   */
  void addToReplicasMap(ReplicaMap volumeMap, File dir, boolean isFinalized
      ) throws IOException {
    File files[] = FileUtil.listFiles(dir);
    for (File file : files) {
      if (file.isDirectory()) {
        addToReplicasMap(volumeMap, file, isFinalized);
      }

      if (isFinalized && FsDatasetUtil.isUnlinkTmpFile(file)) {
        file = recoverTempUnlinkedBlock(file);
        if (file == null) { // the original block still exists, so we cover it
          // in another iteration and can continue here
          continue;
        }
      }
      if (!Block.isBlockFilename(file))
        continue;
      
      long genStamp = FsDatasetUtil.getGenerationStampFromFile(
          files, file);
      long blockId = Block.filename2id(file.getName());
      ReplicaInfo newReplica = null;
      if (isFinalized) {
        newReplica = new FinalizedReplica(blockId, 
            file.length(), genStamp, volume, file.getParentFile());
      } else {

        boolean loadRwr = true;
        File restartMeta = new File(file.getParent()  +
            File.pathSeparator + "." + file.getName() + ".restart");
        Scanner sc = null;
        try {
          sc = new Scanner(restartMeta);
          // The restart meta file exists
          if (sc.hasNextLong() && (sc.nextLong() > Time.now())) {
            // It didn't expire. Load the replica as a RBW.
            newReplica = new ReplicaBeingWritten(blockId,
                validateIntegrityAndSetLength(file, genStamp),
                genStamp, volume, file.getParentFile(), null);
            loadRwr = false;
          }
          sc.close();
          if (!restartMeta.delete()) {
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
          newReplica = new ReplicaWaitingToBeRecovered(blockId,
              validateIntegrityAndSetLength(file, genStamp),
              genStamp, volume, file.getParentFile());
        }
      }

      ReplicaInfo oldReplica = volumeMap.add(bpid, newReplica);
      if (oldReplica != null) {
        FsDatasetImpl.LOG.warn("Two block files with the same block id exist " +
            "on disk: " + oldReplica.getBlockFile() + " and " + file );
      }
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
    DataInputStream checksumIn = null;
    InputStream blockIn = null;
    try {
      final File metaFile = FsDatasetUtil.getMetaFile(blockFile, genStamp);
      long blockFileLen = blockFile.length();
      long metaFileLen = metaFile.length();
      int crcHeaderLen = DataChecksum.getChecksumHeaderSize();
      if (!blockFile.exists() || blockFileLen == 0 ||
          !metaFile.exists() || metaFileLen < crcHeaderLen) {
        return 0;
      }
      checksumIn = new DataInputStream(
          new BufferedInputStream(new FileInputStream(metaFile),
              HdfsConstants.IO_FILE_BUFFER_SIZE));

      // read and handle the common header here. For now just a version
      BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
      short version = header.getVersion();
      if (version != BlockMetadataHeader.VERSION) {
        FsDatasetImpl.LOG.warn("Wrong version (" + version + ") for metadata file "
            + metaFile + " ignoring ...");
      }
      DataChecksum checksum = header.getChecksum();
      int bytesPerChecksum = checksum.getBytesPerChecksum();
      int checksumSize = checksum.getChecksumSize();
      long numChunks = Math.min(
          (blockFileLen + bytesPerChecksum - 1)/bytesPerChecksum, 
          (metaFileLen - crcHeaderLen)/checksumSize);
      if (numChunks == 0) {
        return 0;
      }
      IOUtils.skipFully(checksumIn, (numChunks-1)*checksumSize);
      blockIn = new FileInputStream(blockFile);
      long lastChunkStartPos = (numChunks-1)*bytesPerChecksum;
      IOUtils.skipFully(blockIn, lastChunkStartPos);
      int lastChunkSize = (int)Math.min(
          bytesPerChecksum, blockFileLen-lastChunkStartPos);
      byte[] buf = new byte[lastChunkSize+checksumSize];
      checksumIn.readFully(buf, lastChunkSize, checksumSize);
      IOUtils.readFully(blockIn, buf, 0, lastChunkSize);

      checksum.update(buf, 0, lastChunkSize);
      long validFileLength;
      if (checksum.compare(buf, lastChunkSize)) { // last chunk matches crc
        validFileLength = lastChunkStartPos + lastChunkSize;
      } else { // last chunck is corrupt
        validFileLength = lastChunkStartPos;
      }

      // truncate if extra bytes are present without CRC
      if (blockFile.length() > validFileLength) {
        RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
        try {
          // truncate blockFile
          blockRAF.setLength(validFileLength);
        } finally {
          blockRAF.close();
        }
      }

      return validFileLength;
    } catch (IOException e) {
      FsDatasetImpl.LOG.warn(e);
      return 0;
    } finally {
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(blockIn);
    }
  }
    
  @Override
  public String toString() {
    return currentDir.getAbsolutePath();
  }
  
  void shutdown() {
    saveDfsUsed();
    dfsUsedSaved = true;
    dfsUsage.shutdown();
  }
}
