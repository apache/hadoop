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

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Random;

/**
 * Test-related utilities to access blocks in {@link FsDatasetImpl}.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FsDatasetImplTestUtils implements FsDatasetTestUtils {
  private static final Log LOG =
      LogFactory.getLog(FsDatasetImplTestUtils.class);
  private final FsDatasetImpl dataset;

  /**
   * By default we assume 2 data directories (volumes) per DataNode.
   */
  public static final int DEFAULT_NUM_OF_DATA_DIRS = 2;

  /**
   * A reference to the replica that is used to corrupt block / meta later.
   */
  private static class FsDatasetImplMaterializedReplica
      implements MaterializedReplica {
    /** Block file of the replica. */
    private final File blockFile;
    private final File metaFile;

    /** Check the existence of the file. */
    private static void checkFile(File file) throws FileNotFoundException {
      if (file == null || !file.exists()) {
        throw new FileNotFoundException(
            "The block file or metadata file " + file + " does not exist.");
      }
    }

    /** Corrupt a block / crc file by truncating it to a newSize */
    private static void truncate(File file, long newSize)
        throws IOException {
      Preconditions.checkArgument(newSize >= 0);
      checkFile(file);
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.setLength(newSize);
      }
    }

    /** Corrupt a block / crc file by deleting it. */
    private static void delete(File file) throws IOException {
      checkFile(file);
      Files.delete(file.toPath());
    }

    FsDatasetImplMaterializedReplica(File blockFile, File metaFile) {
      this.blockFile = blockFile;
      this.metaFile = metaFile;
    }

    @Override
    public void corruptData() throws IOException {
      checkFile(blockFile);
      LOG.info("Corrupting block file: " + blockFile);
      final int BUF_SIZE = 32;
      byte[] buf = new byte[BUF_SIZE];
      try (RandomAccessFile raf = new RandomAccessFile(blockFile, "rw")) {
        int nread = raf.read(buf);
        for (int i = 0; i < nread; i++) {
          buf[i]++;
        }
        raf.seek(0);
        raf.write(buf);
      }
    }

    @Override
    public void corruptData(byte[] newContent) throws IOException {
      checkFile(blockFile);
      LOG.info("Corrupting block file with new content: " + blockFile);
      try (RandomAccessFile raf = new RandomAccessFile(blockFile, "rw")) {
        raf.write(newContent);
      }
    }

    @Override
    public void truncateData(long newSize) throws IOException {
      LOG.info("Truncating block file: " + blockFile);
      truncate(blockFile, newSize);
    }

    @Override
    public void deleteData() throws IOException {
      LOG.info("Deleting block file: " + blockFile);
      delete(blockFile);
    }

    @Override
    public void corruptMeta() throws IOException {
      checkFile(metaFile);
      LOG.info("Corrupting meta file: " + metaFile);
      Random random = new Random();
      try (RandomAccessFile raf = new RandomAccessFile(metaFile, "rw")) {
        FileChannel channel = raf.getChannel();
        int offset = random.nextInt((int)channel.size() / 2);
        raf.seek(offset);
        raf.write("BADBAD".getBytes());
      }
    }

    @Override
    public void deleteMeta() throws IOException {
      LOG.info("Deleting metadata file: " + metaFile);
      delete(metaFile);
    }

    @Override
    public void truncateMeta(long newSize) throws IOException {
      LOG.info("Truncating metadata file: " + metaFile);
      truncate(metaFile, newSize);
    }

    @Override
    public String toString() {
      return String.format("MaterializedReplica: file=%s", blockFile);
    }
  }

  public FsDatasetImplTestUtils(DataNode datanode) {
    Preconditions.checkArgument(
        datanode.getFSDataset() instanceof FsDatasetImpl);
    dataset = (FsDatasetImpl) datanode.getFSDataset();
  }

  /**
   * Return a materialized replica from the FsDatasetImpl.
   */
  @Override
  public MaterializedReplica getMaterializedReplica(ExtendedBlock block)
      throws ReplicaNotFoundException {
    File blockFile;
    try {
       blockFile = dataset.getBlockFile(
           block.getBlockPoolId(), block.getBlockId());
    } catch (IOException e) {
      LOG.error("Block file for " + block + " does not existed:", e);
      throw new ReplicaNotFoundException(block);
    }
    File metaFile = FsDatasetUtil.getMetaFile(
        blockFile, block.getGenerationStamp());
    return new FsDatasetImplMaterializedReplica(blockFile, metaFile);
  }

  @Override
  public Replica createFinalizedReplica(ExtendedBlock block)
      throws IOException {
    try (FsVolumeReferences volumes = dataset.getFsVolumeReferences()) {
      return createFinalizedReplica(volumes.get(0), block);
    }
  }

  @Override
  public Replica createFinalizedReplica(FsVolumeSpi volume, ExtendedBlock block)
      throws IOException {
    FsVolumeImpl vol = (FsVolumeImpl) volume;
    ReplicaInfo info = new FinalizedReplica(block.getLocalBlock(), vol,
        vol.getCurrentDir().getParentFile());
    dataset.volumeMap.add(block.getBlockPoolId(), info);
    info.getBlockFile().createNewFile();
    info.getMetaFile().createNewFile();
    return info;
  }

  @Override
  public Replica createReplicaInPipeline(ExtendedBlock block)
      throws IOException {
    try (FsVolumeReferences volumes = dataset.getFsVolumeReferences()) {
      return createReplicaInPipeline(volumes.get(0), block);
    }
  }

  @Override
  public Replica createReplicaInPipeline(
      FsVolumeSpi volume, ExtendedBlock block) throws IOException {
    FsVolumeImpl vol = (FsVolumeImpl) volume;
    ReplicaInPipeline rip = new ReplicaInPipeline(
        block.getBlockId(), block.getGenerationStamp(), volume,
        vol.createTmpFile(
            block.getBlockPoolId(), block.getLocalBlock()).getParentFile(),
        0);
    dataset.volumeMap.add(block.getBlockPoolId(), rip);
    return rip;
  }


  @Override
  public Replica createRBW(ExtendedBlock eb) throws IOException {
    try (FsVolumeReferences volumes = dataset.getFsVolumeReferences()) {
      return createRBW(volumes.get(0), eb);
    }
  }

  @Override
  public Replica createRBW(FsVolumeSpi volume, ExtendedBlock eb)
      throws IOException {
    FsVolumeImpl vol = (FsVolumeImpl) volume;
    final String bpid = eb.getBlockPoolId();
    final Block block = eb.getLocalBlock();
    ReplicaBeingWritten rbw = new ReplicaBeingWritten(
        eb.getLocalBlock(), volume,
        vol.createRbwFile(bpid, block).getParentFile(), null);
    rbw.getBlockFile().createNewFile();
    rbw.getMetaFile().createNewFile();
    dataset.volumeMap.add(bpid, rbw);
    return rbw;
  }

  @Override
  public Replica createReplicaWaitingToBeRecovered(ExtendedBlock eb)
      throws IOException {
    try (FsVolumeReferences volumes = dataset.getFsVolumeReferences()) {
      return createReplicaInPipeline(volumes.get(0), eb);
    }
  }

  @Override
  public Replica createReplicaWaitingToBeRecovered(
      FsVolumeSpi volume, ExtendedBlock eb) throws IOException {
    FsVolumeImpl vol = (FsVolumeImpl) volume;
    final String bpid = eb.getBlockPoolId();
    final Block block = eb.getLocalBlock();
    ReplicaWaitingToBeRecovered rwbr =
        new ReplicaWaitingToBeRecovered(eb.getLocalBlock(), volume,
            vol.createRbwFile(bpid, block).getParentFile());
    dataset.volumeMap.add(bpid, rwbr);
    return rwbr;
  }

  @Override
  public Replica createReplicaUnderRecovery(
      ExtendedBlock block, long recoveryId) throws IOException {
    try (FsVolumeReferences volumes = dataset.getFsVolumeReferences()) {
      FsVolumeImpl volume = (FsVolumeImpl) volumes.get(0);
      ReplicaUnderRecovery rur = new ReplicaUnderRecovery(new FinalizedReplica(
          block.getLocalBlock(), volume, volume.getCurrentDir().getParentFile()),
          recoveryId
      );
      dataset.volumeMap.add(block.getBlockPoolId(), rur);
      return rur;
    }
  }

  @Override
  public void checkStoredReplica(Replica replica) throws IOException {
    Preconditions.checkArgument(replica instanceof ReplicaInfo);
    ReplicaInfo r = (ReplicaInfo) replica;
    FsDatasetImpl.checkReplicaFiles(r);
  }

  @Override
  public void injectCorruptReplica(ExtendedBlock block) throws IOException {
    Preconditions.checkState(!dataset.contains(block),
        "Block " + block + " already exists on dataset.");
    try (FsVolumeReferences volRef = dataset.getFsVolumeReferences()) {
      FsVolumeImpl volume = (FsVolumeImpl) volRef.get(0);
      FinalizedReplica finalized = new FinalizedReplica(
          block.getLocalBlock(),
          volume,
          volume.getFinalizedDir(block.getBlockPoolId()));
      File blockFile = finalized.getBlockFile();
      if (!blockFile.createNewFile()) {
        throw new FileExistsException(
            "Block file " + blockFile + " already exists.");
      }
      File metaFile = FsDatasetUtil.getMetaFile(blockFile, 1000);
      if (!metaFile.createNewFile()) {
        throw new FileExistsException(
            "Meta file " + metaFile + " already exists."
        );
      }
    }
  }

  @Override
  public Replica fetchReplica(ExtendedBlock block) {
    return dataset.fetchReplicaInfo(block.getBlockPoolId(), block.getBlockId());
  }

  @Override
  public int getDefaultNumOfDataDirs() {
    return this.DEFAULT_NUM_OF_DATA_DIRS;
  }

  @Override
  public long getRawCapacity() throws IOException {
    try (FsVolumeReferences volRefs = dataset.getFsVolumeReferences()) {
      Preconditions.checkState(volRefs.size() != 0);
      DF df = new DF(new File(volRefs.get(0).getBasePath()),
          dataset.datanode.getConf());
      return df.getCapacity();
    }
  }
}
