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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;

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
}
