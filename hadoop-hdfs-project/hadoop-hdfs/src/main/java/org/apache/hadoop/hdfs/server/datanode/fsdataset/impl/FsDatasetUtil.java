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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

/** Utility methods. */
@InterfaceAudience.Private
public class FsDatasetUtil {
  static boolean isUnlinkTmpFile(File f) {
    return f.getName().endsWith(DatanodeUtil.UNLINK_BLOCK_SUFFIX);
  }

  public static byte[] createNullChecksumByteArray() {
    DataChecksum csum =
        DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 512);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      BlockMetadataHeader.writeHeader(dataOut, csum);
      dataOut.close();
    } catch (IOException e) {
      FsVolumeImpl.LOG.error(
          "Exception in creating null checksum stream: " + e);
      return null;
    }
    return out.toByteArray();
  }

  static File getOrigFile(File unlinkTmpFile) {
    final String name = unlinkTmpFile.getName();
    if (!name.endsWith(DatanodeUtil.UNLINK_BLOCK_SUFFIX)) {
      throw new IllegalArgumentException("unlinkTmpFile=" + unlinkTmpFile
          + " does not end with " + DatanodeUtil.UNLINK_BLOCK_SUFFIX);
    }
    final int n = name.length() - DatanodeUtil.UNLINK_BLOCK_SUFFIX.length(); 
    return new File(unlinkTmpFile.getParentFile(), name.substring(0, n));
  }
  
  static File getMetaFile(File f, long gs) {
    return new File(f.getParent(),
        DatanodeUtil.getMetaName(f.getName(), gs));
  }

  /** Find the corresponding meta data file from a given block file */
  public static File findMetaFile(final File blockFile) throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    final File[] matches = parent.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return dir.equals(parent) && name.startsWith(prefix)
            && name.endsWith(Block.METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      throw new IOException("Meta file not found, blockFile=" + blockFile);
    }
    if (matches.length > 1) {
      throw new IOException("Found more than one meta files: " 
          + Arrays.asList(matches));
    }
    return matches[0];
  }

  public static FileDescriptor openAndSeek(File file, long offset)
      throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(file, "r");
      if (offset > 0) {
        raf.seek(offset);
      }
      return raf.getFD();
    } catch(IOException ioe) {
      IOUtils.cleanup(null, raf);
      throw ioe;
    }
  }

  public static InputStream getInputStreamAndSeek(File file, long offset)
      throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(file, "r");
      raf.seek(offset);
      return Channels.newInputStream(raf.getChannel());
    } catch(IOException ioe) {
      IOUtils.cleanupWithLogger(null, raf);
      throw ioe;
    }
  }

  /**
   * Find the meta-file for the specified block file and then return the
   * generation stamp from the name of the meta-file. Generally meta file will
   * be the next file in sorted array of file's.
   *
   * @param listdir
   *          sorted list of file based on name.
   * @param blockFile
   *          block file for which generation stamp is needed.
   * @param index
   *          index of block file in array.
   * @return generation stamp for block file.
   */
  static long getGenerationStampFromFile(File[] listdir, File blockFile,
      int index) {
    String blockName = blockFile.getName();
    if ((index + 1) < listdir.length) {
      // Check if next index file is meta file
      String metaFile = listdir[index + 1].getName();
      if (metaFile.startsWith(blockName)) {
        return Block.getGenerationStamp(metaFile);
      }
    }
    FsDatasetImpl.LOG.warn("Block " + blockFile + " does not have a metafile!");
    return HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }

  /** Find the corresponding meta data file from a given block file */
  static long parseGenerationStamp(File blockFile, File metaFile
      ) throws IOException {
    final String metaname = metaFile.getName();
    final String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - Block.METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw new IOException("Failed to parse generation stamp: blockFile="
          + blockFile + ", metaFile=" + metaFile, nfe);
    }
  }

  /**
   * Compute the checksum for a block file that does not already have
   * its checksum computed, and save it to dstMeta file.
   */
  public static void computeChecksum(File srcMeta, File dstMeta,
      File blockFile, int smallBufferSize, Configuration conf)
          throws IOException {
    Preconditions.checkNotNull(srcMeta);
    Preconditions.checkNotNull(dstMeta);
    Preconditions.checkNotNull(blockFile);
    // Create a dummy ReplicaInfo object pointing to the blockFile.
    ReplicaInfo wrapper = new FinalizedReplica(0, 0, 0, null, null) {
      @Override
      public URI getMetadataURI() {
        return srcMeta.toURI();
      }

      @Override
      public InputStream getDataInputStream(long seekOffset)
          throws IOException {
        return new FileInputStream(blockFile);
      }
    };

    FsDatasetImpl.computeChecksum(wrapper, dstMeta, smallBufferSize, conf);
  }

  public static void deleteMappedFile(String filePath) throws IOException {
    if (filePath == null) {
      throw new IOException("The filePath should not be null!");
    }
    boolean result = Files.deleteIfExists(Paths.get(filePath));
    if (!result) {
      throw new IOException(
          "Failed to delete the mapped file: " + filePath);
    }
  }
}
