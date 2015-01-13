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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

/** Provide utility methods for Datanode. */
@InterfaceAudience.Private
public class DatanodeUtil {
  public static final String UNLINK_BLOCK_SUFFIX = ".unlinked";

  public static final String DISK_ERROR = "Possible disk error: ";

  private static final String SEP = System.getProperty("file.separator");

  /** Get the cause of an I/O exception if caused by a possible disk error
   * @param ioe an I/O exception
   * @return cause if the I/O exception is caused by a possible disk error;
   *         null otherwise.
   */ 
  static IOException getCauseIfDiskError(IOException ioe) {
    if (ioe.getMessage()!=null && ioe.getMessage().startsWith(DISK_ERROR)) {
      return (IOException)ioe.getCause();
    } else {
      return null;
    }
  }

  /**
   * Create a new file.
   * @throws IOException 
   * if the file already exists or if the file cannot be created.
   */
  public static File createTmpFile(Block b, File f) throws IOException {
    if (f.exists()) {
      throw new IOException("Failed to create temporary file for " + b
          + ".  File " + f + " should not be present, but is.");
    }
    // Create the zero-length temp file
    final boolean fileCreated;
    try {
      fileCreated = f.createNewFile();
    } catch (IOException ioe) {
      throw new IOException(DISK_ERROR + "Failed to create " + f, ioe);
    }
    if (!fileCreated) {
      throw new IOException("Failed to create temporary file for " + b
          + ".  File " + f + " should be creatable, but is already present.");
    }
    return f;
  }
  
  /**
   * @return the meta name given the block name and generation stamp.
   */
  public static String getMetaName(String blockName, long generationStamp) {
    return blockName + "_" + generationStamp + Block.METADATA_EXTENSION; 
  }

  /** @return the unlink file. */
  public static File getUnlinkTmpFile(File f) {
    return new File(f.getParentFile(), f.getName()+UNLINK_BLOCK_SUFFIX);
  }

  /**
   * Checks whether there are any files anywhere in the directory tree rooted
   * at dir (directories don't count as files). dir must exist
   * @return true if there are no files
   * @throws IOException if unable to list subdirectories
   */
  public static boolean dirNoFilesRecursive(File dir) throws IOException {
    File[] contents = dir.listFiles();
    if (contents == null) {
      throw new IOException("Cannot list contents of " + dir);
    }
    for (File f : contents) {
      if (!f.isDirectory() || (f.isDirectory() && !dirNoFilesRecursive(f))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the directory where a finalized block with this ID should be stored.
   * Do not attempt to create the directory.
   * @param root the root directory where finalized blocks are stored
   * @param blockId
   * @return
   */
  public static File idToBlockDir(File root, long blockId) {
    int d1 = (int)((blockId >> 16) & 0xff);
    int d2 = (int)((blockId >> 8) & 0xff);
    String path = DataStorage.BLOCK_SUBDIR_PREFIX + d1 + SEP +
        DataStorage.BLOCK_SUBDIR_PREFIX + d2;
    return new File(root, path);
  }

  /**
   * @return the FileInputStream for the meta data of the given block.
   * @throws FileNotFoundException
   *           if the file not found.
   * @throws ClassCastException
   *           if the underlying input stream is not a FileInputStream.
   */
  public static FileInputStream getMetaDataInputStream(
      ExtendedBlock b, FsDatasetSpi<?> data) throws IOException {
    final LengthInputStream lin = data.getMetaDataInputStream(b);
    if (lin == null) {
      throw new FileNotFoundException("Meta file for " + b + " not found.");
    }
    return (FileInputStream)lin.getWrappedStream();
  }
}
