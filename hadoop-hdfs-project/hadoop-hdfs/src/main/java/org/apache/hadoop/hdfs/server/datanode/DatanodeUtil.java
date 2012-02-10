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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;

/** Provide utility methods for Datanode. */
@InterfaceAudience.Private
class DatanodeUtil {
  static final String METADATA_EXTENSION = ".meta";

  static final String UNLINK_BLOCK_SUFFIX = ".unlinked";

  private final static String DISK_ERROR = "Possible disk error on file creation: ";

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
  static File createTmpFile(Block b, File f) throws IOException {
    if (f.exists()) {
      throw new IOException("Unexpected problem in creating temporary file for "
          + b + ".  File " + f + " should not be present, but is.");
    }
    // Create the zero-length temp file
    final boolean fileCreated;
    try {
      fileCreated = f.createNewFile();
    } catch (IOException ioe) {
      throw (IOException)new IOException(DISK_ERROR + f).initCause(ioe);
    }
    if (!fileCreated) {
      throw new IOException("Unexpected problem in creating temporary file for "
          + b + ".  File " + f + " should be creatable, but is already present.");
    }
    return f;
  }
  
  static String getMetaFileName(String blockFileName, long genStamp) {
    return blockFileName + "_" + genStamp + METADATA_EXTENSION;
  }
  
  static File getMetaFile(File f, long genStamp) {
    return new File(getMetaFileName(f.getAbsolutePath(), genStamp));
  }

  /** Find the corresponding meta data file from a given block file */
  static File findMetaFile(final File blockFile) throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    File[] matches = parent.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return dir.equals(parent)
            && name.startsWith(prefix) && name.endsWith(METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      throw new IOException("Meta file not found, blockFile=" + blockFile);
    }
    else if (matches.length > 1) {
      throw new IOException("Found more than one meta files: " 
          + Arrays.asList(matches));
    }
    return matches[0];
  }
  
  static File getUnlinkTmpFile(File f) {
    return new File(f.getParentFile(), f.getName()+UNLINK_BLOCK_SUFFIX);
  }
}
