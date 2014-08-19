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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;

/** Utility methods. */
@InterfaceAudience.Private
public class FsDatasetUtil {
  static boolean isUnlinkTmpFile(File f) {
    return f.getName().endsWith(DatanodeUtil.UNLINK_BLOCK_SUFFIX);
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

  /**
   * Find the meta-file for the specified block file
   * and then return the generation stamp from the name of the meta-file.
   */
  static long getGenerationStampFromFile(File[] listdir, File blockFile) {
    String blockName = blockFile.getName();
    for (int j = 0; j < listdir.length; j++) {
      String path = listdir[j].getName();
      if (!path.startsWith(blockName)) {
        continue;
      }
      if (blockFile == listdir[j]) {
        continue;
      }
      return Block.getGenerationStamp(listdir[j].getName());
    }
    FsDatasetImpl.LOG.warn("Block " + blockFile + " does not have a metafile!");
    return GenerationStamp.GRANDFATHER_GENERATION_STAMP;
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
}
