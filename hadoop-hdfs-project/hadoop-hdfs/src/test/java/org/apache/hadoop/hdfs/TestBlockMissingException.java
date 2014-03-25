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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;

public class TestBlockMissingException {
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.hdfs.TestBlockMissing");
  final static int NUM_DATANODES = 3;

  Configuration conf;
  MiniDFSCluster dfs = null;
  DistributedFileSystem fileSys = null;

  /**
   * Test DFS Raid
   */
  @Test
  public void testBlockMissingException() throws Exception {
    LOG.info("Test testBlockMissingException started.");
    long blockSize = 1024L;
    int numBlocks = 4;
    conf = new HdfsConfiguration();
    try {
      dfs = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      Path file1 = new Path("/user/dhruba/raidtest/file1");
      createOldFile(fileSys, file1, 1, numBlocks, blockSize);

      // extract block locations from File system. Wait till file is closed.
      LocatedBlocks locations = null;
      locations = fileSys.dfs.getNamenode().getBlockLocations(file1.toString(),
          0, numBlocks * blockSize);
      // remove block of file
      LOG.info("Remove first block of file");
      corruptBlock(file1, locations.get(0).getBlock());

      // validate that the system throws BlockMissingException
      validateFile(fileSys, file1);
    } finally {
      if (fileSys != null) fileSys.close();
      if (dfs != null) dfs.shutdown();
    }
    LOG.info("Test testBlockMissingException completed.");
  }
  
  //
  // creates a file and populate it with data.
  //
  private void createOldFile(FileSystem fileSys, Path name, int repl, int numBlocks, long blocksize)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blocksize);
    // fill data into file
    final byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      stm.write(b);
    }
    stm.close();
  }

  //
  // validates that file encounters BlockMissingException
  //
  private void validateFile(FileSystem fileSys, Path name)
    throws IOException {

    FSDataInputStream stm = fileSys.open(name);
    final byte[] b = new byte[4192];
    int num = 0;
    boolean gotException = false;

    try {
      while (num >= 0) {
        num = stm.read(b);
        if (num < 0) {
          break;
        }
      }
    } catch (BlockMissingException e) {
      gotException = true;
    }
    stm.close();
    assertTrue("Expected BlockMissingException ", gotException);
  }

  //
  // Corrupt specified block of file
  //
  void corruptBlock(Path file, ExtendedBlock blk) {
    // Now deliberately remove/truncate data blocks from the file.
    File[] blockFiles = dfs.getAllBlockFiles(blk);
    for (File f : blockFiles) {
      f.delete();
      LOG.info("Deleted block " + f);
    }
  }
}
