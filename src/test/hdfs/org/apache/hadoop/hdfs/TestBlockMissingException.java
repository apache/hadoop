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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.BlockMissingException;

public class TestBlockMissingException extends TestCase {
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.hdfs.TestBlockMissing");
  final static int NUM_DATANODES = 3;

  Configuration conf;
  MiniDFSCluster dfs = null;
  DistributedFileSystem fileSys = null;

  /**
   * Test DFS Raid
   */
  public void testBlockMissingException() throws Exception {
    LOG.info("Test testBlockMissingException started.");
    long blockSize = 1024L;
    int numBlocks = 4;
    conf = new HdfsConfiguration();
    try {
      dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
      dfs.waitActive();
      fileSys = (DistributedFileSystem)dfs.getFileSystem();
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
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blocksize);
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

  /*
   * The Data directories for a datanode
   */
  private File[] getDataNodeDirs(int i) throws IOException {
    String base_dir = MiniDFSCluster.getBaseDirectory();
    File data_dir = new File(base_dir, "data");
    File dir1 = new File(data_dir, "data"+(2*i+1));
    File dir2 = new File(data_dir, "data"+(2*i+2));
    if (dir1.isDirectory() && dir2.isDirectory()) {
      File[] dir = new File[2];
      dir[0] = new File(dir1, MiniDFSCluster.FINALIZED_DIR_NAME);
      dir[1] = new File(dir2, MiniDFSCluster.FINALIZED_DIR_NAME); 
      return dir;
    }
    return new File[0];
  }

  //
  // Corrupt specified block of file
  //
  void corruptBlock(Path file, Block blockNum) throws IOException {
    long id = blockNum.getBlockId();

    // Now deliberately remove/truncate data blocks from the block.
    //
    for (int i = 0; i < NUM_DATANODES; i++) {
      File[] dirs = getDataNodeDirs(i);
      
      for (int j = 0; j < dirs.length; j++) {
        File[] blocks = dirs[j].listFiles();
        assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length >= 0));
        for (int idx = 0; idx < blocks.length; idx++) {
          if (blocks[idx].getName().startsWith("blk_" + id) &&
              !blocks[idx].getName().endsWith(".meta")) {
            blocks[idx].delete();
            LOG.info("Deleted block " + blocks[idx]);
          }
        }
      }
    }
  }

}
