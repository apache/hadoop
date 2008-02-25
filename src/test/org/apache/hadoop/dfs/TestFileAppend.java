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
package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import java.net.*;
import java.util.Random;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil.HardLink;

/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestFileAppend extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 1024;
  static final int numBlocks = 10;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  /*
   * creates a file but does not close it
   */ 
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  private void writeFile(FSDataOutputStream stm) throws IOException {
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
  }

  /**
   * Test that copy on write for blocks works correctly
   */
  public void testCopyOnWrite() throws IOException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    try {

      // create a new file, write to it and close it.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      writeFile(stm);
      stm.close();

      // Get a handle to the datanode
      DataNode[] dn = cluster.listDataNodes();
      assertTrue("There should be only one datanode but found " + dn.length,
                  dn.length == 1);

      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      List<LocatedBlock> blocks = locations.getLocatedBlocks();
      FSDataset dataset = (FSDataset) dn[0].data;

      //
      // Create hard links for a few of the blocks
      //
      for (int i = 0; i < blocks.size(); i = i + 2) {
        Block b = (Block) blocks.get(i).getBlock();
        FSDataset fsd = (FSDataset) dataset;
        File f = fsd.getFile(b);
        File link = new File(f.toString() + ".link");
        System.out.println("Creating hardlink for File " + f + 
                           " to " + link);
        HardLink.createHardLink(f, link);
      }

      //
      // Detach all blocks. This should remove hardlinks (if any)
      //
      for (int i = 0; i < blocks.size(); i++) {
        Block b = (Block) blocks.get(i).getBlock();
        System.out.println("testCopyOnWrite detaching block " + b);
        assertTrue("Detaching block " + b + " should have returned true",
                   dataset.detachBlock(b, 1) == true);
      }

      // Since the blocks were already detached earlier, these calls should
      // return false
      //
      for (int i = 0; i < blocks.size(); i++) {
        Block b = (Block) blocks.get(i).getBlock();
        System.out.println("testCopyOnWrite detaching block " + b);
        assertTrue("Detaching block " + b + " should have returned false",
                   dataset.detachBlock(b, 1) == false);
      }

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
