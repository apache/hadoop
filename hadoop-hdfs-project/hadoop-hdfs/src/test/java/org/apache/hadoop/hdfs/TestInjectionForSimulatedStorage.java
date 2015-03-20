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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.Time;
import org.junit.Test;


/**
 * This class tests the replication and injection of blocks of a DFS file for simulated storage.
 */
public class TestInjectionForSimulatedStorage {
  private final int checksumSize = 16;
  private final int blockSize = checksumSize*2;
  private final int numBlocks = 4;
  private final int filesize = blockSize*numBlocks;
  private final int numDataNodes = 4;
  private static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.hdfs.TestInjectionForSimulatedStorage");

  
  private void writeFile(FileSystem fileSys, Path name, int repl)
                                                throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[filesize];
    for (int i=0; i<buffer.length; i++) {
      buffer[i] = '1';
    }
    stm.write(buffer);
    stm.close();
  }
  
  // Waits for all of the blocks to have expected replication

  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(String filename, 
                                       ClientProtocol namenode,
                                       int expected, long maxWaitSec) 
                                       throws IOException {
    long start = Time.monotonicNow();
    
    //wait for all the blocks to be replicated;
    LOG.info("Checking for block replication for " + filename);
    
    LocatedBlocks blocks = namenode.getBlockLocations(filename, 0, Long.MAX_VALUE);
    assertEquals(numBlocks, blocks.locatedBlockCount());
    
    for (int i = 0; i < numBlocks; ++i) {
      LOG.info("Checking for block:" + (i+1));
      while (true) { // Loop to check for block i (usually when 0 is done all will be done
        blocks = namenode.getBlockLocations(filename, 0, Long.MAX_VALUE);
        assertEquals(numBlocks, blocks.locatedBlockCount());
        LocatedBlock block = blocks.get(i);
        int actual = block.getLocations().length;
        if ( actual == expected ) {
          LOG.info("Got enough replicas for " + (i+1) + "th block " + block.getBlock() +
              ", got " + actual + ".");
          break;
        }
        LOG.info("Not enough replicas for " + (i+1) + "th block " + block.getBlock() +
                               " yet. Expecting " + expected + ", got " + 
                               actual + ".");
      
        if (maxWaitSec > 0 && 
            (Time.monotonicNow() - start) > (maxWaitSec * 1000)) {
          throw new IOException("Timedout while waiting for all blocks to " +
                                " be replicated for " + filename);
        }
      
        try {
          Thread.sleep(500);
        } catch (InterruptedException ignored) {}
      }
    }
  }
 
  
  
  /* This test makes sure that NameNode retries all the available blocks 
   * for under replicated blocks. This test uses simulated storage and one
   * of its features to inject blocks,
   * 
   * It creates a file with several blocks and replication of 4. 
   * The cluster is then shut down - NN retains its state but the DNs are 
   * all simulated and hence loose their blocks. 
   * The blocks are then injected in one of the DNs. The  expected behaviour is
   * that the NN will arrange for themissing replica will be copied from a valid source.
   */
  @Test
  public void testInjection() throws IOException {
    
    MiniDFSCluster cluster = null;

    String testFile = "/replication-test-file";
    Path testPath = new Path(testFile);
    
    byte buffer[] = new byte[1024];
    for (int i=0; i<buffer.length; i++) {
      buffer[i] = '1';
    }
    
    try {
      Configuration conf = new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, Integer.toString(numDataNodes));
      conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, checksumSize);
      SimulatedFSDataset.setFactory(conf);
      //first time format
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      String bpid = cluster.getNamesystem().getBlockPoolId();
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                            cluster.getNameNodePort()),
                                            conf);
      
      writeFile(cluster.getFileSystem(), testPath, numDataNodes);
      waitForBlockReplication(testFile, dfsClient.getNamenode(), numDataNodes, 20);
      List<Map<DatanodeStorage, BlockListAsLongs>> blocksList = cluster.getAllBlockReports(bpid);
      
      cluster.shutdown();
      cluster = null;
      
      /* Start the MiniDFSCluster with more datanodes since once a writeBlock
       * to a datanode node fails, same block can not be written to it
       * immediately. In our case some replication attempts will fail.
       */
      
      LOG.info("Restarting minicluster");
      conf = new HdfsConfiguration();
      SimulatedFSDataset.setFactory(conf);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.0f"); 
      
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(numDataNodes * 2)
                                  .format(false)
                                  .build();
      cluster.waitActive();
      Set<Block> uniqueBlocks = new HashSet<Block>();
      for(Map<DatanodeStorage, BlockListAsLongs> map : blocksList) {
        for(BlockListAsLongs blockList : map.values()) {
          for(Block b : blockList) {
            uniqueBlocks.add(new Block(b));
          }
        }
      }
      // Insert all the blocks in the first data node
      
      LOG.info("Inserting " + uniqueBlocks.size() + " blocks");
      cluster.injectBlocks(0, uniqueBlocks, null);
      
      dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                  cluster.getNameNodePort()),
                                  conf);
      
      waitForBlockReplication(testFile, dfsClient.getNamenode(), numDataNodes, -1);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }  
}
