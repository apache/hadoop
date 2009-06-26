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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

/**
 * Test for metrics published by the Namenode
 */
public class TestNameNodeMetrics extends TestCase {
  private static final Configuration CONF = new Configuration();
  static {
    CONF.setLong("dfs.block.size", 100);
    CONF.setInt("io.bytes.per.checksum", 1);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setInt("dfs.replication.interval", 1);
  }
  
  private MiniDFSCluster cluster;
  private FSNamesystemMetrics metrics;
  private DistributedFileSystem fs;
  private Random rand = new Random();
  private FSNamesystem namesystem;

  @Override
  protected void setUp() throws Exception {
    cluster = new MiniDFSCluster(CONF, 3, true, null);
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    fs = (DistributedFileSystem) cluster.getFileSystem();
    metrics = namesystem.getFSNamesystemMetrics();
  }
  
  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  /** create a file with a length of <code>fileLen</code> */
  private void createFile(String fileName, long fileLen, short replicas) throws IOException {
    Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, fileLen, replicas, rand.nextLong());
  }

  private void updateMetrics() throws Exception {
    // Wait for metrics update (corresponds to dfs.replication.interval
    // for some block related metrics to get updated)
    Thread.sleep(1000);
    metrics.doUpdates(null);
  }

  /** Test metrics associated with addition of a file */
  public void testFileAdd() throws Exception {
    // Add files with 100 blocks
    final String file = "/tmp/t";
    createFile(file, 3200, (short)3);
    final int blockCount = 32;
    int blockCapacity = namesystem.getBlockCapacity();
    updateMetrics();
    assertEquals(blockCapacity, metrics.blockCapacity.get());

    // Blocks are stored in a hashmap. Compute its capacity, which
    // doubles every time the number of entries reach the threshold.
    int threshold = (int)(blockCapacity * FSNamesystem.DEFAULT_MAP_LOAD_FACTOR);
    while (threshold < blockCount) {
      blockCapacity <<= 1;
    }
    updateMetrics();
    assertEquals(3, metrics.filesTotal.get());
    assertEquals(blockCount, metrics.blocksTotal.get());
    assertEquals(blockCapacity, metrics.blockCapacity.get());
    fs.delete(new Path(file), true);
  }
  
  /** Corrupt a block and ensure metrics reflects it */
  public void testCorruptBlock() throws Exception {
    // Create a file with single block with two replicas
    String file = "/tmp/t";
    createFile(file, 100, (short)2);
    
    // Corrupt first replica of the block
    LocatedBlock block = namesystem.getBlockLocations(file, 0, 1).get(0);
    namesystem.markBlockAsCorrupt(block.getBlock(), block.getLocations()[0]);
    updateMetrics();
    assertEquals(1, metrics.corruptBlocks.get());
    assertEquals(1, metrics.pendingReplicationBlocks.get());
    assertEquals(1, metrics.scheduledReplicationBlocks.get());
    fs.delete(new Path(file), true);
    updateMetrics();
    assertEquals(0, metrics.corruptBlocks.get());
    assertEquals(0, metrics.pendingReplicationBlocks.get());
    assertEquals(0, metrics.scheduledReplicationBlocks.get());
  }
  
  /** Create excess blocks by reducing the replication factor for
   * for a file and ensure metrics reflects it
   */
  public void testExcessBlocks() throws Exception {
    String file = "/tmp/t";
    createFile(file, 100, (short)2);
    int totalBlocks = 1;
    namesystem.setReplication(file, (short)1);
    updateMetrics();
    assertEquals(totalBlocks, metrics.excessBlocks.get());
    assertEquals(totalBlocks, metrics.pendingDeletionBlocks.get());
    fs.delete(new Path(file), true);
  }
  
  /** Test to ensure metrics reflects missing blocks */
  public void testMissingBlock() throws Exception {
    // Create a file with single block with two replicas
    String file = "/tmp/t";
    createFile(file, 100, (short)1);
    
    // Corrupt the only replica of the block to result in a missing block
    LocatedBlock block = namesystem.getBlockLocations(file, 0, 1).get(0);
    namesystem.markBlockAsCorrupt(block.getBlock(), block.getLocations()[0]);
    updateMetrics();
    assertEquals(1, metrics.underReplicatedBlocks.get());
    assertEquals(1, metrics.missingBlocks.get());
    fs.delete(new Path(file), true);
    updateMetrics();
    assertEquals(0, metrics.underReplicatedBlocks.get());
  }
}
