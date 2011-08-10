/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

/**
 * Test {@link FSUtils}.
 */
public class TestFSUtils {
  
  @Test public void testIsHDFS() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    htu.getConfiguration().setBoolean("dfs.support.append", false);
    assertFalse(FSUtils.isHDFS(htu.getConfiguration()));
    htu.getConfiguration().setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = null;
    try {
      cluster = htu.startMiniDFSCluster(1);
      assertTrue(FSUtils.isHDFS(htu.getConfiguration()));
      assertTrue(FSUtils.isAppendSupported(htu.getConfiguration()));
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }
  
  private void WriteDataToHDFS(FileSystem fs, Path file, int dataSize)
    throws Exception {
    FSDataOutputStream out = fs.create(file);
    byte [] data = new byte[dataSize];
    out.write(data, 0, dataSize);
    out.close();
  }
  
  @Test public void testcomputeHDFSBlocksDistribution() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    final int DEFAULT_BLOCK_SIZE = 1024;
    htu.getConfiguration().setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    MiniDFSCluster cluster = null;
    Path testFile = null;
    
    try {
      // set up a cluster with 3 nodes
      String hosts[] = new String[] { "host1", "host2", "host3" };
      cluster = htu.startMiniDFSCluster(hosts);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create a file with two blocks
      testFile = new Path("/test1.txt");
      WriteDataToHDFS(fs, testFile, 2*DEFAULT_BLOCK_SIZE);
      
      // given the default replication factor is 3, the same as the number of
      // datanodes; the locality index for each host should be 100%,
      // or getWeight for each host should be the same as getUniqueBlocksWeights
      FileStatus status = fs.getFileStatus(testFile);
      HDFSBlocksDistribution blocksDistribution =
        FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
      long uniqueBlocksTotalWeight =
        blocksDistribution.getUniqueBlocksTotalWeight();
      for (String host : hosts) {
        long weight = blocksDistribution.getWeight(host);
        assertTrue(uniqueBlocksTotalWeight == weight);
      }
    } finally {
      htu.shutdownMiniDFSCluster();
    }

    
    try {
      // set up a cluster with 4 nodes
      String hosts[] = new String[] { "host1", "host2", "host3", "host4" };
      cluster = htu.startMiniDFSCluster(hosts);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create a file with three blocks
      testFile = new Path("/test2.txt");        
      WriteDataToHDFS(fs, testFile, 3*DEFAULT_BLOCK_SIZE);
              
      // given the default replication factor is 3, we will have total of 9
      // replica of blocks; thus the host with the highest weight should have
      // weight == 3 * DEFAULT_BLOCK_SIZE
      FileStatus status = fs.getFileStatus(testFile);
      HDFSBlocksDistribution blocksDistribution =
        FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
      long uniqueBlocksTotalWeight =
        blocksDistribution.getUniqueBlocksTotalWeight();
      
      String tophost = blocksDistribution.getTopHosts().get(0);
      long weight = blocksDistribution.getWeight(tophost);
      assertTrue(uniqueBlocksTotalWeight == weight);
      
    } finally {
      htu.shutdownMiniDFSCluster();
    }

    
    try {
      // set up a cluster with 4 nodes
      String hosts[] = new String[] { "host1", "host2", "host3", "host4" };
      cluster = htu.startMiniDFSCluster(hosts);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create a file with one block
      testFile = new Path("/test3.txt");        
      WriteDataToHDFS(fs, testFile, DEFAULT_BLOCK_SIZE);
      
      // given the default replication factor is 3, we will have total of 3
      // replica of blocks; thus there is one host without weight
      FileStatus status = fs.getFileStatus(testFile);
      HDFSBlocksDistribution blocksDistribution =
        FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
      assertTrue(blocksDistribution.getTopHosts().size() == 3);
    } finally {
      htu.shutdownMiniDFSCluster();
    }
    
  }
  
}