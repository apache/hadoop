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
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test WebHDFS which provides data locality using HTTP redirection.
 */
public class TestWebHdfsDataLocality {
  static final Log LOG = LogFactory.getLog(TestWebHdfsDataLocality.class);
  {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }
  
  private static final String RACK0 = "/rack0";
  private static final String RACK1 = "/rack1";
  private static final String RACK2 = "/rack2";

  @Test
  public void testDataLocality() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    final String[] racks = {RACK0, RACK0, RACK1, RACK1, RACK2, RACK2};
    final int nDataNodes = racks.length;
    LOG.info("nDataNodes=" + nDataNodes + ", racks=" + Arrays.asList(racks));

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(nDataNodes)
        .racks(racks)
        .build();
    try {
      cluster.waitActive();

      final DistributedFileSystem dfs = cluster.getFileSystem();
      final NameNode namenode = cluster.getNameNode();
      final DatanodeManager dm = namenode.getNamesystem().getBlockManager(
          ).getDatanodeManager();
      LOG.info("dm=" + dm);
  
      final long blocksize = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
      final String f = "/foo";

      { //test CREATE
        for(int i = 0; i < nDataNodes; i++) {
          //set client address to a particular datanode
          final DataNode dn = cluster.getDataNodes().get(i);
          final String ipAddr = dm.getDatanode(dn.getDatanodeId()).getIpAddr();

          //The chosen datanode must be the same as the client address
          final DatanodeInfo chosen = NamenodeWebHdfsMethods.chooseDatanode(
              namenode, f, PutOpParam.Op.CREATE, -1L, blocksize, null);
          Assert.assertEquals(ipAddr, chosen.getIpAddr());
        }
      }
  
      //create a file with one replica.
      final Path p = new Path(f);
      final FSDataOutputStream out = dfs.create(p, (short)1);
      out.write(1);
      out.close();
  
      //get replica location.
      final LocatedBlocks locatedblocks = NameNodeAdapter.getBlockLocations(
          namenode, f, 0, 1);
      final List<LocatedBlock> lb = locatedblocks.getLocatedBlocks();
      Assert.assertEquals(1, lb.size());
      final DatanodeInfo[] locations = lb.get(0).getLocations();
      Assert.assertEquals(1, locations.length);
      final DatanodeInfo expected = locations[0];
      
      //For GETFILECHECKSUM, OPEN and APPEND,
      //the chosen datanode must be the same as the replica location.

      { //test GETFILECHECKSUM
        final DatanodeInfo chosen = NamenodeWebHdfsMethods.chooseDatanode(
            namenode, f, GetOpParam.Op.GETFILECHECKSUM, -1L, blocksize, null);
        Assert.assertEquals(expected, chosen);
      }
  
      { //test OPEN
        final DatanodeInfo chosen = NamenodeWebHdfsMethods.chooseDatanode(
            namenode, f, GetOpParam.Op.OPEN, 0, blocksize, null);
        Assert.assertEquals(expected, chosen);
      }

      { //test APPEND
        final DatanodeInfo chosen = NamenodeWebHdfsMethods.chooseDatanode(
            namenode, f, PostOpParam.Op.APPEND, -1L, blocksize, null);
        Assert.assertEquals(expected, chosen);
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testExcludeDataNodes() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    final String[] racks = {RACK0, RACK0, RACK1, RACK1, RACK2, RACK2};
    final String[] hosts = {"DataNode1", "DataNode2", "DataNode3","DataNode4","DataNode5","DataNode6"};
    final int nDataNodes = hosts.length;
    LOG.info("nDataNodes=" + nDataNodes + ", racks=" + Arrays.asList(racks)
        + ", hosts=" + Arrays.asList(hosts));

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .hosts(hosts).numDataNodes(nDataNodes).racks(racks).build();
    
    try {
      cluster.waitActive();

      final DistributedFileSystem dfs = cluster.getFileSystem();
      final NameNode namenode = cluster.getNameNode();
      final DatanodeManager dm = namenode.getNamesystem().getBlockManager(
          ).getDatanodeManager();
      LOG.info("dm=" + dm);
  
      final long blocksize = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
      final String f = "/foo";
      
      //create a file with three replica.
      final Path p = new Path(f);
      final FSDataOutputStream out = dfs.create(p, (short)3);
      out.write(1);
      out.close(); 
      
      //get replica location.
      final LocatedBlocks locatedblocks = NameNodeAdapter.getBlockLocations(
          namenode, f, 0, 1);
      final List<LocatedBlock> lb = locatedblocks.getLocatedBlocks();
      Assert.assertEquals(1, lb.size());
      final DatanodeInfo[] locations = lb.get(0).getLocations();
      Assert.assertEquals(3, locations.length);
      
      
      //For GETFILECHECKSUM, OPEN and APPEND,
      //the chosen datanode must be different with exclude nodes.

      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < 2; i++) {
        sb.append(locations[i].getXferAddr());
        { // test GETFILECHECKSUM
          final DatanodeInfo chosen = NamenodeWebHdfsMethods.chooseDatanode(
              namenode, f, GetOpParam.Op.GETFILECHECKSUM, -1L, blocksize,
              sb.toString());
          for (int j = 0; j <= i; j++) {
            Assert.assertNotEquals(locations[j].getHostName(),
                chosen.getHostName());
          }
        }

        { // test OPEN
          final DatanodeInfo chosen = NamenodeWebHdfsMethods.chooseDatanode(
              namenode, f, GetOpParam.Op.OPEN, 0, blocksize, sb.toString());
          for (int j = 0; j <= i; j++) {
            Assert.assertNotEquals(locations[j].getHostName(),
                chosen.getHostName());
          }
        }
  
        { // test APPEND
          final DatanodeInfo chosen = NamenodeWebHdfsMethods
              .chooseDatanode(namenode, f, PostOpParam.Op.APPEND, -1L,
                  blocksize, sb.toString());
          for (int j = 0; j <= i; j++) {
            Assert.assertNotEquals(locations[j].getHostName(),
                chosen.getHostName());
          }
        }
        
        sb.append(",");
      }
    } finally {
      cluster.shutdown();
    }
  }
}