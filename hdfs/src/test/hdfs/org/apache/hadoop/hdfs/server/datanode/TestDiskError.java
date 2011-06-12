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


import java.io.DataOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Sender;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/** Test if a datanode can correctly handle errors during block read/write*/
public class TestDiskError extends TestCase {
  public void testShutdown() throws Exception {
    if (System.getProperty("os.name").startsWith("Windows")) {
      /**
       * This test depends on OS not allowing file creations on a directory
       * that does not have write permissions for the user. Apparently it is 
       * not the case on Windows (at least under Cygwin), and possibly AIX.
       * This is disabled on Windows.
       */
      return;
    }
    // bring up a cluster of 3
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512L);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    final int dnIndex = 0;
    String dataDir = cluster.getDataDirectory();
    File dir1 = new File(new File(dataDir, "data"+(2*dnIndex+1)), "current/rbw");
    File dir2 = new File(new File(dataDir, "data"+(2*dnIndex+2)), "current/rbw");
    try {
      // make the data directory of the first datanode to be readonly
      assertTrue(dir1.setReadOnly());
      assertTrue(dir2.setReadOnly());

      // create files and make sure that first datanode will be down
      DataNode dn = cluster.getDataNodes().get(dnIndex);
      for (int i=0; DataNode.isDatanodeUp(dn); i++) {
        Path fileName = new Path("/test.txt"+i);
        DFSTestUtil.createFile(fs, fileName, 1024, (short)2, 1L);
        DFSTestUtil.waitReplication(fs, fileName, (short)2);
        fs.delete(fileName, true);
      }
    } finally {
      // restore its old permission
      dir1.setWritable(true);
      dir2.setWritable(true);
      cluster.shutdown();
    }
  }
  
  public void testReplicationError() throws Exception {
    // bring up a cluster of 1
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    
    try {
      // create a file of replication factor of 1
      final Path fileName = new Path("/test.txt");
      final int fileLen = 1;
      DFSTestUtil.createFile(fs, fileName, 1, (short)1, 1L);
      DFSTestUtil.waitReplication(fs, fileName, (short)1);

      // get the block belonged to the created file
      LocatedBlocks blocks = NameNodeAdapter.getBlockLocations(
          cluster.getNameNode(), fileName.toString(), 0, (long)fileLen);
      assertEquals(blocks.locatedBlockCount(), 1);
      LocatedBlock block = blocks.get(0);
      
      // bring up a second datanode
      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.waitActive();
      final int sndNode = 1;
      DataNode datanode = cluster.getDataNodes().get(sndNode);
      
      // replicate the block to the second datanode
      InetSocketAddress target = datanode.getSelfAddr();
      Socket s = new Socket(target.getAddress(), target.getPort());
        //write the header.
      DataOutputStream out = new DataOutputStream(
          s.getOutputStream());

      Sender.opWriteBlock(out, block.getBlock().getBlockId(), 
          block.getBlock().getGenerationStamp(), 1, 
          BlockConstructionStage.PIPELINE_SETUP_CREATE, 
          0L, 0L, 0L, "", null, new DatanodeInfo[0], 
          BlockAccessToken.DUMMY_TOKEN);

      // write check header
      out.writeByte( 1 );
      out.writeInt( 512 );

      out.flush();

      // close the connection before sending the content of the block
      out.close();
      
      // the temporary block & meta files should be deleted
      String dataDir = cluster.getDataDirectory();
      File dir1 = new File(new File(dataDir, "data"+(2*sndNode+1)), "current/rbw");
      File dir2 = new File(new File(dataDir, "data"+(2*sndNode+2)), "current/rbw");
      while (dir1.listFiles().length != 0 || dir2.listFiles().length != 0) {
        Thread.sleep(100);
      }
      
      // then increase the file's replication factor
      fs.setReplication(fileName, (short)2);
      // replication should succeed
      DFSTestUtil.waitReplication(fs, fileName, (short)1);
      
      // clean up the file
      fs.delete(fileName, false);
    } finally {
      cluster.shutdown();
    }
  }

  public void testLocalDirs() throws Exception {
    Configuration conf = new Configuration();
    final String permStr = "755";
    FsPermission expected = new FsPermission(permStr);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY, permStr);
    MiniDFSCluster cluster = null;

    try {
      // Start the cluster
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();

      // Check permissions on directories in 'dfs.data.dir'
      FileSystem localFS = FileSystem.getLocal(conf);
      for (DataNode dn : cluster.getDataNodes()) {
        String[] dataDirs =
          dn.getConf().getStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
        for (String dir : dataDirs) {
          Path dataDir = new Path(dir);
          FsPermission actual = localFS.getFileStatus(dataDir).getPermission();
          assertEquals("Permission for dir: " + dataDir + ", is " + actual +
                           ", while expected is " + expected,
                       expected, actual);
        }
      }
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }

  }
}
