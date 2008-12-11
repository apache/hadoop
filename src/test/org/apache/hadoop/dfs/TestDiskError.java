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

import java.io.DataOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.dfs.DFSTestUtil;
import org.apache.hadoop.dfs.FSConstants;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/** Test if a datanode can correctly handle errors during block read/write*/
public class TestDiskError extends TestCase {
  public void testShutdown() throws Exception {
    // bring up a cluster of 3
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", 512L);
    conf.setInt("dfs.dataXceiver.timeoutInMS", 1000);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    final int dnIndex = 0;

    FileSystem localFs = FileSystem.getLocal(conf);
    Path dataDir = new Path(
      System.getProperty("test.build.data", "build/test/data"), "dfs");
    dataDir = new Path(dataDir, "data");
    Path dir1 = new Path(new Path(dataDir, "data"+(2*dnIndex+1)), "tmp");
    Path dir2 = new Path(new Path(dataDir, "data"+(2*dnIndex+2)), "tmp");
    FsPermission oldPerm1 = localFs.getFileStatus(dir1).getPermission();
    FsPermission oldPerm2 = localFs.getFileStatus(dir2).getPermission();
    try {
      // make the data directory of the first datanode to be readonly
      final FsPermission readPermission =
        new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);
      localFs.setPermission(dir1, readPermission);
      localFs.setPermission(dir2, readPermission);

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
      localFs.setPermission(dir1, oldPerm1);
      localFs.setPermission(dir2, oldPerm2);
      cluster.shutdown();
    }
  }
  
  public void testReplicationError() throws Exception {
    // bring up a cluster of 1
    Configuration conf = new Configuration();
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
      LocatedBlocks blocks = cluster.getNameNode().namesystem.getBlockLocations(
          fileName.toString(), 0, (long)fileLen);
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

      out.writeShort( FSConstants.DATA_TRANSFER_VERSION );
      out.write( FSConstants.OP_WRITE_BLOCK );
      out.writeLong( block.getBlock().getBlockId());
      out.writeLong( block.getBlock().getGenerationStamp() );
      out.writeInt(1);
      out.writeBoolean( false );       // recovery flag
      Text.writeString( out, "" );
      out.writeBoolean(false); // Not sending src node information
      out.writeInt(0);
      
      // write check header
      out.writeByte( 1 );
      out.writeInt( 512 );

      out.flush();

      // close the connection before sending the content of the block
      out.close();
      
      // the temporary block & meta files should be deleted
      String dataDir = new File(
         System.getProperty("test.build.data", "build/test/data"), 
         "dfs").toString() + "/data";
      File dir1 = new File(new File(dataDir, "data"+(2*sndNode+1)), "tmp");
      File dir2 = new File(new File(dataDir, "data"+(2*sndNode+2)), "tmp");
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
}
