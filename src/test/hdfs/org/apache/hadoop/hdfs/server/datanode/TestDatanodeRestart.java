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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.junit.Test;
import org.junit.Assert;

/** Test if a datanode can correctly upgrade itself */
public class TestDatanodeRestart {
  // test finalized replicas persist across DataNode restarts
  @Test public void testFinalizedReplicas() throws Exception {
    // bring up a cluster of 3
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024L);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 512);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      // test finalized replicas
      final String TopDir = "/test";
      DFSTestUtil util = new DFSTestUtil("TestCrcCorruption", 2, 3, 8*1024);
      util.createFiles(fs, TopDir, (short)3);
      util.waitReplication(fs, TopDir, (short)3);
      util.checkFiles(fs, TopDir);
      cluster.restartDataNodes();
      cluster.waitActive();
      util.checkFiles(fs, TopDir);
    } finally {
      cluster.shutdown();
    }
  }
  
  // test rbw replicas persist across DataNode restarts
  public void testRbwReplicas() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024L);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 512);
    conf.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    cluster.waitActive();
    try {
      testRbwReplicas(cluster, false);
      testRbwReplicas(cluster, true);
    } finally {
      cluster.shutdown();
    }
  }
    
  private void testRbwReplicas(MiniDFSCluster cluster, boolean isCorrupt) 
  throws IOException {
    FSDataOutputStream out = null;
    FileSystem fs = cluster.getFileSystem();
    final Path src = new Path("/test.txt");
    try {
      final int fileLen = 515;
      // create some rbw replicas on disk
      byte[] writeBuf = new byte[fileLen];
      new Random().nextBytes(writeBuf);
      out = fs.create(src);
      out.write(writeBuf);
      out.hflush();
      DataNode dn = cluster.getDataNodes().get(0);
      for (FSVolume volume : ((FSDataset)dn.data).volumes.volumes) {
        File currentDir = volume.getDir().getParentFile();
        File rbwDir = new File(currentDir, "rbw");
        for (File file : rbwDir.listFiles()) {
          if (isCorrupt && Block.isBlockFilename(file)) {
            new RandomAccessFile(file, "rw").setLength(fileLen-1); // corrupt
          }
        }
      }
      cluster.restartDataNodes();
      cluster.waitActive();
      dn = cluster.getDataNodes().get(0);

      // check volumeMap: one rwr replica
      ReplicasMap replicas = ((FSDataset)(dn.data)).volumeMap;
      Assert.assertEquals(1, replicas.size());
      ReplicaInfo replica = replicas.replicas().iterator().next();
      Assert.assertEquals(ReplicaState.RWR, replica.getState());
      if (isCorrupt) {
        Assert.assertEquals((fileLen-1)/512*512, replica.getNumBytes());
      } else {
        Assert.assertEquals(fileLen, replica.getNumBytes());
      }
      dn.data.invalidate(new Block[]{replica});
    } finally {
      IOUtils.closeStream(out);
      if (fs.exists(src)) {
        fs.delete(src, false);
      }
      fs.close();
    }      
  }

  // test recovering unlinked tmp replicas
  @Test public void testRecoverReplicas() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024L);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 512);
    conf.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    try {
      FileSystem fs = cluster.getFileSystem();
      for (int i=0; i<4; i++) {
        Path fileName = new Path("/test"+i);
        DFSTestUtil.createFile(fs, fileName, 1, (short)1, 0L);
        DFSTestUtil.waitReplication(fs, fileName, (short)1);
      }
      DataNode dn = cluster.getDataNodes().get(0);
      Iterator<ReplicaInfo> replicasItor = 
        ((FSDataset)dn.data).volumeMap.replicas().iterator();
      ReplicaInfo replica = replicasItor.next();
      createUnlinkTmpFile(replica, true, true); // rename block file
      createUnlinkTmpFile(replica, false, true); // rename meta file
      replica = replicasItor.next();
      createUnlinkTmpFile(replica, true, false); // copy block file
      createUnlinkTmpFile(replica, false, false); // copy meta file
      replica = replicasItor.next();
      createUnlinkTmpFile(replica, true, true); // rename block file
      createUnlinkTmpFile(replica, false, false); // copy meta file

      cluster.restartDataNodes();
      cluster.waitActive();
      dn = cluster.getDataNodes().get(0);

      // check volumeMap: 4 finalized replica
      Collection<ReplicaInfo> replicas = 
        ((FSDataset)(dn.data)).volumeMap.replicas();
      Assert.assertEquals(4, replicas.size());
      replicasItor = replicas.iterator();
      while (replicasItor.hasNext()) {
        Assert.assertEquals(ReplicaState.FINALIZED, 
            replicasItor.next().getState());
      }
    } finally {
      cluster.shutdown();
    }
  }

  private static void createUnlinkTmpFile(ReplicaInfo replicaInfo, 
      boolean changeBlockFile, 
      boolean isRename) throws IOException {
    File src;
    if (changeBlockFile) {
      src = replicaInfo.getBlockFile();
    } else {
      src = replicaInfo.getMetaFile();
    }
    File dst = FSDataset.getUnlinkTmpFile(src);
    if (isRename) {
      src.renameTo(dst);
    } else {
      FileInputStream in = new FileInputStream(src);
      try {
        FileOutputStream out = new FileOutputStream(dst);
        try {
          IOUtils.copyBytes(in, out, 1);
        } finally {
          out.close();
        }
      } finally {
        in.close();
      }
    }
  }
}
