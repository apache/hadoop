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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/** Test if a datanode can correctly upgrade itself */
public class TestDatanodeRestart {
  // test finalized replicas persist across DataNode restarts
  @Test public void testFinalizedReplicas() throws Exception {
    // bring up a cluster of 3
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024L);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 512);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      // test finalized replicas
      final String TopDir = "/test";
      DFSTestUtil util = new DFSTestUtil.Builder().
          setName("TestDatanodeRestart").setNumFiles(2).build();
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
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
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
      for (FsVolumeSpi v : dataset(dn).getVolumes()) {
        final FsVolumeImpl volume = (FsVolumeImpl)v;
        File currentDir = volume.getCurrentDir().getParentFile().getParentFile();
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
      String bpid = cluster.getNamesystem().getBlockPoolId();
      ReplicaMap replicas = dataset(dn).volumeMap;
      Assert.assertEquals(1, replicas.size(bpid));
      ReplicaInfo replica = replicas.replicas(bpid).iterator().next();
      Assert.assertEquals(ReplicaState.RWR, replica.getState());
      if (isCorrupt) {
        Assert.assertEquals((fileLen-1)/512*512, replica.getNumBytes());
      } else {
        Assert.assertEquals(fileLen, replica.getNumBytes());
      }
      dataset(dn).invalidate(bpid, new Block[]{replica});
    } finally {
      IOUtils.closeStream(out);
      if (fs.exists(src)) {
        fs.delete(src, false);
      }
      fs.close();
    }      
  }

  // test recovering unlinked tmp replicas
  @Test public void testRecoverReplicas() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024L);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 512);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    try {
      FileSystem fs = cluster.getFileSystem();
      for (int i=0; i<4; i++) {
        Path fileName = new Path("/test"+i);
        DFSTestUtil.createFile(fs, fileName, 1, (short)1, 0L);
        DFSTestUtil.waitReplication(fs, fileName, (short)1);
      }
      String bpid = cluster.getNamesystem().getBlockPoolId();
      DataNode dn = cluster.getDataNodes().get(0);
      Iterator<ReplicaInfo> replicasItor = 
          dataset(dn).volumeMap.replicas(bpid).iterator();
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
      Collection<ReplicaInfo> replicas = dataset(dn).volumeMap.replicas(bpid);
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

  private static FsDatasetImpl dataset(DataNode dn) {
    return (FsDatasetImpl)DataNodeTestUtils.getFSDataset(dn);
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
    File dst = DatanodeUtil.getUnlinkTmpFile(src);
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

  @Test
  public void testWaitForRegistrationOnRestart() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_BP_READY_TIMEOUT_KEY, 5);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5000);

    // This makes the datanode appear registered to the NN, but it won't be
    // able to get to the saved dn reg internally.
    DataNodeFaultInjector dnFaultInjector = new DataNodeFaultInjector() {
      @Override
      public void noRegistration() throws IOException {
        throw new IOException("no reg found for testing");
      }
    };
    DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector.set(dnFaultInjector);
    MiniDFSCluster cluster = null;
    long start = 0;
    Path file = new Path("/reg");
    try {
      int numDNs = 1;
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
      cluster.waitActive();

      start = System.currentTimeMillis();
      FileSystem fileSys = cluster.getFileSystem();
      try {
        DFSTestUtil.createFile(fileSys, file, 10240L, (short)1, 0L);
        // It is a bug if this does not fail.
        throw new IOException("Did not fail!");
      } catch (org.apache.hadoop.ipc.RemoteException e) {
        long elapsed = System.currentTimeMillis() - start;
        // timers have at-least semantics, so it should be at least 5 seconds.
        if (elapsed < 5000 || elapsed > 10000) {
          throw new IOException(elapsed + " seconds passed.", e);
        }
      }
      DataNodeFaultInjector.set(oldDnInjector);
      // this should succeed now.
      DFSTestUtil.createFile(fileSys, file, 10240L, (short)1, 0L);

      // turn it back to under-construction, so that the client calls
      // getReplicaVisibleLength() rpc method against the datanode.
      fileSys.append(file);
      // back to simulating unregistered node.
      DataNodeFaultInjector.set(dnFaultInjector);
      byte[] buffer = new byte[8];
      start = System.currentTimeMillis();
      try {
        fileSys.open(file).read(0L, buffer, 0, 1);
        throw new IOException("Did not fail!");
      } catch (IOException e) {
        long elapsed = System.currentTimeMillis() - start;
        if (e.getMessage().contains("readBlockLength")) {
          throw new IOException("Failed, but with unexpected exception:", e);
        }
        // timers have at-least semantics, so it should be at least 5 seconds.
        if (elapsed < 5000 || elapsed > 10000) {
          throw new IOException(elapsed + " seconds passed.", e);
        }
      }
      DataNodeFaultInjector.set(oldDnInjector);
      fileSys.open(file).read(0L, buffer, 0, 1);
    } finally {
      DataNodeFaultInjector.set(oldDnInjector);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
