/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DU_INTERVAL_KEY;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for ReplicaCachingGetSpaceUsed class.
 */
public class TestReplicaCachingGetSpaceUsed {
  private Configuration conf = null;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DataNode dataNode;

  @Before
  public void setUp()
      throws IOException, NoSuchMethodException, InterruptedException {
    conf = new Configuration();
    conf.setClass("fs.getspaceused.classname", ReplicaCachingGetSpaceUsed.class,
        CachingGetSpaceUsed.class);
    conf.setLong(FS_DU_INTERVAL_KEY, 1000);
    conf.setLong("fs.getspaceused.jitterMillis", 0);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    dataNode = cluster.getDataNodes().get(0);

    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testReplicaCachingGetSpaceUsedByFINALIZEDReplica()
      throws Exception {
    FSDataOutputStream os = fs
        .create(new Path("/testReplicaCachingGetSpaceUsedByFINALIZEDReplica"));
    byte[] bytes = new byte[20480];
    InputStream is = new ByteArrayInputStream(bytes);
    IOUtils.copyBytes(is, os, bytes.length);
    os.hsync();
    os.close();

    DFSInputStream dfsInputStream = fs.getClient()
        .open("/testReplicaCachingGetSpaceUsedByFINALIZEDReplica");
    long blockLength = 0;
    long metaLength = 0;
    List<LocatedBlock> locatedBlocks = dfsInputStream.getAllBlocks();
    for (LocatedBlock locatedBlock : locatedBlocks) {
      ExtendedBlock extendedBlock = locatedBlock.getBlock();
      blockLength += extendedBlock.getLocalBlock().getNumBytes();
      metaLength += dataNode.getFSDataset()
          .getMetaDataInputStream(extendedBlock).getLength();
    }

    // Guarantee ReplicaCachingGetSpaceUsed#refresh() is called after replica
    // has been written to disk.
    Thread.sleep(2000);
    assertEquals(blockLength + metaLength,
        dataNode.getFSDataset().getDfsUsed());

    fs.delete(new Path("/testReplicaCachingGetSpaceUsedByFINALIZEDReplica"),
        true);
  }

  @Test
  public void testReplicaCachingGetSpaceUsedByRBWReplica() throws Exception {
 // This test cannot pass on Windows
    assumeNotWindows();
    FSDataOutputStream os =
        fs.create(new Path("/testReplicaCachingGetSpaceUsedByRBWReplica"));
    byte[] bytes = new byte[20480];
    InputStream is = new ByteArrayInputStream(bytes);
    IOUtils.copyBytes(is, os, bytes.length);
    os.hsync();

    DFSInputStream dfsInputStream =
        fs.getClient().open("/testReplicaCachingGetSpaceUsedByRBWReplica");
    long blockLength = 0;
    long metaLength = 0;
    List<LocatedBlock> locatedBlocks = dfsInputStream.getAllBlocks();
    for (LocatedBlock locatedBlock : locatedBlocks) {
      ExtendedBlock extendedBlock = locatedBlock.getBlock();
      blockLength += extendedBlock.getLocalBlock().getNumBytes();
      metaLength += dataNode.getFSDataset()
          .getMetaDataInputStream(extendedBlock).getLength();
    }

    // Guarantee ReplicaCachingGetSpaceUsed#refresh() is called after replica
    // has been written to disk.
    Thread.sleep(2000);
    assertEquals(blockLength + metaLength,
        dataNode.getFSDataset().getDfsUsed());

    os.close();

    // Guarantee ReplicaCachingGetSpaceUsed#refresh() is called, dfsspaceused is
    // recalculated
    Thread.sleep(2000);
    // After close operation, the replica state will be transformed from RBW to
    // finalized. But the space used of these replicas are all included and the
    // dfsUsed value should be same.
    assertEquals(blockLength + metaLength,
        dataNode.getFSDataset().getDfsUsed());

    fs.delete(new Path("/testReplicaCachingGetSpaceUsedByRBWReplica"), true);
  }

  @Test(timeout = 15000)
  public void testFsDatasetImplDeepCopyReplica() {
    FsDatasetSpi<?> fsDataset = dataNode.getFSDataset();
    ModifyThread modifyThread = new ModifyThread();
    modifyThread.start();
    String bpid = cluster.getNamesystem(0).getBlockPoolId();
    int retryTimes = 10;

    while (retryTimes > 0) {
      try {
        Set<? extends Replica> replicas = fsDataset.deepCopyReplica(bpid);
        if (replicas.size() > 0) {
          retryTimes--;
        }
      } catch (IOException e) {
        modifyThread.setShouldRun(false);
        Assert.fail("Encounter IOException when deep copy replica.");
      }
    }
    modifyThread.setShouldRun(false);
  }

  private class ModifyThread extends Thread {
    private boolean shouldRun = true;

    @Override
    public void run() {
      FSDataOutputStream os = null;
      while (shouldRun) {
        try {
          int id = RandomUtils.nextInt();
          os = fs.create(new Path("/testFsDatasetImplDeepCopyReplica/" + id));
          byte[] bytes = new byte[2048];
          InputStream is = new ByteArrayInputStream(bytes);
          IOUtils.copyBytes(is, os, bytes.length);
          os.hsync();
          os.close();
        } catch (IOException e) {}
      }

      try {
        fs.delete(new Path("/testFsDatasetImplDeepCopyReplica"), true);
      } catch (IOException e) {}
    }

    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
  }
}