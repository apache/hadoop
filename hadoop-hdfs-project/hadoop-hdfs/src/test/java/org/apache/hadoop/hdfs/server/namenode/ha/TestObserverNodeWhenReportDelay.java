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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getServiceState;
import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.OBSERVER_PROBE_RETRY_PERIOD_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestObserverNodeWhenReportDelay {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestObserverNodeWhenReportDelay.class.getName());

  private final static String CONTENT = "0123456789";
  private final static Path TEST_PATH = new Path("/TestObserverNode");

  private static Configuration conf;
  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void startUpCluster() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    conf.setInt(DFS_BLOCK_SIZE_KEY, 5);
    conf.setInt(DFS_BYTES_PER_CHECKSUM_KEY, 5);
    conf.setInt(DFS_REPLICATION_KEY, 1);
    conf.setInt(OBSERVER_PROBE_RETRY_PERIOD_KEY, -1);
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 2, true);
    dfsCluster = qjmhaCluster.getDfsCluster();
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    dfs = HATestUtil.configureObserverReadFs(dfsCluster, conf, ObserverReadProxyProvider.class,
        true);
  }

  @After
  public void cleanUp() throws IOException {
    dfs.delete(TEST_PATH, true);
    assertEquals("NN[0] should be active", HAServiceState.ACTIVE,
        getServiceState(dfsCluster.getNameNode(0)));
    assertEquals("NN[1] should be standby", HAServiceState.STANDBY,
        getServiceState(dfsCluster.getNameNode(1)));
    assertEquals("NN[2] should be observer", HAServiceState.OBSERVER,
        getServiceState(dfsCluster.getNameNode(2)));
  }

  @Test
  public void testReadWriteWhenReportDelay() throws Exception {
    // 1 Disable block report to observer.
    for (DataNode dn : dfsCluster.getDataNodes()) {
      Configuration conf = new Configuration(dn.getConf());
      conf.set("dfs.ha.namenodes.ns1", "nn0,nn1");
      dn.refreshNamenodes(conf);
    }

    // 2 Create File
    FSDataOutputStream out = dfs.create(TEST_PATH);
    out.write(CONTENT.getBytes());
    out.close();
    dfsCluster.rollEditLogAndTail(0);

    // 3 Read file
    //   The block report of observer is delayed, so failover to active
    byte[] buf = new byte[CONTENT.length()];
    FSDataInputStream in = dfs.open(TEST_PATH);
    in.readFully(buf);
    in.close();
    Assert.assertEquals(CONTENT, new String(buf));
    assertSentTo(0);

    // 4 List path with location
    //   the block report of observer is delayed, so failover to active.
    DirectoryListing listing = dfs.getClient().listPaths("/", new byte[0], true);
    assertSentTo(0);
    Assert.assertEquals(1, listing.getPartialListing().length);
    LocatedBlocks lbs =((HdfsLocatedFileStatus) listing.getPartialListing()[0]).getLocatedBlocks();
    List<LocatedBlock> blocks = lbs.getLocatedBlocks();
    Assert.assertEquals(2, blocks.size());
    Assert.assertTrue(ArrayUtils.isNotEmpty(blocks.get(0).getLocations()));
    Assert.assertTrue(ArrayUtils.isNotEmpty(blocks.get(1).getLocations()));

    // 5 Get located file info with location
    //   the block report of observer is delayed, so failover to active.
    HdfsLocatedFileStatus status = dfs.getClient().getLocatedFileInfo(TEST_PATH.toString(), false);
    assertSentTo(0);
    blocks = status.getLocatedBlocks().getLocatedBlocks();
    Assert.assertEquals(2, blocks.size());
    Assert.assertTrue(ArrayUtils.isNotEmpty(blocks.get(0).getLocations()));
    Assert.assertTrue(ArrayUtils.isNotEmpty(blocks.get(1).getLocations()));
  }


  private void assertSentTo(int nnIdx) throws IOException {
    assertTrue("Request was not sent to the expected namenode " + nnIdx,
        HATestUtil.isSentToAnyOfNameNodes(dfs, dfsCluster, nnIdx));
  }
}
