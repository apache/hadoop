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

import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getServiceState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.TestFsck;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test main functionality of ObserverNode.
 */
public class TestObserverNode {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestObserverNode.class.getName());

  private static Configuration conf;
  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  private final Path testPath= new Path("/TestObserverNode");

  @BeforeClass
  public static void startUpCluster() throws Exception {
    conf = new Configuration();
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 0, true);
    dfsCluster = qjmhaCluster.getDfsCluster();
  }

  @Before
  public void setUp() throws Exception {
    setObserverRead(true);
  }

  @After
  public void cleanUp() throws IOException {
    dfs.delete(testPath, true);
    assertEquals("NN[0] should be active", HAServiceState.ACTIVE,
        getServiceState(dfsCluster.getNameNode(0)));
    assertEquals("NN[1] should be standby", HAServiceState.STANDBY,
        getServiceState(dfsCluster.getNameNode(1)));
    assertEquals("NN[2] should be observer", HAServiceState.OBSERVER,
        getServiceState(dfsCluster.getNameNode(2)));
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testNoActiveToObserver() throws Exception {
    try {
      dfsCluster.transitionToObserver(0);
    } catch (ServiceFailedException e) {
      return;
    }
    fail("active cannot be transitioned to observer");
  }

  @Test
  public void testNoObserverToActive() throws Exception {
    try {
      dfsCluster.transitionToActive(2);
    } catch (ServiceFailedException e) {
      return;
    }
    fail("observer cannot be transitioned to active");
  }

  @Test
  public void testSimpleRead() throws Exception {
    Path testPath2 = new Path(testPath, "test2");

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);
  }

  @Test
  public void testFailover() throws Exception {
    Path testPath2 = new Path(testPath, "test2");
    setObserverRead(false);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive(1);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(1);
    dfs.getFileStatus(testPath);
    assertSentTo(1);

    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);
  }

  @Test
  public void testDoubleFailover() throws Exception {
    Path testPath2 = new Path(testPath, "test2");
    Path testPath3 = new Path(testPath, "test3");
    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive(1);

    dfsCluster.rollEditLogAndTail(1);
    dfs.getFileStatus(testPath2);
    assertSentTo(2);
    dfs.mkdir(testPath3, FsPermission.getDefault());
    assertSentTo(1);

    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);

    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath3);
    assertSentTo(2);
    dfs.delete(testPath3, false);
    assertSentTo(0);
  }

  @Test
  public void testObserverShutdown() throws Exception {
    dfs.mkdir(testPath, FsPermission.getDefault());
    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    // Shutdown the observer - requests should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    // Start the observer again - requests should go to observer
    dfsCluster.restartNameNode(2);
    dfsCluster.transitionToObserver(2);
    // The first request goes to the active because it has not refreshed yet;
    // the second will properly go to the observer
    dfs.getFileStatus(testPath);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
  }

  @Test
  public void testObserverFailOverAndShutdown() throws Exception {
    dfs.mkdir(testPath, FsPermission.getDefault());
    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive(1);

    // Shutdown the observer - requests should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(1);

    // Start the observer again - requests should go to observer
    dfsCluster.restartNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(1);

    dfsCluster.transitionToObserver(2);
    dfs.getFileStatus(testPath);
    // The first request goes to the active because it has not refreshed yet;
    // the second will properly go to the observer
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);
  }

  @Test
  public void testBootstrap() throws Exception {
    for (URI u : dfsCluster.getNameDirs(2)) {
      File dir = new File(u.getPath());
      assertTrue(FileUtil.fullyDelete(dir));
    }
    int rc = BootstrapStandby.run(
        new String[]{"-nonInteractive"},
        dfsCluster.getConfiguration(2)
    );
    assertEquals(0, rc);
  }

  /**
   * Test the case where Observer should throw RetriableException, just like
   * active NN, for certain open() calls where block locations are not
   * available. See HDFS-13898 for details.
   */
  @Test
  public void testObserverNodeSafeModeWithBlockLocations() throws Exception {
    // Create a new file - the request should go to active.
    dfs.create(testPath, (short)1).close();
    assertSentTo(0);

    dfsCluster.rollEditLogAndTail(0);
    dfs.open(testPath).close();
    assertSentTo(2);

    // Set observer to safe mode.
    dfsCluster.getFileSystem(2).setSafeMode(SafeModeAction.SAFEMODE_ENTER);

    // Mock block manager for observer to generate some fake blocks which
    // will trigger the (retriable) safe mode exception.
    BlockManager bmSpy =
        NameNodeAdapter.spyOnBlockManager(dfsCluster.getNameNode(2));
    doAnswer((invocation) -> {
      ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
      LocatedBlock fakeBlock = new LocatedBlock(b, DatanodeInfo.EMPTY_ARRAY);
      List<LocatedBlock> fakeBlocks = new ArrayList<>();
      fakeBlocks.add(fakeBlock);
      return new LocatedBlocks(0, false, fakeBlocks, null, true, null, null);
    }).when(bmSpy).createLocatedBlocks(any(), anyLong(), anyBoolean(),
        anyLong(), anyLong(), anyBoolean(), anyBoolean(), any(), any());

    // Open the file again - it should throw retriable exception and then
    // failover to active.
    dfs.open(testPath).close();
    assertSentTo(0);

    Mockito.reset(bmSpy);

    // Remove safe mode on observer, request should still go to it.
    dfsCluster.getFileSystem(2).setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    dfs.open(testPath).close();
    assertSentTo(2);
  }

  @Test
  public void testObserverNodeBlockMissingRetry() throws Exception {
    setObserverRead(true);

    dfs.create(testPath, (short)1).close();
    assertSentTo(0);

    dfsCluster.rollEditLogAndTail(0);

    // Mock block manager for observer to generate some fake blocks which
    // will trigger the block missing exception.

    BlockManager bmSpy = NameNodeAdapter
        .spyOnBlockManager(dfsCluster.getNameNode(2));
    doAnswer((invocation) -> {
      List<LocatedBlock> fakeBlocks = new ArrayList<>();
      // Remove the datanode info for the only block so it will throw
      // BlockMissingException and retry.
      ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
      LocatedBlock fakeBlock = new LocatedBlock(b, DatanodeInfo.EMPTY_ARRAY);
      fakeBlocks.add(fakeBlock);
      return new LocatedBlocks(0, false, fakeBlocks, null, true, null, null);
    }).when(bmSpy).createLocatedBlocks(Mockito.any(), anyLong(),
        anyBoolean(), anyLong(), anyLong(), anyBoolean(), anyBoolean(),
        Mockito.any(), Mockito.any());

    dfs.open(testPath);
    assertSentTo(0);

    Mockito.reset(bmSpy);
  }

  @Test
  public void testFsckWithObserver() throws Exception {
    setObserverRead(true);

    dfs.create(testPath, (short)1).close();
    assertSentTo(0);

    final String result = TestFsck.runFsck(conf, 0, true, "/");
    LOG.info("result=" + result);
    assertTrue(result.contains("Status: HEALTHY"));
  }

  private void assertSentTo(int nnIdx) throws IOException {
    assertTrue("Request was not sent to the expected namenode " + nnIdx,
        HATestUtil.isSentToAnyOfNameNodes(dfs, dfsCluster, nnIdx));
  }

  private static void setObserverRead(boolean flag) throws Exception {
    dfs = HATestUtil.configureObserverReadFs(
        dfsCluster, conf, ObserverReadProxyProvider.class, flag);
  }
}
