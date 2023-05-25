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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter.getServiceState;
import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.namenode.TestFsck;
import org.apache.hadoop.hdfs.tools.GetGroups;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
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
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    // Set observer probe retry period to 0. Required by the tests that restart
    // Observer and immediately try to read from it.
    conf.setTimeDuration(
        OBSERVER_PROBE_RETRY_PERIOD_KEY, 0, TimeUnit.MILLISECONDS);
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 1, true);
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
  public void testObserverRequeue() throws Exception {
    ScheduledExecutorService interruptor =
        Executors.newScheduledThreadPool(1);

    FSNamesystem observerFsNS = dfsCluster.getNamesystem(2);
    RpcMetrics obRpcMetrics = ((NameNodeRpcServer)dfsCluster
        .getNameNodeRpc(2)).getClientRpcServer().getRpcMetrics();
    try {
      // Stop EditlogTailer of Observer NameNode.
      observerFsNS.getEditLogTailer().stop();
      long oldRequeueNum = obRpcMetrics.getRpcRequeueCalls();
      ScheduledFuture<FileStatus> scheduledFuture = interruptor.schedule(
          () -> {
            Path tmpTestPath = new Path("/TestObserverRequeue");
            dfs.create(tmpTestPath, (short)1).close();
            assertSentTo(0);
            // This operation will be blocked in ObserverNameNode
            // until EditlogTailer tailed edits from journalNode.
            FileStatus fileStatus = dfs.getFileStatus(tmpTestPath);
            assertSentTo(2);
            return fileStatus;
          }, 0, TimeUnit.SECONDS);

      GenericTestUtils.waitFor(() -> obRpcMetrics.getRpcRequeueCalls() > oldRequeueNum,
          50, 10000);

      observerFsNS.getEditLogTailer().doTailEdits();
      FileStatus fileStatus = scheduledFuture.get(10000, TimeUnit.MILLISECONDS);
      assertNotNull(fileStatus);
    } finally {
      EditLogTailer editLogTailer = new EditLogTailer(observerFsNS, conf);
      observerFsNS.setEditLogTailerForTests(editLogTailer);
      editLogTailer.start();
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

  /**
   * Test that non-ClientProtocol proxies such as
   * {@link org.apache.hadoop.tools.GetUserMappingsProtocol} still work
   * when run in an environment with observers.
   */
  @Test
  public void testGetGroups() throws Exception {
    GetGroups getGroups = new GetGroups(conf);
    assertEquals(0, getGroups.run(new String[0]));
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
    dfsCluster.getFileSystem(2).setSafeMode(SafeModeAction.ENTER);

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
    dfsCluster.getFileSystem(2).setSafeMode(SafeModeAction.LEAVE);
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

    dfs.getClient().listPaths("/", new byte[0], true);
    assertSentTo(0);

    dfs.getClient().getLocatedFileInfo(testPath.toString(), false);
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

  /**
   * Test that, if a write happens happens to go to Observer,
   * Observer would throw {@link ObserverRetryOnActiveException},
   * to inform client to retry on Active
   *
   * @throws Exception
   */
  @Test
  public void testObserverRetryActiveException() throws Exception {
    boolean thrownRetryException = false;
    try {
      // Force a write on Observer, which should throw
      // retry on active exception.
      dfsCluster.getNameNode(2)
          .getRpcServer()
          .mkdirs("/testActiveRetryException",
              FsPermission.createImmutable((short) 0755), true);
    } catch (ObserverRetryOnActiveException orae) {
      thrownRetryException = true;
    }
    assertTrue(thrownRetryException);
  }

  /**
   * Test that for open call, if access time update is required,
   * the open call should go to active, instead of observer.
   *
   * @throws Exception
   */
  @Test
  public void testAccessTimeUpdateRedirectToActive() throws Exception {
    // Create a new pat to not mess up other tests
    Path tmpTestPath = new Path("/TestObserverNodeAccessTime");
    dfs.create(tmpTestPath, (short)1).close();
    assertSentTo(0);
    dfs.open(tmpTestPath).close();
    assertSentTo(2);
    // Set access time to a time in the far past.
    // So that next read call is guaranteed to
    // have passed access time period.
    dfs.setTimes(tmpTestPath, 0, 0);
    // Verify that aTime update redirects on Active
    dfs.open(tmpTestPath).close();
    assertSentTo(0);
  }


  /**
   * Test that if client connects to Active it does not try to find Observer
   * on next calls during some period of time.
   */
  @Test
  public void testStickyActive() throws Exception {
    Path testFile = new Path(testPath, "testStickyActive");
    Configuration newConf = new Configuration(conf);
    // Observer probe retry period set to 5 sec
    newConf.setLong(OBSERVER_PROBE_RETRY_PERIOD_KEY, 5000);
    // Disable cache, so that a new client actually gets created with new conf.
    newConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    DistributedFileSystem newFs = (DistributedFileSystem) FileSystem.get(newConf);
    newFs.create(testFile, (short)1).close();
    assertSentTo(0);
    dfsCluster.rollEditLogAndTail(0);
    // No Observers present, should still go to Active
    dfsCluster.transitionToStandby(2);
    assertEquals("NN[2] should be standby", HAServiceState.STANDBY,
        getServiceState(dfsCluster.getNameNode(2)));
    newFs.open(testFile).close();
    assertSentTo(0);
    // Restore Observer
    int newObserver = 1;
    dfsCluster.transitionToObserver(newObserver);
    assertEquals("NN[" + newObserver + "] should be observer",
        HAServiceState.OBSERVER,
        getServiceState(dfsCluster.getNameNode(newObserver)));
    long startTime = Time.monotonicNow();
    try {
      while(Time.monotonicNow() - startTime <= 5000) {
        newFs.open(testFile).close();
        // Client should still talk to Active
        assertSentTo(0);
        Thread.sleep(200);
      }
    } catch(AssertionError ae) {
      if(Time.monotonicNow() - startTime <= 5000) {
        throw ae;
      }
      assertSentTo(newObserver);
    } finally {
      dfsCluster.transitionToStandby(1);
      dfsCluster.transitionToObserver(2);
    }
  }

  @Test
  public void testFsckDelete() throws Exception {
    setObserverRead(true);
    DFSTestUtil.createFile(dfs, testPath, 512, (short) 1, 0);
    DFSTestUtil.waitForReplication(dfs, testPath, (short) 1, 5000);
    ExtendedBlock block = DFSTestUtil.getFirstBlock(dfs, testPath);
    int dnToCorrupt = DFSTestUtil.firstDnWithBlock(dfsCluster, block);
    FSNamesystem ns = dfsCluster.getNameNode(0).getNamesystem();
    // corrupt Replicas are detected on restarting datanode
    dfsCluster.corruptReplica(dnToCorrupt, block);
    dfsCluster.restartDataNode(dnToCorrupt);
    DFSTestUtil.waitCorruptReplicas(dfs, ns, testPath, block, 1);
    final String result = TestFsck.runFsck(conf, 1, true, "/", "-delete");
    // filesystem should be in corrupt state
    LOG.info("result=" + result);
    assertTrue(result.contains("The filesystem under path '/' is CORRUPT"));
  }

  /**
   * The test models the race of two mkdirs RPC calls on the same path to
   * Active NameNode. The first arrived call will journal a mkdirs transaction.
   * The subsequent call hitting the NameNode before the mkdirs transaction is
   * synced will see that the directory already exists, but will obtain
   * lastSeenStateId smaller than the txId of the mkdirs transaction
   * since the latter hasn't been synced yet.
   * This causes stale read from Observer for the second client.
   * See HDFS-15915.
   */
  @Test
  public void testMkdirsRaceWithObserverRead() throws Exception {
    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);
    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    // Create a spy on FSEditLog, which delays MkdirOp transaction by 100 mec
    FSEditLog spyEditLog = NameNodeAdapter.spyDelayMkDirTransaction(
        dfsCluster.getNameNode(0), 100);

    final int numThreads = 4;
    ClientState[] clientStates = new ClientState[numThreads];
    final ExecutorService threadPool =
        HadoopExecutors.newFixedThreadPool(numThreads);
    final Future<?>[] futures = new Future<?>[numThreads];

    Configuration conf2 = new Configuration(conf);
    // Disable FS cache so that different DFS clients are used
    conf2.setBoolean("fs.hdfs.impl.disable.cache", true);

    for (int i = 0; i < numThreads; i++) {
      clientStates[i] = new ClientState();
      futures[i] = threadPool.submit(new MkDirRunner(conf2, clientStates[i]));
    }

    Thread.sleep(150); // wait until mkdir is logged
    long activStateId =
        dfsCluster.getNameNode(0).getFSImage().getLastAppliedOrWrittenTxId();
    dfsCluster.rollEditLogAndTail(0);
    boolean finished = true;
    // wait for all dispatcher threads to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        finished = false;
        LOG.warn("MkDirRunner thread failed", e.getCause());
      }
    }
    assertTrue("Not all threads finished", finished);
    threadPool.shutdown();

    assertEquals("Active and Observer stateIds don't match",
        dfsCluster.getNameNode(0).getFSImage().getLastAppliedOrWrittenTxId(),
        dfsCluster.getNameNode(2).getFSImage().getLastAppliedOrWrittenTxId());
    for (int i = 0; i < numThreads; i++) {
      assertTrue("Client #" + i
          + " lastSeenStateId=" + clientStates[i].lastSeenStateId
          + " activStateId=" + activStateId
          + "\n" + clientStates[i].fnfe,
          clientStates[i].lastSeenStateId >= activStateId &&
          clientStates[i].fnfe == null);
    }

    // Restore edit log
    Mockito.reset(spyEditLog);
  }

  static class ClientState {
    private long lastSeenStateId = -7;
    private FileNotFoundException fnfe;
  }

  static class MkDirRunner implements Runnable {
    private static final Path DIR_PATH =
        new Path("/TestObserverNode/testMkdirsRaceWithObserverRead");

    private DistributedFileSystem fs;
    private ClientState clientState;

    MkDirRunner(Configuration conf, ClientState cs) throws IOException {
      super();
      fs = (DistributedFileSystem) FileSystem.get(conf);
      clientState = cs;
    }

    @Override
    public void run() {
      try {
        fs.mkdirs(DIR_PATH);
        clientState.lastSeenStateId = HATestUtil.getLastSeenStateId(fs);
        assertSentTo(fs, 0);

        FileStatus stat = fs.getFileStatus(DIR_PATH);
        assertSentTo(fs, 2);
        assertTrue("Should be a directory", stat.isDirectory());
      } catch (FileNotFoundException ioe) {
        clientState.fnfe = ioe;
      } catch (Exception e) {
        fail("Unexpected exception: " + e);
      }
    }
  }

  @Test
  public void testGetListingForDeletedDir() throws Exception {
    Path path = new Path("/dir1/dir2/testFile");
    dfs.create(path).close();

    assertTrue(dfs.delete(new Path("/dir1/dir2"), true));

    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> dfs.listLocatedStatus(new Path("/dir1/dir2")));
  }

  @Test
  public void testSimpleReadEmptyDirOrFile() throws IOException {
    // read empty dir
    dfs.mkdirs(new Path("/emptyDir"));
    assertSentTo(0);

    dfs.getClient().listPaths("/", new byte[0], true);
    assertSentTo(2);

    dfs.getClient().getLocatedFileInfo("/emptyDir", true);
    assertSentTo(2);

    // read empty file
    dfs.create(new Path("/emptyFile"), (short)1);
    assertSentTo(0);

    dfs.getClient().getLocatedFileInfo("/emptyFile", true);
    assertSentTo(2);

    dfs.getClient().getBlockLocations("/emptyFile", 0, 1);
    assertSentTo(2);
  }

  private static void assertSentTo(DistributedFileSystem fs, int nnIdx)
      throws IOException {
    assertTrue("Request was not sent to the expected namenode " + nnIdx,
        HATestUtil.isSentToAnyOfNameNodes(fs, dfsCluster, nnIdx));
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
