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

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


// Main unit tests for ObserverNode
public class TestObserverNode {
  private Configuration conf;
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster dfsCluster;
  private NameNode[] namenodes;
  private Path testPath;
  private Path testPath2;
  private Path testPath3;

  /** These are set in each individual test case */
  private DistributedFileSystem dfs;
  private ObserverReadProxyProvider<?> provider;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setTimeDuration(
        DFS_HA_TAILEDITS_PERIOD_KEY, 100, TimeUnit.MILLISECONDS);

    testPath = new Path("/test");
    testPath2 = new Path("/test2");
    testPath3 = new Path("/test3");
  }

  @After
  public void cleanUp() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testSimpleRead() throws Exception {
    setUpCluster(1);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);
  }

  @Test
  public void testFailover() throws Exception {
    setUpCluster(1);
    setObserverRead(false);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive();

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(1);
    dfs.getFileStatus(testPath);
    assertSentTo(1);
  }

  @Test
  public void testDoubleFailover() throws Exception {
    setUpCluster(1);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive(1);

    rollEditLogAndTail(1);
    dfs.getFileStatus(testPath2);
    assertSentTo(2);
    dfs.mkdir(testPath3, FsPermission.getDefault());
    assertSentTo(1);

    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath3);
    assertSentTo(2);
    dfs.delete(testPath3, false);
    assertSentTo(0);
  }

  @Test
  public void testObserverFailover() throws Exception {
    setUpCluster(2);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentToAny(2, 3);

    // Transition observer #2 to standby, request should go to the #3.
    dfsCluster.transitionToStandby(2);
    dfs.getFileStatus(testPath);
    assertSentTo(3);

    // Transition observer #3 to standby, request should go to active
    dfsCluster.transitionToStandby(3);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    // Transition #2 back to observer, request should go to #2
    dfsCluster.transitionToObserver(2);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    // Transition #3 back to observer, request should go to either #2 or #3
    dfsCluster.transitionToObserver(3);
    dfs.getFileStatus(testPath);
    assertSentToAny(2, 3);
  }

  @Test
  public void testObserverShutdown() throws Exception {
    setUpCluster(1);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    rollEditLogAndTail(0);
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
    setUpCluster(1);
    // Test the case when there is a failover before ONN shutdown
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive();

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
  }

  @Test
  public void testMultiObserver() throws Exception {
    setUpCluster(2);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentToAny(2, 3);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    rollEditLogAndTail(0);

    // Shutdown first observer, request should go to the second one
    dfsCluster.shutdownNameNode(2);
    dfs.listStatus(testPath2);
    assertSentTo(3);

    // Restart the first observer
    dfsCluster.restartNameNode(2);
    dfs.listStatus(testPath);
    assertSentTo(3);

    dfsCluster.transitionToObserver(2);
    dfs.listStatus(testPath);
    assertSentToAny(2, 3);

    dfs.mkdir(testPath3, FsPermission.getDefault());
    rollEditLogAndTail(0);

    // Now shutdown the second observer, request should go to the first one
    dfsCluster.shutdownNameNode(3);
    dfs.listStatus(testPath3);
    assertSentTo(2);

    // Shutdown both, request should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.listStatus(testPath3);
    assertSentTo(0);
  }

  @Test
  public void testBootstrap() throws Exception {
    setUpCluster(1);
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

  // TODO this does not currently work because fetching the service state from
  // e.g. the StandbyNameNode also waits for the transaction ID to catch up.
  // This is disabled pending HDFS-13872 and HDFS-13749.
  @Ignore("Disabled until HDFS-13872 and HDFS-13749 are committed")
  @Test
  public void testMsyncSimple() throws Exception {
    // disable fast path here because this test's assertions are based on the
    // timing of explicitly called rollEditLogAndTail. Although this means this
    // test takes some time to run
    // TODO: revisit if there is a better way.
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_KEY, false);
    conf.setTimeDuration(DFS_HA_LOGROLL_PERIOD_KEY, 60, TimeUnit.SECONDS);
    conf.setTimeDuration(
        DFS_HA_TAILEDITS_PERIOD_KEY, 30, TimeUnit.SECONDS);
    setUpCluster(1);
    setObserverRead(true);

    // 0 == not completed, 1 == succeeded, -1 == failed
    final AtomicInteger readStatus = new AtomicInteger(0);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    Thread reader = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // this read will block until roll and tail edits happen.
          dfs.getFileStatus(testPath);
          readStatus.set(1);
        } catch (IOException e) {
          e.printStackTrace();
          readStatus.set(-1);
        }
      }
    });

    reader.start();
    // the reader is still blocking, not succeeded yet.
    assertEquals(0, readStatus.get());
    rollEditLogAndTail(0);
    // wait a while for all the change to be done
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return readStatus.get() != 0;
      }
    }, 100, 10000);
    // the reader should have succeed.
    assertEquals(1, readStatus.get());
  }

  @Test
  public void testUncoordinatedCall() throws Exception {
    // disable fast tailing so that coordination takes time.
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_KEY, false);
    conf.setTimeDuration(DFS_HA_LOGROLL_PERIOD_KEY, 300, TimeUnit.SECONDS);
    conf.setTimeDuration(
        DFS_HA_TAILEDITS_PERIOD_KEY, 200, TimeUnit.SECONDS);
    setUpCluster(1);
    setObserverRead(true);

    // make a write call so that client will be ahead of
    // observer for now.
    dfs.mkdir(testPath, FsPermission.getDefault());

    // a status flag, initialized to 0, after reader finished, this will be
    // updated to 1, -1 on error
    AtomicInteger readStatus = new AtomicInteger(0);

    // create a separate thread to make a blocking read.
    Thread reader = new Thread(() -> {
      try {
        // this read call will block until server state catches up. But due to
        // configuration, this will take a very long time.
        dfs.getClient().getFileInfo("/");
        readStatus.set(1);
        fail("Should have been interrupted before getting here.");
      } catch (IOException e) {
        e.printStackTrace();
        readStatus.set(-1);
      }
    });
    reader.start();

    long before = System.currentTimeMillis();
    dfs.getClient().datanodeReport(HdfsConstants.DatanodeReportType.ALL);
    long after = System.currentTimeMillis();

    // should succeed immediately, because datanodeReport is marked an
    // uncoordinated call, and will not be waiting for server to catch up.
    assertTrue(after - before < 200);
    // by this time, reader thread should still be blocking, so the status not
    // updated
    assertEquals(0, readStatus.get());
    Thread.sleep(5000);
    // reader thread status should still be unchanged after 5 sec...
    assertEquals(0, readStatus.get());
    // and the reader thread is not dead, so it must be still waiting
    assertEquals(Thread.State.WAITING, reader.getState());
    reader.interrupt();
  }

  private void setUpCluster(int numObservers) throws Exception {
    qjmhaCluster = new MiniQJMHACluster.Builder(conf)
        .setNumNameNodes(2 + numObservers)
        .build();
    dfsCluster = qjmhaCluster.getDfsCluster();

    namenodes = new NameNode[2 + numObservers];
    for (int i = 0; i < namenodes.length; i++) {
      namenodes[i] = dfsCluster.getNameNode(i);
    }

    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);

    for (int i = 0; i < numObservers; i++) {
      dfsCluster.transitionToObserver(2 + i);
    }
  }

  private void assertSentTo(int nnIdx) {
    assertSentToAny(nnIdx);
  }

  private void assertSentToAny(int... nnIndices) {
    FailoverProxyProvider.ProxyInfo<?> pi = provider.getLastProxy();
    for (int nnIdx : nnIndices) {
      if (pi.proxyInfo.equals(
          dfsCluster.getNameNode(nnIdx).getNameNodeAddress().toString())) {
        return;
      }
    }
    fail("Request was not sent to any of the expected namenodes");
  }

  private void setObserverRead(boolean flag) throws Exception {
    dfs = HATestUtil.configureObserverReadFs(dfsCluster, conf, 0);
    RetryInvocationHandler<?> handler =
        (RetryInvocationHandler<?>) Proxy.getInvocationHandler(
            dfs.getClient().getNamenode());
    provider = (ObserverReadProxyProvider<?>) handler.getProxyProvider();
    provider.setObserverReadEnabled(flag);
  }

  private void rollEditLogAndTail(int indexForActiveNN) throws Exception {
    dfsCluster.getNameNode(indexForActiveNN).getRpcServer().rollEditLog();
    for (int i = 2; i < namenodes.length; i++) {
      dfsCluster.getNameNode(i).getNamesystem().getEditLogTailer()
          .doTailEdits();
    }
  }
}
