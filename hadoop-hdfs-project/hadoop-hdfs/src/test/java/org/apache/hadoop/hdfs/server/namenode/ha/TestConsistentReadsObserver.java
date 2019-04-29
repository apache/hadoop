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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.ipc.RpcScheduler;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test consistency of reads while accessing an ObserverNode.
 * The tests are based on traditional (non fast path) edits tailing.
 */
public class TestConsistentReadsObserver {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestConsistentReadsObserver.class.getName());

  private static Configuration conf;
  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private DistributedFileSystem dfs;

  private final Path testPath= new Path("/TestConsistentReadsObserver");

  @BeforeClass
  public static void startUpCluster() throws Exception {
    conf = new Configuration();
    // disable fast tailing here because this test's assertions are based on the
    // timing of explicitly called rollEditLogAndTail. Although this means this
    // test takes some time to run
    // TODO: revisit if there is a better way.
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 0, false);
    dfsCluster = qjmhaCluster.getDfsCluster();
  }

  @Before
  public void setUp() throws Exception {
    dfs = setObserverRead(true);
  }

  @After
  public void cleanUp() throws IOException {
    dfs.delete(testPath, true);
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testRequeueCall() throws Exception {
    setObserverRead(true);

    // Update the configuration just for the observer, by enabling
    // IPC backoff and using the test scheduler class, which starts to backoff
    // after certain number of calls.
    final int observerIdx = 2;
    NameNode nn = dfsCluster.getNameNode(observerIdx);
    int port = nn.getNameNodeAddress().getPort();
    Configuration configuration = dfsCluster.getConfiguration(observerIdx);
    String prefix = CommonConfigurationKeys.IPC_NAMESPACE + "." + port + ".";
    configuration.set(prefix + CommonConfigurationKeys.IPC_SCHEDULER_IMPL_KEY,
        TestRpcScheduler.class.getName());
    configuration.setBoolean(prefix
        + CommonConfigurationKeys.IPC_BACKOFF_ENABLE, true);

    NameNodeAdapter.getRpcServer(nn).refreshCallQueue(configuration);

    dfs.create(testPath, (short)1).close();
    assertSentTo(0);

    // Since we haven't tailed edit logs on the observer, it will fall behind
    // and keep re-queueing the incoming request. Eventually, RPC backoff will
    // be triggered and client should retry active NN.
    dfs.getFileStatus(testPath);
    assertSentTo(0);
  }

  @Test
  public void testMsyncSimple() throws Exception {
    // 0 == not completed, 1 == succeeded, -1 == failed
    AtomicInteger readStatus = new AtomicInteger(0);

    // Making an uncoordinated call, which initialize the proxy
    // to Observer node.
    dfs.getClient().getHAServiceState();
    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    Thread reader = new Thread(() -> {
      try {
        // this read will block until roll and tail edits happen.
        dfs.getFileStatus(testPath);
        readStatus.set(1);
      } catch (IOException e) {
        e.printStackTrace();
        readStatus.set(-1);
      }
    });

    reader.start();
    // the reader is still blocking, not succeeded yet.
    assertEquals(0, readStatus.get());
    dfsCluster.rollEditLogAndTail(0);
    // wait a while for all the change to be done
    GenericTestUtils.waitFor(() -> readStatus.get() != 0, 100, 10000);
    // the reader should have succeed.
    assertEquals(1, readStatus.get());
  }

  private void testMsync(boolean autoMsync, long autoMsyncPeriodMs)
      throws Exception {
    // 0 == not completed, 1 == succeeded, -1 == failed
    AtomicInteger readStatus = new AtomicInteger(0);
    Configuration conf2 = new Configuration(conf);

    // Disable FS cache so two different DFS clients will be used.
    conf2.setBoolean("fs.hdfs.impl.disable.cache", true);
    if (autoMsync) {
      conf2.setTimeDuration(
          ObserverReadProxyProvider.AUTO_MSYNC_PERIOD_KEY_PREFIX
              + "." + dfs.getUri().getHost(),
          autoMsyncPeriodMs, TimeUnit.MILLISECONDS);
    }
    DistributedFileSystem dfs2 = (DistributedFileSystem) FileSystem.get(conf2);

    // Initialize the proxies for Observer Node.
    dfs.getClient().getHAServiceState();
    // This initialization will perform the msync-on-startup, so that another
    // form of msync is required later
    dfs2.getClient().getHAServiceState();

    // Advance Observer's state ID so it is ahead of client's.
    dfs.mkdir(new Path("/test"), FsPermission.getDefault());
    dfsCluster.rollEditLogAndTail(0);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    Thread reader = new Thread(() -> {
      try {
        // After msync, client should have the latest state ID from active.
        // Therefore, the subsequent getFileStatus call should succeed.
        if (!autoMsync) {
          // If not testing auto-msync, perform an explicit one here
          dfs2.getClient().msync();
        } else if (autoMsyncPeriodMs > 0) {
          Thread.sleep(autoMsyncPeriodMs);
        }
        dfs2.getFileStatus(testPath);
        if (HATestUtil.isSentToAnyOfNameNodes(dfs2, dfsCluster, 2)) {
          readStatus.set(1);
        } else {
          readStatus.set(-1);
        }
      } catch (Exception e) {
        e.printStackTrace();
        readStatus.set(-1);
      }
    });

    reader.start();

    Thread.sleep(100);
    assertEquals(0, readStatus.get());

    dfsCluster.rollEditLogAndTail(0);

    GenericTestUtils.waitFor(() -> readStatus.get() != 0, 100, 3000);
    assertEquals(1, readStatus.get());
  }

  @Test
  public void testExplicitMsync() throws Exception {
    testMsync(false, -1);
  }

  @Test
  public void testAutoMsyncPeriod0() throws Exception {
    testMsync(true, 0);
  }

  @Test
  public void testAutoMsyncPeriod5() throws Exception {
    testMsync(true, 5);
  }

  @Test(expected = TimeoutException.class)
  public void testAutoMsyncLongPeriod() throws Exception {
    // This should fail since the auto-msync is never activated
    testMsync(true, Long.MAX_VALUE);
  }

  // A new client should first contact the active, before using an observer,
  // to ensure that it is up-to-date with the current state
  @Test
  public void testCallFromNewClient() throws Exception {
    // Set the order of nodes: Observer, Standby, Active
    // This is to ensure that test doesn't pass trivially because the active is
    // the first node contacted
    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToObserver(0);
    dfsCluster.transitionToStandby(2);
    dfsCluster.transitionToActive(2);
    try {
      // 0 == not completed, 1 == succeeded, -1 == failed
      AtomicInteger readStatus = new AtomicInteger(0);

      // Initialize the proxies for Observer Node.
      dfs.getClient().getHAServiceState();

      // Advance Observer's state ID so it is ahead of client's.
      dfs.mkdir(new Path("/test"), FsPermission.getDefault());
      dfsCluster.getNameNode(2).getRpcServer().rollEditLog();
      dfsCluster.getNameNode(0)
          .getNamesystem().getEditLogTailer().doTailEdits();

      dfs.mkdir(testPath, FsPermission.getDefault());
      assertSentTo(2);

      Configuration conf2 = new Configuration(conf);

      // Disable FS cache so two different DFS clients will be used.
      conf2.setBoolean("fs.hdfs.impl.disable.cache", true);
      DistributedFileSystem dfs2 =
          (DistributedFileSystem) FileSystem.get(conf2);
      dfs2.getClient().getHAServiceState();

      Thread reader = new Thread(() -> {
        try {
          dfs2.getFileStatus(testPath);
          readStatus.set(1);
        } catch (Exception e) {
          e.printStackTrace();
          readStatus.set(-1);
        }
      });

      reader.start();

      Thread.sleep(100);
      assertEquals(0, readStatus.get());

      dfsCluster.getNameNode(2).getRpcServer().rollEditLog();
      dfsCluster.getNameNode(0)
          .getNamesystem().getEditLogTailer().doTailEdits();

      GenericTestUtils.waitFor(() -> readStatus.get() != 0, 100, 10000);
      assertEquals(1, readStatus.get());
    } finally {
      // Put the cluster back the way it was when the test started
      dfsCluster.transitionToStandby(2);
      dfsCluster.transitionToObserver(2);
      dfsCluster.transitionToStandby(0);
      dfsCluster.transitionToActive(0);
    }
  }

  @Test
  public void testUncoordinatedCall() throws Exception {
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

    long before = Time.now();
    dfs.getClient().datanodeReport(HdfsConstants.DatanodeReportType.ALL);
    long after = Time.now();

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

  private void assertSentTo(int nnIdx) throws IOException {
    assertTrue("Request was not sent to the expected namenode " + nnIdx,
        HATestUtil.isSentToAnyOfNameNodes(dfs, dfsCluster, nnIdx));
  }

  private DistributedFileSystem setObserverRead(boolean flag) throws Exception {
    return HATestUtil.configureObserverReadFs(
        dfsCluster, conf, ObserverReadProxyProvider.class, flag);
  }

  /**
   * A dummy test scheduler that starts backoff after a fixed number
   * of requests.
   */
  public static class TestRpcScheduler implements RpcScheduler {
    // Allow a number of RPCs to pass in order for the NN restart to succeed.
    private int allowed = 10;
    public TestRpcScheduler() {}

    @Override
    public int getPriorityLevel(Schedulable obj) {
      return 0;
    }

    @Override
    public boolean shouldBackOff(Schedulable obj) {
      return --allowed < 0;
    }

    @Override
    public void stop() {
    }
  }
}
