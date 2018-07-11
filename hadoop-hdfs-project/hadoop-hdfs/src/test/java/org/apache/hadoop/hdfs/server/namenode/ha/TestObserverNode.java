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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;

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
    setUpCluster(1);

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
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    try {
      dfs.getFileStatus(testPath);
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Pass
    }

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);
  }

  @Test
  public void testFailover() throws Exception {
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
    dfs.getFileStatus(testPath);
    assertSentTo(2);
  }

  @Test
  public void testObserverFailOverAndShutdown() throws Exception {
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
