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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests multiple ObserverNodes.
 */
public class TestMultiObserverNode {
  private static Configuration conf;
  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  private final Path testPath= new Path("/TestMultiObserverNode");

  @BeforeClass
  public static void startUpCluster() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 2, 0, true);
    dfsCluster = qjmhaCluster.getDfsCluster();
    dfs = HATestUtil.configureObserverReadFs(
        dfsCluster, conf, ObserverReadProxyProvider.class, true);
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
  public void testObserverFailover() throws Exception {
    dfs.mkdir(testPath, FsPermission.getDefault());
    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2, 3);

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
    assertSentTo(2, 3);
  }

  @Test
  public void testMultiObserver() throws Exception {
    Path testPath2 = new Path(testPath, "test2");
    Path testPath3 = new Path(testPath, "test3");
    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    dfsCluster.rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2, 3);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    dfsCluster.rollEditLogAndTail(0);

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
    assertSentTo(2, 3);

    dfs.mkdir(testPath3, FsPermission.getDefault());
    dfsCluster.rollEditLogAndTail(0);

    // Now shutdown the second observer, request should go to the first one
    dfsCluster.shutdownNameNode(3);
    dfs.listStatus(testPath3);
    assertSentTo(2);

    // Shutdown both, request should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.listStatus(testPath3);
    assertSentTo(0);

    dfsCluster.restartNameNode(2);
    dfsCluster.transitionToObserver(2);
    dfsCluster.restartNameNode(3);
    dfsCluster.transitionToObserver(3);
  }

  @Test
  public void testObserverFallBehind() throws Exception {
    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    // Set large state Id on the client
    long realStateId = HATestUtil.setACStateId(dfs, 500000);
    dfs.getFileStatus(testPath);
    // Should end up on ANN
    assertSentTo(0);
    HATestUtil.setACStateId(dfs, realStateId);
  }

  private void assertSentTo(int... nnIndices) throws IOException {
    assertTrue("Request was not sent to any of the expected namenodes.",
        HATestUtil.isSentToAnyOfNameNodes(dfs, dfsCluster, nnIndices));
  }
}
