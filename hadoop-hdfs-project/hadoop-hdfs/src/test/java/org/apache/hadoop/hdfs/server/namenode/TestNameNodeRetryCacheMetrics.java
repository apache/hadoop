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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.ipc.metrics.RetryCacheMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;

/**
 * Tests for ensuring the namenode retry cache metrics works correctly for
 * non-idempotent requests.
 *
 * Retry cache works based on tracking previously received request based on the
 * ClientId and CallId received in RPC requests and storing the response. The
 * response is replayed on retry when the same request is received again.
 *
 */
public class TestNameNodeRetryCacheMetrics {
  private MiniDFSCluster cluster;
  private FSNamesystem namesystem;
  private DistributedFileSystem filesystem;
  private final int namenodeId = 0;
  private Configuration conf;
  private RetryCacheMetrics metrics;

  /** Start a cluster */
  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Failover.RANDOM_ORDER, false);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY, 2);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(3)
        .build();
    cluster.waitActive();
    cluster.transitionToActive(namenodeId);
    HATestUtil.setFailoverConfigurations(cluster, conf);
    filesystem = (DistributedFileSystem) HATestUtil.configureFailoverFs(cluster, conf);
    namesystem = cluster.getNamesystem(namenodeId);
    metrics = namesystem.getRetryCache().getMetricsForTests();
  }

  /**
   * Cleanup after the test
   * @throws IOException
   **/
  @After
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testRetryCacheMetrics() throws IOException {
    checkMetrics(0, 0, 0);

    // DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY is 2 ,
    // so 2 requests are dropped at first.
    // After that, 1 request will reach NameNode correctly.
    trySaveNamespace();
    checkMetrics(2, 0, 1);

    // RetryCache will be cleared after Namesystem#close()
    namesystem.close();
    checkMetrics(2, 1, 1);
  }

  private void checkMetrics(long hit, long cleared, long updated) {
    assertEquals("CacheHit", hit, metrics.getCacheHit());
    assertEquals("CacheCleared", cleared, metrics.getCacheCleared());
    assertEquals("CacheUpdated", updated, metrics.getCacheUpdated());
  }

  private void trySaveNamespace() throws IOException {
    filesystem.setSafeMode(SafeModeAction.ENTER);
    filesystem.saveNamespace();
    filesystem.setSafeMode(SafeModeAction.LEAVE);
  }

}
