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
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test fsck with multiple NameNodes
 */
public class TestFsckWithMultipleNameNodes {
  static final Logger LOG =
      LoggerFactory.getLogger(TestFsckWithMultipleNameNodes.class);
  {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }

  
  private static final String FILE_NAME = "/tmp.txt";
  private static final Path FILE_PATH = new Path(FILE_NAME);
  
  private static final Random RANDOM = new Random();

  static {
    TestBalancer.initTestSetup();
  }

  /** Common objects used in various methods. */
  private static class Suite {
    final MiniDFSCluster cluster;
    final ClientProtocol[] clients;
    final short replication;
    
    Suite(MiniDFSCluster cluster, final int nNameNodes, final int nDataNodes)
        throws IOException {
      this.cluster = cluster;
      clients = new ClientProtocol[nNameNodes];
      for(int i = 0; i < nNameNodes; i++) {
        clients[i] = cluster.getNameNode(i).getRpcServer();
      }
      replication = (short)Math.max(1, nDataNodes - 1);
    }

    /** create a file with a length of <code>fileLen</code> */
    private void createFile(int index, long len
        ) throws IOException, InterruptedException, TimeoutException {
      final FileSystem fs = cluster.getFileSystem(index);
      DFSTestUtil.createFile(fs, FILE_PATH, len, replication, RANDOM.nextLong());
      DFSTestUtil.waitReplication(fs, FILE_PATH, replication);
    }

  }

  private static Configuration createConf() {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    return conf;
  }

  private void runTest(final int nNameNodes, final int nDataNodes,
      Configuration conf) throws Exception {
    LOG.info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);

    LOG.info("RUN_TEST -1");
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))
        .numDataNodes(nDataNodes)
        .build();
    LOG.info("RUN_TEST 0");
    DFSTestUtil.setFederatedConfiguration(cluster, conf);

    try {
      cluster.waitActive();
      LOG.info("RUN_TEST 1");
      final Suite s = new Suite(cluster, nNameNodes, nDataNodes);
      for(int i = 0; i < nNameNodes; i++) {
        s.createFile(i, 1024);
      }

      LOG.info("RUN_TEST 2");
      final String[] urls = new String[nNameNodes];
      for(int i = 0; i < urls.length; i++) {
        urls[i] = cluster.getFileSystem(i).getUri() + FILE_NAME;
        LOG.info("urls[" + i + "]=" + urls[i]);
        final String result = TestFsck.runFsck(conf, 0, false, urls[i]);
        LOG.info("result=" + result);
        Assert.assertTrue(result.contains("Status: HEALTHY"));
      }

      // Test viewfs
      //
      LOG.info("RUN_TEST 3");
      final String[] vurls = new String[nNameNodes];
      for (int i = 0; i < vurls.length; i++) {
        String link = "/mount/nn_" + i + FILE_NAME;
        ConfigUtil.addLink(conf, link, new URI(urls[i]));
        vurls[i] = "viewfs:" + link;
      }

      for(int i = 0; i < vurls.length; i++) {
        LOG.info("vurls[" + i + "]=" + vurls[i]);
        final String result = TestFsck.runFsck(conf, 0, false, vurls[i]);
        LOG.info("result=" + result);
        Assert.assertTrue(result.contains("Status: HEALTHY"));
      }
    } finally {
      cluster.shutdown();
    }
    LOG.info("RUN_TEST 6");
  }
  
  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster
   */
  @Test
  public void testFsck() throws Exception {
    final Configuration conf = createConf();
    runTest(3, 1, conf);
  }

}

