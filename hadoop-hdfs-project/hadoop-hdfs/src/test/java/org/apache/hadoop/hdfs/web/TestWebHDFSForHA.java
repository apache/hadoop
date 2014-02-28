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

package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;

/** Test whether WebHDFS can connect to an HA cluster */
public class TestWebHDFSForHA {
  private static final String LOGICAL_NAME = "minidfs";
  private static final MiniDFSNNTopology topo = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf(LOGICAL_NAME).addNN(
          new MiniDFSNNTopology.NNConf("nn1")).addNN(
          new MiniDFSNNTopology.NNConf("nn2")));

  @Test
  public void testHA() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
          .numDataNodes(0).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);

      cluster.waitActive();

      final String uri = WebHdfsFileSystem.SCHEME + "://" + LOGICAL_NAME;
      fs = FileSystem.get(URI.create(uri), conf);
      cluster.transitionToActive(0);

      final Path dir = new Path("/test");
      Assert.assertTrue(fs.mkdirs(dir));

      cluster.shutdownNameNode(0);
      cluster.transitionToActive(1);

      final Path dir2 = new Path("/test2");
      Assert.assertTrue(fs.mkdirs(dir2));
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testSecureHA() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    MiniDFSCluster cluster = null;
    WebHdfsFileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
          .numDataNodes(0).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);
      cluster.waitActive();

      final String uri = WebHdfsFileSystem.SCHEME + "://" + LOGICAL_NAME;
      fs = (WebHdfsFileSystem) FileSystem.get(URI.create(uri), conf);

      cluster.transitionToActive(0);
      Token<?> token = fs.getDelegationToken(null);

      cluster.shutdownNameNode(0);
      cluster.transitionToActive(1);

      fs.renewDelegationToken(token);
      fs.cancelDelegationToken(token);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}