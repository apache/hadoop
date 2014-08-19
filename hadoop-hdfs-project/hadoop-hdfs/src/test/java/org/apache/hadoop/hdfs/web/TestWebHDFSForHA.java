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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestWebHDFSForHA {
  private static final String LOGICAL_NAME = "minidfs";
  private static final URI WEBHDFS_URI = URI.create(WebHdfsFileSystem.SCHEME +
          "://" + LOGICAL_NAME);
  private static final MiniDFSNNTopology topo = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf(LOGICAL_NAME).addNN(
          new MiniDFSNNTopology.NNConf("nn1")).addNN(
          new MiniDFSNNTopology.NNConf("nn2")));

  @Test
  public void testHA() throws IOException {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
          .numDataNodes(0).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);

      cluster.waitActive();

      fs = FileSystem.get(WEBHDFS_URI, conf);
      cluster.transitionToActive(0);

      final Path dir = new Path("/test");
      Assert.assertTrue(fs.mkdirs(dir));

      cluster.shutdownNameNode(0);
      cluster.transitionToActive(1);

      final Path dir2 = new Path("/test2");
      Assert.assertTrue(fs.mkdirs(dir2));
    } finally {
      IOUtils.cleanup(null, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testSecureHAToken() throws IOException, InterruptedException {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    conf.setBoolean(DFSConfigKeys
            .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

    MiniDFSCluster cluster = null;
    WebHdfsFileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
          .numDataNodes(0).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);
      cluster.waitActive();

      fs = spy((WebHdfsFileSystem) FileSystem.get(WEBHDFS_URI, conf));
      FileSystemTestHelper.addFileSystemForTesting(WEBHDFS_URI, conf, fs);

      cluster.transitionToActive(0);
      Token<?> token = fs.getDelegationToken(null);

      cluster.shutdownNameNode(0);
      cluster.transitionToActive(1);
      token.renew(conf);
      token.cancel(conf);
      verify(fs).renewDelegationToken(token);
      verify(fs).cancelDelegationToken(token);
    } finally {
      IOUtils.cleanup(null, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFailoverAfterOpen() throws IOException {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    conf.set(FS_DEFAULT_NAME_KEY, HdfsConstants.HDFS_URI_SCHEME +
        "://" + LOGICAL_NAME);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    final Path p = new Path("/test");
    final byte[] data = "Hello".getBytes();

    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
              .numDataNodes(1).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);

      cluster.waitActive();

      fs = FileSystem.get(WEBHDFS_URI, conf);
      cluster.transitionToActive(1);

      FSDataOutputStream out = fs.create(p);
      cluster.shutdownNameNode(1);
      cluster.transitionToActive(0);

      out.write(data);
      out.close();
      FSDataInputStream in = fs.open(p);
      byte[] buf = new byte[data.length];
      IOUtils.readFully(in, buf, 0, buf.length);
      Assert.assertArrayEquals(data, buf);
    } finally {
      IOUtils.cleanup(null, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMultipleNamespacesConfigured() throws Exception {
    Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    MiniDFSCluster cluster = null;
    WebHdfsFileSystem fs = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
              .numDataNodes(1).build();

      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);

      cluster.waitActive();
      DFSTestUtil.addHAConfiguration(conf, LOGICAL_NAME + "remote");
      DFSTestUtil.setFakeHttpAddresses(conf, LOGICAL_NAME + "remote");

      fs = (WebHdfsFileSystem)FileSystem.get(WEBHDFS_URI, conf);
      Assert.assertEquals(2, fs.getResolvedNNAddr().length);
    } finally {
      IOUtils.cleanup(null, fs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Make sure the WebHdfsFileSystem will retry based on RetriableException when
   * rpcServer is null in NamenodeWebHdfsMethods while NameNode starts up.
   */
  @Test (timeout=120000)
  public void testRetryWhileNNStartup() throws Exception {
    final Configuration conf = DFSTestUtil.newHAConfiguration(LOGICAL_NAME);
    MiniDFSCluster cluster = null;
    final Map<String, Boolean> resultMap = new HashMap<String, Boolean>();

    try {
      cluster = new MiniDFSCluster.Builder(conf).nnTopology(topo)
          .numDataNodes(0).build();
      HATestUtil.setFailoverConfigurations(cluster, conf, LOGICAL_NAME);
      cluster.waitActive();
      cluster.transitionToActive(0);

      final NameNode namenode = cluster.getNameNode(0);
      final NamenodeProtocols rpcServer = namenode.getRpcServer();
      Whitebox.setInternalState(namenode, "rpcServer", null);

      new Thread() {
        @Override
        public void run() {
          boolean result = false;
          FileSystem fs = null;
          try {
            fs = FileSystem.get(WEBHDFS_URI, conf);
            final Path dir = new Path("/test");
            result = fs.mkdirs(dir);
          } catch (IOException e) {
            result = false;
          } finally {
            IOUtils.cleanup(null, fs);
          }
          synchronized (TestWebHDFSForHA.this) {
            resultMap.put("mkdirs", result);
            TestWebHDFSForHA.this.notifyAll();
          }
        }
      }.start();

      Thread.sleep(1000);
      Whitebox.setInternalState(namenode, "rpcServer", rpcServer);
      synchronized (this) {
        while (!resultMap.containsKey("mkdirs")) {
          this.wait();
        }
        Assert.assertTrue(resultMap.get("mkdirs"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
