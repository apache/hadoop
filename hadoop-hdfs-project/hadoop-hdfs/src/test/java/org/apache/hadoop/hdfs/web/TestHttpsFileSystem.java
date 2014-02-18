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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHttpsFileSystem {
  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" + TestHttpsFileSystem.class.getSimpleName();

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  private static String keystoresDir;
  private static String sslConfDir;
  private static String nnAddr;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestHttpsFileSystem.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    OutputStream os = cluster.getFileSystem().create(new Path("/test"));
    os.write(23);
    os.close();
    InetSocketAddress addr = cluster.getNameNode().getHttpsAddress();
    nnAddr = NetUtils.getHostPortString(addr);
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, nnAddr);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test
  public void testHsftpFileSystem() throws Exception {
    FileSystem fs = FileSystem.get(new URI("hsftp://" + nnAddr), conf);
    Assert.assertTrue(fs.exists(new Path("/test")));
    InputStream is = fs.open(new Path("/test"));
    Assert.assertEquals(23, is.read());
    is.close();
    fs.close();
  }

  @Test
  public void testSWebHdfsFileSystem() throws Exception {
    FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, "swebhdfs");
    final Path f = new Path("/testswebhdfs");
    FSDataOutputStream os = fs.create(f);
    os.write(23);
    os.close();
    Assert.assertTrue(fs.exists(f));
    InputStream is = fs.open(f);
    Assert.assertEquals(23, is.read());
    is.close();
    fs.close();
  }
}
