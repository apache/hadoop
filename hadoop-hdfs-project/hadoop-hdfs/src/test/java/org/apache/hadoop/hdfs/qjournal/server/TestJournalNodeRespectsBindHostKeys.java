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
package org.apache.hadoop.hdfs.qjournal.server;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_RPC_BIND_HOST_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.HdfsConfiguration;

/**
 * This test checks that the JournalNode respects the following keys.
 *
 *  - DFS_JOURNALNODE_RPC_BIND_HOST_KEY
 *  - DFS_JOURNALNODE_HTTP_BIND_HOST_KEY
 *  - DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY
 */
public class TestJournalNodeRespectsBindHostKeys {

  public static final Log LOG = LogFactory.getLog(
      TestJournalNodeRespectsBindHostKeys.class);
  private static final String WILDCARD_ADDRESS = "0.0.0.0";
  private static final String LOCALHOST_SERVER_ADDRESS = "127.0.0.1:0";
  private static final int NUM_JN = 1;

  private HdfsConfiguration conf;
  private MiniJournalCluster jCluster;
  private JournalNode jn;

  @Before
  public void setUp() {
    conf = new HdfsConfiguration();
  }

  @After
  public void tearDown() throws IOException {
    if (jCluster != null) {
      jCluster.shutdown();
      jCluster = null;
    }
  }

  private static String getRpcServerAddress(JournalNode jn) {
    JournalNodeRpcServer rpcServer = jn.getRpcServer();
    return rpcServer.getRpcServer().getListenerAddress().getAddress().
        toString();
  }

  @Test (timeout=300000)
  public void testRpcBindHostKey() throws IOException {
    LOG.info("Testing without " + DFS_JOURNALNODE_RPC_BIND_HOST_KEY);

    // NN should not bind the wildcard address by default.
    jCluster = new MiniJournalCluster.Builder(conf).format(true)
        .numJournalNodes(NUM_JN).build();
    jn = jCluster.getJournalNode(0);
    String address = getRpcServerAddress(jn);
    assertThat("Bind address not expected to be wildcard by default.",
        address, not("/" + WILDCARD_ADDRESS));

    LOG.info("Testing with " + DFS_JOURNALNODE_RPC_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_JOURNALNODE_RPC_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    jCluster = new MiniJournalCluster.Builder(conf).format(true)
        .numJournalNodes(NUM_JN).build();
    jn = jCluster.getJournalNode(0);
    address = getRpcServerAddress(jn);
    assertThat("Bind address " + address + " is not wildcard.",
        address, is("/" + WILDCARD_ADDRESS));
  }

  @Test(timeout=300000)
  public void testHttpBindHostKey() throws IOException {
    LOG.info("Testing without " + DFS_JOURNALNODE_HTTP_BIND_HOST_KEY);

    // NN should not bind the wildcard address by default.
    conf.set(DFS_JOURNALNODE_HTTP_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
    jCluster = new MiniJournalCluster.Builder(conf).format(true)
        .numJournalNodes(NUM_JN).build();
    jn = jCluster.getJournalNode(0);
    String address = jn.getHttpAddress().toString();
    assertFalse("HTTP Bind address not expected to be wildcard by default.",
        address.startsWith(WILDCARD_ADDRESS));

    LOG.info("Testing with " + DFS_JOURNALNODE_HTTP_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_JOURNALNODE_HTTP_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    conf.set(DFS_JOURNALNODE_HTTP_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
    jCluster = new MiniJournalCluster.Builder(conf).format(true)
        .numJournalNodes(NUM_JN).build();
    jn = jCluster.getJournalNode(0);
    address = jn.getHttpAddress().toString();
    assertTrue("HTTP Bind address " + address + " is not wildcard.",
        address.startsWith(WILDCARD_ADDRESS));
  }

  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" +
      TestJournalNodeRespectsBindHostKeys.class.getSimpleName();

  private static void setupSsl() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    assertTrue(base.mkdirs());
    final String keystoresDir = new File(BASEDIR).getAbsolutePath();
    final String sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestJournalNodeRespectsBindHostKeys.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
  }

  /**
   * HTTPS test is different since we need to setup SSL configuration.
   * NN also binds the wildcard address for HTTPS port by default so we must
   * pick a different host/port combination.
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testHttpsBindHostKey() throws Exception {
    LOG.info("Testing behavior without " + DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY);

    setupSsl();

    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());

    // NN should not bind the wildcard address by default.
    conf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
    jCluster = new MiniJournalCluster.Builder(conf).format(true)
        .numJournalNodes(NUM_JN).build();
    jn = jCluster.getJournalNode(0);
    String address = jn.getHttpsAddress().toString();
    assertFalse("HTTP Bind address not expected to be wildcard by default.",
        address.startsWith(WILDCARD_ADDRESS));

    LOG.info("Testing behavior with " + DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    conf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
    jCluster = new MiniJournalCluster.Builder(conf).format(true)
        .numJournalNodes(NUM_JN).build();
    jn = jCluster.getJournalNode(0);
    address = jn.getHttpsAddress().toString();
    assertTrue("HTTP Bind address " + address + " is not wildcard.",
        address.startsWith(WILDCARD_ADDRESS));
  }
}
