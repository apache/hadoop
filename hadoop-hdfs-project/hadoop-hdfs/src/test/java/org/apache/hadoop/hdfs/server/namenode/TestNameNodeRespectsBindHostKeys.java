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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

import org.junit.Test;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * This test checks that the NameNode respects the following keys:
 *
 *  - DFS_NAMENODE_RPC_BIND_HOST_KEY
 *  - DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY
 *  - DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY
 *  - DFS_NAMENODE_HTTP_BIND_HOST_KEY
 *  - DFS_NAMENODE_HTTPS_BIND_HOST_KEY
 */
public class TestNameNodeRespectsBindHostKeys {
  public static final Log LOG = LogFactory.getLog(TestNameNodeRespectsBindHostKeys.class);
  private static final String WILDCARD_ADDRESS = "0.0.0.0";
  private static final String LOCALHOST_SERVER_ADDRESS = "127.0.0.1:0";
  private static String keystoresDir;
  private static String sslConfDir;

  private static String getRpcServerAddress(MiniDFSCluster cluster) {
    NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNodeRpc();
    return rpcServer.getClientRpcServer().getListenerAddress().getAddress().toString();
  }

  private static String getServiceRpcServerAddress(MiniDFSCluster cluster) {
    NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNodeRpc();
    return rpcServer.getServiceRpcServer().getListenerAddress().getAddress().toString();
  }

  private static String getLifelineRpcServerAddress(MiniDFSCluster cluster) {
    NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNodeRpc();
    return rpcServer.getLifelineRpcServer().getListenerAddress().getAddress()
        .toString();
  }

  @Test (timeout=300000)
  public void testRpcBindHostKey() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    
    LOG.info("Testing without " + DFS_NAMENODE_RPC_BIND_HOST_KEY);
    
    // NN should not bind the wildcard address by default.
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = getRpcServerAddress(cluster);
      assertThat("Bind address not expected to be wildcard by default.",
                 address, not("/" + WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }

    LOG.info("Testing with " + DFS_NAMENODE_RPC_BIND_HOST_KEY);
    
    // Tell NN to bind the wildcard address.
    conf.set(DFS_NAMENODE_RPC_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = getRpcServerAddress(cluster);
      assertThat("Bind address " + address + " is not wildcard.",
                 address, is("/" + WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }    
  }

  @Test (timeout=300000)
  public void testServiceRpcBindHostKey() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    LOG.info("Testing without " + DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
    
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);

    // NN should not bind the wildcard address by default.
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = getServiceRpcServerAddress(cluster);
      assertThat("Bind address not expected to be wildcard by default.",
                 address, not("/" + WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }

    LOG.info("Testing with " + DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = getServiceRpcServerAddress(cluster);
      assertThat("Bind address " + address + " is not wildcard.",
                 address, is("/" + WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test (timeout=300000)
  public void testLifelineRpcBindHostKey() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    LOG.info("Testing without " + DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY);

    conf.set(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);

    // NN should not bind the wildcard address by default.
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = getLifelineRpcServerAddress(cluster);
      assertThat("Bind address not expected to be wildcard by default.",
                 address, not("/" + WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }

    LOG.info("Testing with " + DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = getLifelineRpcServerAddress(cluster);
      assertThat("Bind address " + address + " is not wildcard.",
                 address, is("/" + WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=300000)
  public void testHttpBindHostKey() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    LOG.info("Testing without " + DFS_NAMENODE_HTTP_BIND_HOST_KEY);

    // NN should not bind the wildcard address by default.
    try {
      conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = cluster.getNameNode().getHttpAddress().toString();
      assertFalse("HTTP Bind address not expected to be wildcard by default.",
                  address.startsWith(WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }

    LOG.info("Testing with " + DFS_NAMENODE_HTTP_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_NAMENODE_HTTP_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    try {
      conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = cluster.getNameNode().getHttpAddress().toString();
      assertTrue("HTTP Bind address " + address + " is not wildcard.",
                 address.startsWith(WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestNameNodeRespectsBindHostKeys.class.getSimpleName());

  private static void setupSsl() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    assertTrue(base.mkdirs());
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestNameNodeRespectsBindHostKeys.class);

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
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    LOG.info("Testing behavior without " + DFS_NAMENODE_HTTPS_BIND_HOST_KEY);

    setupSsl();
    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());

    // NN should not bind the wildcard address by default.
    try {
      conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = cluster.getNameNode().getHttpsAddress().toString();
      assertFalse("HTTP Bind address not expected to be wildcard by default.",
                  address.startsWith(WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }

    LOG.info("Testing behavior with " + DFS_NAMENODE_HTTPS_BIND_HOST_KEY);

    // Tell NN to bind the wildcard address.
    conf.set(DFS_NAMENODE_HTTPS_BIND_HOST_KEY, WILDCARD_ADDRESS);

    // Verify that NN binds wildcard address now.
    try {
      conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, LOCALHOST_SERVER_ADDRESS);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      String address = cluster.getNameNode().getHttpsAddress().toString();
      assertTrue("HTTP Bind address " + address + " is not wildcard.",
                 address.startsWith(WILDCARD_ADDRESS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (keystoresDir != null && !keystoresDir.isEmpty()
          && sslConfDir != null && !sslConfDir.isEmpty()) {
        KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
      }
    }
  }  
}
