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
package org.apache.hadoop.hdfs.server.datanode.web;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestDatanodeHttpServer {
  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestDatanodeHttpServer.class.getSimpleName());
  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration conf;
  private static URLConnectionFactory connectionFactory;

  @Parameters
  public static Collection<Object[]> policy() {
    Object[][] params = new Object[][] {{HttpConfig.Policy.HTTP_ONLY},
        {HttpConfig.Policy.HTTPS_ONLY}, {HttpConfig.Policy.HTTP_AND_HTTPS}};
    return Arrays.asList(params);
  }

  private final HttpConfig.Policy policy;

  public TestDatanodeHttpServer(Policy policy) {
    super();
    this.policy = policy;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    conf = new Configuration();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestDatanodeHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    conf.set(DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test
  public void testHttpPolicy() throws Exception {
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, policy.name());
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    DatanodeHttpServer server = null;
    try {
      server = new DatanodeHttpServer(conf, null, null);
      server.start();

      Assert.assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      Assert.assertTrue(implies(!policy.isHttpEnabled(),
          server.getHttpAddress() == null));

      Assert.assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      Assert.assertTrue(implies(!policy.isHttpsEnabled(),
          server.getHttpsAddress() == null));

    } finally {
      if (server != null) {
        server.close();
      }
    }
  }

  private static boolean canAccess(String scheme, InetSocketAddress addr) {
    if (addr == null) {
      return false;
    }
    try {
      URL url = new URL(scheme + "://" + NetUtils.getHostPortString(addr));
      URLConnection conn = connectionFactory.openConnection(url);
      conn.connect();
      Assert.assertTrue(conn instanceof java.net.HttpURLConnection);
      java.net.HttpURLConnection httpConn = (java.net.HttpURLConnection) conn;
      if (httpConn.getResponseCode() != 200) {
        return false;
      }

      StringBuilder builder = new StringBuilder();
      InputStreamReader responseReader = new InputStreamReader((conn.getInputStream()));
      try (BufferedReader reader = new BufferedReader(responseReader)) {
        String output;
        while ((output = reader.readLine()) != null) {
          builder.append(output);
        }
      }
      return builder.toString().contains("Hadoop Administration");
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean implies(boolean a, boolean b) {
    return !a || b;
  }
}
