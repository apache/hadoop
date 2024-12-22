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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;

public class TestBalancerHttpServer {
  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestBalancerHttpServer.class.getSimpleName());
  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration conf;
  private static URLConnectionFactory connectionFactory;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTP_AND_HTTPS.name());
    conf.set(DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_KEY, "localhost:0");
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestBalancerHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test
  public void testHttpServer() throws Exception {
    BalancerHttpServer server = null;
    try {
      server = new BalancerHttpServer(conf);
      server.start();
      Assertions.assertTrue(checkConnection("http", server.getHttpAddress()));
      Assertions.assertTrue(checkConnection("https", server.getHttpsAddress()));
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private boolean checkConnection(String scheme, InetSocketAddress address) {
    if (address == null)
      return false;
    try {
      URL url = new URL(scheme + "://" + NetUtils.getHostPortString(address));
      URLConnection conn = connectionFactory.openConnection(url);
      conn.connect();
      conn.getContent();
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
