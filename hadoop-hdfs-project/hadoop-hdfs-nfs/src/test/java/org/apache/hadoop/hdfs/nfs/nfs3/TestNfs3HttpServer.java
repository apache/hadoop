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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNfs3HttpServer {
  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestNfs3HttpServer.class.getSimpleName());
  private static NfsConfiguration conf = new NfsConfiguration();
  private static MiniDFSCluster cluster;
  private static String keystoresDir;
  private static String sslConfDir;

  @BeforeClass
  public static void setUp() throws Exception {
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY,
        HttpConfig.Policy.HTTP_AND_HTTPS.name());
    conf.set(NfsConfigKeys.NFS_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(NfsConfigKeys.NFS_HTTPS_ADDRESS_KEY, "localhost:0");
    // Use emphral port in case tests are running in parallel
    conf.setInt(NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY, 0);
    conf.setInt(NfsConfigKeys.DFS_NFS_MOUNTD_PORT_KEY, 0);
    
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestNfs3HttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtil.fullyDelete(new File(BASEDIR));
    if (cluster != null) {
      cluster.shutdown();
    }
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test
  public void testHttpServer() throws Exception {
    Nfs3 nfs = new Nfs3(conf);
    nfs.startServiceInternal(false);
    RpcProgramNfs3 nfsd = (RpcProgramNfs3) nfs.getRpcProgram();
    Nfs3HttpServer infoServer = nfsd.getInfoServer();

    String urlRoot = infoServer.getServerURI().toString();

    // Check default servlets.
    String pageContents = DFSTestUtil.urlGet(new URL(urlRoot + "/jmx"));
    assertTrue("Bad contents: " + pageContents,
        pageContents.contains("java.lang:type="));
    System.out.println("pc:" + pageContents);

    int port = infoServer.getSecurePort();
    assertTrue("Can't get https port", port > 0);
  }
}
