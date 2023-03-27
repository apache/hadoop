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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests use of the cross-site-request forgery (CSRF) prevention filter with
 * WebHDFS.  This is a parameterized test that covers various combinations of
 * CSRF protection enabled or disabled at the NameNode, the DataNode and the
 * WebHDFS client.  If the server is configured with CSRF prevention, but the
 * client is not, then protected operations are expected to fail.
 */
@RunWith(Parameterized.class)
public class TestWebHdfsWithRestCsrfPreventionFilter {

  private static final Path FILE = new Path("/file");

  private final boolean nnRestCsrf;
  private final boolean dnRestCsrf;
  private final boolean clientRestCsrf;

  private MiniDFSCluster cluster;
  private FileSystem fs, webhdfs;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public TestWebHdfsWithRestCsrfPreventionFilter(boolean nnRestCsrf,
      boolean dnRestCsrf, boolean clientRestCsrf) {
    this.nnRestCsrf = nnRestCsrf;
    this.dnRestCsrf = dnRestCsrf;
    this.clientRestCsrf = clientRestCsrf;
  }

  @Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {false, false, false},
        {true, true, true},
        {true, true, false},
        {true, false, true},
        {true, false, false},
        {false, true, true},
        {false, true, false},
        {false, false, true}});
  }

  @Before
  public void before() throws Exception {
    Configuration nnConf = new Configuration();
    nnConf.setBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY, nnRestCsrf);
    // Set configuration to treat anything as a browser, so that CSRF prevention
    // checks actually get enforced.
    nnConf.set(DFS_WEBHDFS_REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY, ".*");
    cluster = new MiniDFSCluster.Builder(nnConf).numDataNodes(0).build();

    Configuration dnConf = new Configuration(nnConf);
    dnConf.setBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY, dnRestCsrf);
    // By default the datanode loads the CSRF filter handler
    dnConf.set(DFS_DATANODE_HTTPSERVER_FILTER_HANDLERS,
        "org.apache.hadoop.hdfs.server.datanode.web.RestCsrfPreventionFilterHandler");
    cluster.startDataNodes(dnConf, 1, true, null, null, null, null, false);

    cluster.waitActive();
    fs = cluster.getFileSystem();

    Configuration clientConf = new Configuration();
    clientConf.setBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY, clientRestCsrf);
    InetSocketAddress addr = cluster.getNameNode().getHttpAddress();
    webhdfs = FileSystem.get(URI.create("webhdfs://" +
        NetUtils.getHostPortString(addr)), clientConf);
  }

  @After
  public void after() {
    IOUtils.closeStream(webhdfs);
    IOUtils.closeStream(fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreate() throws Exception {
    // create is a HTTP PUT that redirects from NameNode to DataNode, so we
    // expect CSRF prevention on either server to block an unconfigured client.
    if ((nnRestCsrf || dnRestCsrf) && !clientRestCsrf) {
      expectException();
    }
    assertTrue(webhdfs.createNewFile(FILE));
  }

  @Test
  public void testDelete() throws Exception {
    DFSTestUtil.createFile(fs, FILE, 1024, (short)1, 0L);
    // delete is an HTTP DELETE that executes solely within the NameNode as a
    // metadata operation, so we expect CSRF prevention configured on the
    // NameNode to block an unconfigured client.
    if (nnRestCsrf && !clientRestCsrf) {
      expectException();
    }
    assertTrue(webhdfs.delete(FILE, false));
  }

  @Test
  public void testGetFileStatus() throws Exception {
    // getFileStatus is an HTTP GET, not subject to CSRF prevention, so we
    // expect it to succeed always, regardless of CSRF configuration.
    assertNotNull(webhdfs.getFileStatus(new Path("/")));
  }

  @Test
  public void testTruncate() throws Exception {
    DFSTestUtil.createFile(fs, FILE, 1024, (short)1, 0L);
    // truncate is an HTTP POST that executes solely within the NameNode as a
    // metadata operation, so we expect CSRF prevention configured on the
    // NameNode to block an unconfigured client.
    if (nnRestCsrf && !clientRestCsrf) {
      expectException();
    }
    assertTrue("WebHdfs supports truncate",
        webhdfs.hasPathCapability(FILE, CommonPathCapabilities.FS_TRUNCATE));
    assertTrue(webhdfs.truncate(FILE, 0L));
  }

  private void expectException() {
    exception.expect(IOException.class);
    exception.expectMessage("Missing Required Header");
  }
}
