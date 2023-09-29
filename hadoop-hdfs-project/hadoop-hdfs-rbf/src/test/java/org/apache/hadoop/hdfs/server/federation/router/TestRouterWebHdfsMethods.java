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

package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createMountTableEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test suite for Router Web Hdfs methods.
 */
public class TestRouterWebHdfsMethods {
  static final Logger LOG =
      LoggerFactory.getLogger(TestRouterWebHdfsMethods.class);

  private static StateStoreDFSCluster cluster;
  private static RouterContext router;
  private static String httpUri;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .rpc()
        .http()
        .admin()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.setIndependentDNs();
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
    router = cluster.getRandomRouter();
    httpUri = "http://"+router.getHttpAddress();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testWebHdfsCreate() throws Exception {
    // the file is created at default ns (ns0)
    String path = "/tmp/file";
    URL url = new URL(getUri(path));
    LOG.info("URL: {}", url);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    verifyFile("ns0", path, true);
    verifyFile("ns1", path, false);
    conn.disconnect();
  }

  @Test
  public void testWebHdfsCreateWithMounts() throws Exception {
    // the file is created at mounted ns (ns1)
    String mountPoint = "/tmp-ns1";
    String path = "/tmp-ns1/file";
    createMountTableEntry(
        router.getRouter(), mountPoint,
        DestinationOrder.RANDOM, Collections.singletonList("ns1"));
    URL url = new URL(getUri(path));
    LOG.info("URL: {}", url);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    verifyFile("ns1", path, true);
    verifyFile("ns0", path, false);
    conn.disconnect();
  }

  private String getUri(String path) {
    final String user = System.getProperty("user.name");
    final StringBuilder uri = new StringBuilder(httpUri);
    uri.append("/webhdfs/v1").
        append(path).
        append("?op=CREATE").
        append("&user.name=" + user);
    return uri.toString();
  }

  private void verifyFile(String ns, String path, boolean shouldExist)
      throws Exception {
    FileSystem fs = cluster.getNamenode(ns, null).getFileSystem();
    try {
      fs.getFileStatus(new Path(path));
      if (!shouldExist) {
        fail(path + " should not exist in ns " + ns);
      }
    } catch (FileNotFoundException e) {
      if (shouldExist) {
        fail(path + " should exist in ns " + ns);
      }
    }
  }

  @Test
  public void testGetNsFromDataNodeNetworkLocation() {
    assertEquals("ns0", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("/ns0/rack-info1"));
    assertEquals("ns0", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("/ns0/row1/rack-info1"));
    assertEquals("", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("/row0"));
    assertEquals("", RouterWebHdfsMethods
        .getNsFromDataNodeNetworkLocation("whatever-rack-info1"));
  }

  @Test
  public void testWebHdfsCreateWithInvalidPath() throws Exception {
    // A path name include duplicated slashes.
    String path = "//tmp//file";
    assertResponse(path);
  }

  private void assertResponse(String path) throws IOException {
    URL url = new URL(getUri(path));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    // Assert response code.
    assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
    // Assert exception.
    Map<?, ?> response = WebHdfsFileSystem.jsonParse(conn, true);
    assertEquals("InvalidPathException",
        ((LinkedHashMap) response.get("RemoteException")).get("exception"));
    conn.disconnect();
  }
}
