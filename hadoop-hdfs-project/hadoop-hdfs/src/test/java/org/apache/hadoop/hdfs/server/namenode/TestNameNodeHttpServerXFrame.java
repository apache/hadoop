/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URI;

/**
 * A class to test the XFrameoptions of Namenode HTTP Server. We are not reusing
 * the TestNameNodeHTTPServer since it is a parameterized class and these
 * following tests will run multiple times doing the same thing, if we had the
 * code in that classs.
 */
public class TestNameNodeHttpServerXFrame {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public static URL getServerURL(HttpServer2 server)
      throws MalformedURLException {
    Assert.assertNotNull("No server", server);
    return new URL("http://"
        + NetUtils.getHostPortString(server.getConnectorAddress(0)));
  }

  @Test
  public void testNameNodeXFrameOptionsEnabled() throws Exception {
    HttpURLConnection conn = createServerwithXFrame(true, null);
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue("X-FRAME-OPTIONS is absent in the header",
        xfoHeader != null);
    Assert.assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption
        .SAMEORIGIN.toString()));
  }

  @Test
  public void testNameNodeXFrameOptionsDisabled() throws Exception {
    HttpURLConnection conn = createServerwithXFrame(false, null);
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue("unexpected X-FRAME-OPTION in header", xfoHeader == null);
  }

  @Test
  public void testNameNodeXFrameOptionsIllegalOption() throws Exception {
    exception.expect(IllegalArgumentException.class);
    createServerwithXFrame(true, "hadoop");
  }

  private HttpURLConnection createServerwithXFrame(boolean enabled, String
      value) throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, enabled);
    if (value != null) {
      conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, value);

    }
    InetSocketAddress addr = InetSocketAddress.createUnresolved("localhost", 0);
    NameNodeHttpServer server = null;

    server = new NameNodeHttpServer(conf, null, addr);
    server.start();

    URL url = getServerURL(server.getHttpServer());
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    return conn;
  }

  @Test
  public void testSecondaryNameNodeXFrame() throws IOException {
    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");

    SecondaryNameNode sn = new SecondaryNameNode(conf);
    sn.startInfoServer();
    InetSocketAddress httpAddress = SecondaryNameNode.getHttpAddress(conf);

    URL url = URI.create("http://" + httpAddress.getHostName()
        + ":" + httpAddress.getPort()).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue("X-FRAME-OPTIONS is absent in the header",
        xfoHeader != null);
    Assert.assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption
        .SAMEORIGIN.toString()));
  }
}
