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

package org.apache.hadoop.hdfs.server.datanode.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.http.HttpServer2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Test that X-Frame-Options works correctly with DatanodeHTTPServer.
 */
public class TestDatanodeHttpXFrame {

  private MiniDFSCluster cluster = null;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @After
  public void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testDataNodeXFrameOptionsEnabled() throws Exception {
    boolean xFrameEnabled = true;
    cluster = createCluster(xFrameEnabled, null);
    HttpURLConnection conn = getConn(cluster);
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue("X-FRAME-OPTIONS is absent in the header",
        xfoHeader != null);
    Assert.assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption
        .SAMEORIGIN.toString()));
  }

  @Test
  public void testNameNodeXFrameOptionsDisabled() throws Exception {
    boolean xFrameEnabled = false;
    cluster = createCluster(xFrameEnabled, null);
    HttpURLConnection conn = getConn(cluster);
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue("unexpected X-FRAME-OPTION in header", xfoHeader == null);
  }

  @Test
  public void testDataNodeXFramewithInvalidOptions() throws Exception {
    exception.expect(IllegalArgumentException.class);
    cluster = createCluster(false, "Hadoop");
  }

  private static MiniDFSCluster createCluster(boolean enabled, String
      value) throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, enabled);
    if (value != null) {
      conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, value);
    }
    MiniDFSCluster dfsCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    dfsCluster.waitActive();
    return dfsCluster;
  }

  private static HttpURLConnection getConn(MiniDFSCluster dfsCluster)
      throws IOException {
    DataNode datanode = dfsCluster.getDataNodes().get(0);
    URL newURL = new URL("http://localhost:" + datanode.getInfoPort());
    HttpURLConnection conn = (HttpURLConnection) newURL.openConnection();
    conn.connect();
    return conn;
  }
}
