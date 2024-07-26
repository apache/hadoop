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

package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import static org.apache.hadoop.http.HttpServer2.XFrameOption.SAMEORIGIN;

/**
 * A class to test the XFrame options of Router HTTP Server.
 */
public class TestRouterHttpServerXFrame {

  @Test
  public void testRouterXFrame() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, true);
    conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, SAMEORIGIN.toString());

    Router router = new Router();
    try {
      router.init(conf);
      router.start();

      InetSocketAddress httpAddress = router.getHttpServerAddress();
      URL url =
          URI.create("http://" + httpAddress.getHostName() + ":" + httpAddress.getPort()).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();

      String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
      Assert.assertNotNull("X-FRAME-OPTIONS is absent in the header", xfoHeader);
      Assert.assertTrue(xfoHeader.endsWith(SAMEORIGIN.toString()));
    } finally {
      router.stop();
      router.close();
    }
  }
}
