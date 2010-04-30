/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Testing, info servers are disabled.  This test enables then and checks that
 * they serve pages.
 */
public class TestInfoServers extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestInfoServers.class);
  
  @Override
  protected void preHBaseClusterSetup() {
    // Bring up info servers on 'odd' port numbers in case the test is not
    // sourcing the src/test/hbase-default.xml.
    conf.setInt("hbase.master.info.port", 60011);
    conf.setInt("hbase.regionserver.info.port", 60031);
  }
  
  /**
   * @throws Exception
   */
  public void testInfoServersAreUp() throws Exception {
    // give the cluster time to start up
    new HTable(conf, ".META.");
    int port = cluster.getMaster().getInfoServer().getPort();
    assertHasExpectedContent(new URL("http://localhost:" + port +
      "/index.html"), "master");
    port = cluster.getRegionServerThreads().get(0).getRegionServer().
      getInfoServer().getPort();
    assertHasExpectedContent(new URL("http://localhost:" + port +
      "/index.html"), "regionserver");
  }
  
  private void assertHasExpectedContent(final URL u, final String expected)
  throws IOException {
    LOG.info("Testing " + u.toString() + " has " + expected);
    java.net.URLConnection c = u.openConnection();
    c.connect();
    assertTrue(c.getContentLength() > 0);
    StringBuilder sb = new StringBuilder(c.getContentLength());
    BufferedInputStream bis = new BufferedInputStream(c.getInputStream());
    byte [] bytes = new byte[1024];
    for (int read = -1; (read = bis.read(bytes)) != -1;) {
      sb.append(new String(bytes, 0, read));
    }
    bis.close();
    String content = sb.toString();
    assertTrue(content.contains(expected));
  }
}