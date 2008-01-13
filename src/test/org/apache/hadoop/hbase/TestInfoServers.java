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
import org.apache.hadoop.io.Text;

/**
 * Testing, info servers are disabled.  This test enables then and checks that
 * they serve pages.
 */
public class TestInfoServers extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestInfoServers.class);

  @Override  
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * @throws Exception
   */
  public void testInfoServersAreUp() throws Exception {
    // Bring up info servers on 'odd' port numbers in case the test is not
    // sourcing the src/test/hbase-default.xml.
    this.conf.setInt("hbase.master.info.port", 60011);
    this.conf.setInt("hbase.regionserver.info.port", 60031);
    MiniHBaseCluster miniHbase = new MiniHBaseCluster(this.conf, 1);
    // Create table so info servers are given time to spin up.
    HBaseAdmin a = new HBaseAdmin(conf);
    a.createTable(new HTableDescriptor(getName()));
    assertTrue(a.tableExists(new Text(getName())));
    try {
      int port = miniHbase.getMaster().infoServer.getPort();
      assertHasExpectedContent(new URL("http://localhost:" + port +
        "/index.html"), "Master");
      port = miniHbase.getRegionThreads().get(0).getRegionServer().
        infoServer.getPort();
      assertHasExpectedContent(new URL("http://localhost:" + port +
        "/index.html"), "Region Server");
    } finally {
      miniHbase.shutdown();
    }
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
    content.matches(expected);
  }
}