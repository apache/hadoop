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

package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.http.HttpServer2;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that X-Frame-Options works correctly with JournalNodeHttpServer.
 */
public class TestJournalNodeHttpServerXFrame {

  private static final int NUM_JN = 1;

  private MiniJournalCluster cluster;

  @Test
  public void testJournalNodeXFrameOptionsEnabled() throws Exception {
    boolean xFrameEnabled = true;
    cluster = createCluster(xFrameEnabled);
    HttpURLConnection conn = getConn(cluster);
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    assertTrue(xfoHeader != null, "X-FRAME-OPTIONS is absent in the header");
    assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption.SAMEORIGIN.toString()));
  }

  @Test
  public void testJournalNodeXFrameOptionsDisabled() throws Exception {
    boolean xFrameEnabled = false;
    cluster = createCluster(xFrameEnabled);
    HttpURLConnection conn = getConn(cluster);
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    System.out.println(xfoHeader);
    assertTrue(xfoHeader == null, "unexpected X-FRAME-OPTION in header");
  }

  @AfterEach
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private static MiniJournalCluster createCluster(boolean enabled) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, enabled);
    MiniJournalCluster jCluster =
        new MiniJournalCluster.Builder(conf).format(true).numJournalNodes(NUM_JN).build();
    jCluster.waitActive();
    return jCluster;
  }

  private static HttpURLConnection getConn(MiniJournalCluster journalCluster) throws IOException {
    JournalNode journalNode = journalCluster.getJournalNode(0);
    URL newURL = new URL(journalNode.getHttpServerURI());
    HttpURLConnection conn = (HttpURLConnection) newURL.openConnection();
    conn.connect();
    return conn;
  }
}