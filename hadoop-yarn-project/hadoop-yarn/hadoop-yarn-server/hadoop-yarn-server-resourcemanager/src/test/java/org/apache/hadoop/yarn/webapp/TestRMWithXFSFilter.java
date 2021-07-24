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

package org.apache.hadoop.yarn.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Used TestRMWebServices as an example of web invocations of RM and added
 * test for XFS Filter.
 */
public class TestRMWithXFSFilter {
  private static MockRM rm;

  private void createMockRm(final Boolean xfsEnabled,
                              final String xfsHeaderValue) {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean("mockrm.webapp.enabled", true);
    if (xfsEnabled != null) {
      conf.setBoolean(YarnConfiguration.YARN_XFS_ENABLED, xfsEnabled);
    }
    if (xfsHeaderValue != null) {
      conf.setStrings(YarnConfiguration.RM_XFS_OPTIONS, xfsHeaderValue);
    }
    rm = new MockRM(conf);
    rm.start();
  }

  @Test
  public void testXFrameOptionsDefaultBehaviour() throws Exception {
    createMockRm(null, null);

    URL url = new URL("http://localhost:8088/ws/v1/cluster/info");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption
        .SAMEORIGIN.toString()));
  }

  @Test
  public void testXFrameOptionsExplicitlyEnabled() throws Exception {
    createMockRm(true, HttpServer2.XFrameOption
        .SAMEORIGIN.toString());

    URL url = new URL("http://localhost:8088/ws/v1/cluster/info");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption
        .SAMEORIGIN.toString()));
  }

  @Test
  public void testXFrameOptionsEnabledDefaultApps() throws Exception {
    createMockRm(true, HttpServer2.XFrameOption
        .SAMEORIGIN.toString());

    URL url = new URL("http://localhost:8088/logs");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertTrue(xfoHeader.endsWith(HttpServer2.XFrameOption
        .SAMEORIGIN.toString()));
  }

  @Test
  public void testXFrameOptionsDisabled() throws Exception {
    createMockRm(false, null);

    URL url = new URL("http://localhost:8088/ws/v1/cluster/info");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    Assert.assertNull("Unexpected X-FRAME-OPTION in header", xfoHeader);
  }

  @Test
  public void testXFrameOptionsIllegalOption() {
    IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> createMockRm(true, "otherValue"));
  }

  @After
  public void tearDown() throws IOException {
    rm.close();
  }
}
