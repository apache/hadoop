/*
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

package org.apache.hadoop.http;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test coverage for async profiler servlets: ProfileServlet and ProfileOutputServlet.
 */
public class TestProfileServlet extends HttpServerFunctionalTest {

  private static HttpServer2 server;
  private static URL baseUrl;

  private static final Logger LOG = LoggerFactory.getLogger(TestProfileServlet.class);

  @BeforeClass
  public static void setup() throws Exception {
    ProfileServlet.setIsTestRun(true);
    System.setProperty("async.profiler.home", UUID.randomUUID().toString());
    server = createTestServer();
    server.start();
    baseUrl = getServerURL(server);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    ProfileServlet.setIsTestRun(false);
    System.clearProperty("async.profiler.home");
    server.stop();
  }

  @Test
  public void testQuery() throws Exception {
    String output = readOutput(new URL(baseUrl, "/prof"));
    LOG.info("/prof output: {}", output);
    assertTrue(output.startsWith(
        "Started [cpu] profiling. This page will automatically redirect to /prof-output-hadoop/"));
    assertTrue(output.contains(
        "If empty diagram and Linux 4.6+, see 'Basic Usage' section on the Async Profiler Home"
            + " Page, https://github.com/jvm-profiling-tools/async-profiler."));

    HttpURLConnection conn =
        (HttpURLConnection) new URL(baseUrl, "/prof").openConnection();
    assertEquals("GET", conn.getHeaderField(ProfileServlet.ACCESS_CONTROL_ALLOW_METHODS));
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    assertNotNull(conn.getHeaderField(ProfileServlet.ACCESS_CONTROL_ALLOW_ORIGIN));
    assertTrue(conn.getHeaderField("Refresh").startsWith("10;/prof-output-hadoop/async-prof-pid"));

    String redirectOutput = readOutput(new URL(baseUrl, "/prof-output-hadoop"));
    LOG.info("/prof-output-hadoop output: {}", redirectOutput);

    HttpURLConnection redirectedConn =
        (HttpURLConnection) new URL(baseUrl, "/prof-output-hadoop").openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, redirectedConn.getResponseCode());

    redirectedConn.disconnect();
    conn.disconnect();
  }

}
