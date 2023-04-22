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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.servlet.http.HttpServletResponse;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Small test to cover default disabled prof endpoint.
 */
public class TestDisabledProfileServlet extends HttpServerFunctionalTest {

  private static HttpServer2 server;
  private static URL baseUrl;

  @BeforeClass
  public static void setup() throws Exception {
    server = createTestServer();
    server.start();
    baseUrl = getServerURL(server);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
  }

  @Test
  public void testQuery() throws Exception {
    try {
      readOutput(new URL(baseUrl, "/prof"));
      throw new IllegalStateException("Should not reach here");
    } catch (IOException e) {
      assertTrue(e.getMessage()
          .contains(HttpServletResponse.SC_INTERNAL_SERVER_ERROR + " for URL: " + baseUrl));
    }

    // CORS headers
    HttpURLConnection conn =
        (HttpURLConnection) new URL(baseUrl, "/prof").openConnection();
    assertEquals("GET", conn.getHeaderField(ProfileServlet.ACCESS_CONTROL_ALLOW_METHODS));
    assertNotNull(conn.getHeaderField(ProfileServlet.ACCESS_CONTROL_ALLOW_ORIGIN));
    conn.disconnect();
  }

  @Test
  public void testRequestMethods() throws IOException {
    HttpURLConnection connection = getConnection("PUT");
    assertEquals("Unexpected response code", HttpServletResponse.SC_METHOD_NOT_ALLOWED,
        connection.getResponseCode());
    connection.disconnect();
    connection = getConnection("POST");
    assertEquals("Unexpected response code", HttpServletResponse.SC_METHOD_NOT_ALLOWED,
        connection.getResponseCode());
    connection.disconnect();
    connection = getConnection("DELETE");
    assertEquals("Unexpected response code", HttpServletResponse.SC_METHOD_NOT_ALLOWED,
        connection.getResponseCode());
    connection.disconnect();
    connection = getConnection("GET");
    assertEquals("Unexpected response code", HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        connection.getResponseCode());
    connection.disconnect();
  }

  private HttpURLConnection getConnection(final String method) throws IOException {
    URL url = new URL(baseUrl, "/prof");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(method);
    return conn;
  }

}
