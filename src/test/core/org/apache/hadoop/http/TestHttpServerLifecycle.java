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
package org.apache.hadoop.http;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.io.File;

public class TestHttpServerLifecycle {


  /**
   * Create but do not start the server
   * @return the server instance in the member variable "server"
   * @throws Exception on any failure
   */
  private HttpServer createServer() throws Exception {
    new File(System.getProperty("build.webapps", "build/webapps") + "/test"
             ).mkdirs();
    HttpServer server = new HttpServer("test", "0.0.0.0", 0, true);
    return server;
  }

  /**
   * Create and start the server
   * @return the newly started server
   * @throws Exception on any failure
   */
  private HttpServer createAndStartServer() throws Exception {
    HttpServer server = createServer();
    server.start();
    return server;
  }

  /**
   * If the server is non null, stop it
   * @throws Exception on any failure
   */
  private void stop(HttpServer server) throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Check that a server is alive by probing the {@link HttpServer#isAlive()} method
   * and the text of its toString() description
   * @param server server
   */
  private void assertAlive(HttpServer server) {
    assertTrue("Server is not alive", server.isAlive());
    assertToStringContains(server, HttpServer.STATE_DESCRIPTION_ALIVE);
  }

  private void assertNotLive(HttpServer server) {
    assertTrue("Server should not be live", !server.isAlive());
    assertToStringContains(server, HttpServer.STATE_DESCRIPTION_NOT_LIVE);
  }

  /**
   * Test that the server is alive once started
   *
   * @throws Throwable on failure
   */
  @Test public void testCreatedServerIsNotAlive() throws Throwable {
    HttpServer server = createServer();
    assertNotLive(server);
  }

  @Test public void testStopUnstartedServer() throws Throwable {
    HttpServer server = createServer();
    stop(server);
  }

  /**
   * Test that the server is alive once started
   *
   * @throws Throwable on failure
   */
  @Test public void testStartedServerIsAlive() throws Throwable {
    HttpServer server = null;
    try {
      server = createServer();
      assertNotLive(server);
      server.start();
      assertAlive(server);
    } finally {
      stop(server);
    }
  }

  /**
   * Assert that the result of {@link HttpServer#toString()} contains the specific text
   * @param server server to examine
   * @param text text to search for
   */
  private void assertToStringContains(HttpServer server, String text) {
    String description = server.toString();
    assertTrue("Did not find \"" + text + "\" in \"" + description + "\"",
               description.contains(text));
  }

  /**
   * Test that the server is not alive once stopped
   *
   * @throws Throwable on failure
   */
  @Test public void testStoppedServerIsNotAlive() throws Throwable {
    HttpServer server = createAndStartServer();
    assertAlive(server);
    stop(server);
    assertNotLive(server);
  }

  /**
   * Test that the server is not alive once stopped
   *
   * @throws Throwable on failure
   */
  @Test public void testStoppingTwiceServerIsAllowed() throws Throwable {
    HttpServer server = createAndStartServer();
    assertAlive(server);
    stop(server);
    assertNotLive(server);
    stop(server);
    assertNotLive(server);
  }

}
