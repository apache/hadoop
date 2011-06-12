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

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * This is a base class for functional tests of the {@link HttpServer}.
 * The methods are static for other classes to import statically.
 */
public class HttpServerFunctionalTest extends Assert {
  /** JVM property for the webapp test dir : {@value} */
  public static final String TEST_BUILD_WEBAPPS = "test.build.webapps";
  /** expected location of the test.build.webapps dir: {@value} */
  private static final String BUILD_WEBAPPS_DIR = "build/webapps";
  
  /** name of the test webapp: {@value} */
  private static final String TEST = "test";

  /**
   * Create but do not start the test webapp server. The test webapp dir is
   * prepared/checked in advance.
   *
   * @return the server instance
   *
   * @throws IOException if a problem occurs
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createTestServer() throws IOException {
    prepareTestWebapp();
    return createServer(TEST);
  }

  /**
   * Create but do not start the test webapp server. The test webapp dir is
   * prepared/checked in advance.
   * @param conf the server configuration to use
   * @return the server instance
   *
   * @throws IOException if a problem occurs
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createTestServer(Configuration conf)
      throws IOException {
    prepareTestWebapp();
    return createServer(TEST, conf);
  }

  /**
   * Prepare the test webapp by creating the directory from the test properties
   * fail if the directory cannot be created.
   * @throws AssertionError if a condition was not met
   */
  protected static void prepareTestWebapp() {
    String webapps = System.getProperty(TEST_BUILD_WEBAPPS, BUILD_WEBAPPS_DIR);
    File testWebappDir = new File(webapps +
        File.pathSeparator + TEST);
    if (!testWebappDir.exists()) {
      assertTrue("Unable to create the test dir " + testWebappDir,
          testWebappDir.mkdirs());
    } else {
      assertTrue("Not a directory " + testWebappDir,
          testWebappDir.isDirectory());
    }
  }

  /**
   * Create an HttpServer instance for the given webapp
   * @param webapp the webapp to work with
   * @return the server
   * @throws IOException if it could not be created
   */
  public static HttpServer createServer(String webapp) throws IOException {
    return new HttpServer(webapp, "0.0.0.0", 0, true);
  }
  /**
   * Create an HttpServer instance for the given webapp
   * @param webapp the webapp to work with
   * @param conf the configuration to use for the server
   * @return the server
   * @throws IOException if it could not be created
   */
  public static HttpServer createServer(String webapp, Configuration conf)
      throws IOException {
    return new HttpServer(webapp, "0.0.0.0", 0, true, conf);
  }

  /**
   * Create and start a server with the test webapp
   *
   * @return the newly started server
   *
   * @throws IOException on any failure
   * @throws AssertionError if a condition was not met
   */
  public static HttpServer createAndStartTestServer() throws IOException {
    HttpServer server = createTestServer();
    server.start();
    return server;
  }

  /**
   * If the server is non null, stop it
   * @param server to stop
   * @throws Exception on any failure
   */
  public static void stop(HttpServer server) throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Pass in a server, return a URL bound to localhost and its port
   * @param server server
   * @return a URL bonded to the base of the server
   * @throws MalformedURLException if the URL cannot be created.
   */
  public static URL getServerURL(HttpServer server)
      throws MalformedURLException {
    assertNotNull("No server", server);
    int port = server.getPort();
    return new URL("http://localhost:" + port + "/");
  }

  /**
   * Read in the content from a URL
   * @param url URL To read
   * @return the text from the output
   * @throws IOException if something went wrong
   */
  protected static String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }
}
