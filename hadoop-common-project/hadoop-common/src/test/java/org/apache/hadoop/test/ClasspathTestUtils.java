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
package org.apache.hadoop.test;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Assertions on the test classpath of a module, so a module can check that
 * it resolves one servlet API and one Jetty release (HADOOP-19970).
 */
public final class ClasspathTestUtils {

  /** The servlet API every module resolves. */
  public static final String SERVLET_API_JAR = "/jetty-servlet-api-";

  /**
   * Jetty jars by their Maven repository path,
   * .../org/eclipse/jetty/[websocket/]artifact/version/. Toolchain
   * artifacts such as jetty-servlet-api have their own versions and
   * do not match.
   */
  private static final Pattern JETTY_JAR = Pattern.compile(
      "/org/eclipse/jetty/(?:websocket/)?([^/]+)/([^/]+)/[^/]+\\.jar!");

  private ClasspathTestUtils() {
  }

  /**
   * Assert that exactly one classpath entry provides a resource,
   * and that the entry is the expected jar.
   * @param resource resource name, such as "javax/servlet/Servlet.class".
   * @param jar a fragment of the URL of the jar that should provide it.
   * @throws IOException if the classpath cannot be read.
   */
  public static void assertSingleProvider(String resource, String jar)
      throws IOException {
    List<URL> urls = Collections.list(
        ClasspathTestUtils.class.getClassLoader().getResources(resource));
    assertEquals(1, urls.size(), "providers of " + resource + ": " + urls);
    assertTrue(urls.get(0).toString().contains(jar),
        "unexpected provider of " + resource + ": " + urls.get(0));
  }

  /**
   * Assert that one servlet API, jetty-servlet-api, is on the classpath.
   * @throws IOException if the classpath cannot be read.
   */
  public static void assertSingleServletApi() throws IOException {
    assertSingleProvider("javax/servlet/Servlet.class", SERVLET_API_JAR);
  }

  /**
   * Assert that the classpath carries Jetty jars, all from one release.
   * @return the Jetty jars found, as artifact-version.
   * @throws IOException if the classpath cannot be read.
   */
  public static Set<String> assertSingleJettyRelease() throws IOException {
    Set<String> jars = new TreeSet<>();
    Set<String> versions = new TreeSet<>();
    for (URL url : Collections.list(ClasspathTestUtils.class.getClassLoader()
        .getResources("META-INF/MANIFEST.MF"))) {
      Matcher m = JETTY_JAR.matcher(url.toString());
      if (m.find()) {
        jars.add(m.group(1) + "-" + m.group(2));
        versions.add(m.group(2));
      }
    }
    assertFalse(jars.isEmpty(), "no Jetty jars found on the classpath");
    assertEquals(1, versions.size(), "Jetty jars on the classpath: " + jars);
    return jars;
  }
}
