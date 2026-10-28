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

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Assertions on the test classpath of a module, so a module can check that
 * it resolves one servlet API and one Jetty release (HADOOP-19970).
 * Jars are recognised by their contents and manifests, not by their file
 * names or repository layout, so the checks hold for a Maven repository, a
 * Gradle cache or a flattened lib directory alike.
 */
public final class ClasspathTestUtils {

  /** Bundle-SymbolicName of the servlet API every module resolves. */
  public static final String SERVLET_API_BUNDLE =
      "org.eclipse.jetty.servlet-api";

  /**
   * Where Jetty's classes live. A jar with classes here is a Jetty jar of
   * whatever group (http2, websocket, alpn, ...); jetty-servlet-api and the
   * orbit jars carry only javax.* classes and are not counted.
   */
  private static final String JETTY_CLASSES = "org/eclipse/jetty/";

  private static final Attributes.Name BUNDLE_SYMBOLIC_NAME =
      new Attributes.Name("Bundle-SymbolicName");

  private static final Attributes.Name BUNDLE_VERSION =
      new Attributes.Name("Bundle-Version");

  private ClasspathTestUtils() {
  }

  /**
   * Assert that exactly one classpath entry provides a resource,
   * and that the entry is the expected bundle.
   * @param resource resource name, such as "javax/servlet/Servlet.class".
   * @param bundle the Bundle-SymbolicName of the jar that should provide it.
   * @throws IOException if the classpath cannot be read.
   */
  public static void assertSingleProvider(String resource, String bundle)
      throws IOException {
    List<URL> urls = Collections.list(
        ClasspathTestUtils.class.getClassLoader().getResources(resource));
    assertEquals(1, urls.size(), "providers of " + resource + ": " + urls);
    JarFile jar = jarOf(urls.get(0));
    assertNotNull(jar, resource + " is not in a jar: " + urls.get(0));
    assertEquals(bundle, mainAttribute(jar, BUNDLE_SYMBOLIC_NAME),
        "unexpected provider of " + resource + ": " + urls.get(0));
  }

  /**
   * Assert that one servlet API, jetty-servlet-api, is on the classpath.
   * @throws IOException if the classpath cannot be read.
   */
  public static void assertSingleServletApi() throws IOException {
    assertSingleProvider("javax/servlet/Servlet.class", SERVLET_API_BUNDLE);
  }

  /**
   * Assert that the classpath carries Jetty jars, all from one release.
   * A jar is a Jetty jar when it has classes under org/eclipse/jetty/,
   * and its release is the Implementation-Version (or else Bundle-Version)
   * of its manifest.
   * @return the Jetty jars found, as "file name (release)".
   * @throws IOException if the classpath cannot be read.
   */
  public static Set<String> assertSingleJettyRelease() throws IOException {
    Set<String> jars = new TreeSet<>();
    Set<String> versions = new TreeSet<>();
    for (URL url : Collections.list(ClasspathTestUtils.class.getClassLoader()
        .getResources(JarFile.MANIFEST_NAME))) {
      JarFile jar = jarOf(url);
      if (jar == null || jar.stream().noneMatch(e ->
          e.getName().startsWith(JETTY_CLASSES)
              && e.getName().endsWith(".class"))) {
        continue;
      }
      String version = mainAttribute(jar, Attributes.Name.IMPLEMENTATION_VERSION);
      if (version == null) {
        version = mainAttribute(jar, BUNDLE_VERSION);
      }
      jars.add(new File(jar.getName()).getName() + " (" + version + ")");
      versions.add(String.valueOf(version));
    }
    assertFalse(jars.isEmpty(), "no Jetty jars found on the classpath");
    assertEquals(1, versions.size(), "Jetty jars on the classpath: " + jars);
    return jars;
  }

  /**
   * The jar a classpath resource comes from.
   * @param resource URL of a resource found through the class loader.
   * @return the jar, shared through the JVM's jar cache, so it must not be
   * closed; or null if the resource is in a directory.
   * @throws IOException if the jar cannot be opened.
   */
  private static JarFile jarOf(URL resource) throws IOException {
    URLConnection connection = resource.openConnection();
    return connection instanceof JarURLConnection
        ? ((JarURLConnection) connection).getJarFile() : null;
  }

  private static String mainAttribute(JarFile jar, Attributes.Name name)
      throws IOException {
    Manifest manifest = jar.getManifest();
    return manifest == null ? null : manifest.getMainAttributes().getValue(name);
  }
}
