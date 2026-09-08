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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Guards the contract Hadoop offers the projects that embed it - HBase, Hive,
 * Spark, Ozone, Knox and the rest - across a Jetty upgrade.
 *
 * Those projects implement Hadoop's servlet-facing extension points and embed
 * HttpServer2, and they do it against javax.servlet. Two things have to stay
 * true for them, and neither is checked by anything else in the tree:
 *
 *  - the extension points keep speaking javax.servlet, never jakarta.servlet,
 *  - Jetty stays out of the signatures, so nobody downstream has to compile
 *    against the container Hadoop happens to embed, or care which one it is.
 *
 * The reflective half of this test would have failed had the Jetty 12 port let
 * an org.eclipse.jetty type into a public signature, and will fail on the day
 * the jakarta rename reaches one, which is the point: that day belongs to a
 * major release and should not arrive by accident.
 */
public class TestDownstreamServletCompatibility
    extends HttpServerFunctionalTest {

  /**
   * The servlet-facing surface a downstream project compiles against. Types
   * are named rather than discovered so that dropping one from the list is a
   * deliberate edit and shows up in review.
   */
  private static final Class<?>[] PUBLIC_SURFACE = {
      org.apache.hadoop.security.authentication.server.AuthenticationHandler.class,
      org.apache.hadoop.security.authentication.server.AuthenticationFilter.class,
      org.apache.hadoop.security.authentication.server.AuthenticationToken.class,
      org.apache.hadoop.security.authentication.client.Authenticator.class,
      org.apache.hadoop.security.http.RestCsrfPreventionFilter.class,
      org.apache.hadoop.security.http.XFrameOptionsFilter.class,
      org.apache.hadoop.http.FilterContainer.class,
      org.apache.hadoop.http.FilterInitializer.class,
      org.apache.hadoop.http.HttpServer2.class,
  };

  private static final String JAKARTA_SERVLET = "jakarta.servlet";
  private static final String JETTY = "org.eclipse.jetty";

  /**
   * How many of PUBLIC_SURFACE are not InterfaceAudience.Private, and so are
   * held to the no-Jetty rule. Asserted as an exact count so that marking one
   * of them Private has to be a deliberate edit here too.
   */
  private static final int EXPECTED_JETTY_FREE_CLASSES = 7;

  /**
   * Nothing on that surface may name a jakarta.servlet type. This is the
   * promise the ee8 environment exists to keep, and the one that would break
   * every downstream implementation at once.
   */
  @Test
  public void testPublicSurfaceNeverNamesJakartaServlet() {
    List<String> offences = new ArrayList<>();
    int javaxReferences = 0;
    for (Class<?> clazz : PUBLIC_SURFACE) {
      for (String type : referencedTypes(clazz)) {
        if (type.startsWith(JAKARTA_SERVLET)) {
          offences.add(clazz.getName() + " exposes " + type
              + "; downstream code is written against javax.servlet");
        }
        if (type.startsWith("javax.servlet")) {
          javaxReferences++;
        }
      }
    }
    // Without this the test would also pass if referencedTypes stopped
    // reporting servlet types at all, which is the failure it exists to catch.
    assertTrue(javaxReferences > 0,
        "no javax.servlet type was found anywhere on the surface; the scan is"
            + " not reading signatures and this test proves nothing");
    assertTrue(offences.isEmpty(),
        "public API has moved to the jakarta namespace:\n  "
            + String.join("\n  ", offences));
  }

  /**
   * The classes downstream projects are expected to reach for on that surface
   * - everything not marked InterfaceAudience.Private - may not name a Jetty
   * type, so that nobody downstream has to compile against the container
   * Hadoop embeds, or track which one it is.
   * <p>
   * Absence of an annotation is not a licence to leak. Several of these are
   * the oldest classes in hadoop-auth and carry no audience marker at all,
   * which does not make them less public in practice; only an explicit
   * Private marker excuses a class, and only two here have one -
   * AuthenticationFilter, and HttpServer2, which names Jetty deliberately
   * (addHandlerAtFront takes a Handler) and is documented as internal.
   */
  @Test
  public void testPublicApiNeverNamesJetty() {
    List<String> offences = new ArrayList<>();
    int checked = 0;
    for (Class<?> clazz : PUBLIC_SURFACE) {
      if (clazz.getAnnotation(InterfaceAudience.Private.class) != null) {
        continue;
      }
      checked++;
      for (String type : referencedTypes(clazz)) {
        if (type.startsWith(JETTY)) {
          offences.add(clazz.getName() + " exposes " + type
              + "; the embedded container must not reach a public signature");
        }
      }
    }
    // A count, not a floor: marking one of these Private would shrink the
    // guarded set silently, and that is a decision for review, not a
    // side effect.
    assertEquals(EXPECTED_JETTY_FREE_CLASSES, checked,
        "the set of classes guarded against naming Jetty has changed; if that"
            + " is deliberate, update EXPECTED_JETTY_FREE_CLASSES to match");
    assertTrue(offences.isEmpty(),
        "public API leaks the servlet container:\n  "
            + String.join("\n  ", offences));
  }

  /**
   * Every type named by a public or protected member of the class, and of the
   * nested classes it publishes. HttpServer2.Builder is the reason for the
   * second half: it is how a downstream project actually configures an
   * embedded server, and scanning only the outer class would miss it.
   */
  private static Set<String> referencedTypes(Class<?> clazz) {
    Set<String> types = declaredTypes(clazz);
    for (Class<?> nested : clazz.getDeclaredClasses()) {
      if (isVisibleDownstream(nested.getModifiers())) {
        types.addAll(declaredTypes(nested));
      }
    }
    return types;
  }

  /** Every type named by a public or protected member declared on the class. */
  private static Set<String> declaredTypes(Class<?> clazz) {
    Set<String> types = new LinkedHashSet<>();
    for (Method m : clazz.getDeclaredMethods()) {
      if (!isVisibleDownstream(m.getModifiers())) {
        continue;
      }
      collect(types, m.getGenericReturnType());
      for (Type t : m.getGenericParameterTypes()) {
        collect(types, t);
      }
      for (Class<?> t : m.getExceptionTypes()) {
        collect(types, t);
      }
    }
    for (Constructor<?> c : clazz.getDeclaredConstructors()) {
      if (!isVisibleDownstream(c.getModifiers())) {
        continue;
      }
      for (Type t : c.getGenericParameterTypes()) {
        collect(types, t);
      }
    }
    for (Field f : clazz.getDeclaredFields()) {
      if (isVisibleDownstream(f.getModifiers())) {
        collect(types, f.getGenericType());
      }
    }
    return types;
  }

  private static boolean isVisibleDownstream(int modifiers) {
    return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
  }

  private static void collect(Set<String> into, Type type) {
    if (type == null) {
      return;
    }
    if (type instanceof Class<?>) {
      Class<?> c = (Class<?>) type;
      while (c.isArray()) {
        c = c.getComponentType();
      }
      if (!c.isPrimitive()) {
        into.add(c.getName());
      }
      return;
    }
    if (type instanceof java.lang.reflect.ParameterizedType) {
      java.lang.reflect.ParameterizedType p =
          (java.lang.reflect.ParameterizedType) type;
      collect(into, p.getRawType());
      for (Type arg : p.getActualTypeArguments()) {
        collect(into, arg);
      }
      return;
    }
    if (type instanceof java.lang.reflect.GenericArrayType) {
      collect(into,
          ((java.lang.reflect.GenericArrayType) type).getGenericComponentType());
    }
  }

  /**
   * An AuthenticationHandler written the way a downstream project writes one:
   * javax.servlet types, no Hadoop-internal or Jetty types anywhere. That this
   * class compiles at all is most of the assertion.
   */
  public static class DownstreamAuthenticationHandler
      implements AuthenticationHandler {

    static final String TYPE = "downstream";
    private boolean initialised;

    @Override
    public String getType() {
      return TYPE;
    }

    @Override
    public void init(Properties config) throws ServletException {
      initialised = true;
    }

    @Override
    public void destroy() {
      initialised = false;
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
        HttpServletRequest request, HttpServletResponse response) {
      return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
        HttpServletResponse response) {
      if (!initialised) {
        return null;
      }
      return new AuthenticationToken(request.getParameter("user"), "p", TYPE);
    }
  }

  /**
   * The extension point still takes and returns javax.servlet types at run
   * time, not only at compile time.
   */
  @Test
  public void testAuthenticationHandlerStillTakesJavaxServlet()
      throws Exception {
    AuthenticationHandler handler = new DownstreamAuthenticationHandler();
    handler.init(new Properties());
    try {
      HttpServletRequest request = mock(HttpServletRequest.class);
      HttpServletResponse response = mock(HttpServletResponse.class);
      when(request.getParameter("user")).thenReturn("alice");

      AuthenticationToken token = handler.authenticate(request, response);
      assertNotNull(token, "handler returned no token");
      assertEquals("alice", token.getUserName());
      assertEquals(DownstreamAuthenticationHandler.TYPE, token.getType());
      assertTrue(handler.managementOperation(token, request, response));
    } finally {
      handler.destroy();
    }
  }

  /** A servlet a downstream project would write: javax.servlet and nothing else. */
  public static class DownstreamServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request,
        HttpServletResponse response) throws IOException {
      response.setContentType("text/plain; charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().print("downstream-ok");
    }
  }

  /** Likewise a plain javax.servlet Filter. */
  public static class DownstreamFilter implements Filter {
    static final AtomicBoolean RAN = new AtomicBoolean(false);

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      RAN.set(true);
      chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
  }

  /**
   * A downstream servlet and filter, registered through the public HttpServer2
   * API, still serve a request on the embedded container.
   */
  @Test
  public void testDownstreamServletAndFilterStillServeRequests()
      throws Exception {
    DownstreamFilter.RAN.set(false);
    HttpServer2 server = createTestServer();
    try {
      server.addServlet("downstream", "/downstream", DownstreamServlet.class);
      server.addFilter("downstream-filter",
          DownstreamFilter.class.getName(), null);
      server.start();

      URL url = new URL(getServerURL(server), "/downstream");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();

      assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
      assertEquals("downstream-ok", body(conn));
      assertTrue(DownstreamFilter.RAN.get(),
          "a downstream filter registered through addFilter did not run");
    } finally {
      stop(server);
    }
  }

  private static String body(HttpURLConnection conn) throws IOException {
    try (InputStream in = conn.getInputStream();
         Scanner scanner = new Scanner(in, StandardCharsets.UTF_8.name())) {
      scanner.useDelimiter("\\A");
      return scanner.hasNext() ? scanner.next().trim() : "";
    }
  }

  /**
   * The servlet API a downstream project resolves alongside Hadoop is the
   * javax one, and it is loadable from Hadoop's own classpath. Guards against
   * an upgrade that quietly swaps the namespace out from under an embedder.
   */
  @Test
  public void testServletApiOnTheClasspathIsJavax() throws Exception {
    Class<?> servlet = Class.forName("javax.servlet.Servlet");
    assertEquals("javax.servlet.Servlet", servlet.getName());
    assertTrue(servlet.isInterface());

    // HttpServer2 is the embedding entry point; what it hands a downstream
    // servlet has to be the same javax.servlet.Servlet loaded here.
    assertTrue(servlet.isAssignableFrom(DownstreamServlet.class),
        "HttpServlet no longer implements the javax.servlet.Servlet on"
            + " the classpath");

    List<String> jakarta = new ArrayList<>();
    for (String name : Arrays.asList("jakarta.servlet.Servlet",
        "jakarta.servlet.http.HttpServlet")) {
      try {
        Class.forName(name);
        jakarta.add(name);
      } catch (ClassNotFoundException expected) {
        // the jakarta namespace must not be resolvable beside the javax one
      }
    }
    assertTrue(jakarta.isEmpty(),
        "both servlet namespaces are on the classpath: " + jakarta);
  }
}
