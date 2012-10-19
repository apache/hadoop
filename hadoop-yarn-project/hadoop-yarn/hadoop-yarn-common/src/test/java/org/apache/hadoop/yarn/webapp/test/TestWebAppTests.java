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

package org.apache.hadoop.yarn.webapp.test;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.servlet.RequestScoped;
import java.io.PrintWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class TestWebAppTests {
  static final Logger LOG = LoggerFactory.getLogger(TestWebAppTests.class);

  @Test public void testInstances() throws Exception {
    Injector injector = WebAppTests.createMockInjector(this);
    HttpServletRequest req = injector.getInstance(HttpServletRequest.class);
    HttpServletResponse res = injector.getInstance(HttpServletResponse.class);
    String val = req.getParameter("foo");
    PrintWriter out = res.getWriter();
    out.println("Hello world!");
    logInstances(req, res, out);

    assertSame(req, injector.getInstance(HttpServletRequest.class));
    assertSame(res, injector.getInstance(HttpServletResponse.class));
    assertSame(this, injector.getInstance(TestWebAppTests.class));

    verify(req).getParameter("foo");
    verify(res).getWriter();
    verify(out).println("Hello world!");
  }

  interface Foo {
  }

  static class Bar implements Foo {
  }

  static class FooBar extends Bar {
  }

  @Test public void testCreateInjector() throws Exception {
    Bar bar = new Bar();
    Injector injector = WebAppTests.createMockInjector(Foo.class, bar);
    logInstances(injector.getInstance(HttpServletRequest.class),
                 injector.getInstance(HttpServletResponse.class),
                 injector.getInstance(HttpServletResponse.class).getWriter());
    assertSame(bar, injector.getInstance(Foo.class));
  }

  @Test public void testCreateInjector2() {
    final FooBar foobar = new FooBar();
    Bar bar = new Bar();
    Injector injector = WebAppTests.createMockInjector(Foo.class, bar,
        new AbstractModule() {
      @Override protected void configure() {
        bind(Bar.class).toInstance(foobar);
      }
    });
    assertNotSame(bar, injector.getInstance(Bar.class));
    assertSame(foobar, injector.getInstance(Bar.class));
  }

  @RequestScoped
  static class ScopeTest {
  }

  @Test public void testRequestScope() {
    Injector injector = WebAppTests.createMockInjector(this);

    assertSame(injector.getInstance(ScopeTest.class),
               injector.getInstance(ScopeTest.class));
  }

  private void logInstances(HttpServletRequest req, HttpServletResponse res,
                            PrintWriter out) {
    LOG.info("request: {}", req);
    LOG.info("response: {}", res);
    LOG.info("writer: {}", out);
  }
}
