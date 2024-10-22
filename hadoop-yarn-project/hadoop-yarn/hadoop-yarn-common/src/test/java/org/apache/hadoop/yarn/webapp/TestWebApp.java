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

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import com.google.inject.Inject;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import org.apache.hadoop.yarn.webapp.view.JQueryUI;
import org.apache.hadoop.yarn.webapp.view.RobotsTextPage;
import org.apache.hadoop.yarn.webapp.view.TextPage;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_TABLE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestWebApp {
  static final Logger LOG = LoggerFactory.getLogger(TestWebApp.class);

  static class FooController extends Controller {
    final TestWebApp test;

    @Inject
    FooController(TestWebApp test) {
      this.test = test;
    }

    @Override public void index() {
      set("key", test.echo("foo"));
    }

    public void bar() {
      set("key", "bar");
    }

    public void names() {
      for (int i = 0; i < 20; ++i) {
        renderText(MockApps.newAppName() + "\n");
      }
    }

    public void ex() {
      boolean err = $("clear").isEmpty();
      renderText(err ? "Should redirect to an error page." : "No error!");
      if (err) {
        throw new RuntimeException("exception test");
      }
    }

    public void tables() {
      render(TablesView.class);
    }
  }

  static class FooView extends TextPage {
    @Override public void render() {
      puts($("key"), $("foo"));
    }
  }

  static class DefaultController extends Controller {
    @Override public void index() {
      set("key", "default");
      render(FooView.class);
    }
  }

  static class TablesView extends HtmlPage {
    @Override
    public void render(Page.HTML<__> html) {
      set(DATATABLES_ID, "t1 t2 t3 t4");
      set(initID(DATATABLES, "t1"), tableInit().append("}").toString());
      set(initID(DATATABLES, "t2"), join("{bJQueryUI:true, sDom:'t',",
          "aoColumns:[null, {bSortable:false, bSearchable:false}]}"));
      set(initID(DATATABLES, "t3"), "{bJQueryUI:true, sDom:'t'}");
      set(initID(DATATABLES, "t4"), "{bJQueryUI:true, sDom:'t'}");
      html.
        title("Test DataTables").
        link("/static/yarn.css").
          __(JQueryUI.class).
        style(".wrapper { padding: 1em }",
              ".wrapper h2 { margin: 0.5em 0 }",
              ".dataTables_wrapper { min-height: 1em }").
        div(".wrapper").
          h2("Default table init").
          table("#t1").
            thead().
              tr().th("Column1").th("Column2").__().__().
            tbody().
              tr().td("c1r1").td("c2r1").__().
              tr().td("c1r2").td("c2r2").__().__().__().
          h2("Nested tables").
          div(_INFO_WRAP).
            table("#t2").
              thead().
                tr().th(_TH, "Column1").th(_TH, "Column2").__().__().
              tbody().
                tr().td("r1"). // th wouldn't work as of dt 1.7.5
                  td().$class(C_TABLE).
                    table("#t3").
                      thead().
                        tr().th("SubColumn1").th("SubColumn2").__().__().
                      tbody().
                        tr().td("subc1r1").td("subc2r1").__().
                        tr().td("subc1r2").td("subc2r2").__().__().__().__().__().
                tr().td("r2"). // ditto
                  td().$class(C_TABLE).
                    table("#t4").
                      thead().
                        tr().th("SubColumn1").th("SubColumn2").__().__().
                      tbody().
                        tr().td("subc1r1").td("subc2r1").__().
                        tr().td("subc1r2").td("subc2r2").__().
          __().__().__().__().__().__().__().__().__();
    }
  }

  String echo(String s) { return s; }

  @Test
  void testCreate() {
    WebApp app = WebApps.$for(this).start();
    app.stop();
  }

  @Test
  void testCreateWithPort() {
    // see if the ephemeral port is updated
    WebApp app = WebApps.$for(this).at(0).start();
    int port = app.getListenerAddress().getPort();
    assertTrue(port > 0);
    app.stop();
    // try to reuse the port
    app = WebApps.$for(this).at(port).start();
    assertEquals(port, app.getListenerAddress().getPort());
    app.stop();
  }

  @Test
  void testCreateWithBindAddressNonZeroPort() {
    assertThrows(org.apache.hadoop.yarn.webapp.WebAppException.class, () -> {
      WebApp app = WebApps.$for(this).at("0.0.0.0:50000").start();
      int port = app.getListenerAddress().getPort();
      assertEquals(50000, port);
      // start another WebApp with same NonZero port
      WebApp app2 = WebApps.$for(this).at("0.0.0.0:50000").start();
      // An exception occurs (findPort disabled)
      app.stop();
      app2.stop();
    });
  }

  @Test
  void testCreateWithNonZeroPort() {
    assertThrows(org.apache.hadoop.yarn.webapp.WebAppException.class, () -> {
      WebApp app = WebApps.$for(this).at(50000).start();
      int port = app.getListenerAddress().getPort();
      assertEquals(50000, port);
      // start another WebApp with same NonZero port
      WebApp app2 = WebApps.$for(this).at(50000).start();
      // An exception occurs (findPort disabled)
      app.stop();
      app2.stop();
    });
  }

  @Test
  void testServePaths() {
    WebApp app = WebApps.$for("test", this).start();
    assertEquals("/test", app.getRedirectPath());
    String[] expectedPaths = {"/test", "/test/*"};
    String[] pathSpecs = app.getServePathSpecs();

    assertEquals(2, pathSpecs.length);
    for (int i = 0; i < expectedPaths.length; i++) {
      assertTrue(ArrayUtils.contains(pathSpecs, expectedPaths[i]));
    }
    app.stop();
  }

  @Test
  void testServePathsNoName() {
    WebApp app = WebApps.$for("", this).start();
    assertEquals("/", app.getRedirectPath());
    String[] expectedPaths = {"/*"};
    String[] pathSpecs = app.getServePathSpecs();

    assertEquals(1, pathSpecs.length);
    for (int i = 0; i < expectedPaths.length; i++) {
      assertTrue(ArrayUtils.contains(pathSpecs, expectedPaths[i]));
    }
    app.stop();
  }

  @Test
  void testDefaultRoutes() throws Exception {
    WebApp app = WebApps.
        $for("test", TestWebApp.class, this, "ws").
        withResourceConfig(configure()).start();
    String baseUrl = baseUrl(app);
    try {
      assertEquals("foo", getContent(baseUrl + "test/foo").trim());
      assertEquals("foo", getContent(baseUrl + "test/foo/index").trim());
      assertEquals("bar", getContent(baseUrl + "test/foo/bar").trim());
      // assertEquals("default", getContent(baseUrl + "test").trim());
      assertEquals("default", getContent(baseUrl + "test/").trim());
      // assertEquals("default", getContent(baseUrl).trim());
    } finally {
      app.stop();
    }
  }

  @Test
  void testCustomRoutes() throws Exception {

    WebApp newWebApp = new WebApp() {
      @Override
      public void setup() {
        route("/:foo", FooController.class);
        route("/bar/foo", FooController.class, "bar");
        route("/foo/:foo", DefaultController.class);
        route("/foo/bar/:foo", DefaultController.class, "index");
      }
    };

    WebApp app = WebApps.$for("test", this)
        .withResourceConfig(configure())
        .start(newWebApp);

    String baseUrl = baseUrl(app);
    try {
      assertEquals("foo", getContent(baseUrl + "test/").trim());
      assertEquals("foo1", getContent(baseUrl + "test/1").trim());
      assertEquals("bar", getContent(baseUrl + "test/bar/foo").trim());
      assertEquals("default", getContent(baseUrl + "test/foo/bar").trim());
      assertEquals("default1", getContent(baseUrl + "test/foo/1").trim());
      assertEquals("default2", getContent(baseUrl + "test/foo/bar/2").trim());
      assertEquals(404, getResponseCode(baseUrl));
      assertEquals(404, getResponseCode(baseUrl + "test/goo"));
      assertEquals(200, getResponseCode(baseUrl + "ws/v1/test"));
      assertTrue(getContent(baseUrl + "ws/v1/test").contains("myInfo"));
    } finally {
      app.stop();
    }
  }

  @Test
  void testEncodedUrl() throws Exception {

    WebApp webApp = new WebApp() {
      @Override
      public void setup() {
        route("/:foo", FooController.class);
      }
    };

    WebApp app = WebApps.$for("test", TestWebApp.class, this, "ws")
        .withResourceConfig(configure()).start(webApp);
    String baseUrl = baseUrl(app);

    try {
      // Test encoded url
      String rawPath = "localhost:8080";
      String encodedUrl = baseUrl + "test/" +
          URLEncoder.encode(rawPath, "UTF-8");
      assertEquals("foo" + rawPath, getContent(encodedUrl).trim());

      rawPath = "@;%$";
      encodedUrl = baseUrl + "test/" +
          URLEncoder.encode(rawPath, "UTF-8");
      assertEquals("foo" + rawPath, getContent(encodedUrl).trim());
    } finally {
      app.stop();
    }
  }

  @Test
  void testRobotsText() throws Exception {

    WebApp newWebApp = new WebApp() {
      @Override
      public void setup() {
      }
    };

    WebApp app = WebApps.$for("test", TestWebApp.class, this, "ws")
        .withResourceConfig(configure()).start(newWebApp);
    String baseUrl = baseUrl(app);
    try {
      //using system line separator here since that is what
      // TextView (via PrintWriter) seems to use.
      String[] robotsTxtOutput = getContent(baseUrl +
          RobotsTextPage.ROBOTS_TXT).trim().split(System.getProperty("line"
          + ".separator"));

      assertEquals(2, robotsTxtOutput.length);
      assertEquals("User-agent: *", robotsTxtOutput[0]);
      assertEquals("Disallow: /", robotsTxtOutput[1]);
    } finally {
      app.stop();
    }
  }

  // This is to test the GuiceFilter should only be applied to webAppContext,
  // not to logContext;
  @Test
  void testYARNWebAppContext() throws Exception {
    // setting up the log context
    System.setProperty("hadoop.log.dir", "/Not/Existing/dir");

    WebApp webApp = new WebApp() {
      @Override
      public void setup() {
        route("/", FooController.class);
      }
    };

    WebApp app = WebApps.$for("test", this)
        .withResourceConfig(configure())
        .start(webApp);
    String baseUrl = baseUrl(app);
    try {
      // Not able to access a non-existing dir, should not redirect to foo.
      assertEquals(404, getResponseCode(baseUrl + "logs"));
      // should be able to redirect to foo.
      assertEquals("foo", getContent(baseUrl+ "test/foo").trim());
    } finally {
      app.stop();
    }
  }

  private static void stopWebApp(WebApp app) {
    if (app != null) {
      app.stop();
    }
  }

  @Test
  void testPortRanges() throws Exception {
    WebApp app = WebApps.$for("test", this).start();
    String baseUrl = baseUrl(app);
    WebApp app1 = null;
    WebApp app2 = null;
    WebApp app3 = null;
    WebApp app4 = null;
    WebApp app5 = null;
    try {
      int port =  ServerSocketUtil.waitForPort(48000, 60);
      assertEquals("foo", getContent(baseUrl + "test/foo").trim());
      app1 = WebApps.$for("test", this).at(port).start();
      assertEquals(port, app1.getListenerAddress().getPort());
      app2 = WebApps.$for("test", this).at("0.0.0.0", port, true).start();
      assertTrue(app2.getListenerAddress().getPort() > port);
      Configuration conf = new Configuration();
      port =  ServerSocketUtil.waitForPort(47000, 60);
      app3 = WebApps.$for("test", this).at(port).withPortRange(conf, "abc").
          start();
      assertEquals(port, app3.getListenerAddress().getPort());
      ServerSocketUtil.waitForPort(46000, 60);
      conf.set("abc", "46000-46500");
      app4 = WebApps.$for("test", this).at(port).withPortRange(conf, "abc").
          start();
      assertEquals(46000, app4.getListenerAddress().getPort());
      app5 = WebApps.$for("test", this).withPortRange(conf, "abc").start();
      assertTrue(app5.getListenerAddress().getPort() > 46000);
    } finally {
      stopWebApp(app);
      stopWebApp(app1);
      stopWebApp(app2);
      stopWebApp(app3);
      stopWebApp(app4);
      stopWebApp(app5);
    }
  }

  static String baseUrl(WebApp app) {
    return "http://localhost:"+ app.port() +"/";
  }

  static String getContent(String url) {
    try {
      StringBuilder out = new StringBuilder();
      InputStream in = new URL(url).openConnection().getInputStream();
      byte[] buffer = new byte[64 * 1024];
      int len = in.read(buffer);
      while (len > 0) {
        out.append(new String(buffer, 0, len));
        len = in.read(buffer);
      }
      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static int getResponseCode(String url) {
    try {
      HttpURLConnection c = (HttpURLConnection)new URL(url).openConnection();
      return c.getResponseCode();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig();
    config.packages("org.apache.hadoop.yarn.webapp");
    config.register(MyTestWebService.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(MyTestJAXBContextResolver.class);
    return config;
  }

 // public static void main(String[] args) throws Exception {
    // For manual controller/view testing.
 //   WebApps.$for("test", new TestWebApp()).at(8888).inDevMode().start().
 //       joinThread();
//        start(new WebApp() {
//          @Override public void setup() {
//            route("/:foo", FooController.class);
//            route("/foo/:foo", FooController.class);
//            route("/bar", FooController.class);
//          }
//        }).join();
 // }
}
