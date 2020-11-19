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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.yarn.webapp.view.RobotsTextPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import com.google.inject.Provides;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.util.FeaturesAndProperties;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * @see WebApps for a usage example
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class WebApp extends ServletModule {
  private static final Logger LOG = LoggerFactory.getLogger(WebApp.class);

  public enum HTTP { GET, POST, HEAD, PUT, DELETE };

  private volatile String name;
  private volatile List<String> servePathSpecs = new ArrayList<String>();
  // path to redirect to
  private volatile String redirectPath;
  private volatile String wsName;
  private volatile Configuration conf;
  private volatile HttpServer2 httpServer;
  private volatile GuiceFilter guiceFilter;
  private final Router router = new Router();

  // index for the parsed route result
  static final int R_PATH = 0;
  static final int R_CONTROLLER = 1;
  static final int R_ACTION = 2;
  static final int R_PARAMS = 3;

  static final Splitter pathSplitter =
      Splitter.on('/').trimResults().omitEmptyStrings();

  void setHttpServer(HttpServer2 server) {
    httpServer = checkNotNull(server, "http server");
  }

  @Provides public HttpServer2 httpServer() { return httpServer; }

  /**
   * Get the address the http server is bound to
   * @return InetSocketAddress
   */
  public InetSocketAddress getListenerAddress() {
    return checkNotNull(httpServer, "httpServer").getConnectorAddress(0);
  }
	
  public int port() {
    InetSocketAddress addr = checkNotNull(httpServer, "httpServer")
        .getConnectorAddress(0);
    return addr == null ? -1 : addr.getPort();
  }

  public void stop() {
    try {
      checkNotNull(httpServer, "httpServer").stop();
      checkNotNull(guiceFilter, "guiceFilter").destroy();
    }
    catch (Exception e) {
      throw new WebAppException(e);
    }
  }

  public void joinThread() {
    try {
      checkNotNull(httpServer, "httpServer").join();
    } catch (InterruptedException e) {
      LOG.info("interrupted", e);
    }
  }

  void setConf(Configuration conf) { this.conf = conf; }

  @Provides public Configuration conf() { return conf; }

  @Provides Router router() { return router; }

  @Provides WebApp webApp() { return this; }

  void setName(String name) { this.name = name; }

  public String name() { return this.name; }

  public String wsName() {
    return this.wsName;
  }

  void addServePathSpec(String path) { this.servePathSpecs.add(path); }

  public String[] getServePathSpecs() {
    return this.servePathSpecs.toArray(new String[this.servePathSpecs.size()]);
  }

  /**
   * Set a path to redirect the user to if they just go to "/". For
   * instance "/" goes to "/yarn/apps". This allows the filters to
   * more easily differentiate the different webapps.
   * @param path  the path to redirect to
   */
  void setRedirectPath(String path) {
    this.redirectPath = path;
  }

  void setWebServices (String name) { this.wsName = name; }

  public String getRedirectPath() { return this.redirectPath; }

  void setHostClass(Class<?> cls) {
    router.setHostClass(cls);
  }

  void setGuiceFilter(GuiceFilter instance) {
    guiceFilter = instance;
  }

  @Override
  public void configureServlets() {
    setup();

    serve("/", "/__stop", RobotsTextPage.ROBOTS_TXT_PATH)
        .with(Dispatcher.class);

    for (String path : this.servePathSpecs) {
      serve(path).with(Dispatcher.class);
    }

    configureWebAppServlets();
  }

  protected void configureWebAppServlets() {
    // Add in the web services filters/serves if app has them.
    // Using Jersey/guice integration module. If user has web services
    // they must have also bound a default one in their webapp code.
    if (this.wsName != null) {
      // There seems to be an issue with the guice/jersey integration
      // where we have to list the stuff we don't want it to serve
      // through the guicecontainer. In this case its everything except
      // the the web services api prefix. We can't just change the filter
      // from /* below - that doesn't work.
      String regex = "(?!/" + this.wsName + ")";
      serveRegex(regex).with(DefaultWrapperServlet.class);

      Map<String, String> params = new HashMap<String, String>();
      params.put(ResourceConfig.FEATURE_IMPLICIT_VIEWABLES, "true");
      params.put(ServletContainer.FEATURE_FILTER_FORWARD_ON_404, "true");
      params.put(FeaturesAndProperties.FEATURE_XMLROOTELEMENT_PROCESSING, "true");
      params.put(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName());
      params.put(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, GZIPContentEncodingFilter.class.getName());
      filter("/*").through(getWebAppFilterClass(), params);
    }
  }

  protected Class<? extends GuiceContainer> getWebAppFilterClass() {
    return GuiceContainer.class;
  }

  /**
   * Setup of a webapp serving route.
   * @param method  the http method for the route
   * @param pathSpec  the path spec in the form of /controller/action/:args etc.
   * @param cls the controller class
   * @param action the controller method
   */
  public void route(HTTP method, String pathSpec,
                    Class<? extends Controller> cls, String action) {
    List<String> res = parseRoute(pathSpec);
    router.add(method, res.get(R_PATH), cls, action,
               res.subList(R_PARAMS, res.size()));
  }

  /**
   * Setup of a webapp serving route without default views added to the page.
   * @param pathSpec  the path spec in the form of /controller/action/:args etc.
   * @param cls the controller class
   * @param action the controller method
   */
  public void routeWithoutDefaultView(String pathSpec,
                    Class<? extends Controller> cls, String action) {
    List<String> res = parseRoute(pathSpec);
    router.addWithoutDefaultView(HTTP.GET, res.get(R_PATH), cls, action,
        res.subList(R_PARAMS, res.size()));
  }

  public void route(String pathSpec, Class<? extends Controller> cls,
                    String action) {
    route(HTTP.GET, pathSpec, cls, action);
  }

  public void route(String pathSpec, Class<? extends Controller> cls) {
    List<String> res = parseRoute(pathSpec);
    router.add(HTTP.GET, res.get(R_PATH), cls, res.get(R_ACTION),
               res.subList(R_PARAMS, res.size()));
  }


  /**
   * /controller/action/:args => [/controller/action, controller, action, args]
   * /controller/:args => [/controller, controller, index, args]
   */
  static List<String> parseRoute(String pathSpec) {
    List<String> result = Lists.newArrayList();
    result.add(getPrefix(checkNotNull(pathSpec, "pathSpec")));
    Iterable<String> parts = pathSplitter.split(pathSpec);
    String controller = null, action = null;
    for (String s : parts) {
      if (controller == null) {
        if (s.charAt(0) == ':') {
          controller = "default";
          result.add(controller);
          action = "index";
          result.add(action);
        } else {
          controller = s;
        }
      } else if (action == null) {
        if (s.charAt(0) == ':') {
          action = "index";
          result.add(action);
        } else {
          action = s;
        }
      }
      result.add(s);
    }
    if (controller == null) {
      result.add("default");
    }
    if (action == null) {
      result.add("index");
    }
    return result;
  }

  static String getPrefix(String pathSpec) {
    int start = 0;
    while (StringUtils.isAnyBlank(Character.toString(pathSpec.charAt(start)))) {
      ++start;
    }
    if (pathSpec.charAt(start) != '/') {
      throw new WebAppException("Path spec syntax error: "+ pathSpec);
    }
    int ci = pathSpec.indexOf(':');
    if (ci == -1) {
      ci = pathSpec.length();
    }
    if (ci == 1) {
      return "/";
    }
    char c;
    do {
      c = pathSpec.charAt(--ci);
    } while (c == '/' || StringUtils.isAnyBlank(Character.toString(c)));
    return pathSpec.substring(start, ci + 1);
  }

  public abstract void setup();

}
