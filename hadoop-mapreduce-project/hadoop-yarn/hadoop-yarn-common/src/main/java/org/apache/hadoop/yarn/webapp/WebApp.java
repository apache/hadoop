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

import com.google.common.base.CharMatcher;
import static com.google.common.base.Preconditions.*;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.inject.Provides;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.yarn.util.StringHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see WebApps for a usage example
 */
public abstract class WebApp extends ServletModule {
  private static final Logger LOG = LoggerFactory.getLogger(WebApp.class);

  public enum HTTP { GET, POST, HEAD, PUT, DELETE };

  private volatile String name;
  private volatile List<String> servePathSpecs = new ArrayList<String>(); 
  // path to redirect to if user goes to "/"
  private volatile String redirectPath;
  private volatile Configuration conf;
  private volatile HttpServer httpServer;
  private volatile GuiceFilter guiceFilter;
  private final Router router = new Router();

  // index for the parsed route result
  static final int R_PATH = 0;
  static final int R_CONTROLLER = 1;
  static final int R_ACTION = 2;
  static final int R_PARAMS = 3;

  static final Splitter pathSplitter =
      Splitter.on('/').trimResults().omitEmptyStrings();

  void setHttpServer(HttpServer server) {
    httpServer = checkNotNull(server, "http server");
  }

  @Provides public HttpServer httpServer() { return httpServer; }

  public int port() {
    return checkNotNull(httpServer, "httpServer").getPort();
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
  void setRedirectPath(String path) { this.redirectPath = path; }

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
    serve("/", "/__stop").with(Dispatcher.class);
    for (String path : this.servePathSpecs) {
      serve(path).with(Dispatcher.class);
    }
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
    while (CharMatcher.WHITESPACE.matches(pathSpec.charAt(start))) {
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
    } while (c == '/' || CharMatcher.WHITESPACE.matches(c));
    return pathSpec.substring(start, ci + 1);
  }

  public abstract void setup();
}
