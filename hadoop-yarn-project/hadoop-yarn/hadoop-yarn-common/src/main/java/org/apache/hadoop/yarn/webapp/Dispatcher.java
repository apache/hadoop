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

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import org.apache.hadoop.yarn.webapp.Router.Dest;
import org.apache.hadoop.yarn.webapp.view.ErrorPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

/**
 * The servlet that dispatch request to various controllers
 * according to the user defined routes in the router.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
@Singleton
public class Dispatcher extends HttpServlet {
  private static final long serialVersionUID = 1L;
  static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);
  static final String ERROR_COOKIE = "last-error";
  static final String STATUS_COOKIE = "last-status";

  private transient final Injector injector;
  private transient final Router router;
  private transient final WebApp webApp;
  private volatile boolean devMode = false;

  @Inject
  Dispatcher(WebApp webApp, Injector injector, Router router) {
    this.webApp = webApp;
    this.injector = injector;
    this.router = router;
  }

  @Override
  public void doOptions(HttpServletRequest req, HttpServletResponse res) {
    // for simplicity
    res.setHeader("Allow", "GET, POST");
  }

  @Override
  public void service(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    res.setCharacterEncoding("UTF-8");
    String uri = HtmlQuoting.quoteHtmlChars(req.getRequestURI());

    if (uri == null) {
      uri = "/";
    }
    if (devMode && uri.equals("/__stop")) {
      // quick hack to restart servers in dev mode without OS commands
      res.setStatus(res.SC_NO_CONTENT);
      LOG.info("dev mode restart requested");
      prepareToExit();
      return;
    }
    // if they provide a redirectPath go there instead of going to
    // "/" so that filters can differentiate the webapps.
    if (uri.equals("/")) {
      String redirectPath = webApp.getRedirectPath();
      if (redirectPath != null && !redirectPath.isEmpty()) {
        res.sendRedirect(redirectPath);
        return;
      }
    }
    String method = req.getMethod();
    if (method.equals("OPTIONS")) {
      doOptions(req, res);
      return;
    }
    if (method.equals("TRACE")) {
      doTrace(req, res);
      return;
    }
    if (method.equals("HEAD")) {
      doGet(req, res); // default to bad request
      return;
    }
    String pathInfo = req.getPathInfo();
    if (pathInfo == null) {
      pathInfo = "/";
    }
    // The implementation class of HttpServletRequest in
    // Guice-3.0 does not decode paths that are encoded,
    // decode path info here for further operation.
    try {
      pathInfo = new URI(pathInfo).getPath();
    }  catch (URISyntaxException ex) {
      // Just leave it alone for compatibility.
      LOG.error(pathInfo + ": Failed to decode path.", ex);
    }
    Controller.RequestContext rc =
        injector.getInstance(Controller.RequestContext.class);
    if (setCookieParams(rc, req) > 0) {
      Cookie ec = rc.cookies().get(ERROR_COOKIE);
      if (ec != null) {
        rc.setStatus(Integer.parseInt(rc.cookies().
            get(STATUS_COOKIE).getValue()));
        removeErrorCookies(res, uri);
        rc.set(Params.ERROR_DETAILS, ec.getValue());
        render(ErrorPage.class);
        return;
      }
    }
    rc.prefix = webApp.name();
    Router.Dest dest = null;
    try {
      dest = router.resolve(method, pathInfo);
    } catch (WebAppException e) {
      rc.error = e;
      if (!e.getMessage().contains("not found")) {
        rc.setStatus(res.SC_INTERNAL_SERVER_ERROR);
        render(ErrorPage.class);
        return;
      }
    }
    if (dest == null) {
      rc.setStatus(res.SC_NOT_FOUND);
      render(ErrorPage.class);
      return;
    }
    rc.devMode = devMode;
    setMoreParams(rc, pathInfo, dest);
    Controller controller = injector.getInstance(dest.controllerClass);
    try {
      // TODO: support args converted from /path/:arg1/...
      dest.action.invoke(controller, (Object[]) null);
      if (!rc.rendered) {
        if (dest.defaultViewClass != null) {
          render(dest.defaultViewClass);
        } else if (rc.status == 200) {
          throw new IllegalStateException("No view rendered for 200");
        }
      }
    } catch (Exception e) {
      LOG.error("error handling URI: "+ uri, e);
      // Page could be half rendered (but still not flushed). So redirect.
      redirectToErrorPage(res, e, uri, devMode);
    }
  }

  public static void redirectToErrorPage(HttpServletResponse res, Throwable e,
                                         String path, boolean devMode) {
    String st = devMode ? ErrorPage.toStackTrace(e, 1024 * 3) // spec: min 4KB
                        : "See logs for stack trace";
    res.setStatus(res.SC_FOUND);
    Cookie cookie = new Cookie(STATUS_COOKIE, String.valueOf(500));
    cookie.setPath(path);
    res.addCookie(cookie);
    cookie = new Cookie(ERROR_COOKIE, st);
    cookie.setPath(path);
    res.addCookie(cookie);
    res.setHeader("Location", path);
  }

  public static void removeErrorCookies(HttpServletResponse res, String path) {
    removeCookie(res, ERROR_COOKIE, path);
    removeCookie(res, STATUS_COOKIE, path);
  }

  public static void removeCookie(HttpServletResponse res, String name,
                                  String path) {
    LOG.debug("removing cookie {} on {}", name, path);
    Cookie c = new Cookie(name, "");
    c.setMaxAge(0);
    c.setPath(path);
    res.addCookie(c);
  }

  private void render(Class<? extends View> cls) {
    injector.getInstance(cls).render();
  }

  // /path/foo/bar with /path/:arg1/:arg2 will set {arg1=>foo, arg2=>bar}
  private void setMoreParams(RequestContext rc, String pathInfo, Dest dest) {
    checkState(pathInfo.startsWith(dest.prefix), "prefix should match");
    if (dest.pathParams.size() == 0 ||
        dest.prefix.length() == pathInfo.length()) {
      return;
    }
    String[] parts = Iterables.toArray(WebApp.pathSplitter.split(
        pathInfo.substring(dest.prefix.length())), String.class);
    LOG.debug("parts={}, params={}", parts, dest.pathParams);
    for (int i = 0; i < dest.pathParams.size() && i < parts.length; ++i) {
      String key = dest.pathParams.get(i);
      if (key.charAt(0) == ':') {
        rc.moreParams().put(key.substring(1), parts[i]);
      }
    }
  }

  private int setCookieParams(RequestContext rc, HttpServletRequest req) {
    Cookie[] cookies = req.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        rc.cookies().put(cookie.getName(), cookie);
      }
      return cookies.length;
    }
    return 0;
  }

  public void setDevMode(boolean choice) {
    devMode = choice;
  }

  private void prepareToExit() {
    checkState(devMode, "only in dev mode");
    new Timer("webapp exit", true).schedule(new TimerTask() {
      @Override public void run() {
        LOG.info("WebAppp /{} exiting...", webApp.name());
        webApp.stop();
        System.exit(0); // FINDBUG: this is intended in dev mode
      }
    }, 18); // enough time for the last local request to complete
  }
}
