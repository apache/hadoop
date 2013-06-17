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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.RequestScoped;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.apache.hadoop.yarn.util.StringHelper.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all views
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class View implements Params {
  public static final Logger LOG = LoggerFactory.getLogger(View.class);

  @RequestScoped
  public static class ViewContext {
    final Controller.RequestContext rc;
    int nestLevel = 0;
    boolean wasInline;

    @Inject ViewContext(Controller.RequestContext ctx) {
      rc = ctx;
    }

    public int nestLevel() { return nestLevel; }
    public boolean wasInline() { return wasInline; }

    public void set(int nestLevel, boolean wasInline) {
      this.nestLevel = nestLevel;
      this.wasInline = wasInline;
    }

    public Controller.RequestContext requestContext() { return rc; }
  }

  private ViewContext vc;
  @Inject Injector injector;

  public View() {
    // Makes injection in subclasses optional.
    // Time will tell if this buy us more than the NPEs :)
  }

  public View(ViewContext ctx) {
    vc = ctx;
  }

  /**
   * The API to render the view
   */
  public abstract void render();

  public ViewContext context() {
    if (vc == null) {
      if (injector == null) {
        // One downside of making the injection in subclasses optional
        throw new WebAppException(join("Error accessing ViewContext from a\n",
            "child constructor, either move the usage of the View methods\n",
            "out of the constructor or inject the ViewContext into the\n",
            "constructor"));
      }
      vc = injector.getInstance(ViewContext.class);
    }
    return vc;
  }

  public Throwable error() { return context().rc.error; }

  public int status() { return context().rc.status; }

  public boolean inDevMode() { return context().rc.devMode; }

  public Injector injector() { return context().rc.injector; }

  public <T> T getInstance(Class<T> cls) {
    return injector().getInstance(cls);
  }

  public HttpServletRequest request() {
    return context().rc.request;
  }

  public HttpServletResponse response() {
    return context().rc.response;
  }

  public Map<String, String> moreParams() {
    return context().rc.moreParams();
  }

  /**
   * Get the cookies
   * @return the cookies map
   */
  public Map<String, Cookie> cookies() {
    return context().rc.cookies();
  }

  public ServletOutputStream outputStream() {
    try {
      return response().getOutputStream();
    } catch (IOException e) {
      throw new WebAppException(e);
    }
  }

  public PrintWriter writer() {
    try {
      return response().getWriter();
    } catch (IOException e) {
      throw new WebAppException(e);
    }
  }

  /**
   * Lookup a value from the current context.
   * @param key to lookup
   * @param defaultValue if key is missing
   * @return the value of the key or the default value
   */
  public String $(String key, String defaultValue) {
    // moreParams take precedence
    String value = moreParams().get(key);
    if (value == null) {
      value = request().getParameter(key);
    }
    return value == null ? defaultValue : value;
  }

  /**
   * Lookup a value from the current context
   * @param key to lookup
   * @return the value of the key or empty string
   */
  public String $(String key) {
    return $(key, "");
  }

  /**
   * Set a context value. (e.g. UI properties for sub views.)
   * Try to avoid any application (vs view/ui) logic.
   * @param key to set
   * @param value to set
   */
  public void set(String key, String value) {
    moreParams().put(key, value);
  }

  public String root() {
    String root = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
    if(root == null || root.isEmpty()) {
      root = "/";
    }
    return root;
  }
  
  public String prefix() {
    if(context().rc.prefix == null) {
      return root();
    } else {
      return ujoin(root(), context().rc.prefix);
    }
  }

  public void setTitle(String title) {
    set(TITLE, title);
  }

  public void setTitle(String title, String url) {
    setTitle(title);
    set(TITLE_LINK, url);
  }

  /**
   * Create an url from url components
   * @param parts components to join
   * @return an url string
   */
  public String root_url(String... parts) {
    return ujoin(root(), parts);
  }

  
  /**
   * Create an url from url components
   * @param parts components to join
   * @return an url string
   */
  public String url(String... parts) {
    return ujoin(prefix(), parts);
  }

  public ResponseInfo info(String about) {
    return getInstance(ResponseInfo.class).about(about);
  }

  /**
   * Render a sub-view
   * @param cls the class of the sub-view
   */
  public void render(Class<? extends SubView> cls) {
    int saved = context().nestLevel;
    getInstance(cls).renderPartial();
    if (context().nestLevel != saved) {
      throw new WebAppException("View "+ cls.getSimpleName() +" not complete");
    }
  }
}
