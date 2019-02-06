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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.view.DefaultPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.RequestScoped;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class Controller implements Params {
  public static final Logger LOG = LoggerFactory.getLogger(Controller.class);
  static final ObjectMapper jsonMapper = new ObjectMapper();

  @RequestScoped
  public static class RequestContext{
    final Injector injector;
    final HttpServletRequest request;
    final HttpServletResponse response;
    private Map<String, String> moreParams;
    private Map<String, Cookie> cookies;
    int status = 200; // pre 3.0 servlet-api doesn't have getStatus
    boolean rendered = false;
    Throwable error;
    boolean devMode = false;
    String prefix;

    @Inject RequestContext(Injector injector, HttpServletRequest request,
                           HttpServletResponse response) {
      this.injector = injector;
      this.request = request;
      this.response = response;
    }

    public int status() { return status; }

    public void setStatus(int status) {
      this.status = status;
      response.setStatus(status);
    }

    public void setRendered(boolean rendered) {
      this.rendered = rendered;
    }

    public Map<String, String> moreParams() {
      if (moreParams == null) {
        moreParams = Maps.newHashMap();
      }
      return moreParams; // OK
    }

    public Map<String, Cookie> cookies() {
      if (cookies == null) {
        cookies = Maps.newHashMap();
        Cookie[] rcookies = request.getCookies();
        if (rcookies != null) {
          for (Cookie cookie : rcookies) {
            cookies.put(cookie.getName(), cookie);
          }
        }
      }
      return cookies; // OK
    }

    public void set(String key, String value) {
      moreParams().put(key, value);
    }

    public String get(String key, String defaultValue) {
      String value = moreParams().get(key);
      if (value == null) {
        value = request.getParameter(key);
      }
      return value == null ? defaultValue : value;
    }

    public String prefix() { return prefix; }

    public HttpServletRequest getRequest() {
      return request;
    }
  }

  private RequestContext context;
  @Inject Injector injector;

  public Controller() {
    // Makes injection in subclasses optional.
    // Time will tell if this buy us more than the NPEs :)
  }

  public Controller(RequestContext ctx) {
    context = ctx;
  }

  public RequestContext context() {
    if (context == null) {
      if (injector == null) {
        // One of the downsides of making injection in subclasses optional.
        throw new WebAppException(join("Error accessing RequestContext from\n",
            "a child constructor, either move the usage of the Controller\n",
            "methods out of the constructor or inject the RequestContext\n",
            "into the constructor"));
      }
      context = injector.getInstance(RequestContext.class);
    }
    return context;
  }

  public Throwable error() { return context().error; }

  public int status() { return context().status; }

  public void setStatus(int status) {
    context().setStatus(status);
  }

  public boolean inDevMode() { return context().devMode; }

  public Injector injector() { return context().injector; }

  public <T> T getInstance(Class<T> cls) {
    return injector.getInstance(cls);
  }

  public HttpServletRequest request() { return context().request; }

  public HttpServletResponse response() { return context().response; }

  public void set(String key, String value) {
    context().set(key, value);
  }

  public String get(String key, String defaultValue) {
    return context().get(key, defaultValue);
  }

  public String $(String key) {
    return get(key, "");
  }

  public void setTitle(String title) {
    set(TITLE, title);
  }

  public void setTitle(String title, String url) {
    setTitle(title);
    set(TITLE_LINK, url);
  }

  public ResponseInfo info(String about) {
    return getInstance(ResponseInfo.class).about(about);
  }

  /**
   * Get the cookies
   * @return the cookies map
   */
  public Map<String, Cookie> cookies() {
    return context().cookies();
  }

 /**
   * Create an url from url components
   * @param parts components to join
   * @return an url string
   */
  public String url(String... parts) {
    return ujoin(context().prefix, parts);
  }

  /**
   * The default action.
   */
  public abstract void index();

  public void echo() {
    render(DefaultPage.class);
  }

  protected void render(Class<? extends View> cls) {
    context().rendered = true;
    getInstance(cls).render();
  }

  /**
   * Convenience method for REST APIs (without explicit views)
   * @param object - the object as the response (in JSON)
   */
  protected void renderJSON(Object object) {
    LOG.debug("{}: {}", MimeType.JSON, object);
    context().rendered = true;
    context().response.setContentType(MimeType.JSON);
    try {
      jsonMapper.writeValue(writer(), object);
    } catch (Exception e) {
      throw new WebAppException(e);
    }
  }

  protected void renderJSON(Class<? extends ToJSON> cls) {
    context().rendered = true;
    response().setContentType(MimeType.JSON);
    getInstance(cls).toJSON(writer());
  }

  /**
   * Convenience method for hello world :)
   * @param s - the content to render as plain text
   */
  protected void renderText(String s) {
    LOG.debug("{}: {}", MimeType.TEXT, s);
    context().rendered = true;
    response().setContentType(MimeType.TEXT);
    writer().print(s);
  }

  protected PrintWriter writer() {
    try {
      return response().getWriter();
    } catch (Exception e) {
      throw new WebAppException(e);
    }
  }
}
