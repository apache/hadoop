/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Servlet filter that logs HTTP request/response headers
 */
public class RequestLoggerFilter implements Filter {
  private static Logger LOG = LoggerFactory.getLogger(RequestLoggerFilter.class);

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
    throws IOException, ServletException {
    if (!LOG.isDebugEnabled()) {
      filterChain.doFilter(request, response);
    }
    else {
      XHttpServletRequest xRequest = new XHttpServletRequest((HttpServletRequest) request);
      XHttpServletResponse xResponse = new XHttpServletResponse((HttpServletResponse) response);
      try {
        LOG.debug(xRequest.getResquestInfo().toString());
        filterChain.doFilter(xRequest, xResponse);
      }
      finally {
        LOG.debug(xResponse.getResponseInfo().toString());
      }
    }
  }

  @Override
  public void destroy() {
  }

  private static class XHttpServletRequest extends HttpServletRequestWrapper {

    public XHttpServletRequest(HttpServletRequest request) {
      super(request);
    }

    public StringBuffer getResquestInfo() {
      StringBuffer sb = new StringBuffer(512);
      sb.append("\n").append("> ").append(getMethod()).append(" ").append(getRequestURL());
      if (getQueryString() != null) {
        sb.append("?").append(getQueryString());
      }
      sb.append("\n");
      Enumeration names = getHeaderNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        Enumeration values = getHeaders(name);
        while (values.hasMoreElements()) {
          String value = (String) values.nextElement();
          sb.append("> ").append(name).append(": ").append(value).append("\n");
        }
      }
      sb.append(">");
      return sb;
    }
  }

  private static class XHttpServletResponse extends HttpServletResponseWrapper {
    private Map<String, List<String>> headers = new HashMap<String, List<String>>();
    private int status;
    private String message;

    public XHttpServletResponse(HttpServletResponse response) {
      super(response);
    }

    private List<String> getHeaderValues(String name, boolean reset) {
      List<String> values = headers.get(name);
      if (reset || values == null) {
        values = new ArrayList<String>();
        headers.put(name, values);
      }
      return values;
    }

    @Override
    public void addCookie(Cookie cookie) {
      super.addCookie(cookie);
      List<String> cookies = getHeaderValues("Set-Cookie", false);
      cookies.add(cookie.getName() + "=" + cookie.getValue());
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {
      super.sendError(sc, msg);
      status = sc;
      message = msg;
    }


    @Override
    public void sendError(int sc) throws IOException {
      super.sendError(sc);
      status = sc;
    }

    @Override
    public void setStatus(int sc) {
      super.setStatus(sc);
      status = sc;
    }

    @Override
    public void setStatus(int sc, String msg) {
      super.setStatus(sc, msg);
      status = sc;
      message = msg;
    }

    @Override
    public void setHeader(String name, String value) {
      super.setHeader(name, value);
      List<String> values = getHeaderValues(name, true);
      values.add(value);
    }

    @Override
    public void addHeader(String name, String value) {
      super.addHeader(name, value);
      List<String> values = getHeaderValues(name, false);
      values.add(value);
    }

    public StringBuffer getResponseInfo() {
      if (status == 0) {
        status = 200;
        message = "OK";
      }
      StringBuffer sb = new StringBuffer(512);
      sb.append("\n").append("< ").append("status code: ").append(status);
      if (message != null) {
        sb.append(", message: ").append(message);
      }
      sb.append("\n");
      for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
        for (String value : entry.getValue()) {
          sb.append("< ").append(entry.getKey()).append(": ").append(value).append("\n");
        }
      }
      sb.append("<");
      return sb;
    }
  }
}
