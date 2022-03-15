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
package org.apache.hadoop.security.http;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter provides protection against cross site request forgery (CSRF)
 * attacks for REST APIs. Enabling this filter on an endpoint results in the
 * requirement of all client to send a particular (configurable) HTTP header
 * with every request. In the absense of this header the filter will reject the
 * attempt as a bad request.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RestCsrfPreventionFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(RestCsrfPreventionFilter.class);

  public static final String HEADER_USER_AGENT = "User-Agent";
  public static final String BROWSER_USER_AGENT_PARAM =
      "browser-useragents-regex";
  public static final String CUSTOM_HEADER_PARAM = "custom-header";
  public static final String CUSTOM_METHODS_TO_IGNORE_PARAM =
      "methods-to-ignore";
  static final String  BROWSER_USER_AGENTS_DEFAULT = "^Mozilla.*,^Opera.*";
  public static final String HEADER_DEFAULT = "X-XSRF-HEADER";
  static final String  METHODS_TO_IGNORE_DEFAULT = "GET,OPTIONS,HEAD,TRACE";
  private String  headerName = HEADER_DEFAULT;
  private Set<String> methodsToIgnore = null;
  private Set<Pattern> browserUserAgents;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String customHeader = filterConfig.getInitParameter(CUSTOM_HEADER_PARAM);
    if (customHeader != null) {
      headerName = customHeader;
    }
    String customMethodsToIgnore =
        filterConfig.getInitParameter(CUSTOM_METHODS_TO_IGNORE_PARAM);
    if (customMethodsToIgnore != null) {
      parseMethodsToIgnore(customMethodsToIgnore);
    } else {
      parseMethodsToIgnore(METHODS_TO_IGNORE_DEFAULT);
    }

    String agents = filterConfig.getInitParameter(BROWSER_USER_AGENT_PARAM);
    if (agents == null) {
      agents = BROWSER_USER_AGENTS_DEFAULT;
    }
    parseBrowserUserAgents(agents);
    LOG.info("Adding cross-site request forgery (CSRF) protection, "
        + "headerName = {}, methodsToIgnore = {}, browserUserAgents = {}",
        headerName, methodsToIgnore, browserUserAgents);
  }

  void parseBrowserUserAgents(String userAgents) {
    String[] agentsArray =  userAgents.split(",");
    browserUserAgents = new HashSet<Pattern>();
    for (String patternString : agentsArray) {
      browserUserAgents.add(Pattern.compile(patternString));
    }
  }

  void parseMethodsToIgnore(String mti) {
    String[] methods = mti.split(",");
    methodsToIgnore = new HashSet<String>();
    for (int i = 0; i < methods.length; i++) {
      methodsToIgnore.add(methods[i]);
    }
  }

  /**
   * This method interrogates the User-Agent String and returns whether it
   * refers to a browser.  If its not a browser, then the requirement for the
   * CSRF header will not be enforced; if it is a browser, the requirement will
   * be enforced.
   * <p>
   * A User-Agent String is considered to be a browser if it matches
   * any of the regex patterns from browser-useragent-regex; the default
   * behavior is to consider everything a browser that matches the following:
   * "^Mozilla.*,^Opera.*".  Subclasses can optionally override
   * this method to use different behavior.
   *
   * @param userAgent The User-Agent String, or null if there isn't one
   * @return true if the User-Agent String refers to a browser, false if not
   */
  protected boolean isBrowser(String userAgent) {
    if (userAgent == null) {
      return false;
    }
    for (Pattern pattern : browserUserAgents) {
      Matcher matcher = pattern.matcher(userAgent);
      if (matcher.matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Defines the minimal API requirements for the filter to execute its
   * filtering logic.  This interface exists to facilitate integration in
   * components that do not run within a servlet container and therefore cannot
   * rely on a servlet container to dispatch to the {@link #doFilter} method.
   * Applications that do run inside a servlet container will not need to write
   * code that uses this interface.  Instead, they can use typical servlet
   * container configuration mechanisms to insert the filter.
   */
  public interface HttpInteraction {

    /**
     * Returns the value of a header.
     *
     * @param header name of header
     * @return value of header
     */
    String getHeader(String header);

    /**
     * Returns the method.
     *
     * @return method
     */
    String getMethod();

    /**
     * Called by the filter after it decides that the request may proceed.
     *
     * @throws IOException if there is an I/O error
     * @throws ServletException if the implementation relies on the servlet API
     *     and a servlet API call has failed
     */
    void proceed() throws IOException, ServletException;

    /**
     * Called by the filter after it decides that the request is a potential
     * CSRF attack and therefore must be rejected.
     *
     * @param code status code to send
     * @param message response message
     * @throws IOException if there is an I/O error
     */
    void sendError(int code, String message) throws IOException;
  }

  /**
   * Handles an {@link HttpInteraction} by applying the filtering logic.
   *
   * @param httpInteraction caller's HTTP interaction
   * @throws IOException if there is an I/O error
   * @throws ServletException if the implementation relies on the servlet API
   *     and a servlet API call has failed
   */
  public void handleHttpInteraction(HttpInteraction httpInteraction)
      throws IOException, ServletException {
    if (!isBrowser(httpInteraction.getHeader(HEADER_USER_AGENT)) ||
        methodsToIgnore.contains(httpInteraction.getMethod()) ||
        httpInteraction.getHeader(headerName) != null) {
      httpInteraction.proceed();
    } else {
      httpInteraction.sendError(HttpServletResponse.SC_BAD_REQUEST,
          "Missing Required Header for CSRF Vulnerability Protection");
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      final FilterChain chain) throws IOException, ServletException {
    final HttpServletRequest httpRequest = (HttpServletRequest)request;
    final HttpServletResponse httpResponse = (HttpServletResponse)response;
    handleHttpInteraction(new ServletFilterHttpInteraction(httpRequest,
        httpResponse, chain));
  }

  @Override
  public void destroy() {
  }

  /**
   * Constructs a mapping of configuration properties to be used for filter
   * initialization.  The mapping includes all properties that start with the
   * specified configuration prefix.  Property names in the mapping are trimmed
   * to remove the configuration prefix.
   *
   * @param conf configuration to read
   * @param confPrefix configuration prefix
   * @return mapping of configuration properties to be used for filter
   *     initialization
   */
  public static Map<String, String> getFilterParams(Configuration conf,
      String confPrefix) {
    return conf.getPropsWithPrefix(confPrefix);
  }

  /**
   * {@link HttpInteraction} implementation for use in the servlet filter.
   */
  private static final class ServletFilterHttpInteraction
      implements HttpInteraction {

    private final FilterChain chain;
    private final HttpServletRequest httpRequest;
    private final HttpServletResponse httpResponse;

    /**
     * Creates a new ServletFilterHttpInteraction.
     *
     * @param httpRequest request to process
     * @param httpResponse response to process
     * @param chain filter chain to forward to if HTTP interaction is allowed
     */
    public ServletFilterHttpInteraction(HttpServletRequest httpRequest,
        HttpServletResponse httpResponse, FilterChain chain) {
      this.httpRequest = httpRequest;
      this.httpResponse = httpResponse;
      this.chain = chain;
    }

    @Override
    public String getHeader(String header) {
      return httpRequest.getHeader(header);
    }

    @Override
    public String getMethod() {
      return httpRequest.getMethod();
    }

    @Override
    public void proceed() throws IOException, ServletException {
      chain.doFilter(httpRequest, httpResponse);
    }

    @Override
    public void sendError(int code, String message) throws IOException {
      if (httpResponse instanceof Response) {
        ((Response)httpResponse).setStatusWithReason(code, message);
      }

      httpResponse.sendError(code, message);
    }
  }
}
