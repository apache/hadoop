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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

public class CrossOriginFilter implements Filter {

  private static final Log LOG = LogFactory.getLog(CrossOriginFilter.class);

  // HTTP CORS Request Headers
  static final String ORIGIN = "Origin";
  static final String ACCESS_CONTROL_REQUEST_METHOD =
      "Access-Control-Request-Method";
  static final String ACCESS_CONTROL_REQUEST_HEADERS =
      "Access-Control-Request-Headers";

  // HTTP CORS Response Headers
  static final String ACCESS_CONTROL_ALLOW_ORIGIN =
      "Access-Control-Allow-Origin";
  static final String ACCESS_CONTROL_ALLOW_CREDENTIALS =
      "Access-Control-Allow-Credentials";
  static final String ACCESS_CONTROL_ALLOW_METHODS =
      "Access-Control-Allow-Methods";
  static final String ACCESS_CONTROL_ALLOW_HEADERS =
      "Access-Control-Allow-Headers";
  static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";

  // Filter configuration
  public static final String ALLOWED_ORIGINS = "allowed-origins";
  public static final String ALLOWED_ORIGINS_DEFAULT = "*";
  public static final String ALLOWED_METHODS = "allowed-methods";
  public static final String ALLOWED_METHODS_DEFAULT = "GET,POST,HEAD";
  public static final String ALLOWED_HEADERS = "allowed-headers";
  public static final String ALLOWED_HEADERS_DEFAULT =
      "X-Requested-With,Content-Type,Accept,Origin";
  public static final String MAX_AGE = "max-age";
  public static final String MAX_AGE_DEFAULT = "1800";

  private List<String> allowedMethods = new ArrayList<String>();
  private List<String> allowedHeaders = new ArrayList<String>();
  private List<String> allowedOrigins = new ArrayList<String>();
  private String maxAge;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    initializeAllowedMethods(filterConfig);
    initializeAllowedHeaders(filterConfig);
    initializeAllowedOrigins(filterConfig);
    initializeMaxAge(filterConfig);
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res,
      FilterChain chain)
      throws IOException, ServletException {
    doCrossFilter((HttpServletRequest) req, (HttpServletResponse) res);
    chain.doFilter(req, res);
  }

  @Override
  public void destroy() {
    allowedMethods.clear();
    allowedHeaders.clear();
    allowedOrigins.clear();
  }

  private void doCrossFilter(HttpServletRequest req, HttpServletResponse res) {

    String origin = encodeHeader(req.getHeader(ORIGIN));
    if (!isCrossOrigin(origin)) {
      return;
    }

    if (!isOriginAllowed(origin)) {
      return;
    }

    String accessControlRequestMethod =
        req.getHeader(ACCESS_CONTROL_REQUEST_METHOD);
    if (!isMethodAllowed(accessControlRequestMethod)) {
      return;
    }

    String accessControlRequestHeaders =
        req.getHeader(ACCESS_CONTROL_REQUEST_HEADERS);
    if (!areHeadersAllowed(accessControlRequestHeaders)) {
      return;
    }

    res.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    res.setHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, Boolean.TRUE.toString());
    res.setHeader(ACCESS_CONTROL_ALLOW_METHODS, getAllowedMethodsHeader());
    res.setHeader(ACCESS_CONTROL_ALLOW_HEADERS, getAllowedHeadersHeader());
    res.setHeader(ACCESS_CONTROL_MAX_AGE, maxAge);
  }

  @VisibleForTesting
  String getAllowedHeadersHeader() {
    return StringUtils.join(allowedHeaders, ',');
  }

  @VisibleForTesting
  String getAllowedMethodsHeader() {
    return StringUtils.join(allowedMethods, ',');
  }

  private void initializeAllowedMethods(FilterConfig filterConfig) {
    String allowedMethodsConfig =
        filterConfig.getInitParameter(ALLOWED_METHODS);
    if (allowedMethodsConfig == null) {
      allowedMethodsConfig = ALLOWED_METHODS_DEFAULT;
    }
    allowedMethods =
        Arrays.asList(allowedMethodsConfig.trim().split("\\s*,\\s*"));
    LOG.info("Allowed Methods: " + getAllowedMethodsHeader());
  }

  private void initializeAllowedHeaders(FilterConfig filterConfig) {
    String allowedHeadersConfig =
        filterConfig.getInitParameter(ALLOWED_HEADERS);
    if (allowedHeadersConfig == null) {
      allowedHeadersConfig = ALLOWED_HEADERS_DEFAULT;
    }
    allowedHeaders =
        Arrays.asList(allowedHeadersConfig.trim().split("\\s*,\\s*"));
    LOG.info("Allowed Headers: " + getAllowedHeadersHeader());
  }

  private void initializeAllowedOrigins(FilterConfig filterConfig) {
    String allowedOriginsConfig =
        filterConfig.getInitParameter(ALLOWED_ORIGINS);
    if (allowedOriginsConfig == null) {
      allowedOriginsConfig = ALLOWED_ORIGINS_DEFAULT;
    }
    allowedOrigins =
        Arrays.asList(allowedOriginsConfig.trim().split("\\s*,\\s*"));
    LOG.info("Allowed Origins: " + StringUtils.join(allowedOrigins, ','));
  }

  private void initializeMaxAge(FilterConfig filterConfig) {
    maxAge = filterConfig.getInitParameter(MAX_AGE);
    if (maxAge == null) {
      maxAge = MAX_AGE_DEFAULT;
    }
    LOG.info("Max Age: " + maxAge);
  }

  static String encodeHeader(final String header) {
    if (header == null) {
      return null;
    }
    try {
      // Protect against HTTP response splitting vulnerability
      // since value is written as part of the response header
      return URLEncoder.encode(header, "ASCII");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  static boolean isCrossOrigin(String origin) {
    return origin != null;
  }

  private boolean isOriginAllowed(String origin) {
    return allowedOrigins.contains(origin);
  }

  private boolean areHeadersAllowed(String accessControlRequestHeaders) {
    if (accessControlRequestHeaders == null) {
      return true;
    }
    String headers[] = accessControlRequestHeaders.trim().split("\\s*,\\s*");
    return allowedHeaders.containsAll(Arrays.asList(headers));
  }

  private boolean isMethodAllowed(String accessControlRequestMethod) {
    if (accessControlRequestMethod == null) {
      return false;
    }
    return allowedMethods.contains(accessControlRequestMethod);
  }
}
