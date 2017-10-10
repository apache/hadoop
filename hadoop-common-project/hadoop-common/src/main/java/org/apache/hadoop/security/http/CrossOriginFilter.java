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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrossOriginFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(CrossOriginFilter.class);

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
  public static final String ALLOWED_ORIGINS_REGEX_PREFIX = "regex:";
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
  private boolean allowAllOrigins = true;
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

    String originsList = encodeHeader(req.getHeader(ORIGIN));
    if (!isCrossOrigin(originsList)) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Header origin is null. Returning");
      }
      return;
    }

    if (!areOriginsAllowed(originsList)) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Header origins '" + originsList + "' not allowed. Returning");
      }
      return;
    }

    String accessControlRequestMethod =
        req.getHeader(ACCESS_CONTROL_REQUEST_METHOD);
    if (!isMethodAllowed(accessControlRequestMethod)) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Access control method '" + accessControlRequestMethod +
            "' not allowed. Returning");
      }
      return;
    }

    String accessControlRequestHeaders =
        req.getHeader(ACCESS_CONTROL_REQUEST_HEADERS);
    if (!areHeadersAllowed(accessControlRequestHeaders)) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Access control headers '" + accessControlRequestHeaders +
            "' not allowed. Returning");
      }
      return;
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("Completed cross origin filter checks. Populating " +
          "HttpServletResponse");
    }
    res.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, originsList);
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
    allowedMethods.addAll(
        Arrays.asList(allowedMethodsConfig.trim().split("\\s*,\\s*")));
    LOG.info("Allowed Methods: " + getAllowedMethodsHeader());
  }

  private void initializeAllowedHeaders(FilterConfig filterConfig) {
    String allowedHeadersConfig =
        filterConfig.getInitParameter(ALLOWED_HEADERS);
    if (allowedHeadersConfig == null) {
      allowedHeadersConfig = ALLOWED_HEADERS_DEFAULT;
    }
    allowedHeaders.addAll(
        Arrays.asList(allowedHeadersConfig.trim().split("\\s*,\\s*")));
    LOG.info("Allowed Headers: " + getAllowedHeadersHeader());
  }

  private void initializeAllowedOrigins(FilterConfig filterConfig) {
    String allowedOriginsConfig =
        filterConfig.getInitParameter(ALLOWED_ORIGINS);
    if (allowedOriginsConfig == null) {
      allowedOriginsConfig = ALLOWED_ORIGINS_DEFAULT;
    }
    allowedOrigins.addAll(
        Arrays.asList(allowedOriginsConfig.trim().split("\\s*,\\s*")));
    allowAllOrigins = allowedOrigins.contains("*");
    LOG.info("Allowed Origins: " + StringUtils.join(allowedOrigins, ','));
    LOG.info("Allow All Origins: " + allowAllOrigins);
    List<String> discouragedAllowedOrigins = allowedOrigins.stream()
            .filter(s -> s.length() > 1 && s.contains("*"))
            .collect(Collectors.toList());
    for (String discouragedAllowedOrigin : discouragedAllowedOrigins) {
        LOG.warn("Allowed Origin pattern '" + discouragedAllowedOrigin + "' is discouraged, use the 'regex:' prefix and use a Java regular expression instead.");
    }
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
    // Protect against HTTP response splitting vulnerability
    // since value is written as part of the response header
    // Ensure this header only has one header by removing
    // CRs and LFs
    return header.split("\n|\r")[0].trim();
  }

  static boolean isCrossOrigin(String originsList) {
    return originsList != null;
  }

  @VisibleForTesting
  boolean areOriginsAllowed(String originsList) {
    if (allowAllOrigins) {
      return true;
    }

    String[] origins = originsList.trim().split("\\s+");
    for (String origin : origins) {
      for (String allowedOrigin : allowedOrigins) {
        Pattern regexPattern = null;
        if (allowedOrigin.startsWith(ALLOWED_ORIGINS_REGEX_PREFIX)) {
            String regex = allowedOrigin.substring(ALLOWED_ORIGINS_REGEX_PREFIX.length());
            regexPattern = Pattern.compile(regex);
        } else if (allowedOrigin.contains("*")) {
            String regex = allowedOrigin.replace(".", "\\.").replace("*", ".*");
            regexPattern = Pattern.compile(regex);
        }

        if (regexPattern != null
                && regexPattern.matcher(origin).matches()) {
            return true;
        } else if (allowedOrigin.equals(origin)) {
            return true;
        }
      }
    }
    return false;
  }

  private boolean areHeadersAllowed(String accessControlRequestHeaders) {
    if (accessControlRequestHeaders == null) {
      return true;
    }
    String[] headers = accessControlRequestHeaders.trim().split("\\s*,\\s*");
    return allowedHeaders.containsAll(Arrays.asList(headers));
  }

  private boolean isMethodAllowed(String accessControlRequestMethod) {
    if (accessControlRequestMethod == null) {
      return true;
    }
    return allowedMethods.contains(accessControlRequestMethod);
  }
}
