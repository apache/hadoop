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
package org.apache.hadoop.hdfs.web;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * A filter to change parameter names to lower cases
 * so that parameter names are considered as case insensitive.
 */
public class ParamFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    if (request instanceof HttpServletRequest) {
      HttpServletRequest httpServletRequest = (HttpServletRequest) request;
      chain.doFilter(new CustomHttpServletRequestWrapper(httpServletRequest), response);
    } else {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void destroy() {
  }

  private static final class CustomHttpServletRequestWrapper
      extends HttpServletRequestWrapper {

    private Map<String, String[]> lowerCaseParams = new HashMap<>();

    private CustomHttpServletRequestWrapper(HttpServletRequest request) {
      super(request);
      Map<String, String[]> originalParams = request.getParameterMap();
      for (Map.Entry<String, String[]> entry : originalParams.entrySet()) {
        lowerCaseParams.put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }

    public String getParameter(String name) {
      String[] values = getParameterValues(name);
      if (values != null && values.length > 0) {
        return values[0];
      } else {
        return null;
      }
    }

    @Override
    public Map<String, String[]> getParameterMap() {
      return Collections.unmodifiableMap(lowerCaseParams);
    }

    @Override
    public Enumeration<String> getParameterNames() {
      return Collections.enumeration(lowerCaseParams.keySet());
    }

    @Override
    public String[] getParameterValues(String name) {
      return lowerCaseParams.get(name.toLowerCase());
    }
  }
}