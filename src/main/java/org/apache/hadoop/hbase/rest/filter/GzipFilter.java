/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.rest.filter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GzipFilter implements Filter {
  private Set<String> mimeTypes = new HashSet<String>();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String s = filterConfig.getInitParameter("mimeTypes");
    if (s != null) {
      StringTokenizer tok = new StringTokenizer(s, ",", false);
      while (tok.hasMoreTokens()) {
        mimeTypes.add(tok.nextToken());
      }
    }
  }

  @Override
  public void destroy() {
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse rsp,
      FilterChain chain) throws IOException, ServletException {
    HttpServletRequest request = (HttpServletRequest)req;
    HttpServletResponse response = (HttpServletResponse)rsp;
    String contentEncoding = request.getHeader("content-encoding");
    String acceptEncoding = request.getHeader("accept-encoding");
    String contentType = request.getHeader("content-type");
    if ((contentEncoding != null) &&
        (contentEncoding.toLowerCase().indexOf("gzip") > -1)) {
      request = new GZIPRequestWrapper(request);
    }
    if (((acceptEncoding != null) &&
          (acceptEncoding.toLowerCase().indexOf("gzip") > -1)) ||
        ((contentType != null) && mimeTypes.contains(contentType))) {
      response = new GZIPResponseWrapper(response);
    }
    chain.doFilter(request, response);
    if (response instanceof GZIPResponseWrapper) {
      OutputStream os = response.getOutputStream();
      if (os instanceof GZIPResponseStream) {
        ((GZIPResponseStream)os).finish();
      }
    }
  }

}