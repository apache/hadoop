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

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilter;

/**
 * Subclass of {@link AuthenticationFilter} that
 * obtains Hadoop-Auth configuration for webhdfs.
 */
public class AuthFilter extends ProxyUserAuthenticationFilter {

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    final HttpServletRequest httpRequest = ProxyUserAuthenticationFilter.
        toLowerCase((HttpServletRequest)request);
    final String tokenString = httpRequest.getParameter(DelegationParam.NAME);
    if (tokenString != null && httpRequest.getServletPath().startsWith(
        WebHdfsFileSystem.PATH_PREFIX)) {
      //Token is present in the url, therefore token will be used for
      //authentication, bypass kerberos authentication.
      filterChain.doFilter(httpRequest, response);
      return;
    }
    super.doFilter(request, response, filterChain);
  }

}