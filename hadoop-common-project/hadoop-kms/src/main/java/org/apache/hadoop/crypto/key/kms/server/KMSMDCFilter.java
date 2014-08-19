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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Servlet filter that captures context of the HTTP request to be use in the
 * scope of KMS calls on the server side.
 */
@InterfaceAudience.Private
public class KMSMDCFilter implements Filter {

  private static class Data {
    private UserGroupInformation ugi;
    private String method;
    private StringBuffer url;

    private Data(UserGroupInformation ugi, String method, StringBuffer url) {
      this.ugi = ugi;
      this.method = method;
      this.url = url;
    }
  }

  private static ThreadLocal<Data> DATA_TL = new ThreadLocal<Data>();

  public static UserGroupInformation getUgi() {
    return DATA_TL.get().ugi;
  }

  public static String getMethod() {
    return DATA_TL.get().method;
  }

  public static String getURL() {
    return DATA_TL.get().url.toString();
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain)
      throws IOException, ServletException {
    try {
      DATA_TL.remove();
      UserGroupInformation ugi = HttpUserGroupInformation.get();
      String method = ((HttpServletRequest) request).getMethod();
      StringBuffer requestURL = ((HttpServletRequest) request).getRequestURL();
      String queryString = ((HttpServletRequest) request).getQueryString();
      if (queryString != null) {
        requestURL.append("?").append(queryString);
      }
      DATA_TL.set(new Data(ugi, method, requestURL));
      chain.doFilter(request, response);
    } finally {
      DATA_TL.remove();
    }
  }

  @Override
  public void destroy() {
  }
}
