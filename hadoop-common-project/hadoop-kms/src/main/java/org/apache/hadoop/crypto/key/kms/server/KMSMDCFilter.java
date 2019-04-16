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

import com.google.common.annotations.VisibleForTesting;

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
    private final UserGroupInformation ugi;
    private final String method;
    private final String url;
    private final String remoteClientAddress;

    private Data(UserGroupInformation ugi, String method, String url,
        String remoteClientAddress) {
      this.ugi = ugi;
      this.method = method;
      this.url = url;
      this.remoteClientAddress = remoteClientAddress;
    }
  }

  private static final ThreadLocal<Data> DATA_TL = new ThreadLocal<Data>();

  public static UserGroupInformation getUgi() {
    Data data = DATA_TL.get();
    return data != null ? data.ugi : null;
  }

  public static String getMethod() {
    Data data = DATA_TL.get();
    return data != null ? data.method : null;
  }

  public static String getURL() {
    Data data = DATA_TL.get();
    return data != null ? data.url : null;
  }

  public static String getRemoteClientAddress() {
    Data data = DATA_TL.get();
    return data != null ? data.remoteClientAddress : null;
  }

  @Override
  public void init(FilterConfig config) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain)
      throws IOException, ServletException {
    try {
      clearContext();
      UserGroupInformation ugi = HttpUserGroupInformation.get();
      HttpServletRequest httpServletRequest = (HttpServletRequest) request;
      String method = httpServletRequest.getMethod();
      StringBuffer requestURL = httpServletRequest.getRequestURL();
      String queryString = httpServletRequest.getQueryString();
      if (queryString != null) {
        requestURL.append("?").append(queryString);
      }
      setContext(ugi, method, requestURL.toString(), request.getRemoteAddr());
      chain.doFilter(request, response);
    } finally {
      clearContext();
    }
  }

  @Override
  public void destroy() {
  }

  /**
   * Sets the context with the given parameters.
   * @param ugi the {@link UserGroupInformation} for the current request.
   * @param method the http method
   * @param requestURL the requested URL.
   * @param remoteAddr the remote address of the client.
   */
  @VisibleForTesting
  public static void setContext(UserGroupInformation ugi,
      String method, String requestURL, String remoteAddr) {
    DATA_TL.set(new Data(ugi, method, requestURL, remoteAddr));
  }

  private static void clearContext() {
    DATA_TL.remove();
  }

}
