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
package org.apache.hadoop.hdfsproxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuthorizationFilter implements Filter {
  public static final Log LOG = LogFactory.getLog(AuthorizationFilter.class);

  /** Pattern for a filter to find out if a request is HFTP/HSFTP request */
  protected static final Pattern HFTP_PATTERN = Pattern
      .compile("^(/listPaths|/data|/streamFile|/file)$");

  /**
   * Pattern for a filter to find out if an HFTP/HSFTP request stores its file
   * path in the extra path information associated with the URL; if not, the
   * file path is stored in request parameter "filename"
   */
  protected static final Pattern FILEPATH_PATTERN = Pattern
      .compile("^(/listPaths|/data|/file)$");

  /** {@inheritDoc} **/
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  /** {@inheritDoc} **/
  @SuppressWarnings("unchecked")
  public void doFilter(ServletRequest request,
                       ServletResponse response,
                       FilterChain chain)
      throws IOException, ServletException {

    HttpServletResponse rsp = (HttpServletResponse) response;
    HttpServletRequest rqst = (HttpServletRequest) request;

    String userId = getUserId(request);
    String groups = getGroups(request);
    List<Path> allowedPaths = getAllowedPaths(request);

    String filePath = getPathFromRequest(rqst);

    if (filePath == null || !checkHdfsPath(filePath, allowedPaths)) {
      String msg = "User " + userId + " (" + groups
          + ") is not authorized to access path " + filePath;
      LOG.warn(msg);
      rsp.sendError(HttpServletResponse.SC_FORBIDDEN, msg);
      return;
    }
    request.setAttribute("authorized.ugi", userId);

    chain.doFilter(request, response);
  }

  protected String getUserId(ServletRequest request) {
     return (String)request.
         getAttribute("org.apache.hadoop.hdfsproxy.authorized.userID");
  }

   protected String getGroups(ServletRequest request) {
      return (String)request.
          getAttribute("org.apache.hadoop.hdfsproxy.authorized.role");
   }

  protected List<Path> getAllowedPaths(ServletRequest request) {
     return (List<Path>)request.
         getAttribute("org.apache.hadoop.hdfsproxy.authorized.paths");
  }
  
   private String getPathFromRequest(HttpServletRequest rqst) {
    String filePath = null;
    // check request path
    String servletPath = rqst.getServletPath();
    if (HFTP_PATTERN.matcher(servletPath).matches()) {
      // request is an HSFTP request
      if (FILEPATH_PATTERN.matcher(servletPath).matches()) {
        // file path as part of the URL
        filePath = rqst.getPathInfo() != null ? rqst.getPathInfo() : "/";
      } else {
        // file path is stored in "filename" parameter
        filePath = rqst.getParameter("filename");
      }
    }
    return filePath;
  }

  /** check that the requested path is listed in the ldap entry */
  public boolean checkHdfsPath(String pathInfo, List<Path> allowedPaths) {
    if (pathInfo == null || pathInfo.length() == 0) {
      LOG.info("Can't get file path from the request");
      return false;
    }
    Path userPath = new Path(pathInfo);
    while (userPath != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("\n Checking file path " + userPath);
      }
      if (allowedPaths.contains(userPath))
        return true;
      userPath = userPath.getParent();
    }
    return false;
  }

  /** {@inheritDoc} **/
  public void destroy() {
  }
}
