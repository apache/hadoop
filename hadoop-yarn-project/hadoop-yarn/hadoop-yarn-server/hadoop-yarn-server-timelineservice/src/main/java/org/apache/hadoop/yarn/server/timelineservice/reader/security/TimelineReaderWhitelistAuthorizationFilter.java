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

package org.apache.hadoop.yarn.server.timelineservice.reader.security;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderWebServicesUtils;

/**
 * Filter to check if a particular user is allowed to read ATSv2 data.
 */

public class TimelineReaderWhitelistAuthorizationFilter implements Filter {

  public static final String EMPTY_STRING = "";

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineReaderWhitelistAuthorizationFilter.class);

  private boolean isWhitelistReadAuthEnabled = false;

  private AccessControlList allowedUsersAclList;
  private AccessControlList adminAclList;

  @Override
  public void destroy() {
    // NOTHING
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    if (isWhitelistReadAuthEnabled) {
      UserGroupInformation callerUGI = TimelineReaderWebServicesUtils
          .getCallerUserGroupInformation((HttpServletRequest) request, true);
      if (callerUGI == null) {
        String msg = "Unable to obtain user name, user not authenticated";
        throw new AuthorizationException(msg);
      }
      if (!(adminAclList.isUserAllowed(callerUGI)
          || allowedUsersAclList.isUserAllowed(callerUGI))) {
        String userName = callerUGI.getShortUserName();
        String msg = "User " + userName
            + " is not allowed to read TimelineService V2 data.";
        Response.status(Status.FORBIDDEN).entity(msg).build();
        throw new ForbiddenException("user " + userName
            + " is not allowed to read TimelineService V2 data");
      }
    }
    if (chain != null) {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void init(FilterConfig conf) throws ServletException {
    String isWhitelistReadAuthEnabledStr = conf
        .getInitParameter(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED);
    if (isWhitelistReadAuthEnabledStr == null) {
      isWhitelistReadAuthEnabled =
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_AUTH_ENABLED;
    } else {
      isWhitelistReadAuthEnabled =
          Boolean.valueOf(isWhitelistReadAuthEnabledStr);
    }

    if (isWhitelistReadAuthEnabled) {
      String listAllowedUsers = conf.getInitParameter(
          YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS);
      if (StringUtils.isEmpty(listAllowedUsers)) {
        listAllowedUsers =
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_ALLOWED_USERS;
      }
      LOG.info("listAllowedUsers=" + listAllowedUsers);
      allowedUsersAclList = new AccessControlList(listAllowedUsers);
      LOG.info("allowedUsersAclList=" + allowedUsersAclList.getUsers());
      // also allow admins
      String adminAclListStr =
          conf.getInitParameter(YarnConfiguration.YARN_ADMIN_ACL);
      if (StringUtils.isEmpty(adminAclListStr)) {
        adminAclListStr =
            TimelineReaderWhitelistAuthorizationFilter.EMPTY_STRING;
        LOG.info("adminAclList not set, hence setting it to \"\"");
      }
      adminAclList = new AccessControlList(adminAclListStr);
      LOG.info("adminAclList=" + adminAclList.getUsers());
    }
  }
}
