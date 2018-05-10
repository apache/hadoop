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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.security.TimelineReaderWhitelistAuthorizationFilter;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link TimelineReaderWhitelistAuthorizationFilter}.
 *
 */
public class TestTimelineReaderWhitelistAuthorizationFilter {

  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES =
      new String[] {GROUP1_NAME, GROUP2_NAME, GROUP3_NAME};

  private static class DummyFilterConfig implements FilterConfig {
    final private Map<String, String> map;

    DummyFilterConfig(Map<String, String> map) {
      this.map = map;
    }

    @Override
    public String getFilterName() {
      return "dummy";
    }

    @Override
    public String getInitParameter(String arg0) {
      return map.get(arg0);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return Collections.enumeration(map.keySet());
    }

    @Override
    public ServletContext getServletContext() {
      return null;
    }
  }

  @Test
  public void checkFilterAllowedUser() throws ServletException, IOException {
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        "user1,user2");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user1";
      }
    });

    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    f.doFilter(mockHsr, r, null);
  }

  @Test
  public void checkFilterNotAllowedUser() throws ServletException, IOException {
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        "user1,user2");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    String userName = "testuser1";
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return userName;
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    f.doFilter(mockHsr, r, null);

    String msg = "User " + userName
        + " is not allowed to read TimelineService V2 data.";
    Mockito.verify(r)
        .sendError(eq(HttpServletResponse.SC_FORBIDDEN), eq(msg));
  }

  @Test
  public void checkFilterAllowedUserGroup()
      throws ServletException, IOException, InterruptedException {
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        "user2 group1,group2");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user1";
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        UserGroupInformation.createUserForTesting("user1", GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });
  }

  @Test
  public void checkFilterNotAlloweGroup()
      throws ServletException, IOException, InterruptedException {
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        " group5,group6");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    String userName = "user200";
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return userName;
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        UserGroupInformation.createUserForTesting(userName, GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });
    String msg = "User " + userName
        + " is not allowed to read TimelineService V2 data.";
    Mockito.verify(r)
        .sendError(eq(HttpServletResponse.SC_FORBIDDEN), eq(msg));
  }

  @Test
  public void checkFilterAllowAdmins()
      throws ServletException, IOException, InterruptedException {
    // check that users in admin acl list are allowed to read
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        "user3 group5,group6");
    map.put(YarnConfiguration.YARN_ADMIN_ACL, " group1,group2");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user90";
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        UserGroupInformation.createUserForTesting("user90", GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });
  }

  @Test
  public void checkFilterAllowAdminsWhenNoUsersSet()
      throws ServletException, IOException, InterruptedException {
    // check that users in admin acl list are allowed to read
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    map.put(YarnConfiguration.YARN_ADMIN_ACL, " group1,group2");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user90";
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        UserGroupInformation.createUserForTesting("user90", GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });
  }

  @Test
  public void checkFilterAllowNoOneWhenAdminAclsEmptyAndUserAclsEmpty()
      throws ServletException, IOException, InterruptedException {
    // check that users in admin acl list are allowed to read
    Map<String, String> map = new HashMap<String, String>();
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED, "true");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    String userName = "user88";
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return userName;
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        UserGroupInformation.createUserForTesting(userName, GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });
    String msg = "User " + userName
        + " is not allowed to read TimelineService V2 data.";
    Mockito.verify(r)
        .sendError(eq(HttpServletResponse.SC_FORBIDDEN), eq(msg));
  }

  @Test
  public void checkFilterReadAuthDisabledNoAclSettings()
      throws ServletException, IOException, InterruptedException {
    // Default settings for Read Auth Enabled (false)
    // No values in admin acls or allowed read user list
    Map<String, String> map = new HashMap<String, String>();
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);
    HttpServletRequest mockHsr = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user437";
      }
    });
    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        UserGroupInformation.createUserForTesting("user437", GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });
  }

  @Test
  public void checkFilterReadAuthDisabledButAclSettingsPopulated()
      throws ServletException, IOException, InterruptedException {
    Map<String, String> map = new HashMap<String, String>();
    // Default settings for Read Auth Enabled (false)
    // But some values in admin acls and allowed read user list
    map.put(YarnConfiguration.YARN_ADMIN_ACL, "user1,user2 group9,group21");
    map.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        "user27,user36 group5,group6");
    TimelineReaderWhitelistAuthorizationFilter f =
        new TimelineReaderWhitelistAuthorizationFilter();
    FilterConfig fc = new DummyFilterConfig(map);
    f.init(fc);

    HttpServletRequest mockHsr = mock(HttpServletRequest.class);
    when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user37";
      }
    });

    HttpServletResponse r = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user1 =
        // both username and group name are not part of admin and
        // read allowed users
        // but read auth is turned off
        UserGroupInformation.createUserForTesting("user37", GROUP_NAMES);
    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r, null);
        return null;
      }
    });

    // test with username in read allowed users
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user27";
      }
    });
    HttpServletResponse r2 = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user2 =
        UserGroupInformation.createUserForTesting("user27", GROUP_NAMES);
    user2.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r2, null);
        return null;
      }
    });

    // test with username in admin users
    Mockito.when(mockHsr.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "user2";
      }
    });
    HttpServletResponse r3 = Mockito.mock(HttpServletResponse.class);
    UserGroupInformation user3 =
        UserGroupInformation.createUserForTesting("user2", GROUP_NAMES);
    user3.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        f.doFilter(mockHsr, r3, null);
        return null;
      }
    });
  }
}
