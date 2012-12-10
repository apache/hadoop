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

package org.apache.hadoop.lib.wsrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.security.Principal;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.hadoop.test.TestException;
import org.apache.hadoop.test.TestExceptionHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.mockito.Mockito;
import org.slf4j.MDC;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.core.spi.component.ComponentScope;

public class TestUserProvider {

  @Rule
  public MethodRule exceptionHelper = new TestExceptionHelper();

  @Test
  @SuppressWarnings("unchecked")
  public void noUser() {
    MDC.remove("user");
    HttpRequestContext request = Mockito.mock(HttpRequestContext.class);
    Mockito.when(request.getUserPrincipal()).thenReturn(null);
    MultivaluedMap map = Mockito.mock(MultivaluedMap.class);
    Mockito.when(map.getFirst(UserProvider.USER_NAME_PARAM)).thenReturn(null);
    Mockito.when(request.getQueryParameters()).thenReturn(map);
    HttpContext context = Mockito.mock(HttpContext.class);
    Mockito.when(context.getRequest()).thenReturn(request);
    UserProvider up = new UserProvider();
    assertNull(up.getValue(context));
    assertNull(MDC.get("user"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void queryStringUser() {
    MDC.remove("user");
    HttpRequestContext request = Mockito.mock(HttpRequestContext.class);
    Mockito.when(request.getUserPrincipal()).thenReturn(null);
    MultivaluedMap map = Mockito.mock(MultivaluedMap.class);
    Mockito.when(map.getFirst(UserProvider.USER_NAME_PARAM)).thenReturn("foo");
    Mockito.when(request.getQueryParameters()).thenReturn(map);
    HttpContext context = Mockito.mock(HttpContext.class);
    Mockito.when(context.getRequest()).thenReturn(request);
    UserProvider up = new UserProvider();
    assertEquals(up.getValue(context).getName(), "foo");
    assertEquals(MDC.get("user"), "foo");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void principalUser() {
    MDC.remove("user");
    HttpRequestContext request = Mockito.mock(HttpRequestContext.class);
    Mockito.when(request.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "bar";
      }
    });
    HttpContext context = Mockito.mock(HttpContext.class);
    Mockito.when(context.getRequest()).thenReturn(request);
    UserProvider up = new UserProvider();
    assertEquals(up.getValue(context).getName(), "bar");
    assertEquals(MDC.get("user"), "bar");
  }

  @Test
  public void getters() {
    UserProvider up = new UserProvider();
    assertEquals(up.getScope(), ComponentScope.PerRequest);
    assertEquals(up.getInjectable(null, null, Principal.class), up);
    assertNull(up.getInjectable(null, null, String.class));
  }

  @Test
  @TestException(exception = IllegalArgumentException.class)
  public void userNameEmpty() {
    UserProvider.UserParam userParam = new UserProvider.UserParam("username");
    userParam.parseParam("");
  }

  @Test
  @TestException(exception = IllegalArgumentException.class)
  public void userNameInvalidStart() {
    UserProvider.UserParam userParam = new UserProvider.UserParam("username");
    userParam.parseParam("1x");
  }

  @Test
  @TestException(exception = IllegalArgumentException.class)
  public void userNameInvalidDollarSign() {
    UserProvider.UserParam userParam = new UserProvider.UserParam("username");
    userParam.parseParam("1$x");
  }

  @Test
  public void userNameMinLength() {
    UserProvider.UserParam userParam = new UserProvider.UserParam("username");
    assertNotNull(userParam.parseParam("a"));
  }

  @Test
  public void userNameValidDollarSign() {
    UserProvider.UserParam userParam = new UserProvider.UserParam("username");
    assertNotNull(userParam.parseParam("a$"));
  }

}
