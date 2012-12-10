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

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;
import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.MDC;

import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import java.lang.reflect.Type;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.regex.Pattern;

@Provider
@InterfaceAudience.Private
public class UserProvider extends AbstractHttpContextInjectable<Principal> implements
  InjectableProvider<Context, Type> {

  public static final String USER_NAME_PARAM = "user.name";

  public static final Pattern USER_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9._-]*[$]?$");

  static class UserParam extends StringParam {

    public UserParam(String user) {
      super(USER_NAME_PARAM, user, USER_PATTERN);
    }

    @Override
    public String parseParam(String str) {
      if (str != null) {
        int len = str.length();
        if (len < 1) {
          throw new IllegalArgumentException(MessageFormat.format(
            "Parameter [{0}], it's length must be at least 1", getName()));
        }
      }
      return super.parseParam(str);
    }
  }

  @Override
  public Principal getValue(HttpContext httpContext) {
    Principal principal = httpContext.getRequest().getUserPrincipal();
    if (principal == null) {
      final String user = httpContext.getRequest().getQueryParameters().getFirst(USER_NAME_PARAM);
      if (user != null) {
        principal = new Principal() {
          @Override
          public String getName() {
            return new UserParam(user).value();
          }
        };
      }
    }
    if (principal != null) {
      MDC.put("user", principal.getName());
    }
    return principal;
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable getInjectable(ComponentContext componentContext, Context context, Type type) {
    return (type.equals(Principal.class)) ? this : null;
  }
}
