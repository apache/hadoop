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
package org.apache.hadoop.hdfs.web.resources;

import java.lang.reflect.Type;
import java.security.Principal;

import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

@Provider
public class UserProvider extends AbstractHttpContextInjectable<Principal>
    implements InjectableProvider<Context, Type> {

  @Override
  public Principal getValue(final HttpContext context) {
    //get principal from the request
    final Principal principal = context.getRequest().getUserPrincipal();
    if (principal != null) {
      return principal;
    }

    //get username from the parameter
    final String username = context.getRequest().getQueryParameters().getFirst(
        UserParam.NAME);
    if (username != null) {
      final UserParam userparam = new UserParam(username);
      return new Principal() {
        @Override
        public String getName() {
          return userparam.getValue();
        }
      };
    }

    //user not found
    return null;
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable<Principal> getInjectable(
      final ComponentContext componentContext, final Context context,
      final Type type) {
    return type.equals(Principal.class)? this : null;
  }
}