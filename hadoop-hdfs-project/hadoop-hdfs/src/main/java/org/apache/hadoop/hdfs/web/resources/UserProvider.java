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

import java.io.IOException;
import java.lang.reflect.Type;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

/** Inject user information to http operations. */
@Provider
public class UserProvider
    extends AbstractHttpContextInjectable<UserGroupInformation>
    implements InjectableProvider<Context, Type> {
  @Context HttpServletRequest request;
  @Context ServletContext servletcontext;

  @Override
  public UserGroupInformation getValue(final HttpContext context) {
    final Configuration conf = (Configuration) servletcontext
        .getAttribute(JspHelper.CURRENT_CONF);
    try {
      return JspHelper.getUGI(servletcontext, request, conf,
          AuthenticationMethod.KERBEROS, false);
    } catch (IOException e) {
      throw new SecurityException(
          "Failed to obtain user group information: " + e, e);
    }
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable<UserGroupInformation> getInjectable(
      final ComponentContext componentContext, final Context context,
      final Type type) {
    return type.equals(UserGroupInformation.class)? this : null;
  }
}