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

package org.apache.hadoop.yarn.server.security.http;

import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

@Private
@Unstable
public class RMAuthenticationFilter extends AuthenticationFilter {

  public static final String AUTH_HANDLER_PROPERTY =
      "yarn.resourcemanager.authentication-handler";

  public RMAuthenticationFilter() {
  }

  @Override
  protected Properties getConfiguration(String configPrefix,
      FilterConfig filterConfig) throws ServletException {

    // In yarn-site.xml, we can simply set type to "kerberos". However, we need
    // to replace the name here to use the customized Kerberos + DT service
    // instead of the standard Kerberos handler.

    Properties properties = super.getConfiguration(configPrefix, filterConfig);
    String yarnAuthHandler = properties.getProperty(AUTH_HANDLER_PROPERTY);
    if (yarnAuthHandler == null || yarnAuthHandler.isEmpty()) {
      // if http auth type is simple, the default authentication filter
      // will handle it, else throw an exception
      if (!properties.getProperty(AUTH_TYPE).equals("simple")) {
        throw new ServletException("Authentication handler class is empty");
      }
    }
    if (properties.getProperty(AUTH_TYPE).equalsIgnoreCase("kerberos")) {
      properties.setProperty(AUTH_TYPE, yarnAuthHandler);
    }
    return properties;
  }

}
