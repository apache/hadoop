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

package org.apache.hadoop.yarn.server.timeline.security;

import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

@Private
@Unstable
public class TimelineAuthenticationFilter extends AuthenticationFilter {

  @Override
  protected Properties getConfiguration(String configPrefix,
      FilterConfig filterConfig) throws ServletException {
    // In yarn-site.xml, we can simply set type to "kerberos". However, we need
    // to replace the name here to use the customized Kerberos + DT service
    // instead of the standard Kerberos handler.
    Properties properties = super.getConfiguration(configPrefix, filterConfig);
    String authType = properties.getProperty(AUTH_TYPE);
    if (authType != null && authType.equals("kerberos")) {
      properties.setProperty(
          AUTH_TYPE, TimelineClientAuthenticationService.class.getName());
    }
    return properties;
  }

}
