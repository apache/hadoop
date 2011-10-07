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
package org.apache.hadoop.hdfs.web;

import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;

/**
 * Subclass of {@link AuthenticationFilter} that
 * obtains Hadoop-Auth configuration for webhdfs.
 */
public class AuthFilter extends AuthenticationFilter {
  private static final String CONF_PREFIX = "dfs.web.authentication.";

  /**
   * Returns the filter configuration properties,
   * including the ones prefixed with {@link #CONF_PREFIX}.
   * The prefix is removed from the returned property names.
   *
   * @param prefix parameter not used.
   * @param config parameter contains the initialization values.
   * @return Hadoop-Auth configuration properties.
   * @throws ServletException 
   */
  @Override
  protected Properties getConfiguration(String prefix, FilterConfig config)
      throws ServletException {
    final Properties p = super.getConfiguration(CONF_PREFIX, config);
    // set authentication type
    p.setProperty(AUTH_TYPE, UserGroupInformation.isSecurityEnabled()?
        KerberosAuthenticationHandler.TYPE: PseudoAuthenticationHandler.TYPE);
    //For Pseudo Authentication, allow anonymous.
    p.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
    //set cookie path
    p.setProperty(COOKIE_PATH, "/");
   return p;
  }
}