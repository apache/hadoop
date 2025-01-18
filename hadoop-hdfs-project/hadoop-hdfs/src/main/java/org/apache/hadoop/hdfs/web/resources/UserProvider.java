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
import java.util.function.Supplier;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

/** Inject user information to http operations. */
@Provider
public class UserProvider implements Supplier<UserGroupInformation> {
  @Context
  private HttpServletRequest request;

  @Context
  private ServletContext servletcontext;

  public UserGroupInformation get() {
    final Configuration conf = (Configuration) servletcontext
        .getAttribute(JspHelper.CURRENT_CONF);
    try {
      return JspHelper.getUGI(servletcontext, request, conf,
          AuthenticationMethod.KERBEROS, false);
    } catch (IOException e) {
      throw new SecurityException(
          SecurityUtil.FAILED_TO_GET_UGI_MSG_HEADER + " " + e, e);
    }
  }
}