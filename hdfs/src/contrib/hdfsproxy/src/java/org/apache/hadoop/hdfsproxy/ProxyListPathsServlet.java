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
package org.apache.hadoop.hdfsproxy;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.ListPathsServlet;
import org.apache.hadoop.security.UnixUserGroupInformation;

/** {@inheritDoc} */
public class ProxyListPathsServlet extends ListPathsServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  /** {@inheritDoc} */
  @Override
  public void init() throws ServletException {
    ServletContext context = getServletContext();
    if (context.getAttribute("name.conf") == null) {
      context.setAttribute("name.conf", new HdfsConfiguration());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected UnixUserGroupInformation getUGI(HttpServletRequest request) {
    String userID = (String) request
        .getAttribute("org.apache.hadoop.hdfsproxy.authorized.userID");
    String groupName = (String) request
        .getAttribute("org.apache.hadoop.hdfsproxy.authorized.role");
    UnixUserGroupInformation ugi;
    if (groupName != null) {
      // group info stored in ldap
      ugi = new UnixUserGroupInformation(userID, groupName.split(","));
    } else {// stronger ugi management
      ugi = ProxyUgiManager.getUgiForUser(userID);
    }
    return ugi;
  }
}
