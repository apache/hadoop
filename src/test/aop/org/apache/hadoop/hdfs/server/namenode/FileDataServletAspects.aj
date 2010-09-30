/*
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
package org.apache.hadoop.hdfs.server.namenode;

import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.security.UserGroupInformation;

public aspect FileDataServletAspects {
  static final Log LOG = FileDataServlet.LOG;

  pointcut callCreateUri() : call (URI FileDataServlet.createUri(
      String, HdfsFileStatus, UserGroupInformation, ClientProtocol,
      HttpServletRequest, String));

  /** Replace host name with "localhost" for unit test environment. */
  URI around () throws URISyntaxException : callCreateUri() {
    final URI original = proceed(); 
    LOG.info("FI: original uri = " + original);
    final URI replaced = new URI(original.getScheme(), original.getUserInfo(),
        "localhost", original.getPort(), original.getPath(),
        original.getQuery(), original.getFragment()) ; 
    LOG.info("FI: replaced uri = " + replaced);
    return replaced;
  }
}
