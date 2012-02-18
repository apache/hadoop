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

import java.net.URL;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.security.UserGroupInformation;

public aspect FileDataServletAspects {
  static final Log LOG = FileDataServlet.LOG;

  pointcut callCreateUrl() : call (URL FileDataServlet.createRedirectURL(
      String, String, HdfsFileStatus, UserGroupInformation, ClientProtocol,
      HttpServletRequest, String));

  /** Replace host name with "localhost" for unit test environment. */
  URL around () throws IOException : callCreateUrl() {
    final URL original = proceed();
    LOG.info("FI: original url = " + original);
    final URL replaced = new URL("http", "localhost", original.getPort(),
        original.getPath() + '?' + original.getQuery());
    LOG.info("FI: replaced url = " + replaced);
    return replaced;
  }
}
