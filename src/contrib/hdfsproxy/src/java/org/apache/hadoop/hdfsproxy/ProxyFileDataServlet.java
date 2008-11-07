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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FileDataServlet;
import org.apache.hadoop.security.UnixUserGroupInformation;

/** {@inheritDoc} */
public class ProxyFileDataServlet extends FileDataServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  /** {@inheritDoc} */
  @Override
  protected URI createUri(FileStatus i, UnixUserGroupInformation ugi,
      ClientProtocol nnproxy, HttpServletRequest request) throws IOException,
      URISyntaxException {
    return new URI(request.getScheme(), null, request.getServerName(), request
        .getServerPort(), "/streamFile", "filename=" + i.getPath() + "&ugi="
        + ugi, null);
  }

  /** {@inheritDoc} */
  @Override
  protected UnixUserGroupInformation getUGI(HttpServletRequest request) {
    return (UnixUserGroupInformation) request.getAttribute("authorized.ugi");
  }
}
