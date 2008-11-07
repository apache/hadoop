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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A base class for the servlets in DFS.
 */
abstract class DfsServlet extends HttpServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  static final Log LOG = LogFactory.getLog(DfsServlet.class.getCanonicalName());

  /** Get {@link UserGroupInformation} from request */
  protected UnixUserGroupInformation getUGI(HttpServletRequest request) {
    String ugi = request.getParameter("ugi");
    try {
      return new UnixUserGroupInformation(ugi.split(","));
    }
    catch(Exception e) {
      LOG.warn("Invalid ugi (= " + ugi + ")");
    }
    return JspHelper.webUGI;
  }

  /**
   * Create a {@link NameNode} proxy from the current {@link ServletContext}. 
   */
  protected ClientProtocol createNameNodeProxy(UnixUserGroupInformation ugi
      ) throws IOException {
    ServletContext context = getServletContext();
    InetSocketAddress nnAddr = (InetSocketAddress)context.getAttribute("name.node.address");
    Configuration conf = new Configuration(
        (Configuration)context.getAttribute("name.conf"));
    UnixUserGroupInformation.saveToConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    return DFSClient.createNamenode(nnAddr, conf);
  }

  /** Create a URI for redirecting request */
  protected URI createRedirectUri(String servletpath, UserGroupInformation ugi,
      DatanodeID host, HttpServletRequest request) throws URISyntaxException {
    final String hostname = host instanceof DatanodeInfo?
        ((DatanodeInfo)host).getHostName(): host.getHost();
    final String scheme = request.getScheme();
    final int port = "https".equals(scheme)?
        (Integer)getServletContext().getAttribute("datanode.https.port")
        : host.getInfoPort();
    final String filename = request.getPathInfo();
    return new URI(scheme, null, hostname, port, servletpath,
        "filename=" + filename + "&ugi=" + ugi, null);
  }

  /** Get filename from the request */
  protected String getFilename(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    final String filename = request.getParameter("filename");
    if (filename == null || filename.length() == 0) {
      throw new IOException("Invalid filename");
    }
    return filename;
  }
}
