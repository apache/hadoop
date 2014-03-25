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
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ServletUtil;

/** Redirect queries about the hosted filesystem to an appropriate datanode.
 * @see org.apache.hadoop.hdfs.web.HftpFileSystem
 */
@InterfaceAudience.Private
public class FileDataServlet extends DfsServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  /** Create a redirection URL */
  private URL createRedirectURL(String path, String encodedPath, HdfsFileStatus status, 
      UserGroupInformation ugi, ClientProtocol nnproxy, HttpServletRequest request, String dt)
      throws IOException {
    String scheme = request.getScheme();
    final LocatedBlocks blks = nnproxy.getBlockLocations(
        status.getFullPath(new Path(path)).toUri().getPath(), 0, 1);
    final Configuration conf = NameNodeHttpServer.getConfFromContext(
        getServletContext());
    final DatanodeID host = pickSrcDatanode(blks, status, conf);
    final String hostname;
    if (host instanceof DatanodeInfo) {
      hostname = host.getHostName();
    } else {
      hostname = host.getIpAddr();
    }

    int port = "https".equals(scheme) ? host.getInfoSecurePort() : host
        .getInfoPort();

    String dtParam = "";
    if (dt != null) {
      dtParam = JspHelper.getDelegationTokenUrlParam(dt);
    }

    // Add namenode address to the url params
    NameNode nn = NameNodeHttpServer.getNameNodeFromContext(
        getServletContext());
    String addr = nn.getNameNodeAddressHostPortString();
    String addrParam = JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, addr);
    
    return new URL(scheme, hostname, port,
        "/streamFile" + encodedPath + '?' +
        "ugi=" + ServletUtil.encodeQueryValue(ugi.getShortUserName()) +
        dtParam + addrParam);
  }

  /** Select a datanode to service this request.
   * Currently, this looks at no more than the first five blocks of a file,
   * selecting a datanode randomly from the most represented.
   * @param conf 
   */
  private DatanodeID pickSrcDatanode(LocatedBlocks blks, HdfsFileStatus i,
      Configuration conf) throws IOException {
    if (i.getLen() == 0 || blks.getLocatedBlocks().size() <= 0) {
      // pick a random datanode
      NameNode nn = NameNodeHttpServer.getNameNodeFromContext(
          getServletContext());
      return NamenodeJspHelper.getRandomDatanode(nn);
    }
    return JspHelper.bestNode(blks, conf);
  }

  /**
   * Service a GET request as described below.
   * Request:
   * {@code
   * GET http://<nn>:<port>/data[/<path>] HTTP/1.1
   * }
   */
  @Override
  public void doGet(final HttpServletRequest request,
      final HttpServletResponse response)
      throws IOException {
    final Configuration conf = NameNodeHttpServer.getConfFromContext(
        getServletContext());
    final UserGroupInformation ugi = getUGI(request, conf);

    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          ClientProtocol nn = createNameNodeProxy();
          final String path = ServletUtil.getDecodedPath(request, "/data");
          final String encodedPath = ServletUtil.getRawPath(request, "/data");
          String delegationToken = request
              .getParameter(JspHelper.DELEGATION_PARAMETER_NAME);

          HdfsFileStatus info = nn.getFileInfo(path);
          if (info != null && !info.isDir()) {
            response.sendRedirect(createRedirectURL(path, encodedPath,
                info, ugi, nn, request, delegationToken).toString());
          } else if (info == null) {
            response.sendError(400, "File not found " + path);
          } else {
            response.sendError(400, path + ": is a directory");
          }
          return null;
        }
      });
    } catch (IOException e) {
      response.sendError(400, e.getMessage());
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
    }
  }

}
