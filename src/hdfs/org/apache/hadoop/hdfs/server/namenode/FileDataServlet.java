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
import java.net.URI;
import java.net.URISyntaxException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UnixUserGroupInformation;

/** Redirect queries about the hosted filesystem to an appropriate datanode.
 * @see org.apache.hadoop.hdfs.HftpFileSystem
 */
public class FileDataServlet extends DfsServlet {

  /** Create a redirection URI */
  protected URI createUri(FileStatus i, UnixUserGroupInformation ugi,
      ClientProtocol nnproxy, HttpServletRequest request)
      throws IOException, URISyntaxException {
    String scheme = request.getScheme();
    final DatanodeID host = pickSrcDatanode(i, nnproxy);
    final String hostname;
    if (host instanceof DatanodeInfo) {
      hostname = ((DatanodeInfo)host).getHostName();
    } else {
      hostname = host.getHost();
    }
    return new URI(scheme, null, hostname,
        "https".equals(scheme)
          ? (Integer)getServletContext().getAttribute("datanode.https.port")
          : host.getInfoPort(),
        "/streamFile", "filename=" + i.getPath() + "&ugi=" + ugi, null);
  }

  private static JspHelper jspHelper = null;

  /** Select a datanode to service this request.
   * Currently, this looks at no more than the first five blocks of a file,
   * selecting a datanode randomly from the most represented.
   */
  private static DatanodeID pickSrcDatanode(FileStatus i,
      ClientProtocol nnproxy) throws IOException {
    // a race condition can happen by initializing a static member this way.
    // A proper fix should make JspHelper a singleton. Since it doesn't affect 
    // correctness, we leave it as is for now.
    if (jspHelper == null)
      jspHelper = new JspHelper();
    final LocatedBlocks blks = nnproxy.getBlockLocations(
        i.getPath().toUri().getPath(), 0, 1);
    if (i.getLen() == 0 || blks.getLocatedBlocks().size() <= 0) {
      // pick a random datanode
      return jspHelper.randomNode();
    }
    return jspHelper.bestNode(blks.get(0));
  }

  /**
   * Service a GET request as described below.
   * Request:
   * {@code
   * GET http://<nn>:<port>/data[/<path>] HTTP/1.1
   * }
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException {
    final UnixUserGroupInformation ugi = getUGI(request);
    final ClientProtocol nnproxy = createNameNodeProxy(ugi);

    try {
      final String path = request.getPathInfo() != null
        ? request.getPathInfo() : "/";
      FileStatus info = nnproxy.getFileInfo(path);
      if ((info != null) && !info.isDir()) {
        response.sendRedirect(createUri(info, ugi, nnproxy,
              request).toURL().toString());
      } else if (info == null){
        response.sendError(400, "cat: File not found " + path);
      } else {
        response.sendError(400, "cat: " + path + ": is a directory");
      }
    } catch (URISyntaxException e) {
      response.getWriter().println(e.toString());
    } catch (IOException e) {
      response.sendError(400, e.getMessage());
    }
  }

}

