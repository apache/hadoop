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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.UserGroupInformation;

public class StreamFile extends DfsServlet {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

  public static final String CONTENT_LENGTH = "Content-Length";

  static InetSocketAddress nameNodeAddr;
  static DataNode datanode = null;
  static {
    if ((datanode = DataNode.getDataNode()) != null) {
      nameNodeAddr = datanode.getNameNodeAddr();
    }
  }
  
  /** getting a client for connecting to dfs */
  protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException, InterruptedException {

    Configuration conf =
      (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    UserGroupInformation ugi = getUGI(request, conf);

    return JspHelper.getDFSClient(ugi, nameNodeAddr, conf);
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final String filename = request.getPathInfo() != null ?
        request.getPathInfo() : "/";
    if (filename == null || filename.length() == 0) {
      response.setContentType("text/plain");
      PrintWriter out = response.getWriter();
      out.print("Invalid input");
      return;
    }
    
    DFSClient dfs;
    try {
      dfs = getDFSClient(request);
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
      return;
    }
    
    final DFSClient.DFSInputStream in = dfs.open(filename);
    OutputStream os = response.getOutputStream();
    response.setHeader("Content-Disposition", "attachment; filename=\"" + 
                       filename + "\"");
    response.setContentType("application/octet-stream");
    response.setHeader(CONTENT_LENGTH, "" + in.getFileLength());
    byte buf[] = new byte[4096];
    try {
      int bytesRead;
      while ((bytesRead = in.read(buf)) != -1) {
        os.write(buf, 0, bytesRead);
      }
    } catch(IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("response.isCommitted()=" + response.isCommitted(), e);
      }
      throw e;
    } finally {
      try {
        in.close();
        os.close();
      } finally {
        dfs.close();
      }
    }
  }
}
