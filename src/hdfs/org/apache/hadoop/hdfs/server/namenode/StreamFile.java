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

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.conf.*;

public class StreamFile extends DfsServlet {
  static InetSocketAddress nameNodeAddr;
  static DataNode datanode = null;
  private static final Configuration masterConf = new Configuration();
  static {
    if ((datanode = DataNode.getDataNode()) != null) {
      nameNodeAddr = datanode.getNameNodeAddr();
    }
  }
  
  /** getting a client for connecting to dfs */
  protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException {
    Configuration conf = new Configuration(masterConf);
    UnixUserGroupInformation.saveToConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, getUGI(request));
    return new DFSClient(nameNodeAddr, conf);
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    String filename = request.getParameter("filename");
    if (filename == null || filename.length() == 0) {
      response.setContentType("text/plain");
      PrintWriter out = response.getWriter();
      out.print("Invalid input");
      return;
    }
    DFSClient dfs = getDFSClient(request);
    FSInputStream in = dfs.open(filename);
    OutputStream os = response.getOutputStream();
    response.setHeader("Content-Disposition", "attachment; filename=\"" + 
                       filename + "\"");
    response.setContentType("application/octet-stream");
    byte buf[] = new byte[4096];
    try {
      int bytesRead;
      while ((bytesRead = in.read(buf)) != -1) {
        os.write(buf, 0, bytesRead);
      }
    } finally {
      in.close();
      os.close();
      dfs.close();
    }
  }
}
