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
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.jetty.InclusiveByteRange;

@InterfaceAudience.Private
public class StreamFile extends DfsServlet {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

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
    final Configuration conf =
      (Configuration) getServletContext().getAttribute("name.conf");
    
    UserGroupInformation ugi = getUGI(request, conf);
    DFSClient client = ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
      @Override
      public DFSClient run() throws IOException {
        return new DFSClient(nameNodeAddr, conf);
      }
    });
    
    return client;
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final String filename = JspHelper.validatePath(
        request.getParameter("filename"));
    if (filename == null) {
      response.setContentType("text/plain");
      PrintWriter out = response.getWriter();
      out.print("Invalid input");
      return;
    }
    
    Enumeration reqRanges = request.getHeaders("Range");
    if (reqRanges != null && !reqRanges.hasMoreElements())
      reqRanges = null;

    DFSClient dfs;
    try {
      dfs = getDFSClient(request);
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
      return;
    }
    
    long fileLen = dfs.getFileInfo(filename).getLen();
    FSInputStream in = dfs.open(filename);
    OutputStream os = response.getOutputStream();

    try {
      if (reqRanges != null) {
        List ranges = InclusiveByteRange.satisfiableRanges(reqRanges,
                                                           fileLen);
        StreamFile.sendPartialData(in, os, response, fileLen, ranges);
      } else {
        // No ranges, so send entire file
        response.setHeader("Content-Disposition", "attachment; filename=\"" + 
                           filename + "\"");
        response.setContentType("application/octet-stream");
        StreamFile.writeTo(in, os, 0L, fileLen);
      }
    } finally {
      in.close();
      os.close();
      dfs.close();
    }      
  }
  
  static void sendPartialData(FSInputStream in,
                              OutputStream os,
                              HttpServletResponse response,
                              long contentLength,
                              List ranges)
  throws IOException {

    if (ranges == null || ranges.size() != 1) {
      //  if there are no satisfiable ranges, or if multiple ranges are
      // requested (we don't support multiple range requests), send 416 response
      response.setContentLength(0);
      int status = HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE;
      response.setStatus(status);
      response.setHeader("Content-Range", 
                InclusiveByteRange.to416HeaderRangeString(contentLength));
    } else {
      //  if there is only a single valid range (must be satisfiable 
      //  since were here now), send that range with a 206 response
      InclusiveByteRange singleSatisfiableRange =
        (InclusiveByteRange)ranges.get(0);
      long singleLength = singleSatisfiableRange.getSize(contentLength);
      response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
      response.setHeader("Content-Range", 
        singleSatisfiableRange.toHeaderRangeString(contentLength));
      System.out.println("first: "+singleSatisfiableRange.getFirst(contentLength));
      System.out.println("singleLength: "+singleLength);
      
      StreamFile.writeTo(in,
                         os,
                         singleSatisfiableRange.getFirst(contentLength),
                         singleLength);
    }
  }
  
  static void writeTo(FSInputStream in,
                      OutputStream os,
                      long start,
                      long count) 
  throws IOException {
    byte buf[] = new byte[4096];
    long bytesRemaining = count;
    int bytesRead;
    int bytesToRead;

    in.seek(start);

    while (true) {
      // number of bytes to read this iteration
      bytesToRead = (int)(bytesRemaining<buf.length ? 
                                                      bytesRemaining:
                                                      buf.length);
      
      // number of bytes actually read this iteration
      bytesRead = in.read(buf, 0, bytesToRead);

      // if we can't read anymore, break
      if (bytesRead == -1) {
        break;
      } 
      
      os.write(buf, 0, bytesRead);

      bytesRemaining -= bytesRead;

      // if we don't need to read anymore, break
      if (bytesRemaining <= 0) {
        break;
      }

    } 
  }
}
