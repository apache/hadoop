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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DatanodeJspHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.jetty.InclusiveByteRange;

@InterfaceAudience.Private
public class StreamFile extends DfsServlet {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

  public static final String CONTENT_LENGTH = "Content-Length";

  /** getting a client for connecting to dfs */
  protected DFSClient getDFSClient(HttpServletRequest request)
      throws IOException, InterruptedException {
    final Configuration conf =
      (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
    UserGroupInformation ugi = getUGI(request, conf);
    final ServletContext context = getServletContext();
    final DataNode datanode = (DataNode) context.getAttribute("datanode");
    return DatanodeJspHelper.getDFSClient(request, datanode, conf, ugi);
  }
  
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    final String path = request.getPathInfo() != null ? 
                                        request.getPathInfo() : "/";
    final String filename = JspHelper.validatePath(path);
    if (filename == null) {
      response.setContentType("text/plain");
      PrintWriter out = response.getWriter();
      out.print("Invalid input");
      return;
    }
    
    Enumeration<?> reqRanges = request.getHeaders("Range");
    if (reqRanges != null && !reqRanges.hasMoreElements())
      reqRanges = null;

    DFSClient dfs;
    try {
      dfs = getDFSClient(request);
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
      return;
    }
    
    final DFSInputStream in = dfs.open(filename);
    final long fileLen = in.getFileLength();
    OutputStream os = response.getOutputStream();

    try {
      if (reqRanges != null) {
        List<?> ranges = InclusiveByteRange.satisfiableRanges(reqRanges,
                                                           fileLen);
        StreamFile.sendPartialData(in, os, response, fileLen, ranges);
      } else {
        // No ranges, so send entire file
        response.setHeader("Content-Disposition", "attachment; filename=\"" + 
                           filename + "\"");
        response.setContentType("application/octet-stream");
        response.setHeader(CONTENT_LENGTH, "" + fileLen);
        StreamFile.copyFromOffset(in, os, 0L, fileLen);
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
  
  /**
   * Send a partial content response with the given range. If there are
   * no satisfiable ranges, or if multiple ranges are requested, which
   * is unsupported, respond with range not satisfiable.
   *
   * @param in stream to read from
   * @param out stream to write to
   * @param response http response to use
   * @param contentLength for the response header
   * @param ranges to write to respond with
   * @throws IOException on error sending the response
   */
  static void sendPartialData(FSInputStream in,
                              OutputStream out,
                              HttpServletResponse response,
                              long contentLength,
                              List<?> ranges)
      throws IOException {
    if (ranges == null || ranges.size() != 1) {
      response.setContentLength(0);
      response.setStatus(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
      response.setHeader("Content-Range",
                InclusiveByteRange.to416HeaderRangeString(contentLength));
    } else {
      InclusiveByteRange singleSatisfiableRange =
        (InclusiveByteRange)ranges.get(0);
      long singleLength = singleSatisfiableRange.getSize(contentLength);
      response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
      response.setHeader("Content-Range", 
        singleSatisfiableRange.toHeaderRangeString(contentLength));
      copyFromOffset(in, out,
                     singleSatisfiableRange.getFirst(contentLength),
                     singleLength);
    }
  }

  /* Copy count bytes at the given offset from one stream to another */
  static void copyFromOffset(FSInputStream in, OutputStream out, long offset,
      long count) throws IOException {
    in.seek(offset);
    IOUtils.copyBytes(in, out, count);
  }
}
