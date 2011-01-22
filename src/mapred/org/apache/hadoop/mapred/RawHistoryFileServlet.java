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

package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;

public class RawHistoryFileServlet extends HttpServlet {

  private ServletContext servletContext;

  @Override
  public void init(ServletConfig servletConfig) throws ServletException {
    super.init(servletConfig);
    servletContext = servletConfig.getServletContext();
  }

  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response)
      throws ServletException, IOException {

    String logFile = request.getParameter("logFile");

    if (logFile == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST,
              "Invalid log file name");
      return;
    }

    FileSystem fs = (FileSystem) servletContext.getAttribute("fileSys");
    JobConf jobConf = (JobConf) servletContext.getAttribute("jobConf");
    ACLsManager aclsManager = (ACLsManager) servletContext.getAttribute("aclManager");
    Path logFilePath = new Path(logFile);
    JobHistory.JobInfo job = null;
    try {
      job = JSPUtil.checkAccessAndGetJobInfo(request,
        response, jobConf, aclsManager, fs, logFilePath);
    } catch (InterruptedException e) {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Request interrupted");
    }

    if (job == null) {
      response.sendError(HttpServletResponse.SC_MOVED_PERMANENTLY,
                "Job details doesn't exist");
      return;
    }

    InputStream in = fs.open(logFilePath);
    try {
      IOUtils.copyBytes(in, response.getOutputStream(), 8192, false);
    } finally {
      in.close();
    }
  }
}
