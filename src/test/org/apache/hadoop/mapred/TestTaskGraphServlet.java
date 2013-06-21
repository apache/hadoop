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

import java.io.PrintWriter;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.mockito.Mockito;

public class TestTaskGraphServlet {
  @Test
  public void testTaskGraphServletShouldNotReturnEmptyContForNotasks()
      throws Exception {
    String jobId = "job_201108291216_0002";
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn(jobId).when(request).getParameter("jobid");
    Mockito.doReturn("map").when(request).getParameter("type");

    JobTracker jobTracker = Mockito.mock(JobTracker.class);
    Mockito.doReturn(new TaskReport[0]).when(jobTracker).getMapTaskReports(
        JobID.forName(jobId));

    ServletContext servletContext = Mockito.mock(ServletContext.class);
    Mockito.doReturn(jobTracker).when(servletContext).getAttribute(
        "job.tracker");

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    PrintWriter printWriter = Mockito.mock(PrintWriter.class);
    Mockito.doReturn(printWriter).when(response).getWriter();

    TaskGraphServlet taskGraphServlet = getTaskGraphServlet(servletContext);

    taskGraphServlet.doGet(request, response);
    Mockito.verify(printWriter, Mockito.atLeastOnce()).print("</svg>");
  }

  private TaskGraphServlet getTaskGraphServlet(
      final ServletContext servletContext) {
    TaskGraphServlet taskGraphServlet = new TaskGraphServlet() {
      private static final long serialVersionUID = 1L;

      @Override
      public ServletContext getServletContext() {
        return servletContext;
      }
    };
    return taskGraphServlet;
  }
}
