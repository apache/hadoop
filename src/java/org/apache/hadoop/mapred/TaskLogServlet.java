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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.*;

import org.apache.hadoop.util.StringUtils;

/**
 * A servlet that is run by the TaskTrackers to provide the task logs via http.
 */
public class TaskLogServlet extends HttpServlet {
  private void printTaskLog(HttpServletResponse response,
                            OutputStream out, String taskId, 
                            long start, long end, boolean plainText, 
                            TaskLog.LogName filter) throws IOException {
    if (!plainText) {
      out.write(("<br><b><u>" + filter + " logs</u></b><br>\n" +
                 "<table border=2 cellpadding=\"2\">\n" +
                 "<tr><td><pre>\n").getBytes());
    }

    try {
      InputStream taskLogReader = 
        new TaskLog.Reader(taskId, filter, start, end);
      byte[] b = new byte[65536];
      int result;
      while (true) {
        result = taskLogReader.read(b);
        if (result > 0) {
          out.write(b, 0, result);
        } else {
          break;
        }
      }
      taskLogReader.close();
      if( !plainText ) {
        out.write("</pre></td></tr></table><hr><br>\n".getBytes());
      }
    } catch (IOException ioe) {
      if (filter == TaskLog.LogName.DEBUGOUT) {
        if (!plainText) {
           out.write("</pre></td></tr></table><hr><br>\n".getBytes());
         }
        // do nothing
      }
      else {
        response.sendError(HttpServletResponse.SC_GONE,
                         "Failed to retrieve " + filter + " log for task: " + 
                         taskId);
        out.write(("TaskLogServlet exception:\n" + 
                 StringUtils.stringifyException(ioe) + "\n").getBytes());
      }
    }
  }

  /**
   * Get the logs via http.
   */
  public void doGet(HttpServletRequest request, 
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    long start = 0;
    long end = -1;
    boolean plainText = false;
    TaskLog.LogName filter = null;

    String taskId = request.getParameter("taskid");
    if (taskId == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, 
                         "Argument taskid is required");
      return;
    }
    String logFilter = request.getParameter("filter");
    if (logFilter != null) {
      try {
        filter = TaskLog.LogName.valueOf(TaskLog.LogName.class, 
                                         logFilter.toUpperCase());
      } catch (IllegalArgumentException iae) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                           "Illegal value for filter: " + logFilter);
        return;
      }
    }
    
    String sLogOff = request.getParameter("start");
    if (sLogOff != null) {
      start = Long.valueOf(sLogOff).longValue();
    }
    
    String sLogEnd = request.getParameter("end");
    if (sLogEnd != null) {
      end = Long.valueOf(sLogEnd).longValue();
    }
    
    String sPlainText = request.getParameter("plaintext");
    if (sPlainText != null) {
      plainText = Boolean.valueOf(sPlainText);
    }

    OutputStream out = response.getOutputStream();
    if( !plainText ) {
      out.write(("<html>\n" +
                 "<title>Task Logs: '" + taskId + "'</title>\n" +
                 "<body>\n" +
                 "<h1>Task Logs: '" +  taskId +  "'</h1><br>\n").getBytes()); 

      if (filter == null) {
        printTaskLog(response, out, taskId, start, end, plainText, 
                     TaskLog.LogName.STDOUT);
        printTaskLog(response, out, taskId, start, end, plainText, 
                     TaskLog.LogName.STDERR);
        printTaskLog(response, out, taskId, start, end, plainText, 
                     TaskLog.LogName.SYSLOG);
        printTaskLog(response, out, taskId, start, end, plainText, 
                TaskLog.LogName.DEBUGOUT);
      } else {
        printTaskLog(response, out, taskId, start, end, plainText, filter);
      }
      
      out.write("</body></html>\n".getBytes());
      out.close();
    } else {
      printTaskLog(response, out, taskId, start, end, plainText, filter);
    } 
  }
}
