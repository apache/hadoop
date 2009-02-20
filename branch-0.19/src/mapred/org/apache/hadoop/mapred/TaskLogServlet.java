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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.StringUtils;

/**
 * A servlet that is run by the TaskTrackers to provide the task logs via http.
 */
public class TaskLogServlet extends HttpServlet {
  private static final long serialVersionUID = -6615764817774487321L;
  
  private boolean haveTaskLog(TaskAttemptID taskId, TaskLog.LogName type) {
    File f = TaskLog.getTaskLogFile(taskId, type);
    return f.canRead();
  }

  /**
   * Construct the taskLogUrl
   * @param taskTrackerHostName
   * @param httpPort
   * @param taskAttemptID
   * @return the taskLogUrl
   */
  public static String getTaskLogUrl(String taskTrackerHostName,
      String httpPort, String taskAttemptID) {
    return ("http://" + taskTrackerHostName + ":" + httpPort
        + "/tasklog?taskid=" + taskAttemptID);
  }

  /**
   * Find the next quotable character in the given array.
   * @param data the bytes to look in
   * @param offset the first index to look in
   * @param end the index after the last one to look in
   * @return the index of the quotable character or end if none was found
   */
  private static int findFirstQuotable(byte[] data, int offset, int end) {
    while (offset < end) {
      switch (data[offset]) {
      case '<':
      case '>':
      case '&':
        return offset;
      default:
        offset += 1;
      }
    }
    return offset;
  }

  private static void quotedWrite(OutputStream out, byte[] data, int offset,
                                  int length) throws IOException {
    int end = offset + length;
    while (offset < end) {
      int next = findFirstQuotable(data, offset, end);
      out.write(data, offset, next - offset);
      offset = next;
      if (offset < end) {
        switch (data[offset]) {
        case '<':
          out.write("&lt;".getBytes());
          break;
        case '>':
          out.write("&gt;".getBytes());
          break;
        case '&':
          out.write("&amp;".getBytes());
          break;
        default:
          out.write(data[offset]);
          break;
        }
        offset += 1;
      }
    }
  }

  private void printTaskLog(HttpServletResponse response,
                            OutputStream out, TaskAttemptID taskId, 
                            long start, long end, boolean plainText, 
                            TaskLog.LogName filter, boolean isCleanup) 
  throws IOException {
    if (!plainText) {
      out.write(("<br><b><u>" + filter + " logs</u></b><br>\n" +
                 "<pre>\n").getBytes());
    }

    try {
      InputStream taskLogReader = 
        new TaskLog.Reader(taskId, filter, start, end, isCleanup);
      byte[] b = new byte[65536];
      int result;
      while (true) {
        result = taskLogReader.read(b);
        if (result > 0) {
          if (plainText) {
            out.write(b, 0, result); 
          } else {
            quotedWrite(out, b, 0, result);
          }
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
           out.write("</pre><hr><br>\n".getBytes());
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
  @Override
  public void doGet(HttpServletRequest request, 
                    HttpServletResponse response
                    ) throws ServletException, IOException {
    long start = 0;
    long end = -1;
    boolean plainText = false;
    TaskLog.LogName filter = null;
    boolean isCleanup = false;

    String taskIdStr = request.getParameter("taskid");
    if (taskIdStr == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, 
                         "Argument taskid is required");
      return;
    }
    TaskAttemptID taskId = TaskAttemptID.forName(taskIdStr);
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
    
    String sCleanup = request.getParameter("cleanup");
    if (sCleanup != null) {
      isCleanup = Boolean.valueOf(sCleanup);
    }
    
    OutputStream out = response.getOutputStream();
    if( !plainText ) {
      out.write(("<html>\n" +
                 "<title>Task Logs: '" + taskId + "'</title>\n" +
                 "<body>\n" +
                 "<h1>Task Logs: '" +  taskId +  "'</h1><br>\n").getBytes()); 

      if (filter == null) {
        printTaskLog(response, out, taskId, start, end, plainText, 
                     TaskLog.LogName.STDOUT, isCleanup);
        printTaskLog(response, out, taskId, start, end, plainText, 
                     TaskLog.LogName.STDERR, isCleanup);
        printTaskLog(response, out, taskId, start, end, plainText, 
                     TaskLog.LogName.SYSLOG, isCleanup);
        if (haveTaskLog(taskId, TaskLog.LogName.DEBUGOUT)) {
          printTaskLog(response, out, taskId, start, end, plainText, 
                       TaskLog.LogName.DEBUGOUT, isCleanup);
        }
        if (haveTaskLog(taskId, TaskLog.LogName.PROFILE)) {
          printTaskLog(response, out, taskId, start, end, plainText, 
                       TaskLog.LogName.PROFILE, isCleanup);
        }
      } else {
        printTaskLog(response, out, taskId, start, end, plainText, filter,
                     isCleanup);
      }
      
      out.write("</body></html>\n".getBytes());
      out.close();
    } else if (filter == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST,
          "You must supply a value for `filter' (STDOUT, STDERR, or SYSLOG) if you set plainText = true");
    } else {
      printTaskLog(response, out, taskId, start, end, plainText, filter, 
                   isCleanup);
    } 
  }
}
