<%
/*
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
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapreduce.TaskType"
  import="org.apache.hadoop.mapreduce.Counters"
  import="org.apache.hadoop.mapreduce.TaskID"
  import="org.apache.hadoop.mapreduce.TaskAttemptID"
  import="org.apache.hadoop.mapreduce.jobhistory.*"
%>

<%!	private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ; %>
<%!	private static final long serialVersionUID = 1L;
%>

<%	
  String logFile = request.getParameter("logFile");
  String tipid = request.getParameter("tipid"); 
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
  JobHistoryParser.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobTracker, fs, new Path(logFile));
  if (job == null) {
    return;
  }
  JobHistoryParser.TaskInfo task = job.getAllTasks().get(TaskID.forName(tipid)); 
  TaskType type = task.getTaskType();
%>
<html>
<body>
<h2><%=tipid %> attempts for <a href="jobdetailshistory.jsp?logFile=<%=logFile%>"> <%=job.getJobId() %> </a></h2>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Start Time</td>
<%	
  if (TaskType.REDUCE.equals(type)) {
%>
    <td>Shuffle Finished</td><td>Sort Finished</td>
<%
  }
%>
<td>Finish Time</td><td>Host</td><td>Error</td><td>Task Logs</td>
<td>Counters</td></tr>

<%
  for (JobHistoryParser.TaskAttemptInfo attempt : task.getAllTaskAttempts().values()) {
    printTaskAttempt(attempt, type, out, logFile);
  }
%>
</table>
</center>
<%	
  if (TaskType.MAP.equals(type)) {
%>
<h3>Input Split Locations</h3>
<table border="2" cellpadding="5" cellspacing="2">
<%
    for (String split : StringUtils.split(task.getSplitLocations()))
    {
      out.println("<tr><td>" + split + "</td></tr>");
    }
%>
</table>    
<%
  }
%>
<%!
  private void printTaskAttempt(JobHistoryParser.TaskAttemptInfo taskAttempt,
                                TaskType type, JspWriter out, String logFile) 
  throws IOException {
    out.print("<tr>"); 
    out.print("<td>" + taskAttempt.getAttemptId() + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getStartTime(), 0 ) + "</td>"); 
    if (TaskType.REDUCE.equals(type)) {
      out.print("<td>" + 
                StringUtils.getFormattedTimeWithDiff(dateFormat, 
                taskAttempt.getShuffleFinishTime(),
                taskAttempt.getStartTime()) + "</td>"); 
      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
                taskAttempt.getSortFinishTime(),
                taskAttempt.getShuffleFinishTime()) + "</td>"); 
    }
    out.print("<td>"+ StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getFinishTime(),
              taskAttempt.getStartTime()) + "</td>"); 
    out.print("<td>" + taskAttempt.getHostname() + "</td>");
    out.print("<td>" + HtmlQuoting.quoteHtmlChars(taskAttempt.getError()) +
              "</td>");

    // Print task log urls
    out.print("<td>");	
    String taskLogsUrl = HistoryViewer.getTaskLogsUrl(taskAttempt);
    if (taskLogsUrl != null) {
	    String tailFourKBUrl = taskLogsUrl + "&start=-4097";
	    String tailEightKBUrl = taskLogsUrl + "&start=-8193";
	    String entireLogUrl = taskLogsUrl + "&all=true";
	    out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
	    out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
	    out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
    } else {
        out.print("n/a");
    }
    out.print("</td>");
    Counters counters = taskAttempt.getCounters();
    if (counters != null) {
      TaskAttemptID attemptId = taskAttempt.getAttemptId();
      out.print("<td>" 
       + "<a href=\"/taskstatshistory.jsp?attemptid=" + attemptId
           + "&logFile=" + logFile + "\">"
           + counters.countCounters() + "</a></td>");
    } else {
      out.print("<td></td>");
    }
    out.print("</tr>"); 
  }
%>
</body>
</html>
