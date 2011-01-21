<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapred.JobHistory.*"
%>
<jsp:include page="loadhistory.jsp">
  <jsp:param name="jobid" value="<%=request.getParameter("jobid") %>"/>
  <jsp:param name="jobTrackerId" value="<%=request.getParameter("jobTrackerId") %>"/>
</jsp:include>
<%!	private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ; %>

<%	
  String jobid = request.getParameter("jobid");
  String logFile = request.getParameter("logFile");
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String taskid = request.getParameter("taskid"); 
  JobHistory.JobInfo job = (JobHistory.JobInfo)
                              request.getSession().getAttribute("job");
  JobHistory.Task task = job.getAllTasks().get(taskid); 
  String type = task.get(Keys.TASK_TYPE);
%>
<html>
<body>
<h2><%=taskid %> attempts for <a href="jobdetailshistory.jsp?jobid=<%=jobid%>&&logFile=<%=encodedLogFileName%>"> <%=jobid %> </a></h2>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Start Time</td>
<%	
  if (Values.REDUCE.name().equals(type)) {
%>
    <td>Shuffle Finished</td><td>Sort Finished</td>
<%
  }
%>
<td>Finish Time</td><td>Host</td><td>Error</td><td>Task Logs</td></tr>
<%
  for (JobHistory.TaskAttempt attempt : task.getTaskAttempts().values()) {
    printTaskAttempt(attempt, type, out);
  }
%>
</table>
</center>
<%	
  if (Values.MAP.name().equals(type)) {
%>
<h3>Input Split Locations</h3>
<table border="2" cellpadding="5" cellspacing="2">
<%
    for (String split : StringUtils.split(task.get(Keys.SPLITS)))
    {
      out.println("<tr><td>" + split + "</td></tr>");
    }
%>
</table>    
<%
  }
%>
<%!
  private void printTaskAttempt(JobHistory.TaskAttempt taskAttempt,
                                String type, JspWriter out) 
  throws IOException {
    out.print("<tr>"); 
    out.print("<td>" + taskAttempt.get(Keys.TASK_ATTEMPT_ID) + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getLong(Keys.START_TIME), 0 ) + "</td>"); 
    if (Values.REDUCE.name().equals(type)) {
      JobHistory.ReduceAttempt reduceAttempt = 
            (JobHistory.ReduceAttempt)taskAttempt; 
      out.print("<td>" + 
                StringUtils.getFormattedTimeWithDiff(dateFormat, 
                reduceAttempt.getLong(Keys.SHUFFLE_FINISHED), 
                reduceAttempt.getLong(Keys.START_TIME)) + "</td>"); 
      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
                reduceAttempt.getLong(Keys.SORT_FINISHED), 
                reduceAttempt.getLong(Keys.SHUFFLE_FINISHED)) + "</td>"); 
    }
    out.print("<td>"+ StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getLong(Keys.FINISH_TIME), 
              taskAttempt.getLong(Keys.START_TIME) ) + "</td>"); 
    out.print("<td>" + taskAttempt.get(Keys.HOSTNAME) + "</td>");
    out.print("<td>" + taskAttempt.get(Keys.ERROR) + "</td>");

    // Print task log urls
    out.print("<td>");	
    String taskLogsUrl = JobHistory.getTaskLogsUrl(taskAttempt);
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
    out.print("</tr>"); 
  }
%>
</body>
</html>
