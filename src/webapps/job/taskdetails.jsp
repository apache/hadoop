<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.util.*"
%>
<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>
<%
  String jobid = request.getParameter("jobid");
  JobTracker tracker = JobTracker.getTracker();
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  String tipid = request.getParameter("taskid");
  TaskStatus[] ts = (job != null) ? tracker.getTaskStatuses(jobid, tipid): null;
%>

<html>
<title>Hadoop Task Details</title>
<body>
<h1>Job <%=jobid%></h1>

<hr>

<h2>All Task Attempts</h2>
<center>
<%
	if( ts.length == 0 ) {
%>
		<h3>No Task Attempts found</h3>
<%
	}else{
%>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Machine</td><td>Status</td><td>Progress</td><td>Start Time</td> 
  <%
	if( ! ts[0].getIsMap() ) { 
  %>
<td>Shuffle Finished</td><td>Sort Finished</td>
  <%
	}
  %>
<td>Finish Time</td><td>Errors</td><td>Task Logs</td></tr>
  <%
    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      String taskTrackerName = status.getTaskTracker();
      TaskTrackerStatus taskTracker = tracker.getTaskTracker(taskTrackerName);
      out.print("<tr><td>" + status.getTaskId() + "</td>");
      String taskAttemptTracker = null;
      if (taskTracker == null) {
        out.print("<td>" + taskTrackerName + "</td>");
      } else {
        taskAttemptTracker = "http://" + taskTracker.getHost() + ":" +
        	taskTracker.getHttpPort();
        out.print("<td><a href=\"" + taskAttemptTracker + "\">" +  
        	taskTracker.getHost() + "</a></td>");
      }
      out.print("<td>" + status.getRunState() + "</td>");
      out.print("<td>"+ StringUtils.formatPercent(status.getProgress(),2) + 
                "</td>");
      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,  
          status.getStartTime(), 0) + "</td>");
      if( ! ts[i].getIsMap() ) {
	      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
	          status.getShuffleFinishTime(), status.getStartTime()) + "</td>");
	      out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
	          status.getSortFinishTime(), status.getShuffleFinishTime()) + "</td>");
      }
      out.println("<td>"+ StringUtils.getFormattedTimeWithDiff(dateFormat, 
          status.getFinishTime(), status.getStartTime()) + "</td>");
      
      out.print("<td><pre>");
      List<String> failures = tracker.getTaskDiagnostics(jobid, tipid,
                                                         status.getTaskId());
      if (failures == null) {
        out.print("&nbsp;");
      } else {
        for(Iterator<String> itr = failures.iterator(); itr.hasNext(); ) {
          out.print(itr.next());
          if (itr.hasNext()) {
            out.print("\n-------\n");
          }
        }
      }
      out.print("</pre></td>");
      out.print("<td>");
      if (taskAttemptTracker == null) {
        out.print("n/a");
      } else {
        String taskLogUrl = taskAttemptTracker + "/tasklog.jsp?taskid=" + 
                              status.getTaskId();
        String tailFourKBUrl = taskLogUrl + "&tail=true&tailsize=4096";
        String tailEightKBUrl = taskLogUrl + "&tail=true&tailsize=8192";
        String entireLogUrl = taskLogUrl + "&all=true";
        out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
        out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
        out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
      }
      out.println("</td></tr>\n");
    }
  }
  %>
</table>
</center>

<hr>
<a href="/jobdetails.jsp?jobid=<%=jobid%>">Go back to the job</a><br>
<a href="/jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
