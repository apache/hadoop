<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
%>
<%
  String jobid = request.getParameter("jobid");
  JobTracker tracker = JobTracker.getTracker();
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  JobProfile profile = (job != null) ? (job.getProfile()) : null;
  JobStatus status = (job != null) ? (job.getStatus()) : null;

  TaskReport[] mapTaskReports = (job != null) ? tracker.getMapTaskReports(jobid) : null;
  TaskReport[] reduceTaskReports = (job != null) ? tracker.getReduceTaskReports(jobid) : null;
%>

<html>
<title>Hadoop MapReduce Job Details</title>
<body>
<%
  if (job == null) {
    %>
    No job found<br>
    <%
  } else {
    %>
<h1>Job '<%=jobid%>'</h1>

<b>Job File:</b> <%=profile.getJobFile()%><br>
<b>The job started at:</b> <%= new Date(job.getStartTime())%><br>
<%
  if (status.getRunState() == JobStatus.RUNNING) {
    out.print("The job is still running.<br>\n");
  } else if (status.getRunState() == JobStatus.SUCCEEDED) {
    out.print("<b>The job completed at:</b> " + new Date(job.getFinishTime()) + "<br>\n");
  } else if (status.getRunState() == JobStatus.FAILED) {
    out.print("<b>The job failed at:</b> " + new Date(job.getFinishTime()) + "<br>\n");
  }
%>
<hr>

<h2>Map Tasks</h2>
  <center>
  <table border=2 cellpadding="5" cellspacing="2">
  <tr><td align="center">Task Id</td><td>Complete</td><td>State</td><td>Errors</td></tr>

  <%

    for (int i = 0; i < mapTaskReports.length; i++) {
      TaskReport report = mapTaskReports[i];

      out.print("<tr><td>" + report.getTaskId() + "</td>");
      out.print("<td>" + report.getProgress() + "</td>");
      out.print("<td>" + report.getState() + "</td>");

      String[] diagnostics = report.getDiagnostics();
      for (int j = 0; j < diagnostics.length ; j++) {
        out.print("<td>" + diagnostics[j] + "</td>");
      }
      out.print("</tr>\n");
    }
  %>
  </table>
  </center>
<hr>


<h2>Reduce Tasks</h2>
  <center>
  <table border=2 cellpadding="5" cellspacing="2">
  <tr><td align="center">Task Id</td><td>Complete</td><td>State</td><td>Errors</td></tr>

  <%
    for (int i = 0; i < reduceTaskReports.length; i++) {
      TaskReport report = reduceTaskReports[i];

      out.print("<tr><td>" + report.getTaskId() + "</td>");
      out.print("<td>" + report.getProgress() + "</td>");
      out.print("<td>" + report.getState() + "</td>");

      String[] diagnostics = report.getDiagnostics();
      for (int j = 0; j < diagnostics.length ; j++) {
        out.print("<td>" + diagnostics[j] + "</td>");
      }
      out.print("</tr>\n");
    }
  %>
  </table>
  </center>
  <%
  }
%>

<hr>
<a href="/jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
