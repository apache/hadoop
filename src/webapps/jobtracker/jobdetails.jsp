<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.nutch.mapred.*"
%>
<%
  String jobid = request.getParameter("jobid");
  JobTracker tracker = JobTracker.getTracker();
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  JobProfile profile = (job != null) ? (job.getProfile()) : null;
  JobStatus status = (job != null) ? (job.getStatus()) : null;

  Vector mapTaskReports[] = (job != null) ? tracker.getMapTaskReport(jobid) : null;
  Vector reduceTaskReports[] = (job != null) ? tracker.getReduceTaskReport(jobid) : null;
%>

<html>
<title>Nutch MapReduce Job Details</title>
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
  <tr><td align="center">Map Task Id</td><td>Pct Complete</td><td>Diagnostic Data</td></tr>

  <%
    for (int i = 0; i < mapTaskReports.length; i++) {
      Vector v = mapTaskReports[i];
      String tipid = (String) v.elementAt(0);
      String progress = (String) v.elementAt(1);
      int diagnosticSize = ((Integer) v.elementAt(2)).intValue();

      out.print("<tr><td>" + tipid + "</td><td>" + progress + "</td><td>");
      for (int j = 0; j < diagnosticSize; j++) {
        Vector taskData = (Vector) v.elementAt(3 + ((2 * j)));
        String taskStateString = (String) v.elementAt(3 + ((2 * j) + 1));
        out.print(taskStateString);
        out.print("<b>");

        for (Iterator it2 = taskData.iterator(); it2.hasNext(); ) {
          out.print("" + it2.next());
          out.println("<b>");
        }
      }
      out.print("</td>");
      out.print("</tr>\n");
    }
  %>
  </table>
  </center>
<hr>


<h2>Reduce Tasks</h2>
  <center>
  <table border=2 cellpadding="5" cellspacing="2">
  <tr><td align="center">Reduce Task Id</td><td>Pct Complete</td><td>Diagnostic Data</td></tr>

  <%
    for (int i = 0; i < reduceTaskReports.length; i++) {
      Vector v = reduceTaskReports[i];
      String tipid = (String) v.elementAt(0);
      String progress = (String) v.elementAt(1);
      int diagnosticSize = ((Integer) v.elementAt(2)).intValue();

      out.print("<tr><td>" + tipid + "</td><td>" + progress + "</td><td>");
      for (int j = 0; j < diagnosticSize; j++) {
        Vector taskData = (Vector) v.elementAt(3 + ((2 * j)));
        String taskStateString = (String) v.elementAt(3 + ((2 * j) + 1));
        out.print(taskStateString);
        out.print("<b>");

        for (Iterator it2 = taskData.iterator(); it2.hasNext(); ) {
          out.print("" + it2.next());
          out.println("<b>");
        }
      }
      out.print("</td>");
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
<a href="http://www.nutch.org/">Nutch</a>, 2005.<br>
</body>
</html>
