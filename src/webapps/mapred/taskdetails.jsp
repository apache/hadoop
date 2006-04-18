<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
%>
<%
  String jobid = request.getParameter("jobid");
  JobTracker tracker = JobTracker.getTracker();
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  String taskid = request.getParameter("taskid");
  TaskStatus[] ts = (job != null) ? tracker.getTaskStatuses(jobid, taskid): null;
%>

<%! 
  public void writeString(JspWriter out, int state) throws IOException{
    String sstate;
    if (state == TaskStatus.RUNNING){
      sstate = "RUNNING";
    }
    else if (state == TaskStatus.SUCCEEDED){
      sstate = "SUCCEDED";
    }
    else if (state == TaskStatus.FAILED){
      sstate = "FAILED";
    }
    else if (state == TaskStatus.UNASSIGNED){
      sstate = "UNASSIGNED";
    }
    else{
      sstate = "ERROR IN STATUS";
   }
    out.print(sstate);
  }
%>

<html>
<title>Hadoop Task Details</title>
<body>
<h1>Job '<%=jobid%>'</h1>

<hr>

<h2>All Task Attempts</h2>
<center>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Machine</td><td>Status</td><td>Errors</td></tr>

  <%
    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      out.print("<tr><td>" + status.getTaskId() + "</td>");
      out.print("<td>" + status.getHostname() + "</td>");
      out.print("<td>");
      writeString(out, status.getRunState()); 
      out.print("</td>");
      out.print("<td><pre>" + status.getDiagnosticInfo() + "</pre></td>");
      out.print("</tr>\n");
    }
  %>
</table>
</center>

<hr>
<%
out.print("<a href=\"/jobdetails.jsp?jobid=" + jobid + "\">" + "Go back to the Job" + "</a><br>");	
%>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
<hr>
<a href="/jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
