<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>

<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
%>
<%!       
  private void printBlackListedTrackers(JspWriter out, 
                             JobInProgress job) throws IOException {
    Map<String, Integer> trackerErrors = job.getTaskTrackerErrors();
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>TaskTracker</th><th>No. of Failures</th></tr>\n");
    int maxErrorsPerTracker = job.getJobConf().getMaxTaskFailuresPerTracker();
    for (Map.Entry<String,Integer> e : trackerErrors.entrySet()) {
      if (e.getValue().intValue() >= maxErrorsPerTracker) {
        out.print("<tr><td>" + e.getKey() + "</td><td>" + e.getValue() + 
            "</td></tr>\n");
      }
    }
    out.print("</table>\n");
  }
%>

<%
    String jobId = request.getParameter("jobid");
    if (jobId == null) {
  	  out.println("<h2>Missing 'jobid' for fetching black-listed tasktrackers!</h2>");
  	  return;
    }
    
    JobInProgress job = (JobInProgress) tracker.getJob(JobID.forName(jobId));
    if (job == null) {
      out.print("<b>Job " + jobId + " not found.</b><br>\n");
      return;
    }
%>

<html>
<title>Hadoop <%=jobId%>'s black-listed tasktrackers</title>
<body>
<h1>Hadoop <a href="jobdetails.jsp?jobid=<%=jobId%>"><%=jobId%></a> - 
Black-listed task-trackers</h1>

<% 
    printBlackListedTrackers(out, job); 
%>

<hr>
<a href="jobdetails.jsp?jobid=<%=jobId%>">Go back to <%=jobId%></a><br>
<%
out.println(ServletUtil.htmlFooter());
%>
