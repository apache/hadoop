<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>

<%!
  JobTracker tracker = JobTracker.getTracker();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  
  private void printTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                double completePercent,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    int failures = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
      failures += task.numTaskFailures();
    }
    out.print("<tr><th><a href=\"/jobtasks.jsp?jobid=" + jobId + 
              "&type="+ kind + "&pagenum=1\">" + kind + "</a></th><td>" + 
              StringUtils.formatPercent(completePercent, 2) +
              "</td><td>" + totalTasks + "</td><td>" + 
              (totalTasks - runningTasks - finishedTasks - killedTasks) + 
              "</td><td>" +
              runningTasks + "</td><td>" +
              finishedTasks + "</td><td>" +
              killedTasks +
              "</td><td><a href=\"/jobfailures.jsp?jobid=" + jobId +
              "&kind=" + kind + "\">" +
              failures + "</a></td></tr>\n");
  }
           
  private void printJobStatus(JspWriter out, 
                              String jobId) throws IOException {
    JobInProgress job = (JobInProgress) tracker.getJob(jobId);
    if (job == null) {
      out.print("<b>Job " + jobId + " not found.</b><br>\n");
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();
    out.print("<b>User:</b> " + profile.getUser() + "<br>\n");
    out.print("<b>Job Name:</b> " + profile.getJobName() + "<br>\n");
    if (runState == JobStatus.RUNNING) {
      out.print("<b>Job File:</b> <a href=\"/jobconf.jsp?jobid=" + jobId + "\">" + 
          profile.getJobFile() + "</a><br>\n");
    } else {
      out.print("<b>Job File:</b> " + profile.getJobFile() + "<br>\n");
    }
    out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
    if (runState == JobStatus.RUNNING) {
      out.print("<b>Status:</b> Running<br>\n");
    } else {
      if (runState == JobStatus.SUCCEEDED) {
        out.print("<b>Status:</b> Succeeded<br>\n");
      } else if (runState == JobStatus.FAILED) {
        out.print("<b>Status:</b> Failed<br>\n");
      }
      out.print("<b>Finished at:</b> " + new Date(job.getFinishTime()) +
                "<br>\n");
    }
    out.print("<hr>\n");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>Kind</th><th>% Complete</th><th>Num Tasks</th>" +
              "<th>Pending</th><th>Running</th><th>Complete</th>" +
              "<th>Killed</th>" +
              "<th><a href=\"/jobfailures.jsp?jobid=" + jobId + 
              "\">Failures</a></th></tr>\n");
    printTaskSummary(out, jobId, "map", status.mapProgress(), 
                     job.getMapTasks());
    printTaskSummary(out, jobId, "reduce", status.reduceProgress(),
                     job.getReduceTasks());
    out.print("</table>\n");
  }
%>

<%
    String jobid = request.getParameter("jobid");
%>

<html>
<head>
<meta http-equiv="refresh" content=60>
<title>Hadoop <%=jobid%> on <%=trackerName%></title>
</head>
<body>
<h1>Hadoop <%=jobid%> on <a href="/jobtracker.jsp"><%=trackerName%></a></h1>

<% 
    printJobStatus(out, jobid); 
%>

<hr>
<a href="/jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
