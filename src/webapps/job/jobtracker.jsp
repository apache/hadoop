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
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
%>
<%!
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");

  public void generateJobTable(JspWriter out, String label, Vector jobs, int refresh) throws IOException {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\" colspan=\"9\"><b>" + label + " Jobs </b></td></tr>\n");
      if (jobs.size() > 0) {
        out.print("<tr><td><b>Jobid</b></td><td><b>User</b></td>");
        out.print("<td><b>Name</b></td>");
        out.print("<td><b>Map % Complete</b></td>");
        out.print("<td><b>Map Total</b></td>");
        out.print("<td><b>Maps Completed</b></td>");
        out.print("<td><b>Reduce % Complete</b></td>");
        out.print("<td><b>Reduce Total</b></td>");
        out.print("<td><b>Reduces Completed</b></td></tr>\n");
        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
          JobInProgress job = (JobInProgress) it.next();
          JobProfile profile = job.getProfile();
          JobStatus status = job.getStatus();
          JobID jobid = profile.getJobID();

          int desiredMaps = job.desiredMaps();
          int desiredReduces = job.desiredReduces();
          int completedMaps = job.finishedMaps();
          int completedReduces = job.finishedReduces();
          String name = profile.getJobName();

          out.print("<tr><td><a href=\"jobdetails.jsp?jobid=" + jobid + 
                     "&refresh=" + refresh + "\">" +
                     jobid + "</a></td>" +
                  "<td>" + profile.getUser() + "</td>" 
                    + "<td>" + ("".equals(name) ? "&nbsp;" : name) + "</td>" + 
                    "<td>" + 
                    StringUtils.formatPercent(status.mapProgress(),2) +
                    JspHelper.percentageGraph(status.mapProgress()  * 100, 80) +
                    "</td><td>" + 
                    desiredMaps + "</td><td>" + completedMaps + "</td><td>" + 
                    StringUtils.formatPercent(status.reduceProgress(),2) +
                    JspHelper.percentageGraph(status.reduceProgress() * 100, 80) +
                    "</td><td>" + 
                    desiredReduces + "</td><td> " + completedReduces + 
                    "</td></tr>\n");
        }
      } else {
        out.print("<tr><td align=\"center\" colspan=\"8\"><i>none</i></td></tr>\n");
      }
      out.print("</table>\n");
      out.print("</center>\n");
  }

  public void generateSummaryTable(JspWriter out,
                                   JobTracker tracker) throws IOException {
    ClusterStatus status = tracker.getClusterStatus();
    String tasksPerNode = status.getTaskTrackers() > 0 ?
      percentFormat.format(((double)(status.getMaxMapTasks() +
                      status.getMaxReduceTasks())) / status.getTaskTrackers()):
      "-";
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n"+
              "<tr><th>Maps</th><th>Reduces</th>" + 
              "<th>Total Submissions</th>" +
              "<th>Nodes</th><th>Map Task Capacity</th>" +
              "<th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th></tr>\n");
    out.print("<tr><td>" + status.getMapTasks() + "</td><td>" +
              status.getReduceTasks() + "</td><td>" + 
              tracker.getTotalSubmissions() +
              "</td><td><a href=\"machines.jsp\">" +
              status.getTaskTrackers() +
              "</a></td><td>" + status.getMaxMapTasks() +
              "</td><td>" + status.getMaxReduceTasks() +
              "</td><td>" + tasksPerNode +
              "</td></tr></table>\n");
  }%>

<%@page import="org.apache.hadoop.dfs.JspHelper"%>
<html>
<head>
<title><%= trackerName %> Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>
<h1><%= trackerName %> Hadoop Map/Reduce Administration</h1>

<b>State:</b> <%= tracker.getClusterStatus().getJobTrackerState() %><br>
<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
<b>Identifier:</b> <%= tracker.getTrackerIdentifier()%><br>                 
                   
<hr>
<h2>Cluster Summary</h2>
<center>
<% 
   generateSummaryTable(out, tracker); 
%>
</center>
<hr>

<h2>Running Jobs</h2>
<%
    generateJobTable(out, "Running", tracker.runningJobs(), 30);
%>
<hr>

<h2>Completed Jobs</h2>
<%
    generateJobTable(out, "Completed", tracker.completedJobs(), 0);
%>

<hr>

<h2>Failed Jobs</h2>
<%
    generateJobTable(out, "Failed", tracker.failedJobs(), 0);
%>

<hr>

<h2>Local logs</h2>
<a href="logs/">Log</a> directory, <a href="jobhistory.jsp">
Job Tracker History</a>

<%
out.println(ServletUtil.htmlFooter());
%>
