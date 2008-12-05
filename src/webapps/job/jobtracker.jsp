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
  ClusterStatus status = tracker.getClusterStatus();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  JobQueueInfo[] queues = tracker.getQueues();
  Vector<JobInProgress> runningJobs = tracker.runningJobs();
  Vector<JobInProgress> completedJobs = tracker.completedJobs();
  Vector<JobInProgress> failedJobs = tracker.failedJobs();
%>
<%!
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");
  
  public void generateSummaryTable(JspWriter out, ClusterStatus status,
                                   JobTracker tracker) throws IOException {
    String tasksPerNode = status.getTaskTrackers() > 0 ?
      percentFormat.format(((double)(status.getMaxMapTasks() +
                      status.getMaxReduceTasks())) / status.getTaskTrackers()):
      "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"+
              "<tr><th>Maps</th><th>Reduces</th>" + 
              "<th>Total Submissions</th>" +
              "<th>Nodes</th><th>Map Task Capacity</th>" +
              "<th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th>" + 
              "<th>Blacklisted Nodes</th></tr>\n");
    out.print("<tr><td>" + status.getMapTasks() + "</td><td>" +
              status.getReduceTasks() + "</td><td>" + 
              tracker.getTotalSubmissions() +
              "</td><td><a href=\"machines.jsp?type=active\">" +
              status.getTaskTrackers() +
              "</a></td><td>" + status.getMaxMapTasks() +
              "</td><td>" + status.getMaxReduceTasks() +
              "</td><td>" + tasksPerNode +
              "</td><td><a href=\"machines.jsp?type=blacklisted\">" +
              status.getBlacklistedTrackers() + "</a>" +
              "</td></tr></table>\n");

    out.print("<br>");
    if (tracker.hasRestarted()) {
      out.print("<span class=\"small\"><i>");
      if (tracker.hasRecovered()) {
        out.print("The JobTracker got restarted and recovered back in " );
        out.print(StringUtils.formatTime(tracker.getRecoveryDuration()));
      } else {
        out.print("The JobTracker got restarted and is still recovering");
      }
      out.print("</i></span>");
    }
  }%>


<html>
<head>
<title><%= trackerName %> Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
</head>
<body>

<% JSPUtil.processButtons(request, response, tracker); %>

<h1><%= trackerName %> Hadoop Map/Reduce Administration</h1>

<div id="quicklinks">
  <a href="#quicklinks" onclick="toggle('quicklinks-list'); return false;">Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#scheduling_info">Scheduling Info</a></li>
    <li><a href="#running_jobs">Running Jobs</a></li>
    <li><a href="#completed_jobs">Completed Jobs</a></li>
    <li><a href="#failed_jobs">Failed Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>
</div>

<b>State:</b> <%= status.getJobTrackerState() %><br>
<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
<b>Identifier:</b> <%= tracker.getTrackerIdentifier()%><br>                 
                   
<hr>
<h2>Cluster Summary (Heap Size is <%= StringUtils.byteDesc(status.getUsedMemory()) %>/<%= StringUtils.byteDesc(status.getMaxMemory()) %>)</h2>
<% 
 generateSummaryTable(out, status, tracker); 
%>
<hr>
<h2 id="scheduling_info">Scheduling Information</h2>
<table border="2" cellpadding="5" cellspacing="2">
<thead style="font-weight: bold">
<tr>
<td> Queue Name </td>
<td> Scheduling Information</td>
</tr>
</thead>
<tbody>
<%
for(JobQueueInfo queue: queues) {
  String queueName = queue.getQueueName();
  String schedulingInformation = queue.getSchedulingInfo();
  if(schedulingInformation == null || schedulingInformation.trim().equals("")) {
    schedulingInformation = "NA";
  }
%>
<tr>
<td><a href="jobqueue_details.jsp?queueName=<%=queueName%>"><%=queueName%></a></td>
<td><%=schedulingInformation.replaceAll("\n","<br/>") %>
</td>
</tr>
<%
}
%>
</tbody>
</table>
<hr/>
<b>Filter (Jobid, Priority, User, Name)</b> <input type="text" id="filter" onkeyup="applyfilter()"> <br>
<span class="small">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>
<hr>

<h2 id="running_jobs">Running Jobs</h2>
<%=JSPUtil.generateJobTable("Running", runningJobs, 30, 0)%>
<hr>

<h2 id="completed_jobs">Completed Jobs</h2>
<%=JSPUtil.generateJobTable("Completed", completedJobs, 0, runningJobs.size())%>
<hr>

<h2 id="failed_jobs">Failed Jobs</h2>
<%=JSPUtil.generateJobTable("Failed", failedJobs, 0, 
    (runningJobs.size()+completedJobs.size()))%>
<hr>

<h2 id="local_logs">Local Logs</h2>
<a href="logs/">Log</a> directory, <a href="jobhistory.jsp">
Job Tracker History</a>

<%
out.println(ServletUtil.htmlFooter());
%>
