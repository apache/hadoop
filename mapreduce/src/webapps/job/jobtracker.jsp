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
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  JobQueueInfo[] queues = tracker.getJobQueues();
  List<JobInProgress> runningJobs = tracker.getRunningJobs();
  List<JobInProgress> completedJobs = tracker.getCompletedJobs();
  List<JobInProgress> failedJobs = tracker.getFailedJobs();
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
              "<tr><th>Queues</th>" +
              "<th>Maps</th><th>Reduces</th>" + 
              "<th>Total Submissions</th>" +
              "<th>Nodes</th><th>Map Task Capacity</th>" +
              "<th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th>" + 
              "<th>Blacklisted Nodes</th>" +
              "<th>Excluded Nodes</th></tr>\n");
    out.print("<tr><td><a href=\"queueinfo.jsp\">" +
              tracker.getRootQueues().length + "</a></td><td>" + 
              status.getMapTasks() + "</td><td>" +
              status.getReduceTasks() + "</td><td>" + 
              tracker.getTotalSubmissions() +
              "</td><td><a href=\"machines.jsp?type=active\">" +
              status.getTaskTrackers() +
              "</a></td><td>" + status.getMaxMapTasks() +
              "</td><td>" + status.getMaxReduceTasks() +
              "</td><td>" + tasksPerNode +
              "</td><td><a href=\"machines.jsp?type=blacklisted\">" +
              status.getBlacklistedTrackers() + "</a>" +
              "</td><td><a href=\"machines.jsp?type=excluded\">" +
              status.getNumExcludedNodes() + "</a>" +
              "</td></tr></table>\n");

    out.print("<br>");
    if (tracker.recoveryManager.shouldRecover()) {
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
    <li><a href="#retired_jobs">Retired Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>
</div>

<b>State:</b> <%= status.getJobTrackerState() %><br>
<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                <%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%> from
                 <%= VersionInfo.getBranch()%><br>
<b>Identifier:</b> <%= tracker.getTrackerIdentifier()%><br>                 
                   
<hr>
<h2>Cluster Summary (Heap Size is <%= StringUtils.byteDesc(status.getUsedMemory()) %>/<%= StringUtils.byteDesc(status.getMaxMemory()) %>)</h2>
<% 
 generateSummaryTable(out, status, tracker); 
%>
<hr>
<b>Filter (Jobid, Priority, User, Name)</b> <input type="text" id="filter" onkeyup="applyfilter()"> <br>
<span class="small">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>
<hr>

<h2 id="running_jobs">Running Jobs</h2>
<%=JSPUtil.generateJobTable("Running", runningJobs, 30, 0)%>
<hr>

<%
if (completedJobs.size() > 0) {
  out.print("<h2 id=\"completed_jobs\">Completed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Completed", completedJobs, 0, 
    runningJobs.size()));
  out.print("<hr>");
}
%>

<%
if (failedJobs.size() > 0) {
  out.print("<h2 id=\"failed_jobs\">Failed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Failed", failedJobs, 0, 
    (runningJobs.size()+completedJobs.size())));
  out.print("<hr>");
}
%>

<h2 id="retired_jobs">Retired Jobs</h2>
<%=JSPUtil.generateRetiredJobTable(tracker, 
  (runningJobs.size()+completedJobs.size()+failedJobs.size()))%>
<hr>

<h2 id="local_logs">Local Logs</h2>
<a href="logs/">Log</a> directory, <a href="jobhistory.jsp">
Job Tracker History</a>

<%
out.println(ServletUtil.htmlFooter());
%>
