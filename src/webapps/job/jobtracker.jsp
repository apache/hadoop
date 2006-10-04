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
  String trackerLabel = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");

  public void generateJobTable(JspWriter out, String label, Vector jobs) throws IOException {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\" colspan=\"9\"><b>" + label + " Jobs </b></td></tr>\n");
      if (jobs.size() > 0) {
        out.print("<tr><td><b>Jobid</b></td><td><b>User</b></td>");
        out.print("<td><b>Name</b></td>");
        out.print("<td><b>Map % complete</b></td>");
        out.print("<td><b>Map total</b></td>");
        out.print("<td><b>Maps completed</b></td>");
        out.print("<td><b>Reduce % complete</b></td>");
        out.print("<td><b>Reduce total</b></td>");
        out.print("<td><b>Reduces completed</b></td></tr>\n");
        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
          JobInProgress job = (JobInProgress) it.next();
          JobProfile profile = job.getProfile();
          JobStatus status = job.getStatus();
          String jobid = profile.getJobId();

          int desiredMaps = job.desiredMaps();
          int desiredReduces = job.desiredReduces();
          int completedMaps = job.finishedMaps();
          int completedReduces = job.finishedReduces();
          String name = profile.getJobName();

          out.print( "<tr><td><a href=\"jobdetails.jsp?jobid=" + jobid + "\">" +
                     jobid + "</a></td>" +
                  "<td>" + profile.getUser() + "</td>" 
                    + "<td>" + ("".equals(name) ? "&nbsp;" : name) + "</td>" + 
                    "<td>" + 
                    StringUtils.formatPercent(status.mapProgress(),2) + 
                    "</td><td>" + 
                    desiredMaps + "</td><td>" + completedMaps + "</td><td>" + 
                    StringUtils.formatPercent(status.reduceProgress(),2) + 
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

  public void generateSummaryTable(JspWriter out) throws IOException {
    ClusterStatus status = tracker.getClusterStatus();
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n"+
              "<tr><th>Maps</th><th>Reduces</th>" + 
              "<th>Tasks/Node</th><th>Nodes</th></tr>\n");
    out.print("<tr><td>" + status.getMapTasks() + "</td><td>" +
              status.getReduceTasks() + "</td><td>" + 
              status.getMaxTasks() + "</td><td><a href=\"/machines.jsp\">" +
              status.getTaskTrackers() + "</a></td></tr></table>\n");
  }
%>

<html>

<title><%= trackerLabel %> Hadoop Map/Reduce Administration</title>

<body>
<h1><%= trackerLabel %> Hadoop Map/Reduce Administration</h1>

<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
                   
<hr>
<h2>Cluster Summary</h2>
<center>
<% 
   generateSummaryTable(out); 
%>
</center>
<hr>

<h2>Running Jobs</h2>
<%
    generateJobTable(out, "Running", tracker.runningJobs());
%>
<hr>

<h2>Completed Jobs</h2>
<%
    generateJobTable(out, "Completed", tracker.completedJobs());
%>

<hr>

<h2>Failed Jobs</h2>
<%
    generateJobTable(out, "Failed", tracker.failedJobs());
%>

<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory, <a href="jobhistory.jsp?historyFile=JobHistory.log&reload=true">
Job Tracker History</a>

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
