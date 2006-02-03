<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.nutch.mapred.*"
%>
<%!
  JobTracker tracker = JobTracker.getTracker();
  String trackerLabel = tracker.getJobTrackerMachine() + ":" + tracker.getTrackerPort();

  public void generateTaskTrackerTable(JspWriter out) throws IOException {
    Collection c = tracker.taskTrackers();

    if (c.size() == 0) {
      out.print("There are currently no known TaskTracker(s).");
    } else {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\" colspan=\"4\"><b>Task Trackers</b></td></tr>\n");
      out.print("<tr><td><b>Name</b></td><td><b>Host</b></td><td><b># running tasks</b></td><td><b>Secs since heartbeat</b></td></tr>\n");

      for (Iterator it = c.iterator(); it.hasNext(); ) {
        TaskTrackerStatus tt = (TaskTrackerStatus) it.next();
        long sinceHeartbeat = System.currentTimeMillis() - tt.getLastSeen();
        if (sinceHeartbeat > 0) {
          sinceHeartbeat = sinceHeartbeat / 1000;
        }
        int numCurTasks = 0;
        for (Iterator it2 = tt.taskReports(); it2.hasNext(); ) {
          it2.next();
          numCurTasks++;
        }

        out.print("<tr><td>" + tt.getTrackerName() + "</td><td>" + tt.getHost() + "</td><td>" + numCurTasks + "</td><td>" + sinceHeartbeat + "</td></tr>\n");
      }
      out.print("</table>\n");
      out.print("</center>\n");
    }
  }

  public void generateJobTable(JspWriter out, String label, Vector jobs) throws IOException {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\" colspan=\"8\"><b>" + label + " Jobs </b></td></tr>\n");

      if (jobs.size() > 0) {
        out.print("<tr><td><b>Jobid</b></td><td><b>% complete</b></td><td><b>Required maps</b></td><td><b>maps completed</b></td><td><b>Required reduces</b></td><td><b>reduces completed</b></td></tr>\n");
        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
          JobInProgress job = (JobInProgress) it.next();
          JobProfile profile = job.getProfile();
          JobStatus status = job.getStatus();

          String jobid = profile.getJobId();
          double completedRatio = (0.5 * (100 * status.mapProgress())) +
                                 (0.5 * (100 * status.reduceProgress()));

          int desiredMaps = job.desiredMaps();
          int desiredReduces = job.desiredReduces();
          int completedMaps = job.finishedMaps();
          int completedReduces = job.finishedReduces();

          out.print("<tr><td><a href=\"jobdetails.jsp?jobid=" + jobid + "\">" + jobid + "</a></td><td>" + completedRatio + "%</td><td>" + desiredMaps + "</td><td>" + completedMaps + "</td><td>" + desiredReduces + "</td><td> " + completedReduces + "</td></tr>\n");
        }
      } else {
        out.print("<tr><td align=\"center\" colspan=\"8\"><i>none</i></td></tr>\n");
      }
      out.print("</table>\n");
      out.print("</center>\n");
  }
%>

<html>

<title>Nutch MapReduce General Administration</title>

<body>
<h1>JobTracker '<%=trackerLabel%>'</h1>

This JobTracker has been up since <%= new Date(tracker.getStartTime())%>.<br>
<hr>


<h2>Task Trackers</h2>
<%
  generateTaskTrackerTable(out);
%>

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
<a href="http://www.nutch.org/">Nutch</a>, 2005.<br>
</body>
</html>
