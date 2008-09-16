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
  int rowId = 0;
%>
<%!
  private static final String PRIVATE_ACTIONS_KEY 
    = "webinterface.private.actions";
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");
  
  public void processButtons(JspWriter out, 
          HttpServletRequest request,
          HttpServletResponse response,
          JobTracker tracker) throws IOException {
  
    if (JspHelper.conf.getBoolean(PRIVATE_ACTIONS_KEY, false) &&
        request.getParameter("killJobs") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");
      if (jobs != null) {
        for (String job : jobs) {
          tracker.killJob(JobID.forName(job));
        }
      }
    }
    
    if (request.getParameter("changeJobPriority") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");
      
      if (jobs != null) {
        JobPriority jobPri = JobPriority.valueOf(request.getParameter("setJobPriority"));
          
        for (String job : jobs) {
          tracker.setJobPriority(JobID.forName(job), jobPri);
        }
      }
    }
  }

  public int generateJobTable(JspWriter out, String label, Vector jobs, int refresh, int rowId) throws IOException {
    
    boolean isModifiable = label.equals("Running");

    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
    
    if (jobs.size() > 0) {
      if (isModifiable) {
        out.print("<tr>");
        out.print("<td><input type=\"Button\" onclick=\"selectAll()\" value=\"Select All\" id=\"checkEm\"></td>");
        out.print("<td>");
        if (JspHelper.conf.getBoolean(PRIVATE_ACTIONS_KEY, false)) {
          out.print("<input type=\"submit\" name=\"killJobs\" value=\"Kill Selected Jobs\">");
        }
        out.print("</td");
        out.print("<td><nobr>");
        out.print("<select name=\"setJobPriority\">");

        for (JobPriority prio : JobPriority.values()) {
         out.print("<option" + (JobPriority.NORMAL == prio ? " selected=\"selected\">" : ">") + prio + "</option>");
        }

        out.print("</select>");
        out.print("<input type=\"submit\" name=\"changeJobPriority\" value=\"Change\">");
        out.print("</nobr></td>");
        out.print("<td colspan=\"8\">&nbsp;</td>");
        out.print("</tr>");
        out.print("<td>&nbsp;</td>");
      } else {
        out.print("<tr>");
      }
      
      out.print("<td><b>Jobid</b></td><td><b>Priority</b></td><td><b>User</b></td>");
      out.print("<td><b>Name</b></td>");
      out.print("<td><b>Map % Complete</b></td>");
      out.print("<td><b>Map Total</b></td>");
      out.print("<td><b>Maps Completed</b></td>");
      out.print("<td><b>Reduce % Complete</b></td>");
      out.print("<td><b>Reduce Total</b></td>");
      out.print("<td><b>Reduces Completed</b></td></tr>\n");
      for (Iterator it = jobs.iterator(); it.hasNext(); ++rowId) {
        JobInProgress job = (JobInProgress) it.next();
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        String name = profile.getJobName();
        String jobpri = job.getPriority().toString();
        
        if (isModifiable) {
          out.print("<tr><td><input TYPE=\"checkbox\" onclick=\"checkButtonVerbage()\" name=\"jobCheckBox\" value=" +
                    jobid + "></td>");
        } else {
            out.print("<tr>");
        }

        out.print("<td id=\"job_" + rowId + "\"><a href=\"jobdetails.jsp?jobid=" +
                  jobid + "&refresh=" + refresh + "\">" + jobid +"</a></td>" +
                  "<td id=\"priority_" + rowId + "\">" + jobpri + "</td>" +
                  "<td id=\"user_" + rowId + "\">" + profile.getUser() + "</td>" + 
                  "<td id=\"name_" + rowId + "\">" + ("".equals(name) ? "&nbsp;" : name) +
                  "</td>" +
                  "<td>" + StringUtils.formatPercent(status.mapProgress(),2) +
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
    
    return rowId;
  }

  public void generateSummaryTable(JspWriter out,
                                   JobTracker tracker) throws IOException {
    ClusterStatus status = tracker.getClusterStatus();
    String tasksPerNode = status.getTaskTrackers() > 0 ?
      percentFormat.format(((double)(status.getMaxMapTasks() +
                      status.getMaxReduceTasks())) / status.getTaskTrackers()):
      "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"+
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

<%@page import="org.apache.hadoop.hdfs.server.namenode.JspHelper"%>
<html>
<head>
<title><%= trackerName %> Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
</head>
<body>

<% processButtons(out, request, response, tracker); %>

<h1><%= trackerName %> Hadoop Map/Reduce Administration</h1>

<div id="quicklinks">
  <a href="#quicklinks" onclick="toggle('quicklinks-list'); return false;">Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#running_jobs">Running Jobs</a></li>
    <li><a href="#completed_jobs">Completed Jobs</a></li>
    <li><a href="#failed_jobs">Failed Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>
</div>

<b>State:</b> <%= tracker.getClusterStatus().getJobTrackerState() %><br>
<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
<b>Identifier:</b> <%= tracker.getTrackerIdentifier()%><br>                 
                   
<hr>
<h2>Cluster Summary</h2>
<% 
 generateSummaryTable(out, tracker); 
%>
<hr>

<b>Filter (Jobid, Priority, User, Name)</b> <input type="text" id="filter" onkeyup="applyfilter()"> <br>
<span class="small">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>
<hr>

<h2 id="running_jobs">Running Jobs</h2>
<%
  out.print("<form action=\"/jobtracker.jsp\" onsubmit=\"return confirmAction();\" method=\"POST\">");
  rowId = generateJobTable(out, "Running", tracker.runningJobs(), 30, rowId);
  out.print("</form>\n");
%>
<hr>

<h2 id="completed_jobs">Completed Jobs</h2>
<%
  rowId = generateJobTable(out, "Completed", tracker.completedJobs(), 0, rowId);
%>
<hr>

<h2 id="failed_jobs">Failed Jobs</h2>
<%
    generateJobTable(out, "Failed", tracker.failedJobs(), 0, rowId);
%>
<hr>

<h2 id="local_logs">Local Logs</h2>
<a href="logs/">Log</a> directory, <a href="jobhistory.jsp">
Job Tracker History</a>

<%
out.println(ServletUtil.htmlFooter());
%>
