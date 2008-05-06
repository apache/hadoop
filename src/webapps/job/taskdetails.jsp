<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.dfs.JspHelper"
%>
<%!static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d-MMM-yyyy HH:mm:ss");

  private static final String PRIVATE_ACTIONS_KEY = "webinterface.private.actions";%>
<%!private void printConfirm(JspWriter out, String jobid, String tipid,
      String taskid, String action) throws IOException {
    String url = "taskdetails.jsp?jobid=" + jobid + "&tipid=" + tipid
        + "&taskid=" + taskid;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url + "\"></head>" + "<body><h3> Are you sure you want to kill/fail "
        + taskid + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<a href=\"" + url + "&action=" + action
        + "\">Kill / Fail</a></td><td width=\"100\"><a href=\"" + url
        + "\">Cancel</a></td></tr></table></body></html>");
  }%>
<%
    JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
    String jobid = request.getParameter("jobid");
    String tipid = request.getParameter("tipid");
    String taskid = request.getParameter("taskid");
    JobID jobidObj = JobID.forName(jobid);
    TaskID tipidObj = TaskID.forName(tipid);
    TaskAttemptID taskidObj = TaskAttemptID.forName(taskid);
    
    JobInProgress job = (JobInProgress) tracker.getJob(jobidObj);
    
    boolean privateActions = JspHelper.conf.getBoolean(PRIVATE_ACTIONS_KEY,
        false);
    if (privateActions) {
      String action = request.getParameter("action");
      if (action != null) {
        if (action.equalsIgnoreCase("confirm")) {
          String subAction = request.getParameter("subaction");
          if (subAction == null)
            subAction = "fail-task";
          printConfirm(out, jobid, tipid, taskid, subAction);
          return;
        }
        else if (action.equalsIgnoreCase("kill-task")) {
          tracker.killTask(taskidObj, false);
          //redirect again so that refreshing the page will not attempt to rekill the task
          response.sendRedirect("/taskdetails.jsp?" + "&subaction=kill-task"
              + "&jobid=" + jobid + "&tipid=" + tipid);
        }
        else if (action.equalsIgnoreCase("fail-task")) {
          tracker.killTask(taskidObj, true);
          response.sendRedirect("/taskdetails.jsp?" + "&subaction=fail-task"
              + "&jobid=" + jobid + "&tipid=" + tipid);
        }
      }
    }
    TaskStatus[] ts = (job != null) ? tracker.getTaskStatuses(tipidObj)
        : null;
%>

<%@page import="org.apache.hadoop.dfs.JspHelper"%>
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  <title>Hadoop Task Details</title>
</head>
<body>
<h1>Job <a href="/jobdetails.jsp?jobid=<%=jobid%>"><%=jobid%></a></h1>

<hr>

<h2>All Task Attempts</h2>
<center>
<%
    if (ts == null || ts.length == 0) {
%>
		<h3>No Task Attempts found</h3>
<%
    } else {
%>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Machine</td><td>Status</td><td>Progress</td><td>Start Time</td> 
  <%
   if (!ts[0].getIsMap()) {
   %>
<td>Shuffle Finished</td><td>Sort Finished</td>
  <%
  }
  %>
<td>Finish Time</td><td>Errors</td><td>Task Logs</td><td>Counters</td><td>Actions</td></tr>
  <%
    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      String taskTrackerName = status.getTaskTracker();
      TaskTrackerStatus taskTracker = tracker.getTaskTracker(taskTrackerName);
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      String taskAttemptTracker = null;
      if (taskTracker == null) {
        out.print("<td>" + taskTrackerName + "</td>");
      } else {
        taskAttemptTracker = "http://" + taskTracker.getHost() + ":"
          + taskTracker.getHttpPort();
        out.print("<td><a href=\"" + taskAttemptTracker + "\">"
          + tracker.getNode(taskTracker.getHost()) + "</a></td>");
        }
        out.print("<td>" + status.getRunState() + "</td>");
        out.print("<td>" + StringUtils.formatPercent(status.getProgress(), 2)
          + JspHelper.percentageGraph(status.getProgress() * 100f, 80) + "</td>");
        out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getStartTime(), 0) + "</td>");
        if (!ts[i].getIsMap()) {
          out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getShuffleFinishTime(), status.getStartTime()) + "</td>");
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getSortFinishTime(), status.getShuffleFinishTime())
          + "</td>");
        }
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getFinishTime(), status.getStartTime()) + "</td>");

        out.print("<td><pre>");
        String [] failures = tracker.getTaskDiagnostics(status.getTaskID());
        if (failures == null) {
          out.print("&nbsp;");
        } else {
          for(int j = 0 ; j < failures.length ; j++){
            out.print(failures[j]);
            if (j < (failures.length - 1)) {
              out.print("\n-------\n");
            }
          }
        }
        out.print("</pre></td>");
        out.print("<td>");
        if (taskAttemptTracker == null) {
          out.print("n/a");
        } else {
          String taskLogUrl = taskAttemptTracker + "/tasklog?taskid="
            + status.getTaskID();
          String tailFourKBUrl = taskLogUrl + "&start=-4097";
          String tailEightKBUrl = taskLogUrl + "&start=-8193";
          String entireLogUrl = taskLogUrl + "&all=true";
          out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
          out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
          out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
        }
        out.print("</td><td>" + "<a href=\"/taskstats.jsp?jobid=" + jobid
          + "&tipid=" + tipid + "&taskid=" + status.getTaskID() + "\">"
          + ((status.getCounters() != null) ? status.getCounters().size() : 0) + "</a></td>");
        out.print("<td>");
        if (privateActions
          && status.getRunState() == TaskStatus.State.RUNNING) {
        out.print("<a href=\"/taskdetails.jsp?action=confirm"
          + "&subaction=kill-task" + "&jobid=" + jobid + "&tipid="
          + tipid + "&taskid=" + status.getTaskID() + "\" > Kill </a>");
        out.print("<br><a href=\"/taskdetails.jsp?action=confirm"
          + "&subaction=fail-task" + "&jobid=" + jobid + "&tipid="
          + tipid + "&taskid=" + status.getTaskID() + "\" > Fail </a>");
        }
        else
          out.print("<pre>&nbsp;</pre>");
        out.println("</td></tr>");
      }
  %>
</table>
</center>

<%
      if (ts[0].getIsMap()) {
%>
<h3>Input Split Locations</h3>
<table border=2 cellpadding="5" cellspacing="2">
<%
        for (String split: StringUtils.split(tracker.getTip(
                                         tipidObj).getSplitNodes())) {
          out.println("<tr><td>" + split + "</td></tr>");
        }
%>
</table>
<%    
      }
    }
%>

<hr>
<a href="jobdetails.jsp?jobid=<%=jobid%>">Go back to the job</a><br>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<%
out.println(ServletUtil.htmlFooter());
%>
