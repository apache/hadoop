<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>

<%!
  JobTracker tracker = JobTracker.getTracker();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  
  private void printFailedAttempts(JspWriter out,
                                   String jobId,
                                   TaskInProgress tip) throws IOException {
    TaskStatus[] statuses = tip.getTaskStatuses();
    String tipId = tip.getTIPId();
    for(int i=0; i < statuses.length; ++i) {
      if (statuses[i].getRunState() == TaskStatus.State.FAILED) {
        String taskTrackerName = statuses[i].getTaskTracker();
        TaskTrackerStatus taskTracker = tracker.getTaskTracker(taskTrackerName);
        out.print("<tr><td>" + statuses[i].getTaskId() +
                  "</td><td><a href=\"/taskdetails.jsp?jobid="+ jobId + 
                  "&taskid=" + tipId + "\">" + tipId +
                  "</a></td>");
        if (taskTracker == null) {
          out.print("<td>" + taskTrackerName + "</td>");
        } else {
          out.print("<td><a href=\"http://" + taskTracker.getHost() + ":" +
                    taskTracker.getHttpPort() + "\">" +  taskTracker.getHost() + 
                    "</a></td>");
        }
        out.print("<td><pre>");
        List<String> failures = 
                     tracker.getTaskDiagnostics(jobId, tipId, 
                                                statuses[i].getTaskId());
        if (failures == null) {
          out.print("&nbsp;");
        } else {
          for(Iterator<String> itr = failures.iterator(); itr.hasNext(); ) {
            out.print(itr.next());
            if (itr.hasNext()) {
              out.print("\n-------\n");
            }
          }
        }
        out.print("</pre></td>");
        
        out.print("<td>");
        if (taskTracker != null) {
          String taskLogUrl = "http://" + taskTracker.getHost() + ":" +
          	taskTracker.getHttpPort() + "/tasklog.jsp?taskid=" + 
          	statuses[i].getTaskId();
          String tailFourKBUrl = taskLogUrl + "&tail=true&tailsize=4096";
          String tailEightKBUrl = taskLogUrl + "&tail=true&tailsize=8192";
          String entireLogUrl = taskLogUrl + "&all=true";
          out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
          out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
          out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
        } else { 
          out.print("n/a"); // task tracker was lost
        }
        out.print("</td>");
        
        out.print("</tr>\n");
       }
    }
  }
             
  private void printFailures(JspWriter out, 
                             String jobId,
                             String kind) throws IOException {
    JobInProgress job = (JobInProgress) tracker.getJob(jobId);
    if (job == null) {
      out.print("<b>Job " + jobId + " not found.</b><br>\n");
      return;
    }
    boolean includeMap = false;
    boolean includeReduce = false;
    if (kind == null) {
      includeMap = true;
      includeReduce = true;
    } else if ("map".equals(kind)) {
      includeMap = true;
    } else if ("reduce".equals(kind)) {
      includeReduce = true;
    } else if ("all".equals(kind)) {
      includeMap = true;
      includeReduce = true;
    } else {
      out.print("<b>Kind " + kind + " not supported.</b><br>\n");
      return;
    }
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>Attempt</th><th>Task</th><th>Machine</th>" +
              "<th>Error</th><th>Logs</th></tr>\n");
    if (includeMap) {
      TaskInProgress[] tips = job.getMapTasks();
      for(int i=0; i < tips.length; ++i) {
        printFailedAttempts(out, jobId, tips[i]);
      }
    }
    if (includeReduce) {
      TaskInProgress[] tips = job.getReduceTasks();
      for(int i=0; i < tips.length; ++i) {
        printFailedAttempts(out, jobId, tips[i]);
      }
    }
    out.print("</table>\n");
  }
%>

<%
    String jobId = request.getParameter("jobid");
    String kind = request.getParameter("kind");
%>

<html>
<title>Hadoop <%=jobId%> failures on <%=trackerName%></title>
<body>
<h1>Hadoop <a href="/jobdetails.jsp?jobid=<%=jobId%>"><%=jobId%></a>
failures on <a href="/jobtracker.jsp"><%=trackerName%></a></h1>

<% 
    printFailures(out, jobId, kind); 
%>

<hr>
<a href="/jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
