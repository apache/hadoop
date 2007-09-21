<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.lang.Integer"
  import="java.text.SimpleDateFormat"
%>
<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String jobid = request.getParameter("jobid");
  String type = request.getParameter("type");
  String pagenum = request.getParameter("pagenum");
  int pnum = Integer.parseInt(pagenum);
  int next_page = pnum+1;
  int numperpage = 2000;
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  JobProfile profile = (job != null) ? (job.getProfile()) : null;
  JobStatus status = (job != null) ? (job.getStatus()) : null;
  TaskReport[] reports = null;
  int start_index = (pnum - 1) * numperpage;
  int end_index = start_index + numperpage;
  int report_len = 0;
  if ("map".equals(type)){
     reports = (job != null) ? tracker.getMapTaskReports(jobid) : null;
    }
  else{
    reports = (job != null) ? tracker.getReduceTaskReports(jobid) : null;
  }
%>

<html>
  <head>
    <title>Hadoop <%=type%> task list for <%=jobid%> on <%=trackerName%></title>
  </head>
<body>
<h1>Hadoop <%=type%> task list for 
<a href="jobdetails.jsp?jobid=<%=jobid%>"><%=jobid%></a> on 
<a href="jobtracker.jsp"><%=trackerName%></a></h1>
<%
  if (job == null) {
    out.print("<b>Job " + jobid + " not found.</b><br>\n");
    return;
  }
  report_len = reports.length;
  
  if (report_len <= start_index) {
    out.print("<b>No such tasks</b>");
  } else {
    out.print("<hr>");
    out.print("<h2>Tasks</h2>");
    out.print("<center>");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><td align=\"center\">Task</td><td>Complete</td><td>Status</td>" +
              "<td>Start Time</td><td>Finish Time</td><td>Errors</td><td>Counters</td></tr>");
    if (end_index > report_len){
        end_index = report_len;
    }
    for (int i = start_index ; i < end_index; i++) {
          TaskReport report = reports[i];
          out.print("<tr><td><a href=\"taskdetails.jsp?jobid=" + jobid + 
                    "&tipid=" + report.getTaskId() + "\">"  + 
                    report.getTaskId() + "</a></td>");
         out.print("<td>" + StringUtils.formatPercent(report.getProgress(),2) + 
                   "</td>");
         out.print("<td>"  + report.getState() + "<br/></td>");
         out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, report.getStartTime(),0) + "<br/></td>");
         out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
             report.getFinishTime(), report.getStartTime()) + "<br/></td>");
         String[] diagnostics = report.getDiagnostics();
         out.print("<td><pre>");
         for (int j = 0; j < diagnostics.length ; j++) {
             out.println(diagnostics[j]);
         }
         out.println("</pre><br/></td>");
         out.println("<td>" + 
             "<a href=\"taskstats.jsp?jobid=" + jobid + 
             "&tipid=" + report.getTaskId() +
             "\">" + report.getCounters().size() +
             "</a></td></tr>");
    }
    out.print("</table>");
    out.print("</center>");
  }
  if (end_index < report_len) {
    out.print("<div style=\"text-align:right\">" + 
              "<a href=\"jobtasks.jsp?jobid="+ jobid + "&type=" + type +
              "&pagenum=" + next_page +
              "\">" + "Next" + "</a></div>");
  }
  if (start_index != 0) {
      out.print("<div style=\"text-align:right\">" + 
                "<a href=\"jobtasks.jsp?jobid="+ jobid + "&type=" + type +
                "&pagenum=" + (pnum -1) + "\">" + "Prev" + "</a></div>");
  }
%>

<hr>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2007.<br>
</body>
</html>
