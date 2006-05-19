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
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");
  
  private String stringifyState(int state) {
    if (state == TaskStatus.RUNNING){
      return "RUNNING";
    } else if (state == TaskStatus.SUCCEEDED){
      return "SUCCEDED";
    } else if (state == TaskStatus.FAILED){
      return "FAILED";
    } else if (state == TaskStatus.UNASSIGNED){
      return "UNASSIGNED";
    }
    return "unknown status";
  }
  
 %>

<%
  TaskTracker tracker = TaskTracker.getTracker();
  String trackerName = tracker.getName();
%>

<html>

<title><%= trackerName %> Task Tracker Status</title>

<body>
<h1><%= trackerName %> Task Tracker Status</h1>
<img src="/static/hadoop-logo.jpg"/>

<h2>Running tasks</h2>
<center>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
    <td>Progress</td><td>Errors</td></tr>

  <%
     Iterator itr = tracker.getRunningTaskStatuses().iterator();
     while (itr.hasNext()) {
       TaskStatus status = (TaskStatus) itr.next();
       out.print("<tr><td>" + status.getTaskId());
       out.print("</td><td>" + stringifyState(status.getRunState())); 
       out.print("</td><td>" + 
                 percentFormat.format(100.0 * status.getProgress()));
       out.print("</td><td><pre>" + status.getDiagnosticInfo() + "</pre></td>");
       out.print("</tr>\n");
     }
  %>
</table>
</center>

<h2>Local Logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
