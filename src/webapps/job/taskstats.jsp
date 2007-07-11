<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.text.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.util.*"
%>
<%
  String jobid = request.getParameter("jobid");
  JobTracker tracker = JobTracker.getTracker();
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  String tipid = request.getParameter("tipid");
  String taskid = request.getParameter("taskid");
  Format decimal = new DecimalFormat();
  Counters counters;
  if (taskid == null) {
    counters = tracker.getTipCounters(jobid, tipid);
    taskid = tipid; // for page title etc
  }
  else {
    TaskStatus taskStatus = tracker.getTaskStatus(jobid, tipid, taskid);
    counters = taskStatus.getCounters();
  }
%>

<html>
  <head>
    <title>Counters for <%=taskid%></title>
  </head>
<body>
<h1>Counters for <%=taskid%></h1>

<hr>

<%
  if ( counters == null ) {
%>
    <h3>No counter information found for this task</h3>
<%
  } else {    
%>
    <table>
<%
      for (String groupName : counters.getGroupNames()) {
        Counters.Group group = counters.getGroup(groupName);
        String displayGroupName = group.getDisplayName();
%>
        <tr>
          <td colspan="3"><br/><b><%=displayGroupName%></b></td>
        </tr>
<%
        for (String counter : group.getCounterNames()) {
          String displayCounterName = group.getDisplayName(counter);
          long value = group.getCounter(counter);
%>
          <tr>
            <td width="50"></td>
            <td><%=displayCounterName%></td>
            <td align="right"><%=decimal.format(value)%></td>
          </tr>
<%
        }
      }
%>
    </table>
<%
  }
%>

<hr>
<a href="jobdetails.jsp?jobid=<%=jobid%>">Go back to the job</a><br>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
