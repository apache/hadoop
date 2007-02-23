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
%>
<%
  String jobid = request.getParameter("jobid");
  JobTracker tracker = JobTracker.getTracker();
  JobInProgress job = (JobInProgress) tracker.getJob(jobid);
  String tipid = request.getParameter("tipid");
  String taskid = request.getParameter("taskid");
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
<title>Counters for <%=taskid%></title>
<body>
<h1>Counters for <%=taskid%></h1>

<hr>

<%
	if( counters == null ) {
%>
		<h3>No counter information found for this task</h3>
<%
	}else{    
%>
      <table border=2 cellpadding="5" cellspacing="2">
          <tr><td align="center">Counter</td><td>Value</td></tr> 
		  <%
		    for (String counter : counters.getCounterNames()) {
		      long value = counters.getCounter(counter);
		      %>
		      <tr><td><%=counter%></td><td><%=value%></td></tr>
              <%
		    }
		  %>
      </table>
<%
    }
%>

<hr>
<a href="/jobdetails.jsp?jobid=<%=jobid%>">Go back to the job</a><br>
<a href="/jobtracker.jsp">Go back to JobTracker</a><br>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
