<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapred.JobHistory.*"
%>
<jsp:include page="loadhistory.jsp">
	<jsp:param name="jobid" value="<%=request.getParameter("jobid") %>"/>
	<jsp:param name="jobTrackerId" value="<%=request.getParameter("jobTrackerId") %>"/>
</jsp:include>
<%!	private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ; %>

<%	
	String jobid = request.getParameter("jobid");
	String jobTrackerId = request.getParameter("jobTrackerId");
	String taskid = request.getParameter("taskid"); 

	JobHistory.JobInfo job = (JobHistory.JobInfo)request.getSession().getAttribute("job");
	JobHistory.Task task = job.getAllTasks().get(taskid); 
%>
<html>
<body>
<h2><%=taskid %> attempts for <a href="jobdetailshistory.jsp?jobid=<%=jobid%>&&jobTrackerId=<%=jobTrackerId %>"> <%=jobid %> </a></h2>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Start Time</td>
<%	
	if( Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE) ) ){
%>
		<td>Shuffle Finished</td><td>Sort Finished</td>
<%
	}
%>
<td>Finish Time</td><td>Host</td><td>Error</td></tr>
<%
	for( JobHistory.TaskAttempt attempt : task.getTaskAttempts().values() ) {
	  printTaskAttempt(attempt, task.get(Keys.TASK_TYPE), out); 
	}
%>
</table>
<%!
	private void printTaskAttempt(JobHistory.TaskAttempt taskAttempt, String type, JspWriter out) throws IOException{
  		out.print("<tr>"); 
  		out.print("<td>" + taskAttempt.get(Keys.TASK_ATTEMPT_ID) + "</td>");
         out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, taskAttempt.getLong(Keys.START_TIME), 0 ) + "</td>") ; 
  		if(Values.REDUCE.name().equals(type) ){
  		  JobHistory.ReduceAttempt reduceAttempt = (JobHistory.ReduceAttempt)taskAttempt ; 
	      out.print("<td>" + 
	          StringUtils.getFormattedTimeWithDiff(dateFormat, 
	              reduceAttempt.getLong(Keys.SHUFFLE_FINISHED), 
	              reduceAttempt.getLong(Keys.START_TIME)) + "</td>") ; 
	      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
	          	 reduceAttempt.getLong(Keys.SORT_FINISHED), 
	          	 reduceAttempt.getLong(Keys.SHUFFLE_FINISHED)) + "</td>") ; 
  		}
	      out.print("<td>"+ StringUtils.getFormattedTimeWithDiff(dateFormat, taskAttempt.getLong(Keys.FINISH_TIME), 
					          taskAttempt.getLong(Keys.START_TIME) ) + "</td>") ; 
  		out.print("<td>" + taskAttempt.get(Keys.HOSTNAME) + "</td>");
  		out.print("<td>" + taskAttempt.get(Keys.ERROR) + "</td>");
  		out.print("</tr>"); 
	}
%>
</center>
</body>
</html>
