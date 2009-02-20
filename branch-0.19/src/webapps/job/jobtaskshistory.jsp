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
	<jsp:param name="logFile" value="<%=request.getParameter("logFile") %>"/>
</jsp:include>
<%!	
  private static SimpleDateFormat dateFormat =
                                    new SimpleDateFormat("d/MM HH:mm:ss") ; 
%>

<%	
  String jobid = request.getParameter("jobid");
  String logFile = request.getParameter("logFile");
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String taskStatus = request.getParameter("status"); 
  String taskType = request.getParameter("taskType"); 
  
  JobHistory.JobInfo job = (JobHistory.JobInfo)request.
                            getSession().getAttribute("job");
  Map<String, JobHistory.Task> tasks = job.getAllTasks(); 
%>
<html>
<body>
<h2><%=taskStatus%> <%=taskType %> task list for <a href="jobdetailshistory.jsp?jobid=<%=jobid%>&&logFile=<%=encodedLogFileName%>"><%=jobid %> </a></h2>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Start Time</td><td>Finish Time<br/></td><td>Error</td></tr>
<%
  for (JobHistory.Task task : tasks.values()) {
    if (taskType.equals(task.get(Keys.TASK_TYPE))){
      Map <String, TaskAttempt> taskAttempts = task.getTaskAttempts();
      for (JobHistory.TaskAttempt taskAttempt : taskAttempts.values()) {
        if (taskStatus.equals(taskAttempt.get(Keys.TASK_STATUS)) || 
          taskStatus.equals("all")){
          printTask(jobid, encodedLogFileName, taskAttempt, out); 
        }
      }
    }
  }
%>
</table>
<%!
  private void printTask(String jobid, String logFile,
    JobHistory.TaskAttempt attempt, JspWriter out) throws IOException{
    out.print("<tr>"); 
    out.print("<td>" + "<a href=\"taskdetailshistory.jsp?jobid=" + jobid + 
          "&logFile="+ logFile +"&taskid="+attempt.get(Keys.TASKID)+"\">" +
          attempt.get(Keys.TASKID) + "</a></td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
          attempt.getLong(Keys.START_TIME), 0 ) + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
          attempt.getLong(Keys.FINISH_TIME),
          attempt.getLong(Keys.START_TIME) ) + "</td>");
    out.print("<td>" + attempt.get(Keys.ERROR) + "</td>");
    out.print("</tr>"); 
  }
%>
</center>
</body>
</html>
