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
<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>
<%
	String jobid = request.getParameter("jobid");
	String jobTrackerId = request.getParameter("jobTrackerId");
	
	JobInfo job = (JobInfo)request.getSession().getAttribute("job");
%>
<html><body>
<h2>Hadoop Job <%=jobid %> </h2>

<b>User: </b> <%=job.get(Keys.USER) %><br/> 
<b>JobName: </b> <%=job.get(Keys.JOBNAME) %><br/> 
<b>JobConf: </b> <a href="jobconf.jsp?jobid=<%=jobid %>"> 
                 <%=job.get(Keys.JOBCONF) %></a><br/> 
<b>Submitted At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME), 0 )  %><br/> 
<b>Launched At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.LAUNCH_TIME), job.getLong(Keys.SUBMIT_TIME)) %><br/>
<b>Finished At: </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME), job.getLong(Keys.LAUNCH_TIME)) %><br/>
<b>Status: </b> <%= ((job.get(Keys.JOB_STATUS) == null)?"Incomplete" :job.get(Keys.JOB_STATUS)) %><br/> 
<%
	Map<String, JobHistory.Task> tasks = job.getAllTasks();
	int totalMaps = 0 ; 
	int totalReduces = 0; 
	int failedMaps = 0; 
	int killedMaps = 0;
	int failedReduces = 0 ; 
	int killedReduces = 0;
	
	long mapStarted = 0 ; 
	long mapFinished = 0 ; 
	long reduceStarted = 0 ; 
	long reduceFinished = 0; 
        
        Map <String,String> allHosts = new TreeMap<String,String>();
	
	for( JobHistory.Task task : tasks.values() ) {
	  
	  long startTime = task.getLong(Keys.START_TIME) ; 
	  long finishTime = task.getLong(Keys.FINISH_TIME) ; 
	  
          allHosts.put(task.get(Keys.HOSTNAME), null);

	  if( Values.MAP.name().equals(task.get(Keys.TASK_TYPE)) ){
	    if( mapStarted==0 || mapStarted > startTime ){
	      mapStarted = startTime; 
	    }
	    if(  mapFinished < finishTime ){
	      mapFinished = finishTime ; 
	    }
	    
	    Map<String, TaskAttempt> attempts = task.getTaskAttempts();
	    for( TaskAttempt attempt : attempts.values() ) {
	        totalMaps++; 
	        if( Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS)) ) {
	            failedMaps++; 
	        }
	        if( Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS)) ) {
	            killedMaps++; 
	        }
	    }
	  }else{
	    if( reduceStarted==0||reduceStarted > startTime ){
	      reduceStarted = startTime ; 
	    }
	    if(  reduceFinished < finishTime ){
	      reduceFinished = finishTime; 
	    }
	    Map<String, TaskAttempt> attempts = task.getTaskAttempts();
	    for( TaskAttempt attempt : attempts.values() ) {
	        totalReduces++; 
	        if( Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS)) ) {
	            failedReduces++; 
	        }
	        if( Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS)) ) {
	            killedReduces++; 
	        }
	    }
	  }
	}
%>
<b>Number of nodes used: </b> <%=allHosts.size() %><br/>
<b><a href="analysejobhistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>">Analyse This Job</a></b> 
<hr/>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr>
<td>Kind</td><td>Total Tasks(successful+failed+killed)</td><td>Successful tasks</td><td>Failed tasks</td><td>Killed tasks</td><td>Start Time</td><td>Finish Time</td>
</tr>
<tr>
<td>Map</td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.MAP.name() %>&status=all">
	  <%=totalMaps %></a></td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.MAP.name() %>&status=<%=Values.SUCCESS %>">
	  <%=job.getInt(Keys.FINISHED_MAPS) %></a></td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.MAP.name() %>&status=<%=Values.FAILED %>">
	  <%=failedMaps %></a></td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.MAP.name() %>&status=<%=Values.KILLED %>">
	  <%=killedMaps %></a></td>
	<td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, mapStarted, 0) %></td>
	<td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, mapFinished, mapStarted) %></td>
</tr>
<tr>
<td>Reduce</td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.REDUCE.name() %>&status=all">
	  <%=totalReduces%></a></td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.REDUCE.name() %>&status=<%=Values.SUCCESS %>">
	  <%=job.getInt(Keys.FINISHED_REDUCES)%></a></td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.REDUCE.name() %>&status=<%=Values.FAILED %>">
	  <%=failedReduces%></a></td>
	<td><a href="jobtaskshistory.jsp?jobid=<%=jobid %>&jobTrackerId=<%=jobTrackerId %>&taskType=<%=Values.REDUCE.name() %>&status=<%=Values.KILLED %>">
	  <%=killedReduces%></a></td>  
	<td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, reduceStarted, 0) %></td>
	<td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, reduceFinished, reduceStarted) %></td>
</tr>
 </table>

<br/>
 <%
	DefaultJobHistoryParser.FailedOnNodesFilter filter = new DefaultJobHistoryParser.FailedOnNodesFilter();
	String dir = System.getProperty("hadoop.log.dir") + File.separator + "history" ; 
 
	JobHistory.parseHistory(new File(dir, jobTrackerId+"_" + jobid), filter); 
	Map<String, Set<String>> badNodes = filter.getValues(); 
	if( badNodes.size() > 0 ) {
 %>
<h3>Failed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Failed Tasks</td></tr>
 <%	  
  for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
    String node = entry.getKey();
    Set<String> failedTasks = entry.getValue();
%>
	<tr>
		<td><%=node %></td>
		<td>
<%
		for( String t : failedTasks ) {
%>
		 <a href="taskdetailshistory.jsp?jobid=<%=jobid%>&jobTrackerId=<%=jobTrackerId %>&taskid=<%=t %>"><%=t %></a>,&nbsp;
<%		  
		}
%>	
		</td>
	</tr>
<%	  
     }
	}
 %>
</table>
<br/>
 <%
	DefaultJobHistoryParser.KilledOnNodesFilter killedFilter = new DefaultJobHistoryParser.KilledOnNodesFilter();
	dir = System.getProperty("hadoop.log.dir") + File.separator + "history" ; 
 
	JobHistory.parseHistory(new File(dir, jobTrackerId+"_" + jobid), filter); 
	badNodes = killedFilter.getValues(); 
	if( badNodes.size() > 0 ) {
 %>
<h3>Killed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Killed Tasks</td></tr>
 <%	  
  for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
    String node = entry.getKey();
    Set<String> killedTasks = entry.getValue();
%>
	<tr>
		<td><%=node %></td>
		<td>
<%
		for( String t : killedTasks ) {
%>
		 <a href="taskdetailshistory.jsp?jobid=<%=jobid%>&jobTrackerId=<%=jobTrackerId %>&taskid=<%=t %>"><%=t %></a>,&nbsp;
<%		  
		}
%>	
		</td>
	</tr>
<%	  
     }
	}
 %>
</table>
 </center>

</body></html>
