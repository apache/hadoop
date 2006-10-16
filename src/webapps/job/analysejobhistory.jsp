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
<html><body>
<%
	String jobid = request.getParameter("jobid");
	String jobTrackerId = request.getParameter("jobTrackerId");
	String numTasks = request.getParameter("numTasks");
	int showTasks = 10 ; 
	if( numTasks != null ) {
	  showTasks = Integer.parseInt(numTasks);  
	}

	JobInfo job = (JobInfo)request.getSession().getAttribute("job");

%>
<h2>Hadoop Job <a href="jobdetailshistory.jsp?jobid=<%=jobid%>&&jobTrackerId=<%=jobTrackerId %>"><%=jobid %> </a></h2>

<b>User : </b> <%=job.get(Keys.USER) %><br/> 
<b>JobName : </b> <%=job.get(Keys.JOBNAME) %><br/> 
<b>JobConf : </b> <%=job.get(Keys.JOBCONF) %><br/> 
<b>Submitted At : </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME), 0 ) %><br/> 
<b>Launched At : </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.LAUNCH_TIME), job.getLong(Keys.SUBMIT_TIME)) %><br/>
<b>Finished At : </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME), job.getLong(Keys.LAUNCH_TIME)) %><br/>
<b>Status : </b> <%= ((job.get(Keys.JOB_STATUS) == null)?"Incomplete" :job.get(Keys.JOB_STATUS)) %><br/> 
<hr/>
<center>
<%
	if( ! Values.SUCCESS.name().equals(job.get(Keys.JOB_STATUS)) ){
	  out.print("<h3>No Analysis available as job did not finish</h3>");
	  return ;
	}
	Map<String, JobHistory.Task> tasks = job.getAllTasks();
	int finishedMaps = job.getInt(Keys.FINISHED_MAPS)  ;
	int finishedReduces = job.getInt(Keys.FINISHED_REDUCES) ;
	JobHistory.Task [] mapTasks = new JobHistory.Task[finishedMaps]; 
	JobHistory.Task [] reduceTasks = new JobHistory.Task[finishedReduces]; 
	int mapIndex = 0 , reduceIndex=0; 
	
	for( JobHistory.Task task : tasks.values() ) {
	  if( Values.MAP.name().equals(task.get(Keys.TASK_TYPE)) ){
		  mapTasks[mapIndex++] = task ; 
	  }else{
	    reduceTasks[reduceIndex++] = task; 
	  }
	}
	 
	Comparator<JobHistory.Task> c = new Comparator<JobHistory.Task>(){
	  public int compare(JobHistory.Task t1, JobHistory.Task t2){
	    Long l1 = new Long(t1.getLong(Keys.FINISH_TIME) - t1.getLong(Keys.START_TIME)); 
	    Long l2 = new Long(t2.getLong(Keys.FINISH_TIME) - t2.getLong(Keys.START_TIME)) ;
	    return l2.compareTo(l1); 
	  }
	}; 
	Arrays.sort(mapTasks, c);
	Arrays.sort(reduceTasks, c); 
	
	JobHistory.Task minMap = mapTasks[mapTasks.length-1] ;
	JobHistory.Task minReduce = reduceTasks[reduceTasks.length-1] ;
	
%>

<h3>Time taken by best performing Map task 
<a href="taskdetailshistory.jsp?jobid=<%=jobid%>&jobTrackerId=<%=jobTrackerId%>&taskid=<%=minMap.get(Keys.TASKID)%>">
<%=minMap.get(Keys.TASKID) %></a> : <%=StringUtils.formatTimeDiff(minMap.getLong(Keys.FINISH_TIME), minMap.getLong(Keys.START_TIME) ) %></h3>
<h3>Worse performing map tasks</h3>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Time taken</td></tr>
<%
	for( int i=0;i<showTasks && i<mapTasks.length; i++){
%>
		<tr>
			<td><a href="taskdetailshistory.jsp?jobid=<%=jobid%>&jobTrackerId=<%=jobTrackerId%>&taskid=<%=mapTasks[i].get(Keys.TASKID)%>">
  		    <%=mapTasks[i].get(Keys.TASKID) %></a></td>
			<td><%=StringUtils.formatTimeDiff(mapTasks[i].getLong(Keys.FINISH_TIME), mapTasks[i].getLong(Keys.START_TIME)) %></td>
		</tr>
<%
	}
%>
</table>
<h3>Time taken by best performing Reduce task : 
<a href="taskdetailshistory.jsp?jobid=<%=jobid%>&jobTrackerId=<%=jobTrackerId%>&taskid=<%=minReduce.get(Keys.TASKID)%>">
<%=minReduce.get(Keys.TASKID) %></a> : <%=StringUtils.formatTimeDiff(minReduce.getLong(Keys.FINISH_TIME), minReduce.getLong(Keys.START_TIME) ) %></h3>

<h3>Worse performing reduce tasks</h3>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Time taken</td></tr>
<%
	for( int i=0;i<showTasks && i<reduceTasks.length; i++){
%>
		<tr>
			<td><a href="taskdetailshistory.jsp?jobid=<%=jobid%>&jobTrackerId=<%=jobTrackerId%>&taskid=<%=reduceTasks[i].get(Keys.TASKID)%>">
			<%=reduceTasks[i].get(Keys.TASKID) %></a></td>
			<td><%=StringUtils.formatTimeDiff(reduceTasks[i].getLong(Keys.FINISH_TIME), reduceTasks[i].getLong(Keys.START_TIME)) %></td>
		</tr>
<%
	}
%>
</table>
 </center>
 </body></html>