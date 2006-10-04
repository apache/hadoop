<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.mapred.JobHistory.*"
%>
<jsp:include page="loadhistory.jsp">
	<jsp:param name="historyFile" value="<%=request.getParameter("historyFile") %>"/>
	<jsp:param name="reload" value="<%=request.getParameter("reload") %>"/>
</jsp:include>
<%!	
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ;
%>

<html><body>
<%
		Map<String, Map<String, JobInfo>> jobTrackerToJobs = 
		  (Map<String, Map<String, JobInfo>>)request.getSession().getAttribute("jobHistory"); 
		
		if( null == jobTrackerToJobs ){
		  out.println("NULL !!!"); 
		  return ; 
		}

		for(String trackerStartTime : jobTrackerToJobs.keySet() ){ 
		  Map<String, JobInfo> jobs = (Map<String, JobInfo>)jobTrackerToJobs.get(trackerStartTime) ; 
%>
<h2>JobTracker started at : <%=new Date(Long.parseLong(trackerStartTime)) %></h2>
<hr/>
<h3>Completed Jobs </h3>
<center>
<%	
		printJobs(trackerStartTime, jobs, Values.SUCCESS.name(), out) ; 
%>
</center> 
<h3>Failed Jobs </h3>
<center>
<%	
		printJobs(trackerStartTime, jobs, Values.FAILED.name() , out) ; 
%>
</center>
<h3>Incomplete Jobs </h3>
<center>
<%	
		printJobs(trackerStartTime, jobs, null , out) ; 
%>
</center>
<hr/><br/>
<%
		} // end while trackers 
%>
 
<%!
	private void printJobs(String trackerid, Map<String, JobInfo> jobs, String status, JspWriter out) throws IOException{
	  if( jobs.size() == 0 ) {
	    out.print("<h3>No Jobs available</h3>"); 
	    return ; 
	  }
      out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
      out.print("<tr>");
   	  out.print("<td align=\"center\">Job Id</td><td>Name</td><td>User</td><td>Submit Time</td>" + 
   	      "<td>Finish Time</td><td>Total Maps</td><td>Total Reduces</td>" +
   		  "<td>Finished Maps</td><td>Finished Reduces</td>") ; 
   	  out.print("</tr>"); 
   	      
	  for( JobInfo job : jobs.values() ) {
		if( null != status && status.equals(job.get(Keys.JOB_STATUS)) ) {
		  printJob(trackerid, job, out); 
		}else if( status == null && job.get(Keys.JOB_STATUS) == null ) {
		  printJob(trackerid, job, out); 
		}
	  }
	  out.print("</table>");
	}

	private void printJob(String trackerid, JobInfo job, JspWriter out)throws IOException{
	    out.print("<tr>"); 
	    out.print("<td>" + "<a href=\"jobdetailshistory.jsp?jobid="+ job.get(Keys.JOBID) + 
	        "&jobTrackerId=" +trackerid  + "\">" + job.get(Keys.JOBID) + "</a></td>"); 
	    out.print("<td>" + job.get(Keys.JOBNAME) + "</td>"); 
	    out.print("<td>" + job.get(Keys.USER) + "</td>"); 
	    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME),0) + "</td>"); 
	    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME) , job.getLong(Keys.SUBMIT_TIME) ) + "</td>");
	    out.print("<td>" + job.get(Keys.TOTAL_MAPS) + "</td>"); 
	    out.print("<td>" + job.get(Keys.TOTAL_REDUCES) + "</td>"); 
	    out.print("<td>" + job.get(Keys.FINISHED_MAPS) + "</td>"); 
	    out.print("<td>" + job.get(Keys.FINISHED_REDUCES) + "</td>"); 
	    out.print("</tr>");
	}
 %>  
</body></html>