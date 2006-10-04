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
<%
	// Reload master index or a job file in session
	String reload = request.getParameter("reload"); 
	String jobid = request.getParameter("jobid"); 
	String jobTrackerId = request.getParameter("jobTrackerId"); 
	
	String jobLogDir = System.getProperty("hadoop.log.dir") + File.separator + "history" ; 
	
	String masterIndex = request.getParameter("historyFile"); ;
	
	if( null != masterIndex ) {
		String filePath = jobLogDir + File.separator + masterIndex ;
		File historyFile = new File(filePath); 
		if( null == request.getSession().getAttribute("jobHistory") || "true".equals(reload) ){
		  request.getSession().setAttribute("jobHistory", 
				DefaultJobHistoryParser.parseMasterIndex(historyFile)); 
		}
	}

	if( jobid != null && jobTrackerId != null ) {
	  
		JobHistory.JobInfo job = (JobHistory.JobInfo)request.getSession().getAttribute("job");
		if( null != job && (! jobid.equals(job.get(Keys.JOBID)) || 
		    ! jobTrackerId.equals(job.get(Keys.JOBTRACKERID)))){
		  // remove jobInfo from session, keep only one job in session at a time
		  request.getSession().removeAttribute("job"); 
		  job = null ; 
		}
		
		if( null == job ) {
  		  String jobLogFile = jobTrackerId + "_" + jobid; 
		  job = new JobHistory.JobInfo(jobid); 
		  job.set(Keys.JOBTRACKERID, jobTrackerId);
	      DefaultJobHistoryParser.parseJobTasks(
		  	    new File(jobLogDir + File.separator + jobLogFile), job) ; 
		  request.getSession().setAttribute("job", job); 
		}
	}
%>
