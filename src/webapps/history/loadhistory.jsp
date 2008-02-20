<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.mapred.JobHistory.*"
%>
<%
    PathFilter jobLogFileFilter = new PathFilter() {
      public boolean accept(Path path) {
        return !(path.getName().endsWith(".xml"));
      }
    };
    
	FileSystem fs = (FileSystem) application.getAttribute("fileSys");
	String jobId =  (String)request.getParameter("jobid");
	JobHistory.JobInfo job = (JobHistory.JobInfo)request.getSession().getAttribute("job");
	if (null != job && (!jobId.equals(job.get(Keys.JOBID)))){
      // remove jobInfo from session, keep only one job in session at a time
      request.getSession().removeAttribute("job"); 
      job = null ; 
    }
	
	if (null == job) {
      String jobLogFile = (String)request.getParameter("logFile");
      job = new JobHistory.JobInfo(jobId); 
      DefaultJobHistoryParser.parseJobTasks(jobLogFile, job, fs) ; 
      request.getSession().setAttribute("job", job);
      request.getSession().setAttribute("fs", fs);
	}
%>
