<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.mapred.JobHistory.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
    PathFilter jobLogFileFilter = new PathFilter() {
      public boolean accept(Path path) {
        return !(path.getName().endsWith(".xml"));
      }
    };

    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String jobId = request.getParameter("jobid");
    JobHistory.JobInfo job = (JobHistory.JobInfo)
                               request.getSession().getAttribute("job");
    // if session attribute of JobInfo exists and is of different job's,
    // then remove the attribute
    // if the job has not yet finished, remove the attribute sothat it 
    // gets refreshed.
    boolean isJobComplete = false;
    if (null != job) {
      String jobStatus = job.get(Keys.JOB_STATUS);
      isJobComplete = Values.SUCCESS.name() == jobStatus
                      || Values.FAILED.name() == jobStatus
                      || Values.KILLED.name() == jobStatus;
    }
    if (null != job && 
       (!jobId.equals(job.get(Keys.JOBID)) 
         || !isJobComplete)) {
      // remove jobInfo from session, keep only one job in session at a time
      request.getSession().removeAttribute("job"); 
      job = null ; 
    }
	
    if (null == job) {
      String jobLogFile = request.getParameter("logFile");
      job = new JobHistory.JobInfo(jobId); 
      DefaultJobHistoryParser.parseJobTasks(jobLogFile, job, fs) ; 
      request.getSession().setAttribute("job", job);
      request.getSession().setAttribute("fs", fs);
    }
%>
