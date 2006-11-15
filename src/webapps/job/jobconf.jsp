<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.net.URL"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>


<%
  String jobId = request.getParameter("jobid");
  if (jobId == null) {
    out.println("<h2>Missing 'jobid' for fetching job configuration!</h2>");
 	return;
  }
%>
  
<html>

<title>Job Configuration: JobId - <%= jobId %></title>

<body>
<h2>Job Configuration: JobId - <%= jobId %></h2><br>

<%
  JobTracker tracker = JobTracker.getTracker();
  
  JobInProgress job = (JobInProgress)tracker.getJob(jobId);
  if (job == null) {
    out.print("<h4>Job '" + jobId + "' not found!</h4><br>\n");
    return;
  }
  
  JobStatus status = job.getStatus();
  int runState = status.getRunState();
  if (runState != JobStatus.RUNNING) {
    out.print("<h4>Job '" + jobId + "' not running!</h4><br>\n");
    return;
  }
  
  try {
    JobConf jobConf = job.getJobConf();
    ByteArrayOutputStream jobConfXML = new ByteArrayOutputStream();
    jobConf.write(jobConfXML);
    XMLUtils.transform(
        jobConf.getConfResourceAsInputStream("webapps/static/jobconf.xsl"),
	    new ByteArrayInputStream(jobConfXML.toByteArray()), 
	    out
	  );
  } catch (Exception e) {
    out.println("Failed to retreive job configuration for job '" + jobId + "!");
    out.println(e);
  }
%>

<br>
<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>

</body>
</html>