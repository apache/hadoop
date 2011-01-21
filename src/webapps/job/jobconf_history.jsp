<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.net.URL"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
%>


<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
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
  Path logDir = new Path(request.getParameter("jobLogDir"));
  Path jobFilePath = new Path(logDir, 
                       request.getParameter("jobUniqueString") + "_conf.xml");
  FileSystem fs = (FileSystem)request.getSession().getAttribute("fs");
  FSDataInputStream jobFile = null; 
  try {
    jobFile = fs.open(jobFilePath);
    JobConf jobConf = new JobConf(jobFilePath);
    XMLUtils.transform(
        jobConf.getConfResourceAsInputStream("webapps/static/jobconf.xsl"),
        jobFile, out);
  } catch (Exception e) {
    out.println("Failed to retreive job configuration for job '" + jobId + "!");
    out.println(e);
  } finally {
    if (jobFile != null) {
      try { 
        jobFile.close(); 
      } catch (IOException e) {}
    }
  } 
%>

<br>
<%
out.println(ServletUtil.htmlFooter());
%>
