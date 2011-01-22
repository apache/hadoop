<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.mapreduce.JobACL"
  import="org.apache.hadoop.security.UserGroupInformation"
  import="org.apache.hadoop.security.authorize.AccessControlList"
  import="org.apache.hadoop.security.AccessControlException"
%>

<%!	private static final long serialVersionUID = 1L;
%>

<%
  String logFileString = request.getParameter("logFile");
  if (logFileString == null) {
    out.println("<h2>Missing 'logFile' for fetching job configuration!</h2>");
    return;
  }

  Path logFile = new Path(logFileString);
  String jobId = JSPUtil.getJobID(logFile.getName());

%>
  
<html>

<title>Job Configuration: JobId - <%= jobId %></title>

<body>
<h2>Job Configuration: JobId - <%= jobId %></h2><br>

<%
  Path jobFilePath = JSPUtil.getJobConfFilePath(logFile);
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  FSDataInputStream jobFile = null; 
  try {
    jobFile = fs.open(jobFilePath);
    JobConf jobConf = new JobConf(jobFilePath);
    JobConf clusterConf = (JobConf) application.getAttribute("jobConf");
    ACLsManager aclsManager = (ACLsManager) application.getAttribute("aclManager");

    JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
        response, clusterConf, aclsManager, fs, logFile);
    if (job == null) {
      return;
    }

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
