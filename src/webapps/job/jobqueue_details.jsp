<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.util.Vector"
  import="java.util.Collection"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.util.ServletUtil"
%>
<%!
private static final long serialVersionUID = 526456771152222127L; 
%>
<%
  JobTracker tracker = 
    (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
    StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String queueName = 
    StringUtils.escapeHTML(request.getParameter("queueName"));
  Vector<JobInProgress> completedJobs = new Vector<JobInProgress>();
  Vector<JobInProgress> failedJobs = new Vector<JobInProgress>();
  Vector<JobInProgress> runningJobs = new Vector<JobInProgress>();
  Vector<JobInProgress> waitingJobs = new Vector<JobInProgress>();
  Collection<JobInProgress> jobs = null;
  TaskScheduler scheduler = tracker.getTaskScheduler();
  
  
  
  if((queueName != null) && !(queueName.trim().equals(""))) {
    jobs = scheduler.getJobs(queueName);
  }
  
  if(jobs!=null && !jobs.isEmpty()) {
    for(JobInProgress job :jobs) {
      switch(job.getStatus().getRunState()){
      case JobStatus.RUNNING:
        runningJobs.add(job);
        break;
      case JobStatus.PREP:
        waitingJobs.add(job);
        break;
      case JobStatus.SUCCEEDED:
        completedJobs.add(job);
        break;
      case JobStatus.FAILED:
        failedJobs.add(job);
        break;
      }
    }
  }
  JobQueueInfo schedInfo = tracker.getQueueInfo(queueName);
%>
<html>
<head>
<title>Queue details for <%=queueName!=null?queueName:""%> </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
</head>
<body>
<% JSPUtil.processButtons(request, response, tracker); %>
<%
  String schedulingInfoString = schedInfo.getSchedulingInfo();
%>
<h1>Hadoop Job Queue Scheduling Information on 
  <a href="jobtracker.jsp"><%=trackerName%></a>
</h1>
<div>
Scheduling Information : <%= schedulingInfoString.replaceAll("\n","<br/>") %>
</div>
<hr/>
<%
if(jobs == null || jobs.isEmpty()) {
%>
<center>
<h2> No Jobs found for the Queue :: <%=queueName!=null?queueName:""%> </h2>
<hr/>
</center>
<%
}else {
%>
<center>
<h2> Job Summary for the Queue :: <%=queueName!=null?queueName:"" %> </h2>
<hr/>
</center>
<h2>Running Jobs</h2>
<%=
  JSPUtil.generateJobTable("Running", runningJobs, 30, 0)
%>
<hr>

<h2>Waiting Jobs</h2>
<%=
  JSPUtil.generateJobTable("Waiting", waitingJobs, 30, runningJobs.size())
%>
<hr>

<h2>Completed Jobs</h2>
<%=
  JSPUtil.generateJobTable("Completed", completedJobs, 0,
      (runningJobs.size()+waitingJobs.size()))
%>

<hr>

<h2>Failed Jobs</h2>
<%=
  JSPUtil.generateJobTable("Failed", failedJobs, 0,
      (runningJobs.size()+waitingJobs.size()+completedJobs.size()))
%>

<hr>


<% } %>

<%
out.println(ServletUtil.htmlFooter());
%>

