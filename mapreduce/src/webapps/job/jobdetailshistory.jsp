<%
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapreduce.TaskAttemptID"
  import="org.apache.hadoop.mapreduce.TaskID"
  import="org.apache.hadoop.mapreduce.Counter"
  import="org.apache.hadoop.mapreduce.Counters"
  import="org.apache.hadoop.mapreduce.CounterGroup"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.*"
  import="org.apache.hadoop.mapreduce.jobhistory.*"
  import="java.security.PrivilegedExceptionAction"
  import="org.apache.hadoop.security.AccessControlException"
  import="org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo"
  import="org.apache.hadoop.mapreduce.JobACL"
  import="org.apache.hadoop.security.authorize.AccessControlList"
%>
<%!private static final long serialVersionUID = 1L;
%>

<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>
<%
    String logFile = request.getParameter("logFile");
    final Path jobFile = new Path(logFile);
    String jobid = JobHistory.getJobIDFromHistoryFilePath(jobFile).toString();

    final FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    final JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
    JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request, response,
        jobTracker, fs, jobFile);
    if (job == null) {
      return;
    }
%>

<html>
<head>
<title>Hadoop Job <%=jobid%> on History Viewer</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>

<h2>Hadoop Job <%=jobid %> on <a href="jobhistory.jsp">History Viewer</a></h2>

<b>User: </b> <%=HtmlQuoting.quoteHtmlChars(job.getUsername()) %><br/>
<b>JobName: </b> <%=HtmlQuoting.quoteHtmlChars(job.getJobname()) %><br/>
<b>JobConf: </b> <a href="jobconf_history.jsp?logFile=<%=logFile%>"> 
                 <%=job.getJobConfPath() %></a><br/> 
<%         
  Map<JobACL, AccessControlList> jobAcls = job.getJobACLs();
  JSPUtil.printJobACLs(jobTracker, jobAcls, out);
%>
<b>Submitted At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getSubmitTime(), 0 )  %><br/> 
<b>Launched At: </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLaunchTime(), job.getSubmitTime()) %><br/>
<b>Finished At: </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getFinishTime(), job.getLaunchTime()) %><br/>
<b>Status: </b> <%= ((job.getJobStatus()) == null ? "Incomplete" :job.getJobStatus()) %><br/> 
<%
    HistoryViewer.SummarizedJob sj = new HistoryViewer.SummarizedJob(job);
%>
<b><a href="analysejobhistory.jsp?logFile=<%=logFile%>">Analyse This Job</a></b> 
<hr/>
<center>
<table border="2" cellpadding="5" cellspacing="2">
<tr>
<td>Kind</td><td>Total Tasks(successful+failed+killed)</td><td>Successful tasks</td><td>Failed tasks</td><td>Killed tasks</td><td>Start Time</td><td>Finish Time</td>
</tr>
<tr>
<td>Setup</td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_SETUP&status=all">
        <%=sj.getTotalSetups()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_SETUP&status=SUCCEEDED">
        <%=sj.getNumFinishedSetups()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_SETUP&status=FAILED">
        <%=sj.getNumFailedSetups()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_SETUP&status=KILLED">
        <%=sj.getNumKilledSetups()%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getSetupStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getSetupFinished(), sj.getSetupStarted()) %></td>
</tr>
<tr>
<td>Map</td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=MAP&status=all">
        <%=sj.getTotalMaps()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=MAP&status=SUCCEEDED">
        <%=job.getFinishedMaps() %></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=MAP&status=FAILED">
        <%=sj.getNumFailedMaps()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=MAP&status=KILLED">
        <%=sj.getNumKilledMaps()%></a></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getMapStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getMapFinished(), sj.getMapStarted()) %></td>
</tr>
<tr>
<td>Reduce</td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=REDUCE&status=all">
        <%=sj.getTotalReduces()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=REDUCE&status=SUCCEEDED">
        <%=job.getFinishedReduces()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=REDUCE&status=FAILED">
        <%=sj.getNumFailedReduces()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=REDUCE&status=KILLED">
        <%=sj.getNumKilledReduces()%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getReduceStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getReduceFinished(), sj.getReduceStarted()) %></td>
</tr>
<tr>
<td>Cleanup</td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=all">
        <%=sj.getTotalCleanups()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=SUCCEEDED">
        <%=sj.getNumFinishedCleanups()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=FAILED">
        <%=sj.getNumFailedCleanups()%></a></td>
    <td><a href="jobtaskshistory.jsp?logFile=<%=logFile%>&taskType=JOB_CLEANUP&status=KILLED>">
        <%=sj.getNumKilledCleanups()%></a></td>  
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getCleanupStarted(), 0) %></td>
    <td><%=StringUtils.getFormattedTimeWithDiff(dateFormat, sj.getCleanupFinished(), sj.getCleanupStarted()) %></td>
</tr>
</table>

<br>
<br>

<table border=2 cellpadding="5" cellspacing="2">
  <tr>
  <th><br/></th>
  <th>Counter</th>
  <th>Map</th>
  <th>Reduce</th>
  <th>Total</th>
</tr>

<%  

 Counters totalCounters = job.getTotalCounters();
 Counters mapCounters = job.getMapCounters();
 Counters reduceCounters = job.getReduceCounters();

 if (totalCounters != null) {
   for (String groupName : totalCounters.getGroupNames()) {
     CounterGroup totalGroup = totalCounters.getGroup(groupName);
     CounterGroup mapGroup = mapCounters.getGroup(groupName);
     CounterGroup reduceGroup = reduceCounters.getGroup(groupName);
  
     Format decimal = new DecimalFormat();
  
     boolean isFirst = true;
     Iterator<Counter> ctrItr = totalGroup.iterator();
     while(ctrItr.hasNext()) {
       Counter counter = ctrItr.next();
       String name = counter.getName();
       String mapValue = 
        decimal.format(mapGroup.findCounter(name).getValue());
       String reduceValue = 
        decimal.format(reduceGroup.findCounter(name).getValue());
       String totalValue = 
        decimal.format(counter.getValue());
%>
       <tr>
<%
       if (isFirst) {
         isFirst = false;
%>
         <td rowspan="<%=totalGroup.size()%>">
         <%=HtmlQuoting.quoteHtmlChars(totalGroup.getDisplayName())%></td>
<%
       }
%>
       <td><%=HtmlQuoting.quoteHtmlChars(counter.getDisplayName())%></td>
       <td align="right"><%=mapValue%></td>
       <td align="right"><%=reduceValue%></td>
       <td align="right"><%=totalValue%></td>
     </tr>
<%
      }
    }
  }
%>
</table>
<br>

<br/>
 <%
    HistoryViewer.FilteredJob filter = new HistoryViewer.FilteredJob(job,TaskStatus.State.FAILED.toString()); 
    Map<String, Set<TaskID>> badNodes = filter.getFilteredMap(); 
    if (badNodes.size() > 0) {
 %>
<h3>Failed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Failed Tasks</td></tr>
 <%	  
      for (Map.Entry<String, Set<TaskID>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<TaskID> failedTasks = entry.getValue();
%>
        <tr>
        <td><%=node %></td>
        <td>
<%
          boolean firstId = true;
          for (TaskID tid : failedTasks) {
             if (firstId) {
              firstId = false;
%>
            <a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=tid %>"><%=tid %></a>
<%		  
          } else {
%>	
            ,&nbsp<a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=tid %>"><%=tid %></a>
<%		  
          }
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
    filter = new HistoryViewer.FilteredJob(job, TaskStatus.State.KILLED.toString());
    badNodes = filter.getFilteredMap(); 
    if (badNodes.size() > 0) {
 %>
<h3>Killed tasks attempts by nodes </h3>
<table border="1">
<tr><td>Hostname</td><td>Killed Tasks</td></tr>
 <%	  
      for (Map.Entry<String, Set<TaskID>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<TaskID> killedTasks = entry.getValue();
%>
        <tr>
        <td><%=node %></td>
        <td>
<%
        boolean firstId = true;
        for (TaskID tid : killedTasks) {
             if (firstId) {
              firstId = false;
%>
            <a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=tid %>"><%=tid %></a>
<%		  
          } else {
%>	
            ,&nbsp<a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=tid %>"><%=tid %></a>
<%		  
          }
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
