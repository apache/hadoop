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
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapreduce.jobhistory.*"
%>

<%!	private static SimpleDateFormat dateFormat 
                              = new SimpleDateFormat("d/MM HH:mm:ss") ; 
%>
<%!	private static final long serialVersionUID = 1L;
%>
<html><body>
<%
  String logFile = request.getParameter("logFile");
  String numTasks = request.getParameter("numTasks");
  int showTasks = 10 ; 
  if (numTasks != null) {
    showTasks = Integer.parseInt(numTasks);  
  }
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobTracker jobTracker = (JobTracker) application.getAttribute("job.tracker");
  JobHistoryParser.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobTracker, fs, new Path(logFile));
  if (job == null) {
    return;
  }
%>
<h2>Hadoop Job <a href="jobdetailshistory.jsp?logFile=<%=logFile%>"><%=job.getJobId() %> </a></h2>
<b>User : </b> <%=HtmlQuoting.quoteHtmlChars(job.getUsername()) %><br/>
<b>JobName : </b> <%=HtmlQuoting.quoteHtmlChars(job.getJobname()) %><br/>
<b>JobConf : </b> <%=job.getJobConfPath() %><br/> 
<b>Submitted At : </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getSubmitTime(), 0 ) %><br/> 
<b>Launched At : </b> <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLaunchTime(), job.getSubmitTime()) %><br/>
<b>Finished At : </b>  <%=StringUtils.getFormattedTimeWithDiff(dateFormat, job.getFinishTime(), job.getLaunchTime()) %><br/>
<b>Status : </b> <%= ((job.getJobStatus() == null)?"Incomplete" :job.getJobStatus()) %><br/> 
<hr/>
<center>
<%
  if (!JobStatus.getJobRunState(JobStatus.SUCCEEDED).equals(job.getJobStatus())) {
    out.print("<h3>No Analysis available as job did not finish</h3>");
    return;
  }
  
  HistoryViewer.AnalyzedJob avg = new HistoryViewer.AnalyzedJob(job);
  JobHistoryParser.TaskAttemptInfo [] mapTasks = avg.getMapTasks();
  JobHistoryParser.TaskAttemptInfo [] reduceTasks = avg.getReduceTasks();

  Comparator<JobHistoryParser.TaskAttemptInfo> cMap = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getFinishTime() - t1.getStartTime();
      long l2 = t2.getFinishTime() - t2.getStartTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  Comparator<JobHistoryParser.TaskAttemptInfo> cShuffle = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getShuffleFinishTime() - t1.getStartTime();
      long l2 = t2.getShuffleFinishTime() - t2.getStartTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  Comparator<JobHistoryParser.TaskAttemptInfo> cFinishShuffle = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getShuffleFinishTime(); 
      long l2 = t2.getShuffleFinishTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  Comparator<JobHistoryParser.TaskAttemptInfo> cFinishMapRed = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getFinishTime(); 
      long l2 = t2.getFinishTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };
  
  Comparator<JobHistoryParser.TaskAttemptInfo> cReduce = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getFinishTime() -
                t1.getShuffleFinishTime();
      long l2 = t2.getFinishTime() -
                t2.getShuffleFinishTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  }; 

  if (mapTasks == null || mapTasks.length <= 0) return;
  Arrays.sort(mapTasks, cMap);
  JobHistoryParser.TaskAttemptInfo minMap = mapTasks[mapTasks.length-1] ;
%>

<h3>Time taken by best performing Map task 
<a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=minMap.getAttemptId().getTaskID()%>">
<%=minMap.getAttemptId().getTaskID() %></a> : <%=StringUtils.formatTimeDiff(minMap.getFinishTime(), minMap.getStartTime() ) %></h3>
<h3>Average time taken by Map tasks: 
<%=StringUtils.formatTimeDiff(avg.getAvgMapTime(), 0) %></h3>
<h3>Worse performing map tasks</h3>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Time taken</td></tr>
<%
  for (int i=0;i<showTasks && i<mapTasks.length; i++) {
%>
    <tr>
    <td><a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=mapTasks[i].getAttemptId().getTaskID()%>">
        <%=mapTasks[i].getAttemptId().getTaskID() %></a></td>
    <td><%=StringUtils.formatTimeDiff(mapTasks[i].getFinishTime(), mapTasks[i].getStartTime()) %></td>
    </tr>
<%
  }
%>
</table>
<%  
  Arrays.sort(mapTasks, cFinishMapRed);
  JobHistoryParser.TaskAttemptInfo lastMap = mapTasks[0] ;
%>

<h3>The last Map task 
<a href="taskdetailshistory.jsp?logFile=<%=logFile%>
&tipid=<%=lastMap.getAttemptId().getTaskID()%>"><%=lastMap.getAttemptId().getTaskID() %></a> 
finished at (relative to the Job launch time): 
<%=StringUtils.getFormattedTimeWithDiff(dateFormat, 
                              lastMap.getFinishTime(), 
                              job.getLaunchTime()) %></h3>
<hr/>

<%
  if (reduceTasks.length <= 0) return;
  Arrays.sort(reduceTasks, cShuffle); 
  JobHistoryParser.TaskAttemptInfo minShuffle = reduceTasks[reduceTasks.length-1] ;
%>
<h3>Time taken by best performing shuffle
<a href="taskdetailshistory.jsp?logFile=<%=logFile%>
&tipid=<%=minShuffle.getAttemptId().getTaskID()%>"><%=minShuffle.getAttemptId().getTaskID()%></a> : 
<%=StringUtils.formatTimeDiff(minShuffle.getShuffleFinishTime(),
                              minShuffle.getStartTime() ) %></h3>
<h3>Average time taken by Shuffle: 
<%=StringUtils.formatTimeDiff(avg.getAvgShuffleTime(), 0) %></h3>
<h3>Worse performing Shuffle(s)</h3>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Time taken</td></tr>
<%
  for (int i=0;i<showTasks && i<reduceTasks.length; i++) {
%>
    <tr>
    <td><a href="taskdetailshistory.jsp?logFile=
<%=logFile%>&tipid=<%=reduceTasks[i].getAttemptId().getTaskID()%>">
<%=reduceTasks[i].getAttemptId().getTaskID() %></a></td>
    <td><%=
           StringUtils.formatTimeDiff(
                       reduceTasks[i].getShuffleFinishTime(),
                       reduceTasks[i].getStartTime()) %>
    </td>
    </tr>
<%
  }
%>
</table>
<%  
  Arrays.sort(reduceTasks, cFinishShuffle);
  JobHistoryParser.TaskAttemptInfo lastShuffle = reduceTasks[0] ;
%>

<h3>The last Shuffle  
<a href="taskdetailshistory.jsp?logFile=<%=logFile%>
&tipid=<%=lastShuffle.getAttemptId().getTaskID()%>"><%=lastShuffle.getAttemptId().getTaskID()%>
</a> finished at (relative to the Job launch time): 
<%=StringUtils.getFormattedTimeWithDiff(dateFormat,
                              lastShuffle.getShuffleFinishTime(),
                              job.getLaunchTime() ) %></h3>

<%
  Arrays.sort(reduceTasks, cReduce); 
  JobHistoryParser.TaskAttemptInfo minReduce = reduceTasks[reduceTasks.length-1] ;
%>
<hr/>
<h3>Time taken by best performing Reduce task : 
<a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=minReduce.getAttemptId().getTaskID()%>">
<%=minReduce.getAttemptId().getTaskID() %></a> : 
<%=StringUtils.formatTimeDiff(minReduce.getFinishTime(),
    minReduce.getShuffleFinishTime() ) %></h3>

<h3>Average time taken by Reduce tasks: 
<%=StringUtils.formatTimeDiff(avg.getAvgReduceTime(), 0) %></h3>
<h3>Worse performing reduce tasks</h3>
<table border="2" cellpadding="5" cellspacing="2">
<tr><td>Task Id</td><td>Time taken</td></tr>
<%
  for (int i=0;i<showTasks && i<reduceTasks.length; i++) {
%>
    <tr>
    <td><a href="taskdetailshistory.jsp?logFile=<%=logFile%>&tipid=<%=reduceTasks[i].getAttemptId().getTaskID()%>">
        <%=reduceTasks[i].getAttemptId().getTaskID() %></a></td>
    <td><%=StringUtils.formatTimeDiff(
             reduceTasks[i].getFinishTime(),
             reduceTasks[i].getShuffleFinishTime()) %></td>
    </tr>
<%
  }
%>
</table>
<%  
  Arrays.sort(reduceTasks, cFinishMapRed);
  JobHistoryParser.TaskAttemptInfo lastReduce = reduceTasks[0] ;
%>

<h3>The last Reduce task 
<a href="taskdetailshistory.jsp?logFile=<%=logFile%>
&tipid=<%=lastReduce.getAttemptId().getTaskID()%>"><%=lastReduce.getAttemptId().getTaskID()%>
</a> finished at (relative to the Job launch time): 
<%=StringUtils.getFormattedTimeWithDiff(dateFormat,
                              lastReduce.getFinishTime(),
                              job.getLaunchTime() ) %></h3>
</center>
</body></html>
