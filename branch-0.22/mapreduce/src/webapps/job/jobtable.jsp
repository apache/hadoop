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
import="org.apache.hadoop.mapred.*"
import="javax.servlet.*"
import="javax.servlet.http.*"
import="java.io.*"
import="java.util.*"
import="org.apache.hadoop.util.ServletUtil"
%>
<%!
private static final long serialVersionUID = 1L;
%>
<%
JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
QueueManager qmgr = tracker.getQueueManager();
String queue = request.getParameter("queue_name");
TaskScheduler scheduler = tracker.getTaskScheduler();
JobQueueInfo queueInfo = tracker.getQueueInfo(queue);
%>
<%
if(queueInfo == null || (queueInfo.getChildren() != null &&
    queueInfo.getChildren().size() != 0) ){
%>
<% 
} else {
%>
<%
Collection<JobInProgress> jobs = scheduler.getJobs(queue);
String[] queueLabelSplits = queue.split(":");
String queueLabel = 
  queueLabelSplits.length==0?queue:queueLabelSplits[queueLabelSplits.length-1];

if(jobs == null || jobs.isEmpty()) {
%>
<center>
<h2><b> No Jobs found for the QueueName:: <%=queueLabel%> </b>
</h2>
</center>
<%
}else {
%>
<center>
<p>
<h1> Job Summary for the Queue :: <%=queueLabel%> </h1>
(In the order maintained by the scheduler)
<br/><br/><br/>
<%=
  JSPUtil.generateJobTable("Job List", jobs, 30, 5, tracker.conf)
%>
</center>
<%
}
%>
<%} %>
