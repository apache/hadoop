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
<%!
private static final long serialVersionUID = 1L; 
%>
<%@ page 
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.util.ServletUtil"
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<%
JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
JobQueueInfo[] queues = tracker.getRootJobQueues();
%>
<title>Queue Information</title>
</head>
<body>
<h2 id="scheduling_info">Scheduling Information</h2>
<table border="2" cellpadding="5" cellspacing="2">
<thead style="font-weight: bold">
<tr>
<td> Queue Name </td>
<td> Scheduling Information</td>
</tr>
</thead>
<tbody>
<%
for(JobQueueInfo queue: queues) {
  String queueName = queue.getQueueName();
  String state = queue.getQueueState();
  String schedulingInformation = queue.getSchedulingInfo();
  if(schedulingInformation == null || schedulingInformation.trim().equals("")) {
    schedulingInformation = "NA";
  }
%>
<tr>
<td><a href="jobqueue_details.jsp?queueName=<%=queueName%>"><%=queueName%></a>
</td>
<td>
<%=HtmlQuoting.quoteHtmlChars(schedulingInformation).replaceAll("\n","<br/>")%>
</td>
</tr>
<%
}
%>
</tbody>
</table>
<a href="jobtracker.jsp"><h2>Job Tracker</h2></a>
</body>
</html>
