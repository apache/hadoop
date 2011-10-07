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
    contentType="application/octet-stream; charset=UTF-8"
    import="org.apache.hadoop.mapred.*"
    import="org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck"
%>

<%
    String jobId = request.getParameter("jobid");
    if (jobId == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing 'jobid'!");
      return;
    }
    
    final JobTracker tracker = (JobTracker) application.getAttribute(
        "job.tracker");

    final JobID jobIdObj = JobID.forName(jobId);
    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobIdObj,
                                                     request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();

    if (job != null && !job.getStatus().isJobComplete()) {
      response.getWriter().print("Job " + jobId + " is still in running");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    String historyFile = JobHistory.getHistoryFilePath(jobIdObj);
    if (historyFile == null) {
      response.getWriter().print("Job " + jobId + " not known");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    String historyUrl = "http://" + JobHistoryServer.getAddress(tracker.conf) +
      "/historyfile?logFile=" +
      JobHistory.JobInfo.encodeJobHistoryFilePath(historyFile);
    response.sendRedirect(response.encodeRedirectURL(historyUrl));
%>