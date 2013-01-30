<%!
/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
%>
<%@ page
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck"
  import="org.apache.hadoop.util.*"
  import="java.lang.Integer"
  import="java.text.SimpleDateFormat"
  import="org.codehaus.jackson.map.ObjectMapper"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; %>

<%! 
  public static class ErrorResponse {

    private final long errorCode;
    private final String errorDescription;

    // Constructor
    ErrorResponse(long ec, String ed) {

      errorCode = ec;
      errorDescription = ed;
    }

    // Getters
    public long getErrorCode() { return errorCode; }
    public String getErrorDescription() { return errorDescription; }
  }

  public static class JobTasksResponse {

    public static class JobTaskInfo {

      private final String taskId;
      private final float completionPercent;
      private final String status;
      private final String startTime;
      private final String finishTime;
      private final long durationSecs;

      // Constructor
      JobTaskInfo(TaskReport tr) {

        taskId = tr.getTaskID().toString();
        completionPercent = tr.getProgress() * 100.0f;
        status = tr.getState();
        startTime = dateFormat.format(new Date(tr.getStartTime()));
        finishTime = dateFormat.format(new Date(tr.getFinishTime()));
        durationSecs = (tr.getFinishTime() - tr.getStartTime())/1000;
      }

      // Getters
      public String getTaskId() { return taskId; }
      public float getCompletionPercent() { return completionPercent; }
      public String getStatus() { return status; }
      public String getStartTime() { return startTime; }
      public String getFinishTime() { return finishTime; }
      public long getDurationSecs() { return durationSecs; }
    }

    private final String jobId;
    private final String type;
    private final String state;
    private final int pageNum;
    private final int startIndex;
    private final int endIndex;
    private final Collection<JobTaskInfo> jobTasksInfo;

    // Constructor
    JobTasksResponse(String ji, String t, String s, int pn, int si, int ei) {

      jobId = ji;
      type = t;
      state = s;
      pageNum = pn;
      startIndex = si;
      endIndex = ei;

      jobTasksInfo = new ArrayList<JobTaskInfo>();
    }

    // To add one JobTaskInfo object at a time.
    void addJobTaskInfo(JobTaskInfo ti) { jobTasksInfo.add(ti); }

    // Getters
    public String getJobId() { return jobId; }
    public String getType() { return type; }
    public String getState() { return state; }
    public int getPageNum() { return pageNum; }
    public int getStartIndex() { return startIndex; }
    public int getEndIndex() { return endIndex; }
    public Collection<JobTaskInfo> getTasksInfo() { return jobTasksInfo; }
  }
%>

<%
  String response_format = request.getParameter("format");

  if (response_format != null) {
    /* Eventually, the HTML output should also be driven off of these *Response
     * objects. 
     * 
     * Someday. 
     */
    JobTasksResponse theJobTasksResponse = null;
    ErrorResponse theErrorResponse = null;

    final JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
    String trackerName = 
             StringUtils.simpleHostname(tracker.getJobTrackerMachine());
    String jobid = request.getParameter("jobid");

    if (jobid != null) {
      final JobID jobidObj = JobID.forName(jobid);

      JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobidObj,
          request, response);

      /* Proceed only if the user is authorized to view this job. */
      if (myJob.isViewJobAllowed()) {
        JobInProgress job = myJob.getJob();

        if (job != null) {
          String type = request.getParameter("type");
          String pagenum = request.getParameter("pagenum");
          String state = request.getParameter("state");
          state = (state!=null) ? state : "all";
          int pnum = Integer.parseInt(pagenum);
          int next_page = pnum+1;
          int numperpage = 2000;
          TaskReport[] reports = null;
          int start_index = (pnum - 1) * numperpage;
          int end_index = start_index + numperpage;
          if ("map".equals(type)) {
            reports = tracker.getMapTaskReports(jobidObj);
          } else if ("reduce".equals(type)) {
            reports = tracker.getReduceTaskReports(jobidObj);
          } else if ("cleanup".equals(type)) {
            reports = tracker.getCleanupTaskReports(jobidObj);
          } else if ("setup".equals(type)) {
            reports = tracker.getSetupTaskReports(jobidObj);
          }

          // Filtering the reports if some filter is specified
          if (!"all".equals(state)) {
            List<TaskReport> filteredReports = new ArrayList<TaskReport>();
            for (int i = 0; i < reports.length; ++i) {
              if (("completed".equals(state) && reports[i].getCurrentStatus() == TIPStatus.COMPLETE) 
                  || ("running".equals(state) && reports[i].getCurrentStatus() == TIPStatus.RUNNING) 
                  || ("killed".equals(state) && reports[i].getCurrentStatus() == TIPStatus.KILLED) 
                  || ("pending".equals(state)  && reports[i].getCurrentStatus() == TIPStatus.PENDING)) {
                filteredReports.add(reports[i]);
              }
            }
            // using filtered reports instead of all the reports
            reports = filteredReports.toArray(new TaskReport[0]);
            filteredReports = null;
          }

          int report_len = reports.length;
          
          if (report_len > start_index) {

            if (end_index > report_len){
              end_index = report_len;
            }

            theJobTasksResponse = new JobTasksResponse
              (jobid, type, state, pnum, start_index, end_index);

            for (int i = start_index ; i < end_index; i++) {
              TaskReport report = reports[i];

              theJobTasksResponse.addJobTaskInfo(
                new JobTasksResponse.JobTaskInfo(
                  report
                )
              );
            }
          } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            theErrorResponse = new ErrorResponse(4102, "No Tasks At Start Index " + start_index);
            }
        } else {
          response.setStatus(HttpServletResponse.SC_NOT_FOUND);
          theErrorResponse = new ErrorResponse(4101, "JobID " + jobid + " Not Found");
        }
      } else {
        /* TODO XXX Try and factor out JSPUtil.setErrorAndForward() for re-use
         * (and to not make it blindly dispatch to job_authorization_error.jsp,
         * which is all HTML. 
         */

        /* TODO XXX Make this return JSON, not HTML. */

        /* Nothing to do here, since JSPUtil.setErrorAndForward() has already been 
         * called from inside JSPUtil.checkAccessAndGetJob().
         */
      }
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      theErrorResponse = new ErrorResponse(4100, "Missing JobID");
    }

    /* ------------ Response generation begins here ------------ */

    /* For now, "json" is the only supported format. 
     *
     * As more formats are supported, this should become a cascading 
     * if-elsif-else block.
     */
    if ("json".equals(response_format)) {

      response.setContentType("application/json");

      ObjectMapper responseObjectMapper = new ObjectMapper();
      /* A lack of an error response implies we have a meaningful 
       * application response? Why not!
       */
      out.println(responseObjectMapper.writeValueAsString
        ((theErrorResponse == null) ? theJobTasksResponse : theErrorResponse));

    } else {
      response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }
  } else {
%>
<%
  // Spit out HTML only in the absence of the "format" query parameter.
  response.setContentType("text/html; charset=UTF-8");

  final JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String jobid = request.getParameter("jobid");
  if (jobid == null) {
    out.println("<h2>Missing 'jobid'!</h2>");
    return;
  }
  final JobID jobidObj = JobID.forName(jobid);

  JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobidObj,
      request, response);
  if (!myJob.isViewJobAllowed()) {
    return; // user is not authorized to view this job
  }

  JobInProgress job = myJob.getJob();

  String type = request.getParameter("type");
  String pagenum = request.getParameter("pagenum");
  String state = request.getParameter("state");
  state = (state!=null) ? state : "all";
  int pnum = Integer.parseInt(pagenum);
  int next_page = pnum+1;
  int numperpage = 2000;
  TaskReport[] reports = null;
  int start_index = (pnum - 1) * numperpage;
  int end_index = start_index + numperpage;
  int report_len = 0;
  if ("map".equals(type)) {
    reports = (job != null) ? tracker.getMapTaskReports(jobidObj) : null;
  } else if ("reduce".equals(type)) {
    reports = (job != null) ? tracker.getReduceTaskReports(jobidObj) : null;
  } else if ("cleanup".equals(type)) {
    reports = (job != null) ? tracker.getCleanupTaskReports(jobidObj) : null;
  } else if ("setup".equals(type)) {
    reports = (job != null) ? tracker.getSetupTaskReports(jobidObj) : null;
  }
%>

<!DOCTYPE html>
<html>
  <head>
    <title>Hadoop <%=type%> task list for <%=jobid%> on <%=trackerName%></title>
    <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  </head>
<body>
<h1>Hadoop <%=type%> task list for 
<a href="jobdetails.jsp?jobid=<%=jobid%>"><%=jobid%></a> on 
<a href="jobtracker.jsp"><%=trackerName%></a></h1>
<%
  if (job == null) {
    out.print("<b>Job " + jobid + " not found.</b><br>\n");
    return;
  }
  // Filtering the reports if some filter is specified
  if (!"all".equals(state)) {
    List<TaskReport> filteredReports = new ArrayList<TaskReport>();
    for (int i = 0; i < reports.length; ++i) {
      if (("completed".equals(state) && reports[i].getCurrentStatus() == TIPStatus.COMPLETE) 
          || ("running".equals(state) && reports[i].getCurrentStatus() == TIPStatus.RUNNING) 
          || ("killed".equals(state) && reports[i].getCurrentStatus() == TIPStatus.KILLED) 
          || ("pending".equals(state)  && reports[i].getCurrentStatus() == TIPStatus.PENDING)) {
        filteredReports.add(reports[i]);
      }
    }
    // using filtered reports instead of all the reports
    reports = filteredReports.toArray(new TaskReport[0]);
    filteredReports = null;
  }
  report_len = reports.length;
  
  if (report_len <= start_index) {
    out.print("<b>No such tasks</b>");
  } else {
    out.print("<hr>");
    out.print("<h2>" + Character.toUpperCase(state.charAt(0)) 
              + state.substring(1).toLowerCase() + " Tasks</h2>");
    out.print("<center>");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><td align=\"center\">Task</td><td>Complete</td><td>Status</td>" +
              "<td>Start Time</td><td>Finish Time</td><td>Errors</td><td>Counters</td></tr>");
    if (end_index > report_len){
        end_index = report_len;
    }
    for (int i = start_index ; i < end_index; i++) {
          TaskReport report = reports[i];
          out.print("<tr><td><a href=\"taskdetails.jsp?tipid=" +
            report.getTaskID() + "\">"  + report.getTaskID() + "</a></td>");
         out.print("<td>" + StringUtils.formatPercent(report.getProgress(),2) +
        		   ServletUtil.percentageGraph(report.getProgress() * 100f, 80) + "</td>");
         out.print("<td>"  + HtmlQuoting.quoteHtmlChars(report.getState()) + "<br/></td>");
         out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, report.getStartTime(),0) + "<br/></td>");
         out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
             report.getFinishTime(), report.getStartTime()) + "<br/></td>");
         String[] diagnostics = report.getDiagnostics();
         out.print("<td><pre>");
         for (int j = 0; j < diagnostics.length ; j++) {
             out.println(HtmlQuoting.quoteHtmlChars(diagnostics[j]));
         }
         out.println("</pre><br/></td>");
         out.println("<td>" + 
             "<a href=\"taskstats.jsp?tipid=" + report.getTaskID() +
             "\">" + report.getCounters().size() +
             "</a></td></tr>");
    }
    out.print("</table>");
    out.print("</center>");
  }
  if (end_index < report_len) {
    out.print("<div style=\"text-align:right\">" + 
              "<a href=\"jobtasks.jsp?jobid="+ jobid + "&type=" + type +
              "&pagenum=" + next_page + "&state=" + state +
              "\">" + "Next" + "</a></div>");
  }
  if (start_index != 0) {
      out.print("<div style=\"text-align:right\">" + 
                "<a href=\"jobtasks.jsp?jobid="+ jobid + "&type=" + type +
                "&pagenum=" + (pnum -1) + "&state=" + state + "\">" + "Prev" + "</a></div>");
  }
%>

<hr>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<%
out.println(ServletUtil.htmlFooter());
%>
<%
} // if (response_format != null) 
%>
