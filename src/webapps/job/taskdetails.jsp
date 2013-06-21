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
  import="java.lang.String"
  import="java.util.*"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.security.UserGroupInformation"
  import="java.security.PrivilegedExceptionAction"
  import="org.apache.hadoop.security.AccessControlException"
  import="org.codehaus.jackson.map.ObjectMapper"
%>
<%!static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d-MMM-yyyy HH:mm:ss");
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%!private void printConfirm(JspWriter out,
      String attemptid, String action) throws IOException {
    String url = "taskdetails.jsp?attemptid=" + attemptid;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url + "\"></head>" + "<body><h3> Are you sure you want to kill/fail "
        + attemptid + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"" + action + "\" />"
        + "<input type=\"submit\" name=\"Kill/Fail\" value=\"Kill/Fail\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
  }
%>

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

  public static class TaskDetailsResponse {

    public static class TaskAttemptInfo {

      public static class TaskAttemptTrackerInfo {

        private final String name;
        private final String host;
        private final int httpPort;
        private final String url;
        private final String node;

        // Constructor
        TaskAttemptTrackerInfo(String trackerName, JobTracker jobTracker) {

          name = trackerName;

          TaskTrackerStatus trackerStatus = jobTracker.getTaskTrackerStatus(name);
          
          if (trackerStatus != null) {

            host = trackerStatus.getHost();
            httpPort = trackerStatus.getHttpPort();
            url = "http://" + host + ":" + httpPort;
            node = jobTracker.getNode(host).toString();

          } else {

            host = null;
            httpPort = 0;
            url = null;
            node = null;
          }
        }

        // Getters
        public String getName() { return name; }
        public String getHost() { return host; }
        public int getHttpPort() { return httpPort; }
        public String getUrl() { return url; }
        public String getNode() { return node; }
      }

      public static class EventTimingInfo {

        private final String timestamp;
        private final long durationSecs;

        // Constructor
        EventTimingInfo(long eventOccurrenceTimeMSecs, long previousEventOccurrenceTimeMSecs) {

          timestamp = dateFormat.format(new Date(eventOccurrenceTimeMSecs));
          durationSecs = (0 != previousEventOccurrenceTimeMSecs) ? 
            /* Pass the difference through Math.abs() to take care of cases 
             * where previousEventOccurrenceTimeMSecs is in the future (likely
             * used only as a hack, when an event is in-progress).
             */
            (Math.abs(eventOccurrenceTimeMSecs - previousEventOccurrenceTimeMSecs)/1000) : 
            0;
        }

        // Getters
        public String getTimestamp() { return timestamp; }
        public long getDurationSecs() { return durationSecs; }
      }

      private final String attemptId;
      private final TaskAttemptTrackerInfo taskAttemptTrackerInfo;
      private final TaskAttemptTrackerInfo cleanupAttemptTrackerInfo;
      private final String status;
      private final float progressPercentage;
      private final EventTimingInfo startTimingInfo;
      private final EventTimingInfo finishTimingInfo;
      private final EventTimingInfo shuffleTimingInfo;
      private final EventTimingInfo sortTimingInfo;
      private final String taskAttemptLogUrl;
      private final String cleanupAttemptLogUrl;

      private String generateAttemptLogUrl(String trackerName, JobTracker jobTracker, boolean isCleanupAttempt) {

        String attemptLogUrl = null;

        TaskTrackerStatus trackerStatus = jobTracker.getTaskTrackerStatus(trackerName);

        if (trackerStatus != null) {

          attemptLogUrl = TaskLogServlet.getTaskLogUrl(trackerStatus.getHost(), 
            String.valueOf(trackerStatus.getHttpPort()), attemptId);

          if (attemptLogUrl != null) {
            attemptLogUrl +=  "&all=true";

            if (isCleanupAttempt) {
              attemptLogUrl += "&cleanup=true";
            }
          }
        }

        return attemptLogUrl;
      }

      // Constructor
      TaskAttemptInfo(String ai, String ttn, String ctn, TaskStatus ts, 
        JobTracker jt, boolean isCleanupOrSetup) {

        attemptId = ai;

        taskAttemptTrackerInfo = new TaskAttemptTrackerInfo(ttn, jt);
        cleanupAttemptTrackerInfo = (ctn != null) ? 
          new TaskAttemptTrackerInfo(ctn, jt) : null;

        status = ts.getRunState().toString();
        progressPercentage = ts.getProgress() * 100.0f;

        startTimingInfo = new EventTimingInfo
          (ts.getStartTime(), 0);
        finishTimingInfo = new EventTimingInfo
          (ts.getFinishTime(), ts.getStartTime());

        if (!ts.getIsMap() && !isCleanupOrSetup) {

          shuffleTimingInfo = new EventTimingInfo
            (ts.getShuffleFinishTime(), ts.getStartTime());
          sortTimingInfo = new EventTimingInfo
            (ts.getSortFinishTime(), ts.getShuffleFinishTime());

        } else {

          shuffleTimingInfo = null;
          sortTimingInfo = null;
        }

        taskAttemptLogUrl = generateAttemptLogUrl(ttn, jt, false);
        cleanupAttemptLogUrl = (ctn != null) ? 
          generateAttemptLogUrl(ctn, jt, true) : null;
      }

      // Getters
      public String getAttemptId() { return attemptId; }
      public TaskAttemptTrackerInfo getTaskAttemptTrackerInfo() { return taskAttemptTrackerInfo; }
      public TaskAttemptTrackerInfo getCleanupAttemptTrackerInfo() { return cleanupAttemptTrackerInfo; }
      public String getStatus() { return status; }
      public float getProgressPercentage() { return progressPercentage; }
      public EventTimingInfo getStartTimingInfo() { return startTimingInfo; }
      public EventTimingInfo getFinishTimingInfo() { return finishTimingInfo; }
      public EventTimingInfo getShuffleTimingInfo() { return shuffleTimingInfo; }
      public EventTimingInfo getSortTimingInfo() { return sortTimingInfo; }
      public String getTaskAttemptLogUrl() { return taskAttemptLogUrl; }
      public String getCleanupAttemptLogUrl() { return cleanupAttemptLogUrl; }
    }

    private final String jobId;
    private final String taskId;
    private final Collection<TaskAttemptInfo> taskAttemptsInfo;
    private final Collection<String> inputSplitLocationsInfo;

    // Constructor
    TaskDetailsResponse(String ji, String ti) {

      jobId = ji;
      taskId = ti;

      taskAttemptsInfo = new ArrayList<TaskAttemptInfo>();
      inputSplitLocationsInfo = new ArrayList<String>();
    }

    // To add one TaskAttemptInfo object at a time.
    void addTaskAttemptInfo(TaskAttemptInfo tai) { taskAttemptsInfo.add(tai); }

    // To add one Input Split Location at a time.
    void addInputSplitLocation(String isl) { inputSplitLocationsInfo.add(isl); }

    // Getters
    public String getJobId() { return jobId; }
    public String getTaskId() { return taskId; }
    public Collection<TaskAttemptInfo> getTaskAttemptsInfo() { return taskAttemptsInfo; }
    public Collection<String> getInputSplitLocationsInfo() { return inputSplitLocationsInfo; }
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
    TaskDetailsResponse theTaskDetailsResponse = null;
    ErrorResponse theErrorResponse = null;

    JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
    String attemptid = request.getParameter("attemptid");
    String tipid = request.getParameter("tipid");

    /* Make sure at least one of the 2 possible query parameters were specified. */
    if ((attemptid != null) || (tipid != null)) {

      TaskAttemptID attemptidObj = TaskAttemptID.forName(attemptid);

      // Obtain tipid for attemptId, if attemptId is available.
      TaskID tipidObj =
          (attemptidObj == null) ? TaskID.forName(tipid)
                                 : attemptidObj.getTaskID();
      if (tipidObj != null) {

        // Obtain jobid from tipid
        final JobID jobidObj = tipidObj.getJobID();
        String jobid = jobidObj.toString();
        
        JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobidObj,
            request, response);

        /* Proceed only if the user is authorized to view this job. */
        if (myJob.isViewJobAllowed()) {

          JobInProgress job = myJob.getJob();

          if (job != null) {

            TaskInProgress tip = job.getTaskInProgress(tipidObj);

            if (tip != null) { 

              TaskStatus[] ts = tip.getTaskStatuses();

              if ((ts != null) && (ts.length > 0)) {

                boolean isCleanupOrSetup = (tip.isJobCleanupTask() || tip.isJobSetupTask());

                theTaskDetailsResponse = 
                  new TaskDetailsResponse(jobid, tipidObj.toString());

                /* Generate Task Attempts. */
                for (int i = 0; i < ts.length; i++) {

                  TaskStatus status = ts[i];

                  TaskAttemptID taskAttemptId = status.getTaskID();

                  String taskTrackerName = status.getTaskTracker();
                  String cleanupTrackerName = null;

                  if (tip.isCleanupAttempt(taskAttemptId)) {
                    cleanupTrackerName = tip.machineWhereCleanupRan(taskAttemptId);
                  }

                  theTaskDetailsResponse.addTaskAttemptInfo(
                    new TaskDetailsResponse.TaskAttemptInfo(
                      taskAttemptId.toString(),
                      taskTrackerName,
                      cleanupTrackerName,
                      status, 
                      tracker,
                      isCleanupOrSetup
                    )
                  );
                }

                /* Generate Input Split Locations. */
                if (ts[0].getIsMap() && !isCleanupOrSetup) {
                  for (String splitLocation: StringUtils.split(tracker.getTip(tipidObj).
                        getSplitNodes())) {
                    theTaskDetailsResponse.addInputSplitLocation(splitLocation); 
                  }
                }
              } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                theErrorResponse = new ErrorResponse(4103, "TaskAttempts For " + tipidObj.toString() + " Not Found");
              }
            }
          } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            theErrorResponse = new ErrorResponse(4102, "JobID " + jobid + " Not Found");
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
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        theErrorResponse = new ErrorResponse(4101, 
          ((attemptid == null) ? ("TipID " + tipid) : ("AttemptID " + attemptid)) + " Invalid");
      }
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      theErrorResponse = new ErrorResponse(4100, "Missing TipID/AttemptID");
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
        ((theErrorResponse == null) ? theTaskDetailsResponse : theErrorResponse));

    } else {
      response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }
  } else {
%>
<%
    // Spit out HTML only in the absence of the "format" query parameter.
    response.setContentType("text/html; charset=UTF-8");

    final JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");

    String attemptid = request.getParameter("attemptid");
    final TaskAttemptID attemptidObj = TaskAttemptID.forName(attemptid);

    // Obtain tipid for attemptid, if attemptid is available.
    TaskID tipidObj =
        (attemptidObj == null) ? TaskID.forName(request.getParameter("tipid"))
                               : attemptidObj.getTaskID();
    if (tipidObj == null) {
      out.print("<b>tipid sent is not valid.</b><br>\n");
      return;
    }
    // Obtain jobid from tipid
    final JobID jobidObj = tipidObj.getJobID();
    String jobid = jobidObj.toString();

    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobidObj,
        request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();
    if (job == null) {
      out.print("<b>Job " + jobid + " not found.</b><br>\n");
      return;
    }
    boolean privateActions = JSPUtil.privateActionsAllowed(tracker.conf);
    if (privateActions) {
      String action = request.getParameter("action");
      if (action != null) {
        String user = request.getRemoteUser();
        UserGroupInformation ugi = null;
        if (user != null) {
          ugi = UserGroupInformation.createRemoteUser(user);
        }
        if (action.equalsIgnoreCase("confirm")) {
          String subAction = request.getParameter("subaction");
          if (subAction == null)
            subAction = "fail-task";
          printConfirm(out, attemptid, subAction);
          return;
        }
        else if (action.equalsIgnoreCase("kill-task") 
            && request.getMethod().equalsIgnoreCase("POST")) {
          if (ugi != null) {
            try {
              ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                tracker.killTask(attemptidObj, false);// checks job modify permission
                return null;
              }
              });
            } catch(AccessControlException e) {
              String errMsg = "User " + user + " failed to kill task "
                  + attemptidObj + "!<br><br>" + e.getMessage() +
                  "<hr><a href=\"jobdetails.jsp?jobid=" + jobid +
                  "\">Go back to Job</a><br>";
              JSPUtil.setErrorAndForward(errMsg, request, response);
              return;
            }
          } else {// no authorization needed
            tracker.killTask(attemptidObj, false);
          }
          //redirect again so that refreshing the page will not attempt to rekill the task
          response.sendRedirect("/taskdetails.jsp?subaction=kill-task" +
              "&tipid=" + tipidObj.toString());
        }
        else if (action.equalsIgnoreCase("fail-task")
            && request.getMethod().equalsIgnoreCase("POST")) {
          if (ugi != null) {
            try {
              ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                tracker.killTask(attemptidObj, true);// checks job modify permission
                return null;
              }
              });
            } catch(AccessControlException e) {
              String errMsg = "User " + user + " failed to fail task "
                  + attemptidObj + "!<br><br>" + e.getMessage() +
                  "<hr><a href=\"jobdetails.jsp?jobid=" + jobid +
                  "\">Go back to Job</a><br>";
              JSPUtil.setErrorAndForward(errMsg, request, response);
              return;
            }
          } else {// no authorization needed
            tracker.killTask(attemptidObj, true);
          }

          response.sendRedirect("/taskdetails.jsp?subaction=fail-task" +
              "&tipid=" + tipidObj.toString());
        }
      }
    }
    TaskInProgress tip = job.getTaskInProgress(tipidObj);
    TaskStatus[] ts = null;
    boolean isCleanupOrSetup = false;
    if (tip != null) { 
      ts = tip.getTaskStatuses();
      isCleanupOrSetup = tip.isJobCleanupTask();
      if (!isCleanupOrSetup) {
        isCleanupOrSetup = tip.isJobSetupTask();
      }
    }
%>


<!DOCTYPE html>
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  <title>Hadoop Task Details</title>
</head>
<body>
<h1>Job <a href="/jobdetails.jsp?jobid=<%=jobid%>"><%=jobid%></a></h1>

<hr>

<h2>All Task Attempts</h2>
<center>
<%
    if (ts == null || ts.length == 0) {
%>
		<h3>No Task Attempts found</h3>
<%
    } else {
%>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Machine</td><td>Status</td><td>Progress</td><td>Start Time</td> 
  <%
   if (!ts[0].getIsMap() && !isCleanupOrSetup) {
   %>
<td>Shuffle Finished</td><td>Sort Finished</td>
  <%
  }
  %>
<td>Finish Time</td><td>Errors</td><td>Task Logs</td><td>Counters</td><td>Actions</td></tr>
  <%
    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      String taskTrackerName = status.getTaskTracker();
      TaskTrackerStatus taskTracker = tracker.getTaskTrackerStatus(taskTrackerName);
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      String taskAttemptTracker = null;
      String cleanupTrackerName = null;
      TaskTrackerStatus cleanupTracker = null;
      String cleanupAttemptTracker = null;
      boolean hasCleanupAttempt = false;
      if (tip != null && tip.isCleanupAttempt(status.getTaskID())) {
        cleanupTrackerName = tip.machineWhereCleanupRan(status.getTaskID());
        cleanupTracker = tracker.getTaskTrackerStatus(cleanupTrackerName);
        if (cleanupTracker != null) {
          cleanupAttemptTracker = "http://" + cleanupTracker.getHost() + ":"
            + cleanupTracker.getHttpPort();
        }
        hasCleanupAttempt = true;
      }
      out.print("<td>");
      if (hasCleanupAttempt) {
        out.print("Task attempt: ");
      }
      if (taskTracker == null) {
        out.print(taskTrackerName);
      } else {
        taskAttemptTracker = "http://" + taskTracker.getHost() + ":"
          + taskTracker.getHttpPort();
        out.print("<a href=\"" + taskAttemptTracker + "\">"
          + tracker.getNode(taskTracker.getHost()) + "</a>");
      }
      if (hasCleanupAttempt) {
        out.print("<br/>Cleanup Attempt: ");
        if (cleanupAttemptTracker == null ) {
          out.print(cleanupTrackerName);
        } else {
          out.print("<a href=\"" + cleanupAttemptTracker + "\">"
            + tracker.getNode(cleanupTracker.getHost()) + "</a>");
        }
      }
      out.print("</td>");
        out.print("<td>" + status.getRunState() + "</td>");
        out.print("<td>" + StringUtils.formatPercent(status.getProgress(), 2)
          + ServletUtil.percentageGraph(status.getProgress() * 100f, 80) + "</td>");
        out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getStartTime(), 0) + "</td>");
        if (!ts[i].getIsMap() && !isCleanupOrSetup) {
          out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getShuffleFinishTime(), status.getStartTime()) + "</td>");
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getSortFinishTime(), status.getShuffleFinishTime())
          + "</td>");
        }
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getFinishTime(), status.getStartTime()) + "</td>");

        out.print("<td><pre>");
        String [] failures = tracker.getTaskDiagnostics(status.getTaskID());
        if (failures == null) {
          out.print("&nbsp;");
        } else {
          for(int j = 0 ; j < failures.length ; j++){
            out.print(HtmlQuoting.quoteHtmlChars(failures[j]));
            if (j < (failures.length - 1)) {
              out.print("\n-------\n");
            }
          }
        }
        out.print("</pre></td>");
        out.print("<td>");
        String taskLogUrl = null;
        if (taskTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(taskTracker.getHost(),
        						String.valueOf(taskTracker.getHttpPort()),
        						status.getTaskID().toString());
      	}
        if (hasCleanupAttempt) {
          out.print("Task attempt: <br/>");
        }
        if (taskLogUrl == null) {
          out.print("n/a");
        } else {
          String tailFourKBUrl = taskLogUrl + "&start=-4097";
          String tailEightKBUrl = taskLogUrl + "&start=-8193";
          String entireLogUrl = taskLogUrl + "&all=true";
          out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
          out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
          out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
        }
        if (hasCleanupAttempt) {
          out.print("Cleanup attempt: <br/>");
          taskLogUrl = null;
          if (cleanupTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(cleanupTracker.getHost(),
                                String.valueOf(cleanupTracker.getHttpPort()),
                                status.getTaskID().toString());
      	  }
          if (taskLogUrl == null) {
            out.print("n/a");
          } else {
            String tailFourKBUrl = taskLogUrl + "&start=-4097&cleanup=true";
            String tailEightKBUrl = taskLogUrl + "&start=-8193&cleanup=true";
            String entireLogUrl = taskLogUrl + "&all=true&cleanup=true";
            out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
            out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
            out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
          }
        }
        out.print("</td><td>" + "<a href=\"/taskstats.jsp?attemptid=" +
          status.getTaskID() + "\">"
          + ((status.getCounters() != null) ? status.getCounters().size() : 0)
          + "</a></td>");
        out.print("<td>");
        if (privateActions
          && status.getRunState() == TaskStatus.State.RUNNING) {
        out.print("<a href=\"/taskdetails.jsp?action=confirm"
          + "&subaction=kill-task" + "&attemptid=" + status.getTaskID()
          + "\" > Kill </a>");
        out.print("<br><a href=\"/taskdetails.jsp?action=confirm"
          + "&subaction=fail-task" + "&attemptid=" + status.getTaskID()
          + "\" > Fail </a>");
        }
        else
          out.print("<pre>&nbsp;</pre>");
        out.println("</td></tr>");
      }
  %>
</table>
</center>

<%
      if (ts[0].getIsMap() && !isCleanupOrSetup) {
%>
<h3>Input Split Locations</h3>
<table border=2 cellpadding="5" cellspacing="2">
<%
        for (String split: StringUtils.split(tracker.getTip(
                                         tipidObj).getSplitNodes())) {
          out.println("<tr><td>" + split + "</td></tr>");
        }
%>
</table>
<%    
      }
    }
%>

<hr>
<a href="jobdetails.jsp?jobid=<%=jobid%>">Go back to the job</a><br>
<a href="jobtracker.jsp">Go back to JobTracker</a><br>
<%
out.println(ServletUtil.htmlFooter());
%>
<%
} // if (response_format != null) 
%>
