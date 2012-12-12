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
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String type = request.getParameter("type");
%>
<%!
  public void generateTaskTrackerTable(JspWriter out,
                                       String type,
                                       JobTracker tracker) throws IOException {
    Collection c;
    if (("blacklisted").equals(type)) {
      out.println("<h2>Blacklisted Task Trackers</h2>");
      c = tracker.blacklistedTaskTrackers();
    } else if (("graylisted").equals(type)) {
      out.println("<h2>Graylisted Task Trackers</h2>");
      c = tracker.graylistedTaskTrackers();
    } else if (("active").equals(type)) {
      out.println("<h2>Active Task Trackers</h2>");
      c = tracker.activeTaskTrackers();
    } else {
      out.println("<h2>Task Trackers</h2>");
      c = tracker.taskTrackers();
    }
    int noCols = 9 + 
      (2 * tracker.getStatistics().collector.DEFAULT_COLLECT_WINDOWS.length);
    if (type.equals("blacklisted") || type.equals("graylisted")) {
      noCols = noCols + 1;
    }
    if (c.size() == 0) {
      out.print("There are currently no known " + type + " Task Trackers.");
    } else {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\" colspan=\""+ noCols +"\"><b>Task Trackers</b></td></tr>\n");
      out.print("<tr><td><b>Name</b></td><td><b>Host</b></td>" +
                "<td><b># running tasks</b></td>" +
                "<td><b>Max Map Tasks</b></td>" +
                "<td><b>Max Reduce Tasks</b></td>" +
                "<td><b>Task Failures</b></td>" +
                "<td><b>Directory Failures</b></td>" +
                "<td><b>Node Health Status</b></td>" +
                "<td><b>Seconds Since Node Last Healthy</b></td>");
      if (type.equals("blacklisted")) {
        out.print("<td><b>Reason for Blacklisting</b></td>");
      } else if (type.equals("graylisted")) {
        out.print("<td><b>Reason for Graylisting</b></td>");
      }
      for (StatisticsCollector.TimeWindow window : tracker.getStatistics().
           collector.DEFAULT_COLLECT_WINDOWS) {
         out.println("<td><b>Total Tasks "+window.name+"</b></td>");
         out.println("<td><b>Succeeded Tasks "+window.name+"</b></td>");
       }
      
      out.print("<td><b>Seconds since heartbeat</b></td></tr>\n");

      int maxFailures = 0;
      String failureKing = null;
      for (Iterator it = c.iterator(); it.hasNext(); ) {
        TaskTrackerStatus tt = (TaskTrackerStatus) it.next();
        long sinceHeartbeat = System.currentTimeMillis() - tt.getLastSeen();
        boolean isHealthy = tt.getHealthStatus().isNodeHealthy();
        long sinceHealthCheck = tt.getHealthStatus().getLastReported();
        String healthString = "";
        if(sinceHealthCheck == 0) {
          healthString = "N/A";
        } else {
          healthString = (isHealthy?"Healthy":"Unhealthy");
          sinceHealthCheck = System.currentTimeMillis() - sinceHealthCheck;
          sinceHealthCheck =  sinceHealthCheck/1000;
        }
        if (sinceHeartbeat > 0) {
          sinceHeartbeat = sinceHeartbeat / 1000;
        }
        int numCurTasks = 0;
        for (Iterator it2 = tt.getTaskReports().iterator(); it2.hasNext(); ) {
          it2.next();
          numCurTasks++;
        }
        int numTaskFailures = tt.getTaskFailures();
        if (numTaskFailures > maxFailures) {
          maxFailures = numTaskFailures;
          failureKing = tt.getTrackerName();
        }
        int numDirFailures = tt.getDirFailures();
        out.print("<tr><td><a href=\"http://");
        out.print(tt.getHost() + ":" + tt.getHttpPort() + "/\">");
        out.print(tt.getTrackerName() + "</a></td><td>");
        out.print(tt.getHost() + "</td><td>" + numCurTasks +
                  "</td><td>" + tt.getMaxMapSlots() +
                  "</td><td>" + tt.getMaxReduceSlots() + 
                  "</td><td>" + numTaskFailures +
                  "</td><td>" + numDirFailures +
                  "</td><td>" + healthString +
                  "</td><td>" + sinceHealthCheck); 
        if (type.equals("blacklisted")) {
          out.print("</td><td>" + tracker.getReasonsForBlacklisting(tt.getHost()));
        } else if (type.equals("graylisted")) {
          out.print("</td><td>" + tracker.getReasonsForGraylisting(tt.getHost()));
        }
        for (StatisticsCollector.TimeWindow window : tracker.getStatistics().
          collector.DEFAULT_COLLECT_WINDOWS) {
          JobTrackerStatistics.TaskTrackerStat ttStat = tracker.getStatistics().
             getTaskTrackerStat(tt.getTrackerName());
          out.println("</td><td>" + ttStat.totalTasksStat.getValues().
                                get(window).getValue());
          out.println("</td><td>" + ttStat.succeededTasksStat.getValues().
                                get(window).getValue());
        }
        
        out.print("</td><td>" + sinceHeartbeat + "</td></tr>\n");
      }
      out.print("</table>\n");
      out.print("</center>\n");
      if (maxFailures > 0) {
        out.print("Highest Failures: " + failureKing + " with " + maxFailures + 
                  " failures<br>\n");
      }
    }
  }

  public void generateTableForExcludedNodes(JspWriter out, JobTracker tracker) 
  throws IOException {
    // excluded nodes
    out.println("<h2>Excluded Nodes</h2>");
    Collection<String> d = tracker.getExcludedNodes();
    if (d.size() == 0) {
      out.print("There are currently no excluded hosts.");
    } else { 
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr>");
      out.print("<td><b>Host Name</b></td></tr>\n");
      for (Iterator it = d.iterator(); it.hasNext(); ) {
        String dt = (String)it.next();
        out.print("<td>" + dt + "</td></tr>\n");
      }
      out.print("</table>\n");
      out.print("</center>\n");
    }
  }
%>

<!DOCTYPE html>
<html>

<title><%=trackerName%> Hadoop Machine List</title>

<body>
<h1><a href="jobtracker.jsp"><%=trackerName%></a> Hadoop Machine List</h1>

<%
  if (("excluded").equals(type)) {
    generateTableForExcludedNodes(out, tracker);
  } else {
    generateTaskTrackerTable(out, type, tracker);
  }
%>

<%
out.println(ServletUtil.htmlFooter());
%>
