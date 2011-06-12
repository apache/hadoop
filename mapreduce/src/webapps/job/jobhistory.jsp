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
  import="java.io.*"
  import="java.util.*"
  import="java.util.regex.Matcher"
  import="java.util.regex.Pattern"
  import="java.net.URLEncoder"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.fs.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapreduce.jobhistory.*"
  import="org.apache.hadoop.mapreduce.jobhistory.JobHistory.JobHistoryJobRecord"
  import="org.apache.hadoop.mapreduce.jobhistory.JobHistory.JobHistoryRecordRetriever"
%>

<%!	private static final long serialVersionUID = 1L;
%>

<%	
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
%>
<%!	
  private static SimpleDateFormat dateFormat = 
                                    new SimpleDateFormat("d/MM HH:mm:ss");
%>
<html>
<head>
<script type="text/JavaScript">
<!--
<% // assuming search is already quoted %>
function showUserHistory(search)
{
var url
if (search == null || "".equals(search)) {
  url="jobhistory.jsp";
} else {
  url="jobhistory.jsp?pageno=1&search=" + search;
}
window.location.href = url;
}
//-->
</script>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title><%= trackerName %> Hadoop Map/Reduce History Viewer</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>
<h1> <a href="jobtracker.jsp"><%= trackerName %></a> Hadoop Map/Reduce 
     <a href="jobhistory.jsp">History Viewer</a></h1>
<hr>
<%
  //{{ // this is here to make indentation work, and must be commented out
    final String search = (request.getParameter("search") == null)
                          ? ""
                          : request.getParameter("search");

    String soughtDate = "";
    String soughtJobName = "";
    String soughtJobid = "";

    // soughtUser : jobid ; jobname ! date

    String splitDate[] = search.split("!");

    final String DATE_PATTERN = "([0-1]?[0-9])/([0-3]?[0-9])/((?:2[0-9])?[0-9][0-9])";

    if (splitDate.length >= 2) {
      soughtDate = splitDate[1];
    }

    String[] splitJobName = splitDate[0].split(";");

    if (splitJobName.length >= 2) {
      soughtJobName = splitJobName[1];
    }

    String[] splitJobid = splitJobName[0].split(":");

    if (splitJobid.length >= 2) {
      soughtJobid = splitJobid[1];
    }

    final String soughtUser = (splitJobid.length >= 1)
                        ? splitJobid[0].toLowerCase()
                        : "";

    if (soughtDate.length() != 0) {
      Pattern p = Pattern.compile(DATE_PATTERN);
      Matcher m = p.matcher(soughtDate);
      if (!m.matches()) {
        soughtDate = "";
      }
    }

    JobHistory jobHistory = (JobHistory) application.getAttribute("jobHistoryHistory");
    String soughtDates[] = new String[1];
    soughtDates[0] = soughtDate;

    JobHistoryRecordRetriever retriever
      = jobHistory.getMatchingJobs
           (soughtUser, soughtJobName, soughtDates, soughtJobid);
    
    JobHistoryJobRecord[] records
      = new JobHistoryJobRecord[retriever.numberMatches()];

    int recordsIndex = 0;

    while (retriever.hasNext()) {
      records[recordsIndex++] = retriever.next();
    }
    
    out.println("<!--  user : " + soughtUser + ", jobid : " + soughtJobid + "-->");
    if (records.length == 0)  {
      out.println("No files found!"); 
      return ; 
    }

    // get the pageno
    int pageno = request.getParameter("pageno") == null
                ? 1
                : Integer.parseInt(request.getParameter("pageno"));

    // get the total number of files to display
    int size = 100;

    // if show-all is requested or jobfiles < size(100)
    if (pageno == -1 || size > records.length) {
      size = records.length;
    }

    if (pageno == -1) { // special case 'show all'
      pageno = 1;
    }

    int maxPageNo = (records.length + size - 1) / size;

    // check and fix pageno
    if (pageno < 1 || pageno > maxPageNo) {
      out.println("Invalid page index");
      return ;
    }

    int length = size ; // determine the length of job history files to be displayed
    if (pageno == maxPageNo) {
      // find the number of files to be shown on the last page
      int startOnLast = ((pageno - 1) * size) + 1;
      length = records.length - startOnLast + 1;
    }

    // Display the search box
    out.println("<form name=search><b> Filter ([username][:jobid][;jobname-key][!MM/DD/YYYY]) </b>"); // heading
    out.println("<input type=text name=search size=\"20\" value=\"" + search + "\">"); // search box
    out.println("<input type=submit value=\"Filter!\" onClick=\"showUserHistory(document.getElementById('search').value)\"></form>");
    out.println("<span class=\"small\">Example: <b>smith</b> will display jobs submitted by user 'smith'. </span>");
    out.println("<span class=\"small\">Job Ids need to be prefixed with a colon(:) For example, <b>:job_200908311030_0001</b> will display the job with that id.  You may search for parts of job names.  Job name search keys need to be prefixed by a semicolon (;).  A filter <b>;budget</b> will find jobs named \"budget calculation\" or \"fussbudget job\".  You may restrict results to jobs that finished on a specific day.  Date criteria are <b>MM/DD/YYYYY</b> and are prefixed with an exclamation point (!).  You may specify multiple criteria.  We only display jobs that satisfy all criteria.</span>"); // example 
    out.println("<hr>");

    //Show the status
    int start = (pageno - 1) * size + 1;

    // DEBUG
    out.println("<!-- pageno : " + pageno + ", size : " + size + ", length : " + length + ", start : " + start + ", maxpg : " + maxPageNo + "-->");

    out.println("<font size=5><b>Available Jobs in History </b></font>");
    // display the number of jobs, start index, end index
    out.println("(<i> <span class=\"small\">Displaying <b>" + length + "</b> jobs from <b>" + start + "</b> to <b>" + (start + length - 1) + "</b> out of <b>" + records.length + "</b> jobs");
    if (!"".equals(soughtUser)) {
      out.println(" for user <b>" + soughtUser + "</b>"); // show the user if present
    }
    if (!"".equals(soughtJobid)) {
      out.println(" for jobid <b>" + soughtJobid + "</b> in it "); // show the jobid keyword if present
    }
    if (!"".equals(soughtDate)) {
      out.println(" for date <b>" + soughtDate + "</b>"); // show the jobid keyword if present
    }
    out.print("</span></i>)");

    // show the 'show-all' link
    out.println(" [<span class=\"small\"><a href=\"jobhistory.jsp?pageno=-1&search=" + search + "\">show all</a></span>]");

    // show the 'first-page' link
    if (pageno > 1) {
      out.println(" [<span class=\"small\"><a href=\"jobhistory.jsp?pageno=1&search=" + search + "\">first page</a></span>]");
    } else {
      out.println("[<span class=\"small\">first page]</span>");
    }

    // show the 'last-page' link
    if (pageno < maxPageNo) {
      out.println(" [<span class=\"small\"><a href=\"jobhistory.jsp?pageno=" + maxPageNo + "&search=" + search + "\">last page</a></span>]");
    } else {
      out.println("<span class=\"small\">[last page]</span>");
    }

    // REVERSE sort the files on creation time.
    Arrays.sort(records, new Comparator<JobHistoryJobRecord>() {
                  public int compare(JobHistoryJobRecord rec1, JobHistoryJobRecord rec2) {
                    String id1 = rec1.getJobIDString(true);
                    String id2 = rec2.getJobIDString(true);

                    String[] idsplit1 = id1.split("_");
                    String[] idsplit2 = id2.split("_");
        
                    // compare job tracker start time
                    Long jtTime2 = Long.parseLong(idsplit2[1]);
                    // comparison sense is reversed
                    int res = jtTime2.compareTo(Long.parseLong(idsplit1[1]));
                    if (res == 0) {
                      // comparison sense is reversed
                      Long sn2 = Long.parseLong(idsplit2[2]);
                      res = sn2.compareTo(Long.parseLong(idsplit1[2]));
                    }
                    return res;
                  }
                });

    out.println("<br><br>");

    // print the navigation info (top)
    printNavigation(pageno, size, maxPageNo, search, out);

    out.print("<table align=center border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr>");
    out.print( "<td>Job submit time</td>");
    out.print("<td>Job Id</td>");
    out.print("<td>User</td>") ; 
    out.print("<td>Job Name</td>") ; 
    out.print("</tr>"); 
    
    Set<String> displayedJobs = new HashSet<String>();
    for (int i = start - 1; i < start + length - 1; ++i) {
      JobHistoryJobRecord record = records[i];

      String jobId = record.getJobIDString();
      String userName = record.getUserName();
      long submitTime = record.getSubmitTime();
      String jobName = record.getJobName();
      Path logPath = record.getPath();

      // Check if the job is already displayed. There can be multiple job 
      // history files for jobs that have restarted
      if (displayedJobs.contains(jobId)) {
        continue;
      } else {
        displayedJobs.add(jobId);
      }
      
%>
<center>
<%	
      printJob(submitTime, jobId, userName, logPath, jobName, out) ; 
%>
</center> 
<%
    } // end while trackers 
    out.print("</table>");

    // show the navigation info (bottom)
    printNavigation(pageno, size, maxPageNo, search, out);
%>
<%!
    private void printJob(long timestamp, String jobId, 
                          String user, Path logFile, String jobName, JspWriter out)
    throws IOException {
      out.print("<tr>"); 
      out.print("<td>" + new Date(timestamp) + "</td>"); 
      out.print("<td>" + "<a href=\"jobdetailshistory.jsp?logFile=" +
       URLEncoder.encode(logFile.toString(), "UTF-8") +
                "\">" + jobId + "</a></td>");
      out.print("<td>" + user + "</td>");
      out.print("<td>" + jobName + "</td>");
      out.print("</tr>");
    }


    // I tolerate this code because I expect a low number of
    // occurrences in a relatively short string
    private static String replaceStringInstances
      (String replacee, String old, String replacement) {
      int index = replacee.indexOf(old);

      while (index > 0) {
        replacee = (replacee.substring(0, index)
                       + replacement
                       + replaceStringInstances
                           (replacee.substring(index + old.length()),
                            old, replacement));

        index = replacee.indexOf(old);
      }

      return replacee;
    }   

    private void printNavigation(int pageno, int size, int max, String search, 
                                 JspWriter out) throws IOException {
      int numIndexToShow = 5; // num indexes to show on either side

      //TODO check this on boundary cases
      out.print("<center> <");

      // show previous link
      if (pageno > 1) {
        out.println("<a href=\"jobhistory.jsp?pageno=" + (pageno - 1) +
            "&search=" + search + "\">Previous</a>");
      }

      // display the numbered index 1 2 3 4
      int firstPage = pageno - numIndexToShow;
      if (firstPage < 1) {
        firstPage = 1; // boundary condition
      }

      int lastPage = pageno + numIndexToShow;
      if (lastPage > max) {
        lastPage = max; // boundary condition
      }

      // debug
      out.println("<!--DEBUG : firstPage : " + firstPage + ", lastPage : " + lastPage + " -->");

      for (int i = firstPage; i <= lastPage; ++i) {
        if (i != pageno) {// needs hyperlink
          out.println(" <a href=\"jobhistory.jsp?pageno=" + i + "&search=" +
              search + "\">" + i + "</a> ");
        } else { // current page
          out.println(i);
        }
      }

      // show the next link
      if (pageno < max) {
        out.println("<a href=\"jobhistory.jsp?pageno=" + (pageno + 1) + "&search=" + search + "\">Next</a>");
      }
      out.print("></center>");
    }
%> 
</body></html>
