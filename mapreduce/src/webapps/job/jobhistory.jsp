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
  import="java.net.URLEncoder"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.fs.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapreduce.jobhistory.*"
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
    final String search = (request.getParameter("search") == null)
                          ? ""
                          : request.getParameter("search");

    String parts[] = search.split(":");

    final String user = (parts.length >= 1)
                        ? parts[0].toLowerCase()
                        : "";
    final String jobid = (parts.length >= 2)
                           ? parts[1].toLowerCase()
                           : "";
    final String rawUser = HtmlQuoting.unquoteHtmlChars(user);
    final String rawJobid = HtmlQuoting.unquoteHtmlChars(jobid);

    PathFilter jobLogFileFilter = new PathFilter() {
      private boolean matchUser(String fileName) {
        // return true if 
        //  - user is not specified
        //  - user matches
        return "".equals(rawUser) || rawUser.equals(fileName.split("_")[3]);
      }

      private boolean matchJobId(String fileName) {
        // return true if 
        //  - jobid is not specified
        //  - jobid matches 
        String[] jobDetails = fileName.split("_");
        String actualId = jobDetails[0] + "_" +jobDetails[1] + "_" + jobDetails[2] ;
        return "".equals(rawJobid) || jobid.equalsIgnoreCase(actualId);
      }

      public boolean accept(Path path) {
        return (!(path.getName().endsWith(".xml") || 
          path.getName().endsWith(JobHistory.OLD_SUFFIX)) && 
          matchUser(path.getName()) && matchJobId(path.getName()));
      }
    };
    
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String historyLogDir = (String) application.getAttribute("historyLogDir");
    if (fs == null) {
      out.println("Null file system. May be namenode is in safemode!");
      return;
    }
    Path[] jobFiles = FileUtil.stat2Paths(fs.listStatus(new Path(historyLogDir),
                                          jobLogFileFilter));
    out.println("<!--  user : " + user + ", jobid : " + jobid + "-->");
    if (null == jobFiles || jobFiles.length == 0)  {
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
    if (pageno == -1 || size > jobFiles.length) {
      size = jobFiles.length;
    }

    if (pageno == -1) { // special case 'show all'
      pageno = 1;
    }

    int maxPageNo = (int)Math.ceil((float)jobFiles.length / size);

    // check and fix pageno
    if (pageno < 1 || pageno > maxPageNo) {
      out.println("Invalid page index");
      return ;
    }

    int length = size ; // determine the length of job history files to be displayed
    if (pageno == maxPageNo) {
      // find the number of files to be shown on the last page
      int startOnLast = ((pageno - 1) * size) + 1;
      length = jobFiles.length - startOnLast + 1;
    }

    // Display the search box
    out.println("<form name=search><b> Filter (username:jobid) </b>"); // heading
    out.println("<input type=text name=search size=\"20\" value=\"" + search + "\">"); // search box
    out.println("<input type=submit value=\"Filter!\" onClick=\"showUserHistory(document.getElementById('search').value)\"></form>");
    out.println("<span class=\"small\">Example: 'smith' will display jobs submitted by user 'smith'. </span>");
    out.println("<span class=\"small\">Job Ids need to be prefixed with a colon(:) For example, :job_200908311030_0001 will display the job with that id. </span>"); // example 
    out.println("<hr>");

    //Show the status
    int start = (pageno - 1) * size + 1;

    // DEBUG
    out.println("<!-- pageno : " + pageno + ", size : " + size + ", length : " + length + ", start : " + start + ", maxpg : " + maxPageNo + "-->");

    out.println("<font size=5><b>Available Jobs in History </b></font>");
    // display the number of jobs, start index, end index
    out.println("(<i> <span class=\"small\">Displaying <b>" + length + "</b> jobs from <b>" + start + "</b> to <b>" + (start + length - 1) + "</b> out of <b>" + jobFiles.length + "</b> jobs");
    if (!"".equals(user)) {
      out.println(" for user <b>" + HtmlQuoting.quoteHtmlChars(user) + "</b>"); // show the user if present
    }
    if (!"".equals(jobid)) {
      out.println(" for jobid <b>" + HtmlQuoting.quoteHtmlChars(jobid) + "</b> in it."); // show the jobid keyword if present
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

    // sort the files on creation time.
    Arrays.sort(jobFiles, new Comparator<Path>() {
      public int compare(Path p1, Path p2) {
        String dp1 = null;
        String dp2 = null;
        
        dp1 = p1.getName();
        dp2 = p2.getName();
                
        String[] split1 = dp1.split("_");
        String[] split2 = dp2.split("_");
        
        // compare job tracker start time
        int res = new Date(Long.parseLong(split1[1])).compareTo(
                             new Date(Long.parseLong(split2[1])));
        if (res == 0) {
          Long l1 = Long.parseLong(split1[2]);
          res = l1.compareTo(Long.parseLong(split2[2]));
        }
        return res;
      }
    });

    out.println("<br><br>");

    // print the navigation info (top)
    printNavigation(pageno, size, maxPageNo, search, out);

    out.print("<table align=center border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr>");
    out.print( "<td>Job Id</td><td>User</td>") ; 
    out.print("</tr>"); 
    
    Set<String> displayedJobs = new HashSet<String>();
    for (int i = start - 1; i < start + length - 1; ++i) {
      Path jobFile = jobFiles[i];

      String jobId = JobHistory.getJobIDFromHistoryFilePath(jobFile).toString();
      String userName = JobHistory.getUserFromHistoryFilePath(jobFile);

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
      printJob(jobId, userName, new Path(jobFile.getParent(), jobFile), 
               out) ; 
%>
</center> 
<%
    } // end while trackers 
    out.print("</table>");

    // show the navigation info (bottom)
    printNavigation(pageno, size, maxPageNo, search, out);
%>
<%!
    private void printJob(String jobId, 
                          String user, Path logFile, JspWriter out)
    throws IOException {
      out.print("<tr>"); 
      out.print("<td>" + "<a href=\"jobdetailshistory.jsp?logFile=" +
       URLEncoder.encode(logFile.toString(), "UTF-8") +
                "\">" + HtmlQuoting.quoteHtmlChars(jobId) + "</a></td>");
      out.print("<td>" + HtmlQuoting.quoteHtmlChars(user) + "</td>");
      out.print("</tr>");
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
