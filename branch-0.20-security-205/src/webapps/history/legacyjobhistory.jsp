<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.net.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.net.*"
  import="org.apache.hadoop.fs.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
%>
<%	
  JobConf jobConf = (JobConf) application.getAttribute("jobConf");
  String trackerUrl;
  String trackerName;

  String trackerAddress = jobConf.get("mapred.job.tracker.http.address");
  InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(trackerAddress);
  if (JobHistoryServer.isEmbedded(jobConf)) {
    trackerName = StringUtils.simpleHostname(InetAddress.
      getLocalHost().getCanonicalHostName());
    trackerName = StringUtils.getHostname();
    trackerUrl = "";
  } else {
    trackerUrl = "http://" + trackerAddress;
    trackerName = StringUtils.simpleHostname(infoSocAddr.getHostName());
  }
%>
<%!	
  private static SimpleDateFormat dateFormat = 
                                    new SimpleDateFormat("d/MM HH:mm:ss");
%>
<%!	private static final long serialVersionUID = 1L;
%>
<html>
<head>
<script type="text/JavaScript">
<!--
function showUserHistory(search)
{
var url
if (search == null || "".equals(search)) {
  url="legacyjobhistory.jsp";
} else {
  url="legacyjobhistory.jsp?pageno=1&search=" + search;
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
<h1> <a href="<%=trackerUrl%>/jobtracker.jsp"><%= trackerName %></a> Hadoop Map/Reduce
     <a href="jobhistoryhome.jsp">History Viewer</a></h1>
<hr>
<%
    final String search = (request.getParameter("search") == null)
                          ? ""
                          : request.getParameter("search");

    String parts[] = search.split(":");

    final String user = (parts.length >= 1)
                        ? parts[0].toLowerCase()
                        : "";
    final String jobname = (parts.length >= 2)
                           ? parts[1].toLowerCase()
                           : "";
    PathFilter jobLogFileFilter = new PathFilter() {
      // unquote params before encoding for search
      final String uqUser = JobHistory.JobInfo.encodeJobHistoryFileName(
            HtmlQuoting.unquoteHtmlChars(user));
      final String uqJobname = JobHistory.JobInfo.encodeJobHistoryFileName(
            HtmlQuoting.unquoteHtmlChars(jobname));
      private boolean matchUser(String fileName) {
        // return true if 
        //  - user is not specified
        //  - user matches
        return "".equals(uqUser) || uqUser.equals(fileName.split("_")[5]);
      }

      private boolean matchJobName(String fileName) {
        // return true if 
        //  - jobname is not specified
        //  - jobname contains the keyword
        return "".equals(uqJobname) || fileName.split("_")[6].toLowerCase().contains(uqJobname);
      }

      public boolean accept(Path path) {
        return !(path.getName().endsWith(".xml")) && matchUser(path.getName()) && matchJobName(path.getName());
      }
    };
    
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String historyLogDir = (String) application.getAttribute("historyLogDir");
    if (fs == null) {
      out.println("Null file system. May be namenode is in safemode!");
      return;
    }
    
    Path[] jobFiles 
       = JobHistory.filteredStat2Paths(fs.listStatus(new Path(historyLogDir),
                                                    jobLogFileFilter),
                                       false, null);

    out.println("<!--  user : " + user +
        ", jobname : " + jobname + "-->");
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
    out.println("<form name=search><b> Filter (username:jobname) </b>"); // heading
    out.println("<input type=text name=search size=\"20\" value=\"" + search + "\">"); // search box
    out.println("<input type=submit value=\"Filter!\" onClick=\"showUserHistory(document.getElementById('search').value)\"></form>");
    out.println("<span class=\"small\">Example: 'smith' will display jobs either submitted by user 'smith'. 'smith:sort' will display jobs from user 'smith' having 'sort' keyword in the jobname.</span>"); // example
    out.println("<hr>");

    //Show the status
    int start = (pageno - 1) * size + 1;

    // DEBUG
    out.println("<!-- pageno : " + pageno + ", size : " + size + ", length : " + length + ", start : " + start + ", maxpg : " + maxPageNo + "-->");

    out.println("<font size=5><b>Available Jobs in History </b></font>");
    // display the number of jobs, start index, end index
    out.println("(<i> <span class=\"small\">Displaying <b>" + length + "</b> jobs from <b>" + start + "</b> to <b>" + (start + length - 1) + "</b> out of <b>" + jobFiles.length + "</b> jobs");
    if (!"".equals(user)) {
      // show the user if present
      out.println(" for user <b>" + user + "</b>");
    }
    if (!"".equals(jobname)) {
      out.println(" with jobname having the keyword <b>" +
          jobname + "</b> in it."); // show the jobname keyword if present
    }
    out.print("</span></i>)");

    // show the 'show-all' link
    out.println(" [<span class=\"small\"><a href=\"legacyjobhistory.jsp?pageno=-1&search=" + search + "\">show all</a></span>]");

    // show the 'first-page' link
    if (pageno > 1) {
      out.println(" [<span class=\"small\"><a href=\"legacyjobhistory.jsp?pageno=1&search=" + search + "\">first page</a></span>]");
    } else {
      out.println("[<span class=\"small\">first page]</span>");
    }

    // show the 'last-page' link
    if (pageno < maxPageNo) {
      out.println(" [<span class=\"small\"><a href=\"legacyjobhistory.jsp?pageno=" + maxPageNo + "&search=" + search + "\">last page</a></span>]");
    } else {
      out.println("<span class=\"small\">[last page]</span>");
    }

    // sort the files on creation time.
    Arrays.sort(jobFiles, new Comparator<Path>() {
      public int compare(Path p1, Path p2) {
        String dp1 = null;
        String dp2 = null;
        
        try {
          dp1 = JobHistory.JobInfo.decodeJobHistoryFileName(p1.getName());
          dp2 = JobHistory.JobInfo.decodeJobHistoryFileName(p2.getName());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
                
        String[] split1 = dp1.split("_");
        String[] split2 = dp2.split("_");
        
        // compare job tracker start time
        int res = new Date(Long.parseLong(split1[1])).compareTo(
                             new Date(Long.parseLong(split2[1])));
        if (res == 0) {
          res = new Date(Long.parseLong(split1[3])).compareTo(
                           new Date(Long.parseLong(split2[3])));
        }
        if (res == 0) {
          Long l1 = Long.parseLong(split1[4]);
          res = l1.compareTo(Long.parseLong(split2[4]));
        }
        return res;
      }
    });

    out.println("<br><br>");

    // print the navigation info (top)
    printNavigation(pageno, size, maxPageNo, search, out);

    out.print("<table align=center border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr>");
    out.print("<td>Job tracker Host Name</td>" +
              "<td>Job tracker Start time</td>" +
              "<td>Job Id</td><td>Name</td><td>User</td>") ; 
    out.print("</tr>"); 
    
    Set<String> displayedJobs = new HashSet<String>();
    for (int i = start - 1; i < start + length - 1; ++i) {
      Path jobFile = jobFiles[i];
      
      String decodedJobFileName = 
          JobHistory.JobInfo.decodeJobHistoryFileName(jobFile.getName());

      String[] jobDetails = decodedJobFileName.split("_");
      String trackerHostName = jobDetails[0];
      String trackerStartTime = jobDetails[1];
      String jobId = jobDetails[2] + "_" +jobDetails[3] + "_" + jobDetails[4] ;
      String userName = jobDetails[5];
      String jobName = jobDetails[6];
      
      // Check if the job is already displayed. There can be multiple job 
      // history files for jobs that have restarted
      if (displayedJobs.contains(jobId)) {
        continue;
      } else {
        displayedJobs.add(jobId);
      }
      
      // Encode the logfile name again to cancel the decoding done by the browser
      String encodedJobFileName = 
          JobHistory.JobInfo.encodeJobHistoryFileName(jobFile.getName());
%>
<center>
<%	
      printJob(trackerHostName, trackerStartTime, jobId,
               jobName, userName, new Path(jobFile.getParent(), encodedJobFileName), 
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
    private void printJob(String trackerHostName, String trackerid,
                          String jobId, String jobName,
                          String user, Path logFile, JspWriter out)
    throws IOException {
      out.print("<tr>"); 
      out.print("<td>" + trackerHostName + "</td>"); 
      out.print("<td>" + new Date(Long.parseLong(trackerid)) + "</td>"); 
      out.print("<td>" + "<a href=\"jobdetailshistory.jsp?logFile=" 
          + logFile.toString() + "\">" + jobId + "</a></td>");
      out.print("<td>" + HtmlQuoting.quoteHtmlChars(jobName) + "</td>"); 
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
        out.println("<a href=\"legacyjobhistory.jsp?pageno=" + (pageno - 1) +
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
          out.println(" <a href=\"legacyjobhistory.jsp?pageno=" + i + "&search=" +
              search + "\">" + i + "</a> ");
        } else { // current page
          out.println(i);
        }
      }

      // show the next link
      if (pageno < max) {
        out.println("<a href=\"legacyjobhistory.jsp?pageno=" + (pageno + 1) + "&search=" + search + "\">Next</a>");
      }
      out.print("></center>");
    }
%> 
</body></html>
