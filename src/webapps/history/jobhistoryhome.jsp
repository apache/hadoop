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
  import="java.io.*"
  import="java.net.*"
  import="java.util.*"
  import="java.util.regex.Pattern"
  import="java.util.regex.Matcher"
  import="java.util.concurrent.atomic.AtomicBoolean"
  import="org.apache.hadoop.net.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
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
    trackerUrl = "";
  } else {
    trackerUrl = "http://" + trackerAddress;
    trackerName = StringUtils.simpleHostname(infoSocAddr.getHostName());
  }
%>
<%!private static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d/MM HH:mm:ss");%>
<%!private static final long serialVersionUID = 1L;%>
<!DOCTYPE html>
<html>
<head>
<script type="text/JavaScript">
<!--
function showUserHistory(search)
{
var url
if (search == null || "".equals(search)) {
  url="jobhistoryhome.jsp";
} else {
  url="jobhistoryhome.jsp?pageno=1&search=" + search;
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
  //{ // these braces are here to make indentation work and 
  //  {// must be removed.

    final int JOB_ID_START = 0;

    final int FILENAME_JOBID_END = JOB_ID_START + 3;

    final int FILENAME_SUBMIT_TIMESTAMP_PART = FILENAME_JOBID_END;
    
    final int FILENAME_USER_PART = FILENAME_JOBID_END + 1;

    final int FILENAME_JOBNAME_PART = FILENAME_JOBID_END + 2;

    final int[] SCAN_SIZES = { 20, 50, 200, 1000 };

    final int FILES_PER_SCAN = 1000;

    final int DEFAULT_PAGE_SIZE = 100;

    final String DEFAULT_DATE_GLOB_COMPONENT = "*/*/*";

    final String SERIAL_NUMBER_GLOB_COMPONENT = "/*";

    final String search = (request.getParameter("search") == null)
                          ? ""
                          : request.getParameter("search");

    final String dateSplit[] = search.split(";");

    final String soughtDate = dateSplit.length > 1 ? dateSplit[1] : "";

    final String parts[] = dateSplit[0].split(":");

    final String rawUser = (parts.length >= 1)
                            ? parts[0].toLowerCase()
                            : "";

    final String userInFname
      = escapeUnderscores(JobHistory.JobInfo.encodeJobHistoryFileName(
            HtmlQuoting.unquoteHtmlChars(rawUser))).toLowerCase();

    final int currentScanSizeIndex
      = (request.getParameter("scansize") == null)
           ? 0 : 
             Math.min(Integer.parseInt(request.getParameter("scansize")), 
                 SCAN_SIZES.length-1);

    final String SEARCH_PARSE_REGEX
      = "([0-1]?[0-9])/([0-3]?[0-9])/((?:2[0-9])[0-9][0-9])";

    final Pattern dateSearchParse = Pattern.compile(SEARCH_PARSE_REGEX);

    final String rawJobname = (parts.length >= 2)
                               ? parts[1].toLowerCase()
                               : "";

    final String jobnameKeywordInFname
      = escapeUnderscores(JobHistory.JobInfo.encodeJobHistoryFileName(
            HtmlQuoting.unquoteHtmlChars(rawJobname))).toLowerCase();

    PathFilter jobLogFileFilter = new PathFilter() {
      private boolean matchUser(String fileName) {
        // return true if 
        //  - user is not specified
        //  - user matches
        return "".equals(userInFname)
           || userInFname.equals(fileName.split("_")[FILENAME_USER_PART]
                .toLowerCase());
      }

      private boolean matchJobName(String fileName) {
        // return true if 
        //  - jobname is not specified
        //  - jobname contains the keyword
        return "".equals(jobnameKeywordInFname) 
                 || fileName.split("_")[FILENAME_JOBNAME_PART].toLowerCase()
                       .contains(jobnameKeywordInFname);
      }
      
      private boolean isHistoryFile(String fileName) {      	
        String[] tokens = null;
        try {
          String dp = JobHistory.JobInfo.decodeJobHistoryFileName(fileName);
          tokens = dp.split("_");
        } catch (IOException ioe) {
        }
	    
        return tokens != null && !fileName.endsWith(".xml") && tokens.length > 3
            && tokens[1].matches("\\d+")  && tokens[2].matches("\\d+")
            && tokens[3].matches("\\d+");
      }

      public boolean accept(Path path) {
        String name = path.getName();

        return isHistoryFile(name) && matchUser(name) && matchJobName(name);
      }
    };
    
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String historyLogDir = (String) application.getAttribute("historyLogDir");
    if (fs == null) {
      out.println("Null file system. May be namenode is in safemode!");
      return;
    }

    Comparator<FileStatus> mtimeComparator
      = new Comparator<FileStatus>() {
          public int compare(FileStatus status1, FileStatus status2) {
            Long time1 = new Long(status1.getModificationTime());
            Long time2 = new Long(status2.getModificationTime());
            return time2.compareTo(time1);
          }
    };

    Comparator<Path> latestFirstCreationTimeComparator
      = new Comparator<Path>() {
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
            // reverse the sense, because we want the newest records first
            int res = new Date(Long.parseLong(split2[1]))
               .compareTo(new Date(Long.parseLong(split1[1])));
            // compare the submit times next
            // again, reverse the sense
            if (res == 0) {
              res = new Date(Long.parseLong(split2[3]))
                .compareTo(new Date(Long.parseLong(split1[3])));
            }
            // lastly, compare the serial numbers [a certain tiebreaker]
            // again, reverse the sense
            if (res == 0) {
              Long l1 = Long.parseLong(split2[2]);
              res = l1.compareTo(Long.parseLong(split1[2]));
            }
            return res;
      }
    };

    String versionComponent = JobHistory.DONE_DIRECTORY_FORMAT_DIRNAME;

    String trackerComponent = "*";

    // build the glob
    // first find the date component
    String dateComponent = DEFAULT_DATE_GLOB_COMPONENT;

    Matcher dateMatcher = dateSearchParse.matcher(soughtDate);

    // burst the sought date: must be [m]m/[d]d/[2y]yy
    if (dateMatcher.matches()) {
      String year = dateMatcher.group(3);
      if (year.length() == 2) {
        year = "20" + year;
      }

      String month = dateMatcher.group(1);
      if (month.length() == 1) {
        month = "0" + month;
      }

      String date = dateMatcher.group(2);
      if (date.length() == 1) {
        date = "0" + date;
      }

      dateComponent = year + "/" + month + "/" + date;
    }

    // now we find all of the serial numbers.  This looks up all the serial
    // number directories, but not the individual files.
    Path historyPath = new Path(historyLogDir);

    String leadGlob = (versionComponent
            + "/" + trackerComponent
            + "/" + dateComponent);

    // Atomicity is unimportant here.
    // I would have used MutableBoxedBoolean if such had been provided.
    AtomicBoolean hasLegacyFiles = new AtomicBoolean(false);

    FileStatus[] buckets = 
      JobHistory.localGlobber
                            (fs, historyPath, "/" + leadGlob, null, hasLegacyFiles);
    Arrays.sort(buckets, mtimeComparator);

    int arrayLimit = SCAN_SIZES[currentScanSizeIndex] > buckets.length ? 
        buckets.length : SCAN_SIZES[currentScanSizeIndex];
    FileStatus[] scanSizeBuckets = Arrays.copyOf(buckets, arrayLimit);
    Path[] snPaths = FileUtil.stat2Paths(scanSizeBuckets);
    
    int numHistoryFiles = 0;

    Path[] jobFiles = null;

    {
      Path[][] pathVectorVector = new Path[arrayLimit][];

      for (int i = 0; i < arrayLimit; ++i) {
        pathVectorVector[i]
          = FileUtil.stat2Paths(fs.listStatus(snPaths[i], jobLogFileFilter));
        numHistoryFiles += pathVectorVector[i].length;
      }

      jobFiles = new Path[numHistoryFiles];

      int pathsCursor = 0;

      for (int i = 0; i < arrayLimit; ++i) {
        System.arraycopy(pathVectorVector[i], 0, jobFiles, pathsCursor,
                         pathVectorVector[i].length);
        pathsCursor += pathVectorVector[i].length;
      }
    }

    boolean sizeIsExact = arrayLimit == snPaths.length;

    // sizeIsExact will be true if arrayLimit is zero.
    long lengthEstimate
      = sizeIsExact ? numHistoryFiles
                    : (long) numHistoryFiles * snPaths.length / arrayLimit;

    if (hasLegacyFiles.get()) {
      out.println("<h2>This history has some legacy files.  "
                  + "<a href=\"legacyjobhistory.jsp\">go to Legacy History Viewer</a>"
                  + "</h2>");
    }

    out.println("<!--  user : " + rawUser +
        ", jobname : " + rawJobname + "-->");
    final String searchMore = "&search=" + search;
    if (null == jobFiles || jobFiles.length == 0) {
      if (currentScanSizeIndex < SCAN_SIZES.length -1) {
        out.println(" [<span class=\"small\"><a href=\"jobhistoryhome.jsp?pageno=1"
          + searchMore + "&scansize=" + (currentScanSizeIndex +1)
          + "\">No files found - try and get more results</a></span>]");
      } else {
        out.println("No files found!"); 
      }
      return;
    }

    // get the pageno
    int pageno = request.getParameter("pageno") == null
                ? 1
                : Integer.parseInt(request.getParameter("pageno"));

    // get the total number of files to display
    int size = DEFAULT_PAGE_SIZE;

    // if show-all is requested or jobfiles < size(100)
    if (pageno == -1 || size > jobFiles.length) {
      size = jobFiles.length;
    }

    if (pageno == -1) { // special case 'show all'
      pageno = 1;
    }

    int maxPageNo = (jobFiles.length + size - 1) / size;
    // int maxPageNo = (int)Math.ceil((float)jobFiles.length / size);

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
    out.println("<input type=text name=search size=\"20\" "
                + "value=\"" + search + "\">"); // search box
    out.println("<input type=submit value=\"Filter!\" onClick=\"showUserHistory"
                + "(document.getElementById('search').value)\"></form>");
    out.println("<p><span class=\"small\">Specify [user][:jobname keyword(s)]"
                + "[;MM/DD/YYYY] .  Each of the three components is "
                + "optional.  Filter components are conjunctive.</span></p>");
    out.println("<p><span class=\"small\">Example: 'smith' will display jobs"
                + " submitted by user 'smith'. 'smith:sort' will display "
                + "jobs from user 'smith' having a 'sort' keyword in the jobname."
                + " ';07/04/2010' restricts to July 4, 2010</span></p>"); // example
    out.println("<hr>");

    //Show the status
    int start = (pageno - 1) * size + 1;

    // DEBUG
    out.println("<!-- pageno : " + pageno + ", size : " + size + ", length : "
                + length + ", start : " + start + ", maxpg : "
                + maxPageNo + "-->");

    out.println("<font size=5><b>Available Jobs in History </b></font>");
    // display the number of jobs, start index, end index
    out.println("(<i> <span class=\"small\">Displaying <b>" + length
                + "</b> jobs from <b>" + start + "</b> to <b>"
                + (start + length - 1) + "</b> out of "
                + (sizeIsExact
                   ? "" : "approximately ") + "<b>"
                + lengthEstimate + "</b> jobs"
                + (sizeIsExact
                   ? ""
                   : ", <b>" + numHistoryFiles + "</b> gotten"));
    if (!"".equals(rawUser)) {
      // show the user if present
      out.println(" for user <b>" + rawUser + "</b>");
    }
    if (!"".equals(rawJobname)) {
      out.println(" with jobname having the keyword <b>" +
          rawJobname + "</b> in it.");
      // show the jobname keyword if present
    }
    if (!DEFAULT_DATE_GLOB_COMPONENT.equals(dateComponent)) {
      out.println(" for the date <b>" + soughtDate + "</b>");
    }
    out.print("</span></i>)");

    final String searchPart = "&search=" + search;

    final String scansizePart = "&scansize=" + currentScanSizeIndex;

    final String searchPlusScan = searchPart + scansizePart;

    // show the expand scope link, if we're restricted
    if (currentScanSizeIndex == SCAN_SIZES.length - 1) {
      out.println("[<span class=\"small\">get more results</span>]");
    } else {
      out.println(" [<span class=\"small\"><a href=\"jobhistoryhome.jsp?pageno=1"
                  + searchPart + "&scansize=" + (currentScanSizeIndex + 1)
                  + "\">get more results</a></span>]");
    }

    // show the 'show-all' link
    out.println(" [<span class=\"small\"><a href=\"jobhistoryhome.jsp?pageno=-1"
                + searchPlusScan + "\">show in one page</a></span>]");

    // show the 'first-page' link
    if (pageno > 1) {
      out.println(" [<span class=\"small\"><a href=\"jobhistoryhome.jsp?pageno=1"
                  + searchPlusScan + "\">first page</a></span>]");
    } else {
      out.println("[<span class=\"small\">first page]</span>");
    }

    // show the 'last-page' link
    if (pageno < maxPageNo) {
      out.println(" [<span class=\"small\"><a href=\"jobhistoryhome.jsp?pageno="
                  + maxPageNo + searchPlusScan + "\">last page</a></span>]");
    } else {
      out.println("<span class=\"small\">[last page]</span>");
    }

    // sort the files on creation time.
    Arrays.sort(jobFiles, latestFirstCreationTimeComparator);

    out.println("<br><br>");

    // print the navigation info (top)
    printNavigationTool(pageno, size, maxPageNo, searchPlusScan, out);

    out.print("<table align=center border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr>");
    out.print("<td>Job submit time</td>" +
              "<td>Job Id</td><td>Name</td><td>User</td>") ; 
    out.print("</tr>"); 
    
    Set<String> displayedJobs = new HashSet<String>();
    for (int i = start - 1; i < start + length - 1; ++i) {
      Path jobFile = jobFiles[i];

      String fname = jobFile.getName();
      String marker = JobHistory.nonOccursString(fname);
      String reescapedFname = JobHistory.replaceStringInstances(fname,
                  JobHistory.UNDERSCORE_ESCAPE, marker);
      
      String decodedJobFileName = 
          JobHistory.JobInfo.decodeJobHistoryFileName(reescapedFname);

      String[] jobDetails = decodedJobFileName.split("_");
      String trackerStartTime = jobDetails[1];
      String jobId = (jobDetails[JOB_ID_START]
                      + "_" + jobDetails[JOB_ID_START + 1]
                      + "_" + jobDetails[JOB_ID_START + 2]);
      String submitTimestamp = jobDetails[FILENAME_SUBMIT_TIMESTAMP_PART];
      String userName = JobHistory.replaceStringInstances(jobDetails[FILENAME_USER_PART],
                  marker, JobHistory.UNDERSCORE_ESCAPE);
      String jobName = JobHistory.replaceStringInstances(jobDetails[FILENAME_JOBNAME_PART],
                  marker, JobHistory.UNDERSCORE_ESCAPE);
      
      // Check if the job is already displayed. There can be multiple job 
      // history files for jobs that have restarted
      if (displayedJobs.contains(jobId)) {
        continue;
      } else {
        displayedJobs.add(jobId);
      }
      
      // Encode the logfile name again to cancel the decoding done by the browser
      String preEncodedJobFileName = 
          JobHistory.JobInfo.encodeJobHistoryFileName(jobFile.getName());

      String encodedJobFileName = 
          JobHistory.replaceStringInstances(preEncodedJobFileName, "%5F", "%255F");
%>
<center>
<%	
      printJob(submitTimestamp, jobId,
               jobName, userName, new Path(jobFile.getParent(), encodedJobFileName), 
               out) ; 
%>
</center> 
<%
    } // end while trackers 
    out.print("</table>");

    // show the navigation info (bottom)
    printNavigationTool(pageno, size, maxPageNo, searchPlusScan, out);
%>
<%!
    private void printJob(String timestamp,
                          String jobId, String jobName,
                          String user, Path logFile, JspWriter out)
    throws IOException {
      out.print("<tr>"); 
      out.print("<td>" + new Date(Long.parseLong(timestamp)) + "</td>"); 
      out.print("<td>" + "<a href=\"jobdetailshistory.jsp?logFile=" 
          + logFile.toString() + "\">" + jobId + "</a></td>");
      out.print("<td>"
                + HtmlQuoting.quoteHtmlChars(unescapeUnderscores(jobName))
                + "</td>"); 
      out.print("<td>"
                + HtmlQuoting.quoteHtmlChars(unescapeUnderscores(user))
                + "</td>"); 
      out.print("</tr>");
    }

    private String escapeUnderscores(String rawString) {
      return convertStrings(rawString, "_", "%5F");
    }

    private String unescapeUnderscores(String rawString) {
      return convertStrings(rawString, "%5F", "_");
    }

    // inefficient if there are a lot of underscores
    private String convertStrings(String escapedString, String from, String to) {
      int firstEscape = escapedString.indexOf(from);

      if (firstEscape < 0) {
        return escapedString;
      }

      return escapedString.substring(0, firstEscape)
            + to
            + unescapeUnderscores(escapedString.substring
                                    (firstEscape + from.length()));
    }

    private void printNavigationTool(int pageno, int size, int max,
                                     String searchPlusScan, JspWriter out)
         throws IOException {
      
      final int NUMBER_INDICES_TO_SHOW = 5;

      int numIndexToShow = NUMBER_INDICES_TO_SHOW; // num indexes to show on either side

      //TODO check this on boundary cases
      out.print("<center> <");

      // show previous link
      if (pageno > 1) {
        out.println("<a href=\"jobhistoryhome.jsp?pageno=" + (pageno - 1)
                    + searchPlusScan + "\">Previous</a>");
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
          out.println(" <a href=\"jobhistoryhome.jsp?pageno=" + i
                      + searchPlusScan + "\">" + i + "</a> ");
        } else { // current page
          out.println(i);
        }
      }

      // show the next link
      if (pageno < max) {
        out.println("<a href=\"jobhistoryhome.jsp?pageno=" + (pageno + 1) + searchPlusScan + "\">Next</a>");
      }
      out.print("></center>");
    }
%> 
</body></html>
