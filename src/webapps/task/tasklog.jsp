<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
%>
<%!
  private void printTaskLog(JspWriter out, String taskId, 
      long logOffset, long logLength, 
      boolean tailLog, long tailSize, int tailWindow,
      boolean entireLog, boolean plainText, TaskLog.LogFilter filter) 
  throws IOException {
    if (!plainText) {
      out.println("<br><b><u>" + filter + " logs</u></b><br>");
      out.println("<table border=2 cellpadding=\"2\">");
    }
    
    boolean gotRequiredData = true;
    try {
  	  TaskLog.Reader taskLogReader = new TaskLog.Reader(taskId, filter);
      byte[] b = null;
  	  int bytesRead = 0;
  	  int targetLength = 0;

  	  if (entireLog) {
  	    b = taskLogReader.fetchAll();
  	    targetLength = bytesRead = b.length;
  	  } else {
  	    if (tailLog) {
  		  b = new byte[(int)tailSize];
  		  targetLength = (int)tailSize;
  		  bytesRead = taskLogReader.tail(b, 0, b.length, tailSize, tailWindow);
  	    } else {
  		  b = new byte[(int)logLength];
  		  targetLength = (int)logLength;
  		  bytesRead = taskLogReader.read(b, 0, b.length, logOffset, logLength);
    	}
  	  }
  	  
  	  if (bytesRead != targetLength && 
  	    targetLength <= taskLogReader.getTotalLogSize()) {
  	    if( !plainText) {
	  	  out.println("<b>Warning: Could not fetch " + targetLength + 
	  		  " bytes from the task-logs; probably purged!</b><br/>");
  	    }else{
	  	  out.println("Warning: Could not fetch " + targetLength + 
  		  " bytes from the task-logs; probably purged!");
  	    }
  	    gotRequiredData = false;
  	  }
	  String logData = new String(b, 0, bytesRead);
	  if (!plainText) {
	    out.print("<tr><td><pre>" + logData + "</pre></td></tr>");
	  } else {
	    out.print(logData);
	  }
    } catch (IOException ioe) {
  	  out.println("Failed to retrieve '" + filter + "' logs for task: " + taskId);
    }
  
    if( !plainText ) {
     out.println("</table>\n");
    } 

    if (!entireLog && !plainText) {
      if (tailLog) {
        if (gotRequiredData) {
  	  	  out.println("<a href='/tasklog.jsp?taskid=" + taskId + 
  		    "&tail=true&tailsize=" + tailSize + "&tailwindow=" + (tailWindow+1) + 
  		    "&filter=" + filter + "'>Earlier</a>");
  	    }
  	    if (tailWindow > 1) {
          out.println("<a href='/tasklog.jsp?taskid=" + taskId + 
  	  	    "&tail=true&tailsize=" + tailSize + "&tailwindow=" + (tailWindow-1) 
  	  	    + "&filter=" + filter + "'>Later</a>");
  	    }
      } else {
        if (gotRequiredData) {
      	  out.println("<a href='/tasklog.jsp?taskid=" + taskId + 
    		"&tail=false&off=" + Math.max(0, (logOffset-logLength)) +
  		  	"&len=" + logLength + "&filter=" + filter + "'>Earlier</a>");
  	    }
  	    out.println("<a href='/tasklog.jsp?taskid=" + taskId + 
  		  "&tail=false&off=" + (logOffset+logLength) +
  		  "&len=" + logLength + "&filter=" + filter + "'>Later</a>");
      }
    }
    
    if (!plainText) {
      out.println("<hr><br>");
    }
  }
%>

<%  
  String taskId = null;
  long logOffset = -1, logLength = -1;
  boolean tailLog = false;
  long tailSize = 1024;
  int tailWindow = 1;
  boolean entireLog = false;
  boolean plainText = false;
  TaskLog.LogFilter filter = null;

  taskId = request.getParameter("taskid");
  if (taskId == null) {
  	out.println("<h2>Missing 'taskid' for fetching logs!</h2>");
  	return;
  }
  
  String logFilter = request.getParameter("filter");
  if (logFilter != null) {
    try {
      filter = TaskLog.LogFilter.valueOf(TaskLog.LogFilter.class, 
                                      logFilter.toUpperCase());
    } catch (IllegalArgumentException iae) {
      out.println("<h2>Illegal 'filter': " + logFilter + "</h2>");
      return;
    }
  }
  
  String sLogOff = request.getParameter("off");
  if (sLogOff != null) {
  	logOffset = Long.valueOf(sLogOff).longValue();
  }
  
  String sLogLen = request.getParameter("len");
  if (sLogLen != null) {
  	logLength = Long.valueOf(sLogLen).longValue();
  }
  
  String sEntireLog = request.getParameter("all");
  if (sEntireLog != null) {
  	entireLog = Boolean.valueOf(sEntireLog);
  }
  
  String sTail = request.getParameter("tail");
  if (sTail != null) {
  	tailLog = Boolean.valueOf(sTail);
  }
  
  String sTailLen = request.getParameter("tailsize");
  if (sTailLen != null) {
  	tailSize = Long.valueOf(sTailLen).longValue();
  }
  
  String sTailWindow = request.getParameter("tailwindow");
  if (sTailWindow != null) {
  	tailWindow = Integer.valueOf(sTailWindow).intValue();
  }
  
  String sPlainText = request.getParameter("plaintext");
  if (sPlainText != null) {
    plainText = Boolean.valueOf(sPlainText);
  }

  if (logOffset == -1 || logLength == -1) {
  	tailLog = true;
  	tailWindow = 1;
  }

  if (entireLog) {
    tailLog = false;
  }
  
  if( !plainText ) {
    out.println("<html>");
    out.println("<title>Task Logs: '" + taskId + "'</title>"); 
    out.println("<body>");
    out.println("<h1>Task Logs: '" +  taskId +  "'</h1><br>"); 

    if (filter == null) {
      printTaskLog(out, taskId, logOffset, logLength, 
          tailLog, tailSize, tailWindow, 
          entireLog, plainText, TaskLog.LogFilter.STDOUT);
      printTaskLog(out, taskId, logOffset, logLength, 
          tailLog, tailSize, tailWindow, 
          entireLog, plainText, TaskLog.LogFilter.STDERR);
      printTaskLog(out, taskId, logOffset, logLength, 
          tailLog, tailSize, tailWindow, 
          entireLog, plainText, TaskLog.LogFilter.SYSLOG);
    } else {
      printTaskLog(out, taskId, logOffset, logLength, 
          tailLog, tailSize, tailWindow, 
          entireLog, plainText, filter);
    }
    
    out.println("<a href='http://lucene.apache.org/hadoop'>Hadoop</a>, 2006.<br>");
    out.println("</body>");
    out.println("</html>");
  } else {
    printTaskLog(out, taskId, logOffset, logLength, tailLog, tailSize, tailWindow, 
        entireLog, plainText, filter);
  } 
%>
