<%@ page
  contentType="application/octet-stream"
  session="false"
  buffer="64kb"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.commons.logging.*"
%><%
  String mapId = request.getParameter("map");
  String reduceId = request.getParameter("reduce");
  if (mapId == null || reduceId == null) {
    throw new IOException("map and reduce parameters are required");
  }
  int reduce = Integer.parseInt(reduceId);
  byte[] buffer = new byte[64*1024];
  OutputStream outStream = response.getOutputStream();
  JobConf conf = (JobConf) application.getAttribute("conf");
  FileSystem fileSys = 
     (FileSystem) application.getAttribute("local.file.system");
  Path filename = conf.getLocalPath(mapId+"/part-"+reduce+".out");
  response.resetBuffer();
  InputStream inStream = null;
  try {
    inStream = fileSys.open(filename);
    int len = inStream.read(buffer);
    while (len > 0) {
      try {
        outStream.write(buffer, 0, len);
      } catch (Exception e) {
        break;
      }
      len = inStream.read(buffer);
    }
  } catch (IOException ie) {
    TaskTracker tracker = 
       (TaskTracker) application.getAttribute("task.tracker");
    Log log = (Log) application.getAttribute("log");
    log.warn("Http server (getMapOutput.jsp): " +
                StringUtils.stringifyException(ie));
    tracker.mapOutputLost(mapId);
    throw ie;
  } finally {
    if (inStream != null) {
      inStream.close();
    }
    if (outStream != null) {
      outStream.close();
    }
  }
%>
