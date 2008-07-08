<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.List"
  import="org.apache.hadoop.hbase.RegionHistorian"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.RegionHistorian.RegionHistoryInformation"
  import="org.apache.hadoop.hbase.HConstants"%><%
  String regionName = request.getParameter("regionname");
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  List<RegionHistoryInformation> informations = RegionHistorian.getInstance().getRegionHistory(regionName);
%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
      <meta http-equiv="refresh" content="30"/>
<title>Region in <%= regionName %></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>

<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">Region <%= regionName %></h1>
<p id="links_menu"><a href="/master.jsp">Master</a>, <a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a></p>
<hr id="head_rule" />
<%if(informations != null && informations.size() > 0) { %>
<table><tr><th>Timestamp</th><th>Event</th><th>Description</th></tr>
<%  for( RegionHistoryInformation information : informations) {%>
<tr><td><%= information.getTimestampAsString() %></td><td><%= information.getEvent() %></td><td><%= information.getDescription()%></td></tr>
<%  } %>
</table>
<p>
Master is the source of following events: creation, open, and assignment.  Regions are the source of following events: split, compaction, and flush.
</p>
<%} else {%>
<p>
This region is no longer available. It may be due to a split, a merge or the name changed.
</p>
<%} %>


</body>
</html>
