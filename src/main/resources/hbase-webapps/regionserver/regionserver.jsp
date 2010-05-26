<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="java.io.IOException"
  import="org.apache.hadoop.io.Text"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.HRegion"
  import="org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HServerLoad"
  import="org.apache.hadoop.hbase.HRegionInfo" %><%
  HRegionServer regionServer = (HRegionServer)getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  HServerInfo serverInfo = null;
  try {
    serverInfo = regionServer.getHServerInfo();
  } catch (IOException e) {
    e.printStackTrace();
  }
  RegionServerMetrics metrics = regionServer.getMetrics();
  Collection<HRegionInfo> onlineRegions = regionServer.getSortedOnlineRegionInfos();
  int interval = regionServer.getConfiguration().getInt("hbase.regionserver.msginterval", 3000)/1000;

%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>HBase Region Server: <%= serverInfo.getServerAddress().getHostname() %>:<%= serverInfo.getServerAddress().getPort() %></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>

<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">Region Server: <%= serverInfo.getServerAddress().getHostname() %>:<%= serverInfo.getServerAddress().getPort() %></h1>
<p id="links_menu"><a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a></p>
<hr id="head_rule" />

<h2>Region Server Attributes</h2>
<table>
<tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>
<tr><td>HBase Version</td><td><%= org.apache.hadoop.hbase.util.VersionInfo.getVersion() %>, r<%= org.apache.hadoop.hbase.util.VersionInfo.getRevision() %></td><td>HBase version and svn revision</td></tr>
<tr><td>HBase Compiled</td><td><%= org.apache.hadoop.hbase.util.VersionInfo.getDate() %>, <%= org.apache.hadoop.hbase.util.VersionInfo.getUser() %></td><td>When HBase version was compiled and by whom</td></tr>
<tr><td>Metrics</td><td><%= metrics.toString() %></td><td>RegionServer Metrics; file and heap sizes are in megabytes</td></tr>
<tr><td>Zookeeper Quorum</td><td><%= regionServer.getZooKeeperWrapper().getQuorumServers() %></td><td>Addresses of all registered ZK servers</td></tr>
</table>

<h2>Online Regions</h2>
<% if (onlineRegions != null && onlineRegions.size() > 0) { %>
<table>
<tr><th>Region Name</th><th>Encoded Name</th><th>Start Key</th><th>End Key</th><th>Metrics</th></tr>
<%   for (HRegionInfo r: onlineRegions) { 
        HServerLoad.RegionLoad load = regionServer.createRegionLoad(r.getRegionName());
 %>
<tr><td><%= r.getRegionNameAsString() %></td><td><%= r.getEncodedName() %></td>
    <td><%= Bytes.toStringBinary(r.getStartKey()) %></td><td><%= Bytes.toStringBinary(r.getEndKey()) %></td>
    <td><%= load.toString() %></td>
    </tr>
<%   } %>
</table>
<p>Region names are made of the containing table's name, a comma,
the start key, a comma, and a randomly generated region id.  To illustrate,
the region named
<em>domains,apache.org,5464829424211263407</em> is party to the table 
<em>domains</em>, has an id of <em>5464829424211263407</em> and the first key
in the region is <em>apache.org</em>.  The <em>-ROOT-</em>
and <em>.META.</em> 'tables' are internal sytem tables (or 'catalog' tables in db-speak).
The -ROOT- keeps a list of all regions in the .META. table.  The .META. table
keeps a list of all regions in the system. The empty key is used to denote
table start and table end.  A region with an empty start key is the first region in a table.
If region has both an empty start and an empty end key, its the only region in the table.  See
<a href="http://hbase.org">HBase Home</a> for further explication.<p>
<% } else { %>
<p>Not serving regions</p>
<% } %>
</body>
</html>
