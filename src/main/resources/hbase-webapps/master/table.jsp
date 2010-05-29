<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.Map"
  import="org.apache.hadoop.io.Writable"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.HTable"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.HServerAddress"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.io.ImmutableBytesWritable"
  import="org.apache.hadoop.hbase.master.HMaster" 
  import="org.apache.hadoop.hbase.master.MetaRegion"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="java.util.Map"
  import="org.apache.hadoop.hbase.HConstants"%><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  HBaseAdmin hbadmin = new HBaseAdmin(conf);
  String tableName = request.getParameter("name");
  HTable table = new HTable(conf, tableName);
  String tableHeader = "<h2>Table Regions</h2><table><tr><th>Name</th><th>Region Server</th><th>Encoded Name</th><th>Start Key</th><th>End Key</th></tr>";
  HServerAddress rootLocation = master.getRegionManager().getRootRegionLocation();
  boolean showFragmentation = conf.getBoolean("hbase.master.ui.fragmentation.enabled", false);
  Map<String, Integer> frags = null;
  if (showFragmentation) {
      frags = master.getTableFragmentation();
  }
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">

<%
  String action = request.getParameter("action");
  String key = request.getParameter("key");
  if ( action != null ) {
%>
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">Table action request accepted</h1>
<p><hr><p>
<%
  if (action.equals("split")) {
    if (key != null && key.length() > 0) {
      Writable[] arr = new Writable[1];
      arr[0] = new ImmutableBytesWritable(Bytes.toBytes(key));
      master.modifyTable(Bytes.toBytes(tableName), HConstants.Modify.TABLE_SPLIT, arr);
    } else {
      master.modifyTable(Bytes.toBytes(tableName), HConstants.Modify.TABLE_SPLIT, null);
    }
    %> Split request accepted. <%
  } else if (action.equals("compact")) {
    if (key != null && key.length() > 0) {
      Writable[] arr = new Writable[1];
      arr[0] = new ImmutableBytesWritable(Bytes.toBytes(key));
      master.modifyTable(Bytes.toBytes(tableName), HConstants.Modify.TABLE_COMPACT, arr);
    } else {
      master.modifyTable(Bytes.toBytes(tableName), HConstants.Modify.TABLE_COMPACT, null);
    }
    %> Compact request accepted. <%
  }
%>
<p>Reload.
</body>
<%
} else {
%>
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>Table: <%= tableName %></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">Table: <%= tableName %></h1>
<p id="links_menu"><a href="/master.jsp">Master</a>, <a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a></p>
<hr id="head_rule" />
<%
  if(tableName.equals(Bytes.toString(HConstants.ROOT_TABLE_NAME))) {
%>
<%= tableHeader %>
<%
  int infoPort = master.getServerManager().getHServerInfo(rootLocation).getInfoPort();
  String url = "http://" + rootLocation.getHostname() + ":" + infoPort + "/";
%>
<tr>
  <td><%= tableName %></td>
  <td><a href="<%= url %>"><%= rootLocation.getHostname() %>:<%= rootLocation.getPort() %></a></td>
  <td>-</td>
  <td></td>
  <td>-</td>
</tr>
</table>
<%
  } else if(tableName.equals(Bytes.toString(HConstants.META_TABLE_NAME))) {
%>
<%= tableHeader %>
<%
  Map<byte [], MetaRegion> onlineRegions = master.getRegionManager().getOnlineMetaRegions();
  for (MetaRegion meta: onlineRegions.values()) {
    int infoPort = master.getServerManager().getHServerInfo(meta.getServer()).getInfoPort();
    String url = "http://" + meta.getServer().getHostname() + ":" + infoPort + "/";
%>
<tr>
  <td><%= Bytes.toString(meta.getRegionName()) %></td>
    <td><a href="<%= url %>"><%= meta.getServer().getHostname().toString() + ":" + infoPort %></a></td>
    <td>-</td><td><%= Bytes.toString(meta.getStartKey()) %></td><td><%= Bytes.toString(meta.getEndKey()) %></td>
</tr>
<%  } %>
</table>
<%} else {
  try { %>
<h2>Table Attributes</h2>
<table>
  <tr>
      <th>Attribute Name</th>
      <th>Value</th>
      <th>Description</th></tr>
  <tr>
      <td>Enabled</td>
      <td><%= hbadmin.isTableEnabled(table.getTableName()) %></td>
      <td>Is the table enabled</td>
  </tr>
<%  if (showFragmentation) { %>
  <tr>
      <td>Fragmentation</td>
      <td><%= frags.get(tableName) != null ? frags.get(tableName).intValue() + "%" : "n/a" %></td>
      <td>How fragmented is the table. After a major compaction it is 0%.</td>
  </tr>
<%  } %>
</table>
<%
  Map<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();
  if(regions != null && regions.size() > 0) { %>
<%=     tableHeader %>
<%
  for(Map.Entry<HRegionInfo, HServerAddress> hriEntry : regions.entrySet()) {
    int infoPort = master.getServerManager().getHServerInfo(hriEntry.getValue()).getInfoPort();
    String urlRegionServer =
        "http://" + hriEntry.getValue().getHostname().toString() + ":" + infoPort + "/";
%>
<tr>
  <td><%= Bytes.toStringBinary(hriEntry.getKey().getRegionName())%></td>
  <td><a href="<%= urlRegionServer %>"><%= hriEntry.getValue().getHostname().toString() + ":" + infoPort %></a></td>
  <td><%= hriEntry.getKey().getEncodedName()%></td> <td><%= Bytes.toStringBinary(hriEntry.getKey().getStartKey())%></td>
  <td><%= Bytes.toStringBinary(hriEntry.getKey().getEndKey())%></td>
</tr>
<% } %>
</table>
<% }
} catch(Exception ex) {
  ex.printStackTrace();
}
} // end else
%>

<p><hr><p>
Actions:
<p>
<center>
<table style="border-style: none" width="90%">
<tr>
  <form method="get">
  <input type="hidden" name="action" value="compact">
  <input type="hidden" name="name" value="<%= tableName %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Compact"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (optional):<input type="text" name="key" size="40"></td>
  <td style="border-style: none">This action will force a compaction of all
  regions of the table, or, if a key is supplied, only the region containing the
  given key.</td>
  </form>
</tr>
<tr><td style="border-style: none" colspan="4">&nbsp;</td></tr>
<tr>
  <form method="get">
  <input type="hidden" name="action" value="split">
  <input type="hidden" name="name" value="<%= tableName %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Split"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (optional):<input type="text" name="key" size="40"></td>
  <td style="border-style: none">This action will force a split of all eligible
  regions of the table, or, if a key is supplied, only the region containing the
  given key. An eligible region is one that does not contain any references to
  other regions. Split requests for noneligible regions will be ignored.</td>
  </form>
</tr>
</table>
</center>
<p>

<%
}
%>

</body>
</html>
