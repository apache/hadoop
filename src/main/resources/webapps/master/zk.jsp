<%@ page contentType="text/html;charset=UTF-8"
  import="java.io.IOException"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.client.HConnection"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.master.HMaster" 
  import="org.apache.hadoop.hbase.HConstants"%><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  HBaseAdmin hbadmin = new HBaseAdmin(conf);
  HConnection connection = hbadmin.getConnection();
  ZooKeeperWrapper wrapper = connection.getZooKeeperWrapper();
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>ZooKeeper Dump</title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
<a id="logo" href="http://hbase.org"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">ZooKeeper Dump</h1>
<p id="links_menu"><a href="/master.jsp">Master</a>, <a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a></p>
<hr id="head_rule" />
<pre>
<%= wrapper.dump() %>
</pre>

</body>
</html>
