<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.util.*"
%>

<html>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop SecondaryNameNode</title>
    
<body>
<h1>SecondaryNameNode</h1>
<%= JspHelper.getVersionTable() %>
<hr />
<pre>
<%= application.getAttribute("secondary.name.node").toString() %>
</pre>

<br />
<b><a href="/logs/">Logs</a></b>
<%= ServletUtil.htmlFooter() %>
