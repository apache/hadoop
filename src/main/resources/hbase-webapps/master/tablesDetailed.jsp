<%--
/**
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
--%>
<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.HTableDescriptor" %><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
%>
<?xml version="1.0" encoding="UTF-8" ?>
<!-- Commenting out DOCTYPE so our blue outline shows on hadoop 0.20.205.0, etc.
     See tail of HBASE-2110 for explaination.
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
-->
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>HBase Master: <%= master.getServerName()%>%></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>

<h2>User Tables</h2>
<% HTableDescriptor[] tables = new HBaseAdmin(conf).listTables();
   if(tables != null && tables.length > 0) { %>
<table>
<tr>
    <th>Table</th>
    <th>Description</th>
</tr>
<%   for(HTableDescriptor htDesc : tables ) { %>
<tr>
    <td><%= htDesc.getNameAsString() %></td>
    <td><%= htDesc.toString() %></td>
</tr>
<%   }  %>

<p> <%= tables.length %> table(s) in set.</p>
</table>
<% } %>
</body>
</html>
