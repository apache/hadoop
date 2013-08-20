<%
/*
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
%>
<%@page import="org.apache.hadoop.hdfs.tools.GetConf"%>
<%@page import="org.apache.hadoop.hdfs.server.datanode.DatanodeJspHelper"%>
<%@page import="org.apache.hadoop.hdfs.server.datanode.DataNode"%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.util.ServletUtil"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;
%>
<%  
  DataNode dataNode = (DataNode)getServletContext().getAttribute("datanode"); 
  String state = dataNode.isDatanodeUp()?"active":"inactive";
  String dataNodeLabel = dataNode.getDisplayName();
%>

<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop DataNode&nbsp;<%=dataNodeLabel%></title>
</head>    
<body>
<h1>DataNode '<%=dataNodeLabel%>' (<%=state%>)</h1>
<%= DatanodeJspHelper.getVersionTable(getServletContext()) %>
<br />
<b><a href="/logs/">DataNode Logs</a></b>
<br />
<b><a href="/logLevel">View/Set Log Level</a></b>
<br />
<b><a href="/metrics">Metrics</a></b>
<br />
<b><a href="/conf">Configuration</a></b>
<br />
<b><a href="/blockScannerReport">Block Scanner Report</a></b>
<%
out.println(ServletUtil.htmlFooter());
%>
