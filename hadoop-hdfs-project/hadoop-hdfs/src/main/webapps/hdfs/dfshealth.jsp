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
<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.ha.HAServiceProtocol.HAServiceState"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;
%>
<%
  final NamenodeJspHelper.HealthJsp healthjsp  = new NamenodeJspHelper.HealthJsp();
  NameNode nn = NameNodeHttpServer.getNameNodeFromContext(application);
  FSNamesystem fsn = nn.getNamesystem();
  HAServiceState nnHAState = nn.getServiceState();
  boolean isActive = (nnHAState == HAServiceState.ACTIVE);
  String namenodeRole = nn.getRole().toString();
  String namenodeState = nnHAState.toString();
  String namenodeLabel = nn.getNameNodeAddressHostPortString();
%>

<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop <%=namenodeRole%>&nbsp;<%=namenodeLabel%></title>
</head>    
<body>
<h1><%=namenodeRole%> '<%=namenodeLabel%>' (<%=namenodeState%>)</h1>
<%= NamenodeJspHelper.getVersionTable(fsn) %>
<br />
<% if (isActive) { %> 
  <b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b><br>
<% } %> 
<b><a href="/logs/"><%=namenodeRole%> Logs</a></b>

<hr>
<h3>Cluster Summary</h3>
<b> <%= NamenodeJspHelper.getSecurityModeText()%> </b>
<b> <%= NamenodeJspHelper.getSafeModeText(fsn)%> </b>
<b> <%= NamenodeJspHelper.getInodeLimitText(fsn)%> </b>
<%= NamenodeJspHelper.getCorruptFilesWarning(fsn)%>

<% healthjsp.generateHealthReport(out, nn, request); %>
<% healthjsp.generateJournalReport(out, nn, request); %>
<hr/>
<% healthjsp.generateConfReport(out, nn, request); %>
<hr>
<%
out.println(ServletUtil.htmlFooter());
%>
