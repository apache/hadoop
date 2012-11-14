
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
<%@ page contentType="text/html; charset=UTF-8"
	import="org.apache.hadoop.util.ServletUtil"
	import="org.apache.hadoop.fs.FileStatus"
	import="org.apache.hadoop.fs.FileUtil"
	import="org.apache.hadoop.fs.Path"
	import="java.util.Collection"
	import="java.util.Arrays" %>
<%!//for java.io.Serializable
  private static final long serialVersionUID = 1L;%>
<%
  NameNode nn = (NameNode) application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  // String namenodeRole = nn.getRole().toString();
  String namenodeLabel = nn.getNameNodeAddress().getHostName() + ":"
      + nn.getNameNodeAddress().getPort();
  Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks = 
	fsn.listCorruptFileBlocks();
  int corruptFileCount = corruptFileBlocks.size();

%>

<html>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop <%=namenodeLabel%></title>
<body>
<h1>'<%=namenodeLabel%>'</h1>
<%=JspHelper.getVersionTable(fsn)%>
<br>
<b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b>
<br>
<b><a href="/logs/"><%=namenodeLabel%> Logs</a></b>
<br>
<b><a href=/dfshealth.jsp> Go back to DFS home</a></b>
<hr>
<h3>Reported Corrupt Files</h3>
<%
  if (corruptFileCount == 0) {
%>
    <i>No missing blocks found at the moment.</i> <br>
    Please run fsck for a thorough health analysis.
<%
  } else {
    for (FSNamesystem.CorruptFileBlockInfo c : corruptFileBlocks) {
      String currentFileBlock = c.toString();
%>
      <%=currentFileBlock%><br>
<%
    }
%>
    <p>
      <b>Total:</b> At least <%=corruptFileCount%> corrupt file(s)
    </p>
<%
  }
%>

<%
  out.println(ServletUtil.htmlFooter());
%>
