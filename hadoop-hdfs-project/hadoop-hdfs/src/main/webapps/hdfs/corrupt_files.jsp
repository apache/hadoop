
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
	import="org.apache.hadoop.ha.HAServiceProtocol.HAServiceState"
	import="java.util.Collection"
	import="java.util.Collections"
	import="java.util.Arrays" %>
<%!//for java.io.Serializable
  private static final long serialVersionUID = 1L;%>
<%
  NameNode nn = NameNodeHttpServer.getNameNodeFromContext(application);
  FSNamesystem fsn = nn.getNamesystem();
  HAServiceState nnHAState = nn.getServiceState();
  boolean isActive = (nnHAState == HAServiceState.ACTIVE);
  String namenodeRole = nn.getRole().toString();
  String namenodeLabel = NamenodeJspHelper.getNameNodeLabel(nn);
  Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks = fsn != null ?
    fsn.listCorruptFileBlocks("/", null) :
    Collections.<FSNamesystem.CorruptFileBlockInfo>emptyList();
  int corruptFileCount = corruptFileBlocks.size();
%>

<!DOCTYPE html>
<html>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop <%=namenodeRole%>&nbsp;<%=namenodeLabel%></title>
<body>
<h1><%=namenodeRole%> '<%=namenodeLabel%>'</h1>
<%=NamenodeJspHelper.getVersionTable(fsn)%>
<br>
<% if (isActive && fsn != null) { %> 
  <b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b>
  <br>
<% } %> 
<b><a href="/logs/"><%=namenodeRole%> Logs</a></b>
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
