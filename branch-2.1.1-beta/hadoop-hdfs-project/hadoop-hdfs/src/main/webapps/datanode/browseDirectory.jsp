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
  import="java.io.IOException"

  import="org.apache.hadoop.hdfs.server.common.JspHelper"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.conf.Configuration"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;
%>
<!DOCTYPE html>
<html>
<head>
<style type=text/css>
<!--
body 
  {
  font-face:sanserif;
  }
-->
</style>
<%JspHelper.createTitle(out, request, request.getParameter("dir")); %>
</head>

<body onload="document.goto.dir.focus()">
<% 
  try {
    Configuration conf = 
      (Configuration) application.getAttribute(JspHelper.CURRENT_CONF);
    DatanodeJspHelper.generateDirectoryStructure(out,request,response, conf);
  }
  catch(IOException ioe) {
    String msg = ioe.getLocalizedMessage();
    int i = msg.indexOf("\n");
    if (i >= 0) {
      msg = msg.substring(0, i);
    }
    out.print("<h3>" + msg + "</h3>");
  }
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>
