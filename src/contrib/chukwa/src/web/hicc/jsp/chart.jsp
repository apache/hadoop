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
<%
   response.setHeader("boxId", request.getParameter("boxId"));
%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<div style="height:250px">
<%
if(request.getParameter("period")==null && ((request.getParameter("start")==null || request.getParameter("end")==null))) {
  out.println("<h2>Select a time range, and host list.</h2>");
} else {
String base = "";
int width = 500;
int height = 250;
String dataurl = request.getRequestURL().toString();
//
// escape the & and stuff:
//
String url = java.net.URLEncoder.encode(dataurl.replace("/chart.jsp", "/chart-data.jsp?"+request.getQueryString()), "UTF-8");

//
// if there are more than one charts on the
// page, give each a different ID
//
//int open_flash_chart_seqno;
String obj_id = "chart";
//String div_name = "flashcontent";
// These values are for when there is >1 chart per page
//    $open_flash_chart_seqno++;
//    $obj_id .= '_<%=open_flash_chart_seqno;
//    $div_name .= '_<%=open_flash_chart_seqno;
// Not using swfobject: <script type="text/javascript" src="< % = base % >js/swfobject.js"></script>
%>
<object classid="clsid:d27cdb6e-ae6d-11cf-96b8-444553540000" 
	codebase="<%= request.getProtocol() %>://fpdownload.macromedia.com/pub/shockwave/cabs/flash/swflash.cab#version=8,0,0,0"
	width="100%" height="100%" id="ie_<%=obj_id %>" align="middle" style="z-index: 1;">
  <param name="allowScriptAccess" value="sameDomain" />
  <param name="movie" value="<%=base%>open-flash-chart.swf?width=<%=width%>&height=<%=height%>&data=<%=url%>" />
  <param name="quality" value="high" />
  <param name="bgcolor" value="#ffffff" />
  <param name="wmode" value="transparent" />
  <embed src="<%=base%>open-flash-chart.swf?data=<%=url%>&metric=CPUBusy" 
    quality="high" bgcolor="#ffffff" 
  	width="100%" height="100%" name="<%=obj_id%>" align="middle" allowScriptAccess="sameDomain"
	type="application/x-shockwave-flash" 
	pluginspage="<%=request.getProtocol()%>://www.macromedia.com/go/getflashplayer" 
	id="<%=obj_id%>"
	/>
</object>
<% } %>
</div>
