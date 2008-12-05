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
<%@ page import = "java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.extraction.engine.*, org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<% String filter=(String)session.getAttribute("filter");
   if(filter==null) {
       filter="";
   } %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta content="text/html; charset=UTF-8" http-equiv="Content-Type"/>
<link href="/hicc/css/flexigrid/flexigrid.css" rel="stylesheet" type="text/css"/>
<script type="text/javascript" src="/hicc/js/jquery-1.2.6.min.js"></script>
<script type="text/javascript" src="/hicc/js/flexigrid.js"></script>
</head>
<body>
<div class="flexigrid">
<table class="flexme1">
	<thead>
    		<tr>
            	<th width="100%">Event</th>
            </tr>
    </thead>
<%
        String[] database = request.getParameterValues("database");
%>
    <tbody>
    </tbody>
</table>
</div>
<script type="text/javascript">
$('.flexme1').flexigrid(
			{
                        url: '/hicc/jsp/event_viewer_data.jsp?<% for(int i=0;i<database.length;i++) { out.print("database="+database[i]+"&"); } %>',
                        dataType: 'json',
			searchitems : [
				{display: 'Event', name : 'event', isdefault: true}
				],
			sortorder: "asc",
			usepager: true,
			useRp: true,
			rp: 15,
                        striped:false,
			showTableToggleBtn: true,
			width: 'auto',
			height: 300
			}
);
</script>
</body>
</html>
