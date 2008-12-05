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
<%@ page import = "java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.dao.*, org.apache.hadoop.chukwa.dao.database.*, org.apache.hadoop.chukwa.dao.hdfs.*" %>
<% String filter=(String)session.getAttribute("filter");
   if(filter==null) {
       filter="";
   } %>
<div style="height:300px;overflow:auto;">
Filter: <input type="text" id="<%= request.getParameter("boxId") %>filter" name="<%= request.getParameter("boxId") %>filter" value="<%= filter %>" class="formInput">
<input type="button" name="apply_filter" value="Filter" onClick="filter_event_viewer('<%= request.getParameter("boxId") %>');" class="formButton">
<table class="simple" width="100%">
<tr>
<th>Time</th>
<th>Event</th>
</tr>
<%
        String cluster = (String) session.getAttribute("cluster");
        ClusterConfig cc = new ClusterConfig();
        String jdbc = cc.getURL(cluster);
        String boxId=request.getParameter("boxId");
        TimeHandler time = new TimeHandler(request);
        String startdate = time.getStartTimeText();
        String enddate = time.getEndTimeText();
	String[] timefield = new String[3];
	String[] database = new String[3];
	database[0]="MRJob";
	database[1]="HodJob";
	database[2]="QueueInfo";
	timefield[0]="LAUNCH_TIME";
	timefield[1]="StartTime";
	timefield[2]="timestamp";
	long startDate = time.getStartTime();
	long endDate = time.getEndTime();

	ChukwaSearchService se = new ChukwaSearchService();
        String cluster = (String) session.getAttribute("cluster");
        DataSourceResult result = se.search(cluster,database,startDate,endDate,filter);
	TreeMap events = result.getEvents();
	
        Iterator ei = (events.keySet()).iterator();
        while(ei.hasNext()) {
	        long time = (Long) ei.next();
                List<Event> tEvents = (List) events.get(time);
	        SimpleDateFormat sdf=  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			for(Event event : tEvents)
			{       
            String cell = event.toString();
                out.println("<tr><td>"+ sdf.format(event.getTime())+"</td><td>"+cell+"</td></tr>");
			}
		 }
%>
</table>
</div>
