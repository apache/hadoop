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
<%@ page import = "java.util.Hashtable, java.util.Enumeration, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.TimeHandler, java.text.NumberFormat" %>
<% String boxId = request.getParameter("boxId"); 
   TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone")); %>
Time Period 
<select id="<%= boxId %>period" name="<%= boxId %>time_period" class="formSelect">
<%
   String period = (String) session.getAttribute("period");
   String[][] periodMap = new String[8][2];
   periodMap[0][0]="last1hr";
   periodMap[0][1]="Last 1 Hour";
   periodMap[1][0]="last2hr";
   periodMap[1][1]="Last 2 Hours";
   periodMap[2][0]="last3hr";
   periodMap[2][1]="Last 3 Hours";
   periodMap[3][0]="last6hr";
   periodMap[3][1]="Last 6 Hours";
   periodMap[4][0]="last12hr";
   periodMap[4][1]="Last 12 Hours";
   periodMap[5][0]="last24hr";
   periodMap[5][1]="Last 24 Hours";
   periodMap[6][0]="last7d";
   periodMap[6][1]="Last 7 Days";
   periodMap[7][0]="last30d";
   periodMap[7][1]="Last 30 Days";
   for (int i=0;i<periodMap.length;i++) {
       String meta = "";
       if(period!=null && period.equals(periodMap[i][0])) {
           meta = "selected";
       }
       out.println("<option value='"+periodMap[i][0]+"' "+meta+">"+periodMap[i][1]+"</option>");
   }
 %>
</select>
<input type="button" name="<%= boxId %>apply" value="Apply" onclick="save_time_range('<%= boxId %>')" class="formButton">
