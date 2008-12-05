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
   session.setAttribute("time_type","range");
   TimeHandler time = new TimeHandler(request,(String)session.getAttribute("time_zone"));
%>
<div class="panel">
<h2>Date/Time</h2>
<fieldset>
<div class="row">
    <label>Start Date</label>
<input type=text id="<%= boxId %>start" name="<%= boxId %>start" value="<%= time.getStartDate() %>" size="10"/>
</div>
<div class="row">
    <label>Start Time</label>
<select id="<%= boxId %>start_hour" name="<%= boxId %>start_hour">
<%
   String startHour = time.getStartHour();
   for(int i=0;i < 24; i++) {
       String meta = "";
       String hour = ""+i;
       if(i<10) {
           hour = "0"+i;
       }
       if(startHour.equals(hour)) {
           meta = "selected";
       }
       out.println("<option value='"+hour+"' "+meta+">"+hour+"</option>");
   } 
%>
</select> : 
<select id="<%= boxId %>start_min" name="<%= boxId %>start_min">
<%
   String startMin = time.getStartMinute();
   for(int i=0;i < 60; i++) {
       String meta = "";
       String minute = ""+i;
       if(i<10) {
           minute = "0"+i;
       }
       if(startMin.equals(minute)) {
           meta = "selected";
       }
       out.println("<option value='"+minute+"' "+meta+">"+minute+"</option>");
   } 
%>
</select>
</div>
<div class="row">
    <label>End Date</label>
<input type=text id="<%= boxId %>end" name="<%= boxId %>end" value="<%= time.getEndDate() %>" size="10"/>
</div>
<div class="row">
    <label>End Time</label>
<select id="<%= boxId %>end_hour" name="<%= boxId %>end_hour">
<%
   String endHour = time.getEndHour();
   for(int i=0;i < 24; i++) {
       String meta = "";
       String hour = ""+i;
       if(i<10) {
           hour = "0"+i;
       }
       if(endHour.equals(hour)) {
           meta = "selected";
       }
       out.println("<option value='"+hour+"' "+meta+">"+hour+"</option>");
   } 
%>
</select> : 
<select id="<%= boxId %>end_min" name="<%= boxId %>end_min">
<%
   String endMin = time.getEndMinute();
   for(int i=0;i < 60; i++) {
       String meta = "";
       String minute = ""+i;
       if(i<10) {
           minute = "0"+i;
       }
       if(endMin.equals(minute)) {
           meta = "selected";
       }
       out.println("<option value='"+minute+"' "+meta+">"+minute+"</option>");
   } 
%>
</select>
</div>
<div class="row">
<input type="button" name="apply" value="Apply" onclick="save_time('<%= boxId %>');" class="formButton">
</div>
</fieldset>
</div>
