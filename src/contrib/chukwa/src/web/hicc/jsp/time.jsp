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
<% 
   if(request.getParameter("time_zone")!=null) {
       session.setAttribute("time_zone",request.getParameter("time_zone"));
   }
   String timeType="combo";
   if(request.getParameter("time_type")!=null) {
       timeType=request.getParameter("time_type");
   }
   if(((String)request.getHeader("user-agent")).indexOf("iPhone")>0) {
%>
       <jsp:include page="workspace/time_iphone.jsp" flush="true" />
<%
   } else if(timeType.equals("date")) {
%>
       <jsp:include page="time_frame.jsp" flush="true" />
<%
   } else if(timeType.equals("range")) {
%>
       <jsp:include page="time_range.jsp" flush="true" />
<%
   } else if(timeType.equals("slider")) {
%>
       <jsp:include page="time_slider_wrapper.jsp" flush="true" />
<%
   } %>
