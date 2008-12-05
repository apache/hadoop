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
   TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
%>
<table width="100%" class="clear_table">
<tr><td align="center">
<a href="#" onclick="return false;" id="<%= boxId %>previous"><img src="/hicc/images/arrow_left.png" border="0"></a>
</td><td align="center">
<a href="#" onclick="return false;" id="<%= boxId %>today">Today</a>
</td><td align="center">
<a href="#" onclick="return false;" id="<%= boxId %>next"><img src="/hicc/images/arrow_right.png" border="0"></a>
</td></tr>
<tr><td colspan="3" align="center">
<div id="<%= boxId %>calendars" class="timeframe_calendar"></div>
</td></tr>
<tr><td colspan="3" align="center">
<span>
  <input type="text" name="<%= boxId %>start" value="<%= time.getStartDate("MMMMM dd, yyyy") %>" id="<%= boxId %>start" class="formInput" size="10"/>
  &ndash;
  <input type="text" name="<%= boxId %>end" value="<%= time.getEndDate("MMMMM dd, yyyy") %>" id="<%= boxId %>end" class="formInput" size="10"/>
</span>
</td></tr>
</table>
<div style="display:none"><a href="#" onclick="return false;" id="<%= boxId %>reset">Reset</a></a></div>
<input type="button" name="<%=boxId%>apply" value="Apply" class="formButton" onclick="save_timeframe('<%= boxId %>');">
<script type="text/javascript">
      new Timeframe('<%= boxId %>calendars', {
        previousButton: "<%= boxId %>previous",
        todayButton: "<%= boxId %>today",
        nextButton: "<%= boxId %>next",
        resetButton: "<%= boxId %>reset",
        months: 1,
        format: '%Y-%m-%d',
        startField: '<%= boxId %>start',
        endField: '<%= boxId %>end'
        }
        );
</script>
