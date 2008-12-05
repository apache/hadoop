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
<%@ page import = "java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<% TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
   long start = time.getStartTime();
   long end = time.getEndTime();
   SimpleDateFormat formatter = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
   String startDate = formatter.format(start);
   String endDate = formatter.format(end);
   String intervalUnit1="MINUTE";
   String intervalUnit2="HOUR";
   if(((end-start)/1000)>(15*60*60*24)) {
       intervalUnit1 = "DAY";
       intervalUnit2 = "WEEK";
   } else if(((end-start)/1000)>(60*60*24*3)) {
       intervalUnit1 = "HOUR";
       intervalUnit2 = "DAY";
   } else {
       intervalUnit1 = "MINUTE";
       intervalUnit2 = "HOUR";
   }
%>
<html>
  <head>
    <link rel='stylesheet' href='/hicc/css/timeline.css' type='text/css' />
    <script src="http://simile.mit.edu/timeline/api/timeline-api.js" type="text/javascript"></script>
    <script src="http://simile.mit.edu/timeline/examples/examples.js" type="text/javascript"></script>
    <script type="text/javascript">
        var theme = Timeline.ClassicTheme.create();
        theme.event.label.width = 250; // px
        theme.event.bubble.width = 250;
        theme.event.bubble.height = 200;
        function onLoad() {
          var eventSource = new Timeline.DefaultEventSource();
          var bandInfos = [
            Timeline.createBandInfo({
                eventSource:    eventSource,
                showEventText:  false,
                trackHeight:    0.5,
                trackGap:       0.2,
                date:           "<%= startDate %>  GMT",
                width:          "100%", 
                intervalUnit:   Timeline.DateTime.<%= intervalUnit2 %>, 
                intervalPixels: 200,
                theme: theme
            })
          ];
          bandInfos[0].highlight = true;
  
          tl = Timeline.create(document.getElementById("my-timeline"), bandInfos);
          Timeline.loadXML("events-xml.jsp", function(xml, url) { eventSource.loadXML(xml, url); });
          setupFilterHighlightControls(document.getElementById("controls"), tl, [0], theme);

        }
        var resizeTimerID = null;
        function onResize() {
            if (resizeTimerID == null) {
                resizeTimerID = window.setTimeout(function() {
                    resizeTimerID = null;
                    tl.layout();
                }, 500);
            }
        }
    </script>
  </head>
  <body onload="onLoad();" onresize="onResize();">
    <div id="my-timeline" style="height: 500px; border: 1px solid #aaa"></div>
    <div class="controls" id="controls">
    </div>
  </body>
</html>
