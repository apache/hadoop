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
<% TimeHandler time = new TimeHandler(request,(String)session.getAttribute("time_zone"));
   String startDate = time.getStartDate();
   String endDate = time.getEndDate();
   SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
   long start = formatter.parse(startDate).getTime(); 
   long end = formatter.parse(endDate).getTime(); 
   end = end + (60*60*24*1000-1);
   String label = "";
   String label2 = "";
   String label3 = "";
   if(end-start>(60*60*24*1000*2)) {
      label = formatter.format(start);
      long mid = (end+start)/2;
      label2 = formatter.format(mid);
      label3 = formatter.format(end); 
   } else {
      SimpleDateFormat formatter2 = new SimpleDateFormat("HH:mm");
      label = formatter2.format(start);
      long mid = (end+start)/2;
      label2 = formatter2.format(mid);
      label3 = formatter2.format(end); 
   }
%>
<html><head>
<script type='text/javascript' src='/hicc/js/workspace/prototype.js'></script>
<script type='text/javascript' src='/hicc/js/workspace/scriptaculous.js'></script>
<link href="/hicc/css/default.css" rel="stylesheet" type="text/css">
<style type="text/css">
  div.slider3 { width:96%; margin:0px; height:15px; position: relative; opacity: 0.6; z-index:5; white-space: nowrap;}
  div.slider { width:96%; margin:0px; height:10px; position: relative; opacity: 0.6; z-index:5; white-space:nowrap;}
  div.slider2 { width:96%; margin:0px; background-color:#ccc; height:4px; position: relative; opacity: 0.4; z-index:5; white-space:nowrap;}
  div.handle { cursor:move; position: absolute; z-index:10; opacity: 1.0; }
  div.label { display: inline-block; text-align: center; position:absolute; z-index:4;}
  span.label { text-align: center; }
</style>
</head>
<body>
  <table class="clear_table"><tr><td align="right">
  Start Time</td><td>&nbsp;
  <div id="start_time" style="display:inline;white-space: nowrap;"></div></td></tr>
  <tr><td align="right">End Time</td><td>&nbsp;
  <div id="end_time" style="display:inline;white-space: nowrap;"></div></td></tr>
  </table>
  <div id="time_slider" class="slider3">
    <div class="handle" style="width:19px; height:30px;"><img src="/hicc/images/slider-images-handle.png" alt="" style="float: left;" /></div>
    <div class="handle" style="width:19px; height:30px;"><img src="/hicc/images/slider-images-handle.png" alt="" style="float: left;" /></div>
  </div>
  <div class="slider2"></div>
  <div id="range_slider" class="slider">
    <div class="handle" style="width:19px; height:20px;"><img src="/hicc/images/slider-images-handle2.png" alt="" style="float: left;"/></div>
  </div>
  <div style="width:100%; height: 1em; font-size: 9px; color: gray;">
  <div class="label" style="left:0%;"><span class="label"><%=label%></span></div>
  <div class="label" style="left:45%;"><span class="label"><%=label2%></span></div>
  <div class="label" style="right:3%;"><span class="label"><%=label3%></span></div>
  </div>
<div id="hidden_start" style="display:none;"></div>
<div id="hidden_end" style="display:none;"></div>
<script type="text/javascript">
    var time_slider = $('time_slider');
    var range_slider = $('range_slider');
    var mid=(<%= time.getStartTime() %>+<%= time.getEndTime() %>)/2;
    var slider = new Control.Slider(time_slider.select('.handle'), time_slider, {
      range: $R(<%= start %>, <%= end %>),
      sliderValue: [<%= time.getStartTime() %>, <%= time.getEndTime() %>],
      restricted: true,
      onSlide: function(values) {
          display_dates(values[0],values[1]);
      },
      onChange: function(values) {
          display_dates(values[0],values[1]);
      }
    });
    var slider_range = new Control.Slider(range_slider.select('.handle'), range_slider, {
                  range: $R(<%= start %>, <%= end %>),
                  sliderValue: mid,
                  onSlide: function(values) {
                               var delta = values-mid;
                               mid = values;
                               slider.setValue(slider.values[0]+delta,0);
                               slider.setValue(slider.values[1]+delta,1);
                               display_dates(slider.values[0]+delta,slider.values[1]+delta);
                  },
                  onChange: function(values) {
                  }
    });

function display_dates(start, end) {
    var s = new Date();
    s.setTime(start);
    var sm = s.getUTCMonth();
    sm = sm + 1;
    if(sm<10) {
        sm = "0" + sm;
    }
    var sd = s.getUTCDate();
    if(sd<10) {
        sd = "0" + sd;
    }
    var sh = s.getUTCHours();
    if(sh<10) {
        sh = "0" + sh;
    }
    var smin = s.getUTCMinutes();
    if(smin<10) {
        smin = "0" + smin;
    }
    var e = new Date();
    e.setTime(end);
    var em = e.getUTCMonth();
    em = em + 1;
    if(em<10) {
        em = "0" + em;
    }
    var ed = e.getUTCDate();
    if(ed<10) {
        ed = "0" + ed;
    }
    var eh = e.getUTCHours();
    if(eh<10) {
        eh = "0" + eh;
    }
    var emin = e.getUTCMinutes();
    if(emin<10) {
        emin = "0" + emin;
    }
    $('start_time').innerHTML=s.getUTCFullYear()+"-"+sm+"-"+sd+" "+sh+":"+smin;
    $('end_time').innerHTML=e.getUTCFullYear()+"-"+em+"-"+ed+" "+eh+":"+emin;
    $('hidden_start').innerHTML=start;
    $('hidden_end').innerHTML=end;
}

display_dates(<%= time.getStartTime() %>,<%= time.getEndTime() %>);
</script>
</body></html>
