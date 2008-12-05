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

function save_timeframe(obj) {
    var start = document.getElementById(obj+"start").value;
    var end = document.getElementById(obj+"end").value;
    var starts = start.split("-");
    var ends = end.split("-");
    var start_date=new Date();
    start_date.setFullYear(starts[0]);
    start_date.setMonth(starts[1]-1);
    start_date.setDate(starts[2]);
    var end_date=new Date();
    end_date.setFullYear(ends[0]);
    end_date.setMonth(ends[1]-1);
    end_date.setDate(ends[2]);
    start = start_date.getTime();
    end = end_date.getTime();
    var myAjax=new Ajax.Request(
       '/hicc/jsp/session.jsp',
       {
           asynchronous: false,
           method: "get",
           parameters: "time_type=range&start="+start+"&end="+end,
           onSuccess: function(transport) {    
                          _currentView.getCurrentPage().refresh_all();
                      }
       }
    );
}

function save_time(boxId) {
    var time_machine="range";
    var start=document.getElementById(boxId+"start").value;
    var end=document.getElementById(boxId+"end").value;
    var start_hour=document.getElementById(boxId+"start_hour").options[document.getElementById(boxId+"start_hour").selectedIndex].value;
    var start_min=document.getElementById(boxId+"start_min").options[document.getElementById(boxId+"start_min").selectedIndex].value;
    var end_hour=document.getElementById(boxId+"end_hour").options[document.getElementById(boxId+"end_hour").selectedIndex].value;
    var end_min=document.getElementById(boxId+"end_min").options[document.getElementById(boxId+"end_min").selectedIndex].value;
    var starts = start.split("-");
    var ends = end.split("-");
    var start_date=new Date();
    start_date.setUTCFullYear(starts[0]);
    start_date.setUTCMonth(starts[1]-1);
    start_date.setUTCDate(starts[2]);
    start_date.setUTCHours(start_hour);
    start_date.setUTCMinutes(start_min);
    var end_date=new Date();
    end_date.setUTCFullYear(ends[0]);
    end_date.setUTCMonth(ends[1]-1);
    end_date.setUTCDate(ends[2]);
    end_date.setUTCHours(end_hour);
    end_date.setUTCMinutes(end_min);
    start = start_date.getTime();
    end = end_date.getTime();
    
    if(end < start) {
        var tmp=start;
        start=end;
        end=tmp;
    }
    var myAjax=new Ajax.Request(
        '/hicc/jsp/session.jsp',
        {
            asynchronous: false,
            method: "get",
            parameters: "time_type="+time_machine+"&start="+start+"&end="+end
        }
    );
    _currentView.getCurrentPage().refresh_all();
}

function save_time_range(boxId) {
    var time_machine="last";
    var obj=document.getElementById(boxId+"period").options;
    var period=obj[document.getElementById(boxId+"period").selectedIndex].value;
    var myAjax=new Ajax.Request(
        '/hicc/jsp/session.jsp',
        {
            asynchronous: false,
            method: "get",
            parameters: "time_type="+time_machine+"&period="+period
        }
    );
    _currentView.getCurrentPage().refresh_all();
}

function save_time_slider(start, end) {
    start=Math.round(start);
    end=Math.round(end);
    var myAjax = new Ajax.Request(
         '/hicc/jsp/session.jsp',
         {
             method:'get',
             parameters: {
                 "time_type": "range",
                 "start": start,
                 "end": end,
             },
             onSuccess: function(request) {
                 _currentView.getCurrentPage().refresh_all();
             },
             onException: function(error) {
                 alert(error);
             }
         });
}

