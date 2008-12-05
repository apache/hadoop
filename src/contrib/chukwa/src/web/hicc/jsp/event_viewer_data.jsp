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
<%@ page import = "java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.extraction.engine.*, org.apache.hadoop.chukwa.hicc.TimeHandler, org.json.*" %>
<% String filter=(String)request.getParameter("query");
   if(filter==null) {
       filter="";
   }
    TimeHandler th = new TimeHandler(request, (String)session.getAttribute("time_zone"));
    long startDate = th.getStartTime();
    long endDate = th.getEndTime();

        Calendar now = Calendar.getInstance();
        long start = 0;
        long end = now.getTimeInMillis();
        String[] database = request.getParameterValues("database");
        String[] timefield = new String[3];
        timefield[0]="LAUNCH_TIME";
        timefield[1]="StartTime";
        timefield[2]="timestamp";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

        ChukwaSearchService se = new ChukwaSearchService();
        String cluster = (String) session.getAttribute("cluster");
        if (cluster == null)
            { cluster = "unknown"; }
        Token token = (Token) session.getAttribute(database+"_token");
        SearchResult result = se.search(cluster,database,startDate,endDate,filter,token);
        TreeMap events = result.getRecords();
        session.setAttribute(database+"_token",result.getToken());

        SimpleDateFormat sdf=  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        JSONArray array = new JSONArray();
        Iterator ei = (events.keySet()).iterator();
        while(ei.hasNext()) {
            long time = (Long) ei.next();
            List<Record> tEvents = (List) events.get(time);
            for(Record event : tEvents) {
                JSONObject eventRecord = new JSONObject();
                eventRecord.put("id",event.getTime());
                JSONArray cells = new JSONArray();
//                cells.put(sdf.format(event.getTime()));
                cells.put(event.toString());
                eventRecord.put("cell",cells);
                array.put(eventRecord);
            }
        }

        JSONObject hash = new JSONObject();
        hash.put("page","1");
        hash.put("total","1");
        hash.put("rows", array);
        out.println(hash.toString());
%>
