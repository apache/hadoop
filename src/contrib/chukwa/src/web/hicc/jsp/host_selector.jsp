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
<%@ page import = "javax.servlet.http.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.database.DatabaseConfig"  %>
<% String boxId = request.getParameter("boxId"); %>
<div class="panel">
<h2>Hosts</h2>
<fieldset>
<div class="row">
<select id="<%= boxId %>group_items" name="<%= boxId %>group_items" MULTIPLE size=10 class="formSelect" style="width:200px;">
<%
    String[] machineNames = (String [])session.getAttribute("machine_names");
    String cluster=request.getParameter("cluster");
    if(cluster!=null && !cluster.equals("null")) {
        session.setAttribute("cluster",cluster);
    } else {
        cluster = (String) session.getAttribute("cluster");
        if(cluster==null || cluster.equals("null")) {
            cluster="demo";
            session.setAttribute("cluster",cluster);
        }
    }
    ClusterConfig cc = new ClusterConfig();
    String jdbc = cc.getURL(cluster);
    TimeHandler time = new TimeHandler(request,(String)session.getAttribute("time_zone"));
    String startS = time.getStartTimeText();
    String endS = time.getEndTimeText();
    String timefield = "timestamp";
    String dateclause = timefield+" >= '"+startS+"' and "+timefield+" <= '"+endS+"'";
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
        HashMap<String, String> hosts = new HashMap<String, String>();
        try {
            String[] selected_hosts = ((String)session.getAttribute("hosts")).split(",");
            for(String name: selected_hosts) {
                hosts.put(name,name);
            }
        } catch (NullPointerException e) {
    }
           conn = DriverManager.getConnection(jdbc);
           stmt = conn.createStatement();
           String query = "";
           String HodID = (String)session.getAttribute("HodID");
           if(HodID!=null && !HodID.equals("null") && !HodID.equals("")) {
               query = "select DISTINCT Machine from HodMachine where HodID='"+HodID+"' order by Machine;";
           } else if(machineNames==null) {
               long start = time.getStartTime();
               long end = time.getEndTime(); 
               String table = "system_metrics";
               DatabaseConfig dbc = new DatabaseConfig();
               String[] tables = dbc.findTableName(table, start, end);
               table=tables[0];
               query="select DISTINCT host from "+table+" order by host";
           }
           // or alternatively, if you don't know ahead of time that
           // the query will be a SELECT...
           if(!query.equals("")) {
               if (stmt.execute(query)) {
                   int i=0;
                   rs = stmt.getResultSet();
                   rs.last();
                   int size = rs.getRow();
                   machineNames = new String[size];
                   rs.beforeFirst();
                   while (rs.next()) {
                       String machine = rs.getString(1);
                       machineNames[i]=machine;
                       if(hosts.containsKey(machine)) {
                           out.println("<option selected>"+machine+"</option>");
                       } else {
                           out.println("<option>"+machine+"</option>");
                       }
                       i++;
                   }
                   if(HodID==null || HodID.equals("null") || HodID.equals("")) {
                       session.setAttribute("machine_names",machineNames);
                   }
               }
           } else {
                   for(String machine : machineNames) {
                       if(hosts.containsKey(machine)) {
                           out.println("<option selected>"+machine+"</option>");
                       } else {
                           out.println("<option>"+machine+"</option>");
                       }
                   }
           }
           // Now do something with the ResultSet ....
       } catch (SQLException ex) {
           // handle any errors
           out.println("SQLException: " + ex.getMessage());
           out.println("SQLState: " + ex.getSQLState());
           out.println("VendorError: " + ex.getErrorCode());
       } finally {
           // it is a good idea to release
           // resources in a finally{} block
           // in reverse-order of their creation
           // if they are no-longer needed
           if (rs != null) {
               try {
                   rs.close();
               } catch (SQLException sqlEx) {
                   // ignore
               }
               rs = null;
           }
           if (stmt != null) {
               try {
                   stmt.close();
               } catch (SQLException sqlEx) {
                   // ignore
               }
               stmt = null;
           }
           if (conn != null) {
               try {
                   conn.close();
               } catch (SQLException sqlEx) {
                   // ignore
               }
               conn = null;
           }
       }
%>
</select></div>
<div class="row">
<input type="button" onClick="save_host('<%= boxId %>');" name="Apply" value="Apply" class="formButton">
</div>
</fieldset>
</div>
