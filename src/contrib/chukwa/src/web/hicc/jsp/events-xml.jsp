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
%><?xml version="1.0" encoding="UTF-8"?>
<%@ page import = "java.util.Calendar, java.util.Date, java.sql.*, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.database.DatabaseConfig" %>
<%
    response.setContentType("text/xml");
    TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
    String cluster = (String) session.getAttribute("cluster");
    String startS = time.getStartTimeText();
    String endS = time.getEndTimeText();
    DatabaseConfig dbc = new DatabaseConfig();
    String[] database = dbc.findTableName("HodJob",time.getStartTime(),time.getEndTime());
    String[] timefield = new String[3];
    timefield[0]="StartTime";
    //timefield[1]="LAUNCH_TIME";
    //timefield[2]="timestamp";
    ArrayList<HashMap<String, Object>> events = new ArrayList<HashMap<String, Object>>();
    int q=0;  

        String dateclause = timefield[q]+" >= '"+startS+"' and "+timefield[q]+" <= '"+endS+"'";
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

               ClusterConfig cc = new ClusterConfig();
               String jdbc = cc.getURL(cluster);
               try {
                   conn = DriverManager.getConnection(jdbc);
                   stmt = conn.createStatement();
                   String query = "";
                   query = "select * from "+database[q]+" where "+dateclause+";";
                   // or alternatively, if you don't know ahead of time that
                   // the query will be a SELECT...
                   if (stmt.execute(query)) {
                       rs = stmt.getResultSet();
                       ResultSetMetaData rmeta = rs.getMetaData();
                       int col = rmeta.getColumnCount();
                       while (rs.next()) {
                           String cell="";
                           HashMap<String, Object> event = new HashMap<String, Object>();
                           long event_time=0;
                           for(int i=1;i<col;i++) {
                               String value = rs.getString(i);
                               if(value!=null) {
                                   cell=cell+" "+rmeta.getColumnName(i)+":"+value;
                               }
                               event.put(rmeta.getColumnName(i),value);
                               if(rmeta.getColumnName(i).equals("EndTime")) {
                                   try {
                                       event.put(rmeta.getColumnName(i), rs.getTimestamp(i).getTime());
                                   } catch(SQLException ex) {
                                       Calendar now = Calendar.getInstance();
                                       event.put(rmeta.getColumnName(i),now.getTime());
                                   }
                               }
                               if(rmeta.getColumnName(i).equals("LAUNCH_TIME")) {
                                   try {
                                       event.put(rmeta.getColumnName(i), rs.getTimestamp(i).getTime());
                                   } catch(SQLException ex) {
                                       Calendar now = Calendar.getInstance();
                                       event.put(rmeta.getColumnName(i),now.getTime());
                                   }
                               }
                               if(rmeta.getColumnName(i).equals("StartTime")) {
                                   try {
                                       event.put(rmeta.getColumnName(i), rs.getTimestamp(i).getTime());
                                   } catch(SQLException ex) {
                                       Calendar now = Calendar.getInstance();
                                       event.put(rmeta.getColumnName(i),now.getTime());
                                   }
                               }
                               if(rmeta.getColumnName(i).equals("Timestamp")) {
                                   try {
                                       event.put(rmeta.getColumnName(i), rs.getTimestamp(i).getTime());
                                   } catch(SQLException ex) {
                                       Calendar now = Calendar.getInstance();
                                       event.put(rmeta.getColumnName(i),now.getTime());
                                   }
                               }
                           }
                           event.put("_event",cell);
                           events.add(event);
                       }
                   }
                   // Now do something with the ResultSet ....
               } catch (SQLException ex) {
                   // handle any errors
                   //out.println("SQLException: " + ex.getMessage());
                   //out.println("SQLState: " + ex.getSQLState());
                   //out.println("VendorError: " + ex.getErrorCode());
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
//        }
%>
<data>
<%
        SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
        for(int i=0;i<events.size();i++) {
            HashMap<String, Object> event = events.get(i);
            long start=(Long)event.get("StartTime");
            long end=(Long)event.get("EndTime");
            String event_time = format.format(start);
            String event_end_time = format.format(end);
            String cell = (String) event.get("_event");
%>
	    <event start="<%= event_time %> GMT" end="<%= event_end_time %> GMT" title="Hod Job: <%= event.get("HodID") %> User: <%= event.get("UserID") %>" link="" isDuration="true"><%= cell %></event>
<%
        } %>
</data>
