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
<%@ page import = "java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*,org.jfree.chart.*, org.jfree.data.category.*, org.jfree.chart.servlet.*, org.jfree.chart.entity.*, org.jfree.chart.plot.*, org.jfree.chart.plot.*, org.jfree.chart.renderer.category.*, org.jfree.data.general.DatasetUtilities, org.jfree.chart.axis.*, org.apache.hadoop.chukwa.hicc.ClusterConfig" %>
<% if(session.getAttribute("HodID")==null || session.getAttribute("HodID").equals("")) { %>
<h2> Select a Hod Job from Hod Job List </h2>
<% } else { %>
<div style="height:300px;overflow:auto;">
<table class="simple">
<tr>
<th>Hod ID</th>
<th>Map Reduce Job ID</th>
<th>Map Reduce Job Name</th>
<th>Submit Time</th>
<th>Launch Time</th>
<th>Finish Time</th>
<th>Mapper phase end time</th>
<th>Total maps</th>
<th>Number of Mapper attempts</th>
<th>Total Reduces</th>
<th>Number of reducer attempts</th>
<th>Mapper phase execution time</th>
</tr>
<%
       Connection conn = null;
       Statement stmt = null;
       ResultSet rs = null;
       String cluster = (String) session.getAttribute("cluster");
       if(cluster==null) {
           cluster="demo";
       }
       ClusterConfig cc = new ClusterConfig();
       String jdbc = cc.getURL(cluster);
       try {
           conn = DriverManager.getConnection(jdbc);
           stmt = conn.createStatement();
           String query = "";
           String HodID = (String)session.getAttribute("HodID");
           if(HodID!=null && !HodID.equals("")) {
               query = "select HodID,MRJobID,MRJobName,SUBMIT_TIME,LAUNCH_TIME,FINISH_TIME,MAPPER_PHASE_END_TIME, TOTAL_MAPS,NUM_OF_MAPPER_ATTEMPTS,TOTAL_REDUCES,NUM_OF_REDUCER_ATTEMPTS,MAPPER_PHASE_EXECUTION_TIME from MRJob where HodID='"+HodID+"';";
           }
           // or alternatively, if you don't know ahead of time that
           // the query will be a SELECT...
           if (stmt.execute(query)) {
               rs = stmt.getResultSet();
               while (rs.next()) { %>
<tr>
<td><%= rs.getString(1) %></td>
<td><%= rs.getString(2) %></td>
<td><%= rs.getString(3) %></td>
<td><%= rs.getString(4) %></td>
<td><%= rs.getString(5) %></td>
<td><%= rs.getString(6) %></td>
<td><%= rs.getString(7) %></td>
<td><%= rs.getString(8) %></td>
<td><%= rs.getString(9) %></td>
<td><%= rs.getString(10) %></td>
<td><%= rs.getString(11) %></td>
<td><%= rs.getString(12) %></td></tr>
<%
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
</table>
</div>
<% } %>
