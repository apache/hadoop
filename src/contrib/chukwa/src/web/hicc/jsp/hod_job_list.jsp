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
<%@ page import = "java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*,org.jfree.chart.*, org.jfree.data.category.*, org.jfree.chart.servlet.*, org.jfree.chart.entity.*, org.jfree.chart.plot.*, org.jfree.chart.plot.*, org.jfree.chart.renderer.category.*, org.jfree.data.general.DatasetUtilities, org.jfree.chart.axis.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.database.DatabaseConfig"  %>
<table class="simple" width="100%">
<tr>
<th width="15%">Hod ID</th>
<th width="25%">User</th>
<th width="10%">Status</th>
<th width="10%">Time in Queue</th>
<th width="20%">Start Time</th>
<th>End Time</th></tr>
</table>
<div style="height:150px;overflow:auto;width:100%">
<table class="simple" width="100%">
<%
       TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
       String startS = time.getStartTimeText();
       String endS = time.getEndTimeText();
       String timefield = "StartTime";
       String dateclause = timefield+" >= '"+startS+"' and "+timefield+" <= '"+endS+"'";
       Connection conn = null;
       Statement stmt = null;
       ResultSet rs = null;
       String cluster = (String) session.getAttribute("cluster");
       if(cluster==null) {
           cluster="demo";
       }
       ClusterConfig cc = new ClusterConfig();
       String jdbc = cc.getURL(cluster);
       String hodID = (String)session.getAttribute("HodID");
       SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
       SimpleDateFormat format2 = new SimpleDateFormat("kk");
       SimpleDateFormat format3 = new SimpleDateFormat("mm");
       try {
           conn = DriverManager.getConnection(jdbc);
           stmt = conn.createStatement();
           DatabaseConfig dbc = new DatabaseConfig();
           String[] tables = dbc.findTableName("HodJob",time.getStartTime(),time.getEndTime());
           for(int i=0;i<tables.length;i++) {
               String query = "select HodID,UserID,Status,TimeQueued,StartTime,EndTime from "+tables[i]+" where "+dateclause+";";
               if (stmt.execute(query)) {
                   rs = stmt.getResultSet();
                   while (rs.next()) {
                       String style="simple";
                       String hodid = rs.getString(1);
                       String user = rs.getString(2);
                       String status = rs.getString(3);
                       String timeQueued = rs.getString(4);
                       String startTime = rs.getString(5);
                       String sDate = format.format((rs.getTimestamp(5)).getTime());
                       String s = format2.format((rs.getTimestamp(5)).getTime());
                       String sm = format3.format((rs.getTimestamp(5)).getTime());
                       String endTime = rs.getString(6);
                       String eDate = format.format((rs.getTimestamp(6)).getTime());
                       String e = format2.format((rs.getTimestamp(6)).getTime());
                       String em = format3.format((rs.getTimestamp(6)).getTime());
                       String hodIDSelect = hodID;
                       if(hodid.equals(hodID)) {
                           style="class=highlight";
                       }
                       hodIDSelect = hodid;
%>
<tr <%=style%>><td width="15%"><a href="#" onclick="save_hod('<%= hodIDSelect %>');"><%= hodid %></a></td>
    <td width="25%"><a href="#" onclick="save_hod('<%= hodIDSelect %>');"><%= user %></a></td>
    <td width="10%"><a href="#" onclick="save_hod('<%= hodIDSelect %>');"><%= status %></a></td>
    <td width="10%"><a href="#" onclick="save_hod('<%= hodIDSelect %>');"><%= timeQueued %></a></td>
    <td width="20%"><a href="#" onclick="save_hod('<%= hodIDSelect %>');"><%= startTime %></a></td>
    <td><a href="#" onclick="save_hod('<%= hodIDSelect %>');"><%= endTime %></a></td>
</tr>
<%
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
</table></div>
<input type="button" onclick="save_hod('');" class="formButton" name="Reset" value="Reset">
<script type="javascript/text">
function save_hod(obj) {
    var myAjax=new Ajax.Request(
        "/hicc/jsp/session.jsp",
        {
            asynchronous: true,
            method: 'get',
            parameters: "HodID="+obj,
        }
    );
}
</script>
