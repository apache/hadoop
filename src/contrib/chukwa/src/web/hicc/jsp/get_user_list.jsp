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
<%@ page import = "java.sql.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.json.*"  %>
<%
       Connection conn = null;
       Statement stmt = null;
       ResultSet rs = null;
       String cluster = (String) session.getAttribute("cluster");
       if(cluster==null) {
           cluster="demo";
       }  
       JSONArray array = new JSONArray();
       JSONObject hash = new JSONObject();
       hash.put("label", "Aggregated");
       hash.put("value", "");
       array.put(hash);
       ClusterConfig cc = new ClusterConfig();
       String jdbc = cc.getURL(cluster);
       try {
           conn = DriverManager.getConnection(jdbc);
           stmt = conn.createStatement();
           String query = "select distinct UserID from HodJob order by UserID";
           if (stmt.execute(query)) {
               rs = stmt.getResultSet();
               while (rs.next()) {
                   JSONObject tmp = new JSONObject();
                   tmp.put("label",rs.getString(1));
                   tmp.put("value",rs.getString(1));
                   array.put(tmp);
               }
               out.println(array.toString());
           }
       } catch(SQLException ex) {
           JSONArray empty = new JSONArray();
           out.println(empty.toString());
       }
%>
