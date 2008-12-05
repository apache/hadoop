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
<%@ page import = "javax.servlet.http.*, java.io.*, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig"  %>
<% String boxId = request.getParameter("boxId"); %>
Cluster
<select id="<%= boxId %>cluster" name="<%= boxId %>cluster" class="formSelect">
<%
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
       Iterator i = cc.getClusters();
       while(i.hasNext()) { 
           String label = i.next().toString();
           if(label.equals(cluster)) {
               out.println("<option selected>"+label+"</option>");
           } else {
               out.println("<option>"+label+"</option>");
           }
       } %>
</select>
<input type="button" onClick="save_cluster('<%= boxId %>');" name="Apply" value="Apply" class="formButton">
