<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="decommission.xsl"?>
<%!
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
<%@ page 
  contentType="application/xml"

  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.DecommissionStatus"
  import="java.util.List"
  import="org.znerd.xmlenc.*"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;
%>
<%
  /**
   * This JSP page provides decomission nodes information cross cluster. 
   * It lists the date nodes with their decommission states/progress 
   * reported by each name node. 
   * It eleminates the data nodes who are not in decommission states.
   */
  final ClusterJspHelper clusterhealthjsp  = new ClusterJspHelper();
   DecommissionStatus dInfo = clusterhealthjsp.generateDecommissioningReport();
   XMLOutputter doc = new XMLOutputter(out, "UTF-8");
   dInfo.toXML(doc);
%>
