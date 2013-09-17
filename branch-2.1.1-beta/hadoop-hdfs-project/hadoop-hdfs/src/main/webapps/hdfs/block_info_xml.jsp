<?xml version="1.0" encoding="UTF-8"?><%!
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
 
 /*
 
  This script outputs information about a block (as XML). The script accepts a 
  GET parameter named blockId which should be block id (as a long).

  Example output is below (the blockId was 8888705098093096373):
    <block_info>
      <block_id>8888705098093096373</block_id>
      <block_name>blk_8888705098093096373</block_name>
      <file>
        <local_name>some_file_name</local_name>
        <local_directory>/input/</local_directory>
        <user_name>user_name</user_name>
        <group_name>supergroup</group_name>
        <is_directory>false</is_directory>
        <access_time>1251166313680</access_time>
        <is_under_construction>false</is_under_construction>
        <ds_quota>-1</ds_quota>
        <permission_status>user_name:supergroup:rw-r--r--</permission_status>
        <replication>1</replication>
        <disk_space_consumed>2815</disk_space_consumed>
        <preferred_block_size>67108864</preferred_block_size>
      </file>
      <replicas>
        <replica>
          <host_name>hostname</host_name>
          <is_corrupt>false</is_corrupt>
        </replica>
      </replicas>
    </block_info> 

  Notes:
    - block_info/file will only exist if the file can be found
    - block_info/replicas can contain 0 or more children 
    - If an error exists, block_info/error will exist and contain a human
      readable error message
 
*/
 
%>
<%@ page
  contentType="application/xml"
  import="org.apache.hadoop.hdfs.server.namenode.NamenodeJspHelper.XMLBlockInfo"
  import="org.apache.hadoop.hdfs.server.common.JspHelper"
  import="org.znerd.xmlenc.*"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;  
%>
<%
NameNode nn = NameNodeHttpServer.getNameNodeFromContext(application);
String namenodeRole = nn.getRole().toString();
FSNamesystem fsn = nn.getNamesystem();

Long blockId = null;
try {
  blockId = JspHelper.validateLong(request.getParameter("blockId"));
} catch(NumberFormatException e) {
  blockId = null;
}


XMLBlockInfo bi = new XMLBlockInfo(fsn, blockId);
XMLOutputter doc = new XMLOutputter(out, "UTF-8");
bi.toXML(doc);

%>
