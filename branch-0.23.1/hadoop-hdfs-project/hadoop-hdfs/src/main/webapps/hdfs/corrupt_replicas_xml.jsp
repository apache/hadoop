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
 
  This script outputs information about corrupt replicas on the system (as XML). 
  
  The script takes two GET parameters:
    - numCorruptBlocks The number of corrupt blocks to return. Must be >= 0 &&
      <= 100. Defaults to 10.
    - startingBlockId The block id (as a long) from which to begin iterating. 
      Output does not include the starting block id (it begins at the following
      block id). If not given, iteration starts from beginning. 

  Example output is below:
      <corrupt_block_info>
        <dfs_replication>1</dfs_replication>
        <num_missing_blocks>1</num_missing_blocks>
        <num_corrupt_replica_blocks>1</num_corrupt_replica_blocks>
        <corrupt_replica_block_ids>
          <block_id>-2207002825050436217</block_id>
        </corrupt_replica_block_ids>
      </corrupt_block_info>

  Notes:
    - corrupt_block_info/corrupt_replica_block_ids will 0 to numCorruptBlocks
      children
    - If an error exists, corrupt_block_info/error will exist and
      contain a human readable error message
 
*/
 
%>
<%@ page
  contentType="application/xml"
  import="java.io.IOException"
  import="java.util.List"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hdfs.server.common.JspHelper"
  import="org.apache.hadoop.hdfs.server.namenode.NamenodeJspHelper.XMLCorruptBlockInfo"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.znerd.xmlenc.*"
%>
<%!
  private static final long serialVersionUID = 1L;
%>
<%

  NameNode nn = NameNodeHttpServer.getNameNodeFromContext(application);
  FSNamesystem fsn = nn.getNamesystem();

  Integer numCorruptBlocks = 10;
  try {
    Long l = JspHelper.validateLong(request.getParameter("numCorruptBlocks"));
    if (l != null) {
      numCorruptBlocks = l.intValue();
    }
  } catch(NumberFormatException e) {
    
  }

  Long startingBlockId = null;
  try {
    startingBlockId =
      JspHelper.validateLong(request.getParameter("startingBlockId"));
  } catch(NumberFormatException e) { 
  }  

  XMLCorruptBlockInfo cbi = new XMLCorruptBlockInfo(fsn,
                                                    new Configuration(),
                                                    numCorruptBlocks,
                                                    startingBlockId);
  XMLOutputter doc = new XMLOutputter(out, "UTF-8");
  cbi.toXML(doc);
%>