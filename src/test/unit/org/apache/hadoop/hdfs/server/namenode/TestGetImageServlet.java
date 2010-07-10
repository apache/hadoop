/**
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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestGetImageServlet {
  
  // Worker class to poke the isValidRequestor method with verifying it accepts
  // or rejects with these standard allowed principals
  private void verifyIsValidReqBehavior(GetImageServlet gim, 
                                        boolean shouldSucceed, String msg) {
    final String [] validRequestors = {DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY,
                                       DFS_NAMENODE_USER_NAME_KEY,
                                       DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY,
                                       DFS_SECONDARY_NAMENODE_USER_NAME_KEY };
    
    for(String v : validRequestors) {
      Configuration conf = new Configuration();
      conf.set(v, "a");
      assertEquals(msg + v, gim.isValidRequestor(shouldSucceed ? "a" : "b", conf), 
                   shouldSucceed);
    }
  }
  
  @Test
  public void IsValidRequestorAcceptsCorrectly() {
    GetImageServlet gim = new GetImageServlet();

    verifyIsValidReqBehavior(gim, true, 
        "isValidRequestor has rejected a valid requestor: ");
  }
  
  @Test
  public void IsValidRequestorRejectsCorrectly() {
    GetImageServlet gim = new GetImageServlet();
    
    // Don't set any valid requestors
    assertFalse("isValidRequestor allowed a requestor despite no values being set",
                gim.isValidRequestor("not set", new Configuration()));
    
    verifyIsValidReqBehavior(gim, false, 
        "isValidRequestor has allowed an invalid requestor: ");
  }

}
