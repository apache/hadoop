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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.datanode.DataNode.BPOfferService;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * Utility class for accessing package-private DataNode information during tests.
 *
 */
public class DataNodeTestUtils {
  public static DatanodeRegistration 
  getDNRegistrationByMachineName(DataNode dn, String mName) {
    return dn.getDNRegistrationByMachineName(mName);
  }
  
  public static DatanodeRegistration 
  getDNRegistrationForBP(DataNode dn, String bpid) throws IOException {
    return dn.getDNRegistrationForBP(bpid);
  }
  
  /**
   * manually setup datanode to testing
   * @param dn - datanode
   * @param nsifno - namenode info
   * @param bpid - block pool id
   * @param nn - namenode object
   * @throws IOException
   */
  public static void setBPNamenodeByIndex(DataNode dn,
      NamespaceInfo nsifno, String bpid, DatanodeProtocol nn) 
  throws IOException {
    // setup the right BPOS..
    BPOfferService [] bposs = dn.getAllBpOs();
    if(bposs.length<0) {
      throw new IOException("Datanode wasn't initializes with at least one NN");
    }
    for(BPOfferService bpos : bposs) {
      bpos.setNamespaceInfo(nsifno);

      dn.setBPNamenode(bpid, nn);
      bpos.setupBPStorage();
    }
  }
}
