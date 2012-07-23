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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Test;
import org.mockito.Mockito;


public class TestDatanodeRegister { 
  public static final Log LOG = LogFactory.getLog(TestDatanodeRegister.class);

  // Invalid address
  static final InetSocketAddress INVALID_ADDR =
    new InetSocketAddress("127.0.0.1", 1);

  @Test
  public void testDataNodeRegister() throws Exception {
    DataNode mockDN = mock(DataNode.class);
    Mockito.doReturn(true).when(mockDN).shouldRun();
    
    BPOfferService bpos = new BPOfferService(INVALID_ADDR, mockDN);

    NamespaceInfo fakeNSInfo = mock(NamespaceInfo.class);
    when(fakeNSInfo.getBuildVersion()).thenReturn("NSBuildVersion");
    DatanodeProtocol fakeDNProt = mock(DatanodeProtocol.class);
    when(fakeDNProt.versionRequest()).thenReturn(fakeNSInfo);

    bpos.setNameNode( fakeDNProt );
    bpos.bpNSInfo = fakeNSInfo;
    try {   
      bpos.retrieveNamespaceInfo();
      fail("register() did not throw exception! " +
           "Expected: IncorrectVersionException");
    } catch (IncorrectVersionException ie) {
      LOG.info("register() returned correct Exception: IncorrectVersionException");
    }
  }
  
  @Test
  public void testDataNodeReregister() throws Exception {
    
    final String hostname = "somehostname";
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "127.0.0.1:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, hostname);
    DataNode dn = new DataNode(conf, null);
    try {
      NamespaceInfo fakeNSInfo = mock(NamespaceInfo.class);
      when(fakeNSInfo.getBuildVersion()).thenReturn("NSBuildVersion");
      DatanodeRegistration bpReg = dn.createBPRegistration(fakeNSInfo);
      assertEquals("Bad hostname in registration", hostname, bpReg.getHost());
    
      // set the datanode name to an IP address and verify the symbolic name
      // is still used during registration
      bpReg.setName("127.0.0.1:0");
      dn.bpRegistrationSucceeded(bpReg, null);
      bpReg = dn.createBPRegistration(fakeNSInfo);
      assertEquals("Bad hostname in re-registration", hostname, bpReg.getHost());
    } finally {
      dn.shutdown();
    }
  }
}
