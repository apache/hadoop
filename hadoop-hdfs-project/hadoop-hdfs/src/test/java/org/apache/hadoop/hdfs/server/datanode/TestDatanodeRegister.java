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

import java.io.File;
import java.io.IOException;
import java.util.AbstractList;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Test;


public class TestDatanodeRegister { 
  public static final Log LOG = LogFactory.getLog(TestDatanodeRegister.class);
  @Test
  public void testDataNodeRegister() throws Exception {
    DataNode.BPOfferService myMockBPOS = mock(DataNode.BPOfferService.class);
    doCallRealMethod().when(myMockBPOS).register();
    myMockBPOS.bpRegistration = mock(DatanodeRegistration.class);
    when(myMockBPOS.bpRegistration.getStorageID()).thenReturn("myTestStorageID");
    
    NamespaceInfo fakeNSInfo = mock(NamespaceInfo.class);
    when(fakeNSInfo.getBuildVersion()).thenReturn("NSBuildVersion");
    DatanodeProtocol fakeDNProt = mock(DatanodeProtocol.class);
    when(fakeDNProt.versionRequest()).thenReturn(fakeNSInfo);
    doCallRealMethod().when(myMockBPOS).setNameNode(fakeDNProt);
    myMockBPOS.setNameNode( fakeDNProt );
    try {   
      myMockBPOS.register();
      fail("register() did not throw exception! " +
           "Expected: IncorrectVersionException");
    } catch (IncorrectVersionException ie) {
      LOG.info("register() returned correct Exception: IncorrectVersionException");
    }
  }
}
