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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import org.junit.Assert;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Before;
import org.junit.Test;

public class TestDatanodeRegister { 
  public static final Logger LOG =
      LoggerFactory.getLogger(TestDatanodeRegister.class);

  // Invalid address
  private static final InetSocketAddress INVALID_ADDR =
    new InetSocketAddress("127.0.0.1", 1);
  
  private BPServiceActor actor;
  NamespaceInfo fakeNsInfo;
  DNConf mockDnConf;
  
  @Before
  public void setUp() throws IOException {
    mockDnConf = mock(DNConf.class);
    doReturn(VersionInfo.getVersion()).when(mockDnConf).getMinimumNameNodeVersion();
    
    DataNode mockDN = mock(DataNode.class);
    doReturn(true).when(mockDN).shouldRun();
    doReturn(mockDnConf).when(mockDN).getDnConf();
    
    BPOfferService mockBPOS = mock(BPOfferService.class);
    doReturn(mockDN).when(mockBPOS).getDataNode();
    
    actor = new BPServiceActor("test", "test", INVALID_ADDR, null, mockBPOS);

    fakeNsInfo = mock(NamespaceInfo.class);
    // Return a a good software version.
    doReturn(VersionInfo.getVersion()).when(fakeNsInfo).getSoftwareVersion();
    // Return a good layout version for now.
    doReturn(HdfsServerConstants.NAMENODE_LAYOUT_VERSION).when(fakeNsInfo)
        .getLayoutVersion();
    
    DatanodeProtocolClientSideTranslatorPB fakeDnProt = 
        mock(DatanodeProtocolClientSideTranslatorPB.class);
    when(fakeDnProt.versionRequest()).thenReturn(fakeNsInfo);
    actor.setNameNode(fakeDnProt);
  }

  @Test
  public void testSoftwareVersionDifferences() throws Exception {
    // We expect no exception to be thrown when the software versions match.
    assertEquals(VersionInfo.getVersion(),
        actor.retrieveNamespaceInfo().getSoftwareVersion());
    
    // We expect no exception to be thrown when the min NN version is below the
    // reported NN version.
    doReturn("4.0.0").when(fakeNsInfo).getSoftwareVersion();
    doReturn("3.0.0").when(mockDnConf).getMinimumNameNodeVersion();
    assertEquals("4.0.0", actor.retrieveNamespaceInfo().getSoftwareVersion());
    
    // When the NN reports a version that's too low, throw an exception.
    doReturn("3.0.0").when(fakeNsInfo).getSoftwareVersion();
    doReturn("4.0.0").when(mockDnConf).getMinimumNameNodeVersion();
    try {
      actor.retrieveNamespaceInfo();
      fail("Should have thrown an exception for NN with too-low version");
    } catch (IncorrectVersionException ive) {
      GenericTestUtils.assertExceptionContains(
          "The reported NameNode version is too low", ive);
      LOG.info("Got expected exception", ive);
    }
  }
  
  @Test
  public void testDifferentLayoutVersions() throws Exception {
    // We expect no exceptions to be thrown when the layout versions match.
    assertEquals(HdfsServerConstants.NAMENODE_LAYOUT_VERSION,
        actor.retrieveNamespaceInfo().getLayoutVersion());
    
    // We expect an exception to be thrown when the NN reports a layout version
    // different from that of the DN.
    doReturn(HdfsServerConstants.NAMENODE_LAYOUT_VERSION * 1000).when(fakeNsInfo)
        .getLayoutVersion();
    try {
      actor.retrieveNamespaceInfo();
    } catch (IOException e) {
      fail("Should not fail to retrieve NS info from DN with different layout version");
    }
  }

  @Test
  public void testDNShutdwonBeforeRegister() throws Exception {
    final InetSocketAddress nnADDR = new InetSocketAddress(
        "localhost", 5020);
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    FileSystem.setDefaultUri(conf,
        "hdfs://" + nnADDR.getHostName() + ":" + nnADDR.getPort());
    ArrayList<StorageLocation> locations = new ArrayList<>();
    DataNode dn = new DataNode(conf, locations, null, null);
    BPOfferService bpos = new BPOfferService("test_ns",
        Lists.newArrayList("nn0"), Lists.newArrayList(nnADDR),
        Collections.<InetSocketAddress>nCopies(1, null), dn);
    DatanodeProtocolClientSideTranslatorPB fakeDnProt =
        mock(DatanodeProtocolClientSideTranslatorPB.class);
    when(fakeDnProt.versionRequest()).thenReturn(fakeNsInfo);

    BPServiceActor localActor = new BPServiceActor("test", "test",
        INVALID_ADDR, null, bpos);
    localActor.setNameNode(fakeDnProt);
    try {
      NamespaceInfo nsInfo = localActor.retrieveNamespaceInfo();
      bpos.setNamespaceInfo(nsInfo);
      localActor.stop();
      localActor.register(nsInfo);
    } catch (IOException e) {
      Assert.assertEquals("DN shut down before block pool registered",
          e.getMessage());
    }
  }

  @Test
  public void testInvalidConfigurationValue() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, -2);
    intercept(HadoopIllegalArgumentException.class,
        "Invalid value configured for dfs.datanode.failed.volumes.tolerated"
            + " - -2 should be greater than or equal to -1",
        () -> new DataNode(conf, new ArrayList<>(), null, null));
  }
}
