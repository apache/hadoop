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

import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Preconditions;

/**
 * An internal-facing only collection of test utilities for the DataNode. This
 * is to ensure that test-scope dependencies aren't inadvertently leaked
 * to clients, e.g. Mockito.
 */
public class InternalDataNodeTestUtils {

  public final static String TEST_CLUSTER_ID = "testClusterID";
  public final static String TEST_POOL_ID = "BP-TEST";

  public static DatanodeRegistration
  getDNRegistrationForBP(DataNode dn, String bpid) throws IOException {
    return dn.getDNRegistrationForBP(bpid);
  }

  /**
   * Insert a Mockito spy object between the given DataNode and
   * the given NameNode. This can be used to delay or wait for
   * RPC calls on the datanode->NN path.
   */
  public static DatanodeProtocolClientSideTranslatorPB spyOnBposToNN(
      DataNode dn, NameNode nn) {
    String bpid = nn.getNamesystem().getBlockPoolId();

    BPOfferService bpos = null;
    for (BPOfferService thisBpos : dn.getAllBpOs()) {
      if (thisBpos.getBlockPoolId().equals(bpid)) {
        bpos = thisBpos;
        break;
      }
    }
    Preconditions.checkArgument(bpos != null,
        "No such bpid: %s", bpid);

    BPServiceActor bpsa = null;
    for (BPServiceActor thisBpsa : bpos.getBPServiceActors()) {
      if (thisBpsa.getNNSocketAddress().equals(nn.getServiceRpcAddress())) {
        bpsa = thisBpsa;
        break;
      }
    }
    Preconditions.checkArgument(bpsa != null,
      "No service actor to NN at %s", nn.getServiceRpcAddress());

    DatanodeProtocolClientSideTranslatorPB origNN = bpsa.getNameNodeProxy();
    DatanodeProtocolClientSideTranslatorPB spy = Mockito.spy(origNN);
    bpsa.setNameNode(spy);
    return spy;
  }

  /**
   * Starts an instance of DataNode with NN mocked. Called should ensure to
   * shutdown the DN
   *
   * @throws IOException
   */
  public static DataNode startDNWithMockNN(
      Configuration conf,
      final InetSocketAddress nnSocketAddr,
      final InetSocketAddress nnServiceAddr,
      final String dnDataDir)
      throws IOException {

    FileSystem.setDefaultUri(conf, "hdfs://" + nnSocketAddr.getHostName() + ":"
        + nnSocketAddr.getPort());
    ArrayList<StorageLocation> locations = new ArrayList<StorageLocation>();
    File dataDir = new File(dnDataDir);
    FileUtil.fullyDelete(dataDir);
    dataDir.mkdirs();
    StorageLocation location = StorageLocation.parse(dataDir.getPath());
    locations.add(location);

    final DatanodeProtocolClientSideTranslatorPB namenode =
        mock(DatanodeProtocolClientSideTranslatorPB.class);

    Mockito.doAnswer(new Answer<DatanodeRegistration>() {
      @Override
      public DatanodeRegistration answer(InvocationOnMock invocation)
          throws Throwable {
        return (DatanodeRegistration) invocation.getArguments()[0];
      }
    }).when(namenode).registerDatanode(Mockito.any(DatanodeRegistration.class));

    when(namenode.versionRequest()).thenReturn(
        new NamespaceInfo(1, TEST_CLUSTER_ID, TEST_POOL_ID,
            1L));

    when(
        namenode.sendHeartbeat(Mockito.any(DatanodeRegistration.class),
            Mockito.any(StorageReport[].class), Mockito.anyLong(),
            Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt(),
            Mockito.anyInt(), Mockito.any(VolumeFailureSummary.class),
            Mockito.anyBoolean(),
            Mockito.any(SlowPeerReports.class),
            Mockito.any(SlowDiskReports.class))).thenReturn(
        new HeartbeatResponse(new DatanodeCommand[0], new NNHAStatusHeartbeat(
            HAServiceState.ACTIVE, 1), null, ThreadLocalRandom.current()
            .nextLong() | 1L));

    DataNode dn = new DataNode(conf, locations, null, null) {
      @Override
      DatanodeProtocolClientSideTranslatorPB connectToNN(
          InetSocketAddress nnAddr) throws IOException {
        Assert.assertEquals(nnServiceAddr, nnAddr);
        return namenode;
      }
    };
    // Trigger a heartbeat so that it acknowledges the NN as active.
    dn.getAllBpOs().get(0).triggerHeartbeatForTests();

    return dn;
  }
}
