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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Preconditions;

/**
 * Utility class for accessing package-private DataNode information during tests.
 *
 */
public class DataNodeTestUtils {
  private static final String DIR_FAILURE_SUFFIX = ".origin";

  public final static String TEST_CLUSTER_ID = "testClusterID";
  public final static String TEST_POOL_ID = "BP-TEST";

  public static DatanodeRegistration
  getDNRegistrationForBP(DataNode dn, String bpid) throws IOException {
    return dn.getDNRegistrationForBP(bpid);
  }

  public static void setHeartbeatsDisabledForTests(DataNode dn,
      boolean heartbeatsDisabledForTests) {
    dn.setHeartbeatsDisabledForTests(heartbeatsDisabledForTests);
  }

  /**
   * Set if cache reports are disabled for all DNs in a mini cluster.
   */
  public static void setCacheReportsDisabledForTests(MiniDFSCluster cluster,
      boolean disabled) {
    for (DataNode dn : cluster.getDataNodes()) {
      dn.setCacheReportsDisabledForTest(disabled);
    }
  }

  public static void triggerDeletionReport(DataNode dn) throws IOException {
    for (BPOfferService bpos : dn.getAllBpOs()) {
      bpos.triggerDeletionReportForTests();
    }
  }

  public static void triggerHeartbeat(DataNode dn) throws IOException {
    for (BPOfferService bpos : dn.getAllBpOs()) {
      bpos.triggerHeartbeatForTests();
    }
  }
  
  public static void triggerBlockReport(DataNode dn) throws IOException {
    for (BPOfferService bpos : dn.getAllBpOs()) {
      bpos.triggerBlockReportForTests();
    }
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

  public static InterDatanodeProtocol createInterDatanodeProtocolProxy(
      DataNode dn, DatanodeID datanodeid, final Configuration conf,
      boolean connectToDnViaHostname) throws IOException {
    if (connectToDnViaHostname != dn.getDnConf().connectToDnViaHostname) {
      throw new AssertionError("Unexpected DN hostname configuration");
    }
    return DataNode.createInterDataNodeProtocolProxy(datanodeid, conf,
        dn.getDnConf().socketTimeout, dn.getDnConf().connectToDnViaHostname);
  }

  /**
   * This method is used for testing. 
   * Examples are adding and deleting blocks directly.
   * The most common usage will be when the data node's storage is simulated.
   * 
   * @return the fsdataset that stores the blocks
   */
  public static FsDatasetSpi<?> getFSDataset(DataNode dn) {
    return dn.getFSDataset();
  }

  /**
   * Fetch a copy of ReplicaInfo from a datanode by block id
   * @param dn datanode to retrieve a replicainfo object from
   * @param bpid Block pool Id
   * @param blkId id of the replica's block
   * @return copy of ReplicaInfo object @link{FSDataset#fetchReplicaInfo}
   */
  public static ReplicaInfo fetchReplicaInfo(final DataNode dn,
      final String bpid, final long blkId) {
    return FsDatasetTestUtil.fetchReplicaInfo(dn.getFSDataset(), bpid, blkId);
  }

  /**
   * It injects disk failures to data dirs by replacing these data dirs with
   * regular files.
   *
   * @param dirs data directories.
   * @throws IOException on I/O error.
   */
  public static void injectDataDirFailure(File... dirs) throws IOException {
    for (File dir : dirs) {
      File renamedTo = new File(dir.getPath() + DIR_FAILURE_SUFFIX);
      if (renamedTo.exists()) {
        throw new IOException(String.format(
            "Can not inject failure to dir: %s because %s exists.",
            dir, renamedTo));
      }
      if (!dir.renameTo(renamedTo)) {
        throw new IOException(String.format("Failed to rename %s to %s.",
            dir, renamedTo));
      }
      if (!dir.createNewFile()) {
        throw new IOException(String.format(
            "Failed to create file %s to inject disk failure.", dir));
      }
    }
  }

  /**
   * Restore the injected data dir failures.
   *
   * @see {@link #injectDataDirFailures}.
   * @param dirs data directories.
   * @throws IOException
   */
  public static void restoreDataDirFromFailure(File... dirs)
      throws IOException {
    for (File dir : dirs) {
      File renamedDir = new File(dir.getPath() + DIR_FAILURE_SUFFIX);
      if (renamedDir.exists()) {
        if (dir.exists()) {
          if (!dir.isFile()) {
            throw new IOException(
                "Injected failure data dir is supposed to be file: " + dir);
          }
          if (!dir.delete()) {
            throw new IOException(
                "Failed to delete injected failure data dir: " + dir);
          }
        }
        if (!renamedDir.renameTo(dir)) {
          throw new IOException(String.format(
              "Failed to recover injected failure data dir %s to %s.",
              renamedDir, dir));
        }
      }
    }
  }
  
  public static void runDirectoryScanner(DataNode dn) throws IOException {
    DirectoryScanner directoryScanner = dn.getDirectoryScanner();
    if (directoryScanner != null) {
      dn.getDirectoryScanner().reconcile();
    }
  }

  /**
   * Starts an instance of DataNode with NN mocked. Called should ensure to
   * shutdown the DN
   *
   * @throws IOException
   */
  public static DataNode startDNWithMockNN(Configuration conf,
      final InetSocketAddress nnSocketAddr, final String dnDataDir)
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
        new NamespaceInfo(1, TEST_CLUSTER_ID, TEST_POOL_ID, 1L));

    when(
        namenode.sendHeartbeat(Mockito.any(DatanodeRegistration.class),
            Mockito.any(StorageReport[].class), Mockito.anyLong(),
            Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt(),
            Mockito.anyInt(), Mockito.any(VolumeFailureSummary.class),
            Mockito.anyBoolean())).thenReturn(
        new HeartbeatResponse(new DatanodeCommand[0], new NNHAStatusHeartbeat(
            HAServiceState.ACTIVE, 1), null, ThreadLocalRandom.current()
            .nextLong() | 1L));

    DataNode dn = new DataNode(conf, locations, null) {
      @Override
      DatanodeProtocolClientSideTranslatorPB connectToNN(
          InetSocketAddress nnAddr) throws IOException {
        Assert.assertEquals(nnSocketAddr, nnAddr);
        return namenode;
      }
    };
    // Trigger a heartbeat so that it acknowledges the NN as active.
    dn.getAllBpOs().get(0).triggerHeartbeatForTests();

    return dn;
  }
}
