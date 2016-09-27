/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.net.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.DataChecksum;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;


/**
 * Mock-based unit test to verify that the DataXceiver correctly handles the
 * LazyPersist hint from clients.
 */
public class TestDataXceiverLazyPersistHint {
  @Rule
  public Timeout timeout = new Timeout(300000);

  private enum PeerLocality {
    LOCAL,
    REMOTE
  }

  private enum NonLocalLazyPersist {
    ALLOWED,
    NOT_ALLOWED
  }

  /**
   * Ensure that the correct hint is passed to the block receiver when
   * the client is local.
   */
  @Test
  public void testWithLocalClient() throws IOException {
    ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
    DataXceiver xceiver = makeStubDataXceiver(
        PeerLocality.LOCAL, NonLocalLazyPersist.NOT_ALLOWED, captor);

    for (Boolean lazyPersistSetting : Arrays.asList(true, false)) {
      issueWriteBlockCall(xceiver, lazyPersistSetting);
      assertThat(captor.getValue(), is(lazyPersistSetting));
    }
  }

  /**
   * Ensure that hint is always false when the client is remote.
   */
  @Test
  public void testWithRemoteClient() throws IOException {
    ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
    DataXceiver xceiver = makeStubDataXceiver(
        PeerLocality.REMOTE, NonLocalLazyPersist.NOT_ALLOWED, captor);

    for (Boolean lazyPersistSetting : Arrays.asList(true, false)) {
      issueWriteBlockCall(xceiver, lazyPersistSetting);
      assertThat(captor.getValue(), is(false));
    }
  }

  /**
   * Ensure that the correct hint is passed to the block receiver when
   * the client is remote AND dfs.datanode.allow.non.local.lazy.persist
   * is set to true.
   */
  @Test
  public void testOverrideWithRemoteClient() throws IOException {
    ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
    DataXceiver xceiver = makeStubDataXceiver(
        PeerLocality.REMOTE, NonLocalLazyPersist.ALLOWED, captor);

    for (Boolean lazyPersistSetting : Arrays.asList(true, false)) {
      issueWriteBlockCall(xceiver, lazyPersistSetting);
      assertThat(captor.getValue(), is(lazyPersistSetting));
    }
  }

  /**
   * Issue a write block call with dummy parameters. The only parameter useful
   * for this test is the value of lazyPersist.
   */
  private void issueWriteBlockCall(DataXceiver xceiver, boolean lazyPersist)
      throws IOException {
    xceiver.writeBlock(
        new ExtendedBlock("Dummy-pool", 0L),
        StorageType.RAM_DISK,
        null,
        "Dummy-Client",
        new DatanodeInfo[0],
        new StorageType[0],
        mock(DatanodeInfo.class),
        BlockConstructionStage.PIPELINE_SETUP_CREATE,
        0, 0, 0, 0,
        DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 0),
        CachingStrategy.newDefaultStrategy(),
        lazyPersist,
        false, null);
  }

  // Helper functions to setup the mock objects.

  private static DataXceiver makeStubDataXceiver(
      PeerLocality locality,
      NonLocalLazyPersist nonLocalLazyPersist,
      final ArgumentCaptor<Boolean> captor) throws IOException {
    final BlockReceiver mockBlockReceiver = mock(BlockReceiver.class);
    doReturn(mock(Replica.class)).when(mockBlockReceiver).getReplica();

    DataXceiver xceiverSpy = spy(DataXceiver.create(
            getMockPeer(locality),
            getMockDn(nonLocalLazyPersist),
            mock(DataXceiverServer.class)));
    doReturn(mockBlockReceiver).when(xceiverSpy).getBlockReceiver(
        any(ExtendedBlock.class), any(StorageType.class),
        any(DataInputStream.class), anyString(), anyString(),
        any(BlockConstructionStage.class), anyLong(), anyLong(), anyLong(),
        anyString(), any(DatanodeInfo.class), any(DataNode.class),
        any(DataChecksum.class), any(CachingStrategy.class),
        captor.capture(), anyBoolean());
    doReturn(mock(DataOutputStream.class)).when(xceiverSpy)
        .getBufferedOutputStream();
    return xceiverSpy;
  }

  private static Peer getMockPeer(PeerLocality locality) {
    Peer peer = mock(Peer.class);
    when(peer.isLocal()).thenReturn(locality == PeerLocality.LOCAL);
    when(peer.getRemoteAddressString()).thenReturn("1.1.1.1:1000");
    when(peer.getLocalAddressString()).thenReturn("2.2.2.2:2000");
    return peer;
  }

  private static DataNode getMockDn(NonLocalLazyPersist nonLocalLazyPersist)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFS_DATANODE_NON_LOCAL_LAZY_PERSIST,
        nonLocalLazyPersist == NonLocalLazyPersist.ALLOWED);

    DatanodeRegistration mockDnReg = mock(DatanodeRegistration.class);
    DataNodeMetrics mockMetrics = mock(DataNodeMetrics.class);
    DataNode mockDn = mock(DataNode.class);
    when(mockDn.getConf()).thenReturn(conf);
    DNConf dnConf = new DNConf(mockDn);
    when(mockDn.getDnConf()).thenReturn(dnConf);
    when(mockDn.getMetrics()).thenReturn(mockMetrics);
    when(mockDn.getDNRegistrationForBP("Dummy-pool")).thenReturn(mockDnReg);
    return mockDn;
  }
}
