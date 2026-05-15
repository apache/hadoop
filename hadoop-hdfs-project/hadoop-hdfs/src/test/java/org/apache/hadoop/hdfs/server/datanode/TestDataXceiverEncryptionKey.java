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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test DataXceiver handling of InvalidEncryptionKeyException.
 */
@Timeout(60)
public class TestDataXceiverEncryptionKey {

  @Test
  public void testWriteBlockRetriesInvalidEncryptionKeyToMirror()
      throws Exception {
    Peer peer = createPeer();
    Configuration conf = new Configuration();
    CountingKeyFactory keyFactory = new CountingKeyFactory();
    RetryDataNode dataNode = new RetryDataNode(conf, keyFactory);
    AtomicInteger socketSendCount = new AtomicInteger();
    org.mockito.Mockito.doAnswer(invocation -> {
      if (socketSendCount.getAndIncrement() == 0) {
        throw new InvalidEncryptionKeyException("test invalid key");
      }
      return new IOStreamPair(
          new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream());
    }).when(dataNode.saslClient).socketSend(
        any(Socket.class), any(OutputStream.class), any(InputStream.class),
        any(DataEncryptionKeyFactory.class), any(),
        any(DatanodeID.class), any());

    DataXceiverServer server = new DataXceiverServer(
        mock(PeerServer.class), conf, dataNode);
    DataXceiver xceiver = spy(DataXceiver.create(peer, dataNode, server));
    mockBlockReceiver(xceiver);

    DatanodeInfo target = DFSTestUtil.getDatanodeInfo(
        "127.0.0.1", "localhost", 1);
    xceiver.writeBlock(
        new ExtendedBlock("bp-1", 1L),
        StorageType.DISK,
        createToken(),
        "",
        new DatanodeInfo[]{target},
        new StorageType[]{StorageType.DISK},
        target,
        BlockConstructionStage.PIPELINE_SETUP_CREATE,
        0, 0, 0, 0,
        createChecksum(),
        CachingStrategy.newDefaultStrategy(),
        false,
        false, null, null, new String[0]);

    assertEquals(2, socketSendCount.get());
    assertEquals(1, keyFactory.clearCount);
  }

  @Test
  public void testReplaceBlockRetriesInvalidEncryptionKeyToProxy()
      throws Exception {
    Peer peer = createPeer();
    Configuration conf = new Configuration();
    CountingKeyFactory keyFactory = new CountingKeyFactory();
    RetryDataNode dataNode = new RetryDataNode(conf, keyFactory);
    AtomicInteger socketSendCount = new AtomicInteger();
    org.mockito.Mockito.doAnswer(invocation -> {
      if (socketSendCount.getAndIncrement() == 0) {
        throw new InvalidEncryptionKeyException("test invalid key");
      }
      return new IOStreamPair(
          new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream());
    }).when(dataNode.saslClient).socketSend(
        any(Socket.class), any(OutputStream.class), any(InputStream.class),
        any(DataEncryptionKeyFactory.class), any(),
        any(DatanodeID.class));

    DataXceiverServer server = new DataXceiverServer(
        mock(PeerServer.class), conf, dataNode);
    DataXceiver xceiver = DataXceiver.create(peer, dataNode, server);

    DatanodeInfo proxySource = DFSTestUtil.getDatanodeInfo(
        "127.0.0.1", "localhost", 1);
    try {
      xceiver.replaceBlock(new ExtendedBlock("bp-1", 1L),
          StorageType.DISK, createToken(), "delHint", proxySource,
          "storage-id");
    } catch (Exception ignored) {
      // The test only exercises the connection setup path; after the retry
      // succeeds, the fake proxy has no copyBlock response to read.
    }

    assertEquals(2, socketSendCount.get());
    assertEquals(1, keyFactory.clearCount);
  }

  private static Peer createPeer() throws Exception {
    Peer peer = mock(Peer.class);
    doReturn("").when(peer).getRemoteAddressString();
    doReturn("").when(peer).getLocalAddressString();
    doReturn(new ByteArrayInputStream(new byte[0])).when(peer).getInputStream();
    doReturn(new ByteArrayOutputStream()).when(peer).getOutputStream();
    return peer;
  }

  private static Token<BlockTokenIdentifier> createToken() {
    Token<BlockTokenIdentifier> token = (Token<BlockTokenIdentifier>) mock(
        Token.class);
    doReturn("".getBytes()).when(token).getIdentifier();
    doReturn("".getBytes()).when(token).getPassword();
    doReturn(new Text("")).when(token).getKind();
    doReturn(new Text("")).when(token).getService();
    return token;
  }

  private static DataChecksum createChecksum() {
    DataChecksum checksum = mock(DataChecksum.class);
    doReturn(DataChecksum.Type.NULL).when(checksum).getChecksumType();
    return checksum;
  }

  private static void mockBlockReceiver(DataXceiver xceiver)
      throws Exception {
    BlockReceiver mockBlockReceiver = mock(BlockReceiver.class);
    Replica replica = mock(Replica.class);
    doReturn(replica).when(mockBlockReceiver).getReplica();
    doReturn("storage-id").when(replica).getStorageUuid();
    doReturn(false).when(replica).isOnTransientStorage();
    doReturn(mock(FsVolumeSpi.class)).when(replica).getVolume();
    doReturn(mockBlockReceiver).when(xceiver).getBlockReceiver(
        any(ExtendedBlock.class), any(StorageType.class),
        any(), anyString(), any(),
        any(BlockConstructionStage.class), anyLong(), anyLong(), anyLong(),
        anyString(), any(DatanodeInfo.class), any(DataNode.class),
        any(DataChecksum.class), any(CachingStrategy.class),
        anyBoolean(), anyBoolean(), any());
  }

  private static final class RetryDataNode extends DataNode {
    private final CountingKeyFactory keyFactory;

    private RetryDataNode(Configuration conf, CountingKeyFactory keyFactory)
        throws Exception {
      super(conf);
      this.keyFactory = keyFactory;
      data = (FsDatasetSpi<FsVolumeSpi>) mock(FsDatasetSpi.class);
      saslClient = mock(SaslDataTransferClient.class);
      metrics = mock(DataNodeMetrics.class);
    }

    @Override
    public DatanodeRegistration getDNRegistrationForBP(String bpid) {
      return null;
    }

    @Override
    public Socket newSocket() {
      return new FakeSocket();
    }

    @Override
    public DataEncryptionKeyFactory getDataEncryptionKeyFactoryForBlock(
        ExtendedBlock block) {
      return keyFactory;
    }

    @Override
    void closeBlock(ExtendedBlock block, String delHint, String storageUuid,
        boolean isTransientStorage) {
    }

    @Override
    void incrDatanodeNetworkErrors(String host) {
    }
  }

  private static final class CountingKeyFactory
      implements DataEncryptionKeyFactory {
    private int clearCount;

    @Override
    public DataEncryptionKey newDataEncryptionKey() {
      return null;
    }

    @Override
    public void clearDataEncryptionKey() {
      clearCount++;
    }
  }

  private static final class FakeSocket extends Socket {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayInputStream in =
        new ByteArrayInputStream(new byte[0]);

    @Override
    public void connect(SocketAddress endpoint, int timeout) {
    }

    @Override
    public SocketChannel getChannel() {
      return null;
    }

    @Override
    public OutputStream getOutputStream() {
      return out;
    }

    @Override
    public InputStream getInputStream() {
      return in;
    }

    @Override
    public void close() {
    }
  }
}
