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

import com.google.protobuf.ByteString;
import javax.crypto.SecretKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Mock-based unit test to verify that DataXceiver does not fail when no
 * storageId or targetStorageTypes are passed - as is the case in Hadoop 2.x.
 */
public class TestDataXceiverBackwardsCompat {
  @Rule
  public Timeout timeout = new Timeout(60000);

  private void failWithException(String message, Exception exception) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    exception.printStackTrace(new PrintStream(buffer));
    String stacktrace = buffer.toString();

    fail(message + ": " + exception + "; " + exception.getMessage() + "\n" +
        stacktrace);
  }

  /**
   * Used for mocking DataNode. Mockito does not provide a way to mock
   * properties (like data or saslClient) so we have to manually set up mocks
   * of those properties inside our own class.
   */
  public class NullDataNode extends DataNode {
    public NullDataNode(Configuration conf, OutputStream out, int port) throws
        Exception {
      super(conf);
      data = (FsDatasetSpi<FsVolumeSpi>)mock(FsDatasetSpi.class);
      saslClient = mock(SaslDataTransferClient.class);

      IOStreamPair pair = new IOStreamPair(null, out);

      doReturn(pair).when(saslClient).socketSend(
          any(Socket.class), any(OutputStream.class), any(InputStream.class),
          any(DataEncryptionKeyFactory.class), any(Token.class),
          any(DatanodeID.class), any(SecretKey.class));
      doReturn(mock(ReplicaHandler.class)).when(data).createTemporary(
          any(StorageType.class), any(String.class), any(ExtendedBlock.class),
          anyBoolean());

      new Thread(new NullServer(port)).start();
    }

    @Override
    public DatanodeRegistration getDNRegistrationForBP(String bpid)
        throws IOException {
      return null;
    }

    @Override
    public Socket newSocket() throws IOException {
      return new Socket();
    }

    /**
     * Class for accepting incoming an incoming connection. Does not read
     * data or repeat in any way: simply allows a single client to connect to
     * a local URL.
     */
    private class NullServer implements Runnable {

      private ServerSocket serverSocket;

      NullServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
      }

      @Override
      public void run() {
        try {
          serverSocket.accept();
          serverSocket.close();
          LOG.info("Client connection accepted by NullServer");
        } catch (Exception e) {
          LOG.info("Exception in NullServer: " + e + "; " + e.getMessage());
        }
      }
    }
  }

  @Test
  public void testBackwardsCompat() throws Exception {
    Peer peer = mock(Peer.class);
    doReturn("").when(peer).getRemoteAddressString();
    Configuration conf = new Configuration();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int port = ServerSocketUtil.getPort(1234, 10);
    DataNode dataNode = new NullDataNode(conf, out, port);
    DataXceiverServer server = new DataXceiverServer(
        mock(PeerServer.class), conf, dataNode);
    DataXceiver xceiver = spy(DataXceiver.create(peer, dataNode, server));

    BlockReceiver mockBlockReceiver = mock(BlockReceiver.class);
    doReturn(mock(Replica.class)).when(mockBlockReceiver).getReplica();

    DatanodeInfo[] targets = {mock(DatanodeInfo.class)};
    doReturn("localhost:" + port).when(targets[0]).getXferAddr(true);
    doReturn("127.0.0.1:" + port).when(targets[0]).getXferAddr(false);
    StorageType[] storageTypes = {StorageType.RAM_DISK};

    doReturn(mockBlockReceiver).when(xceiver).getBlockReceiver(
        any(ExtendedBlock.class), any(StorageType.class),
        any(DataInputStream.class), anyString(), anyString(),
        any(BlockConstructionStage.class), anyLong(), anyLong(), anyLong(),
        anyString(), any(DatanodeInfo.class), any(DataNode.class),
        any(DataChecksum.class), any(CachingStrategy.class),
        ArgumentCaptor.forClass(Boolean.class).capture(),
        anyBoolean(), any(String.class));

    Token<BlockTokenIdentifier> token = (Token<BlockTokenIdentifier>)mock(
        Token.class);
    doReturn("".getBytes()).when(token).getIdentifier();
    doReturn("".getBytes()).when(token).getPassword();
    doReturn(new Text("")).when(token).getKind();
    doReturn(new Text("")).when(token).getService();

    DataChecksum checksum = mock(DataChecksum.class);
    doReturn(DataChecksum.Type.NULL).when(checksum).getChecksumType();

    DatanodeInfo datanodeInfo = mock(DatanodeInfo.class);
    doReturn("localhost").when(datanodeInfo).getHostName();
    doReturn(ByteString.copyFromUtf8("localhost"))
        .when(datanodeInfo).getHostNameBytes();
    doReturn("127.0.0.1").when(datanodeInfo).getIpAddr();
    doReturn(ByteString.copyFromUtf8("127.0.0.1"))
        .when(datanodeInfo).getIpAddrBytes();
    doReturn(DatanodeInfo.AdminStates.NORMAL).when(datanodeInfo)
        .getAdminState();
    final String uuid = UUID.randomUUID().toString();
    doReturn(uuid).when(datanodeInfo).getDatanodeUuid();
    doReturn(ByteString.copyFromUtf8(uuid))
        .when(datanodeInfo).getDatanodeUuidBytes();

    Exception storedException = null;
    try {
      xceiver.writeBlock(
          new ExtendedBlock("Dummy-pool", 0L),
          StorageType.RAM_DISK,
          token,
          "Dummy-Client",
          targets,
          storageTypes,
          datanodeInfo,
          BlockConstructionStage.PIPELINE_SETUP_CREATE,
          0, 0, 0, 0,
          checksum,
          CachingStrategy.newDefaultStrategy(),
          false,
          false, new boolean[0], null, new String[0]);
    } catch (Exception e) {
      // Not enough things have been mocked for this to complete without
      // exceptions, but we want to make sure we can at least get as far as
      // sending data to the server with null values for storageId and
      // targetStorageTypes.
      storedException = e;
    }
    byte[] output = out.toByteArray();
    if (output.length == 0) {
      if (storedException == null) {
        failWithException("No output written, but no exception either (this " +
            "shouldn't happen", storedException);
      } else {
        failWithException("Exception occurred before anything was written",
            storedException);
      }
    }
  }
}
