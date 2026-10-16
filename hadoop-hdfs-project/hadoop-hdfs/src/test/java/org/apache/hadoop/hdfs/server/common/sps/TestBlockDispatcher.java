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
package org.apache.hadoop.hdfs.server.common.sps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test BlockDispatcher class.
 */
@Timeout(60)
public class TestBlockDispatcher {

  /**
   * Verify that when InvalidEncryptionKeyException is encountered during
   * block move, the dispatcher clears the cached data encryption key before
   * retry.
   */
  @Test
  public void testClearEncryptionKeyOnRetry() throws Exception {
    DatanodeInfo target =
        DFSTestUtil.getDatanodeInfo("127.0.0.1", "localhost", 1);
    DatanodeInfo source =
        DFSTestUtil.getDatanodeInfo("127.0.0.1", "localhost", 2);

    BlockMovingInfo blkMovingInfo = new BlockMovingInfo(
        new Block(1, 100, 1001),
        source, target,
        StorageType.DISK, StorageType.ARCHIVE);

    InvalidKeySaslClient saslClient = new InvalidKeySaslClient();
    CountingKeyFactory km = new CountingKeyFactory();
    ExtendedBlock eb = new ExtendedBlock("bp-1", 1, 100, 1001);
    Token<BlockTokenIdentifier> accessToken = new Token<>();

    // Use small socketTimeout (100ms) to keep test fast.
    BlockDispatcher dispatcher = new BlockDispatcher(100, 1024, false) {
      @Override
      Socket newSocket() {
        return new FakeSocket();
      }
    };

    assertThrows(InvalidEncryptionKeyException.class,
        () -> dispatcher.moveBlock(blkMovingInfo, saslClient, eb,
            new FakeSocket(), km, accessToken));

    assertEquals(1, km.clearCount);
    assertEquals(2, saslClient.socketSendCount);
  }

  private static final class InvalidKeySaslClient
      extends SaslDataTransferClient {
    private int socketSendCount;

    private InvalidKeySaslClient() {
      super(null, null, null);
    }

    @Override
    public IOStreamPair socketSend(Socket socket, OutputStream underlyingOut,
        InputStream underlyingIn, DataEncryptionKeyFactory encryptionKeyFactory,
        Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
        throws InvalidEncryptionKeyException {
      socketSendCount++;
      throw new InvalidEncryptionKeyException("test: encryption key expired");
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
