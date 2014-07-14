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
package org.apache.hadoop.hdfs.net;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.net.unix.DomainSocket;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;

/**
 * Represents a peer that we communicate with by using an encrypted
 * communications medium.
 */
@InterfaceAudience.Private
public class EncryptedPeer implements Peer {
  private final Peer enclosedPeer;

  /**
   * An encrypted InputStream.
   */
  private final InputStream in;
  
  /**
   * An encrypted OutputStream.
   */
  private final OutputStream out;
  
  /**
   * An encrypted ReadableByteChannel.
   */
  private final ReadableByteChannel channel;

  public EncryptedPeer(Peer enclosedPeer, IOStreamPair ios) {
    this.enclosedPeer = enclosedPeer;
    this.in = ios.in;
    this.out = ios.out;
    this.channel = ios.in instanceof ReadableByteChannel ? 
        (ReadableByteChannel)ios.in : null;
  }

  @Override
  public ReadableByteChannel getInputStreamChannel() {
    return channel;
  }

  @Override
  public void setReadTimeout(int timeoutMs) throws IOException {
    enclosedPeer.setReadTimeout(timeoutMs);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    return enclosedPeer.getReceiveBufferSize();
  }

  @Override
  public boolean getTcpNoDelay() throws IOException {
    return enclosedPeer.getTcpNoDelay();
  }

  @Override
  public void setWriteTimeout(int timeoutMs) throws IOException {
    enclosedPeer.setWriteTimeout(timeoutMs);
  }

  @Override
  public boolean isClosed() {
    return enclosedPeer.isClosed();
  }

  @Override
  public void close() throws IOException {
    try {
      in.close();
    } finally {
      try {
        out.close();
      } finally {
        enclosedPeer.close();
      }
    }
  }

  @Override
  public String getRemoteAddressString() {
    return enclosedPeer.getRemoteAddressString();
  }

  @Override
  public String getLocalAddressString() {
    return enclosedPeer.getLocalAddressString();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return in;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return out;
  }

  @Override
  public boolean isLocal() {
    return enclosedPeer.isLocal();
  }

  @Override
  public String toString() {
    return "EncryptedPeer(" + enclosedPeer + ")";
  }

  @Override
  public DomainSocket getDomainSocket() {
    return enclosedPeer.getDomainSocket();
  }

  @Override
  public boolean hasSecureChannel() {
    return true;
  }
}
