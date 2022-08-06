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

package org.apache.hadoop.ipc.netty.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcException;
import org.apache.hadoop.security.SaslRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

/** Manages the input and output streams for an IPC connection.
 *  Only exposed for use by SaslRpcClient.
 */
@InterfaceAudience.Private
public abstract class IpcStreams implements Closeable, Flushable {
  public DataInputStream in;
  public DataOutputStream out;
  private int maxResponseLength;
  private boolean firstResponse = true;
  public static boolean useSSLSelfSignedCertificate = true;
  public static Configuration conf = null;
  public static final Logger LOG = LoggerFactory.getLogger(Client.class);

  public static IpcStreams newInstance(Socket socket, int maxResponseLength,
                                       Configuration conf) throws IOException {
    IpcStreams.conf = conf;
    boolean useNettySSL = conf.getBoolean(
        CommonConfigurationKeys.IPC_SSL_KEY,
        CommonConfigurationKeys.IPC_SSL_DEFAULT);

    useSSLSelfSignedCertificate = conf.getBoolean(
        CommonConfigurationKeys.IPC_SSL_SELF_SIGNED_CERTIFICATE_TEST,
        CommonConfigurationKeys.IPC_SSL_SELF_SIGNED_CERTIFICATE_TEST_DEFAULT);

    // The SSL implementation works only if a channel is available.
    if (useNettySSL && socket.getChannel() == null) {
      throw new IOException("Unable to initialize SSL since the " +
          "socket channel is unavailable.");
    }

    // Initialize the correct IO Stream based on the type of connection
    // request.
    IpcStreams streams = useNettySSL ?
        new NettyIpcStreams(socket) : new NioIpcStreams(socket);
    streams.maxResponseLength = maxResponseLength;
    return streams;
  }

  public abstract Future<?> submit(Runnable call);

  public void setSaslClient(SaslRpcClient client) throws IOException {
    // Wrap the input stream in a BufferedInputStream to fill the buffer
    // before reading its length (HADOOP-14062).
    setInputStream(new BufferedInputStream(client.getInputStream(in)));
    setOutputStream(client.getOutputStream(out));
  }

  public void setInputStream(InputStream is) {
    this.in = (is instanceof DataInputStream)
        ? (DataInputStream)is : new DataInputStream(is);
  }

  void setOutputStream(OutputStream os) {
    this.out = (os instanceof DataOutputStream)
        ? (DataOutputStream)os : new DataOutputStream(os);
  }

  public ByteBuffer readResponse() throws IOException {
    int length = in.readInt();
    if (firstResponse) {
      firstResponse = false;
      // pre-rpcv9 exception, almost certainly a version mismatch.
      if (length == -1) {
        in.readInt(); // ignore fatal/error status, it's fatal for us.
        throw new RemoteException(WritableUtils.readString(in),
            WritableUtils.readString(in));
      }
    }
    if (length <= 0) {
      throw new RpcException("RPC response has invalid length");
    }
    if (maxResponseLength > 0 && length > maxResponseLength) {
      throw new RpcException("RPC response exceeds maximum data length");
    }
    ByteBuffer bb = ByteBuffer.allocate(length);
    in.readFully(bb.array());
    return bb;
  }

  public void sendRequest(byte[] buf) throws IOException {
    out.write(buf);
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() {
    IOUtils.closeStream(out);
    IOUtils.closeStream(in);
  }

}