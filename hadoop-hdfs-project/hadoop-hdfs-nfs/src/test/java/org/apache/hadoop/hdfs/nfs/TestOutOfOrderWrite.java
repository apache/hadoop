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

package org.apache.hadoop.hdfs.nfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.nfs.nfs3.Nfs3Utils;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.CREATE3Request;
import org.apache.hadoop.nfs.nfs3.request.SetAttr3;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.oncrpc.RegistrationClient;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcReply;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.oncrpc.SimpleTcpClient;
import org.apache.hadoop.oncrpc.SimpleTcpClientHandler;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;

public class TestOutOfOrderWrite {
  public final static Logger LOG =
      LoggerFactory.getLogger(TestOutOfOrderWrite.class);

  static FileHandle handle = null;
  static Channel channel;

  static byte[] data1 = new byte[1000];
  static byte[] data2 = new byte[1000];
  static byte[] data3 = new byte[1000];

  static XDR create() {
    XDR request = new XDR();
    RpcCall.getInstance(0x8000004c, Nfs3Constant.PROGRAM, Nfs3Constant.VERSION,
        Nfs3Constant.NFSPROC3.CREATE.getValue(), new CredentialsNone(),
        new VerifierNone()).write(request);

    SetAttr3 objAttr = new SetAttr3();
    CREATE3Request createReq = new CREATE3Request(new FileHandle("/"),
        "out-of-order-write" + System.currentTimeMillis(), 0, objAttr, 0);
    createReq.serialize(request);
    return request;
  }

  static XDR write(FileHandle handle, int xid, long offset, int count,
      byte[] data) {
    XDR request = new XDR();
    RpcCall.getInstance(xid, Nfs3Constant.PROGRAM, Nfs3Constant.VERSION,
        Nfs3Constant.NFSPROC3.CREATE.getValue(), new CredentialsNone(),
        new VerifierNone()).write(request);

    WRITE3Request write1 = new WRITE3Request(handle, offset, count,
        WriteStableHow.UNSTABLE, ByteBuffer.wrap(data));
    write1.serialize(request);
    return request;
  }

  static void testRequest(XDR request) {
    RegistrationClient registrationClient = new RegistrationClient("localhost",
        Nfs3Constant.SUN_RPCBIND, request);
    registrationClient.run();
  }

  static class WriteHandler extends SimpleTcpClientHandler {

    public WriteHandler(XDR request) {
      super(request);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // Get handle from create response
      ChannelBuffer buf = (ChannelBuffer) e.getMessage();
      XDR rsp = new XDR(buf.array());
      if (rsp.getBytes().length == 0) {
        LOG.info("rsp length is zero, why?");
        return;
      }
      LOG.info("rsp length=" + rsp.getBytes().length);

      RpcReply reply = RpcReply.read(rsp);
      int xid = reply.getXid();
      // Only process the create response
      if (xid != 0x8000004c) {
        return;
      }
      int status = rsp.readInt();
      if (status != Nfs3Status.NFS3_OK) {
        LOG.error("Create failed, status =" + status);
        return;
      }
      LOG.info("Create succeeded");
      rsp.readBoolean(); // value follow
      handle = new FileHandle();
      handle.deserialize(rsp);
      channel = e.getChannel();
    }
  }

  static class WriteClient extends SimpleTcpClient {

    public WriteClient(String host, int port, XDR request, Boolean oneShot) {
      super(host, port, request, oneShot);
    }

    @Override
    protected ChannelPipelineFactory setPipelineFactory() {
      this.pipelineFactory = new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() {
          return Channels.pipeline(
              RpcUtil.constructRpcFrameDecoder(),
              new WriteHandler(request));
        }
      };
      return this.pipelineFactory;
    }

  }

  public static void main(String[] args) throws InterruptedException {

    Arrays.fill(data1, (byte) 7);
    Arrays.fill(data2, (byte) 8);
    Arrays.fill(data3, (byte) 9);

    // NFS3 Create request
    NfsConfiguration conf = new NfsConfiguration();
    WriteClient client = new WriteClient("localhost", conf.getInt(
        NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY,
        NfsConfigKeys.DFS_NFS_SERVER_PORT_DEFAULT), create(), false);
    client.run();

    while (handle == null) {
      Thread.sleep(1000);
      System.out.println("handle is still null...");
    }
    LOG.info("Send write1 request");

    XDR writeReq;

    writeReq = write(handle, 0x8000005c, 2000, 1000, data3);
    Nfs3Utils.writeChannel(channel, writeReq, 1);
    writeReq = write(handle, 0x8000005d, 1000, 1000, data2);
    Nfs3Utils.writeChannel(channel, writeReq, 2);
    writeReq = write(handle, 0x8000005e, 0, 1000, data1);
    Nfs3Utils.writeChannel(channel, writeReq, 3);

    // TODO: convert to Junit test, and validate result automatically
  }
}
