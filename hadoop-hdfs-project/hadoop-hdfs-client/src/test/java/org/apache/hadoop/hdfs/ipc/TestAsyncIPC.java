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
package org.apache.hadoop.hdfs.ipc;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.RpcChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ipc.protobuf.TestRpcProtos;
import org.apache.hadoop.hdfs.ipc.protobuf.TestRpcProtos.EchoRequestProto;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAsyncIPC {

  private static Configuration CONF;

  private static TestServer SERVER;

  private static int PORT;

  @BeforeClass
  public static void setUp() throws IOException {
    CONF = new Configuration();
    RPC.setProtocolEngine(CONF, TestRpcProtocolPB.class,
        ProtobufRpcEngine.class);
    SERVER = new TestServer(CONF);
    SERVER.start();
    PORT = SERVER.port();
  }

  @AfterClass
  public static void tearDown() {
    SERVER.stop();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    try (RpcClient client = new RpcClient()) {
      RpcChannel channel = client.createRpcChannel(TestRpcProtocolPB.class,
          new InetSocketAddress("localhost", PORT),
          UserGroupInformation.getCurrentUser());
      TestRpcProtos.TestRpcService.Interface stub =
          TestRpcProtos.TestRpcService.newStub(channel);
      Map<Integer, String> results = new HashMap<>();
      int count = 100;
      CountDownLatch latch = new CountDownLatch(count);
      for (int i = 0; i < count; i++) {
        final int index = i;
        stub.echo(new HdfsRpcController(),
            EchoRequestProto.newBuilder().setMessage("Echo-" + index).build(),
            resp -> {
              results.put(index, resp.getMessage());
              latch.countDown();
            });
      }
      latch.await();
      assertEquals(count, results.size());
      for (int i = 0; i < count; i++) {
        assertEquals("Echo-" + i, results.get(i));
      }
    }
  }
}
