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
package org.apache.hadoop.hdfs.web.http2;

import static org.junit.Assert.assertEquals;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.ByteStreams;

public class TestHttp2Client extends AbstractTestHttp2Client {

  @Before
  public void setUp() throws Exception {
    start();
  }

  @After
  public void tearDown() throws Exception {
    stop();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException,
      IOException {
    Http2StreamChannel stream = connect(false);
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    stream.write(stream.alloc().buffer()
        .writeBytes("Hello World".getBytes(StandardCharsets.UTF_8)));
    stream.writeAndFlush(LastHttp2Message.get());
    assertEquals(receiver.waitForResponse().status(),
      HttpResponseStatus.OK.codeAsText());
    assertEquals("Hello World",
      new String(ByteStreams.toByteArray(receiver.content()),
          StandardCharsets.UTF_8));
  }

  @Override
  protected void setHandler(Server server) {
    server.setHandler(new EchoHandler());
  }
}