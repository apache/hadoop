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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestHttp2ClientMultiThread extends AbstractTestHttp2Client {

  private int requestCount = 10000;

  private int concurrency = 10;

  private ExecutorService executor = Executors.newFixedThreadPool(concurrency,
    new ThreadFactoryBuilder().setNameFormat("Echo-Client-%d").setDaemon(true)
        .build());

  @Before
  public void setUp() throws Exception {
    start();
  }

  @After
  public void tearDown() throws Exception {
    stop();
  }

  private void testEcho() throws InterruptedException, ExecutionException,
      IOException {
    Http2StreamChannel stream = connect(false);
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    byte[] b = new byte[ThreadLocalRandom.current().nextInt(10, 100)];
    ThreadLocalRandom.current().nextBytes(b);
    stream.write(stream.alloc().buffer(b.length).writeBytes(b));
    stream.writeAndFlush(LastHttp2Message.get());
    assertEquals(receiver.waitForResponse().status(),
      HttpResponseStatus.OK.codeAsText());
    assertArrayEquals(b, ByteStreams.toByteArray(receiver.content()));
  }

  @Test
  public void test() throws InterruptedException {
    final AtomicBoolean succ = new AtomicBoolean(true);
    for (int i = 0; i < requestCount; i++) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            testEcho();
          } catch (Throwable t) {
            t.printStackTrace();
            succ.set(false);
          }
        }
      });
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.MINUTES));
    assertTrue(succ.get());
  }

  @Override
  protected void setHandler(Server server) {
    server.setHandler(new EchoHandler());
  }
}