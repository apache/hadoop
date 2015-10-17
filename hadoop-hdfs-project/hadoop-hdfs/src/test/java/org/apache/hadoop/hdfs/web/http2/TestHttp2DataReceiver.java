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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class TestHttp2DataReceiver extends AbstractTestHttp2Client {

  private File largeFile = new File(".largeFile");

  @Before
  public void setUp() throws Exception {
    byte[] b = new byte[64 * 1024];
    try (FileOutputStream out = new FileOutputStream(largeFile)) {
      for (int i = 0; i < 1024; i++) {
        ThreadLocalRandom.current().nextBytes(b);
        out.write(b);
      }
    }
    largeFile.deleteOnExit();
    start();
  }

  @After
  public void tearDown() throws Exception {
    stop();
    largeFile.delete();
  }

  @Override
  protected void setHandler(Server server) {
    server.setHandler(new AbstractHandler() {

      @Override
      public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
        Files.copy(largeFile, response.getOutputStream());
        response.getOutputStream().flush();
      }
    });
  }

  private void assertContentEquals(byte[] expected, byte[] actual, int length) {
    for (int i = 0; i < length; i++) {
      assertEquals("differ at index " + i + ", expected " + expected[i]
          + ", actual " + actual[i], expected[i], actual[i]);
    }
  }

  @Test
  public void test() throws InterruptedException, ExecutionException,
      IOException {
    Http2StreamChannel stream = connect(true);
    Http2DataReceiver receiver = stream.pipeline().get(Http2DataReceiver.class);
    assertEquals(HttpResponseStatus.OK.codeAsText(), receiver.waitForResponse()
        .status());
    byte[] buf = new byte[4 * 1024];
    byte[] fileBuf = new byte[buf.length];
    try (InputStream in = receiver.content();
        FileInputStream fileIn = new FileInputStream(largeFile)) {
      for (;;) {
        int read = in.read(buf);
        if (read == -1) {
          assertEquals(-1, fileIn.read());
          break;
        }
        ByteStreams.readFully(fileIn, fileBuf, 0, read);
        assertContentEquals(fileBuf, buf, read);
      }
    }
  }
}
