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

package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestWebHdfsContentLength {
  private static ServerSocket listenSocket;
  private static String bindAddr;
  private static Path p;
  private static FileSystem fs;

  private static final Pattern contentLengthPattern = Pattern.compile(
      "^(Content-Length|Transfer-Encoding):\\s*(.*)", Pattern.MULTILINE);

  private static String errResponse =
      "HTTP/1.1 500 Boom\r\n" +
      "Content-Length: 0\r\n" +
      "Connection: close\r\n\r\n";
  private static String redirectResponse;

  private static ExecutorService executor;

  @BeforeClass
  public static void setup() throws IOException {
    listenSocket = new ServerSocket();
    listenSocket.bind(null);
    bindAddr = NetUtils.getHostPortString(
        (InetSocketAddress)listenSocket.getLocalSocketAddress());
    redirectResponse =
        "HTTP/1.1 307 Redirect\r\n" +
        "Location: http://"+bindAddr+"/path\r\n" +
        "Connection: close\r\n\r\n";

    p = new Path("webhdfs://"+bindAddr+"/path");
    fs = p.getFileSystem(new Configuration());
    executor = Executors.newSingleThreadExecutor();    
  }
  
  @AfterClass
  public static void teardown() throws IOException {
    if (listenSocket != null) {
      listenSocket.close();
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void testGetOp() throws Exception {
    Future<String> future = contentLengthFuture(errResponse);
    try {
      fs.getFileStatus(p);
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals(null, getContentLength(future));
  }

  @Test
  public void testGetOpWithRedirect() {
    Future<String> future1 = contentLengthFuture(redirectResponse);
    Future<String> future2 = contentLengthFuture(errResponse);
    try {
      fs.open(p).read();
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals(null, getContentLength(future1));
    Assert.assertEquals(null, getContentLength(future2));
  }
  
  @Test
  public void testPutOp() {
    Future<String> future = contentLengthFuture(errResponse);
    try {
      fs.mkdirs(p);
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals("0", getContentLength(future));
  }

  @Test
  public void testPutOpWithRedirect() {
    Future<String> future1 = contentLengthFuture(redirectResponse);
    Future<String> future2 = contentLengthFuture(errResponse);
    try {
      FSDataOutputStream os = fs.create(p);
      os.write(new byte[]{0});
      os.close();
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals("0", getContentLength(future1));
    Assert.assertEquals("chunked", getContentLength(future2));
  }
  
  @Test
  public void testPostOp() {  
    Future<String> future = contentLengthFuture(errResponse);
    try {
      fs.concat(p, new Path[]{p});
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals("0", getContentLength(future));
  }
  
  @Test
  public void testPostOpWithRedirect() {
    // POST operation with redirect
    Future<String> future1 = contentLengthFuture(redirectResponse);
    Future<String> future2 = contentLengthFuture(errResponse);
    try {
      FSDataOutputStream os = fs.append(p);
      os.write(new byte[]{0});
      os.close();
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals("0", getContentLength(future1));
    Assert.assertEquals("chunked", getContentLength(future2));
  }
  
  @Test
  public void testDelete() {
    Future<String> future = contentLengthFuture(errResponse);
    try {
      fs.delete(p, false);
      Assert.fail();
    } catch (IOException ioe) {} // expected
    Assert.assertEquals(null, getContentLength(future));
  }  

  private String getContentLength(Future<String> future)  {
    String request = null;
    try {
      request = future.get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      Assert.fail(e.toString());
    }
    Matcher matcher = contentLengthPattern.matcher(request);
    return matcher.find() ? matcher.group(2) : null;
  }
  
  private Future<String> contentLengthFuture(final String response) {
    return executor.submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Socket client = listenSocket.accept();
        client.setSoTimeout(2000);
        try {
          client.getOutputStream().write(response.getBytes());
          client.shutdownOutput();
          byte[] buf = new byte[4*1024]; // much bigger than request
          int n = client.getInputStream().read(buf);
          return new String(buf, 0, n);
        } finally {
          client.close();
        }
      }
    });
  }
}
