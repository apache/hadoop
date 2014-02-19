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
package org.apache.hadoop.net.unix;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket.DomainChannel;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;

import com.google.common.io.Files;

public class TestDomainSocket {
  private static TemporarySocketDirectory sockDir;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }
  
  @Before
  public void before() {
    Assume.assumeTrue(DomainSocket.getLoadingFailureReason() == null);
  }
    
  /**
   * Test that we can create a socket and close it, even if it hasn't been
   * opened.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketCreateAndClose() throws IOException {
    DomainSocket serv = DomainSocket.bindAndListen(
      new File(sockDir.getDir(), "test_sock_create_and_close").
        getAbsolutePath());
    serv.close();
  }

  /**
   * Test DomainSocket path setting and getting.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketPathSetGet() throws IOException {
    Assert.assertEquals("/var/run/hdfs/sock.100",
        DomainSocket.getEffectivePath("/var/run/hdfs/sock._PORT", 100));
  }

  /**
   * Test that we get a read result of -1 on EOF.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketReadEof() throws Exception {
    final String TEST_PATH = new File(sockDir.getDir(),
        "testSocketReadEof").getAbsolutePath();
    final DomainSocket serv = DomainSocket.bindAndListen(TEST_PATH);
    ExecutorService exeServ = Executors.newSingleThreadExecutor();
    Callable<Void> callable = new Callable<Void>() {
      public Void call(){
        DomainSocket conn;
        try {
          conn = serv.accept();
        } catch (IOException e) {
          throw new RuntimeException("unexpected IOException", e);
        }
        byte buf[] = new byte[100];
        for (int i = 0; i < buf.length; i++) {
          buf[i] = 0;
        }
        try {
          Assert.assertEquals(-1, conn.getInputStream().read());
        } catch (IOException e) {
          throw new RuntimeException("unexpected IOException", e);
        }
        return null;
      }
    };
    Future<Void> future = exeServ.submit(callable);
    DomainSocket conn = DomainSocket.connect(serv.getPath());
    Thread.sleep(50);
    conn.close();
    serv.close();
    future.get(2, TimeUnit.MINUTES);
  }

  /**
   * Test that if one thread is blocking in a read or write operation, another
   * thread can close the socket and stop the accept.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketAcceptAndClose() throws Exception {
    final String TEST_PATH =
        new File(sockDir.getDir(), "test_sock_accept_and_close").getAbsolutePath();
    final DomainSocket serv = DomainSocket.bindAndListen(TEST_PATH);
    ExecutorService exeServ = Executors.newSingleThreadExecutor();
    Callable<Void> callable = new Callable<Void>() {
      public Void call(){
        try {
          serv.accept();
          throw new RuntimeException("expected the accept() to be " +
              "interrupted and fail");
        } catch (AsynchronousCloseException e) {
          return null;
        } catch (IOException e) {
          throw new RuntimeException("unexpected IOException", e);
        }
      }
    };
    Future<Void> future = exeServ.submit(callable);
    Thread.sleep(500);
    serv.close();
    future.get(2, TimeUnit.MINUTES);
  }

  /**
   * Test that we get an AsynchronousCloseException when the DomainSocket
   * we're using is closed during a read or write operation.
   *
   * @throws IOException
   */
  private void testAsyncCloseDuringIO(final boolean closeDuringWrite)
      throws Exception {
    final String TEST_PATH = new File(sockDir.getDir(),
        "testAsyncCloseDuringIO(" + closeDuringWrite + ")").getAbsolutePath();
    final DomainSocket serv = DomainSocket.bindAndListen(TEST_PATH);
    ExecutorService exeServ = Executors.newFixedThreadPool(2);
    Callable<Void> serverCallable = new Callable<Void>() {
      public Void call() {
        DomainSocket serverConn = null;
        try {
          serverConn = serv.accept();
          byte buf[] = new byte[100];
          for (int i = 0; i < buf.length; i++) {
            buf[i] = 0;
          }
          // The server just continues either writing or reading until someone
          // asynchronously closes the client's socket.  At that point, all our
          // reads return EOF, and writes get a socket error.
          if (closeDuringWrite) {
            try {
              while (true) {
                serverConn.getOutputStream().write(buf);
              }
            } catch (IOException e) {
            }
          } else {
            do { ; } while 
              (serverConn.getInputStream().read(buf, 0, buf.length) != -1);
          }
        } catch (IOException e) {
          throw new RuntimeException("unexpected IOException", e);
        } finally {
          IOUtils.cleanup(DomainSocket.LOG, serverConn);
        }
        return null;
      }
    };
    Future<Void> serverFuture = exeServ.submit(serverCallable);
    final DomainSocket clientConn = DomainSocket.connect(serv.getPath());
    Callable<Void> clientCallable = new Callable<Void>() {
      public Void call(){
        // The client writes or reads until another thread
        // asynchronously closes the socket.  At that point, we should
        // get ClosedChannelException, or possibly its subclass
        // AsynchronousCloseException.
        byte buf[] = new byte[100];
        for (int i = 0; i < buf.length; i++) {
          buf[i] = 0;
        }
        try {
          if (closeDuringWrite) {
            while (true) {
              clientConn.getOutputStream().write(buf);
            }
          } else {
            while (true) {
              clientConn.getInputStream().read(buf, 0, buf.length);
            }
          }
        } catch (ClosedChannelException e) {
          return null;
        } catch (IOException e) {
          throw new RuntimeException("unexpected IOException", e);
        }
      }
    };
    Future<Void> clientFuture = exeServ.submit(clientCallable);
    Thread.sleep(500);
    clientConn.close();
    serv.close();
    clientFuture.get(2, TimeUnit.MINUTES);
    serverFuture.get(2, TimeUnit.MINUTES);
  }
  
  @Test(timeout=180000)
  public void testAsyncCloseDuringWrite() throws Exception {
    testAsyncCloseDuringIO(true);
  }
  
  @Test(timeout=180000)
  public void testAsyncCloseDuringRead() throws Exception {
    testAsyncCloseDuringIO(false);
  }
  
  /**
   * Test that attempting to connect to an invalid path doesn't work.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testInvalidOperations() throws IOException {
    try {
      DomainSocket.connect(
        new File(sockDir.getDir(), "test_sock_invalid_operation").
          getAbsolutePath());
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("connect(2) error: ", e);
    }
  }

  /**
   * Test setting some server options.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testServerOptions() throws Exception {
    final String TEST_PATH = new File(sockDir.getDir(),
        "test_sock_server_options").getAbsolutePath();
    DomainSocket serv = DomainSocket.bindAndListen(TEST_PATH);
    try {
      // Let's set a new receive buffer size
      int bufSize = serv.getAttribute(DomainSocket.RECEIVE_BUFFER_SIZE);
      int newBufSize = bufSize / 2;
      serv.setAttribute(DomainSocket.RECEIVE_BUFFER_SIZE, newBufSize);
      int nextBufSize = serv.getAttribute(DomainSocket.RECEIVE_BUFFER_SIZE);
      Assert.assertEquals(newBufSize, nextBufSize);
      // Let's set a server timeout
      int newTimeout = 1000;
      serv.setAttribute(DomainSocket.RECEIVE_TIMEOUT, newTimeout);
      int nextTimeout = serv.getAttribute(DomainSocket.RECEIVE_TIMEOUT);
      Assert.assertEquals(newTimeout, nextTimeout);
      try {
        serv.accept();
        Assert.fail("expected the accept() to time out and fail");
      } catch (SocketTimeoutException e) {
        GenericTestUtils.assertExceptionContains("accept(2) error: ", e);
      }
    } finally {
      serv.close();
      Assert.assertFalse(serv.isOpen());
    }
  }
  
  /**
   * A Throwable representing success.
   *
   * We can't use null to represent this, because you cannot insert null into
   * ArrayBlockingQueue.
   */
  static class Success extends Throwable {
    private static final long serialVersionUID = 1L;
  }
  
  static interface WriteStrategy {
    /**
     * Initialize a WriteStrategy object from a Socket.
     */
    public void init(DomainSocket s) throws IOException;
    
    /**
     * Write some bytes.
     */
    public void write(byte b[]) throws IOException;
  }
  
  static class OutputStreamWriteStrategy implements WriteStrategy {
    private OutputStream outs = null;
    
    public void init(DomainSocket s) throws IOException {
      outs = s.getOutputStream();
    }
    
    public void write(byte b[]) throws IOException {
      outs.write(b);
    }
  }
  
  abstract static class ReadStrategy {
    /**
     * Initialize a ReadStrategy object from a DomainSocket.
     */
    public abstract void init(DomainSocket s) throws IOException;
    
    /**
     * Read some bytes.
     */
    public abstract int read(byte b[], int off, int length) throws IOException;
    
    public void readFully(byte buf[], int off, int len) throws IOException {
      int toRead = len;
      while (toRead > 0) {
        int ret = read(buf, off, toRead);
        if (ret < 0) {
          throw new IOException( "Premature EOF from inputStream");
        }
        toRead -= ret;
        off += ret;
      }
    }
  }
  
  static class InputStreamReadStrategy extends ReadStrategy {
    private InputStream ins = null;
    
    @Override
    public void init(DomainSocket s) throws IOException {
      ins = s.getInputStream();
    }

    @Override
    public int read(byte b[], int off, int length) throws IOException {
      return ins.read(b, off, length);
    }
  }
  
  static class DirectByteBufferReadStrategy extends ReadStrategy {
    private DomainChannel ch = null;

    @Override
    public void init(DomainSocket s) throws IOException {
      ch = s.getChannel();
    }
    
    @Override
    public int read(byte b[], int off, int length) throws IOException {
      ByteBuffer buf = ByteBuffer.allocateDirect(b.length);
      int nread = ch.read(buf);
      if (nread < 0) return nread;
      buf.flip();
      buf.get(b, off, nread);
      return nread;
    }
  }

  static class ArrayBackedByteBufferReadStrategy extends ReadStrategy {
    private DomainChannel ch = null;
    
    @Override
    public void init(DomainSocket s) throws IOException {
      ch = s.getChannel();
    }
    
    @Override
    public int read(byte b[], int off, int length) throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(b);
      int nread = ch.read(buf);
      if (nread < 0) return nread;
      buf.flip();
      buf.get(b, off, nread);
      return nread;
    }
  }
  
  /**
   * Test a simple client/server interaction.
   *
   * @throws IOException
   */
  void testClientServer1(final Class<? extends WriteStrategy> writeStrategyClass,
      final Class<? extends ReadStrategy> readStrategyClass,
      final DomainSocket preConnectedSockets[]) throws Exception {
    final String TEST_PATH = new File(sockDir.getDir(),
        "test_sock_client_server1").getAbsolutePath();
    final byte clientMsg1[] = new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6 };
    final byte serverMsg1[] = new byte[] { 0x9, 0x8, 0x7, 0x6, 0x5 };
    final byte clientMsg2 = 0x45;
    final ArrayBlockingQueue<Throwable> threadResults =
        new ArrayBlockingQueue<Throwable>(2);
    final DomainSocket serv = (preConnectedSockets != null) ?
      null : DomainSocket.bindAndListen(TEST_PATH);
    Thread serverThread = new Thread() {
      public void run(){
        // Run server
        DomainSocket conn = null;
        try {
          conn = preConnectedSockets != null ?
                    preConnectedSockets[0] : serv.accept();
          byte in1[] = new byte[clientMsg1.length];
          ReadStrategy reader = readStrategyClass.newInstance();
          reader.init(conn);
          reader.readFully(in1, 0, in1.length);
          Assert.assertTrue(Arrays.equals(clientMsg1, in1));
          WriteStrategy writer = writeStrategyClass.newInstance();
          writer.init(conn);
          writer.write(serverMsg1);
          InputStream connInputStream = conn.getInputStream();
          int in2 = connInputStream.read();
          Assert.assertEquals((int)clientMsg2, in2);
          conn.close();
        } catch (Throwable e) {
          threadResults.add(e);
          Assert.fail(e.getMessage());
        }
        threadResults.add(new Success());
      }
    };
    serverThread.start();
    
    Thread clientThread = new Thread() {
      public void run(){
        try {
          DomainSocket client = preConnectedSockets != null ?
                preConnectedSockets[1] : DomainSocket.connect(TEST_PATH);
          WriteStrategy writer = writeStrategyClass.newInstance();
          writer.init(client);
          writer.write(clientMsg1);
          ReadStrategy reader = readStrategyClass.newInstance();
          reader.init(client);
          byte in1[] = new byte[serverMsg1.length];
          reader.readFully(in1, 0, in1.length);
          Assert.assertTrue(Arrays.equals(serverMsg1, in1));
          OutputStream clientOutputStream = client.getOutputStream();
          clientOutputStream.write(clientMsg2);
          client.close();
        } catch (Throwable e) {
          threadResults.add(e);
        }
        threadResults.add(new Success());
      }
    };
    clientThread.start();
    
    for (int i = 0; i < 2; i++) {
      Throwable t = threadResults.take();
      if (!(t instanceof Success)) {
        Assert.fail(t.getMessage() + ExceptionUtils.getStackTrace(t));
      }
    }
    serverThread.join(120000);
    clientThread.join(120000);
    if (serv != null) {
      serv.close();
    }
  }

  @Test(timeout=180000)
  public void testClientServerOutStreamInStream() throws Exception {
    testClientServer1(OutputStreamWriteStrategy.class,
        InputStreamReadStrategy.class, null);
  }

  @Test(timeout=180000)
  public void testClientServerOutStreamInStreamWithSocketpair() throws Exception {
    testClientServer1(OutputStreamWriteStrategy.class,
        InputStreamReadStrategy.class, DomainSocket.socketpair());
  }

  @Test(timeout=180000)
  public void testClientServerOutStreamInDbb() throws Exception {
    testClientServer1(OutputStreamWriteStrategy.class,
        DirectByteBufferReadStrategy.class, null);
  }

  @Test(timeout=180000)
  public void testClientServerOutStreamInDbbWithSocketpair() throws Exception {
    testClientServer1(OutputStreamWriteStrategy.class,
        DirectByteBufferReadStrategy.class, DomainSocket.socketpair());
  }

  @Test(timeout=180000)
  public void testClientServerOutStreamInAbb() throws Exception {
    testClientServer1(OutputStreamWriteStrategy.class,
        ArrayBackedByteBufferReadStrategy.class, null);
  }

  @Test(timeout=180000)
  public void testClientServerOutStreamInAbbWithSocketpair() throws Exception {
    testClientServer1(OutputStreamWriteStrategy.class,
        ArrayBackedByteBufferReadStrategy.class, DomainSocket.socketpair());
  }

  static private class PassedFile {
    private final int idx;
    private final byte[] contents;
    private FileInputStream fis;
    
    public PassedFile(int idx) throws IOException {
      this.idx = idx;
      this.contents = new byte[] { (byte)(idx % 127) };
      Files.write(contents, new File(getPath()));
      this.fis = new FileInputStream(getPath());
    }

    public String getPath() {
      return new File(sockDir.getDir(), "passed_file" + idx).getAbsolutePath();
    }

    public FileInputStream getInputStream() throws IOException {
      return fis;
    }
    
    public void cleanup() throws IOException {
      new File(getPath()).delete();
      fis.close();
    }

    public void checkInputStream(FileInputStream fis) throws IOException {
      byte buf[] = new byte[contents.length];
      IOUtils.readFully(fis, buf, 0, buf.length);
      Arrays.equals(contents, buf);
    }
    
    protected void finalize() {
      try {
        cleanup();
      } catch(Throwable t) {
        // ignore
      }
    }
  }

  /**
   * Test file descriptor passing.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testFdPassing() throws Exception {
    final String TEST_PATH =
        new File(sockDir.getDir(), "test_sock").getAbsolutePath();
    final byte clientMsg1[] = new byte[] { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66 };
    final byte serverMsg1[] = new byte[] { 0x31, 0x30, 0x32, 0x34, 0x31, 0x33,
          0x44, 0x1, 0x1, 0x1, 0x1, 0x1 };
    final ArrayBlockingQueue<Throwable> threadResults =
        new ArrayBlockingQueue<Throwable>(2);
    final DomainSocket serv = DomainSocket.bindAndListen(TEST_PATH);
    final PassedFile passedFiles[] =
        new PassedFile[] { new PassedFile(1), new PassedFile(2) };
    final FileDescriptor passedFds[] = new FileDescriptor[passedFiles.length];
    for (int i = 0; i < passedFiles.length; i++) {
      passedFds[i] = passedFiles[i].getInputStream().getFD();
    }
    Thread serverThread = new Thread() {
      public void run(){
        // Run server
        DomainSocket conn = null;
        try {
          conn = serv.accept();
          byte in1[] = new byte[clientMsg1.length];
          InputStream connInputStream = conn.getInputStream();
          IOUtils.readFully(connInputStream, in1, 0, in1.length);
          Assert.assertTrue(Arrays.equals(clientMsg1, in1));
          DomainSocket domainConn = (DomainSocket)conn;
          domainConn.sendFileDescriptors(passedFds, serverMsg1, 0,
              serverMsg1.length);
          conn.close();
        } catch (Throwable e) {
          threadResults.add(e);
          Assert.fail(e.getMessage());
        }
        threadResults.add(new Success());
      }
    };
    serverThread.start();

    Thread clientThread = new Thread() {
      public void run(){
        try {
          DomainSocket client = DomainSocket.connect(TEST_PATH);
          OutputStream clientOutputStream = client.getOutputStream();
          InputStream clientInputStream = client.getInputStream();
          clientOutputStream.write(clientMsg1);
          DomainSocket domainConn = (DomainSocket)client;
          byte in1[] = new byte[serverMsg1.length];
          FileInputStream recvFis[] = new FileInputStream[passedFds.length];
          int r = domainConn.
              recvFileInputStreams(recvFis, in1, 0, in1.length - 1);
          Assert.assertTrue(r > 0);
          IOUtils.readFully(clientInputStream, in1, r, in1.length - r);
          Assert.assertTrue(Arrays.equals(serverMsg1, in1));
          for (int i = 0; i < passedFds.length; i++) {
            Assert.assertNotNull(recvFis[i]);
            passedFiles[i].checkInputStream(recvFis[i]);
          }
          for (FileInputStream fis : recvFis) {
            fis.close();
          }
          client.close();
        } catch (Throwable e) {
          threadResults.add(e);
        }
        threadResults.add(new Success());
      }
    };
    clientThread.start();
    
    for (int i = 0; i < 2; i++) {
      Throwable t = threadResults.take();
      if (!(t instanceof Success)) {
        Assert.fail(t.getMessage() + ExceptionUtils.getStackTrace(t));
      }
    }
    serverThread.join(120000);
    clientThread.join(120000);
    serv.close();
    for (PassedFile pf : passedFiles) {
      pf.cleanup();
    }
  }
  
  /**
   * Run validateSocketPathSecurity
   *
   * @param str            The path to validate
   * @param prefix         A prefix to skip validation for
   * @throws IOException
   */
  private static void testValidateSocketPath(String str, String prefix)
      throws IOException {
    int skipComponents = 1;
    File prefixFile = new File(prefix);
    while (true) {
      prefixFile = prefixFile.getParentFile();
      if (prefixFile == null) {
        break;
      }
      skipComponents++;
    }
    DomainSocket.validateSocketPathSecurity0(str,
        skipComponents);
  }
  
  /**
   * Test file descriptor path security.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testFdPassingPathSecurity() throws Exception {
    TemporarySocketDirectory tmp = new TemporarySocketDirectory();
    try {
      String prefix = tmp.getDir().getAbsolutePath();
      Shell.execCommand(new String [] {
          "mkdir", "-p", prefix + "/foo/bar/baz" });
      Shell.execCommand(new String [] {
          "chmod", "0700", prefix + "/foo/bar/baz" });
      Shell.execCommand(new String [] {
          "chmod", "0700", prefix + "/foo/bar" });
      Shell.execCommand(new String [] {
          "chmod", "0707", prefix + "/foo" });
      Shell.execCommand(new String [] {
          "mkdir", "-p", prefix + "/q1/q2" });
      Shell.execCommand(new String [] {
          "chmod", "0700", prefix + "/q1" });
      Shell.execCommand(new String [] {
          "chmod", "0700", prefix + "/q1/q2" });
      testValidateSocketPath(prefix + "/q1/q2", prefix);
      try {
        testValidateSocketPath(prefix + "/foo/bar/baz", prefix);
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains("/foo' is world-writable.  " +
            "Its permissions are 0707.  Please fix this or select a " +
            "different socket path.", e);
      }
      try {
        testValidateSocketPath(prefix + "/nope", prefix);
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains("failed to stat a path " +
            "component: ", e);
      }
      // Root should be secure
      DomainSocket.validateSocketPathSecurity0("/foo", 1);
    } finally {
      tmp.close();
    }
  }

  @Test(timeout=180000)
  public void testShutdown() throws Exception {
    final AtomicInteger bytesRead = new AtomicInteger(0);
    final AtomicBoolean failed = new AtomicBoolean(false);
    final DomainSocket[] socks = DomainSocket.socketpair();
    Runnable reader = new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            int ret = socks[1].getInputStream().read();
            if (ret == -1) return;
            bytesRead.addAndGet(1);
          } catch (IOException e) {
            DomainSocket.LOG.error("reader error", e);
            failed.set(true);
            return;
          }
        }
      }
    };
    Thread readerThread = new Thread(reader);
    readerThread.start();
    socks[0].getOutputStream().write(1);
    socks[0].getOutputStream().write(2);
    socks[0].getOutputStream().write(3);
    Assert.assertTrue(readerThread.isAlive());
    socks[0].shutdown();
    readerThread.join();
    Assert.assertFalse(failed.get());
    Assert.assertEquals(3, bytesRead.get());
    IOUtils.cleanup(null, socks);
  }
}
