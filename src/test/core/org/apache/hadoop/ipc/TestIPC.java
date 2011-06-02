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

package org.apache.hadoop.ipc;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;

import java.util.Random;
import java.io.DataInput;
import java.io.File;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import javax.net.SocketFactory;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;

/** Unit tests for IPC. */
public class TestIPC {
  public static final Log LOG =
    LogFactory.getLog(TestIPC.class);
  
  final private static Configuration conf = new Configuration();
  final static private int PING_INTERVAL = 1000;
  final static private int MIN_SLEEP_TIME = 1000;

  /**
   * Flag used to turn off the fault injection behavior
   * of the various writables.
   **/
  static boolean WRITABLE_FAULTS_ENABLED = true;
  
  static {
    Client.setPingInterval(conf, PING_INTERVAL);
  }

  private static final Random RANDOM = new Random();

  private static final String ADDRESS = "0.0.0.0";

  /** Directory where we can count open file descriptors on Linux */
  private static final File FD_DIR = new File("/proc/self/fd");

  private static class TestServer extends Server {
    private boolean sleep;
    private Class<? extends Writable> responseClass;

    public TestServer(int handlerCount, boolean sleep) throws IOException {
      this(handlerCount, sleep, LongWritable.class, null);
    }
    
    public TestServer(int handlerCount, boolean sleep,
        Class<? extends Writable> paramClass,
        Class<? extends Writable> responseClass) 
      throws IOException {
      super(ADDRESS, 0, paramClass, handlerCount, conf);
      this.sleep = sleep;
      this.responseClass = responseClass;
    }

    @Override
    public Writable call(Class<?> protocol, Writable param, long receiveTime)
        throws IOException {
      if (sleep) {
        // sleep a bit
        try {
          Thread.sleep(RANDOM.nextInt(PING_INTERVAL) + MIN_SLEEP_TIME);
        } catch (InterruptedException e) {}
      }
      if (responseClass != null) {
        try {
          return responseClass.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }  
      } else {
        return param;                               // echo param as result
      }
    }
  }

  private static class SerialCaller extends Thread {
    private Client client;
    private InetSocketAddress server;
    private int count;
    private boolean failed;

    public SerialCaller(Client client, InetSocketAddress server, int count) {
      this.client = client;
      this.server = server;
      this.count = count;
    }

    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          LongWritable param = new LongWritable(RANDOM.nextLong());
          LongWritable value =
            (LongWritable)client.call(param, server, null, null, 0, conf);
          if (!param.equals(value)) {
            LOG.fatal("Call failed!");
            failed = true;
            break;
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }
  }

  private static class ParallelCaller extends Thread {
    private Client client;
    private int count;
    private InetSocketAddress[] addresses;
    private boolean failed;
    
    public ParallelCaller(Client client, InetSocketAddress[] addresses,
                          int count) {
      this.client = client;
      this.addresses = addresses;
      this.count = count;
    }

    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          Writable[] params = new Writable[addresses.length];
          for (int j = 0; j < addresses.length; j++)
            params[j] = new LongWritable(RANDOM.nextLong());
          Writable[] values = client.call(params, addresses, null, null, conf);
          for (int j = 0; j < addresses.length; j++) {
            if (!params[j].equals(values[j])) {
              LOG.fatal("Call failed!");
              failed = true;
              break;
            }
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }
  }

  @Test
  public void testSerial() throws Exception {
    testSerial(3, false, 2, 5, 100);
    testSerial(3, true, 2, 5, 10);
  }

  public void testSerial(int handlerCount, boolean handlerSleep, 
                         int clientCount, int callerCount, int callCount)
    throws Exception {
    Server server = new TestServer(handlerCount, handlerSleep);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(LongWritable.class, conf);
    }
    
    SerialCaller[] callers = new SerialCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new SerialCaller(clients[i%clientCount], addr, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    server.stop();
  }
	
  @Test
  public void testParallel() throws Exception {
    testParallel(10, false, 2, 4, 2, 4, 100);
  }

  public void testParallel(int handlerCount, boolean handlerSleep,
                           int serverCount, int addressCount,
                           int clientCount, int callerCount, int callCount)
    throws Exception {
    Server[] servers = new Server[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = new TestServer(handlerCount, handlerSleep);
      servers[i].start();
    }

    InetSocketAddress[] addresses = new InetSocketAddress[addressCount];
    for (int i = 0; i < addressCount; i++) {
      addresses[i] = NetUtils.getConnectAddress(servers[i%serverCount]);
    }

    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(LongWritable.class, conf);
    }
    
    ParallelCaller[] callers = new ParallelCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] =
        new ParallelCaller(clients[i%clientCount], addresses, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    for (int i = 0; i < serverCount; i++) {
      servers[i].stop();
    }
  }
	
  @Test
  public void testStandAloneClient() throws Exception {
    testParallel(10, false, 2, 4, 2, 4, 100);
    Client client = new Client(LongWritable.class, conf);
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10);
    try {
      client.call(new LongWritable(RANDOM.nextLong()),
              address, null, null, 0, conf);
      fail("Expected an exception to have been thrown");
    } catch (IOException e) {
      String message = e.getMessage();
      String addressText = address.toString();
      assertTrue("Did not find "+addressText+" in "+message,
              message.contains(addressText));
      Throwable cause=e.getCause();
      assertNotNull("No nested exception in "+e,cause);
      String causeText=cause.getMessage();
      assertTrue("Did not find " + causeText + " in " + message,
              message.contains(causeText));
    }
  }
  
  static void maybeThrowIOE() throws IOException {
    if (WRITABLE_FAULTS_ENABLED) {
      throw new IOException("Injected fault");
    }
  }

  static void maybeThrowRTE() {
    if (WRITABLE_FAULTS_ENABLED) {
      throw new RuntimeException("Injected fault");
    }
  }

  @SuppressWarnings("unused")
  private static class IOEOnReadWritable extends LongWritable {
    public IOEOnReadWritable() {}

    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      maybeThrowIOE();
    }
  }
  
  @SuppressWarnings("unused")
  private static class RTEOnReadWritable extends LongWritable {
    public RTEOnReadWritable() {}
    
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      maybeThrowRTE();
    }
  }
  
  @SuppressWarnings("unused")
  private static class IOEOnWriteWritable extends LongWritable {
    public IOEOnWriteWritable() {}

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      maybeThrowIOE();
    }
  }

  @SuppressWarnings("unused")
  private static class RTEOnWriteWritable extends LongWritable {
    public RTEOnWriteWritable() {}

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      maybeThrowRTE();
    }
  }
  
  /**
   * Generic test case for exceptions thrown at some point in the IPC
   * process.
   * 
   * @param clientParamClass - client writes this writable for parameter
   * @param serverParamClass - server reads this writable for parameter
   * @param serverResponseClass - server writes this writable for response
   * @param clientResponseClass - client reads this writable for response
   */
  private void doErrorTest(
      Class<? extends LongWritable> clientParamClass,
      Class<? extends LongWritable> serverParamClass,
      Class<? extends LongWritable> serverResponseClass,
      Class<? extends LongWritable> clientResponseClass) throws Exception {
    
    // start server
    Server server = new TestServer(1, false,
        serverParamClass, serverResponseClass);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    // start client
    WRITABLE_FAULTS_ENABLED = true;
    Client client = new Client(clientResponseClass, conf);
    try {
      LongWritable param = clientParamClass.newInstance();

      try {
        client.call(param, addr, null, null, 0, conf);
        fail("Expected an exception to have been thrown");
      } catch (Throwable t) {
        assertExceptionContains(t, "Injected fault");
      }
      
      // Doing a second call with faults disabled should return fine --
      // ie the internal state of the client or server should not be broken
      // by the failed call
      WRITABLE_FAULTS_ENABLED = false;
      client.call(param, addr, null, null, 0, conf);
      
    } finally {
      server.stop();
    }
  }

  @Test
  public void testIOEOnClientWriteParam() throws Exception {
    doErrorTest(IOEOnWriteWritable.class,
        LongWritable.class,
        LongWritable.class,
        LongWritable.class);
  }
  
  @Test
  public void testRTEOnClientWriteParam() throws Exception {
    doErrorTest(RTEOnWriteWritable.class,
        LongWritable.class,
        LongWritable.class,
        LongWritable.class);
  }

  @Test
  public void testIOEOnServerReadParam() throws Exception {
    doErrorTest(LongWritable.class,
        IOEOnReadWritable.class,
        LongWritable.class,
        LongWritable.class);
  }
  
  @Test
  public void testRTEOnServerReadParam() throws Exception {
    doErrorTest(LongWritable.class,
        RTEOnReadWritable.class,
        LongWritable.class,
        LongWritable.class);
  }

  
  @Test
  public void testIOEOnServerWriteResponse() throws Exception {
    doErrorTest(LongWritable.class,
        LongWritable.class,
        IOEOnWriteWritable.class,
        LongWritable.class);
  }
  
  @Test
  public void testRTEOnServerWriteResponse() throws Exception {
    doErrorTest(LongWritable.class,
        LongWritable.class,
        RTEOnWriteWritable.class,
        LongWritable.class);
  }
  
  @Test
  public void testIOEOnClientReadResponse() throws Exception {
    doErrorTest(LongWritable.class,
        LongWritable.class,
        LongWritable.class,
        IOEOnReadWritable.class);
  }
  
  @Test
  public void testRTEOnClientReadResponse() throws Exception {
    doErrorTest(LongWritable.class,
        LongWritable.class,
        LongWritable.class,
        RTEOnReadWritable.class);
  }
  
  private static void assertExceptionContains(
      Throwable t, String substring) {
    String msg = StringUtils.stringifyException(t);
    assertTrue("Exception should contain substring '" + substring + "':\n" +
        msg, msg.contains(substring));
    LOG.info("Got expected exception", t);
  }
  
  /**
   * Test that, if the socket factory throws an IOE, it properly propagates
   * to the client.
   */
  @Test
  public void testSocketFactoryException() throws Exception {
    SocketFactory mockFactory = mock(SocketFactory.class);
    doThrow(new IOException("Injected fault")).when(mockFactory).createSocket();
    Client client = new Client(LongWritable.class, conf, mockFactory);
    
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10);
    try {
      client.call(new LongWritable(RANDOM.nextLong()),
              address, null, null, 0, conf);
      fail("Expected an exception to have been thrown");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Injected fault"));
    }
  }

  @Test
  public void testIpcTimeout() throws Exception {
    // start server
    Server server = new TestServer(1, true);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    // start client
    Client client = new Client(LongWritable.class, conf);
    // set timeout to be less than MIN_SLEEP_TIME
    try {
      client.call(new LongWritable(RANDOM.nextLong()),
              addr, null, null, MIN_SLEEP_TIME/2, conf);
      fail("Expected an exception to have been thrown");
    } catch (SocketTimeoutException e) {
      LOG.info("Get a SocketTimeoutException ", e);
    }
    // set timeout to be bigger than 3*ping interval
    client.call(new LongWritable(RANDOM.nextLong()),
        addr, null, null, 3*PING_INTERVAL+MIN_SLEEP_TIME, conf);
  }
  
  /**
   * Check that file descriptors aren't leaked by starting
   * and stopping IPC servers.
   */
  @Test
  public void testSocketLeak() throws Exception {
    Assume.assumeTrue(FD_DIR.exists()); // only run on Linux

    long startFds = countOpenFileDescriptors();
    for (int i = 0; i < 50; i++) {
      Server server = new TestServer(1, true);
      server.start();
      server.stop();
    }
    long endFds = countOpenFileDescriptors();
    
    assertTrue("Leaked " + (endFds - startFds) + " file descriptors",
        endFds - startFds < 20);
  }
  
  private long countOpenFileDescriptors() {
    return FD_DIR.list().length;
  }

  public static void main(String[] args) throws Exception {

    //new TestIPC().testSerial(5, false, 2, 10, 1000);

    new TestIPC().testParallel(10, false, 2, 4, 2, 4, 1000);

  }

}
