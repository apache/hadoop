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

import java.util.Random;
import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

/** Unit tests for IPC. */
public class TestIPC extends TestCase {
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.TestIPC");

  private static Configuration conf = new Configuration();
  
  public TestIPC(String name) { super(name); }

  private static final Random RANDOM = new Random();

  private static final int PORT = 1234;
  private static final String ADDRESS = "0.0.0.0";

  private static class TestServer extends Server {
    private boolean sleep;

    public TestServer(String bindAddress, int port, int handlerCount, boolean sleep) {
      super(bindAddress, port, LongWritable.class, handlerCount, conf);
      this.setTimeout(1000);
      this.sleep = sleep;
    }

    public Writable call(Writable param) throws IOException {
      if (sleep) {
        try {
          Thread.sleep(RANDOM.nextInt(200));      // sleep a bit
        } catch (InterruptedException e) {}
      }
      return param;                               // echo param as result
    }
  }

  private static class SerialCaller extends Thread {
    private Client client;
    private int count;
    private boolean failed;

    public SerialCaller(Client client, int count) {
      this.client = client;
      this.count = count;
      client.setTimeout(1000);
    }

    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          LongWritable param = new LongWritable(RANDOM.nextLong());
          LongWritable value =
            (LongWritable)client.call(param, new InetSocketAddress(PORT));
          if (!param.equals(value)) {
            LOG.fatal("Call failed!");
            failed = true;
            break;
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + e);
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
      client.setTimeout(1000);
    }

    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          Writable[] params = new Writable[addresses.length];
          for (int j = 0; j < addresses.length; j++)
            params[j] = new LongWritable(RANDOM.nextLong());
          Writable[] values = client.call(params, addresses);
          for (int j = 0; j < addresses.length; j++) {
            if (!params[j].equals(values[j])) {
              LOG.fatal("Call failed!");
              failed = true;
              break;
            }
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + e);
          failed = true;
        }
      }
    }
  }

  public void testSerial() throws Exception {
    testSerial(3, false, 2, 5, 100);
  }

  public void testSerial(int handlerCount, boolean handlerSleep, 
                          int clientCount, int callerCount, int callCount)
    throws Exception {
    Server server = new TestServer(ADDRESS, PORT, handlerCount, handlerSleep);
    server.start();

    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(LongWritable.class, conf);
    }
    
    SerialCaller[] callers = new SerialCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new SerialCaller(clients[i%clientCount], callCount);
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
	
  public void testParallel() throws Exception {
    testParallel(10, false, 2, 4, 2, 4, 100);
  }

  public void testParallel(int handlerCount, boolean handlerSleep,
                           int serverCount, int addressCount,
                           int clientCount, int callerCount, int callCount)
    throws Exception {
    Server[] servers = new Server[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = new TestServer(ADDRESS, PORT+i+1, handlerCount, handlerSleep);
      servers[i].start();
    }

    InetSocketAddress[] addresses = new InetSocketAddress[addressCount];
    for (int i = 0; i < addressCount; i++) {
      addresses[i] = new InetSocketAddress(PORT+1+(i%serverCount));
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
	
  public static void main(String[] args) throws Exception {

    //new TestIPC("test").testSerial(5, false, 2, 10, 1000);

    new TestIPC("test").testParallel(10, false, 2, 4, 2, 4, 1000);

  }

}
