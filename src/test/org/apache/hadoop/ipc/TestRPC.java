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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.lang.reflect.Method;

import junit.framework.TestCase;

import java.util.Arrays;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.ipc.VersionedProtocol;

/** Unit tests for RPC. */
public class TestRPC extends TestCase {
  private static final int PORT = 1234;
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.TestRPC");
  
  private static Configuration conf = new Configuration();

  // quiet during testing, since output ends up on console
  static {
    conf.setInt("ipc.client.timeout", 5000);
  }

  public TestRPC(String name) { super(name); }
	
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
    
    void ping() throws IOException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    Writable echo(Writable value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
    void testServerGet() throws IOException;
  }

  public class TestImpl implements TestProtocol {

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.versionID;
    }
    
    public void ping() {}

    public String echo(String value) throws IOException { return value; }

    public String[] echo(String[] values) throws IOException { return values; }

    public Writable echo(Writable writable) {
      return writable;
    }
    public int add(int v1, int v2) {
      return v1 + v2;
    }

    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    public int error() throws IOException {
      throw new IOException("bobo");
    }

    public void testServerGet() throws IOException {
      if (!(Server.get() instanceof RPC.Server)) {
        throw new IOException("Server.get() failed");
      }
    }

  }

  public void testCalls() throws Exception {
    Server server = RPC.getServer(new TestImpl(), ADDRESS, PORT, conf);
    server.start();

    InetSocketAddress addr = new InetSocketAddress(PORT);
    TestProtocol proxy =
      (TestProtocol)RPC.getProxy(TestProtocol.class, TestProtocol.versionID, addr, conf);
    
    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    stringResult = proxy.echo((String)null);
    assertEquals(stringResult, null);

    String[] stringResults = proxy.echo(new String[]{"foo","bar"});
    assertTrue(Arrays.equals(stringResults, new String[]{"foo","bar"}));

    stringResults = proxy.echo((String[])null);
    assertTrue(Arrays.equals(stringResults, null));

    UTF8 utf8Result = (UTF8)proxy.echo(new UTF8("hello world"));
    assertEquals(utf8Result, new UTF8("hello world"));

    utf8Result = (UTF8)proxy.echo((UTF8)null);
    assertEquals(utf8Result, null);

    int intResult = proxy.add(1, 2);
    assertEquals(intResult, 3);

    intResult = proxy.add(new int[] {1, 2});
    assertEquals(intResult, 3);

    boolean caught = false;
    try {
      proxy.error();
    } catch (IOException e) {
      LOG.debug("Caught " + e);
      caught = true;
    }
    assertTrue(caught);

    proxy.testServerGet();

    // try some multi-calls
    Method echo =
      TestProtocol.class.getMethod("echo", new Class[] { String.class });
    String[] strings = (String[])RPC.call(echo, new String[][]{{"a"},{"b"}},
                                         new InetSocketAddress[] {addr, addr}, conf);
    assertTrue(Arrays.equals(strings, new String[]{"a","b"}));

    Method ping = TestProtocol.class.getMethod("ping", new Class[] {});
    Object[] voids = (Object[])RPC.call(ping, new Object[][]{{},{}},
                                        new InetSocketAddress[] {addr, addr}, conf);
    assertEquals(voids, null);

    server.stop();
  }
  public static void main(String[] args) throws Exception {

    new TestRPC("test").testCalls();

  }

}
