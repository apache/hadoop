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

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import org.junit.After;

import org.junit.Test;

/** Unit test for supporting method-name based compatible RPCs. */
public class TestRPCCompatibility {
  private static final String ADDRESS = "0.0.0.0";
  private static InetSocketAddress addr;
  private static Server server;
  private ProtocolProxy<?> proxy;

  public static final Log LOG =
    LogFactory.getLog(TestRPCCompatibility.class);
  
  private static Configuration conf = new Configuration();

  public interface TestProtocol0 extends VersionedProtocol {
    public static final long versionID = 0L;
    void ping() throws IOException;    
  }
  
  public interface TestProtocol1 extends TestProtocol0 {
    String echo(String value) throws IOException;
  }

  public interface TestProtocol2 extends TestProtocol1 {
    int echo(int value)  throws IOException;
  }

  public static class TestImpl0 implements TestProtocol0 {
    @Override
    public long getProtocolVersion(String protocol,
        long clientVersion) throws IOException {
      return versionID;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHashCode)
    throws IOException {
      Class<? extends VersionedProtocol> inter;
      try {
        inter = (Class<? extends VersionedProtocol>)getClass().getGenericInterfaces()[0];
      } catch (Exception e) {
        throw new IOException(e);
      }
      return ProtocolSignature.getProtocolSignature(clientMethodsHashCode, 
          getProtocolVersion(protocol, clientVersion), inter);
    }

    @Override
    public void ping() { return; }
  }

  public static class TestImpl1 extends TestImpl0 implements TestProtocol1 {
    @Override
    public String echo(String value) { return value; }
  }

  public static class TestImpl2 extends TestImpl1 implements TestProtocol2 {
    @Override
    public int echo(int value) { return value; }
  }
  
  @After
  public void tearDown() throws IOException {
    if (proxy != null) {
      RPC.stopProxy(proxy.getProxy());
    }
    if (server != null) {
      server.stop();
    }
  }
  
  @Test  // old client vs new server
  public void testVersion0ClientVersion1Server() throws Exception {
    // create a server with two handlers
    server = RPC.getServer(TestProtocol1.class,
                              new TestImpl1(), ADDRESS, 0, 2, false, conf, null);
    server.start();
    addr = NetUtils.getConnectAddress(server);

    proxy = RPC.getProtocolProxy(
        TestProtocol0.class, TestProtocol0.versionID, addr, conf);

    TestProtocol0 proxy0 = (TestProtocol0)proxy.getProxy();
    proxy0.ping();
  }
  
  @Test  // old client vs new server
  public void testVersion1ClientVersion0Server() throws Exception {
    // create a server with two handlers
    server = RPC.getServer(TestProtocol0.class,
                              new TestImpl0(), ADDRESS, 0, 2, false, conf, null);
    server.start();
    addr = NetUtils.getConnectAddress(server);

    proxy = RPC.getProtocolProxy(
        TestProtocol1.class, TestProtocol1.versionID, addr, conf);

    TestProtocol1 proxy1 = (TestProtocol1)proxy.getProxy();
    proxy1.ping();
    try {
      proxy1.echo("hello");
      fail("Echo should fail");
    } catch(IOException e) {
    }
  }
  
  private class Version2Client {

    private TestProtocol2 proxy2;
    private ProtocolProxy<TestProtocol2> serverInfo;
    
    private Version2Client() throws IOException {
      serverInfo =  RPC.getProtocolProxy(
          TestProtocol2.class, TestProtocol2.versionID, addr, conf);
      proxy2 = serverInfo.getProxy();
    }
    
    public int echo(int value) throws IOException, NumberFormatException {
      if (serverInfo.isMethodSupported("echo", int.class)) {
        return -value;  // use version 3 echo long
      } else { // server is version 2
        return Integer.parseInt(proxy2.echo(String.valueOf(value)));
      }
    }

    public String echo(String value) throws IOException {
      return proxy2.echo(value);
    }

    public void ping() throws IOException {
      proxy2.ping();
    }
  }

  @Test // Compatible new client & old server
  public void testVersion2ClientVersion1Server() throws Exception {
    // create a server with two handlers
    server = RPC.getServer(TestProtocol1.class,
                              new TestImpl1(), ADDRESS, 0, 2, false, conf, null);
    server.start();
    addr = NetUtils.getConnectAddress(server);


    Version2Client client = new Version2Client();
    client.ping();
    assertEquals("hello", client.echo("hello"));
    
    // echo(int) is not supported by server, so returning 3
    // This verifies that echo(int) and echo(String)'s hash codes are different
    assertEquals(3, client.echo(3));
  }
  
  @Test // equal version client and server
  public void testVersion2ClientVersion2Server() throws Exception {
    // create a server with two handlers
    server = RPC.getServer(TestProtocol2.class,
                              new TestImpl2(), ADDRESS, 0, 2, false, conf, null);
    server.start();
    addr = NetUtils.getConnectAddress(server);

    Version2Client client = new Version2Client();

    client.ping();
    assertEquals("hello", client.echo("hello"));
    
    // now that echo(int) is supported by the server, echo(int) should return -3
    assertEquals(-3, client.echo(3));
  }
  
  public interface TestProtocol3 {
    int echo(String value);
    int echo(int value);
    int echo_alias(int value);
    int echo(int value1, int value2);
  }
  
  @Test
  public void testHashCode() throws Exception {
    // make sure that overriding methods have different hashcodes
    Method strMethod = TestProtocol3.class.getMethod("echo", String.class);
    int stringEchoHash = ProtocolSignature.getFingerprint(strMethod);
    Method intMethod = TestProtocol3.class.getMethod("echo", int.class);
    int intEchoHash = ProtocolSignature.getFingerprint(intMethod);
    assertFalse(stringEchoHash == intEchoHash);
    
    // make sure methods with the same signature 
    // from different declaring classes have the same hash code
    int intEchoHash1 = ProtocolSignature.getFingerprint(
        TestProtocol2.class.getMethod("echo", int.class));
    assertEquals(intEchoHash, intEchoHash1);
    
    // Methods with the same name and parameter types but different returning
    // types have different hash codes
    int stringEchoHash1 = ProtocolSignature.getFingerprint(
        TestProtocol2.class.getMethod("echo", String.class));
    assertFalse(stringEchoHash == stringEchoHash1);
    
    // Make sure that methods with the same returning type and parameter types
    // but different method names have different hash code
    int intEchoHashAlias = ProtocolSignature.getFingerprint(
        TestProtocol3.class.getMethod("echo_alias", int.class));
    assertFalse(intEchoHash == intEchoHashAlias);
    
    // Make sure that methods with the same returninig type and method name but
    // larger number of parameter types have different hash code
    int intEchoHash2 = ProtocolSignature.getFingerprint(
        TestProtocol3.class.getMethod("echo", int.class, int.class));
    assertFalse(intEchoHash == intEchoHash2);
    
    // make sure that methods order does not matter for method array hash code
    int hash1 = ProtocolSignature.getFingerprint(new Method[] {intMethod, strMethod});
    int hash2 = ProtocolSignature.getFingerprint(new Method[] {strMethod, intMethod});
    assertEquals(hash1, hash2);
  }
}