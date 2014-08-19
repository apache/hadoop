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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Unit test for supporting method-name based compatible RPCs. */
public class TestRPCCompatibility {
  private static final String ADDRESS = "0.0.0.0";
  private static InetSocketAddress addr;
  private static RPC.Server server;
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

  
  // TestProtocol2 is a compatible impl of TestProtocol1 - hence use its name
  @ProtocolInfo(protocolName=
      "org.apache.hadoop.ipc.TestRPCCompatibility$TestProtocol1")
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
    @Override
    public long getProtocolVersion(String protocol,
        long clientVersion) throws IOException {
        return TestProtocol1.versionID;
    }
  }

  public static class TestImpl2 extends TestImpl1 implements TestProtocol2 {
    @Override
    public int echo(int value) { return value; }

    @Override
    public long getProtocolVersion(String protocol,
        long clientVersion) throws IOException {
      return TestProtocol2.versionID;
    }

  }

  @Before
  public void setUp() {
    ProtocolSignature.resetCache();
  }
  
  @After
  public void tearDown() {
    if (proxy != null) {
      RPC.stopProxy(proxy.getProxy());
      proxy = null;
    }
    if (server != null) {
      server.stop();
      server = null;
    }
  }
  
  @Test  // old client vs new server
  public void testVersion0ClientVersion1Server() throws Exception {
    // create a server with two handlers
    TestImpl1 impl = new TestImpl1();
    server = new RPC.Builder(conf).setProtocol(TestProtocol1.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
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
    server = new RPC.Builder(conf).setProtocol(TestProtocol0.class)
        .setInstance(new TestImpl0()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
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
System.out.println("echo int is supported");
        return -value;  // use version 3 echo long
      } else { // server is version 2
System.out.println("echo int is NOT supported");
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
    TestImpl1 impl = new TestImpl1();
    server = new RPC.Builder(conf).setProtocol(TestProtocol1.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
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
    TestImpl2 impl = new TestImpl2();
    server = new RPC.Builder(conf).setProtocol(TestProtocol2.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
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
    
    // Make sure that methods with the same returning type and method name but
    // larger number of parameter types have different hash code
    int intEchoHash2 = ProtocolSignature.getFingerprint(
        TestProtocol3.class.getMethod("echo", int.class, int.class));
    assertFalse(intEchoHash == intEchoHash2);
    
    // make sure that methods order does not matter for method array hash code
    int hash1 = ProtocolSignature.getFingerprint(new Method[] {intMethod, strMethod});
    int hash2 = ProtocolSignature.getFingerprint(new Method[] {strMethod, intMethod});
    assertEquals(hash1, hash2);
  }
  
  @ProtocolInfo(protocolName=
      "org.apache.hadoop.ipc.TestRPCCompatibility$TestProtocol1")
  public interface TestProtocol4 extends TestProtocol2 {
    public static final long versionID = 4L;
    @Override
    int echo(int value)  throws IOException;
  }
  
  @Test
  public void testVersionMismatch() throws IOException {
    server = new RPC.Builder(conf).setProtocol(TestProtocol2.class)
        .setInstance(new TestImpl2()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    server.start();
    addr = NetUtils.getConnectAddress(server);

    TestProtocol4 proxy = RPC.getProxy(TestProtocol4.class,
        TestProtocol4.versionID, addr, conf);
    try {
      proxy.echo(21);
      fail("The call must throw VersionMismatch exception");
    } catch (RemoteException ex) {
      Assert.assertEquals(RPC.VersionMismatch.class.getName(), 
          ex.getClassName());
      Assert.assertTrue(ex.getErrorCode().equals(
          RpcErrorCodeProto.ERROR_RPC_VERSION_MISMATCH));
    }  catch (IOException ex) {
      fail("Expected version mismatch but got " + ex);
    }
  }
  
  @Test
  public void testIsMethodSupported() throws IOException {
    server = new RPC.Builder(conf).setProtocol(TestProtocol2.class)
        .setInstance(new TestImpl2()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    server.start();
    addr = NetUtils.getConnectAddress(server);

    TestProtocol2 proxy = RPC.getProxy(TestProtocol2.class,
        TestProtocol2.versionID, addr, conf);
    boolean supported = RpcClientUtil.isMethodSupported(proxy,
        TestProtocol2.class, RPC.RpcKind.RPC_WRITABLE,
        RPC.getProtocolVersion(TestProtocol2.class), "echo");
    Assert.assertTrue(supported);
    supported = RpcClientUtil.isMethodSupported(proxy,
        TestProtocol2.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(TestProtocol2.class), "echo");
    Assert.assertFalse(supported);
  }

  /**
   * Verify that ProtocolMetaInfoServerSideTranslatorPB correctly looks up
   * the server registry to extract protocol signatures and versions.
   */
  @Test
  public void testProtocolMetaInfoSSTranslatorPB() throws Exception {
    TestImpl1 impl = new TestImpl1();
    server = new RPC.Builder(conf).setProtocol(TestProtocol1.class)
        .setInstance(impl).setBindAddress(ADDRESS).setPort(0).setNumHandlers(2)
        .setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, TestProtocol0.class, impl);
    server.start();

    ProtocolMetaInfoServerSideTranslatorPB xlator = 
        new ProtocolMetaInfoServerSideTranslatorPB(server);

    GetProtocolSignatureResponseProto resp = xlator.getProtocolSignature(
        null,
        createGetProtocolSigRequestProto(TestProtocol1.class,
            RPC.RpcKind.RPC_PROTOCOL_BUFFER));
    //No signatures should be found
    Assert.assertEquals(0, resp.getProtocolSignatureCount());
    resp = xlator.getProtocolSignature(
        null,
        createGetProtocolSigRequestProto(TestProtocol1.class,
            RPC.RpcKind.RPC_WRITABLE));
    Assert.assertEquals(1, resp.getProtocolSignatureCount());
    ProtocolSignatureProto sig = resp.getProtocolSignatureList().get(0);
    Assert.assertEquals(TestProtocol1.versionID, sig.getVersion());
    boolean found = false;
    int expected = ProtocolSignature.getFingerprint(TestProtocol1.class
        .getMethod("echo", String.class));
    for (int m : sig.getMethodsList()) {
      if (expected == m) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
  }
  
  private GetProtocolSignatureRequestProto createGetProtocolSigRequestProto(
      Class<?> protocol, RPC.RpcKind rpcKind) {
    GetProtocolSignatureRequestProto.Builder builder = 
        GetProtocolSignatureRequestProto.newBuilder();
    builder.setProtocol(protocol.getName());
    builder.setRpcKind(rpcKind.toString());
    return builder.build();
  }
}
