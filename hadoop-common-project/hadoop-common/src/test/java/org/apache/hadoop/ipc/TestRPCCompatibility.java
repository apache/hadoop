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

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Unit test for supporting method-name based compatible RPCs. */
public class TestRPCCompatibility {
  private static final String ADDRESS = "0.0.0.0";
  private static InetSocketAddress addr;
  private static RPC.Server server;
  private ProtocolProxy<?> proxy;

  public static final Logger LOG =
      LoggerFactory.getLogger(TestRPCCompatibility.class);

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

    RPC.setProtocolEngine(conf,
        TestProtocol0.class, ProtobufRpcEngine.class);

    RPC.setProtocolEngine(conf,
        TestProtocol1.class, ProtobufRpcEngine.class);

    RPC.setProtocolEngine(conf,
        TestProtocol2.class, ProtobufRpcEngine.class);

    RPC.setProtocolEngine(conf,
        TestProtocol3.class, ProtobufRpcEngine.class);

    RPC.setProtocolEngine(conf,
        TestProtocol4.class, ProtobufRpcEngine.class);
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
}
