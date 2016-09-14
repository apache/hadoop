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

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.net.NetUtils;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import com.google.protobuf.BlockingService;

public class TestMultipleProtocolServer extends TestRpcBase {
  private static InetSocketAddress addr;
  private static RPC.Server server;

  private static Configuration conf = new Configuration();
  
  
  @ProtocolInfo(protocolName="Foo")
  interface Foo0 extends VersionedProtocol {
    public static final long versionID = 0L;
    String ping() throws IOException;
    
  }
  
  @ProtocolInfo(protocolName="Foo")
  interface Foo1 extends VersionedProtocol {
    public static final long versionID = 1L;
    String ping() throws IOException;
    String ping2() throws IOException;
  }
  
  @ProtocolInfo(protocolName="Foo")
  interface FooUnimplemented extends VersionedProtocol {
    public static final long versionID = 2L;
    String ping() throws IOException;  
  }
  
  interface Mixin extends VersionedProtocol{
    public static final long versionID = 0L;
    void hello() throws IOException;
  }

  interface Bar extends Mixin {
    public static final long versionID = 0L;
    int echo(int i) throws IOException;
  }
  
  class Foo0Impl implements Foo0 {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return Foo0.versionID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      Class<? extends VersionedProtocol> inter;
      try {
        inter = (Class<? extends VersionedProtocol>)getClass().
                                          getGenericInterfaces()[0];
      } catch (Exception e) {
        throw new IOException(e);
      }
      return ProtocolSignature.getProtocolSignature(clientMethodsHash, 
          getProtocolVersion(protocol, clientVersion), inter);
    }

    @Override
    public String ping() {
      return "Foo0";     
    }
    
  }
  
  class Foo1Impl implements Foo1 {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return Foo1.versionID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      Class<? extends VersionedProtocol> inter;
      try {
        inter = (Class<? extends VersionedProtocol>)getClass().
                                        getGenericInterfaces()[0];
      } catch (Exception e) {
        throw new IOException(e);
      }
      return ProtocolSignature.getProtocolSignature(clientMethodsHash, 
          getProtocolVersion(protocol, clientVersion), inter);
    }

    @Override
    public String ping() {
      return "Foo1";
    }

    @Override
    public String ping2() {
      return "Foo1";
      
    }
    
  }

  
  class BarImpl implements Bar {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return Bar.versionID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      Class<? extends VersionedProtocol> inter;
      try {
        inter = (Class<? extends VersionedProtocol>)getClass().
                                          getGenericInterfaces()[0];
      } catch (Exception e) {
        throw new IOException(e);
      }
      return ProtocolSignature.getProtocolSignature(clientMethodsHash, 
          getProtocolVersion(protocol, clientVersion), inter);
    }

    @Override
    public int echo(int i) {
      return i;
    }

    @Override
    public void hello() {

      
    }
  }
  @Before
  public void setUp() throws Exception {
    // create a server with two handlers
    server = new RPC.Builder(conf).setProtocol(Foo0.class)
        .setInstance(new Foo0Impl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, Foo1.class, new Foo1Impl());
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, Bar.class, new BarImpl());
    server.addProtocol(RPC.RpcKind.RPC_WRITABLE, Mixin.class, new BarImpl());
    
    
    // Add Protobuf server
    // Create server side implementation
    PBServerImpl pbServerImpl = new PBServerImpl();
    BlockingService service = TestProtobufRpcProto
        .newReflectiveBlockingService(pbServerImpl);
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService.class,
        service);
    server.start();
    addr = NetUtils.getConnectAddress(server);
  }
  
  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void test1() throws IOException {
    ProtocolProxy<?> proxy;
    proxy = RPC.getProtocolProxy(Foo0.class, Foo0.versionID, addr, conf);

    Foo0 foo0 = (Foo0)proxy.getProxy(); 
    Assert.assertEquals("Foo0", foo0.ping());
    
    
    proxy = RPC.getProtocolProxy(Foo1.class, Foo1.versionID, addr, conf);
    
    
    Foo1 foo1 = (Foo1)proxy.getProxy(); 
    Assert.assertEquals("Foo1", foo1.ping());
    Assert.assertEquals("Foo1", foo1.ping());
    
    
    proxy = RPC.getProtocolProxy(Bar.class, Foo1.versionID, addr, conf);
    
    
    Bar bar = (Bar)proxy.getProxy(); 
    Assert.assertEquals(99, bar.echo(99));
    
    // Now test Mixin class method
    
    Mixin mixin = bar;
    mixin.hello();
  }
  
  
  // Server does not implement the FooUnimplemented version of protocol Foo.
  // See that calls to it fail.
  @Test(expected=IOException.class)
  public void testNonExistingProtocol() throws IOException {
    ProtocolProxy<?> proxy;
    proxy = RPC.getProtocolProxy(FooUnimplemented.class, 
        FooUnimplemented.versionID, addr, conf);

    FooUnimplemented foo = (FooUnimplemented)proxy.getProxy(); 
    foo.ping();
  }

  /**
   * getProtocolVersion of an unimplemented version should return highest version
   * Similarly getProtocolSignature should work.
   * @throws IOException
   */
  @Test
  public void testNonExistingProtocol2() throws IOException {
    ProtocolProxy<?> proxy;
    proxy = RPC.getProtocolProxy(FooUnimplemented.class, 
        FooUnimplemented.versionID, addr, conf);

    FooUnimplemented foo = (FooUnimplemented)proxy.getProxy(); 
    Assert.assertEquals(Foo1.versionID, 
        foo.getProtocolVersion(RPC.getProtocolName(FooUnimplemented.class), 
        FooUnimplemented.versionID));
    foo.getProtocolSignature(RPC.getProtocolName(FooUnimplemented.class), 
        FooUnimplemented.versionID, 0);
  }
  
  @Test(expected=IOException.class)
  public void testIncorrectServerCreation() throws IOException {
    new RPC.Builder(conf).setProtocol(Foo1.class).setInstance(new Foo0Impl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(2).setVerbose(false)
        .build();
  } 
  
  // Now test a PB service - a server  hosts both PB and Writable Rpcs.
  @Test
  public void testPBService() throws Exception {
    // Set RPC engine to protobuf RPC engine
    Configuration conf2 = new Configuration();
    RPC.setProtocolEngine(conf2, TestRpcService.class,
        ProtobufRpcEngine.class);
    TestRpcService client = RPC.getProxy(TestRpcService.class, 0, addr, conf2);
    TestProtoBufRpc.testProtoBufRpc(client);
  }
}
