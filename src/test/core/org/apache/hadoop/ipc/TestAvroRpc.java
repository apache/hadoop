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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.security.sasl.Sasl;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.TestSaslRPC.CustomSecurityInfo;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenIdentifier;
import org.apache.hadoop.ipc.TestSaslRPC.TestTokenSecretManager;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/** Unit tests for AvroRpc. */
public class TestAvroRpc extends TestCase {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestAvroRpc.class);
  
  int datasize = 1024*100;
  int numThreads = 50;

  public TestAvroRpc(String name) { super(name); }
	
  public static class TestImpl implements AvroTestProtocol {

    public void ping() {}
    
    public String echo(String value) { return value; }

    public int add(int v1, int v2) { return v1 + v2; }

    public int error() throws Problem {
      throw new Problem();
    }
  }

  public void testReflect() throws Exception {
    testReflect(false);
  }

  public void testSecureReflect() throws Exception {
    testReflect(true);
  }

  public void testSpecific() throws Exception {
    testSpecific(false);
  }

  public void testSecureSpecific() throws Exception {
    testSpecific(true);
  }

  private void testReflect(boolean secure) throws Exception {
    Configuration conf = new Configuration();
    TestTokenSecretManager sm = null;
    if (secure) {
      makeSecure(conf);
      sm = new TestTokenSecretManager();
    }
    UserGroupInformation.setConfiguration(conf);
    RPC.setProtocolEngine(conf, AvroTestProtocol.class, AvroRpcEngine.class);
    Server server = RPC.getServer(AvroTestProtocol.class,
                                  new TestImpl(), ADDRESS, 0, 5, true, 
                                  conf, sm);
    try {
      server.start();
      InetSocketAddress addr = NetUtils.getConnectAddress(server);

      if (secure) {
        addToken(sm, addr);
        //QOP must be auth
        Assert.assertEquals("auth", SaslRpcServer.SASL_PROPS.get(Sasl.QOP));
      }

      AvroTestProtocol proxy =
        (AvroTestProtocol)RPC.getProxy(AvroTestProtocol.class, 0, addr, conf);

      proxy.ping();

      String echo = proxy.echo("hello world");
      assertEquals("hello world", echo);

      int intResult = proxy.add(1, 2);
      assertEquals(3, intResult);

      boolean caught = false;
      try {
        proxy.error();
      } catch (AvroRemoteException e) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Caught " + e);
        }
        caught = true;
      }
      assertTrue(caught);

    } finally {
      server.stop();
    }
  }

  private void makeSecure(Configuration conf) {
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set("hadoop.rpc.socket.factory.class.default", "");
    //Avro doesn't work with security annotations on protocol.
    //Avro works ONLY with custom security context
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_INFO_CLASS_NAME,
        CustomSecurityInfo.class.getName());
  }

  private void addToken(TestTokenSecretManager sm, 
      InetSocketAddress addr) throws IOException {
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    
    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()));
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId,
        sm);
    Text host = new Text(addr.getAddress().getHostAddress() + ":"
        + addr.getPort());
    token.setService(host);
    LOG.info("Service IP address for token is " + host);
    current.addToken(token);
  }

  private void testSpecific(boolean secure) throws Exception {
    Configuration conf = new Configuration();
    TestTokenSecretManager sm = null;
    if (secure) {
      makeSecure(conf);
      sm = new TestTokenSecretManager();
    }
    UserGroupInformation.setConfiguration(conf);
    RPC.setProtocolEngine(conf, AvroSpecificTestProtocol.class, 
        AvroSpecificRpcEngine.class);
    Server server = RPC.getServer(AvroSpecificTestProtocol.class,
        new AvroSpecificTestProtocolImpl(), ADDRESS, 0, 5, true, 
        conf, sm);
    try {
      server.start();
      InetSocketAddress addr = NetUtils.getConnectAddress(server);

      if (secure) {
        addToken(sm, addr);
        //QOP must be auth
        Assert.assertEquals("auth", SaslRpcServer.SASL_PROPS.get(Sasl.QOP));
      }

      AvroSpecificTestProtocol proxy =
        (AvroSpecificTestProtocol)RPC.getProxy(AvroSpecificTestProtocol.class, 
            0, addr, conf);
      
      Utf8 echo = proxy.echo(new Utf8("hello world"));
      assertEquals("hello world", echo.toString());

      int intResult = proxy.add(1, 2);
      assertEquals(3, intResult);

    } finally {
      server.stop();
    }
  }
  
  public static class AvroSpecificTestProtocolImpl implements 
      AvroSpecificTestProtocol {

    @Override
    public int add(int arg1, int arg2) throws AvroRemoteException {
      return arg1 + arg2;
    }

    @Override
    public Utf8 echo(Utf8 msg) throws AvroRemoteException {
      return msg;
    }
    
  }

}
