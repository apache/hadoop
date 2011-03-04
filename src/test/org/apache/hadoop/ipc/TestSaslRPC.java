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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.commons.logging.*;
import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.log4j.Level;
import org.junit.Test;

/** Unit tests for using Sasl over RPC. */
public class TestSaslRPC {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestSaslRPC.class);
  
  static final String SERVER_PRINCIPAL_KEY = "test.ipc.server.principal";
  private static Configuration conf;
  static {
    conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  static {
    ((Log4JLogger) Client.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) Server.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslInputStream.LOG).getLogger().setLevel(Level.ALL);
  }

  public static class TestTokenIdentifier extends TokenIdentifier {
    private Text tokenid;
    private Text realUser;
    final static Text KIND_NAME = new Text("test.token");
    
    public TestTokenIdentifier() {
      this.tokenid = new Text();
      this.realUser = new Text();
    }
    public TestTokenIdentifier(Text tokenid) {
      this.tokenid = tokenid;
      this.realUser = new Text();
    }
    public TestTokenIdentifier(Text tokenid, Text realUser) {
      this.tokenid = tokenid;
      if (realUser == null) {
        this.realUser = new Text();
      } else {
        this.realUser = realUser;
      }
    }
    @Override
    public Text getKind() {
      return KIND_NAME;
    }
    @Override
    public UserGroupInformation getUser() {
      if ((realUser == null) || ("".equals(realUser.toString()))) {
        return UserGroupInformation.createRemoteUser(tokenid.toString());
      } else {
        UserGroupInformation realUgi = UserGroupInformation
            .createRemoteUser(realUser.toString());
        return UserGroupInformation
            .createProxyUser(tokenid.toString(), realUgi);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      tokenid.readFields(in);
      realUser.readFields(in);
    }
    @Override
    public void write(DataOutput out) throws IOException {
      tokenid.write(out);
      if (realUser != null) {
        realUser.write(out);
      }
    }
  }
  
  public static class TestTokenSecretManager extends
      SecretManager<TestTokenIdentifier> {
    public byte[] createPassword(TestTokenIdentifier id) {
      return id.getBytes();
    }

    public byte[] retrievePassword(TestTokenIdentifier id) 
        throws InvalidToken {
      return id.getBytes();
    }
    
    public TestTokenIdentifier createIdentifier() {
      return new TestTokenIdentifier();
    }
  }

  public static class TestTokenSelector implements
      TokenSelector<TestTokenIdentifier> {
    @SuppressWarnings("unchecked")
    @Override
    public Token<TestTokenIdentifier> selectToken(Text service,
        Collection<Token<? extends TokenIdentifier>> tokens) {
      if (service == null) {
        return null;
      }
      for (Token<? extends TokenIdentifier> token : tokens) {
        if (TestTokenIdentifier.KIND_NAME.equals(token.getKind())
            && service.equals(token.getService())) {
          return (Token<TestTokenIdentifier>) token;
        }
      }
      return null;
    }
  }
  
  @KerberosInfo(SERVER_PRINCIPAL_KEY)
  @TokenInfo(TestTokenSelector.class)
  public interface TestSaslProtocol extends TestRPC.TestProtocol {
  }
  
  public static class TestSaslImpl extends TestRPC.TestImpl implements
      TestSaslProtocol {
  }

  @Test
  public void testDigestRpc() throws Exception {
    TestTokenSecretManager sm = new TestTokenSecretManager();
    final Server server = RPC.getServer(
        new TestSaslImpl(), ADDRESS, 0, 5, true, conf, sm);

    server.start();

    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()));
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId,
        sm);
    Text host = new Text(addr.getAddress().getHostAddress() + ":"
        + addr.getPort());
    token.setService(host);
    LOG.info("Service IP address for token is " + host);
    current.addToken(token);

    TestSaslProtocol proxy = null;
    try {
      proxy = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, conf);
      proxy.ping();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  static void testKerberosRpc(String principal, String keytab) throws Exception {
    final Configuration newConf = new Configuration(conf);
    newConf.set(SERVER_PRINCIPAL_KEY, principal);
    UserGroupInformation.loginUserFromKeytab(principal, keytab);
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    System.out.println("UGI: " + current);

    Server server = RPC.getServer(new TestSaslImpl(),
        ADDRESS, 0, 5, true, newConf, null);
    TestSaslProtocol proxy = null;

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    try {
      proxy = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy.ping();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    System.out.println("Testing Kerberos authentication over RPC");
    if (args.length != 2) {
      System.err
          .println("Usage: java <options> org.apache.hadoop.ipc.TestSaslRPC "
              + " <serverPrincipal> <keytabFile>");
      System.exit(-1);
    }
    String principal = args[0];
    String keytab = args[1];
    testKerberosRpc(principal, keytab);
  }

}
