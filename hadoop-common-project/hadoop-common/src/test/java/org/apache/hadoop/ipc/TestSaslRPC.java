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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslPlainServer;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.TestUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for using Sasl over RPC. */
@RunWith(Parameterized.class)
public class TestSaslRPC {
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    for (QualityOfProtection qop : QualityOfProtection.values()) {
      params.add(new Object[]{ new QualityOfProtection[]{qop},qop, null });
    }
    params.add(new Object[]{ new QualityOfProtection[]{
        QualityOfProtection.PRIVACY,QualityOfProtection.AUTHENTICATION },
        QualityOfProtection.PRIVACY, null});
    params.add(new Object[]{ new QualityOfProtection[]{
        QualityOfProtection.PRIVACY,QualityOfProtection.AUTHENTICATION },
        QualityOfProtection.AUTHENTICATION ,
        "org.apache.hadoop.ipc.TestSaslRPC$AuthSaslPropertiesResolver" });

    return params;
  }

  QualityOfProtection[] qop;
  QualityOfProtection expectedQop;
  String saslPropertiesResolver ;
  
  public TestSaslRPC(QualityOfProtection[] qop,
      QualityOfProtection expectedQop,
      String saslPropertiesResolver) {
    this.qop=qop;
    this.expectedQop = expectedQop;
    this.saslPropertiesResolver = saslPropertiesResolver;
  }
  
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestSaslRPC.class);
  
  static final String ERROR_MESSAGE = "Token is invalid";
  static final String SERVER_PRINCIPAL_KEY = "test.ipc.server.principal";
  static final String SERVER_KEYTAB_KEY = "test.ipc.server.keytab";
  static final String SERVER_PRINCIPAL_1 = "p1/foo@BAR";
  static final String SERVER_PRINCIPAL_2 = "p2/foo@BAR";
  private static Configuration conf;
  // If this is set to true AND the auth-method is not simple, secretManager
  // will be enabled.
  static Boolean enableSecretManager = null;
  // If this is set to true, secretManager will be forecefully enabled
  // irrespective of auth-method.
  static Boolean forceSecretManager = null;
  static Boolean clientFallBackToSimpleAllowed = true;
  
  static enum UseToken {
    NONE(),
    VALID(),
    INVALID(),
    OTHER();
  }
  
  @BeforeClass
  public static void setupKerb() {
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.realm", "NONE");
    Security.addProvider(new SaslPlainServer.SecurityProvider());
  }    

  @Before
  public void setup() {
    LOG.info("---------------------------------");
    LOG.info("Testing QOP:"+ getQOPNames(qop));
    LOG.info("---------------------------------");
    conf = new Configuration();
    // the specific tests for kerberos will enable kerberos.  forcing it
    // for all tests will cause tests to fail if the user has a TGT
    conf.set(HADOOP_SECURITY_AUTHENTICATION, SIMPLE.toString());
    conf.set(HADOOP_RPC_PROTECTION, getQOPNames(qop));
    if (saslPropertiesResolver != null){
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
        saslPropertiesResolver);
    }
    UserGroupInformation.setConfiguration(conf);
    enableSecretManager = null;
    forceSecretManager = null;
    clientFallBackToSimpleAllowed = true;
  }

  static String getQOPNames (QualityOfProtection[] qops){
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (QualityOfProtection qop:qops){
     sb.append(qop.name().toLowerCase());
     if (++i < qops.length){
       sb.append(",");
     }
    }
    return sb.toString();
  }

  static {
    ((Log4JLogger) Client.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) Server.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslInputStream.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SecurityUtil.LOG).getLogger().setLevel(Level.ALL);
  }

  public static class TestTokenIdentifier extends TokenIdentifier {
    private Text tokenid;
    private Text realUser;
    final static Text KIND_NAME = new Text("test.token");
    
    public TestTokenIdentifier() {
      this(new Text(), new Text());
    }
    public TestTokenIdentifier(Text tokenid) {
      this(tokenid, new Text());
    }
    public TestTokenIdentifier(Text tokenid, Text realUser) {
      this.tokenid = tokenid == null ? new Text() : tokenid;
      this.realUser = realUser == null ? new Text() : realUser;
    }
    @Override
    public Text getKind() {
      return KIND_NAME;
    }
    @Override
    public UserGroupInformation getUser() {
      if (realUser.toString().isEmpty()) {
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
      realUser.write(out);
    }
  }
  
  public static class TestTokenSecretManager extends
      SecretManager<TestTokenIdentifier> {
    @Override
    public byte[] createPassword(TestTokenIdentifier id) {
      return id.getBytes();
    }

    @Override
    public byte[] retrievePassword(TestTokenIdentifier id) 
        throws InvalidToken {
      return id.getBytes();
    }
    
    @Override
    public TestTokenIdentifier createIdentifier() {
      return new TestTokenIdentifier();
    }
  }
  
  public static class BadTokenSecretManager extends TestTokenSecretManager {

    @Override
    public byte[] retrievePassword(TestTokenIdentifier id) 
        throws InvalidToken {
      throw new InvalidToken(ERROR_MESSAGE);
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
  
  @KerberosInfo(
      serverPrincipal = SERVER_PRINCIPAL_KEY)
  @TokenInfo(TestTokenSelector.class)
  public interface TestSaslProtocol extends TestRPC.TestProtocol {
    public AuthMethod getAuthMethod() throws IOException;
    public String getAuthUser() throws IOException;
  }
  
  public static class TestSaslImpl extends TestRPC.TestImpl implements
      TestSaslProtocol {
    @Override
    public AuthMethod getAuthMethod() throws IOException {
      return UserGroupInformation.getCurrentUser()
          .getAuthenticationMethod().getAuthMethod();
    }
    @Override
    public String getAuthUser() throws IOException {
      return UserGroupInformation.getCurrentUser().getUserName();
    }
  }

  public static class CustomSecurityInfo extends SecurityInfo {

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
      return new KerberosInfo() {
        @Override
        public Class<? extends Annotation> annotationType() {
          return null;
        }
        @Override
        public String serverPrincipal() {
          return SERVER_PRINCIPAL_KEY;
        }
        @Override
        public String clientPrincipal() {
          return null;
        }
      };
    }

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
      return new TokenInfo() {
        @Override
        public Class<? extends TokenSelector<? extends 
            TokenIdentifier>> value() {
          return TestTokenSelector.class;
        }
        @Override
        public Class<? extends Annotation> annotationType() {
          return null;
        }
      };
    }
  }

  @Test
  public void testDigestRpc() throws Exception {
    TestTokenSecretManager sm = new TestTokenSecretManager();
    final Server server = new RPC.Builder(conf)
        .setProtocol(TestSaslProtocol.class).setInstance(new TestSaslImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .setSecretManager(sm).build();
    
    doDigestRpc(server, sm);
  }

  @Test
  public void testDigestRpcWithoutAnnotation() throws Exception {
    TestTokenSecretManager sm = new TestTokenSecretManager();
    try {
      SecurityUtil.setSecurityInfoProviders(new CustomSecurityInfo());
      final Server server = new RPC.Builder(conf)
          .setProtocol(TestSaslProtocol.class).setInstance(new TestSaslImpl())
          .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5)
          .setVerbose(true).setSecretManager(sm).build();
      doDigestRpc(server, sm);
    } finally {
      SecurityUtil.setSecurityInfoProviders(new SecurityInfo[0]);
    }
  }

  @Test
  public void testErrorMessage() throws Exception {
    BadTokenSecretManager sm = new BadTokenSecretManager();
    final Server server = new RPC.Builder(conf)
        .setProtocol(TestSaslProtocol.class).setInstance(new TestSaslImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .setSecretManager(sm).build();

    boolean succeeded = false;
    try {
      doDigestRpc(server, sm);
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertEquals(ERROR_MESSAGE, e.getLocalizedMessage());
      assertTrue(e.unwrapRemoteException() instanceof InvalidToken);
      succeeded = true;
    }
    assertTrue(succeeded);
  }
  
  private void doDigestRpc(Server server, TestTokenSecretManager sm
                           ) throws Exception {
    server.start();

    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()));
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId,
        sm);
    SecurityUtil.setTokenService(token, addr);
    current.addToken(token);

    TestSaslProtocol proxy = null;
    try {
      proxy = RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, conf);
      AuthMethod authMethod = proxy.getAuthMethod();
      assertEquals(TOKEN, authMethod);
      //QOP must be auth
      assertEquals(expectedQop.saslQop,
                   RPC.getConnectionIdForProxy(proxy).getSaslQop());            
      proxy.ping();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }
  
  @Test
  public void testPingInterval() throws Exception {
    Configuration newConf = new Configuration(conf);
    newConf.set(SERVER_PRINCIPAL_KEY, SERVER_PRINCIPAL_1);
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);

    // set doPing to true
    newConf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
    ConnectionId remoteId = ConnectionId.getConnectionId(
        new InetSocketAddress(0), TestSaslProtocol.class, null, 0, newConf);
    assertEquals(CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT,
        remoteId.getPingInterval());
    // set doPing to false
    newConf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
    remoteId = ConnectionId.getConnectionId(
        new InetSocketAddress(0), TestSaslProtocol.class, null, 0, newConf);
    assertEquals(0, remoteId.getPingInterval());
  }
  
  @Test
  public void testPerConnectionConf() throws Exception {
    TestTokenSecretManager sm = new TestTokenSecretManager();
    final Server server = new RPC.Builder(conf)
        .setProtocol(TestSaslProtocol.class).setInstance(new TestSaslImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .setSecretManager(sm).build();
    server.start();
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestTokenIdentifier tokenId = new TestTokenIdentifier(new Text(current
        .getUserName()));
    Token<TestTokenIdentifier> token = new Token<TestTokenIdentifier>(tokenId,
        sm);
    SecurityUtil.setTokenService(token, addr);
    current.addToken(token);

    Configuration newConf = new Configuration(conf);
    newConf.set(CommonConfigurationKeysPublic.
        HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, "");

    Client client = null;
    TestSaslProtocol proxy1 = null;
    TestSaslProtocol proxy2 = null;
    TestSaslProtocol proxy3 = null;
    int timeouts[] = {111222, 3333333};
    try {
      newConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, timeouts[0]);
      proxy1 = RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy1.getAuthMethod();
      client = WritableRpcEngine.getClient(newConf);
      Set<ConnectionId> conns = client.getConnectionIds();
      assertEquals("number of connections in cache is wrong", 1, conns.size());
      // same conf, connection should be re-used
      proxy2 = RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy2.getAuthMethod();
      assertEquals("number of connections in cache is wrong", 1, conns.size());
      // different conf, new connection should be set up
      newConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, timeouts[1]);
      proxy3 = RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy3.getAuthMethod();
      assertEquals("number of connections in cache is wrong", 2, conns.size());
      // now verify the proxies have the correct connection ids and timeouts
      ConnectionId[] connsArray = {
          RPC.getConnectionIdForProxy(proxy1),
          RPC.getConnectionIdForProxy(proxy2),
          RPC.getConnectionIdForProxy(proxy3)
      };
      assertEquals(connsArray[0], connsArray[1]);
      assertEquals(connsArray[0].getMaxIdleTime(), timeouts[0]);
      assertFalse(connsArray[0].equals(connsArray[2]));
      assertNotSame(connsArray[2].getMaxIdleTime(), timeouts[1]);
    } finally {
      server.stop();
      // this is dirty, but clear out connection cache for next run
      if (client != null) {
        client.getConnectionIds().clear();
      }
      if (proxy1 != null) RPC.stopProxy(proxy1);
      if (proxy2 != null) RPC.stopProxy(proxy2);
      if (proxy3 != null) RPC.stopProxy(proxy3);
    }
  }
  
  static void testKerberosRpc(String principal, String keytab) throws Exception {
    final Configuration newConf = new Configuration(conf);
    newConf.set(SERVER_PRINCIPAL_KEY, principal);
    newConf.set(SERVER_KEYTAB_KEY, keytab);
    SecurityUtil.login(newConf, SERVER_KEYTAB_KEY, SERVER_PRINCIPAL_KEY);
    TestUserGroupInformation.verifyLoginMetrics(1, 0);
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    System.out.println("UGI: " + current);

    Server server = new RPC.Builder(newConf)
        .setProtocol(TestSaslProtocol.class).setInstance(new TestSaslImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .build();
    TestSaslProtocol proxy = null;

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    try {
      proxy = RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy.ping();
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    System.out.println("Test is successful.");
  }

  @Test
  public void testSaslPlainServer() throws IOException {
    runNegotiation(
        new TestPlainCallbacks.Client("user", "pass"),
        new TestPlainCallbacks.Server("user", "pass"));
  }

  @Test
  public void testSaslPlainServerBadPassword() {
    SaslException e = null;
    try {
      runNegotiation(
          new TestPlainCallbacks.Client("user", "pass1"),
          new TestPlainCallbacks.Server("user", "pass2"));
    } catch (SaslException se) {
      e = se;
    }
    assertNotNull(e);
    assertEquals("PLAIN auth failed: wrong password", e.getMessage());
  }


  private void runNegotiation(CallbackHandler clientCbh,
                              CallbackHandler serverCbh)
                                  throws SaslException {
    String mechanism = AuthMethod.PLAIN.getMechanismName();

    SaslClient saslClient = Sasl.createSaslClient(
        new String[]{ mechanism }, null, null, null, null, clientCbh);
    assertNotNull(saslClient);

    SaslServer saslServer = Sasl.createSaslServer(
        mechanism, null, "localhost", null, serverCbh);
    assertNotNull("failed to find PLAIN server", saslServer);
    
    byte[] response = saslClient.evaluateChallenge(new byte[0]);
    assertNotNull(response);
    assertTrue(saslClient.isComplete());

    response = saslServer.evaluateResponse(response);
    assertNull(response);
    assertTrue(saslServer.isComplete());
    assertNotNull(saslServer.getAuthorizationID());
  }
  
  static class TestPlainCallbacks {
    public static class Client implements CallbackHandler {
      String user = null;
      String password = null;
      
      Client(String user, String password) {
        this.user = user;
        this.password = password;
      }
      
      @Override
      public void handle(Callback[] callbacks)
          throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
          if (callback instanceof NameCallback) {
            ((NameCallback) callback).setName(user);
          } else if (callback instanceof PasswordCallback) {
            ((PasswordCallback) callback).setPassword(password.toCharArray());
          } else {
            throw new UnsupportedCallbackException(callback,
                "Unrecognized SASL PLAIN Callback");
          }
        }
      }
    }
    
    public static class Server implements CallbackHandler {
      String user = null;
      String password = null;
      
      Server(String user, String password) {
        this.user = user;
        this.password = password;
      }
      
      @Override
      public void handle(Callback[] callbacks)
          throws UnsupportedCallbackException, SaslException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        
        for (Callback callback : callbacks) {
          if (callback instanceof NameCallback) {
            nc = (NameCallback)callback;
            assertEquals(user, nc.getName());
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback)callback;
            if (!password.equals(new String(pc.getPassword()))) {
              throw new IllegalArgumentException("wrong password");
            }
          } else if (callback instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback)callback;
            assertEquals(user, ac.getAuthorizationID());
            assertEquals(user, ac.getAuthenticationID());
            ac.setAuthorized(true);
            ac.setAuthorizedID(ac.getAuthenticationID());
          } else {
            throw new UnsupportedCallbackException(callback,
                "Unsupported SASL PLAIN Callback");
          }
        }
        assertNotNull(nc);
        assertNotNull(pc);
        assertNotNull(ac);
      }
    }
  }
  
  private static Pattern BadToken =
      Pattern.compile(".*DIGEST-MD5: digest response format violation.*");
  private static Pattern KrbFailed =
      Pattern.compile(".*Failed on local exception:.* " +
                      "Failed to specify server's Kerberos principal name.*");
  private static Pattern Denied(AuthMethod method) {
      return Pattern.compile(".*RemoteException.*AccessControlException.*: "
          + method + " authentication is not enabled.*");
  }
  private static Pattern No(AuthMethod ... method) {
    String methods = StringUtils.join(method, ",\\s*");
    return Pattern.compile(".*Failed on local exception:.* " +
        "Client cannot authenticate via:\\[" + methods + "\\].*");
  }
  private static Pattern NoTokenAuth =
      Pattern.compile(".*IllegalArgumentException: " +
                      "TOKEN authentication requires a secret manager");
  private static Pattern NoFallback = 
      Pattern.compile(".*Failed on local exception:.* " +
          "Server asks us to fall back to SIMPLE auth, " +
          "but this client is configured to only allow secure connections.*");

  /*
   *  simple server
   */
  @Test
  public void testSimpleServer() throws Exception {
    assertAuthEquals(SIMPLE,    getAuthMethod(SIMPLE,   SIMPLE));
    assertAuthEquals(SIMPLE,    getAuthMethod(SIMPLE,   SIMPLE, UseToken.OTHER));
    // SASL methods are normally reverted to SIMPLE
    assertAuthEquals(SIMPLE,    getAuthMethod(KERBEROS, SIMPLE));
    assertAuthEquals(SIMPLE,    getAuthMethod(KERBEROS, SIMPLE, UseToken.OTHER));
  }

  @Test
  public void testNoClientFallbackToSimple()
      throws Exception {
    clientFallBackToSimpleAllowed = false;
    // tokens are irrelevant w/o secret manager enabled
    assertAuthEquals(SIMPLE,     getAuthMethod(SIMPLE, SIMPLE));
    assertAuthEquals(SIMPLE,     getAuthMethod(SIMPLE, SIMPLE, UseToken.OTHER));
    assertAuthEquals(SIMPLE,     getAuthMethod(SIMPLE, SIMPLE, UseToken.VALID));
    assertAuthEquals(SIMPLE,     getAuthMethod(SIMPLE, SIMPLE, UseToken.INVALID));

    // A secure client must not fallback
    assertAuthEquals(NoFallback, getAuthMethod(KERBEROS, SIMPLE));
    assertAuthEquals(NoFallback, getAuthMethod(KERBEROS, SIMPLE, UseToken.OTHER));
    assertAuthEquals(NoFallback, getAuthMethod(KERBEROS, SIMPLE, UseToken.VALID));
    assertAuthEquals(NoFallback, getAuthMethod(KERBEROS, SIMPLE, UseToken.INVALID));

    // Now set server to simple and also force the secret-manager. Now server
    // should have both simple and token enabled.
    forceSecretManager = true;
    assertAuthEquals(SIMPLE,     getAuthMethod(SIMPLE, SIMPLE));
    assertAuthEquals(SIMPLE,     getAuthMethod(SIMPLE, SIMPLE, UseToken.OTHER));
    assertAuthEquals(TOKEN,      getAuthMethod(SIMPLE, SIMPLE, UseToken.VALID));
    assertAuthEquals(BadToken,   getAuthMethod(SIMPLE, SIMPLE, UseToken.INVALID));

    // A secure client must not fallback
    assertAuthEquals(NoFallback, getAuthMethod(KERBEROS, SIMPLE));
    assertAuthEquals(NoFallback, getAuthMethod(KERBEROS, SIMPLE, UseToken.OTHER));
    assertAuthEquals(TOKEN,      getAuthMethod(KERBEROS, SIMPLE, UseToken.VALID));
    assertAuthEquals(BadToken,   getAuthMethod(KERBEROS, SIMPLE, UseToken.INVALID));
    
    // doesn't try SASL
    assertAuthEquals(Denied(SIMPLE), getAuthMethod(SIMPLE, TOKEN));
    // does try SASL
    assertAuthEquals(No(TOKEN),      getAuthMethod(SIMPLE, TOKEN, UseToken.OTHER));
    assertAuthEquals(TOKEN,          getAuthMethod(SIMPLE, TOKEN, UseToken.VALID));
    assertAuthEquals(BadToken,       getAuthMethod(SIMPLE, TOKEN, UseToken.INVALID));
    
    assertAuthEquals(No(TOKEN),      getAuthMethod(KERBEROS, TOKEN));
    assertAuthEquals(No(TOKEN),      getAuthMethod(KERBEROS, TOKEN, UseToken.OTHER));
    assertAuthEquals(TOKEN,          getAuthMethod(KERBEROS, TOKEN, UseToken.VALID));
    assertAuthEquals(BadToken,       getAuthMethod(KERBEROS, TOKEN, UseToken.INVALID));
  }

  @Test
  public void testSimpleServerWithTokens() throws Exception {
    // Client not using tokens
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE));
    // SASL methods are reverted to SIMPLE
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE));

    // Use tokens. But tokens are ignored because client is reverted to simple
    // due to server not using tokens
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.VALID));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.OTHER));

    // server isn't really advertising tokens
    enableSecretManager = true;
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, UseToken.VALID));
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, UseToken.OTHER));
    
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.VALID));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.OTHER));
    
    // now the simple server takes tokens
    forceSecretManager = true;
    assertAuthEquals(TOKEN,  getAuthMethod(SIMPLE,   SIMPLE, UseToken.VALID));
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, UseToken.OTHER));
    
    assertAuthEquals(TOKEN,  getAuthMethod(KERBEROS, SIMPLE, UseToken.VALID));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.OTHER));
  }

  @Test
  public void testSimpleServerWithInvalidTokens() throws Exception {
    // Tokens are ignored because client is reverted to simple
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, UseToken.INVALID));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.INVALID));
    enableSecretManager = true;
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, UseToken.INVALID));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, UseToken.INVALID));
    forceSecretManager = true;
    assertAuthEquals(BadToken, getAuthMethod(SIMPLE,   SIMPLE, UseToken.INVALID));
    assertAuthEquals(BadToken, getAuthMethod(KERBEROS, SIMPLE, UseToken.INVALID));
  }
  
  /*
   *  token server
   */
  @Test
  public void testTokenOnlyServer() throws Exception {
    // simple client w/o tokens won't try SASL, so server denies
    assertAuthEquals(Denied(SIMPLE), getAuthMethod(SIMPLE,   TOKEN));
    assertAuthEquals(No(TOKEN),      getAuthMethod(SIMPLE,   TOKEN, UseToken.OTHER));
    assertAuthEquals(No(TOKEN),      getAuthMethod(KERBEROS, TOKEN));
    assertAuthEquals(No(TOKEN),      getAuthMethod(KERBEROS, TOKEN, UseToken.OTHER));
  }

  @Test
  public void testTokenOnlyServerWithTokens() throws Exception {
    assertAuthEquals(TOKEN,       getAuthMethod(SIMPLE,   TOKEN, UseToken.VALID));
    assertAuthEquals(TOKEN,       getAuthMethod(KERBEROS, TOKEN, UseToken.VALID));
    enableSecretManager = false;
    assertAuthEquals(NoTokenAuth, getAuthMethod(SIMPLE,   TOKEN, UseToken.VALID));
    assertAuthEquals(NoTokenAuth, getAuthMethod(KERBEROS, TOKEN, UseToken.VALID));
  }

  @Test
  public void testTokenOnlyServerWithInvalidTokens() throws Exception {
    assertAuthEquals(BadToken,    getAuthMethod(SIMPLE,   TOKEN, UseToken.INVALID));
    assertAuthEquals(BadToken,    getAuthMethod(KERBEROS, TOKEN, UseToken.INVALID));
    enableSecretManager = false;
    assertAuthEquals(NoTokenAuth, getAuthMethod(SIMPLE,   TOKEN, UseToken.INVALID));
    assertAuthEquals(NoTokenAuth, getAuthMethod(KERBEROS, TOKEN, UseToken.INVALID));
  }

  /*
   * kerberos server
   */
  @Test
  public void testKerberosServer() throws Exception {
    // doesn't try SASL
    assertAuthEquals(Denied(SIMPLE),     getAuthMethod(SIMPLE,   KERBEROS));
    // does try SASL
    assertAuthEquals(No(TOKEN,KERBEROS), getAuthMethod(SIMPLE,   KERBEROS, UseToken.OTHER));
    // no tgt
    assertAuthEquals(KrbFailed,          getAuthMethod(KERBEROS, KERBEROS));
    assertAuthEquals(KrbFailed,          getAuthMethod(KERBEROS, KERBEROS, UseToken.OTHER));
  }

  @Test
  public void testKerberosServerWithTokens() throws Exception {
    // can use tokens regardless of auth
    assertAuthEquals(TOKEN,        getAuthMethod(SIMPLE,   KERBEROS, UseToken.VALID));
    assertAuthEquals(TOKEN,        getAuthMethod(KERBEROS, KERBEROS, UseToken.VALID));
    enableSecretManager = false;
    // shouldn't even try token because server didn't tell us to
    assertAuthEquals(No(KERBEROS), getAuthMethod(SIMPLE,   KERBEROS, UseToken.VALID));
    assertAuthEquals(KrbFailed,    getAuthMethod(KERBEROS, KERBEROS, UseToken.VALID));
  }

  @Test
  public void testKerberosServerWithInvalidTokens() throws Exception {
    assertAuthEquals(BadToken,     getAuthMethod(SIMPLE,   KERBEROS, UseToken.INVALID));
    assertAuthEquals(BadToken,     getAuthMethod(KERBEROS, KERBEROS, UseToken.INVALID));
    enableSecretManager = false;
    assertAuthEquals(No(KERBEROS), getAuthMethod(SIMPLE,   KERBEROS, UseToken.INVALID));
    assertAuthEquals(KrbFailed,    getAuthMethod(KERBEROS, KERBEROS, UseToken.INVALID));
  }


  // test helpers

  private String getAuthMethod(
      final AuthMethod clientAuth,
      final AuthMethod serverAuth) throws Exception {
    try {
      return internalGetAuthMethod(clientAuth, serverAuth, UseToken.NONE);
    } catch (Exception e) {
      LOG.warn("Auth method failure", e);
      return e.toString();
    }
  }

  private String getAuthMethod(
      final AuthMethod clientAuth,
      final AuthMethod serverAuth,
      final UseToken tokenType) throws Exception {
    try {
      return internalGetAuthMethod(clientAuth, serverAuth, tokenType);
    } catch (Exception e) {
      LOG.warn("Auth method failure", e);
      return e.toString();
    }
  }
  
  private String internalGetAuthMethod(
      final AuthMethod clientAuth,
      final AuthMethod serverAuth,
      final UseToken tokenType) throws Exception {
    
    final Configuration serverConf = new Configuration(conf);
    serverConf.set(HADOOP_SECURITY_AUTHENTICATION, serverAuth.toString());
    UserGroupInformation.setConfiguration(serverConf);
    
    final UserGroupInformation serverUgi = (serverAuth == KERBEROS)
        ? UserGroupInformation.createRemoteUser("server/localhost@NONE")
        : UserGroupInformation.createRemoteUser("server");
    serverUgi.setAuthenticationMethod(serverAuth);

    final TestTokenSecretManager sm = new TestTokenSecretManager();
    boolean useSecretManager = (serverAuth != SIMPLE);
    if (enableSecretManager != null) {
      useSecretManager &= enableSecretManager.booleanValue();
    }
    if (forceSecretManager != null) {
      useSecretManager |= forceSecretManager.booleanValue();
    }
    final SecretManager<?> serverSm = useSecretManager ? sm : null;
    
    Server server = serverUgi.doAs(new PrivilegedExceptionAction<Server>() {
      @Override
      public Server run() throws IOException {
        Server server = new RPC.Builder(serverConf)
        .setProtocol(TestSaslProtocol.class)
        .setInstance(new TestSaslImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true)
        .setSecretManager(serverSm)
        .build();      
        server.start();
        return server;
      }
    });

    final Configuration clientConf = new Configuration(conf);
    clientConf.set(HADOOP_SECURITY_AUTHENTICATION, clientAuth.toString());
    clientConf.setBoolean(
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        clientFallBackToSimpleAllowed);
    UserGroupInformation.setConfiguration(clientConf);
    
    final UserGroupInformation clientUgi =
        UserGroupInformation.createRemoteUser("client");
    clientUgi.setAuthenticationMethod(clientAuth);    

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    if (tokenType != UseToken.NONE) {
      TestTokenIdentifier tokenId = new TestTokenIdentifier(
          new Text(clientUgi.getUserName()));
      Token<TestTokenIdentifier> token = null;
      switch (tokenType) {
        case VALID:
          token = new Token<TestTokenIdentifier>(tokenId, sm);
          SecurityUtil.setTokenService(token, addr);
          break;
        case INVALID:
          token = new Token<TestTokenIdentifier>(
              tokenId.getBytes(), "bad-password!".getBytes(),
              tokenId.getKind(), null);
          SecurityUtil.setTokenService(token, addr);
          break;
        case OTHER:
          token = new Token<TestTokenIdentifier>();
          break;
        case NONE: // won't get here
      }
      clientUgi.addToken(token);
    }

    try {
      LOG.info("trying ugi:"+clientUgi+" tokens:"+clientUgi.getTokens());
      return clientUgi.doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws IOException {
          TestSaslProtocol proxy = null;
          try {
            proxy = RPC.getProxy(TestSaslProtocol.class,
                TestSaslProtocol.versionID, addr, clientConf);
            
            proxy.ping();
            // make sure the other side thinks we are who we said we are!!!
            assertEquals(clientUgi.getUserName(), proxy.getAuthUser());
            AuthMethod authMethod = proxy.getAuthMethod();
            // verify sasl completed with correct QOP
            assertEquals((authMethod != SIMPLE) ? expectedQop.saslQop : null,
                         RPC.getConnectionIdForProxy(proxy).getSaslQop());            
            return authMethod.toString();
          } finally {
            if (proxy != null) {
              RPC.stopProxy(proxy);
            }
          }
        }
      });
    } finally {
      server.stop();
    }
  }

  private static void assertAuthEquals(AuthMethod expect,
      String actual) {
    assertEquals(expect.toString(), actual);
  }
  
  private static void assertAuthEquals(Pattern expect,
      String actual) {
    // this allows us to see the regexp and the value it didn't match
    if (!expect.matcher(actual).matches()) {
      assertEquals(expect, actual); // it failed
    } else {
      assertTrue(true); // it matched
    }
  }

  /*
   * Class used to test overriding QOP values using SaslPropertiesResolver
   */
  static class AuthSaslPropertiesResolver extends SaslPropertiesResolver{

    @Override
    public Map<String, String> getServerProperties(InetAddress address) {
      Map<String, String> newPropertes = new HashMap<String, String>(getDefaultProperties());
      newPropertes.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.getSaslQop());
      return newPropertes;
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
