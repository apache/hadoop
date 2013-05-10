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

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.*;
import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for using Sasl over RPC. */
public class TestSaslRPC {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestSaslRPC.class);
  
  static final String ERROR_MESSAGE = "Token is invalid";
  static final String SERVER_PRINCIPAL_KEY = "test.ipc.server.principal";
  static final String SERVER_KEYTAB_KEY = "test.ipc.server.keytab";
  static final String SERVER_PRINCIPAL_1 = "p1/foo@BAR";
  static final String SERVER_PRINCIPAL_2 = "p2/foo@BAR";
  private static Configuration conf;
  static Boolean forceSecretManager = null;
  
  @BeforeClass
  public static void setupKerb() {
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.realm", "NONE");
    Security.addProvider(new SaslPlainServer.SecurityProvider());
  }    

  @Before
  public void setup() {
    conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    forceSecretManager = null;
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
      if ("".equals(realUser.toString())) {
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
    public AuthenticationMethod getAuthMethod() throws IOException;
    public String getAuthUser() throws IOException;
  }
  
  public static class TestSaslImpl extends TestRPC.TestImpl implements
      TestSaslProtocol {
    @Override
    public AuthenticationMethod getAuthMethod() throws IOException {
      return UserGroupInformation.getCurrentUser().getAuthenticationMethod();
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
      assertTrue(ERROR_MESSAGE.equals(e.getLocalizedMessage()));
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
      proxy = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, conf);
      //QOP must be auth
      Assert.assertEquals(SaslRpcServer.SASL_PROPS.get(Sasl.QOP), "auth");
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
  public void testGetRemotePrincipal() throws Exception {
    try {
      Configuration newConf = new Configuration(conf);
      newConf.set(SERVER_PRINCIPAL_KEY, SERVER_PRINCIPAL_1);
      ConnectionId remoteId = ConnectionId.getConnectionId(
          new InetSocketAddress(0), TestSaslProtocol.class, null, 0, newConf);
      assertEquals(SERVER_PRINCIPAL_1, remoteId.getServerPrincipal());
      // this following test needs security to be off
      SecurityUtil.setAuthenticationMethod(SIMPLE, newConf);
      UserGroupInformation.setConfiguration(newConf);
      remoteId = ConnectionId.getConnectionId(new InetSocketAddress(0),
          TestSaslProtocol.class, null, 0, newConf);
      assertEquals(
          "serverPrincipal should be null when security is turned off", null,
          remoteId.getServerPrincipal());
    } finally {
      // revert back to security is on
      UserGroupInformation.setConfiguration(conf);
    }
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
    newConf.set(SERVER_PRINCIPAL_KEY, SERVER_PRINCIPAL_1);

    TestSaslProtocol proxy1 = null;
    TestSaslProtocol proxy2 = null;
    TestSaslProtocol proxy3 = null;
    try {
      proxy1 = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy1.getAuthMethod();
      Client client = WritableRpcEngine.getClient(conf);
      Set<ConnectionId> conns = client.getConnectionIds();
      assertEquals("number of connections in cache is wrong", 1, conns.size());
      // same conf, connection should be re-used
      proxy2 = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy2.getAuthMethod();
      assertEquals("number of connections in cache is wrong", 1, conns.size());
      // different conf, new connection should be set up
      newConf.set(SERVER_PRINCIPAL_KEY, SERVER_PRINCIPAL_2);
      proxy3 = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
          TestSaslProtocol.versionID, addr, newConf);
      proxy3.getAuthMethod();
      ConnectionId[] connsArray = conns.toArray(new ConnectionId[0]);
      assertEquals("number of connections in cache is wrong", 2,
          connsArray.length);
      String p1 = connsArray[0].getServerPrincipal();
      String p2 = connsArray[1].getServerPrincipal();
      assertFalse("should have different principals", p1.equals(p2));
      assertTrue("principal not as expected", p1.equals(SERVER_PRINCIPAL_1)
          || p1.equals(SERVER_PRINCIPAL_2));
      assertTrue("principal not as expected", p2.equals(SERVER_PRINCIPAL_1)
          || p2.equals(SERVER_PRINCIPAL_2));
    } finally {
      server.stop();
      RPC.stopProxy(proxy1);
      RPC.stopProxy(proxy2);
      RPC.stopProxy(proxy3);
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
      proxy = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
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
  public void testSaslPlainServerBadPassword() throws IOException {
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
  private static Pattern Denied(AuthenticationMethod method) {
      return Pattern.compile(".*RemoteException.*AccessControlException.*: "
          +method.getAuthMethod() + " authentication is not enabled.*");
  }
  private static Pattern NoTokenAuth =
      Pattern.compile(".*IllegalArgumentException: " +
                      "TOKEN authentication requires a secret manager");
  
  /*
   *  simple server
   */
  @Test
  public void testSimpleServer() throws Exception {
    assertAuthEquals(SIMPLE,    getAuthMethod(SIMPLE,   SIMPLE));
    // SASL methods are reverted to SIMPLE, but test setup fails
    assertAuthEquals(KrbFailed, getAuthMethod(KERBEROS, SIMPLE));
  }

  @Test
  public void testSimpleServerWithTokens() throws Exception {
    // Tokens are ignored because client is reverted to simple
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, true));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, true));
    forceSecretManager = true;
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, true));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, true));
  }
    
  @Test
  public void testSimpleServerWithInvalidTokens() throws Exception {
    // Tokens are ignored because client is reverted to simple
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, false));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, false));
    forceSecretManager = true;
    assertAuthEquals(SIMPLE, getAuthMethod(SIMPLE,   SIMPLE, false));
    assertAuthEquals(SIMPLE, getAuthMethod(KERBEROS, SIMPLE, false));
  }
  
  /*
   *  token server
   */
  @Test
  public void testTokenOnlyServer() throws Exception {
    assertAuthEquals(Denied(SIMPLE), getAuthMethod(SIMPLE,   TOKEN));
    assertAuthEquals(KrbFailed,      getAuthMethod(KERBEROS, TOKEN));
  }

  @Test
  public void testTokenOnlyServerWithTokens() throws Exception {
    assertAuthEquals(TOKEN, getAuthMethod(SIMPLE,   TOKEN, true));
    assertAuthEquals(TOKEN, getAuthMethod(KERBEROS, TOKEN, true));
    forceSecretManager = false;
    assertAuthEquals(NoTokenAuth, getAuthMethod(SIMPLE,   TOKEN, true));
    assertAuthEquals(NoTokenAuth, getAuthMethod(KERBEROS, TOKEN, true));
  }

  @Test
  public void testTokenOnlyServerWithInvalidTokens() throws Exception {
    assertAuthEquals(BadToken, getAuthMethod(SIMPLE,   TOKEN, false));
    assertAuthEquals(BadToken, getAuthMethod(KERBEROS, TOKEN, false));
    forceSecretManager = false;
    assertAuthEquals(NoTokenAuth, getAuthMethod(SIMPLE,   TOKEN, false));
    assertAuthEquals(NoTokenAuth, getAuthMethod(KERBEROS, TOKEN, false));
  }

  /*
   * kerberos server
   */
  @Test
  public void testKerberosServer() throws Exception {
    assertAuthEquals(Denied(SIMPLE), getAuthMethod(SIMPLE,   KERBEROS));
    assertAuthEquals(KrbFailed,      getAuthMethod(KERBEROS, KERBEROS));    
  }

  @Test
  public void testKerberosServerWithTokens() throws Exception {
    // can use tokens regardless of auth
    assertAuthEquals(TOKEN, getAuthMethod(SIMPLE,   KERBEROS, true));
    assertAuthEquals(TOKEN, getAuthMethod(KERBEROS, KERBEROS, true));
    // can't fallback to simple when using kerberos w/o tokens
    forceSecretManager = false;
    assertAuthEquals(Denied(TOKEN), getAuthMethod(SIMPLE,   KERBEROS, true));
    assertAuthEquals(Denied(TOKEN), getAuthMethod(KERBEROS, KERBEROS, true));
  }

  @Test
  public void testKerberosServerWithInvalidTokens() throws Exception {
    assertAuthEquals(BadToken, getAuthMethod(SIMPLE,   KERBEROS, false));
    assertAuthEquals(BadToken, getAuthMethod(KERBEROS, KERBEROS, false));
    forceSecretManager = false;
    assertAuthEquals(Denied(TOKEN), getAuthMethod(SIMPLE,   KERBEROS, false));
    assertAuthEquals(Denied(TOKEN), getAuthMethod(KERBEROS, KERBEROS, false));
  }


  // test helpers

  private String getAuthMethod(
      final AuthenticationMethod clientAuth,
      final AuthenticationMethod serverAuth) throws Exception {
    try {
      return internalGetAuthMethod(clientAuth, serverAuth, false, false);
    } catch (Exception e) {
      return e.toString();
    }
  }

  private String getAuthMethod(
      final AuthenticationMethod clientAuth,
      final AuthenticationMethod serverAuth,
      final boolean useValidToken) throws Exception {
    try {
      return internalGetAuthMethod(clientAuth, serverAuth, true, useValidToken);
    } catch (Exception e) {
      return e.toString();
    }
  }
  
  private String internalGetAuthMethod(
      final AuthenticationMethod clientAuth,
      final AuthenticationMethod serverAuth,
      final boolean useToken,
      final boolean useValidToken) throws Exception {
    
    String currentUser = UserGroupInformation.getCurrentUser().getUserName();
    
    final Configuration serverConf = new Configuration(conf);
    SecurityUtil.setAuthenticationMethod(serverAuth, serverConf);
    UserGroupInformation.setConfiguration(serverConf);
    
    final UserGroupInformation serverUgi =
        UserGroupInformation.createRemoteUser(currentUser + "-SERVER");
    serverUgi.setAuthenticationMethod(serverAuth);

    final TestTokenSecretManager sm = new TestTokenSecretManager();
    boolean useSecretManager = (serverAuth != SIMPLE);
    if (forceSecretManager != null) {
      useSecretManager &= forceSecretManager.booleanValue();
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
    SecurityUtil.setAuthenticationMethod(clientAuth, clientConf);
    UserGroupInformation.setConfiguration(clientConf);
    
    final UserGroupInformation clientUgi =
        UserGroupInformation.createRemoteUser(currentUser + "-CLIENT");
    clientUgi.setAuthenticationMethod(clientAuth);    

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    if (useToken) {
      TestTokenIdentifier tokenId = new TestTokenIdentifier(
          new Text(clientUgi.getUserName()));
      Token<TestTokenIdentifier> token = useValidToken
          ? new Token<TestTokenIdentifier>(tokenId, sm)
          : new Token<TestTokenIdentifier>(
              tokenId.getBytes(), "bad-password!".getBytes(),
              tokenId.getKind(), null);
      
      SecurityUtil.setTokenService(token, addr);
      clientUgi.addToken(token);
    }

    try {
      return clientUgi.doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws IOException {
          TestSaslProtocol proxy = null;
          try {
            proxy = (TestSaslProtocol) RPC.getProxy(TestSaslProtocol.class,
                TestSaslProtocol.versionID, addr, clientConf);
            
            proxy.ping();
            // verify sasl completed
            if (serverAuth != SIMPLE) {
              assertEquals(SaslRpcServer.SASL_PROPS.get(Sasl.QOP), "auth");
            }
            
            // make sure the other side thinks we are who we said we are!!!
            assertEquals(clientUgi.getUserName(), proxy.getAuthUser());
            return proxy.getAuthMethod().toString();
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

  private static void assertAuthEquals(AuthenticationMethod expect,
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
