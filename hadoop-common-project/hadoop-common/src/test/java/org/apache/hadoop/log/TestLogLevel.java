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
package org.apache.hadoop.log;

import java.io.File;
import java.net.SocketException;
import java.net.URI;
import java.util.concurrent.Callable;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.log.LogLevel.CLI;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.Assert;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test LogLevel.
 */
public class TestLogLevel extends KerberosSecurityTestcase {
  private static final File BASEDIR = GenericTestUtils.getRandomizedTestDir();
  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration conf;
  private static Configuration sslConf;
  private final String logName = TestLogLevel.class.getName();
  private String clientPrincipal;
  private String serverPrincipal;
  private final Logger log = Logger.getLogger(logName);
  private final static String PRINCIPAL = "loglevel.principal";
  private final static String KEYTAB  = "loglevel.keytab";
  private static final String PREFIX = "hadoop.http.authentication.";

  @BeforeClass
  public static void setUp() throws Exception {
    org.slf4j.Logger logger =
        LoggerFactory.getLogger(KerberosAuthenticator.class);
    GenericTestUtils.setLogLevel(logger, Level.DEBUG);
    FileUtil.fullyDelete(BASEDIR);
    if (!BASEDIR.mkdirs()) {
      throw new Exception("unable to create the base directory for testing");
    }
    conf = new Configuration();

    setupSSL(BASEDIR);
  }

  static private void setupSSL(File base) throws Exception {
    keystoresDir = base.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestLogLevel.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    sslConf = KeyStoreTestUtil.getSslConfig();
  }

  @Before
  public void setupKerberos() throws Exception {
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    clientPrincipal = KerberosTestUtils.getClientPrincipal();
    serverPrincipal = KerberosTestUtils.getServerPrincipal();
    clientPrincipal = clientPrincipal.substring(0,
        clientPrincipal.lastIndexOf("@"));
    serverPrincipal = serverPrincipal.substring(0,
        serverPrincipal.lastIndexOf("@"));
    getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    FileUtil.fullyDelete(BASEDIR);
  }

  /**
   * Test client command line options. Does not validate server behavior.
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testCommandOptions() throws Exception {
    final String className = this.getClass().getName();

    assertFalse(validateCommand(new String[] {"-foo" }));
    // fail due to insufficient number of arguments
    assertFalse(validateCommand(new String[] {}));
    assertFalse(validateCommand(new String[] {"-getlevel" }));
    assertFalse(validateCommand(new String[] {"-setlevel" }));
    assertFalse(validateCommand(new String[] {"-getlevel", "foo.bar:8080" }));

    // valid command arguments
    assertTrue(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className }));
    assertTrue(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG" }));
    assertTrue(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className, "-protocol",
            "http" }));
    assertTrue(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className, "-protocol",
            "https" }));
    assertTrue(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG",
            "-protocol", "http" }));
    assertTrue(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG",
            "-protocol", "https" }));

    // fail due to the extra argument
    assertFalse(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className, "-protocol",
            "https", "blah" }));
    assertFalse(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG",
            "-protocol", "https", "blah" }));
    assertFalse(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className, "-protocol",
            "https", "-protocol", "https" }));
    assertFalse(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className,
            "-setlevel", "foo.bar:8080", className }));
  }

  /**
   * Check to see if a command can be accepted.
   *
   * @param args a String array of arguments
   * @return true if the command can be accepted, false if not.
   */
  private boolean validateCommand(String[] args) throws Exception {
    CLI cli = new CLI(sslConf);
    try {
      cli.parseArguments(args);
    } catch (HadoopIllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      // this is used to verify the command arguments only.
      // no HadoopIllegalArgumentException = the arguments are good.
      return true;
    }
    return true;
  }

  /**
   * Creates and starts a Jetty server binding at an ephemeral port to run
   * LogLevel servlet.
   * @param protocol "http" or "https"
   * @param isSpnego true if SPNEGO is enabled
   * @return a created HttpServer2 object
   * @throws Exception if unable to create or start a Jetty server
   */
  private HttpServer2 createServer(String protocol, boolean isSpnego)
      throws Exception {
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("..")
        .addEndpoint(new URI(protocol + "://localhost:0"))
        .setFindPort(true)
        .setConf(conf);
    if (isSpnego) {
      // Set up server Kerberos credentials.
      // Since the server may fall back to simple authentication,
      // use ACL to make sure the connection is Kerberos/SPNEGO authenticated.
      builder.setSecurityEnabled(true)
          .setUsernameConfKey(PRINCIPAL)
          .setKeytabConfKey(KEYTAB)
          .setACL(new AccessControlList(clientPrincipal));
    }

    // if using HTTPS, configure keystore/truststore properties.
    if (protocol.equals(LogLevel.PROTOCOL_HTTPS)) {
      builder = builder.
          keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
          .keyStore(sslConf.get("ssl.server.keystore.location"),
              sslConf.get("ssl.server.keystore.password"),
              sslConf.get("ssl.server.keystore.type", "jks"))
          .trustStore(sslConf.get("ssl.server.truststore.location"),
              sslConf.get("ssl.server.truststore.password"),
              sslConf.get("ssl.server.truststore.type", "jks"));
    }
    HttpServer2 server = builder.build();
    // Enable SPNEGO for LogLevel servlet
    if (isSpnego) {
      server.addInternalServlet("logLevel", "/logLevel", LogLevel.Servlet.class,
          true);
    }
    server.start();
    return server;
  }

  private void testDynamicLogLevel(final String bindProtocol,
      final String connectProtocol, final boolean isSpnego)
      throws Exception {
    testDynamicLogLevel(bindProtocol, connectProtocol, isSpnego,
        Level.DEBUG.toString());
  }

  /**
   * Run both client and server using the given protocol.
   *
   * @param bindProtocol specify either http or https for server
   * @param connectProtocol specify either http or https for client
   * @param isSpnego true if SPNEGO is enabled
   * @throws Exception
   */
  private void testDynamicLogLevel(final String bindProtocol,
      final String connectProtocol, final boolean isSpnego,
      final String newLevel) throws Exception {
    if (!LogLevel.isValidProtocol(bindProtocol)) {
      throw new Exception("Invalid server protocol " + bindProtocol);
    }
    if (!LogLevel.isValidProtocol(connectProtocol)) {
      throw new Exception("Invalid client protocol " + connectProtocol);
    }
    Level oldLevel = log.getEffectiveLevel();
    Assert.assertNotEquals("Get default Log Level which shouldn't be ERROR.",
        Level.ERROR, oldLevel);

    // configs needed for SPNEGO at server side
    if (isSpnego) {
      conf.set(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
      conf.set(KEYTAB, KerberosTestUtils.getKeytabFile());
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      conf.set(PREFIX + "type", "kerberos");
      conf.set(PREFIX + "kerberos.keytab", KerberosTestUtils.getKeytabFile());
      conf.set(PREFIX + "kerberos.principal",
           KerberosTestUtils.getServerPrincipal());
      conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
           AuthenticationFilterInitializer.class.getName());

      conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
          true);
      UserGroupInformation.setConfiguration(conf);
    }

    final HttpServer2 server = createServer(bindProtocol, isSpnego);
    // get server port
    final String authority = NetUtils.getHostPortString(server
        .getConnectorAddress(0));

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // client command line
        getLevel(connectProtocol, authority);
        setLevel(connectProtocol, authority, newLevel);
        return null;
      }
    });
    server.stop();
    // restore log level
    GenericTestUtils.setLogLevel(log, oldLevel);
  }

  /**
   * Run LogLevel command line to start a client to get log level of this test
   * class.
   *
   * @param protocol specify either http or https
   * @param authority daemon's web UI address
   * @throws Exception if unable to connect
   */
  private void getLevel(String protocol, String authority) throws Exception {
    String[] getLevelArgs = {"-getlevel", authority, logName, "-protocol",
        protocol};
    CLI cli = new CLI(sslConf);
    cli.run(getLevelArgs);
  }

  /**
   * Run LogLevel command line to start a client to set log level of this test
   * class to debug.
   *
   * @param protocol specify either http or https
   * @param authority daemon's web UI address
   * @throws Exception if unable to run or log level does not change as expected
   */
  private void setLevel(String protocol, String authority, String newLevel)
      throws Exception {
    String[] setLevelArgs = {"-setlevel", authority, logName,
        newLevel, "-protocol", protocol};
    CLI cli = new CLI(sslConf);
    cli.run(setLevelArgs);

    assertEquals("new level not equal to expected: ", newLevel.toUpperCase(),
        log.getEffectiveLevel().toString());
  }

  /**
   * Test setting log level to "Info".
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testInfoLogLevel() throws Exception {
    testDynamicLogLevel(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTP, false,
        "Info");
  }

  /**
   * Test setting log level to "Error".
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testErrorLogLevel() throws Exception {
    testDynamicLogLevel(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTP, false,
        "Error");
  }

  /**
   * Server runs HTTP, no SPNEGO.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testLogLevelByHttp() throws Exception {
    testDynamicLogLevel(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTP, false);
    try {
      testDynamicLogLevel(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTPS,
          false);
      fail("A HTTPS Client should not have succeeded in connecting to a " +
          "HTTP server");
    } catch (SSLException e) {
      GenericTestUtils.assertExceptionContains("Error while authenticating "
          + "with endpoint", e);
      GenericTestUtils.assertExceptionContains("recognized SSL message", e
          .getCause());
    }
  }

  /**
   * Server runs HTTP + SPNEGO.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testLogLevelByHttpWithSpnego() throws Exception {
    testDynamicLogLevel(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTP, true);
    try {
      testDynamicLogLevel(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTPS,
          true);
      fail("A HTTPS Client should not have succeeded in connecting to a " +
          "HTTP server");
    } catch (SSLException e) {
      GenericTestUtils.assertExceptionContains("Error while authenticating "
          + "with endpoint", e);
      GenericTestUtils.assertExceptionContains("recognized SSL message", e
          .getCause());
    }
  }

  /**
   * Server runs HTTPS, no SPNEGO.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testLogLevelByHttps() throws Exception {
    testDynamicLogLevel(LogLevel.PROTOCOL_HTTPS, LogLevel.PROTOCOL_HTTPS,
        false);
    try {
      testDynamicLogLevel(LogLevel.PROTOCOL_HTTPS, LogLevel.PROTOCOL_HTTP,
          false);
      fail("A HTTP Client should not have succeeded in connecting to a " +
          "HTTPS server");
    } catch (SocketException e) {
      GenericTestUtils.assertExceptionContains("Error while authenticating "
          + "with endpoint", e);
      GenericTestUtils.assertExceptionContains(
          "Unexpected end of file from server", e.getCause());
    }
  }

  /**
   * Server runs HTTPS + SPNEGO.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testLogLevelByHttpsWithSpnego() throws Exception {
    testDynamicLogLevel(LogLevel.PROTOCOL_HTTPS, LogLevel.PROTOCOL_HTTPS,
        true);
    try {
      testDynamicLogLevel(LogLevel.PROTOCOL_HTTPS, LogLevel.PROTOCOL_HTTP,
          true);
      fail("A HTTP Client should not have succeeded in connecting to a " +
          "HTTPS server");
    }  catch (SocketException e) {
      GenericTestUtils.assertExceptionContains("Error while authenticating "
          + "with endpoint", e);
      GenericTestUtils.assertExceptionContains(
          "Unexpected end of file from server", e.getCause());
    }
  }
}