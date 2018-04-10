
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
package org.apache.hadoop.crypto.key.kms.server;

import com.google.common.base.Supplier;
import com.google.common.cache.LoadingCache;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.KMSDelegationToken;
import org.apache.hadoop.crypto.key.kms.KMSTokenRenewer;
import org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider;
import org.apache.hadoop.crypto.key.kms.TestLoadBalancingKMSClientProvider;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.KMSUtil;
import org.apache.hadoop.util.KMSUtilFaultInjector;
import org.apache.hadoop.util.Time;
import org.apache.http.client.utils.URIBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.event.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_COPY_LEGACY_TOKEN_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_KIND;
import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_LEGACY_KIND;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class TestKMS {
  private static final Logger LOG = LoggerFactory.getLogger(TestKMS.class);

  private static final String SSL_RELOADER_THREAD_NAME =
      "Truststore reloader thread";

  private SSLFactory sslFactory;

  private final KMSUtilFaultInjector oldInjector =
      KMSUtilFaultInjector.get();

  // Injector to create providers with different ports. Can only happen in tests
  private final KMSUtilFaultInjector testInjector =
      new KMSUtilFaultInjector() {
        @Override
        public KeyProvider createKeyProviderForTests(String value,
            Configuration conf) throws IOException {
          return TestLoadBalancingKMSClientProvider
              .createKeyProviderForTests(value, conf);
        }
      };

  // Keep track of all key providers created during a test case, so they can be
  // closed at test tearDown.
  private List<KeyProvider> providersCreated = new LinkedList<>();

  @Rule
  public final Timeout testTimeout = new Timeout(180000);

  @Before
  public void setUp() throws Exception {
    GenericTestUtils.setLogLevel(KMSClientProvider.LOG, Level.TRACE);
    GenericTestUtils
        .setLogLevel(DelegationTokenAuthenticationHandler.LOG, Level.TRACE);
    GenericTestUtils
        .setLogLevel(DelegationTokenAuthenticator.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(KMSUtil.LOG, Level.TRACE);
    // resetting kerberos security
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
  }

  public static File getTestDir() throws Exception {
    File file = new File("dummy");
    file = file.getAbsoluteFile();
    file = file.getParentFile();
    file = new File(file, "target");
    file = new File(file, UUID.randomUUID().toString());
    if (!file.mkdirs()) {
      throw new RuntimeException("Could not create test directory: " + file);
    }
    return file;
  }

  public static abstract class KMSCallable<T> implements Callable<T> {
    private List<URL> kmsUrl;

    protected URL getKMSUrl() {
      return kmsUrl.get(0);
    }

    protected URL[] getKMSHAUrl() {
      URL[] urls = new URL[kmsUrl.size()];
      return kmsUrl.toArray(urls);
    }

    protected void addKMSUrl(URL url) {
      if (kmsUrl == null) {
        kmsUrl = new ArrayList<URL>();
      }
      kmsUrl.add(url);
    }

    /*
     * The format of the returned value will be
     * kms://http:kms1.example1.com:port1,kms://http:kms2.example2.com:port2
     */
    protected String generateLoadBalancingKeyProviderUriString() {
      if (kmsUrl == null || kmsUrl.size() == 0) {
        return null;
      }
      StringBuffer sb = new StringBuffer();

      for (int i = 0; i < kmsUrl.size(); i++) {
        sb.append(KMSClientProvider.SCHEME_NAME + "://" +
            kmsUrl.get(0).getProtocol() + "@");
        URL url = kmsUrl.get(i);
        sb.append(url.getAuthority());
        if (url.getPath() != null) {
          sb.append(url.getPath());
        }
        if (i < kmsUrl.size() - 1) {
          sb.append(",");
        }
      }
      return sb.toString();
    }
  }

  protected KeyProvider createProvider(URI uri, Configuration conf)
      throws IOException {
    final KeyProvider ret = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] {new KMSClientProvider(uri, conf, uri)}, conf);
    providersCreated.add(ret);
    return ret;
  }

  /**
   * create a LoadBalancingKMSClientProvider from an array of URIs.
   * @param uris an array of KMS URIs
   * @param conf configuration object
   * @return a LoadBalancingKMSClientProvider object
   * @throws IOException
   */
  protected LoadBalancingKMSClientProvider createHAProvider(URI[] uris,
      Configuration conf, String originalUri) throws IOException {
    KMSClientProvider[] providers = new KMSClientProvider[uris.length];
    for (int i = 0; i < providers.length; i++) {
      providers[i] =
          new KMSClientProvider(uris[i], conf, URI.create(originalUri));
    }
    return new LoadBalancingKMSClientProvider(providers, conf);
  }

  private KMSClientProvider createKMSClientProvider(URI uri, Configuration conf)
      throws IOException {
    final KMSClientProvider ret = new KMSClientProvider(uri, conf, uri);
    providersCreated.add(ret);
    return ret;
  }

  protected <T> T runServer(String keystore, String password, File confDir,
      KMSCallable<T> callable) throws Exception {
    return runServer(-1, keystore, password, confDir, callable);
  }

  protected <T> T runServer(int port, String keystore, String password, File confDir,
      KMSCallable<T> callable) throws Exception {
    return runServer(new int[] {port}, keystore, password, confDir, callable);
  }

  protected <T> T runServer(int[] ports, String keystore, String password,
      File confDir, KMSCallable<T> callable) throws Exception {
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder().setKmsConfDir(confDir)
        .setLog4jConfFile("log4j.properties");
    if (keystore != null) {
      miniKMSBuilder.setSslConf(new File(keystore), password);
    }
    final List<MiniKMS> kmsList = new ArrayList<>();
    for (int i=0; i< ports.length; i++) {
      if (ports[i] > 0) {
        miniKMSBuilder.setPort(ports[i]);
      }
      MiniKMS miniKMS = miniKMSBuilder.build();
      kmsList.add(miniKMS);
      miniKMS.start();
      LOG.info("Test KMS running at: " + miniKMS.getKMSUrl());
      callable.addKMSUrl(miniKMS.getKMSUrl());
    }
    try {
      return callable.call();
    } finally {
      for (MiniKMS miniKMS: kmsList) {
        miniKMS.stop();
      }
    }
  }

  protected Configuration createBaseKMSConf(File keyStoreDir) throws Exception {
    return createBaseKMSConf(keyStoreDir, null);
  }

  /**
   * The Configuration object is shared by both KMS client and server in unit
   * tests because UGI gets/sets it to a static variable.
   * As a workaround, make sure the client configurations are copied to server
   * so that client can read them.
   * @param keyStoreDir where keystore is located.
   * @param conf KMS client configuration
   * @return KMS server configuration based on client.
   * @throws Exception
   */
  protected Configuration createBaseKMSConf(File keyStoreDir,
      Configuration conf) throws Exception {
    Configuration newConf;
    if (conf == null) {
      newConf = new Configuration(false);
    } else {
      newConf = new Configuration(conf);
    }
    newConf.set(KMSConfiguration.KEY_PROVIDER_URI,
        "jceks://file@" + new Path(keyStoreDir.getAbsolutePath(), "kms.keystore").toUri());
    newConf.set("hadoop.kms.authentication.type", "simple");
    return newConf;
  }

  public static void writeConf(File confDir, Configuration conf)
      throws Exception {
    Writer writer = new FileWriter(new File(confDir,
        KMSConfiguration.KMS_SITE_XML));
    conf.writeXml(writer);
    writer.close();

    writer = new FileWriter(new File(confDir, KMSConfiguration.KMS_ACLS_XML));
    conf.writeXml(writer);
    writer.close();

    //create empty core-site.xml
    writer = new FileWriter(new File(confDir, "core-site.xml"));
    new Configuration(false).writeXml(writer);
    writer.close();
  }

  public static URI createKMSUri(URL kmsUrl) throws Exception {
    String str = kmsUrl.toString();
    str = str.replaceFirst("://", "@");
    return new URI("kms://" + str);
  }

  public static URI[] createKMSHAUri(URL[] kmsUrls) throws Exception {
    URI[] uris = new URI[kmsUrls.length];
    for (int i = 0; i < kmsUrls.length; i++) {
      uris[i] = createKMSUri(kmsUrls[i]);
    }
    return uris;
  }

  private static class KerberosConfiguration
      extends javax.security.auth.login.Configuration {
    private String principal;
    private String keytab;
    private boolean isInitiator;

    private KerberosConfiguration(String principal, File keytab,
        boolean client) {
      this.principal = principal;
      this.keytab = keytab.getAbsolutePath();
      this.isInitiator = client;
    }

    public static javax.security.auth.login.Configuration createClientConfig(
        String principal,
        File keytab) {
      return new KerberosConfiguration(principal, keytab, true);
    }

    private static String getKrb5LoginModuleName() {
      return System.getProperty("java.vendor").contains("IBM")
             ? "com.ibm.security.auth.module.Krb5LoginModule"
             : "com.sun.security.auth.module.Krb5LoginModule";
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", Boolean.toString(isInitiator));
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }
  }

  private static MiniKdc kdc;
  private static File keytab;

  private static void setUpMiniKdc(Properties kdcConf) throws Exception {
    File kdcDir = getTestDir();
    kdc = new MiniKdc(kdcConf, kdcDir);
    kdc.start();
    keytab = new File(kdcDir, "keytab");
    List<String> principals = new ArrayList<String>();
    principals.add("HTTP/localhost");
    principals.add("client");
    principals.add("hdfs");
    principals.add("otheradmin");
    principals.add("client/host");
    principals.add("client1");
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      principals.add(type.toString());
    }
    principals.add("CREATE_MATERIAL");
    principals.add("ROLLOVER_MATERIAL");
    kdc.createPrincipal(keytab,
        principals.toArray(new String[principals.size()]));
  }

  @BeforeClass
  public static void setUpMiniKdc() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    setUpMiniKdc(kdcConf);
  }

  @After
  public void tearDown() throws Exception {
    UserGroupInformation.setShouldRenewImmediatelyForTests(false);
    UserGroupInformation.reset();
    KMSUtilFaultInjector.set(oldInjector);
    if (!providersCreated.isEmpty()) {
      final MultipleIOException.Builder b = new MultipleIOException.Builder();
      for (KeyProvider kp : providersCreated) {
        try {
          kp.close();
        } catch (IOException e) {
          LOG.error("Failed to close key provider.", e);
          b.add(e);
        }
      }
      providersCreated.clear();
      if (!b.isEmpty()) {
        throw b.build();
      }
    }
  }

  @AfterClass
  public static void shutdownMiniKdc() {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
  }

  private <T> T doAs(String user, final PrivilegedExceptionAction<T> action)
      throws Exception {
    UserGroupInformation.loginUserFromKeytab(user, keytab.getAbsolutePath());
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    try {
      return ugi.doAs(action);
    } finally {
      ugi.logoutUserFromKeytab();
    }
  }

  /**
   * Read in the content from an URL connection.
   * @param conn URLConnection To read
   * @return the text from the output
   * @throws IOException if something went wrong
   */
  private static String readOutput(URLConnection conn) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = conn.getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }

  private static void assertReFind(String re, String value) {
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    Assert.assertTrue("'" + p + "' does not match " + value, m.find());
  }

  private URLConnection openJMXConnection(URL baseUrl, boolean kerberos)
      throws Exception {
    URIBuilder b = new URIBuilder(baseUrl + "/jmx");
    if (!kerberos) {
      b.addParameter("user.name", "dr.who");
    }
    URL url = b.build().toURL();
    LOG.info("JMX URL " + url);
    URLConnection conn = url.openConnection();
    if (sslFactory != null) {
      HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
      try {
        httpsConn.setSSLSocketFactory(sslFactory.createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      httpsConn.setHostnameVerifier(sslFactory.getHostnameVerifier());
    }
    return conn;
  }

  private void testJMXQuery(URL baseUrl, boolean kerberos) throws Exception {
    LOG.info("Testing JMX");
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"",
        readOutput(openJMXConnection(baseUrl, kerberos)));
  }

  public void testStartStop(final boolean ssl, final boolean kerberos)
      throws Exception {
    Configuration conf = new Configuration();
    if (kerberos) {
      conf.set("hadoop.security.authentication", "kerberos");
    }
    File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);

    final String keystore;
    final String password;
    if (ssl) {
      String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestKMS.class);
      KeyStoreTestUtil.setupSSLConfig(testDir.getAbsolutePath(), sslConfDir,
          conf, false);
      keystore = testDir.getAbsolutePath() + "/serverKS.jks";
      password = "serverP";
    } else {
      keystore = null;
      password = null;
    }

    conf.set("hadoop.kms.authentication.token.validity", "1");
    if (kerberos) {
      conf.set("hadoop.kms.authentication.type", "kerberos");
      conf.set("hadoop.kms.authentication.kerberos.keytab",
          keytab.getAbsolutePath());
      conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
      conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    }

    writeConf(testDir, conf);

    if (ssl) {
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
    }

    runServer(keystore, password, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        URL url = getKMSUrl();
        Assert.assertEquals(keystore != null,
            url.getProtocol().equals("https"));
        final URI uri = createKMSUri(getKMSUrl());

        if (ssl) {
          KeyProvider testKp = createProvider(uri, conf);
          ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
          while (threadGroup.getParent() != null) {
            threadGroup = threadGroup.getParent();
          }
          Thread[] threads = new Thread[threadGroup.activeCount()];
          threadGroup.enumerate(threads);
          Thread reloaderThread = null;
          for (Thread thread : threads) {
            if ((thread.getName() != null)
                && (thread.getName().contains(SSL_RELOADER_THREAD_NAME))) {
              reloaderThread = thread;
            }
          }
          Assert.assertTrue("Reloader is not alive", reloaderThread.isAlive());
          // Explicitly close the provider so we can verify the internal thread
          // is shutdown
          testKp.close();
          boolean reloaderStillAlive = true;
          for (int i = 0; i < 10; i++) {
            reloaderStillAlive = reloaderThread.isAlive();
            if (!reloaderStillAlive) break;
            Thread.sleep(1000);
          }
          Assert.assertFalse("Reloader is still alive", reloaderStillAlive);
        }

        if (kerberos) {
          for (String user : new String[]{"client", "client/host"}) {
            doAs(user, new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                testJMXQuery(url, kerberos);

                final KeyProvider kp = createProvider(uri, conf);
                // getKeys() empty
                Assert.assertTrue(kp.getKeys().isEmpty());

                Thread.sleep(4000);
                Token<?>[] tokens =
                    ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)kp)
                    .addDelegationTokens("myuser", new Credentials());
                assertEquals(2, tokens.length);
                assertEquals(KMSDelegationToken.TOKEN_KIND,
                    tokens[0].getKind());
                kp.close();
                return null;
              }
            });
          }
        } else {
          testJMXQuery(url, kerberos);

          KeyProvider kp = createProvider(uri, conf);
          // getKeys() empty
          Assert.assertTrue(kp.getKeys().isEmpty());

          Thread.sleep(4000);
          Token<?>[] tokens =
              ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)kp)
              .addDelegationTokens("myuser", new Credentials());
          assertEquals(2, tokens.length);
          assertEquals(KMSDelegationToken.TOKEN_KIND, tokens[0].getKind());
          kp.close();
        }
        return null;
      }
    });

    if (sslFactory != null) {
      sslFactory.destroy();
      sslFactory = null;
    }
  }

  @Test
  public void testStartStopHttpPseudo() throws Exception {
    testStartStop(false, false);
  }

  @Test
  public void testStartStopHttpsPseudo() throws Exception {
    testStartStop(true, false);
  }

  @Test
  public void testStartStopHttpKerberos() throws Exception {
    testStartStop(false, true);
  }

  @Test
  public void testStartStopHttpsKerberos() throws Exception {
    testStartStop(true, true);
  }

  @Test(timeout = 30000)
  public void testSpecialKeyNames() throws Exception {
    final String specialKey = "key %^[\n{]}|\"<>\\";
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    File confDir = getTestDir();
    conf = createBaseKMSConf(confDir, conf);
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + specialKey + ".ALL", "*");
    writeConf(confDir, conf);

    runServer(null, null, confDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = new Configuration();
        URI uri = createKMSUri(getKMSUrl());
        KeyProvider kp = createProvider(uri, conf);
        Assert.assertTrue(kp.getKeys().isEmpty());
        Assert.assertEquals(0, kp.getKeysMetadata().length);

        KeyProvider.Options options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        options.setDescription("l1");
        LOG.info("Creating key with name '{}'", specialKey);

        KeyProvider.KeyVersion kv0 = kp.createKey(specialKey, options);
        Assert.assertNotNull(kv0);
        Assert.assertEquals(specialKey, kv0.getName());
        Assert.assertNotNull(kv0.getVersionName());
        Assert.assertNotNull(kv0.getMaterial());
        return null;
      }
    });
  }

  @Test
  @SuppressWarnings("checkstyle:methodlength")
  public void testKMSProvider() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    File confDir = getTestDir();
    conf = createBaseKMSConf(confDir, conf);
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k1.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k2.MANAGEMENT", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k2.READ", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k3.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k4.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k5.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k6.ALL", "*");
    writeConf(confDir, conf);

    runServer(null, null, confDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Date started = new Date();
        Configuration conf = new Configuration();
        URI uri = createKMSUri(getKMSUrl());
        KeyProvider kp = createProvider(uri, conf);

        // getKeys() empty
        Assert.assertTrue(kp.getKeys().isEmpty());

        // getKeysMetadata() empty
        Assert.assertEquals(0, kp.getKeysMetadata().length);

        // createKey()
        KeyProvider.Options options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        options.setDescription("l1");
        KeyProvider.KeyVersion kv0 = kp.createKey("k1", options);
        Assert.assertNotNull(kv0);
        Assert.assertNotNull(kv0.getVersionName());
        Assert.assertNotNull(kv0.getMaterial());

        // getKeyVersion()
        KeyProvider.KeyVersion kv1 = kp.getKeyVersion(kv0.getVersionName());
        Assert.assertEquals(kv0.getVersionName(), kv1.getVersionName());
        Assert.assertNotNull(kv1.getMaterial());

        // getCurrent()
        KeyProvider.KeyVersion cv1 = kp.getCurrentKey("k1");
        Assert.assertEquals(kv0.getVersionName(), cv1.getVersionName());
        Assert.assertNotNull(cv1.getMaterial());

        // getKeyMetadata() 1 version
        KeyProvider.Metadata m1 = kp.getMetadata("k1");
        Assert.assertEquals("AES/CTR/NoPadding", m1.getCipher());
        Assert.assertEquals("AES", m1.getAlgorithm());
        Assert.assertEquals(128, m1.getBitLength());
        Assert.assertEquals(1, m1.getVersions());
        Assert.assertNotNull(m1.getCreated());
        Assert.assertTrue(started.before(m1.getCreated()));

        // getKeyVersions() 1 version
        List<KeyProvider.KeyVersion> lkv1 = kp.getKeyVersions("k1");
        Assert.assertEquals(1, lkv1.size());
        Assert.assertEquals(kv0.getVersionName(), lkv1.get(0).getVersionName());
        Assert.assertNotNull(kv1.getMaterial());

        // rollNewVersion()
        KeyProvider.KeyVersion kv2 = kp.rollNewVersion("k1");
        Assert.assertNotSame(kv0.getVersionName(), kv2.getVersionName());
        Assert.assertNotNull(kv2.getMaterial());

        // getKeyVersion()
        kv2 = kp.getKeyVersion(kv2.getVersionName());
        boolean eq = true;
        for (int i = 0; i < kv1.getMaterial().length; i++) {
          eq = eq && kv1.getMaterial()[i] == kv2.getMaterial()[i];
        }
        Assert.assertFalse(eq);

        // getCurrent()
        KeyProvider.KeyVersion cv2 = kp.getCurrentKey("k1");
        Assert.assertEquals(kv2.getVersionName(), cv2.getVersionName());
        Assert.assertNotNull(cv2.getMaterial());
        eq = true;
        for (int i = 0; i < kv1.getMaterial().length; i++) {
          eq = eq && cv2.getMaterial()[i] == kv2.getMaterial()[i];
        }
        Assert.assertTrue(eq);

        // getKeyVersions() 2 versions
        List<KeyProvider.KeyVersion> lkv2 = kp.getKeyVersions("k1");
        Assert.assertEquals(2, lkv2.size());
        Assert.assertEquals(kv1.getVersionName(), lkv2.get(0).getVersionName());
        Assert.assertNotNull(lkv2.get(0).getMaterial());
        Assert.assertEquals(kv2.getVersionName(), lkv2.get(1).getVersionName());
        Assert.assertNotNull(lkv2.get(1).getMaterial());

        // getKeyMetadata() 2 version
        KeyProvider.Metadata m2 = kp.getMetadata("k1");
        Assert.assertEquals("AES/CTR/NoPadding", m2.getCipher());
        Assert.assertEquals("AES", m2.getAlgorithm());
        Assert.assertEquals(128, m2.getBitLength());
        Assert.assertEquals(2, m2.getVersions());
        Assert.assertNotNull(m2.getCreated());
        Assert.assertTrue(started.before(m2.getCreated()));

        // getKeys() 1 key
        List<String> ks1 = kp.getKeys();
        Assert.assertEquals(1, ks1.size());
        Assert.assertEquals("k1", ks1.get(0));

        // getKeysMetadata() 1 key 2 versions
        KeyProvider.Metadata[] kms1 = kp.getKeysMetadata("k1");
        Assert.assertEquals(1, kms1.length);
        Assert.assertEquals("AES/CTR/NoPadding", kms1[0].getCipher());
        Assert.assertEquals("AES", kms1[0].getAlgorithm());
        Assert.assertEquals(128, kms1[0].getBitLength());
        Assert.assertEquals(2, kms1[0].getVersions());
        Assert.assertNotNull(kms1[0].getCreated());
        Assert.assertTrue(started.before(kms1[0].getCreated()));

        // test generate and decryption of EEK
        KeyProvider.KeyVersion kv = kp.getCurrentKey("k1");
        KeyProviderCryptoExtension kpExt =
            KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);

        EncryptedKeyVersion ek1 = kpExt.generateEncryptedKey(kv.getName());
        Assert.assertEquals(KeyProviderCryptoExtension.EEK,
            ek1.getEncryptedKeyVersion().getVersionName());
        Assert.assertNotNull(ek1.getEncryptedKeyVersion().getMaterial());
        Assert.assertEquals(kv.getMaterial().length,
            ek1.getEncryptedKeyVersion().getMaterial().length);
        KeyProvider.KeyVersion k1 = kpExt.decryptEncryptedKey(ek1);
        Assert.assertEquals(KeyProviderCryptoExtension.EK, k1.getVersionName());
        KeyProvider.KeyVersion k1a = kpExt.decryptEncryptedKey(ek1);
        Assert.assertArrayEquals(k1.getMaterial(), k1a.getMaterial());
        Assert.assertEquals(kv.getMaterial().length, k1.getMaterial().length);

        EncryptedKeyVersion ek2 = kpExt.generateEncryptedKey(kv.getName());
        KeyProvider.KeyVersion k2 = kpExt.decryptEncryptedKey(ek2);
        boolean isEq = true;
        for (int i = 0; isEq && i < ek2.getEncryptedKeyVersion()
            .getMaterial().length; i++) {
          isEq = k2.getMaterial()[i] == k1.getMaterial()[i];
        }
        Assert.assertFalse(isEq);

        // test re-encrypt
        kpExt.rollNewVersion(ek1.getEncryptionKeyName());
        EncryptedKeyVersion ek1r = kpExt.reencryptEncryptedKey(ek1);
        assertEquals(KeyProviderCryptoExtension.EEK,
            ek1r.getEncryptedKeyVersion().getVersionName());
        assertFalse(Arrays.equals(ek1.getEncryptedKeyVersion().getMaterial(),
            ek1r.getEncryptedKeyVersion().getMaterial()));
        assertEquals(kv.getMaterial().length,
            ek1r.getEncryptedKeyVersion().getMaterial().length);
        assertEquals(ek1.getEncryptionKeyName(), ek1r.getEncryptionKeyName());
        assertArrayEquals(ek1.getEncryptedKeyIv(), ek1r.getEncryptedKeyIv());
        assertNotEquals(ek1.getEncryptionKeyVersionName(),
            ek1r.getEncryptionKeyVersionName());

        KeyProvider.KeyVersion k1r = kpExt.decryptEncryptedKey(ek1r);
        assertEquals(KeyProviderCryptoExtension.EK, k1r.getVersionName());
        assertArrayEquals(k1.getMaterial(), k1r.getMaterial());
        assertEquals(kv.getMaterial().length, k1r.getMaterial().length);

        // test re-encrypt batch
        EncryptedKeyVersion ek3 = kpExt.generateEncryptedKey(kv.getName());
        KeyVersion latest = kpExt.rollNewVersion(kv.getName());
        List<EncryptedKeyVersion> ekvs = new ArrayList<>(3);
        ekvs.add(ek1);
        ekvs.add(ek2);
        ekvs.add(ek3);
        ekvs.add(ek1);
        ekvs.add(ek2);
        ekvs.add(ek3);
        kpExt.reencryptEncryptedKeys(ekvs);
        for (EncryptedKeyVersion ekv: ekvs) {
          assertEquals(latest.getVersionName(),
              ekv.getEncryptionKeyVersionName());
        }

        // deleteKey()
        kp.deleteKey("k1");

        // Check decryption after Key deletion
        try {
          kpExt.decryptEncryptedKey(ek1);
          Assert.fail("Should not be allowed !!");
        } catch (Exception e) {
          Assert.assertTrue(e.getMessage().contains("'k1@1' not found"));
        }

        // getKey()
        Assert.assertNull(kp.getKeyVersion("k1"));

        // getKeyVersions()
        Assert.assertNull(kp.getKeyVersions("k1"));

        // getMetadata()
        Assert.assertNull(kp.getMetadata("k1"));

        // getKeys() empty
        Assert.assertTrue(kp.getKeys().isEmpty());

        // getKeysMetadata() empty
        Assert.assertEquals(0, kp.getKeysMetadata().length);

        // createKey() no description, no tags
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        KeyVersion kVer2 = kp.createKey("k2", options);
        KeyProvider.Metadata meta = kp.getMetadata("k2");
        Assert.assertNull(meta.getDescription());
        Assert.assertEquals("k2", meta.getAttributes().get("key.acl.name"));

        // test key ACL.. k2 is granted only MANAGEMENT Op access
        try {
          kpExt =
              KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
          kpExt.generateEncryptedKey(kVer2.getName());
          Assert.fail("User should not be allowed to encrypt !!");
        } catch (Exception ex) {
          // 
        }

        // createKey() description, no tags
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        options.setDescription("d");
        kp.createKey("k3", options);
        meta = kp.getMetadata("k3");
        Assert.assertEquals("d", meta.getDescription());
        Assert.assertEquals("k3", meta.getAttributes().get("key.acl.name"));

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("a", "A");

        // createKey() no description, tags
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        attributes.put("key.acl.name", "k4");
        options.setAttributes(attributes);
        kp.createKey("k4", options);
        meta = kp.getMetadata("k4");
        Assert.assertNull(meta.getDescription());
        Assert.assertEquals(attributes, meta.getAttributes());

        // createKey() description, tags
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        options.setDescription("d");
        attributes.put("key.acl.name", "k5");
        options.setAttributes(attributes);
        kp.createKey("k5", options);
        meta = kp.getMetadata("k5");
        Assert.assertEquals("d", meta.getDescription());
        Assert.assertEquals(attributes, meta.getAttributes());

        // test rollover draining
        KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.
            createKeyProviderCryptoExtension(kp);
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        kpce.createKey("k6", options);

        EncryptedKeyVersion ekv1 = kpce.generateEncryptedKey("k6");
        kpce.rollNewVersion("k6");
        kpce.invalidateCache("k6");
        EncryptedKeyVersion ekv2 = kpce.generateEncryptedKey("k6");
        assertNotEquals("rollover did not generate a new key even after"
            + " queue is drained", ekv1.getEncryptionKeyVersionName(),
            ekv2.getEncryptionKeyVersionName());
        return null;
      }
    });
  }

  @Test
  public void testKMSProviderCaching() throws Exception {
    Configuration conf = new Configuration();
    File confDir = getTestDir();
    conf = createBaseKMSConf(confDir, conf);
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k1.ALL", "*");
    writeConf(confDir, conf);

    runServer(null, null, confDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final String keyName = "k1";
        final String mockVersionName = "mock";
        final Configuration conf = new Configuration();
        final URI uri = createKMSUri(getKMSUrl());
        KMSClientProvider kmscp = createKMSClientProvider(uri, conf);

        // get the reference to the internal cache, to test invalidation.
        ValueQueue vq =
            (ValueQueue) Whitebox.getInternalState(kmscp, "encKeyVersionQueue");
        LoadingCache<String, LinkedBlockingQueue<EncryptedKeyVersion>> kq =
            ((LoadingCache<String, LinkedBlockingQueue<EncryptedKeyVersion>>)
                Whitebox.getInternalState(vq, "keyQueues"));
        EncryptedKeyVersion mockEKV = Mockito.mock(EncryptedKeyVersion.class);
        when(mockEKV.getEncryptionKeyName()).thenReturn(keyName);
        when(mockEKV.getEncryptionKeyVersionName()).thenReturn(mockVersionName);

        // createKey()
        KeyProvider.Options options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        options.setDescription("l1");
        KeyProvider.KeyVersion kv0 = kmscp.createKey(keyName, options);
        assertNotNull(kv0.getVersionName());

        assertEquals("Default key version name is incorrect.", "k1@0",
            kmscp.generateEncryptedKey(keyName).getEncryptionKeyVersionName());

        kmscp.invalidateCache(keyName);
        kq.get(keyName).put(mockEKV);
        assertEquals("Key version incorrect after invalidating cache + putting"
                + " mock key.", mockVersionName,
            kmscp.generateEncryptedKey(keyName).getEncryptionKeyVersionName());

        // test new version is returned after invalidation.
        for (int i = 0; i < 100; ++i) {
          kq.get(keyName).put(mockEKV);
          kmscp.invalidateCache(keyName);
          assertEquals("Cache invalidation guarantee failed.", "k1@0",
              kmscp.generateEncryptedKey(keyName)
                  .getEncryptionKeyVersionName());
        }
        return null;
      }
    });
  }

  @Test
  @SuppressWarnings("checkstyle:methodlength")
  public void testKeyACLs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), type.toString());
    }
    conf.set(KMSACLs.Type.CREATE.getAclConfigKey(),"CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK,DECRYPT_EEK");
    conf.set(KMSACLs.Type.ROLLOVER.getAclConfigKey(),"CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK,DECRYPT_EEK");
    conf.set(KMSACLs.Type.GENERATE_EEK.getAclConfigKey(),"CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK,DECRYPT_EEK");
    conf.set(KMSACLs.Type.DECRYPT_EEK.getAclConfigKey(),"CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK");

    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "test_key.MANAGEMENT", "CREATE");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "some_key.MANAGEMENT", "ROLLOVER");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "DECRYPT_EEK");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "ALL", "DECRYPT_EEK");

    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "all_access.ALL", "GENERATE_EEK");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "all_access.DECRYPT_EEK", "ROLLOVER");
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "ROLLOVER");
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK", "SOMEBODY");
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "ALL", "ROLLOVER");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {

      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        doAs("CREATE", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              Options options = new KeyProvider.Options(conf);
              Map<String, String> attributes = options.getAttributes();
              HashMap<String,String> newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "test_key");
              options.setAttributes(newAttribs);
              KeyProvider.KeyVersion kv = kp.createKey("k0", options);
              Assert.assertNull(kv.getMaterial());
              KeyVersion rollVersion = kp.rollNewVersion("k0");
              Assert.assertNull(rollVersion.getMaterial());
              KeyProviderCryptoExtension kpce =
                  KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
              try {
                kpce.generateEncryptedKey("k0");
                Assert.fail("User [CREATE] should not be allowed to generate_eek on k0");
              } catch (Exception e) {
                // Ignore
              }
              newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "all_access");
              options.setAttributes(newAttribs);
              try {
                kp.createKey("kx", options);
                Assert.fail("User [CREATE] should not be allowed to create kx");
              } catch (Exception e) {
                // Ignore
              }
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        // Test whitelist key access..
        // DECRYPT_EEK is whitelisted for MANAGEMENT operations only
        doAs("DECRYPT_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              Options options = new KeyProvider.Options(conf);
              Map<String, String> attributes = options.getAttributes();
              HashMap<String,String> newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "some_key");
              options.setAttributes(newAttribs);
              KeyProvider.KeyVersion kv = kp.createKey("kk0", options);
              Assert.assertNull(kv.getMaterial());
              KeyVersion rollVersion = kp.rollNewVersion("kk0");
              Assert.assertNull(rollVersion.getMaterial());
              KeyProviderCryptoExtension kpce =
                  KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
              try {
                kpce.generateEncryptedKey("kk0");
                Assert.fail("User [DECRYPT_EEK] should not be allowed to generate_eek on kk0");
              } catch (Exception e) {
                // Ignore
              }
              newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "all_access");
              options.setAttributes(newAttribs);
              kp.createKey("kkx", options);
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("ROLLOVER", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              Options options = new KeyProvider.Options(conf);
              Map<String, String> attributes = options.getAttributes();
              HashMap<String,String> newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "test_key2");
              options.setAttributes(newAttribs);
              KeyProvider.KeyVersion kv = kp.createKey("k1", options);
              Assert.assertNull(kv.getMaterial());
              KeyVersion rollVersion = kp.rollNewVersion("k1");
              Assert.assertNull(rollVersion.getMaterial());
              try {
                kp.rollNewVersion("k0");
                Assert.fail("User [ROLLOVER] should not be allowed to rollover k0");
              } catch (Exception e) {
                // Ignore
              }
              KeyProviderCryptoExtension kpce =
                  KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
              try {
                kpce.generateEncryptedKey("k1");
                Assert.fail("User [ROLLOVER] should not be allowed to generate_eek on k1");
              } catch (Exception e) {
                // Ignore
              }
              newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "all_access");
              options.setAttributes(newAttribs);
              try {
                kp.createKey("kx", options);
                Assert.fail("User [ROLLOVER] should not be allowed to create kx");
              } catch (Exception e) {
                // Ignore
              }
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("GET", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              Options options = new KeyProvider.Options(conf);
              Map<String, String> attributes = options.getAttributes();
              HashMap<String,String> newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "test_key");
              options.setAttributes(newAttribs);
              try {
                kp.createKey("k2", options);
                Assert.fail("User [GET] should not be allowed to create key..");
              } catch (Exception e) {
                // Ignore
              }
              newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "all_access");
              options.setAttributes(newAttribs);
              try {
                kp.createKey("kx", options);
                Assert.fail("User [GET] should not be allowed to create kx");
              } catch (Exception e) {
                // Ignore
              }
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        final EncryptedKeyVersion ekv = doAs("GENERATE_EEK", new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              Options options = new KeyProvider.Options(conf);
              Map<String, String> attributes = options.getAttributes();
              HashMap<String,String> newAttribs = new HashMap<String, String>(attributes);
              newAttribs.put("key.acl.name", "all_access");
              options.setAttributes(newAttribs);
              kp.createKey("kx", options);
              KeyProviderCryptoExtension kpce =
                  KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
              try {
                return kpce.generateEncryptedKey("kx");
              } catch (Exception e) {
                Assert.fail("User [GENERATE_EEK] should be allowed to generate_eek on kx");
              }
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("ROLLOVER", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProviderCryptoExtension kpce =
                  KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
              kpce.decryptEncryptedKey(ekv);
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });
        return null;
      }
    });

    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK", "*");
    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {

      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        doAs("GENERATE_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            KeyProviderCryptoExtension kpce =
                KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
            EncryptedKeyVersion ekv = kpce.generateEncryptedKey("k1");
            kpce.reencryptEncryptedKey(ekv);
            List<EncryptedKeyVersion> ekvs = new ArrayList<>(2);
            ekvs.add(ekv);
            ekvs.add(ekv);
            kpce.reencryptEncryptedKeys(ekvs);
            return null;
          }
        });
        return null;
      }
    });
  }

  @Test
  public void testKMSRestartKerberosAuth() throws Exception {
    doKMSRestart(true);
  }

  @Test
  public void testKMSRestartSimpleAuth() throws Exception {
    doKMSRestart(false);
  }

  public void doKMSRestart(boolean useKrb) throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    if (useKrb) {
      conf.set("hadoop.kms.authentication.type", "kerberos");
    }
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), type.toString());
    }
    conf.set(KMSACLs.Type.CREATE.getAclConfigKey(),
        KMSACLs.Type.CREATE.toString() + ",SET_KEY_MATERIAL");

    conf.set(KMSACLs.Type.ROLLOVER.getAclConfigKey(),
        KMSACLs.Type.ROLLOVER.toString() + ",SET_KEY_MATERIAL");

    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k0.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k1.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k2.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k3.ALL", "*");

    writeConf(testDir, conf);

    KMSCallable<KeyProvider> c =
        new KMSCallable<KeyProvider>() {
      @Override
      public KeyProvider call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        final KeyProvider kp =
            doAs("SET_KEY_MATERIAL",
                new PrivilegedExceptionAction<KeyProvider>() {
                  @Override
                  public KeyProvider run() throws Exception {
                    KeyProvider kp = createProvider(uri, conf);
                        kp.createKey("k1", new byte[16],
                            new KeyProvider.Options(conf));
                    return kp;
                  }
                });
        return kp;
      }
    };

    final KeyProvider retKp =
        runServer(null, null, testDir, c);

    // Restart server (using the same port)
    runServer(c.getKMSUrl().getPort(), null, null, testDir,
        new KMSCallable<Void>() {
          @Override
          public Void call() throws Exception {
            final Configuration conf = new Configuration();
            conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
            doAs("SET_KEY_MATERIAL",
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                    retKp.createKey("k2", new byte[16],
                        new KeyProvider.Options(conf));
                    retKp.createKey("k3", new byte[16],
                        new KeyProvider.Options(conf));
                    return null;
                  }
                });
            return null;
          }
        });
  }

  @Test
  public void testKMSAuthFailureRetry() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    conf.set("hadoop.kms.authentication.token.validity", "1");

    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), type.toString());
    }
    conf.set(KMSACLs.Type.CREATE.getAclConfigKey(),
        KMSACLs.Type.CREATE.toString() + ",SET_KEY_MATERIAL");

    conf.set(KMSACLs.Type.ROLLOVER.getAclConfigKey(),
        KMSACLs.Type.ROLLOVER.toString() + ",SET_KEY_MATERIAL");

    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k0.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k1.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k2.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k3.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k4.ALL", "*");

    writeConf(testDir, conf);

    runServer(null, null, testDir,
        new KMSCallable<Void>() {
          @Override
          public Void call() throws Exception {
            final Configuration conf = new Configuration();
            conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
            final URI uri = createKMSUri(getKMSUrl());
            doAs("SET_KEY_MATERIAL",
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                    KeyProvider kp = createProvider(uri, conf);

                    kp.createKey("k0", new byte[16],
                        new KeyProvider.Options(conf));
                    // This happens before rollover
                    kp.createKey("k1", new byte[16],
                        new KeyProvider.Options(conf));
                    // Atleast 2 rollovers.. so should induce signer Exception
                    Thread.sleep(3500);
                    kp.createKey("k2", new byte[16],
                      new KeyProvider.Options(conf));
                    return null;
                  }
                });
            return null;
          }
        });

    // Test retry count
    runServer(null, null, testDir,
        new KMSCallable<Void>() {
          @Override
          public Void call() throws Exception {
            final Configuration conf = new Configuration();
            conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
            conf.setInt(KMSClientProvider.AUTH_RETRY, 0);
            final URI uri = createKMSUri(getKMSUrl());
            doAs("SET_KEY_MATERIAL",
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                    KeyProvider kp = createProvider(uri, conf);
                    kp.createKey("k3", new byte[16],
                        new KeyProvider.Options(conf));
                    // Atleast 2 rollovers.. so should induce signer Exception
                    Thread.sleep(3500);
                    try {
                      kp.createKey("k4", new byte[16],
                          new KeyProvider.Options(conf));
                      Assert.fail("This should not succeed..");
                    } catch (IOException e) {
                      Assert.assertTrue(
                          "HTTP exception must be a 401 : " + e.getMessage(), e
                              .getMessage().contains("401"));
                    }
                    return null;
                  }
                });
            return null;
          }
        });
  }

  @Test
  @SuppressWarnings("checkstyle:methodlength")
  public void testACLs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), type.toString());
    }
    conf.set(KMSACLs.Type.CREATE.getAclConfigKey(),
        KMSACLs.Type.CREATE.toString() + ",SET_KEY_MATERIAL");

    conf.set(KMSACLs.Type.ROLLOVER.getAclConfigKey(),
        KMSACLs.Type.ROLLOVER.toString() + ",SET_KEY_MATERIAL");

    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k0.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k1.ALL", "*");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        //nothing allowed
        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              kp.createKey("k", new KeyProvider.Options(conf));
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.createKey("k", new byte[16], new KeyProvider.Options(conf));
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.rollNewVersion("k");
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.rollNewVersion("k", new byte[16]);
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.getKeys();
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.getKeysMetadata("k");
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              // we are using JavaKeyStoreProvider for testing, so we know how
              // the keyversion is created.
              kp.getKeyVersion("k@0");
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.getCurrentKey("k");
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.getMetadata("k");
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            try {
              kp.getKeyVersions("k");
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }

            return null;
          }
        });

        doAs("CREATE", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProvider.KeyVersion kv = kp.createKey("k0",
                  new KeyProvider.Options(conf));
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("DELETE", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              kp.deleteKey("k0");
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("SET_KEY_MATERIAL", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProvider.KeyVersion kv = kp.createKey("k1", new byte[16],
                  new KeyProvider.Options(conf));
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("ROLLOVER", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProvider.KeyVersion kv = kp.rollNewVersion("k1");
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("SET_KEY_MATERIAL", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProvider.KeyVersion kv =
                  kp.rollNewVersion("k1", new byte[16]);
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        final KeyVersion currKv =
            doAs("GET", new PrivilegedExceptionAction<KeyVersion>() {
          @Override
          public KeyVersion run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              kp.getKeyVersion("k1@0");
              KeyVersion kv = kp.getCurrentKey("k1");
              return kv;
            } catch (Exception ex) {
              Assert.fail(ex.toString());
            }
            return null;
          }
        });

        final EncryptedKeyVersion encKv =
            doAs("GENERATE_EEK",
                new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.
                      createKeyProviderCryptoExtension(kp);
              EncryptedKeyVersion ek1 =
                  kpCE.generateEncryptedKey(currKv.getName());
              return ek1;
            } catch (Exception ex) {
              Assert.fail(ex.toString());
            }
            return null;
          }
        });

        doAs("GENERATE_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.
                createKeyProviderCryptoExtension(kp);
            kpCE.reencryptEncryptedKey(encKv);
            List<EncryptedKeyVersion> ekvs = new ArrayList<>(2);
            ekvs.add(encKv);
            ekvs.add(encKv);
            kpCE.reencryptEncryptedKeys(ekvs);
            return null;
          }
        });

        doAs("DECRYPT_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.
                      createKeyProviderCryptoExtension(kp);
              kpCE.decryptEncryptedKey(encKv);
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("GET_KEYS", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              kp.getKeys();
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("GET_METADATA", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              kp.getMetadata("k1");
              kp.getKeysMetadata("k1");
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        //stop the reloader, to avoid running while we are writing the new file
        KMSWebApp.getACLs().stopReloader();

        GenericTestUtils.setLogLevel(KMSConfiguration.LOG, Level.TRACE);
        // test ACL reloading
        conf.set(KMSACLs.Type.CREATE.getAclConfigKey(), "foo");
        conf.set(KMSACLs.Type.GENERATE_EEK.getAclConfigKey(), "foo");
        writeConf(testDir, conf);
        KMSWebApp.getACLs().forceNextReloadForTesting();
        KMSWebApp.getACLs().run(); // forcing a reload by hand.

        // should not be able to create a key now
        doAs("CREATE", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = createProvider(uri, conf);
              KeyProvider.KeyVersion kv = kp.createKey("k2",
                  new KeyProvider.Options(conf));
              Assert.fail();
            } catch (AuthorizationException ex) {
              //NOP
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }

            return null;
          }
        });

        doAs("GENERATE_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.
                  createKeyProviderCryptoExtension(kp);
              kpCE.generateEncryptedKey("k1");
            } catch (IOException ex) {
              // This isn't an AuthorizationException because generate goes
              // through the ValueQueue. See KMSCP#generateEncryptedKey.
              if (ex.getCause().getCause() instanceof AuthorizationException) {
                LOG.info("Caught expected exception.", ex);
              } else {
                throw ex;
              }
            }
            return null;
          }
        });

        doAs("GENERATE_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.
                  createKeyProviderCryptoExtension(kp);
              kpCE.reencryptEncryptedKey(encKv);
              fail("Should not have been able to reencryptEncryptedKey");
            } catch (AuthorizationException ex) {
              LOG.info("reencryptEncryptedKey caught expected exception.", ex);
            }
            return null;
          }
        });
        doAs("GENERATE_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            try {
              KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.
                  createKeyProviderCryptoExtension(kp);
              List<EncryptedKeyVersion> ekvs = new ArrayList<>(2);
              ekvs.add(encKv);
              ekvs.add(encKv);
              kpCE.reencryptEncryptedKeys(ekvs);
              fail("Should not have been able to reencryptEncryptedKeys");
            } catch (AuthorizationException ex) {
              LOG.info("reencryptEncryptedKeys caught expected exception.", ex);
            }
            return null;
          }
        });
        return null;
      }
    });
  }

  @Test
  public void testKMSBlackList() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), " ");
    }
    conf.set(KMSACLs.Type.CREATE.getAclConfigKey(), "client,hdfs,otheradmin");
    conf.set(KMSACLs.Type.GENERATE_EEK.getAclConfigKey(), "client,hdfs,otheradmin");
    conf.set(KMSACLs.Type.DECRYPT_EEK.getAclConfigKey(), "client,hdfs,otheradmin");
    conf.set(KMSACLs.Type.DECRYPT_EEK.getBlacklistConfigKey(), "hdfs,otheradmin");

    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "ck0.ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "ck1.ALL", "*");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = createProvider(uri, conf);
              KeyProvider.KeyVersion kv = kp.createKey("ck0",
                  new KeyProvider.Options(conf));
              EncryptedKeyVersion eek =
                  ((CryptoExtension)kp).generateEncryptedKey("ck0");
              ((CryptoExtension)kp).decryptEncryptedKey(eek);
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("hdfs", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = createProvider(uri, conf);
              KeyProvider.KeyVersion kv = kp.createKey("ck1",
                  new KeyProvider.Options(conf));
              EncryptedKeyVersion eek =
                  ((CryptoExtension)kp).generateEncryptedKey("ck1");
              ((CryptoExtension)kp).decryptEncryptedKey(eek);
              Assert.fail("admin user must not be allowed to decrypt !!");
            } catch (Exception ex) {
            }
            return null;
          }
        });

        doAs("otheradmin", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = createProvider(uri, conf);
              KeyProvider.KeyVersion kv = kp.createKey("ck2",
                  new KeyProvider.Options(conf));
              EncryptedKeyVersion eek =
                  ((CryptoExtension)kp).generateEncryptedKey("ck2");
              ((CryptoExtension)kp).decryptEncryptedKey(eek);
              Assert.fail("admin user must not be allowed to decrypt !!");
            } catch (Exception ex) {
            }
            return null;
          }
        });

        return null;
      }
    });
  }

  @Test
  public void testServicePrincipalACLs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), " ");
    }
    conf.set(KMSACLs.Type.CREATE.getAclConfigKey(), "client");
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "client,client/host");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = createProvider(uri, conf);
              KeyProvider.KeyVersion kv = kp.createKey("ck0",
                  new KeyProvider.Options(conf));
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });

        doAs("client/host", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = createProvider(uri, conf);
              KeyProvider.KeyVersion kv = kp.createKey("ck1",
                  new KeyProvider.Options(conf));
              Assert.assertNull(kv.getMaterial());
            } catch (Exception ex) {
              Assert.fail(ex.getMessage());
            }
            return null;
          }
        });
        return null;
      }
    });
  }

  /**
   * Test the configurable timeout in the KMSClientProvider.  Open up a
   * socket, but don't accept connections for it.  This leads to a timeout
   * when the KMS client attempts to connect.
   * @throws Exception
   */
  @Test
  public void testKMSTimeout() throws Exception {
    File confDir = getTestDir();
    Configuration conf = createBaseKMSConf(confDir);
    conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_TIMEOUT_SECONDS, 1);
    writeConf(confDir, conf);

    ServerSocket sock;
    int port;
    try {
      sock = new ServerSocket(0, 50, InetAddress.getByName("localhost"));
      port = sock.getLocalPort();
    } catch ( Exception e ) {
      /* Problem creating socket?  Just bail. */
      return;
    }

    URL url = new URL("http://localhost:" + port + "/kms");
    URI uri = createKMSUri(url);

    boolean caughtTimeout = false;
    try {
      KeyProvider kp = createProvider(uri, conf);
      kp.getKeys();
    } catch (SocketTimeoutException e) {
      caughtTimeout = true;
    } catch (IOException e) {
      Assert.assertTrue("Caught unexpected exception" + e.toString(), false);
    }

    caughtTimeout = false;
    try {
      KeyProvider kp = createProvider(uri, conf);
      KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp)
          .generateEncryptedKey("a");
    } catch (SocketTimeoutException e) {
      caughtTimeout = true;
    } catch (IOException e) {
      Assert.assertTrue("Caught unexpected exception" + e.toString(), false);
    }

    caughtTimeout = false;
    try {
      KeyProvider kp = createProvider(uri, conf);
      KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp)
          .decryptEncryptedKey(
              new KMSClientProvider.KMSEncryptedKeyVersion("a",
                  "a", new byte[] {1, 2}, "EEK", new byte[] {1, 2}));
    } catch (SocketTimeoutException e) {
      caughtTimeout = true;
    } catch (IOException e) {
      Assert.assertTrue("Caught unexpected exception" + e.toString(), false);
    }

    Assert.assertTrue(caughtTimeout);

    sock.close();
  }

  @Test
  public void testDelegationTokenAccess() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

    final String keyA = "key_a";
    final String keyD = "key_d";
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + keyA + ".ALL", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + keyD + ".ALL", "*");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());
        final Credentials credentials = new Credentials();
        final UserGroupInformation nonKerberosUgi =
            UserGroupInformation.getCurrentUser();

        try {
          KeyProvider kp = createProvider(uri, conf);
          kp.createKey(keyA, new KeyProvider.Options(conf));
        } catch (IOException ex) {
          System.out.println(ex.getMessage());
        }

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            KeyProviderDelegationTokenExtension kpdte =
                KeyProviderDelegationTokenExtension.
                    createKeyProviderDelegationTokenExtension(kp);
            kpdte.addDelegationTokens("foo", credentials);
            return null;
          }
        });
        nonKerberosUgi.addCredentials(credentials);

        try {
          KeyProvider kp = createProvider(uri, conf);
          kp.createKey(keyA, new KeyProvider.Options(conf));
        } catch (IOException ex) {
          System.out.println(ex.getMessage());
        }

        nonKerberosUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, conf);
            kp.createKey(keyD, new KeyProvider.Options(conf));
            return null;
          }
        });

        return null;
      }
    });
  }

  private Configuration setupConfForKerberos(File confDir) throws Exception {
    final Configuration conf =  createBaseKMSConf(confDir, null);
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal",
        "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    return conf;
  }

  @Test
  public void testDelegationTokensOpsHttpPseudo() throws Exception {
    testDelegationTokensOps(false, false);
  }

  @Test
  public void testDelegationTokensOpsHttpKerberized() throws Exception {
    testDelegationTokensOps(false, true);
  }

  @Test
  public void testDelegationTokensOpsHttpsPseudo() throws Exception {
    testDelegationTokensOps(true, false);
  }

  @Test
  public void testDelegationTokensOpsHttpsKerberized() throws Exception {
    testDelegationTokensOps(true, true);
  }

  private Text getTokenService(KeyProvider provider) {
    assertTrue("KeyProvider should be an instance of KMSClientProvider",
        (provider instanceof LoadBalancingKMSClientProvider));
    assertEquals("Num client providers should be 1", 1,
        ((LoadBalancingKMSClientProvider)provider).getProviders().length);
    Text tokenService =
        (((LoadBalancingKMSClientProvider)provider).getProviders()[0])
        .getDelegationTokenService();
    return tokenService;
  }

  private void testDelegationTokensOps(final boolean ssl, final boolean kerb)
      throws Exception {
    final File confDir = getTestDir();
    final Configuration conf;
    if (kerb) {
      conf = setupConfForKerberos(confDir);
    } else {
      conf = createBaseKMSConf(confDir, null);
    }

    final String keystore;
    final String password;
    if (ssl) {
      final String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestKMS.class);
      KeyStoreTestUtil.setupSSLConfig(confDir.getAbsolutePath(), sslConfDir,
          conf, false);
      keystore = confDir.getAbsolutePath() + "/serverKS.jks";
      password = "serverP";
    } else {
      keystore = null;
      password = null;
    }
    writeConf(confDir, conf);

    runServer(keystore, password, confDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration clientConf = new Configuration();
        final URI uri = createKMSUri(getKMSUrl());
        clientConf.set(KeyProviderFactory.KEY_PROVIDER_PATH,
            createKMSUri(getKMSUrl()).toString());
        clientConf.setBoolean(KMS_CLIENT_COPY_LEGACY_TOKEN_KEY, false);

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, clientConf);
            // Unset the conf value for key provider path just to be sure that
            // the key provider created for renew and cancel token is from
            // token service field.
            clientConf.unset(HADOOP_SECURITY_KEY_PROVIDER_PATH);
            // test delegation token retrieval
            KeyProviderDelegationTokenExtension kpdte =
                KeyProviderDelegationTokenExtension.
                    createKeyProviderDelegationTokenExtension(kp);
            final Credentials credentials = new Credentials();
            final Token<?>[] tokens =
                kpdte.addDelegationTokens("client1", credentials);
            Text tokenService = getTokenService(kp);
            assertEquals(1, credentials.getAllTokens().size());
            assertEquals(TOKEN_KIND,
                credentials.getToken(tokenService).getKind());

            // Test non-renewer user cannot renew.
            for (Token<?> token : tokens) {
              if (!(token.getKind().equals(KMSDelegationToken.TOKEN_KIND))) {
                LOG.info("Skipping token {}", token);
                continue;
              }
              LOG.info("Got dt for " + uri + "; " + token);
              try {
                token.renew(clientConf);
                Assert.fail("client should not be allowed to renew token with"
                    + "renewer=client1");
              } catch (Exception e) {
                final DelegationTokenIdentifier identifier =
                    (DelegationTokenIdentifier) token.decodeIdentifier();
                GenericTestUtils.assertExceptionContains(
                    "tries to renew a token (" + identifier
                        + ") with non-matching renewer", e);
              }
            }

            final UserGroupInformation otherUgi;
            if (kerb) {
              UserGroupInformation
                  .loginUserFromKeytab("client1", keytab.getAbsolutePath());
              otherUgi = UserGroupInformation.getLoginUser();
            } else {
              otherUgi = UserGroupInformation.createUserForTesting("client1",
                  new String[] {"other group"});
              UserGroupInformation.setLoginUser(otherUgi);
            }
            try {
              // test delegation token renewal via renewer
              otherUgi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                  boolean renewed = false;
                  for (Token<?> token : tokens) {
                    if (!(token.getKind()
                        .equals(KMSDelegationToken.TOKEN_KIND))) {
                      LOG.info("Skipping token {}", token);
                      continue;
                    }
                    LOG.info("Got dt for " + uri + "; " + token);
                    long tokenLife = token.renew(clientConf);
                    LOG.info("Renewed token of kind {}, new lifetime:{}",
                        token.getKind(), tokenLife);
                    Thread.sleep(100);
                    long newTokenLife = token.renew(clientConf);
                    LOG.info("Renewed token of kind {}, new lifetime:{}",
                        token.getKind(), newTokenLife);
                    Assert.assertTrue(newTokenLife > tokenLife);
                    renewed = true;
                  }
                  Assert.assertTrue(renewed);

                  // test delegation token cancellation
                  for (Token<?> token : tokens) {
                    if (!(token.getKind()
                        .equals(KMSDelegationToken.TOKEN_KIND))) {
                      LOG.info("Skipping token {}", token);
                      continue;
                    }
                    LOG.info("Got dt for " + uri + "; " + token);
                    token.cancel(clientConf);
                    LOG.info("Cancelled token of kind {}", token.getKind());
                    try {
                      token.renew(clientConf);
                      Assert
                          .fail("should not be able to renew a canceled token");
                    } catch (Exception e) {
                      LOG.info("Expected exception when renewing token", e);
                    }
                  }
                  return null;
                }
              });
              // Close the client provider. We will verify all providers'
              // Truststore reloader threads are closed later.
              kp.close();
              return null;
            } finally {
              otherUgi.logoutUserFromKeytab();
            }
          }
        });
        return null;
      }
    });

    // verify that providers created by KMSTokenRenewer are closed.
    if (ssl) {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          final Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
          for (Thread t : threadSet) {
            if (t.getName().contains(SSL_RELOADER_THREAD_NAME)) {
              return false;
            }
          }
          return true;
        }
      }, 1000, 10000);
    }
  }

  @Test
  public void testDelegationTokensUpdatedInUGI() throws Exception {
    Configuration conf = new Configuration();
    File confDir = getTestDir();
    conf = createBaseKMSConf(confDir, conf);
    conf.set(
        "hadoop.kms.authentication.delegation-token.max-lifetime.sec", "5");
    conf.set(
        "hadoop.kms.authentication.delegation-token.renew-interval.sec", "5");
    writeConf(confDir, conf);

    // Running as a service (e.g. YARN in practice).
    runServer(null, null, confDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration clientConf = new Configuration();
        final URI uri = createKMSUri(getKMSUrl());
        clientConf.set(KeyProviderFactory.KEY_PROVIDER_PATH,
            createKMSUri(getKMSUrl()).toString());
        clientConf.setBoolean(KMS_CLIENT_COPY_LEGACY_TOKEN_KEY, false);
        final KeyProvider kp = createProvider(uri, clientConf);
        final KeyProviderDelegationTokenExtension kpdte =
            KeyProviderDelegationTokenExtension.
                createKeyProviderDelegationTokenExtension(kp);

        // Job 1 (e.g. YARN log aggregation job), with user DT.
        final Collection<Token<?>> job1Token = new HashSet<>();
        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            // Get a DT and use it.
            final Credentials credentials = new Credentials();
            kpdte.addDelegationTokens("client", credentials);
            Text tokenService = getTokenService(kp);
            Assert.assertEquals(1, credentials.getAllTokens().size());

            UserGroupInformation.getCurrentUser().addCredentials(credentials);
            LOG.info("Added kms dt to credentials: {}", UserGroupInformation.
                getCurrentUser().getCredentials().getAllTokens());
            final Token<?> token =
                UserGroupInformation.getCurrentUser().getCredentials()
                    .getToken(tokenService);
            assertNotNull(token);
            assertEquals(TOKEN_KIND, token.getKind());
            job1Token.add(token);

            // Decode the token to get max time.
            ByteArrayInputStream buf =
                new ByteArrayInputStream(token.getIdentifier());
            DataInputStream dis = new DataInputStream(buf);
            DelegationTokenIdentifier id =
                new DelegationTokenIdentifier(token.getKind());
            id.readFields(dis);
            dis.close();
            final long maxTime = id.getMaxDate();

            // wait for token to expire.
            Thread.sleep(5100);
            Assert.assertTrue("maxTime " + maxTime + " is not less than now.",
                maxTime > 0 && maxTime < Time.now());
            try {
              kp.getKeys();
              Assert.fail("Operation should fail since dt is expired.");
            } catch (Exception e) {
              LOG.info("Expected error.", e);
            }
            return null;
          }
        });
        Assert.assertFalse(job1Token.isEmpty());

        // job 2 (e.g. Another YARN log aggregation job, with user DT.
        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            // Get a new DT, but don't use it yet.
            final Credentials newCreds = new Credentials();
            kpdte.addDelegationTokens("client", newCreds);
            assertEquals(1, newCreds.getAllTokens().size());
            final Text tokenService = getTokenService(kp);
            assertEquals(TOKEN_KIND,
                newCreds.getToken(tokenService).getKind());

            // Using job 1's DT should fail.
            final Credentials oldCreds = new Credentials();
            for (Token<?> token : job1Token) {
              if (token.getKind().equals(TOKEN_KIND)) {
                oldCreds.addToken(tokenService, token);
              }
            }
            UserGroupInformation.getCurrentUser().addCredentials(oldCreds);
            LOG.info("Added old kms dt to credentials: {}", UserGroupInformation
                .getCurrentUser().getCredentials().getAllTokens());
            try {
              kp.getKeys();
              Assert.fail("Operation should fail since dt is expired.");
            } catch (Exception e) {
              LOG.info("Expected error.", e);
            }

            // Using the new DT should succeed.
            assertEquals(1, newCreds.getAllTokens().size());
            assertEquals(TOKEN_KIND,
                newCreds.getToken(tokenService).getKind());
            UserGroupInformation.getCurrentUser().addCredentials(newCreds);
            LOG.info("Credentials now are: {}", UserGroupInformation
                .getCurrentUser().getCredentials().getAllTokens());
            kp.getKeys();
            return null;
          }
        });
        return null;
      }
    });
  }

  @Test
  public void testKMSWithZKSigner() throws Exception {
    doKMSWithZK(true, false);
  }

  @Test
  public void testKMSWithZKDTSM() throws Exception {
    doKMSWithZK(false, true);
  }

  @Test
  public void testKMSWithZKSignerAndDTSM() throws Exception {
    doKMSWithZK(true, true);
  }

  private <T> T runServerWithZooKeeper(boolean zkDTSM, boolean zkSigner,
      KMSCallable<T> callable) throws Exception {
    return runServerWithZooKeeper(zkDTSM, zkSigner, callable, 1);
  }

  private <T> T runServerWithZooKeeper(boolean zkDTSM, boolean zkSigner,
      KMSCallable<T> callable, int kmsSize) throws Exception {
    TestingServer zkServer = null;
    try {
      zkServer = new TestingServer();
      zkServer.start();

      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "kerberos");
      final File testDir = getTestDir();
      conf = createBaseKMSConf(testDir, conf);
      conf.set("hadoop.kms.authentication.type", "kerberos");
      conf.set("hadoop.kms.authentication.kerberos.keytab", keytab.getAbsolutePath());
      conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
      conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

      if (zkSigner) {
        conf.set("hadoop.kms.authentication.signer.secret.provider", "zookeeper");
        conf.set("hadoop.kms.authentication.signer.secret.provider.zookeeper.path","/testKMSWithZKDTSM");
        conf.set("hadoop.kms.authentication.signer.secret.provider.zookeeper.connection.string",zkServer.getConnectString());
      }

      if (zkDTSM) {
        conf.set("hadoop.kms.authentication.zk-dt-secret-manager.enable", "true");
      }
      if (zkDTSM && !zkSigner) {
        conf.set("hadoop.kms.authentication.zk-dt-secret-manager.zkConnectionString", zkServer.getConnectString());
        conf.set("hadoop.kms.authentication.zk-dt-secret-manager.znodeWorkingPath", "testZKPath");
        conf.set("hadoop.kms.authentication.zk-dt-secret-manager.zkAuthType", "none");
      }

      for (KMSACLs.Type type : KMSACLs.Type.values()) {
        conf.set(type.getAclConfigKey(), type.toString());
      }
      conf.set(KMSACLs.Type.CREATE.getAclConfigKey(),
          KMSACLs.Type.CREATE.toString() + ",SET_KEY_MATERIAL");

      conf.set(KMSACLs.Type.ROLLOVER.getAclConfigKey(),
          KMSACLs.Type.ROLLOVER.toString() + ",SET_KEY_MATERIAL");

      conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k0.ALL", "*");
      conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k1.ALL", "*");
      conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k2.ALL", "*");
      conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "k3.ALL", "*");

      writeConf(testDir, conf);

      int[] ports = new int[kmsSize];
      for (int i = 0; i < ports.length; i++) {
        ports[i] = -1;
      }
      return runServer(ports, null, null, testDir, callable);
    } finally {
      if (zkServer != null) {
        zkServer.stop();
        zkServer.close();
      }
    }
  }

  public void doKMSWithZK(boolean zkDTSM, boolean zkSigner) throws Exception {
    KMSCallable<KeyProvider> c =
        new KMSCallable<KeyProvider>() {
          @Override
          public KeyProvider call() throws Exception {
            final Configuration conf = new Configuration();
            conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
            final URI uri = createKMSUri(getKMSUrl());

            final KeyProvider kp =
                doAs("SET_KEY_MATERIAL",
                    new PrivilegedExceptionAction<KeyProvider>() {
                      @Override
                      public KeyProvider run() throws Exception {
                        KeyProvider kp = createProvider(uri, conf);
                        kp.createKey("k1", new byte[16],
                            new KeyProvider.Options(conf));
                        kp.createKey("k2", new byte[16],
                            new KeyProvider.Options(conf));
                        kp.createKey("k3", new byte[16],
                            new KeyProvider.Options(conf));
                        return kp;
                      }
                    });
            return kp;
          }
        };

    runServerWithZooKeeper(zkDTSM, zkSigner, c);
  }

  @Test
  public void doKMSHAZKWithDelegationTokenAccess() throws Exception {
    KMSCallable<Void> c = new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI[] uris = createKMSHAUri(getKMSHAUrl());
        final Credentials credentials = new Credentials();
        final String lbUri = generateLoadBalancingKeyProviderUriString();
        final LoadBalancingKMSClientProvider lbkp =
            createHAProvider(uris, conf, lbUri);
        conf.unset(HADOOP_SECURITY_KEY_PROVIDER_PATH);
        // Login as a Kerberos user principal using keytab.
        // Connect to KMS to create a delegation token and add it to credentials
        final String keyName = "k0";
        doAs("SET_KEY_MATERIAL",
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                KeyProviderDelegationTokenExtension kpdte =
                    KeyProviderDelegationTokenExtension.
                        createKeyProviderDelegationTokenExtension(lbkp);
                kpdte.addDelegationTokens("SET_KEY_MATERIAL", credentials);
                kpdte.createKey(keyName, new KeyProvider.Options(conf));
                return null;
              }
            });

        assertTokenIdentifierEquals(credentials);

        final LoadBalancingKMSClientProvider lbkp1 =
            createHAProvider(uris, conf, lbUri);
        // verify both tokens can be used to authenticate
        for (Token t : credentials.getAllTokens()) {
          assertTokenAccess(lbkp1, keyName, t);
        }
        return null;
      }
    };
    runServerWithZooKeeper(true, true, c, 2);
  }

  /**
   * Assert that the passed in credentials have 2 tokens, of kind
   * {@link KMSDelegationToken#TOKEN_KIND} and
   * {@link KMSDelegationToken#TOKEN_LEGACY_KIND}. Assert that the 2 tokens have
   * the same identifier.
   */
  private void assertTokenIdentifierEquals(Credentials credentials)
      throws IOException {
    // verify the 2 tokens have the same identifier
    assertEquals(2, credentials.getAllTokens().size());
    Token token = null;
    Token legacyToken = null;
    for (Token t : credentials.getAllTokens()) {
      if (KMSDelegationToken.TOKEN_KIND.equals(t.getKind())) {
        token = t;
      } else if (KMSDelegationToken.TOKEN_LEGACY_KIND.equals(t.getKind())) {
        legacyToken = t;
      }
    }
    assertNotNull(token);
    assertNotNull(legacyToken);
    final DelegationTokenIdentifier tokenId =
        (DelegationTokenIdentifier) token.decodeIdentifier();
    final DelegationTokenIdentifier legacyTokenId =
        (DelegationTokenIdentifier) legacyToken.decodeIdentifier();
    assertEquals("KMS DT and legacy dt should have identical identifier",
        tokenId, legacyTokenId);
  }

  /**
   * Tests token access with each providers in the
   * {@link LoadBalancingKMSClientProvider}. This is to make sure the 2 token
   * kinds are compatible and can both be used to authenticate.
   */
  private void assertTokenAccess(final LoadBalancingKMSClientProvider lbkp,
      final String keyName, final Token token) throws Exception {
    UserGroupInformation tokenUgi =
        UserGroupInformation.createUserForTesting("test", new String[] {});
    // Verify the tokens can authenticate to any KMS
    tokenUgi.addToken(token);
    tokenUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // Create a kms client with one provider at a time. Must use one
        // provider so that if it fails to authenticate, it does not fall
        // back to the next KMS instance.
        // It should succeed because its delegation token can access any
        // KMS instances.
        for (KMSClientProvider provider : lbkp.getProviders()) {
          if (token.getKind().equals(TOKEN_LEGACY_KIND) && !token.getService()
              .equals(provider.getDelegationTokenService())) {
            // Historically known issue: Legacy token can only work with the
            // key provider specified in the token's Service
            continue;
          }
          LOG.info("Rolling key {} via provider {} with token {}.", keyName,
              provider, token);
          provider.rollNewVersion(keyName);
        }
        return null;
      }
    });
  }

  @Test
  public void testKMSHAZKDelegationTokenRenewCancel() throws Exception {
    testKMSHAZKDelegationTokenRenewCancel(TOKEN_KIND);
  }

  @Test
  public void testKMSHAZKDelegationTokenRenewCancelLegacy() throws Exception {
    testKMSHAZKDelegationTokenRenewCancel(TOKEN_LEGACY_KIND);
  }

  private void testKMSHAZKDelegationTokenRenewCancel(final Text tokenKind)
      throws Exception {
    GenericTestUtils.setLogLevel(KMSTokenRenewer.LOG, Level.TRACE);
    assertTrue(tokenKind == TOKEN_KIND || tokenKind == TOKEN_LEGACY_KIND);
    KMSCallable<Void> c = new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        final URI[] uris = createKMSHAUri(getKMSHAUrl());
        final Credentials credentials = new Credentials();
        // Create a UGI without Kerberos auth. It will be authenticated with
        // delegation token.
        final UserGroupInformation nonKerberosUgi =
            UserGroupInformation.getCurrentUser();
        final String lbUri = generateLoadBalancingKeyProviderUriString();
        final LoadBalancingKMSClientProvider lbkp =
            createHAProvider(uris, conf, lbUri);
        conf.unset(HADOOP_SECURITY_KEY_PROVIDER_PATH);
        // Login as a Kerberos user principal using keytab.
        // Connect to KMS to create a delegation token and add it to credentials
        doAs("SET_KEY_MATERIAL",
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                KeyProviderDelegationTokenExtension kpdte =
                    KeyProviderDelegationTokenExtension.
                        createKeyProviderDelegationTokenExtension(lbkp);
                kpdte.addDelegationTokens("SET_KEY_MATERIAL", credentials);
                return null;
              }
            });

        // Test token renewal and cancellation
        final Collection<Token<? extends TokenIdentifier>> tokens =
            credentials.getAllTokens();
        doAs("SET_KEY_MATERIAL", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            Assert.assertEquals(2, tokens.size());
            boolean tokenFound = false;
            for (Token token : tokens) {
              if (!tokenKind.equals(token.getKind())) {
                continue;
              } else {
                tokenFound = true;
              }
              KMSUtilFaultInjector.set(testInjector);
              setupConfForToken(token.getKind(), conf, lbUri);

              LOG.info("Testing token: {}", token);
              long tokenLife = token.renew(conf);
              LOG.info("Renewed token {}, new lifetime:{}", token, tokenLife);
              Thread.sleep(10);
              long newTokenLife = token.renew(conf);
              LOG.info("Renewed token {}, new lifetime:{}", token,
                  newTokenLife);
              assertTrue(newTokenLife > tokenLife);

              boolean canceled = false;
              // test delegation token cancellation
              if (!canceled) {
                token.cancel(conf);
                LOG.info("Cancelled token {}", token);
                canceled = true;
              }
              assertTrue("token should have been canceled", canceled);
              try {
                token.renew(conf);
                fail("should not be able to renew a canceled token " + token);
              } catch (Exception e) {
                LOG.info("Expected exception when renewing token", e);
              }
            }
            assertTrue("Should have found token kind " + tokenKind + " from "
                + tokens, tokenFound);
            return null;
          }
        });
        return null;
      }
    };
    runServerWithZooKeeper(true, true, c, 2);
  }

  /**
   * Set or unset the key provider configuration based on token kind.
   */
  private void setupConfForToken(Text tokenKind, Configuration conf,
      String lbUri) {
    if (tokenKind.equals(TOKEN_KIND)) {
      conf.unset(HADOOP_SECURITY_KEY_PROVIDER_PATH);
    } else {
      // conf is only required for legacy tokens to create provider,
      // new tokens create provider by parsing its own Service field
      assertEquals(TOKEN_LEGACY_KIND, tokenKind);
      conf.set(HADOOP_SECURITY_KEY_PROVIDER_PATH, lbUri);
    }
  }

  @Test
  public void testProxyUserKerb() throws Exception {
    doProxyUserTest(true);
  }

  @Test
  public void testProxyUserSimple() throws Exception {
    doProxyUserTest(false);
  }

  public void doProxyUserTest(final boolean kerberos) throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    if (kerberos) {
      conf.set("hadoop.kms.authentication.type", "kerberos");
    }
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    conf.set("hadoop.kms.proxyuser.client.users", "foo,bar");
    conf.set("hadoop.kms.proxyuser.client.hosts", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kaa.ALL", "client");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kbb.ALL", "foo");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kcc.ALL", "foo1");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kdd.ALL", "bar");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        UserGroupInformation proxyUgi = null;
        if (kerberos) {
          // proxyuser client using kerberos credentials
          proxyUgi = UserGroupInformation.
              loginUserFromKeytabAndReturnUGI("client", keytab.getAbsolutePath());
        } else {
          proxyUgi = UserGroupInformation.createRemoteUser("client");
          UserGroupInformation.setLoginUser(proxyUgi);
        }

        final UserGroupInformation clientUgi = proxyUgi; 
        clientUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            final KeyProvider kp = createProvider(uri, conf);
            kp.createKey("kaa", new KeyProvider.Options(conf));

            // authorized proxyuser
            UserGroupInformation fooUgi =
                UserGroupInformation.createProxyUser("foo", clientUgi);
            fooUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                Assert.assertNotNull(kp.createKey("kbb",
                    new KeyProvider.Options(conf)));
                return null;
              }
            });

            // unauthorized proxyuser
            UserGroupInformation foo1Ugi =
                UserGroupInformation.createProxyUser("foo1", clientUgi);
            foo1Ugi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                try {
                  kp.createKey("kcc", new KeyProvider.Options(conf));
                  Assert.fail();
                } catch (AuthorizationException ex) {
                  // OK
                } catch (Exception ex) {
                  Assert.fail(ex.getMessage());
                }
                return null;
              }
            });

            // authorized proxyuser
            UserGroupInformation barUgi =
                UserGroupInformation.createProxyUser("bar", clientUgi);
            barUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                Assert.assertNotNull(kp.createKey("kdd",
                    new KeyProvider.Options(conf)));
                return null;
              }
            });
            return null;
          }
        });

        return null;
      }
    });
  }

  @Test
  public void testWebHDFSProxyUserKerb() throws Exception {
    doWebHDFSProxyUserTest(true);
  }

  @Test
  public void testWebHDFSProxyUserSimple() throws Exception {
    doWebHDFSProxyUserTest(false);
  }

  @Test
  public void testTGTRenewal() throws Exception {
    shutdownMiniKdc();
    try {
      testTgtRenewalInt();
    } finally {
      shutdownMiniKdc();
      setUpMiniKdc();
    }
  }

  private void testTgtRenewalInt() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "3");
    kdcConf.setProperty(MiniKdc.MIN_TICKET_LIFETIME, "3");
    setUpMiniKdc(kdcConf);

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    conf.set("hadoop.kms.proxyuser.client.users", "*");
    conf.set("hadoop.kms.proxyuser.client.hosts", "*");
    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        final URI uri = createKMSUri(getKMSUrl());
        UserGroupInformation.setShouldRenewImmediatelyForTests(true);
        UserGroupInformation
            .loginUserFromKeytab("client", keytab.getAbsolutePath());
        final UserGroupInformation clientUgi =
            UserGroupInformation.getCurrentUser();
        clientUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            // Verify getKeys can relogin
            Thread.sleep(3100);
            KeyProvider kp = createProvider(uri, conf);
            kp.getKeys();

            // Verify addDelegationTokens can relogin
            // (different code path inside KMSClientProvider than getKeys)
            Thread.sleep(3100);
            kp = createProvider(uri, conf);
            ((KeyProviderDelegationTokenExtension.DelegationTokenExtension) kp)
                .addDelegationTokens("myuser", new Credentials());

            // Verify getKeys can relogin with proxy user
            UserGroupInformation anotherUgi =
                UserGroupInformation.createProxyUser("client1", clientUgi);
            anotherUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                Thread.sleep(3100);
                KeyProvider kp = createProvider(uri, conf);
                kp.getKeys();
                return null;
              }
            });
            return null;
          }
        });
        return null;
      }
    });
  }

  public void doWebHDFSProxyUserTest(final boolean kerberos) throws Exception {
    Configuration conf = new Configuration();
    if (kerberos) {
      conf.set("hadoop.security.authentication", "kerberos");
    }
    UserGroupInformation.setConfiguration(conf);

    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir, conf);
    if (kerberos) {
      conf.set("hadoop.kms.authentication.type", "kerberos");
    }
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    conf.set("hadoop.security.kms.client.timeout", "300");
    conf.set("hadoop.kms.proxyuser.client.users", "foo,bar");
    conf.set("hadoop.kms.proxyuser.client.hosts", "*");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kaa.ALL", "foo");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kbb.ALL", "foo1");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "kcc.ALL", "bar");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        UserGroupInformation proxyUgi = null;
        if (kerberos) {
          // proxyuser client using kerberos credentials
          proxyUgi = UserGroupInformation.
              loginUserFromKeytabAndReturnUGI("client", keytab.getAbsolutePath());
        } else {
          proxyUgi = UserGroupInformation.createRemoteUser("client");
        }

        final UserGroupInformation clientUgi = proxyUgi; 
        clientUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {

            // authorized proxyuser
            UserGroupInformation fooUgi =
                UserGroupInformation.createProxyUser("foo", clientUgi);
            fooUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                KeyProvider kp = createProvider(uri, conf);
                Assert.assertNotNull(kp.createKey("kaa",
                    new KeyProvider.Options(conf)));
                return null;
              }
            });

            // unauthorized proxyuser
            UserGroupInformation foo1Ugi =
                UserGroupInformation.createProxyUser("foo1", clientUgi);
            foo1Ugi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                try {
                  KeyProvider kp = createProvider(uri, conf);
                  kp.createKey("kbb", new KeyProvider.Options(conf));
                  Assert.fail();
                } catch (Exception ex) {
                  Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("Forbidden"));
                }
                return null;
              }
            });

            // authorized proxyuser
            UserGroupInformation barUgi =
                UserGroupInformation.createProxyUser("bar", clientUgi);
            barUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                KeyProvider kp = createProvider(uri, conf);
                Assert.assertNotNull(kp.createKey("kcc",
                    new KeyProvider.Options(conf)));
                return null;
              }
            });

            return null;
          }
        });

        return null;
      }
    });
  }

  /*
   * Test the jmx page can return, and contains the basic JvmMetrics. Only
   * testing in simple mode since the page content is the same, kerberized
   * or not.
   */
  @Test
  public void testKMSJMX() throws Exception {
    Configuration conf = new Configuration();
    final File confDir = getTestDir();
    conf = createBaseKMSConf(confDir, conf);
    final String processName = "testkmsjmx";
    conf.set(KMSConfiguration.METRICS_PROCESS_NAME_KEY, processName);
    writeConf(confDir, conf);

    runServer(null, null, confDir, new KMSCallable<Void>() {
      @Override
      public Void call() throws Exception {
        final URL jmxUrl = new URL(
            getKMSUrl() + "/jmx?user.name=whatever&qry=Hadoop:service="
                + processName + ",name=JvmMetrics");
        LOG.info("Requesting jmx from " + jmxUrl);
        final StringBuilder sb = new StringBuilder();
        final InputStream in = jmxUrl.openConnection().getInputStream();
        final byte[] buffer = new byte[64 * 1024];
        int len;
        while ((len = in.read(buffer)) > 0) {
          sb.append(new String(buffer, 0, len));
        }
        LOG.info("jmx returned: " + sb.toString());
        assertTrue(sb.toString().contains("JvmMetrics"));
        return null;
      }
    });
  }
}
