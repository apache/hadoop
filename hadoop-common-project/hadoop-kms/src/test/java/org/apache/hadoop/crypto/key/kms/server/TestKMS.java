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
import org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.http.client.utils.URIBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.net.InetSocketAddress;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  @Rule
  public final Timeout testTimeout = new Timeout(180000);

  @Before
  public void setUp() throws Exception {
    setUpMiniKdc();
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
    private URL kmsUrl;

    protected URL getKMSUrl() {
      return kmsUrl;
    }
  }

  protected KeyProvider createProvider(URI uri, Configuration conf)
      throws IOException {
    return new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] { new KMSClientProvider(uri, conf) }, conf);
  }

  private KMSClientProvider createKMSClientProvider(URI uri, Configuration conf)
      throws IOException {
    return new KMSClientProvider(uri, conf);
  }

  protected <T> T runServer(String keystore, String password, File confDir,
      KMSCallable<T> callable) throws Exception {
    return runServer(-1, keystore, password, confDir, callable);
  }

  protected <T> T runServer(int port, String keystore, String password, File confDir,
      KMSCallable<T> callable) throws Exception {
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder().setKmsConfDir(confDir)
        .setLog4jConfFile("log4j.properties");
    if (keystore != null) {
      miniKMSBuilder.setSslConf(new File(keystore), password);
    }
    if (port > 0) {
      miniKMSBuilder.setPort(port);
    }
    MiniKMS miniKMS = miniKMSBuilder.build();
    miniKMS.start();
    try {
      System.out.println("Test KMS running at: " + miniKMS.getKMSUrl());
      callable.kmsUrl = miniKMS.getKMSUrl();
      return callable.call();
    } finally {
      miniKMS.stop();
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

  private void setUpMiniKdc() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    setUpMiniKdc(kdcConf);
  }

  @After
  public void tearDownMiniKdc() throws Exception {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
    UserGroupInformation.setShouldRenewImmediatelyForTests(false);
    UserGroupInformation.reset();
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
                Assert.assertEquals(1, tokens.length);
                Assert.assertEquals("kms-dt", tokens[0].getKind().toString());
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
          Assert.assertEquals(1, tokens.length);
          Assert.assertEquals("kms-dt", tokens[0].getKind().toString());
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
    conf.setInt(KMSClientProvider.TIMEOUT_ATTR, 1);
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

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = createProvider(uri, clientConf);
            // test delegation token retrieval
            KeyProviderDelegationTokenExtension kpdte =
                KeyProviderDelegationTokenExtension.
                    createKeyProviderDelegationTokenExtension(kp);
            final Credentials credentials = new Credentials();
            final Token<?>[] tokens =
                kpdte.addDelegationTokens("client1", credentials);
            Assert.assertEquals(1, credentials.getAllTokens().size());
            InetSocketAddress kmsAddr =
                new InetSocketAddress(getKMSUrl().getHost(),
                    getKMSUrl().getPort());
            Assert.assertEquals(KMSDelegationToken.TOKEN_KIND,
                credentials.getToken(SecurityUtil.buildTokenService(kmsAddr)).
                    getKind());

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
        final KeyProvider kp = createProvider(uri, clientConf);
        final KeyProviderDelegationTokenExtension kpdte =
            KeyProviderDelegationTokenExtension.
                createKeyProviderDelegationTokenExtension(kp);
        final InetSocketAddress kmsAddr =
            new InetSocketAddress(getKMSUrl().getHost(), getKMSUrl().getPort());

        // Job 1 (e.g. YARN log aggregation job), with user DT.
        final Collection<Token<?>> job1Token = new HashSet<>();
        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            // Get a DT and use it.
            final Credentials credentials = new Credentials();
            kpdte.addDelegationTokens("client", credentials);
            Assert.assertEquals(1, credentials.getAllTokens().size());
            Assert.assertEquals(KMSDelegationToken.TOKEN_KIND, credentials.
                getToken(SecurityUtil.buildTokenService(kmsAddr)).getKind());
            UserGroupInformation.getCurrentUser().addCredentials(credentials);
            LOG.info("Added kms dt to credentials: {}", UserGroupInformation.
                getCurrentUser().getCredentials().getAllTokens());
            Token<?> token =
                UserGroupInformation.getCurrentUser().getCredentials()
                    .getToken(SecurityUtil.buildTokenService(kmsAddr));
            Assert.assertNotNull(token);
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
            Assert.assertEquals(1, newCreds.getAllTokens().size());
            Assert.assertEquals(KMSDelegationToken.TOKEN_KIND,
                newCreds.getToken(SecurityUtil.buildTokenService(kmsAddr)).
                    getKind());

            // Using job 1's DT should fail.
            final Credentials oldCreds = new Credentials();
            for (Token<?> token : job1Token) {
              if (token.getKind().equals(KMSDelegationToken.TOKEN_KIND)) {
                oldCreds
                    .addToken(SecurityUtil.buildTokenService(kmsAddr), token);
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
            Assert.assertEquals(1, newCreds.getAllTokens().size());
            Assert.assertEquals(KMSDelegationToken.TOKEN_KIND,
                newCreds.getToken(SecurityUtil.buildTokenService(kmsAddr)).
                    getKind());
            UserGroupInformation.getCurrentUser().addCredentials(newCreds);
            LOG.info("Credetials now are: {}", UserGroupInformation
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

  public void doKMSWithZK(boolean zkDTSM, boolean zkSigner) throws Exception {
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

      runServer(null, null, testDir, c);
    } finally {
      if (zkServer != null) {
        zkServer.stop();
        zkServer.close();
      }
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
    tearDownMiniKdc();
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
