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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public class TestKMS {

  @Before
  public void cleanUp() {
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

  public static Server createJettyServer(String keyStore, String password) {
    try {
      boolean ssl = keyStore != null;
      InetAddress localhost = InetAddress.getByName("localhost");
      String host = "localhost";
      ServerSocket ss = new ServerSocket(0, 50, localhost);
      int port = ss.getLocalPort();
      ss.close();
      Server server = new Server(0);
      if (!ssl) {
        server.getConnectors()[0].setHost(host);
        server.getConnectors()[0].setPort(port);
      } else {
        SslSocketConnector c = new SslSocketConnector();
        c.setHost(host);
        c.setPort(port);
        c.setNeedClientAuth(false);
        c.setKeystore(keyStore);
        c.setKeystoreType("jks");
        c.setKeyPassword(password);
        server.setConnectors(new Connector[]{c});
      }
      return server;
    } catch (Exception ex) {
      throw new RuntimeException("Could not start embedded servlet container, "
          + ex.getMessage(), ex);
    }
  }

  public static URL getJettyURL(Server server) {
    boolean ssl = server.getConnectors()[0].getClass()
        == SslSocketConnector.class;
    try {
      String scheme = (ssl) ? "https" : "http";
      return new URL(scheme + "://" +
          server.getConnectors()[0].getHost() + ":" +
          server.getConnectors()[0].getPort());
    } catch (MalformedURLException ex) {
      throw new RuntimeException("It should never happen, " + ex.getMessage(),
          ex);
    }
  }

  public static abstract class KMSCallable implements Callable<Void> {
    private URL kmsUrl;

    protected URL getKMSUrl() {
      return kmsUrl;
    }
  }

  protected void runServer(String keystore, String password, File confDir,
      KMSCallable callable) throws Exception {
    System.setProperty(KMSConfiguration.KMS_CONFIG_DIR,
        confDir.getAbsolutePath());
    System.setProperty("log4j.configuration", "log4j.properties");
    Server jetty = createJettyServer(keystore, password);
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL url = cl.getResource("webapp");
      if (url == null) {
        throw new RuntimeException(
            "Could not find webapp/ dir in test classpath");
      }
      WebAppContext context = new WebAppContext(url.getPath(), "/kms");
      jetty.addHandler(context);
      jetty.start();
      url = new URL(getJettyURL(jetty), "kms");
      System.out.println("Test KMS running at: " + url);
      callable.kmsUrl = url;
      callable.call();
    } finally {
      if (jetty != null && jetty.isRunning()) {
        try {
          jetty.stop();
        } catch (Exception ex) {
          throw new RuntimeException("Could not stop embedded Jetty, " +
              ex.getMessage(), ex);
        }
      }
    }
  }

  protected Configuration createBaseKMSConf(File keyStoreDir) throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.security.key.provider.path",
        "jceks://file@/" + keyStoreDir.getAbsolutePath() + "/kms.keystore");
    conf.set("hadoop.kms.authentication.type", "simple");
    return conf;
  }

  protected void writeConf(File confDir, Configuration conf) throws Exception {
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

  protected URI createKMSUri(URL kmsUrl) throws Exception {
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

  @BeforeClass
  public static void setUpMiniKdc() throws Exception {
    File kdcDir = getTestDir();
    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, kdcDir);
    kdc.start();
    keytab = new File(kdcDir, "keytab");
    List<String> principals = new ArrayList<String>();
    principals.add("HTTP/localhost");
    principals.add("client");
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

  @AfterClass
  public static void tearDownMiniKdc() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
  }

  private <T> T doAs(String user, final PrivilegedExceptionAction<T> action)
      throws Exception {
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(new KerberosPrincipal(user));

    //client login
    Subject subject = new Subject(false, principals,
        new HashSet<Object>(), new HashSet<Object>());
    LoginContext loginContext = new LoginContext("", subject, null,
        KerberosConfiguration.createClientConfig(user, keytab));
    try {
      loginContext.login();
      subject = loginContext.getSubject();
      UserGroupInformation ugi =
          UserGroupInformation.getUGIFromSubject(subject);
      return ugi.doAs(action);
    } finally {
      loginContext.logout();
    }
  }

  public void testStartStop(final boolean ssl, final boolean kerberos)
      throws Exception {
    Configuration conf = new Configuration();
    if (kerberos) {
      conf.set("hadoop.security.authentication", "kerberos");
    }
    UserGroupInformation.setConfiguration(conf);
    File testDir = getTestDir();
    conf = createBaseKMSConf(testDir);

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

    if (kerberos) {
      conf.set("hadoop.kms.authentication.type", "kerberos");
      conf.set("hadoop.kms.authentication.kerberos.keytab",
          keytab.getAbsolutePath());
      conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
      conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    }

    writeConf(testDir, conf);

    runServer(keystore, password, testDir, new KMSCallable() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        URL url = getKMSUrl();
        Assert.assertEquals(keystore != null,
            url.getProtocol().equals("https"));
        final URI uri = createKMSUri(getKMSUrl());

        if (kerberos) {
          for (String user : new String[]{"client", "client/host"}) {
            doAs(user, new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                final KeyProvider kp = new KMSClientProvider(uri, conf);
                // getKeys() empty
                Assert.assertTrue(kp.getKeys().isEmpty());
                return null;
              }
            });
          }
        } else {
          KeyProvider kp = new KMSClientProvider(uri, conf);
          // getKeys() empty
          Assert.assertTrue(kp.getKeys().isEmpty());
        }
        return null;
      }
    });
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

  @Test
  public void testKMSProvider() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    File confDir = getTestDir();
    conf = createBaseKMSConf(confDir);
    writeConf(confDir, conf);

    runServer(null, null, confDir, new KMSCallable() {
      @Override
      public Void call() throws Exception {
        Date started = new Date();
        Configuration conf = new Configuration();
        URI uri = createKMSUri(getKMSUrl());
        KeyProvider kp = new KMSClientProvider(uri, conf);

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

        // deleteKey()
        kp.deleteKey("k1");

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
        kp.createKey("k2", options);
        KeyProvider.Metadata meta = kp.getMetadata("k2");
        Assert.assertNull(meta.getDescription());
        Assert.assertTrue(meta.getAttributes().isEmpty());

        // createKey() description, no tags
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
        options.setDescription("d");
        kp.createKey("k3", options);
        meta = kp.getMetadata("k3");
        Assert.assertEquals("d", meta.getDescription());
        Assert.assertTrue(meta.getAttributes().isEmpty());

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("a", "A");

        // createKey() no description, tags
        options = new KeyProvider.Options(conf);
        options.setCipher("AES/CTR/NoPadding");
        options.setBitLength(128);
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
        options.setAttributes(attributes);
        kp.createKey("k5", options);
        meta = kp.getMetadata("k5");
        Assert.assertEquals("d", meta.getDescription());
        Assert.assertEquals(attributes, meta.getAttributes());

        KeyProviderDelegationTokenExtension kpdte =
            KeyProviderDelegationTokenExtension.
                createKeyProviderDelegationTokenExtension(kp);
        Credentials credentials = new Credentials();
        kpdte.addDelegationTokens("foo", credentials);
        Assert.assertEquals(1, credentials.getAllTokens().size());
        InetSocketAddress kmsAddr = new InetSocketAddress(getKMSUrl().getHost(),
            getKMSUrl().getPort());

        Assert.assertEquals(new Text("kms-dt"), credentials.getToken(
            SecurityUtil.buildTokenService(kmsAddr)).getKind());
        return null;
      }
    });
  }

  @Test
  public void testACLs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getConfigKey(), type.toString());
    }
    conf.set(KMSACLs.Type.CREATE.getConfigKey(),
        KMSACLs.Type.CREATE.toString() + ",SET_KEY_MATERIAL");

    conf.set(KMSACLs.Type.ROLLOVER.getConfigKey(),
        KMSACLs.Type.ROLLOVER.toString() + ",SET_KEY_MATERIAL");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        final URI uri = createKMSUri(getKMSUrl());

        //nothing allowed
        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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

        doAs("DECRYPT_EEK", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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
            KeyProvider kp = new KMSClientProvider(uri, conf);
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

        // test ACL reloading
        Thread.sleep(10); // to ensure the ACLs file modifiedTime is newer
        conf.set(KMSACLs.Type.CREATE.getConfigKey(), "foo");
        writeConf(testDir, conf);
        Thread.sleep(1000);

        KMSWebApp.getACLs().run(); // forcing a reload by hand.

        // should not be able to create a key now
        doAs("CREATE", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = new KMSClientProvider(uri, conf);
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

        return null;
      }
    });
  }

  @Test
  public void testServicePrincipalACLs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    File testDir = getTestDir();
    conf = createBaseKMSConf(testDir);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getConfigKey(), " ");
    }
    conf.set(KMSACLs.Type.CREATE.getConfigKey(), "client");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 128);
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 64);
        final URI uri = createKMSUri(getKMSUrl());

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              KeyProvider kp = new KMSClientProvider(uri, conf);
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
              KeyProvider kp = new KMSClientProvider(uri, conf);
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
      KeyProvider kp = new KMSClientProvider(uri, conf);
      kp.getKeys();
    } catch (SocketTimeoutException e) {
      caughtTimeout = true;
    } catch (IOException e) {
      Assert.assertTrue("Caught unexpected exception" + e.toString(), false);
    }

    caughtTimeout = false;
    try {
      KeyProvider kp = new KMSClientProvider(uri, conf);
      KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp)
          .generateEncryptedKey("a");
    } catch (SocketTimeoutException e) {
      caughtTimeout = true;
    } catch (IOException e) {
      Assert.assertTrue("Caught unexpected exception" + e.toString(), false);
    }

    caughtTimeout = false;
    try {
      KeyProvider kp = new KMSClientProvider(uri, conf);
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
    UserGroupInformation.setConfiguration(conf);
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");

    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 64);
        final URI uri = createKMSUri(getKMSUrl());
        final Credentials credentials = new Credentials();
        final UserGroupInformation nonKerberosUgi =
            UserGroupInformation.getCurrentUser();

        try {
          KeyProvider kp = new KMSClientProvider(uri, conf);
          kp.createKey("kA", new KeyProvider.Options(conf));
        } catch (IOException ex) {
          System.out.println(ex.getMessage());
        }

        doAs("client", new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = new KMSClientProvider(uri, conf);
            KeyProviderDelegationTokenExtension kpdte =
                KeyProviderDelegationTokenExtension.
                    createKeyProviderDelegationTokenExtension(kp);
            kpdte.addDelegationTokens("foo", credentials);
            return null;
          }
        });

        nonKerberosUgi.addCredentials(credentials);

        try {
          KeyProvider kp = new KMSClientProvider(uri, conf);
          kp.createKey("kA", new KeyProvider.Options(conf));
        } catch (IOException ex) {
          System.out.println(ex.getMessage());
        }

        nonKerberosUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            KeyProvider kp = new KMSClientProvider(uri, conf);
            kp.createKey("kD", new KeyProvider.Options(conf));
            return null;
          }
        });

        return null;
      }
    });
  }

  @Test
  public void testProxyUser() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    final File testDir = getTestDir();
    conf = createBaseKMSConf(testDir);
    conf.set("hadoop.kms.authentication.type", "kerberos");
    conf.set("hadoop.kms.authentication.kerberos.keytab",
        keytab.getAbsolutePath());
    conf.set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
    conf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    conf.set("hadoop.kms.proxyuser.client.users", "foo");
    conf.set("hadoop.kms.proxyuser.client.hosts", "*");
    writeConf(testDir, conf);

    runServer(null, null, testDir, new KMSCallable() {
      @Override
      public Void call() throws Exception {
        final Configuration conf = new Configuration();
        conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 64);
        final URI uri = createKMSUri(getKMSUrl());

        // proxyuser client using kerberos credentials
        UserGroupInformation clientUgi = UserGroupInformation.
            loginUserFromKeytabAndReturnUGI("client", keytab.getAbsolutePath());
        clientUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            final KeyProvider kp = new KMSClientProvider(uri, conf);
            kp.createKey("kAA", new KeyProvider.Options(conf));

            // authorized proxyuser
            UserGroupInformation fooUgi =
                UserGroupInformation.createRemoteUser("foo");
            fooUgi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                Assert.assertNotNull(kp.createKey("kBB",
                    new KeyProvider.Options(conf)));
                return null;
              }
            });

            // unauthorized proxyuser
            UserGroupInformation foo1Ugi =
                UserGroupInformation.createRemoteUser("foo1");
            foo1Ugi.doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                try {
                  kp.createKey("kCC", new KeyProvider.Options(conf));
                  Assert.fail();
                } catch (AuthorizationException ex) {
                  // OK
                } catch (Exception ex) {
                  Assert.fail(ex.getMessage());
                }
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

}
