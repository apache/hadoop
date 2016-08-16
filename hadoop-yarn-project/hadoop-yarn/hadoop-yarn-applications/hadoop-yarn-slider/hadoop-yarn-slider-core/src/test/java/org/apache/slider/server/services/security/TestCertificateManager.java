/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.services.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.slider.Slider;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestCertificateManager {
  @Rule
  public TemporaryFolder workDir = new TemporaryFolder();
  private File secDir;
  private CertificateManager certMan;

  @Before
  public void setup() throws Exception {
    certMan = new CertificateManager();
    MapOperations compOperations = new MapOperations();
    secDir = new File(workDir.getRoot(), SliderKeys.SECURITY_DIR);
    File keystoreFile = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    compOperations.put(SliderXmlConfKeys.KEY_KEYSTORE_LOCATION,
                       keystoreFile.getAbsolutePath());
    certMan.initialize(compOperations, "cahost", null, null);
  }

  @Test
  public void testServerCertificateGenerated() throws Exception {
    File serverCrt = new File(secDir, SliderKeys.CRT_FILE_NAME);
    Assert.assertTrue("Server CRD does not exist:" + serverCrt,
                      serverCrt.exists());
  }

  @Test
  public void testAMKeystoreGenerated() throws Exception {
    File keystoreFile = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    Assert.assertTrue("Keystore does not exist: " + keystoreFile,
                      keystoreFile.exists());
    InputStream is = null;
    try {

      is = new FileInputStream(keystoreFile);
      KeyStore keystore = KeyStore.getInstance("pkcs12");
      String password = SecurityUtils.getKeystorePass();
      keystore.load(is, password.toCharArray());

      Certificate certificate = keystore.getCertificate(
          keystore.aliases().nextElement());
      Assert.assertNotNull(certificate);

      if (certificate instanceof X509Certificate) {
        X509Certificate x509cert = (X509Certificate) certificate;

        // Get subject
        Principal principal = x509cert.getSubjectDN();
        String subjectDn = principal.getName();
        Assert.assertEquals("wrong DN",
                            "CN=cahost",
                            subjectDn);

        // Get issuer
        principal = x509cert.getIssuerDN();
        String issuerDn = principal.getName();
        Assert.assertEquals("wrong Issuer DN",
                            "CN=cahost",
                            issuerDn);
      }
    } finally {
      if(null != is) {
        is.close();
      }
    }
  }

  @Test
  public void testContainerCertificateGeneration() throws Exception {
    certMan.generateContainerCertificate("testhost", "container1");
    Assert.assertTrue("container certificate not generated",
                      new File(secDir, "container1.crt").exists());
  }

  @Test
  public void testContainerKeystoreGeneration() throws Exception {
    SecurityStore keystoreFile = certMan.generateContainerKeystore("testhost",
                                                                   "container1",
                                                                   "component1",
                                                                   "password");
    validateKeystore(keystoreFile.getFile(), "testhost", "cahost");
  }

  private void validateKeystore(File keystoreFile, String certHostname,
                                String issuerHostname)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    Assert.assertTrue("container keystore not generated",
                      keystoreFile.exists());

    InputStream is = null;
    try {

      is = new FileInputStream(keystoreFile);
      KeyStore keystore = KeyStore.getInstance("pkcs12");
      String password = "password";
      keystore.load(is, password.toCharArray());

      Certificate certificate = keystore.getCertificate(
          keystore.aliases().nextElement());
      Assert.assertNotNull(certificate);

      if (certificate instanceof X509Certificate) {
        X509Certificate x509cert = (X509Certificate) certificate;

        // Get subject
        Principal principal = x509cert.getSubjectDN();
        String subjectDn = principal.getName();
        Assert.assertEquals("wrong DN", "CN=" + certHostname + ", OU=container1",
                            subjectDn);

        // Get issuer
        principal = x509cert.getIssuerDN();
        String issuerDn = principal.getName();
        Assert.assertEquals("wrong Issuer DN",
                            "CN=" + issuerHostname,
                            issuerDn);
      }
    } finally {
      if(null != is) {
        is.close();
      }
    }
  }

  @Test
  public void testContainerKeystoreGenerationViaStoresGenerator() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = new MapOperations();
    instanceDefinition.getAppConf().components.put("component1", compOps);
    compOps.put(SliderKeys.COMP_KEYSTORE_PASSWORD_PROPERTY_KEY,
                "app1.component1.password.property");
    compOps.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "true");
    instanceDefinition.getAppConf().global.put(
        "app1.component1.password.property", "password");
    instanceDefinition.resolve();
    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 1, files.length);
    validateKeystore(files[0].getFile(), "testhost", "cahost");
  }

  @Test
  public void testContainerKeystoreGenerationViaStoresGeneratorUsingGlobalProps() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = new MapOperations();
    instanceDefinition.getAppConf().components.put("component1", compOps);
    compOps.put(SliderKeys.COMP_KEYSTORE_PASSWORD_PROPERTY_KEY,
                "app1.component1.password.property");
    instanceDefinition.getAppConf().global.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "true");
    compOps.put(
        "app1.component1.password.property", "password");
    instanceDefinition.resolve();
    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 1, files.length);
    validateKeystore(files[0].getFile(), "testhost", "cahost");
  }

  @Test
  public void testContainerKeystoreGenerationViaStoresGeneratorOverrideGlobalSetting() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = setupComponentOptions(true, null,
                                                  "app1.component1.password.property",
                                                  null, null);
    instanceDefinition.getAppConf().components.put("component1", compOps);
    instanceDefinition.getAppConf().global.put(
        "app1.component1.password.property", "password");
    instanceDefinition.getAppConf().global.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "false");
    instanceDefinition.resolve();
    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 1, files.length);
    validateKeystore(files[0].getFile(), "testhost", "cahost");
  }

  @Test
  public void testContainerTrusttoreGeneration() throws Exception {
    SecurityStore keystoreFile =
        certMan.generateContainerKeystore("testhost",
                                          "container1",
                                          "component1",
                                          "keypass");
    Assert.assertTrue("container keystore not generated",
                      keystoreFile.getFile().exists());
    SecurityStore truststoreFile =
        certMan.generateContainerTruststore("container1",
                                            "component1", "trustpass"
        );
    Assert.assertTrue("container truststore not generated",
                      truststoreFile.getFile().exists());

    validateTruststore(keystoreFile.getFile(), truststoreFile.getFile());
  }

  @Test
  public void testContainerGenerationUsingStoresGeneratorNoTruststore() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = new MapOperations();
    compOps.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "true");
    compOps.put(SliderKeys.COMP_KEYSTORE_PASSWORD_ALIAS_KEY,
                "test.keystore.password");

    setupCredentials(instanceDefinition, "test.keystore.password", null);

    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 1, files.length);
    File keystoreFile = CertificateManager.getContainerKeystoreFilePath(
        "container1", "component1");
    Assert.assertTrue("container keystore not generated",
                      keystoreFile.exists());

    Assert.assertTrue("keystore not in returned list",
                      Arrays.asList(files).contains(new SecurityStore(keystoreFile,
                                                    SecurityStore.StoreType.keystore)));
    File truststoreFile =
        CertificateManager.getContainerTruststoreFilePath("component1",
                                                          "container1");
    Assert.assertFalse("container truststore generated",
                      truststoreFile.exists());
    Assert.assertFalse("truststore in returned list",
                      Arrays.asList(files).contains(new SecurityStore(truststoreFile,
                                                    SecurityStore.StoreType.truststore)));

  }

  @Test
  public void testContainerGenerationUsingStoresGeneratorJustTruststoreWithDefaultAlias() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = setupComponentOptions(true);

    setupCredentials(instanceDefinition, null,
                     SliderKeys.COMP_TRUSTSTORE_PASSWORD_ALIAS_DEFAULT);

    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 1, files.length);
    File keystoreFile = CertificateManager.getContainerKeystoreFilePath(
        "container1", "component1");
    Assert.assertFalse("container keystore generated",
                       keystoreFile.exists());
    Assert.assertFalse("keystore in returned list",
                       Arrays.asList(files).contains(keystoreFile));
    File truststoreFile =
        CertificateManager.getContainerTruststoreFilePath("component1",
                                                          "container1");
    Assert.assertTrue("container truststore not generated",
                      truststoreFile.exists());
    Assert.assertTrue("truststore not in returned list",
                      Arrays.asList(files).contains(new SecurityStore(truststoreFile,
                                                                      SecurityStore.StoreType.truststore)));

  }

  @Test
  public void testContainerTrusttoreGenerationUsingStoresGenerator() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = setupComponentOptions(true,
                                                  "test.keystore.password",
                                                  null,
                                                  "test.truststore.password",
                                                  null);

    setupCredentials(instanceDefinition, "test.keystore.password",
                     "test.truststore.password");

    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 2, files.length);
    File keystoreFile = CertificateManager.getContainerKeystoreFilePath(
        "container1", "component1");
    Assert.assertTrue("container keystore not generated",
                      keystoreFile.exists());
    Assert.assertTrue("keystore not in returned list",
                      Arrays.asList(files).contains(new SecurityStore(keystoreFile,
                                                                      SecurityStore.StoreType.keystore)));
    File truststoreFile =
        CertificateManager.getContainerTruststoreFilePath("component1",
                                                          "container1");
    Assert.assertTrue("container truststore not generated",
                      truststoreFile.exists());
    Assert.assertTrue("truststore not in returned list",
                      Arrays.asList(files).contains(new SecurityStore(truststoreFile,
                                                                      SecurityStore.StoreType.truststore)));

    validateTruststore(keystoreFile, truststoreFile);
  }

  private void setupCredentials(AggregateConf instanceDefinition,
                                String keyAlias, String trustAlias)
      throws Exception {
    Configuration conf = new Configuration();
    final Path jksPath = new Path(SecurityUtils.getSecurityDir(), "test.jks");
    final String ourUrl =
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(SecurityUtils.getSecurityDir(), "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    instanceDefinition.getAppConf().credentials.put(ourUrl, new ArrayList<String>());

    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);

    // create new aliases
    try {

      if (keyAlias != null) {
        char[] storepass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
        provider.createCredentialEntry(
            keyAlias, storepass);
      }

      if (trustAlias != null) {
        char[] trustpass = {'t', 'r', 'u', 's', 't', 'p', 'a', 's', 's'};
        provider.createCredentialEntry(
            trustAlias, trustpass);
      }

      // write out so that it can be found in checks
      provider.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private MapOperations setupComponentOptions(boolean storesRequired) {
    return this.setupComponentOptions(storesRequired, null, null, null, null);
  }

  private MapOperations setupComponentOptions(boolean storesRequired,
                                              String keyAlias,
                                              String keyPwd,
                                              String trustAlias,
                                              String trustPwd) {
    MapOperations compOps = new MapOperations();
    compOps.put(SliderKeys.COMP_STORES_REQUIRED_KEY,
                Boolean.toString(storesRequired));
    if (keyAlias != null) {
      compOps.put(SliderKeys.COMP_KEYSTORE_PASSWORD_ALIAS_KEY,
                  "test.keystore.password");
    }
    if (trustAlias != null) {
      compOps.put(SliderKeys.COMP_TRUSTSTORE_PASSWORD_ALIAS_KEY,
                  "test.truststore.password");
    }
    if (keyPwd != null) {
      compOps.put(SliderKeys.COMP_KEYSTORE_PASSWORD_PROPERTY_KEY,
                  keyPwd);
    }
    if (trustPwd != null) {
      compOps.put(SliderKeys.COMP_TRUSTSTORE_PASSWORD_PROPERTY_KEY,
                  trustPwd);
    }
    return compOps;
  }

  @Test
  public void testContainerStoresGenerationKeystoreOnly() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = new MapOperations();
    compOps.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "true");

    setupCredentials(instanceDefinition,
                     SliderKeys.COMP_KEYSTORE_PASSWORD_ALIAS_DEFAULT, null);

    SecurityStore[]
        files = StoresGenerator.generateSecurityStores("testhost",
                                                       "container1",
                                                       "component1",
                                                       instanceDefinition,
                                                       compOps);
    assertEquals("wrong number of stores", 1, files.length);
    File keystoreFile = CertificateManager.getContainerKeystoreFilePath(
        "container1", "component1");
    Assert.assertTrue("container keystore not generated",
                      keystoreFile.exists());
    Assert.assertTrue("keystore not in returned list",
                      Arrays.asList(files).contains(new SecurityStore(keystoreFile,
                                                                      SecurityStore.StoreType.keystore)));
    File truststoreFile =
        CertificateManager.getContainerTruststoreFilePath("component1",
                                                          "container1");
    Assert.assertFalse("container truststore generated",
                       truststoreFile.exists());
    Assert.assertFalse("truststore in returned list",
                       Arrays.asList(files).contains(new SecurityStore(truststoreFile,
                                                                       SecurityStore.StoreType.truststore)));

  }

  @Test
  public void testContainerStoresGenerationMisconfiguration() throws Exception {
    AggregateConf instanceDefinition = new AggregateConf();
    MapOperations compOps = new MapOperations();
    compOps.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "true");

    setupCredentials(instanceDefinition, "cant.be.found", null);

    try {
      StoresGenerator.generateSecurityStores("testhost", "container1",
                                                            "component1", instanceDefinition,
                                                            compOps);
      Assert.fail("SliderException should have been generated");
    } catch (SliderException e) {
      // ignore - should be thrown
    }
  }

  private void validateTruststore(File keystoreFile, File truststoreFile)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    InputStream keyis = null;
    InputStream trustis = null;
    try {

      // create keystore
      keyis = new FileInputStream(keystoreFile);
      KeyStore keystore = KeyStore.getInstance("pkcs12");
      String password = "keypass";
      keystore.load(keyis, password.toCharArray());

      // obtain server cert
      Certificate certificate = keystore.getCertificate(
          keystore.aliases().nextElement());
      Assert.assertNotNull(certificate);

      // create trust store from generated trust store file
      trustis = new FileInputStream(truststoreFile);
      KeyStore truststore = KeyStore.getInstance("pkcs12");
      password = "trustpass";
      truststore.load(trustis, password.toCharArray());

      // validate keystore cert using trust store
      TrustManagerFactory
          trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(truststore);

      for (TrustManager trustManager: trustManagerFactory.getTrustManagers()) {
        if (trustManager instanceof X509TrustManager) {
          X509TrustManager x509TrustManager = (X509TrustManager)trustManager;
          x509TrustManager.checkServerTrusted(
              new X509Certificate[] {(X509Certificate) certificate},
              "RSA_EXPORT");
        }
      }

    } finally {
      if(null != keyis) {
        keyis.close();
      }
      if(null != trustis) {
        trustis.close();
      }
    }
  }

}
