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

package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import java.security.InvalidKeyException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.x509.X509V1CertificateGenerator;

public class KeyStoreTestUtil {

  public static String getClasspathDir(Class klass) throws Exception {
    String file = klass.getName();
    file = file.replace('.', '/') + ".class";
    URL url = Thread.currentThread().getContextClassLoader().getResource(file);
    String baseDir = url.toURI().getPath();
    baseDir = baseDir.substring(0, baseDir.length() - file.length() - 1);
    return baseDir;
  }

  @SuppressWarnings("deprecation")
  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   */
  public static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
      throws CertificateEncodingException,
             InvalidKeyException,
             IllegalStateException,
             NoSuchProviderException, NoSuchAlgorithmException, SignatureException{

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000l);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal  dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);

    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }

  public static KeyPair generateKeyPair(String algorithm)
      throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore()
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, String filename,
      String password)
      throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(filename);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  public static void createKeyStore(String filename,
      String password, String alias,
      Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(),
        new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  /**
   * Creates a keystore with a single key and saves it to a file.
   *
   * @param filename String file to save
   * @param password String store password to set on keystore
   * @param keyPassword String key password to set on key
   * @param alias String alias to use for the key
   * @param privateKey Key to save in keystore
   * @param cert Certificate to use as certificate chain associated to key
   * @throws GeneralSecurityException for any error with the security APIs
   * @throws IOException if there is an I/O error saving the file
   */
  public static void createKeyStore(String filename,
      String password, String keyPassword, String alias,
      Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(),
        new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  public static void createTrustStore(String filename,
      String password, String alias,
      Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    saveKeyStore(ks, filename, password);
  }

  public static <T extends Certificate> void createTrustStore(
      String filename, String password, Map<String, T> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, filename, password);
  }

  public static void cleanupSSLConfig(String keystoresDir, String sslConfDir)
      throws Exception {
    File f = new File(keystoresDir + "/clientKS.jks");
    f.delete();
    f = new File(keystoresDir + "/serverKS.jks");
    f.delete();
    f = new File(keystoresDir + "/trustKS.jks");
    f.delete();
    f = new File(sslConfDir + "/ssl-client.xml");
    f.delete();
    f = new File(sslConfDir + "/ssl-server.xml");
    f.delete();
  }

  /**
   * Performs complete setup of SSL configuration in preparation for testing an
   * SSLFactory.  This includes keys, certs, keystores, truststores, the server
   * SSL configuration file, the client SSL configuration file, and the master
   * configuration file read by the SSLFactory.
   *
   * @param keystoresDir String directory to save keystores
   * @param sslConfDir String directory to save SSL configuration files
   * @param conf Configuration master configuration to be used by an SSLFactory,
   * which will be mutated by this method
   * @param useClientCert boolean true to make the client present a cert in the
   * SSL handshake
   */
  public static void setupSSLConfig(String keystoresDir, String sslConfDir,
      Configuration conf, boolean useClientCert) throws Exception {
    setupSSLConfig(keystoresDir, sslConfDir, conf, useClientCert, true);
  }

  /**
   * Performs complete setup of SSL configuration in preparation for testing an
   * SSLFactory.  This includes keys, certs, keystores, truststores, the server
   * SSL configuration file, the client SSL configuration file, and the master
   * configuration file read by the SSLFactory.
   *
   * @param keystoresDir String directory to save keystores
   * @param sslConfDir String directory to save SSL configuration files
   * @param conf Configuration master configuration to be used by an SSLFactory,
   * which will be mutated by this method
   * @param useClientCert boolean true to make the client present a cert in the
   * SSL handshake
   * @param trustStore boolean true to create truststore, false not to create it
   */
  public static void setupSSLConfig(String keystoresDir, String sslConfDir,
                                    Configuration conf, boolean useClientCert,
      boolean trustStore)
    throws Exception {
    String clientKS = keystoresDir + "/clientKS.jks";
    String clientPassword = "clientP";
    String serverKS = keystoresDir + "/serverKS.jks";
    String serverPassword = "serverP";
    String trustKS = null;
    String trustPassword = "trustP";

    File sslClientConfFile = new File(sslConfDir + "/ssl-client.xml");
    File sslServerConfFile = new File(sslConfDir + "/ssl-server.xml");

    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();

    if (useClientCert) {
      KeyPair cKP = KeyStoreTestUtil.generateKeyPair("RSA");
      X509Certificate cCert =
        KeyStoreTestUtil.generateCertificate("CN=localhost, O=client", cKP, 30,
                                             "SHA1withRSA");
      KeyStoreTestUtil.createKeyStore(clientKS, clientPassword, "client",
                                      cKP.getPrivate(), cCert);
      certs.put("client", cCert);
    }

    KeyPair sKP = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate sCert =
      KeyStoreTestUtil.generateCertificate("CN=localhost, O=server", sKP, 30,
                                           "SHA1withRSA");
    KeyStoreTestUtil.createKeyStore(serverKS, serverPassword, "server",
                                    sKP.getPrivate(), sCert);
    certs.put("server", sCert);

    if (trustStore) {
      trustKS = keystoresDir + "/trustKS.jks";
      KeyStoreTestUtil.createTrustStore(trustKS, trustPassword, certs);
    }

    Configuration clientSSLConf = createClientSSLConfig(clientKS, clientPassword,
      clientPassword, trustKS);
    Configuration serverSSLConf = createServerSSLConfig(serverKS, serverPassword,
      serverPassword, trustKS);

    saveConfig(sslClientConfFile, clientSSLConf);
    saveConfig(sslServerConfFile, serverSSLConf);

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    conf.set(SSLFactory.SSL_CLIENT_CONF_KEY, sslClientConfFile.getName());
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServerConfFile.getName());
    conf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, useClientCert);
  }

  /**
   * Creates SSL configuration for a client.
   * 
   * @param clientKS String client keystore file
   * @param password String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for client SSL
   */
  public static Configuration createClientSSLConfig(String clientKS,
      String password, String keyPassword, String trustKS) {
    Configuration clientSSLConf = createSSLConfig(SSLFactory.Mode.CLIENT,
      clientKS, password, keyPassword, trustKS);
    return clientSSLConf;
  }

  /**
   * Creates SSL configuration for a server.
   * 
   * @param serverKS String server keystore file
   * @param password String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for server SSL
   */
  public static Configuration createServerSSLConfig(String serverKS,
      String password, String keyPassword, String trustKS) throws IOException {
    Configuration serverSSLConf = createSSLConfig(SSLFactory.Mode.SERVER,
      serverKS, password, keyPassword, trustKS);
    return serverSSLConf;
  }

  /**
   * Creates SSL configuration.
   * 
   * @param mode SSLFactory.Mode mode to configure
   * @param keystore String keystore file
   * @param password String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for SSL
   */
  private static Configuration createSSLConfig(SSLFactory.Mode mode,
      String keystore, String password, String keyPassword, String trustKS) {
    String trustPassword = "trustP";

    Configuration sslConf = new Configuration(false);
    if (keystore != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), keystore);
    }
    if (password != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), password);
    }
    if (keyPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY),
        keyPassword);
    }
    if (trustKS != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY), trustKS);
    }
    if (trustPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY),
        trustPassword);
    }
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
      FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY), "1000");

    return sslConf;
  }

  /**
   * Saves configuration to a file.
   * 
   * @param file File to save
   * @param conf Configuration contents to write to file
   * @throws IOException if there is an I/O error saving the file
   */
  public static void saveConfig(File file, Configuration conf)
      throws IOException {
    Writer writer = new FileWriter(file);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
  }

  public static void provisionPasswordsToCredentialProvider() throws Exception {
    File testDir = new File(System.getProperty("test.build.data",
        "target/test-dir"));

    Configuration conf = new Configuration();
    final Path jksPath = new Path(testDir.toString(), "test.jks");
    final String ourUrl =
    JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(testDir, "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    char[] keypass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
    char[] storepass = {'s', 't', 'o', 'r', 'e', 'p', 'a', 's', 's'};

    // create new aliases
    try {
      provider.createCredentialEntry(
          FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
              FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY),
              storepass);

      provider.createCredentialEntry(
          FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
              FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY),
              keypass);

      // write out so that it can be found in checks
      provider.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
