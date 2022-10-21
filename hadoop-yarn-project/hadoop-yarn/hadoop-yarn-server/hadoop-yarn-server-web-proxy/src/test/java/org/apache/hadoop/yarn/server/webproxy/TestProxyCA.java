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

package org.apache.hadoop.yarn.server.webproxy;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestProxyCA {

  @Test
  void testInit() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    assertNull(proxyCA.getCaCert());
    assertNull(proxyCA.getCaKeyPair());
    assertNull(proxyCA.getX509KeyManager());
    assertNull(proxyCA.getHostnameVerifier());

    proxyCA.init();
    assertNotNull(proxyCA.getCaCert());
    assertNotNull(proxyCA.getCaKeyPair());
    assertNotNull(proxyCA.getX509KeyManager());
    assertNotNull(proxyCA.getHostnameVerifier());
  }

  @Test
  void testInit2Null() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    assertNull(proxyCA.getCaCert());
    assertNull(proxyCA.getCaKeyPair());
    assertNull(proxyCA.getX509KeyManager());
    assertNull(proxyCA.getHostnameVerifier());

    // null certificate and private key
    proxyCA.init(null, null);
    assertNotNull(proxyCA.getCaCert());
    assertNotNull(proxyCA.getCaKeyPair());
    assertNotNull(proxyCA.getX509KeyManager());
    assertNotNull(proxyCA.getHostnameVerifier());
  }

  @Test
  void testInit2Mismatch() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    assertNull(proxyCA.getCaCert());
    assertNull(proxyCA.getCaKeyPair());
    assertNull(proxyCA.getX509KeyManager());
    assertNull(proxyCA.getHostnameVerifier());

    // certificate and private key don't match
    CertKeyPair pair1 = createCertAndKeyPair();
    CertKeyPair pair2 = createCertAndKeyPair();
    assertNotEquals(pair1.getCert(), pair2.getCert());
    assertNotEquals(pair1.getKeyPair().getPrivate(),
        pair2.getKeyPair().getPrivate());
    assertNotEquals(pair1.getKeyPair().getPublic(),
        pair2.getKeyPair().getPublic());
    proxyCA.init(pair1.getCert(), pair2.getKeyPair().getPrivate());
    assertNotNull(proxyCA.getCaCert());
    assertNotNull(proxyCA.getCaKeyPair());
    assertNotNull(proxyCA.getX509KeyManager());
    assertNotNull(proxyCA.getHostnameVerifier());
    assertNotEquals(proxyCA.getCaCert(), pair1.getCert());
    assertNotEquals(proxyCA.getCaKeyPair().getPrivate(),
        pair2.getKeyPair().getPrivate());
    assertNotEquals(proxyCA.getCaKeyPair().getPublic(),
        pair2.getKeyPair().getPublic());
  }

  @Test
  void testInit2Invalid() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    assertNull(proxyCA.getCaCert());
    assertNull(proxyCA.getCaKeyPair());
    assertNull(proxyCA.getX509KeyManager());
    assertNull(proxyCA.getHostnameVerifier());

    // Invalid key - fail the verification
    X509Certificate certificate = Mockito.mock(X509Certificate.class);
    PrivateKey privateKey = Mockito.mock(PrivateKey.class);
    try {
      proxyCA.init(certificate, privateKey);
      fail("Expected InvalidKeyException");
    } catch (InvalidKeyException e) {
      // expected
    }
  }

  @Test
  void testInit2() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    assertNull(proxyCA.getCaCert());
    assertNull(proxyCA.getCaKeyPair());
    assertNull(proxyCA.getX509KeyManager());
    assertNull(proxyCA.getHostnameVerifier());

    // certificate and private key do match
    CertKeyPair pair = createCertAndKeyPair();
    proxyCA.init(pair.getCert(), pair.getKeyPair().getPrivate());
    assertEquals(pair.getCert(), proxyCA.getCaCert());
    assertEquals(pair.getKeyPair().getPrivate(),
        proxyCA.getCaKeyPair().getPrivate());
    assertEquals(pair.getKeyPair().getPublic(),
        proxyCA.getCaKeyPair().getPublic());
    assertNotNull(proxyCA.getX509KeyManager());
    assertNotNull(proxyCA.getHostnameVerifier());
  }

  @Test
  void testCreateChildKeyStore() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    byte[] keystoreBytes = proxyCA.createChildKeyStore(appId,
        "password");
    KeyStore keyStore = KeyStoreTestUtil.bytesToKeyStore(keystoreBytes,
        "password");
    assertEquals(1, keyStore.size());
    Certificate[] certChain = keyStore.getCertificateChain("server");
    assertEquals(2, certChain.length);
    X509Certificate caCert = (X509Certificate) certChain[1];
    X509Certificate cert = (X509Certificate) certChain[0];

    // check child cert
    assertEquals(caCert.getSubjectX500Principal().toString(),
        cert.getIssuerDN().toString());
    assertEquals(new X500Principal("CN=" + appId),
        cert.getSubjectX500Principal());
    assertFalse(cert.getSubjectX500Principal().toString().contains(","),
        "Found multiple fields in X500 Principal, when there " +
            "should have only been one: " + cert.getSubjectX500Principal());
    assertEquals("SHA512withRSA", cert.getSigAlgName());
    assertEquals(cert.getNotBefore(), cert.getNotAfter());
    assertTrue(cert.getNotAfter().before(new Date()),
        "Expected certificate to be expired but was not: " + cert.getNotAfter());
    assertEquals(new X500Principal("CN=" + appId).toString(),
        cert.getSubjectDN().toString());
    Key privateKey = keyStore.getKey("server", "password".toCharArray());
    assertEquals("RSA", privateKey.getAlgorithm());
    assertEquals(-1, cert.getBasicConstraints());

    // verify signature on child cert
    PublicKey caPublicKey = caCert.getPublicKey();
    cert.verify(caPublicKey);

    // check CA cert
    checkCACert(caCert);
    assertEquals(proxyCA.getCaCert(), caCert);

    // verify signature on CA cert
    caCert.verify(caPublicKey);

    // verify CA public key matches private key
    PrivateKey caPrivateKey =
        proxyCA.getX509KeyManager().getPrivateKey(null);
    checkPrivatePublicKeys(caPrivateKey, caPublicKey);
    assertEquals(proxyCA.getCaKeyPair().getPublic(), caPublicKey);
    assertEquals(proxyCA.getCaKeyPair().getPrivate(), caPrivateKey);
  }

  @Test
  void testGetChildTrustStore() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    byte[] truststoreBytes = proxyCA.getChildTrustStore("password");
    KeyStore truststore = KeyStoreTestUtil.bytesToKeyStore(truststoreBytes,
        "password");
    assertEquals(1, truststore.size());
    X509Certificate caCert =
        (X509Certificate) truststore.getCertificate("client");

    // check CA cert
    checkCACert(caCert);
    assertEquals(proxyCA.getCaCert(), caCert);

    // verify signature on CA cert
    PublicKey caPublicKey = caCert.getPublicKey();
    caCert.verify(caPublicKey);

    // verify CA public key matches private key
    PrivateKey caPrivateKey =
        proxyCA.getX509KeyManager().getPrivateKey(null);
    checkPrivatePublicKeys(caPrivateKey, caPublicKey);
    assertEquals(proxyCA.getCaKeyPair().getPublic(), caPublicKey);
    assertEquals(proxyCA.getCaKeyPair().getPrivate(), caPrivateKey);
  }

  @Test
  void testGenerateKeyStorePassword() throws Exception {
    // We can't possibly test every possible string, but we can at least verify
    // a few things about a few of the generated strings as a sanity check
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    Set<String> passwords = new HashSet<>();

    for (int i = 0; i < 5; i++) {
      String password = proxyCA.generateKeyStorePassword();
      assertEquals(16, password.length());
      for (char c : password.toCharArray()) {
        assertFalse(c < ' ', "Found character '" + c + "' in password '"
            + password + "' which is outside of the expected range");
        assertFalse(c > 'z', "Found character '" + c + "' in password '"
            + password + "' which is outside of the expected range");
      }
      assertFalse(passwords.contains(password),
          "Password " + password
              + " was generated twice, which is _extremely_ unlikely"
              + " and shouldn't practically happen: " + passwords);
      passwords.add(password);
    }
  }

  @Test
  void testCreateTrustManagerDefaultTrustManager() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509TrustManager defaultTrustManager = Mockito.mock(X509TrustManager.class);
    proxyCA.setDefaultTrustManager(defaultTrustManager);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    X509TrustManager trustManager = proxyCA.createTrustManager(appId);
    Mockito.when(defaultTrustManager.getAcceptedIssuers()).thenReturn(
        new X509Certificate[]{KeyStoreTestUtil.generateCertificate(
            "CN=foo", KeyStoreTestUtil.generateKeyPair("RSA"), 30,
            "SHA1withRSA")});

    assertArrayEquals(defaultTrustManager.getAcceptedIssuers(),
        trustManager.getAcceptedIssuers());
    trustManager.checkClientTrusted(null, null);
  }

  @Test
  void testCreateTrustManagerYarnCert() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509TrustManager defaultTrustManager = Mockito.mock(X509TrustManager.class);
    proxyCA.setDefaultTrustManager(defaultTrustManager);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    X509TrustManager trustManager = proxyCA.createTrustManager(appId);

    X509Certificate[] certChain = castCertificateArrayToX509CertificateArray(
        KeyStoreTestUtil.bytesToKeyStore(
            proxyCA.createChildKeyStore(appId, "password"), "password")
            .getCertificateChain("server"));
    trustManager.checkServerTrusted(certChain, "RSA");
    Mockito.verify(defaultTrustManager, Mockito.times(0))
        .checkServerTrusted(certChain, "RSA");
  }

  @Test
  void testCreateTrustManagerWrongApp() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509TrustManager defaultTrustManager = Mockito.mock(X509TrustManager.class);
    proxyCA.setDefaultTrustManager(defaultTrustManager);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationId appId2 =
        ApplicationId.newInstance(System.currentTimeMillis(), 2);
    X509TrustManager trustManager = proxyCA.createTrustManager(appId);

    X509Certificate[] certChain = castCertificateArrayToX509CertificateArray(
        KeyStoreTestUtil.bytesToKeyStore(
            proxyCA.createChildKeyStore(appId2, "password"), "password")
            .getCertificateChain("server"));
    try {
      trustManager.checkServerTrusted(certChain, "RSA");
      fail("Should have thrown a CertificateException, but did not");
    } catch (CertificateException ce) {
      assertEquals("Expected to find Subject X500 Principal with CN=" +
          appId + " but found CN=" + appId2, ce.getMessage());
    }
  }

  @Test
  void testCreateTrustManagerWrongRM() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509TrustManager defaultTrustManager = Mockito.mock(X509TrustManager.class);
    proxyCA.setDefaultTrustManager(defaultTrustManager);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    X509TrustManager trustManager = proxyCA.createTrustManager(appId);

    ProxyCA proxyCA2 = new ProxyCA(); // Simulates another RM
    proxyCA2.init();
    X509Certificate[] certChain = castCertificateArrayToX509CertificateArray(
        KeyStoreTestUtil.bytesToKeyStore(
            proxyCA2.createChildKeyStore(appId, "password"), "password")
            .getCertificateChain("server"));
    Mockito.verify(defaultTrustManager, Mockito.times(0))
        .checkServerTrusted(certChain, "RSA");
    trustManager.checkServerTrusted(certChain, "RSA");
    Mockito.verify(defaultTrustManager, Mockito.times(1))
        .checkServerTrusted(certChain, "RSA");
  }

  @Test
  void testCreateTrustManagerRealCert() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509TrustManager defaultTrustManager = Mockito.mock(X509TrustManager.class);
    proxyCA.setDefaultTrustManager(defaultTrustManager);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    X509TrustManager trustManager = proxyCA.createTrustManager(appId);

    // "real" cert
    X509Certificate[]
        certChain = new X509Certificate[]{
            KeyStoreTestUtil.generateCertificate("CN=foo.com",
                KeyStoreTestUtil.generateKeyPair("RSA"), 30, "SHA1withRSA")};
    Mockito.verify(defaultTrustManager, Mockito.times(0))
        .checkServerTrusted(certChain, "RSA");
    trustManager.checkServerTrusted(certChain, "RSA");
    Mockito.verify(defaultTrustManager, Mockito.times(1))
        .checkServerTrusted(certChain, "RSA");

    // "real" cert x2
    certChain = new X509Certificate[]{
        KeyStoreTestUtil.generateCertificate("CN=foo.com",
            KeyStoreTestUtil.generateKeyPair("RSA"), 30, "SHA1withRSA"),
        KeyStoreTestUtil.generateCertificate("CN=foo.com",
            KeyStoreTestUtil.generateKeyPair("RSA"), 30, "SHA1withRSA")};
    Mockito.verify(defaultTrustManager, Mockito.times(0))
        .checkServerTrusted(certChain, "RSA");
    trustManager.checkServerTrusted(certChain, "RSA");
    Mockito.verify(defaultTrustManager, Mockito.times(1))
        .checkServerTrusted(certChain, "RSA");
  }

  @Test
  void testCreateTrustManagerExceptions() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509TrustManager defaultTrustManager = Mockito.mock(X509TrustManager.class);
    proxyCA.setDefaultTrustManager(defaultTrustManager);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    X509TrustManager trustManager = proxyCA.createTrustManager(appId);

    for (Exception e : new Exception[]{
        new CertificateException(), new NoSuchAlgorithmException(),
        new InvalidKeyException(), new SignatureException(),
        new NoSuchProviderException()}) {
      X509Certificate[] certChain = castCertificateArrayToX509CertificateArray(
          KeyStoreTestUtil.bytesToKeyStore(
              proxyCA.createChildKeyStore(appId, "password"), "password")
              .getCertificateChain("server"));
      X509Certificate cert = Mockito.spy(certChain[0]);
      certChain[0] = cert;
      // Throw e to simulate problems with verifying
      Mockito.doThrow(e).when(certChain[0]).verify(Mockito.any());
      Mockito.verify(defaultTrustManager, Mockito.times(0))
          .checkServerTrusted(certChain, "RSA");
      trustManager.checkServerTrusted(certChain, "RSA");
      Mockito.verify(defaultTrustManager, Mockito.times(1))
          .checkServerTrusted(certChain, "RSA");
    }
  }

  @Test
  void testCreateKeyManager() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509KeyManager keyManager = proxyCA.getX509KeyManager();

    assertArrayEquals(new String[]{"client"},
        keyManager.getClientAliases(null, null));
    assertEquals("client",
        keyManager.chooseClientAlias(null, null, null));
    assertNull(keyManager.getServerAliases(null, null));
    assertNull(keyManager.chooseServerAlias(null, null, null));

    byte[] truststoreBytes = proxyCA.getChildTrustStore("password");
    KeyStore truststore = KeyStoreTestUtil.bytesToKeyStore(truststoreBytes,
        "password");
    assertEquals(1, truststore.size());
    X509Certificate caCert =
        (X509Certificate) truststore.getCertificate("client");
    assertArrayEquals(new X509Certificate[]{caCert},
        keyManager.getCertificateChain(null));
    assertEquals(proxyCA.getCaCert(), caCert);

    PrivateKey caPrivateKey = keyManager.getPrivateKey(null);
    PublicKey caPublicKey = caCert.getPublicKey();
    checkPrivatePublicKeys(caPrivateKey, caPublicKey);
    assertEquals(proxyCA.getCaKeyPair().getPublic(), caPublicKey);
    assertEquals(proxyCA.getCaKeyPair().getPrivate(), caPrivateKey);
  }

  @Test
  void testCreateHostnameVerifier() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    Mockito.when(sslSession.getPeerCertificates()).thenReturn(
        KeyStoreTestUtil.bytesToKeyStore(
            proxyCA.createChildKeyStore(
                ApplicationId.newInstance(System.currentTimeMillis(), 1),
                "password"), "password").getCertificateChain("server"));
    assertTrue(verifier.verify("foo", sslSession));
  }

  @Test
  void testCreateHostnameVerifierSSLPeerUnverifiedException()
      throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    Mockito.when(sslSession.getPeerCertificates()).thenThrow(
        new SSLPeerUnverifiedException(""));
    assertFalse(verifier.verify("foo", sslSession));
  }

  @Test
  void testCreateHostnameVerifierWrongRM() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    ProxyCA proxyCA2 = new ProxyCA(); // Simulate another RM
    proxyCA2.init();
    Mockito.when(sslSession.getPeerCertificates()).thenReturn(
        KeyStoreTestUtil.bytesToKeyStore(
            proxyCA2.createChildKeyStore(
                ApplicationId.newInstance(System.currentTimeMillis(), 1),
                "password"), "password").getCertificateChain("server"));
    assertFalse(verifier.verify("foo", sslSession));
  }

  @Test
  void testCreateHostnameVerifierExceptions() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    for (Exception e : new Exception[]{
        new CertificateException(), new NoSuchAlgorithmException(),
        new InvalidKeyException(), new SignatureException(),
        new NoSuchProviderException()}) {
      SSLSession sslSession = Mockito.mock(SSLSession.class);
      Mockito.when(sslSession.getPeerCertificates()).thenAnswer(
          new Answer<Certificate[]>() {
            @Override
            public Certificate[] answer(InvocationOnMock invocation)
                throws Throwable {
              Certificate[] certChain = KeyStoreTestUtil.bytesToKeyStore(
                  proxyCA.createChildKeyStore(
                      ApplicationId.newInstance(System.currentTimeMillis(), 1),
                      "password"), "password").getCertificateChain("server");
              Certificate cert = Mockito.spy(certChain[0]);
              certChain[0] = cert;
              // Throw e to simulate problems with verifying
              Mockito.doThrow(e).when(cert).verify(Mockito.any());
              return certChain;
            }
          });
      assertFalse(verifier.verify("foo", sslSession));
    }
  }

  @Test
  void testCreateHostnameVerifierRealCert() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    Mockito.when(sslSession.getPeerCertificates()).thenAnswer(
        new Answer<Certificate[]>() {
          @Override
          public Certificate[] answer(InvocationOnMock invocation)
              throws Throwable {
            // "real" cert
            Certificate[] certChain = new Certificate[]{
                KeyStoreTestUtil.generateCertificate("CN=foo.com",
                    KeyStoreTestUtil.generateKeyPair("RSA"), 30, "SHA1withRSA")
            };
            return certChain;
          }
        });
    assertTrue(verifier.verify("foo.com", sslSession));
  }

  @Test
  void testCreateHostnameVerifierRealCertBad() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    Mockito.when(sslSession.getPeerCertificates()).thenAnswer(
        new Answer<Certificate[]>() {
          @Override
          public Certificate[] answer(InvocationOnMock invocation)
              throws Throwable {
            // "real" cert
            Certificate[] certChain = new Certificate[]{
                KeyStoreTestUtil.generateCertificate("CN=foo.com",
                    KeyStoreTestUtil.generateKeyPair("RSA"), 30, "SHA1withRSA")
            };
            return certChain;
          }
        });
    assertFalse(verifier.verify("bar.com", sslSession));
  }

  private void checkCACert(X509Certificate caCert) {
    assertEquals(caCert.getSubjectX500Principal().toString(),
        caCert.getIssuerDN().toString());
    assertEquals(caCert.getSubjectX500Principal().toString(),
        caCert.getSubjectDN().toString());
    assertTrue(caCert.getSubjectX500Principal().toString().startsWith("OU=YARN-"),
        "Expected CA certificate X500 Principal to start with" +
            " 'OU=YARN-', but did not: " + caCert.getSubjectX500Principal());
    assertFalse(caCert.getSubjectX500Principal().toString().contains(","),
        "Found multiple fields in X500 Principal, when there " +
            "should have only been one: " + caCert.getSubjectX500Principal());
    assertEquals("SHA512withRSA", caCert.getSigAlgName());
    assertEquals(
        new GregorianCalendar(2037, Calendar.DECEMBER, 31).getTime(),
        caCert.getNotAfter());
    assertTrue(caCert.getNotBefore().before(new Date()),
        "Expected certificate to have started but was not: " + caCert.getNotBefore());
    assertEquals(0, caCert.getBasicConstraints());
  }

  private void checkPrivatePublicKeys(PrivateKey privateKey,
      PublicKey publicKey) throws NoSuchAlgorithmException, InvalidKeyException,
      SignatureException {
    byte[] data = new byte[2000];
    new Random().nextBytes(data);
    Signature signer = Signature.getInstance("SHA512withRSA");
    signer.initSign(privateKey);
    signer.update(data);
    byte[] sig = signer.sign();
    signer = Signature.getInstance("SHA512withRSA");
    signer.initVerify(publicKey);
    signer.update(data);
    assertTrue(signer.verify(sig));
  }

  private X509Certificate[] castCertificateArrayToX509CertificateArray(
      Certificate[] certs) {
    return Arrays.copyOf(certs, certs.length, X509Certificate[].class);
  }

  private static class CertKeyPair {
    private X509Certificate cert;
    private KeyPair keyPair;

    public CertKeyPair(X509Certificate cert, KeyPair keyPair) {
      this.cert = cert;
      this.keyPair = keyPair;
    }

    public X509Certificate getCert() {
      return cert;
    }

    public KeyPair getKeyPair() {
      return keyPair;
    }
  }

  private CertKeyPair createCertAndKeyPair() throws Exception {
    // Re-use a ProxyCA to generate a valid Certificate and KeyPair
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    return new CertKeyPair(proxyCA.getCaCert(), proxyCA.getCaKeyPair());
  }
}
