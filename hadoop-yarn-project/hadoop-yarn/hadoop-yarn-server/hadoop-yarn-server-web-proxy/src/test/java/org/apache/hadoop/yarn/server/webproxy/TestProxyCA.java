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

import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import java.security.InvalidKeyException;
import java.security.Key;
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

public class TestProxyCA {

  @Test
  public void testInit() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    Assert.assertNull(proxyCA.getCaCert());
    Assert.assertNull(proxyCA.getCaKeyPair());
    Assert.assertNull(proxyCA.getX509KeyManager());
    Assert.assertNull(proxyCA.getHostnameVerifier());

    proxyCA.init();
    Assert.assertNotNull(proxyCA.getCaCert());
    Assert.assertNotNull(proxyCA.getCaKeyPair());
    Assert.assertNotNull(proxyCA.getX509KeyManager());
    Assert.assertNotNull(proxyCA.getHostnameVerifier());
  }

  @Test
  public void testCreateChildKeyStore() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    byte[] keystoreBytes = proxyCA.createChildKeyStore(appId,
        "password");
    KeyStore keyStore = KeyStoreTestUtil.bytesToKeyStore(keystoreBytes,
        "password");
    Assert.assertEquals(1, keyStore.size());
    Certificate[] certChain = keyStore.getCertificateChain("server");
    Assert.assertEquals(2, certChain.length);
    X509Certificate caCert = (X509Certificate) certChain[1];
    X509Certificate cert = (X509Certificate) certChain[0];

    // check child cert
    Assert.assertEquals(caCert.getSubjectX500Principal().toString(),
        cert.getIssuerDN().toString());
    Assert.assertEquals(new X500Principal("CN=" + appId),
        cert.getSubjectX500Principal());
    Assert.assertFalse("Found multiple fields in X500 Principal, when there " +
            "should have only been one: " + cert.getSubjectX500Principal(),
        cert.getSubjectX500Principal().toString().contains(","));
    Assert.assertEquals("SHA512withRSA", cert.getSigAlgName());
    Assert.assertEquals(cert.getNotBefore(), cert.getNotAfter());
    Assert.assertTrue("Expected certificate to be expired but was not: "
        + cert.getNotAfter(), cert.getNotAfter().before(new Date()));
    Assert.assertEquals(new X500Principal("CN=" + appId).toString(),
        cert.getSubjectDN().toString());
    Key privateKey = keyStore.getKey("server", "password".toCharArray());
    Assert.assertEquals("RSA", privateKey.getAlgorithm());
    Assert.assertEquals(-1, cert.getBasicConstraints());

    // verify signature on child cert
    PublicKey caPublicKey = caCert.getPublicKey();
    cert.verify(caPublicKey);

    // check CA cert
    checkCACert(caCert);
    Assert.assertEquals(proxyCA.getCaCert(), caCert);

    // verify signature on CA cert
    caCert.verify(caPublicKey);

    // verify CA public key matches private key
    PrivateKey caPrivateKey =
        proxyCA.getX509KeyManager().getPrivateKey(null);
    checkPrivatePublicKeys(caPrivateKey, caPublicKey);
    Assert.assertEquals(proxyCA.getCaKeyPair().getPublic(), caPublicKey);
    Assert.assertEquals(proxyCA.getCaKeyPair().getPrivate(), caPrivateKey);
  }

  @Test
  public void testGetChildTrustStore() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    byte[] truststoreBytes = proxyCA.getChildTrustStore("password");
    KeyStore truststore = KeyStoreTestUtil.bytesToKeyStore(truststoreBytes,
        "password");
    Assert.assertEquals(1, truststore.size());
    X509Certificate caCert =
        (X509Certificate) truststore.getCertificate("client");

    // check CA cert
    checkCACert(caCert);
    Assert.assertEquals(proxyCA.getCaCert(), caCert);

    // verify signature on CA cert
    PublicKey caPublicKey = caCert.getPublicKey();
    caCert.verify(caPublicKey);

    // verify CA public key matches private key
    PrivateKey caPrivateKey =
        proxyCA.getX509KeyManager().getPrivateKey(null);
    checkPrivatePublicKeys(caPrivateKey, caPublicKey);
    Assert.assertEquals(proxyCA.getCaKeyPair().getPublic(), caPublicKey);
    Assert.assertEquals(proxyCA.getCaKeyPair().getPrivate(), caPrivateKey);
  }

  @Test
  public void testGenerateKeyStorePassword() throws Exception {
    // We can't possibly test every possible string, but we can at least verify
    // a few things about a few of the generated strings as a sanity check
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    Set<String> passwords = new HashSet<>();

    for (int i = 0; i < 5; i++) {
      String password = proxyCA.generateKeyStorePassword();
      Assert.assertEquals(16, password.length());
      for (char c : password.toCharArray()) {
        Assert.assertFalse("Found character '" + c + "' in password '"
            + password + "' which is outside of the expected range", c < ' ');
        Assert.assertFalse("Found character '" + c + "' in password '"
            + password + "' which is outside of the expected range", c > 'z');
      }
      Assert.assertFalse("Password " + password
          + " was generated twice, which is _extremely_ unlikely"
          + " and shouldn't practically happen: " + passwords,
          passwords.contains(password));
      passwords.add(password);
    }
  }

  @Test
  public void testCreateTrustManagerDefaultTrustManager() throws Exception {
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

    Assert.assertArrayEquals(defaultTrustManager.getAcceptedIssuers(),
        trustManager.getAcceptedIssuers());
    trustManager.checkClientTrusted(null, null);
  }

  @Test
  public void testCreateTrustManagerYarnCert() throws Exception {
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
  public void testCreateTrustManagerWrongApp() throws Exception {
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
      Assert.fail("Should have thrown a CertificateException, but did not");
    } catch (CertificateException ce) {
      Assert.assertEquals("Expected to find Subject X500 Principal with CN=" +
          appId + " but found CN=" + appId2, ce.getMessage());
    }
  }

  @Test
  public void testCreateTrustManagerWrongRM() throws Exception {
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
  public void testCreateTrustManagerRealCert() throws Exception {
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
  public void testCreateTrustManagerExceptions() throws Exception {
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
  public void testCreateKeyManager() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    X509KeyManager keyManager = proxyCA.getX509KeyManager();

    Assert.assertArrayEquals(new String[]{"client"},
        keyManager.getClientAliases(null, null));
    Assert.assertEquals("client",
        keyManager.chooseClientAlias(null, null, null));
    Assert.assertNull(keyManager.getServerAliases(null, null));
    Assert.assertNull(keyManager.chooseServerAlias(null, null, null));

    byte[] truststoreBytes = proxyCA.getChildTrustStore("password");
    KeyStore truststore = KeyStoreTestUtil.bytesToKeyStore(truststoreBytes,
        "password");
    Assert.assertEquals(1, truststore.size());
    X509Certificate caCert =
        (X509Certificate) truststore.getCertificate("client");
    Assert.assertArrayEquals(new X509Certificate[]{caCert},
        keyManager.getCertificateChain(null));
    Assert.assertEquals(proxyCA.getCaCert(), caCert);

    PrivateKey caPrivateKey = keyManager.getPrivateKey(null);
    PublicKey caPublicKey = caCert.getPublicKey();
    checkPrivatePublicKeys(caPrivateKey, caPublicKey);
    Assert.assertEquals(proxyCA.getCaKeyPair().getPublic(), caPublicKey);
    Assert.assertEquals(proxyCA.getCaKeyPair().getPrivate(), caPrivateKey);
  }

  @Test
  public void testCreateHostnameVerifier() throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    Mockito.when(sslSession.getPeerCertificates()).thenReturn(
        KeyStoreTestUtil.bytesToKeyStore(
            proxyCA.createChildKeyStore(
                ApplicationId.newInstance(System.currentTimeMillis(), 1),
                "password"), "password").getCertificateChain("server"));
    Assert.assertTrue(verifier.verify("foo", sslSession));
  }

  @Test
  public void testCreateHostnameVerifierSSLPeerUnverifiedException()
      throws Exception {
    ProxyCA proxyCA = new ProxyCA();
    proxyCA.init();
    HostnameVerifier verifier = proxyCA.getHostnameVerifier();

    SSLSession sslSession = Mockito.mock(SSLSession.class);
    Mockito.when(sslSession.getPeerCertificates()).thenThrow(
        new SSLPeerUnverifiedException(""));
    Assert.assertFalse(verifier.verify("foo", sslSession));
  }

  @Test
  public void testCreateHostnameVerifierWrongRM() throws Exception {
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
    Assert.assertFalse(verifier.verify("foo", sslSession));
  }

  @Test
  public void testCreateHostnameVerifierExceptions() throws Exception {
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
      Assert.assertFalse(verifier.verify("foo", sslSession));
    }
  }

  @Test
  public void testCreateHostnameVerifierRealCert() throws Exception {
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
    Assert.assertTrue(verifier.verify("foo.com", sslSession));
  }

  @Test
  public void testCreateHostnameVerifierRealCertBad() throws Exception {
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
    Assert.assertFalse(verifier.verify("bar.com", sslSession));
  }

  private void checkCACert(X509Certificate caCert) {
    Assert.assertEquals(caCert.getSubjectX500Principal().toString(),
        caCert.getIssuerDN().toString());
    Assert.assertEquals(caCert.getSubjectX500Principal().toString(),
        caCert.getSubjectDN().toString());
    Assert.assertTrue("Expected CA certificate X500 Principal to start with" +
            " 'OU=YARN-', but did not: " + caCert.getSubjectX500Principal(),
        caCert.getSubjectX500Principal().toString().startsWith("OU=YARN-"));
    Assert.assertFalse("Found multiple fields in X500 Principal, when there " +
            "should have only been one: " + caCert.getSubjectX500Principal(),
        caCert.getSubjectX500Principal().toString().contains(","));
    Assert.assertEquals("SHA512withRSA", caCert.getSigAlgName());
    Assert.assertEquals(
        new GregorianCalendar(2037, Calendar.DECEMBER, 31).getTime(),
        caCert.getNotAfter());
    Assert.assertTrue("Expected certificate to have started but was not: "
        + caCert.getNotBefore(), caCert.getNotBefore().before(new Date()));
    Assert.assertEquals(0, caCert.getBasicConstraints());
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
    Assert.assertTrue(signer.verify(sig));
  }

  private X509Certificate[] castCertificateArrayToX509CertificateArray(
      Certificate[] certs) {
    return Arrays.copyOf(certs, certs.length, X509Certificate[].class);
  }
}
