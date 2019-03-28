/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.webproxy;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.UUID;

/**
 * Allows for the generation and acceptance of specialized HTTPS Certificates to
 * be used for HTTPS communication between the AMs and the RM Proxy.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProxyCA {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyCA.class);

  private X509Certificate caCert;
  private KeyPair caKeyPair;
  private KeyStore childTrustStore;
  private final Random srand;
  private X509TrustManager defaultTrustManager;
  private X509KeyManager x509KeyManager;
  private HostnameVerifier hostnameVerifier;
  private static final AlgorithmIdentifier SIG_ALG_ID =
      new DefaultSignatureAlgorithmIdentifierFinder().find("SHA512WITHRSA");

  public ProxyCA() {
    srand = new SecureRandom();

    // This only has to be done once
    Security.addProvider(new BouncyCastleProvider());
  }

  public void init() throws GeneralSecurityException, IOException {
    createCACertAndKeyPair();
    initInternal();
  }

  public void init(X509Certificate caCert, PrivateKey caPrivateKey)
      throws GeneralSecurityException, IOException {
    if (caCert == null || caPrivateKey == null
        || !verifyCertAndKeys(caCert, caPrivateKey)) {
      LOG.warn("Could not verify Certificate, Public Key, and Private Key: " +
          "regenerating");
      createCACertAndKeyPair();
    } else {
      this.caCert = caCert;
      this.caKeyPair = new KeyPair(caCert.getPublicKey(), caPrivateKey);
    }
    initInternal();
  }

  private void initInternal() throws GeneralSecurityException, IOException {
    defaultTrustManager = null;
    TrustManagerFactory factory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
    factory.init((KeyStore) null);
    for (TrustManager manager : factory.getTrustManagers()) {
      if (manager instanceof X509TrustManager) {
        defaultTrustManager = (X509TrustManager) manager;
        break;
      }
    }
    if (defaultTrustManager == null) {
      throw new YarnRuntimeException(
          "Could not find default X509 Trust Manager");
    }

    this.x509KeyManager = createKeyManager();
    this.hostnameVerifier = createHostnameVerifier();
    this.childTrustStore = createTrustStore("client", caCert);
  }

  private X509Certificate createCert(boolean isCa, String issuerStr,
      String subjectStr, Date from, Date to, PublicKey publicKey,
      PrivateKey privateKey) throws GeneralSecurityException, IOException {
    X500Name issuer = new X500Name(issuerStr);
    X500Name subject = new X500Name(subjectStr);
    SubjectPublicKeyInfo subPubKeyInfo =
        SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
    X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
        issuer, new BigInteger(64, srand), from, to, subject, subPubKeyInfo);
    AlgorithmIdentifier digAlgId =
        new DefaultDigestAlgorithmIdentifierFinder().find(SIG_ALG_ID);
    ContentSigner contentSigner;
    try {
      contentSigner = new BcRSAContentSignerBuilder(SIG_ALG_ID, digAlgId)
          .build(PrivateKeyFactory.createKey(privateKey.getEncoded()));
    } catch (OperatorCreationException oce) {
      throw new GeneralSecurityException(oce);
    }
    if (isCa) {
      // BasicConstraints(0) indicates a CA and a path length of 0.  This is
      // important to indicate that child certificates can't issue additional
      // grandchild certificates
      certBuilder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(0));
    } else {
      // BasicConstraints(false) indicates this is not a CA
      certBuilder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(false));
      certBuilder.addExtension(Extension.authorityKeyIdentifier, false,
          new JcaX509ExtensionUtils().createAuthorityKeyIdentifier(caCert));
    }
    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
    X509Certificate cert = new JcaX509CertificateConverter().setProvider("BC")
        .getCertificate(certHolder);
    LOG.info("Created Certificate for {}", subject);
    return cert;
  }

  private void createCACertAndKeyPair()
      throws GeneralSecurityException, IOException {
    Date from = new Date();
    Date to = new GregorianCalendar(2037, Calendar.DECEMBER, 31).getTime();
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    caKeyPair = keyGen.genKeyPair();
    String subject = "OU=YARN-" + UUID.randomUUID();
    caCert = createCert(true, subject, subject, from, to,
        caKeyPair.getPublic(), caKeyPair.getPrivate());
    LOG.debug("CA Certificate: \n{}", caCert);
  }

  public byte[] createChildKeyStore(ApplicationId appId, String ksPassword)
      throws Exception {
    // We don't check the expiration date, and this will provide further reason
    // for outside users to not accept these certificates
    Date from = new Date();
    Date to = from;
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    KeyPair keyPair = keyGen.genKeyPair();
    String issuer = caCert.getSubjectX500Principal().getName();
    String subject = "CN=" + appId;
    X509Certificate cert = createCert(false, issuer, subject, from, to,
        keyPair.getPublic(), caKeyPair.getPrivate());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Certificate for {}: \n{}", appId, cert);
    }

    KeyStore keyStore = createChildKeyStore(ksPassword, "server",
        keyPair.getPrivate(), cert);
    return keyStoreToBytes(keyStore, ksPassword);
  }

  public byte[] getChildTrustStore(String password)
      throws GeneralSecurityException, IOException {
    return keyStoreToBytes(childTrustStore, password);
  }

  private KeyStore createEmptyKeyStore()
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private KeyStore createChildKeyStore(String password, String alias,
      Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(),
        new Certificate[]{cert, caCert});
    return ks;
  }

  public String generateKeyStorePassword() {
    return RandomStringUtils.random(16, 0, 0, true, true, null, srand);
  }

  private byte[] keyStoreToBytes(KeyStore ks, String password)
      throws GeneralSecurityException, IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      ks.store(out, password.toCharArray());
      return out.toByteArray();
    }
  }

  private KeyStore createTrustStore(String alias, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    return ks;
  }

  public SSLContext createSSLContext(ApplicationId appId)
      throws GeneralSecurityException {
    // We need the normal TrustManager, plus our custom one.  While the
    // SSLContext accepts an array of TrustManagers, the docs indicate that only
    // the first instance of any particular implementation type is used
    // (e.g. X509KeyManager) - this means that simply putting both TrustManagers
    // in won't work.  We need to have ours do both.
    TrustManager[] trustManagers = new TrustManager[] {
        createTrustManager(appId)};
    KeyManager[] keyManagers = new KeyManager[]{x509KeyManager};

    SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(keyManagers, trustManagers, new SecureRandom());
    return sc;
  }

  @VisibleForTesting
  X509TrustManager createTrustManager(ApplicationId appId) {
    return new X509TrustManager() {
      @Override
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return defaultTrustManager.getAcceptedIssuers();
      }

      @Override
      public void checkClientTrusted(
          java.security.cert.X509Certificate[] certs, String authType) {
        // not used
      }

      @Override
      public void checkServerTrusted(
          java.security.cert.X509Certificate[] certs, String authType)
          throws CertificateException {
        // Our certs will always have 2 in the chain, with 0 being the app's
        // cert and 1 being the RM's cert
        boolean issuedByRM = false;
        if (certs.length == 2) {
          try {
            // We can verify both certs using the CA cert's public key - the
            // child cert's info is not needed
            certs[0].verify(caKeyPair.getPublic());
            certs[1].verify(caKeyPair.getPublic());
            issuedByRM = true;
          } catch (CertificateException | NoSuchAlgorithmException
              | InvalidKeyException | NoSuchProviderException
              | SignatureException e) {
            // Fall back to the default trust manager
            LOG.debug("Could not verify certificate with RM CA, falling " +
                "back to default", e);
            defaultTrustManager.checkServerTrusted(certs, authType);
          }
        } else {
          LOG.debug("Certificate not issued by RM CA, falling back to " +
              "default");
          defaultTrustManager.checkServerTrusted(certs, authType);
        }
        if (issuedByRM) {
          // Check that it has the correct App ID
          if (!certs[0].getSubjectX500Principal().getName()
              .equals("CN=" + appId)) {
            throw new CertificateException(
                "Expected to find Subject X500 Principal with CN="
                    + appId + " but found "
                    + certs[0].getSubjectX500Principal().getName());
          }
          LOG.debug("Verified certificate signed by RM CA");
        }
      }
    };
  }

  @VisibleForTesting
  X509KeyManager getX509KeyManager() {
    return x509KeyManager;
  }

  private X509KeyManager createKeyManager() {
    return new X509KeyManager() {
      @Override
      public String[] getClientAliases(String s, Principal[] principals) {
        return new String[]{"client"};
      }

      @Override
      public String chooseClientAlias(String[] strings,
          Principal[] principals, Socket socket) {
        return "client";
      }

      @Override
      public String[] getServerAliases(String s, Principal[] principals) {
        return null;
      }

      @Override
      public String chooseServerAlias(String s, Principal[] principals,
          Socket socket) {
        return null;
      }

      @Override
      public X509Certificate[] getCertificateChain(String s) {
        return new X509Certificate[]{caCert};
      }

      @Override
      public PrivateKey getPrivateKey(String s) {
        return caKeyPair.getPrivate();
      }
    };
  }

  public HostnameVerifier getHostnameVerifier() {
    return hostnameVerifier;
  }

  private HostnameVerifier createHostnameVerifier() {
    HostnameVerifier defaultHostnameVerifier =
        new DefaultHostnameVerifier(PublicSuffixMatcherLoader.getDefault());
    return new HostnameVerifier() {
      @Override
      public boolean verify(String host, SSLSession sslSession) {
        try {
          Certificate[] certs = sslSession.getPeerCertificates();
          if (certs.length == 2) {
            // Make sure this is one of our certs.  More thorough checking would
            // have already been done by the SSLContext
            certs[0].verify(caKeyPair.getPublic());
            LOG.debug("Verified certificate signed by RM CA, " +
                "skipping hostname verification");
            return true;
          }
        } catch (SSLPeerUnverifiedException e) {
          // No certificate
          return false;
        } catch (CertificateException | NoSuchAlgorithmException
            | InvalidKeyException | SignatureException
            | NoSuchProviderException e) {
          // fall back to normal verifier below
          LOG.debug("Could not verify certificate with RM CA, " +
              "falling back to default hostname verification", e);
        }
        return defaultHostnameVerifier.verify(host, sslSession);
      }
    };
  }

  @VisibleForTesting
  void setDefaultTrustManager(X509TrustManager trustManager) {
    this.defaultTrustManager = trustManager;
  }

  @VisibleForTesting
  public X509Certificate getCaCert() {
    return caCert;
  }

  @VisibleForTesting
  public KeyPair getCaKeyPair() {
    return caKeyPair;
  }

  private boolean verifyCertAndKeys(X509Certificate cert,
      PrivateKey privateKey) throws GeneralSecurityException {
    PublicKey publicKey = cert.getPublicKey();
    byte[] data = new byte[2000];
    srand.nextBytes(data);
    Signature signer = Signature.getInstance("SHA512withRSA");
    signer.initSign(privateKey);
    signer.update(data);
    byte[] sig = signer.sign();
    signer = Signature.getInstance("SHA512withRSA");
    signer.initVerify(publicKey);
    signer.update(data);
    return signer.verify(sig);
  }
}
