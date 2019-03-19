/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.client;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.cert.X509CertificateHolder;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertStore;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.*;

/**
 * Default Certificate client implementation. It provides certificate
 * operations that needs to be performed by certificate clients in the Ozone
 * eco-system.
 */
public abstract class DefaultCertificateClient implements CertificateClient {

  private static final String CERT_FILE_NAME_FORMAT = "%s.crt";
  private final Logger logger;
  private final SecurityConfig securityConfig;
  private final KeyCodec keyCodec;
  private PrivateKey privateKey;
  private PublicKey publicKey;
  private X509Certificate x509Certificate;
  private Map<String, X509Certificate> certificateMap;
  private String certSerialId;


  DefaultCertificateClient(SecurityConfig securityConfig, Logger log,
      String certSerialId) {
    Objects.requireNonNull(securityConfig);
    this.securityConfig = securityConfig;
    keyCodec = new KeyCodec(securityConfig);
    this.logger = log;
    this.certificateMap = new ConcurrentHashMap<>();
    this.certSerialId = certSerialId;

    loadAllCertificates();
  }

  /**
   * Load all certificates from configured location.
   * */
  private void loadAllCertificates() {
    // See if certs directory exists in file system.
    Path certPath = securityConfig.getCertificateLocation();
    if (Files.exists(certPath) && Files.isDirectory(certPath)) {
      getLogger().info("Loading certificate from location:{}.",
          certPath);
      File[] certFiles = certPath.toFile().listFiles();

      if (certFiles != null) {
        CertificateCodec certificateCodec =
            new CertificateCodec(securityConfig);
        for (File file : certFiles) {
          if (file.isFile()) {
            try {
              X509CertificateHolder x509CertificateHolder = certificateCodec
                  .readCertificate(certPath, file.getName());
              X509Certificate cert =
                  CertificateCodec.getX509Certificate(x509CertificateHolder);
              if (cert != null && cert.getSerialNumber() != null) {
                if (cert.getSerialNumber().toString().equals(certSerialId)) {
                  x509Certificate = cert;
                }
                certificateMap.putIfAbsent(cert.getSerialNumber().toString(),
                    cert);
                getLogger().info("Added certificate from file:{}.",
                    file.getAbsolutePath());
              } else {
                getLogger().error("Error reading certificate from file:{}",
                    file);
              }
            } catch (java.security.cert.CertificateException | IOException e) {
              getLogger().error("Error reading certificate from file:{}.",
                  file.getAbsolutePath(), e);
            }
          }
        }
      }
    }
  }

  /**
   * Returns the private key of the specified  if it exists on the local
   * system.
   *
   * @return private key or Null if there is no data.
   */
  @Override
  public PrivateKey getPrivateKey() {
    if (privateKey != null) {
      return privateKey;
    }

    Path keyPath = securityConfig.getKeyLocation();
    if (OzoneSecurityUtil.checkIfFileExist(keyPath,
        securityConfig.getPrivateKeyFileName())) {
      try {
        privateKey = keyCodec.readPrivateKey();
      } catch (InvalidKeySpecException | NoSuchAlgorithmException
          | IOException e) {
        getLogger().error("Error while getting private key.", e);
      }
    }
    return privateKey;
  }

  /**
   * Returns the public key of the specified if it exists on the local system.
   *
   * @return public key or Null if there is no data.
   */
  @Override
  public PublicKey getPublicKey() {
    if (publicKey != null) {
      return publicKey;
    }

    Path keyPath = securityConfig.getKeyLocation();
    if (OzoneSecurityUtil.checkIfFileExist(keyPath,
        securityConfig.getPublicKeyFileName())) {
      try {
        publicKey = keyCodec.readPublicKey();
      } catch (InvalidKeySpecException | NoSuchAlgorithmException
          | IOException e) {
        getLogger().error("Error while getting public key.", e);
      }
    }
    return publicKey;
  }

  /**
   * Returns the default certificate of given client if it exists.
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public X509Certificate getCertificate() {
    if (x509Certificate != null) {
      return x509Certificate;
    }

    if (certSerialId == null) {
      getLogger().error("Default certificate serial id is not set. Can't " +
          "locate the default certificate for this client.");
      return null;
    }
    // Refresh the cache from file system.
    loadAllCertificates();
    if (certificateMap.containsKey(certSerialId)) {
      x509Certificate = certificateMap.get(certSerialId);
    }
    return x509Certificate;
  }

  /**
   * Returns the certificate  with the specified certificate serial id if it
   * exists else try to get it from SCM.
   * @param  certId
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public X509Certificate getCertificate(String certId)
      throws CertificateException {
    // Check if it is in cache.
    if (certificateMap.containsKey(certId)) {
      return certificateMap.get(certId);
    }
    // Try to get it from SCM.
    return this.getCertificateFromScm(certId);
  }

  /**
   * Get certificate from SCM and store it in local file system.
   * @param certId
   * @return certificate
   */
  private X509Certificate getCertificateFromScm(String certId)
      throws CertificateException {

    getLogger().info("Getting certificate with certSerialId:{}.",
        certId);
    try {
      SCMSecurityProtocol scmSecurityProtocolClient = getScmSecurityClient(
          (OzoneConfiguration) securityConfig.getConfiguration());
      String pemEncodedCert =
          scmSecurityProtocolClient.getCertificate(certId);
      this.storeCertificate(pemEncodedCert, true);
      return CertificateCodec.getX509Certificate(pemEncodedCert);
    } catch (Exception e) {
      getLogger().error("Error while getting Certificate with " +
          "certSerialId:{} from scm.", certId, e);
      throw new CertificateException("Error while getting certificate for " +
          "certSerialId:" + certId, e, CERTIFICATE_ERROR);
    }
  }

  /**
   * Verifies if this certificate is part of a trusted chain.
   *
   * @param certificate - certificate.
   * @return true if it trusted, false otherwise.
   */
  @Override
  public boolean verifyCertificate(X509Certificate certificate) {
    throw new UnsupportedOperationException("Operation not supported.");
  }

  /**
   * Creates digital signature over the data stream using the s private key.
   *
   * @param stream - Data stream to sign.
   * @throws CertificateException - on Error.
   */
  @Override
  public byte[] signDataStream(InputStream stream)
      throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initSign(getPrivateKey());
      byte[] buffer = new byte[1024 * 4];

      int len;
      while (-1 != (len = stream.read(buffer))) {
        sign.update(buffer, 0, len);
      }
      return sign.sign();
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException | IOException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGN_ERROR);
    }
  }

  /**
   * Creates digital signature over the data stream using the s private key.
   *
   * @param data - Data to sign.
   * @throws CertificateException - on Error.
   */
  @Override
  public byte[] signData(byte[] data) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());

      sign.initSign(getPrivateKey());
      sign.update(data);

      return sign.sign();
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGN_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param stream - Data Stream.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  @Override
  public boolean verifySignature(InputStream stream, byte[] signature,
      X509Certificate cert) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initVerify(cert);
      byte[] buffer = new byte[1024 * 4];

      int len;
      while (-1 != (len = stream.read(buffer))) {
        sign.update(buffer, 0, len);
      }
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException | IOException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  @Override
  public boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initVerify(cert);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param pubKey - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  private boolean verifySignature(byte[] data, byte[] signature,
      PublicKey pubKey) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initVerify(pubKey);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
        .setConfiguration(securityConfig.getConfiguration());
    try {
      DomainValidator validator = DomainValidator.getInstance();
      // Add all valid ips.
      OzoneSecurityUtil.getValidInetsForCurrentHost().forEach(
          ip -> {
            builder.addIpAddress(ip.getHostAddress());
            if(validator.isValid(ip.getCanonicalHostName())) {
              builder.addDnsName(ip.getCanonicalHostName());
            }
          });
    } catch (IOException e) {
      throw new CertificateException("Error while adding ip to CSR builder",
          e, CSR_ERROR);
    }
    return builder;
  }

  /**
   * Get the certificate of well-known entity from SCM.
   *
   * @param query - String Query, please see the implementation for the
   * discussion on the query formats.
   * @return X509Certificate or null if not found.
   */
  @Override
  public X509Certificate queryCertificate(String query) {
    // TODO:
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * Stores the Certificate  for this client. Don't use this api to add trusted
   * certificates of others.
   *
   * @param pemEncodedCert - pem encoded X509 Certificate
   * @param force - override any existing file
   * @throws CertificateException - on Error.
   *
   */
  @Override
  public void storeCertificate(String pemEncodedCert, boolean force)
      throws CertificateException {
    CertificateCodec certificateCodec = new CertificateCodec(securityConfig);
    try {
      Path basePath = securityConfig.getCertificateLocation();

      X509Certificate cert =
          CertificateCodec.getX509Certificate(pemEncodedCert);
      String certName = String.format(CERT_FILE_NAME_FORMAT,
          cert.getSerialNumber().toString());

      certificateCodec.writeCertificate(basePath, certName,
          pemEncodedCert, force);
      certificateMap.putIfAbsent(cert.getSerialNumber().toString(), cert);
    } catch (IOException | java.security.cert.CertificateException e) {
      throw new CertificateException("Error while storing certificate.", e,
          CERTIFICATE_ERROR);
    }
  }

  /**
   * Stores the trusted chain of certificates for a specific .
   *
   * @param ks - Key Store.
   * @throws CertificateException - on Error.
   */
  @Override
  public synchronized void storeTrustChain(CertStore ks)
      throws CertificateException {
    throw new UnsupportedOperationException("Operation not supported.");
  }


  /**
   * Stores the trusted chain of certificates for a specific .
   *
   * @param certificates - List of Certificates.
   * @throws CertificateException - on Error.
   */
  @Override
  public synchronized void storeTrustChain(List<X509Certificate> certificates)
      throws CertificateException {
    throw new UnsupportedOperationException("Operation not supported.");
  }

  /**
   * Defines 8 cases of initialization.
   * Each case specifies objects found.
   * 0. NONE                  Keypair as well as certificate not found.
   * 1. CERT                  Certificate found but keypair missing.
   * 2. PUBLIC_KEY            Public key found but private key and
   *                          certificate is missing.
   * 3. PUBLICKEY_CERT        Only public key and certificate is present.
   * 4. PRIVATE_KEY           Only private key is present.
   * 5. PRIVATEKEY_CERT       Only private key and certificate is present.
   * 6. PUBLICKEY_PRIVATEKEY  indicates private and public key were read
   *                          successfully from configured location but
   *                          Certificate.
   * 7. All                   Keypair as well as certificate is present.
   *
   * */
  protected enum InitCase {
    NONE,
    CERT,
    PUBLIC_KEY,
    PUBLICKEY_CERT,
    PRIVATE_KEY,
    PRIVATEKEY_CERT,
    PUBLICKEY_PRIVATEKEY,
    ALL
  }

  /**
   *
   * Initializes client by performing following actions.
   * 1. Create key dir if not created already.
   * 2. Generates and stores a keypair.
   * 3. Try to recover public key if private key and certificate is present
   *    but public key is missing.
   *
   * Truth table:
   *  +--------------+-----------------+--------------+----------------+
   *  | Private Key  | Public Keys     | Certificate  |   Result       |
   *  +--------------+-----------------+--------------+----------------+
   *  | False  (0)   | False   (0)     | False  (0)   |   GETCERT  000 |
   *  | False  (0)   | False   (0)     | True   (1)   |   FAILURE  001 |
   *  | False  (0)   | True    (1)     | False  (0)   |   FAILURE  010 |
   *  | False  (0)   | True    (1)     | True   (1)   |   FAILURE  011 |
   *  | True   (1)   | False   (0)     | False  (0)   |   FAILURE  100 |
   *  | True   (1)   | False   (0)     | True   (1)   |   SUCCESS  101 |
   *  | True   (1)   | True    (1)     | False  (0)   |   GETCERT  110 |
   *  | True   (1)   | True    (1)     | True   (1)   |   SUCCESS  111 |
   *  +--------------+-----------------+--------------+----------------+
   *
   * @return InitResponse
   * Returns FAILURE in following cases:
   * 1. If private key is missing but public key or certificate is available.
   * 2. If public key and certificate is missing.
   *
   * Returns SUCCESS in following cases:
   * 1. If keypair as well certificate is available.
   * 2. If private key and certificate is available and public key is
   *    recovered successfully.
   *
   * Returns GETCERT in following cases:
   * 1. First time when keypair and certificate is not available, keypair
   *    will be generated and stored at configured location.
   * 2. When keypair (public/private key) is available but certificate is
   *    missing.
   *
   */
  @Override
  public synchronized InitResponse init() throws CertificateException {
    int initCase = 0;
    PrivateKey pvtKey= getPrivateKey();
    PublicKey pubKey = getPublicKey();
    X509Certificate certificate = getCertificate();

    if(pvtKey != null){
      initCase = initCase | 1<<2;
    }
    if(pubKey != null){
      initCase = initCase | 1<<1;
    }
    if(certificate != null){
      initCase = initCase | 1;
    }
    getLogger().info("Certificate client init case: {}", initCase);
    Preconditions.checkArgument(initCase < 8, "Not a " +
        "valid case.");
    InitCase init = InitCase.values()[initCase];
    return handleCase(init);
  }

  /**
   * Default handling of each {@link InitCase}.
   * */
  protected InitResponse handleCase(InitCase init)
      throws CertificateException {
    switch (init) {
    case NONE:
      getLogger().info("Creating keypair for client as keypair and " +
          "certificate not found.");
      bootstrapClientKeys();
      return GETCERT;
    case CERT:
      getLogger().error("Private key not found, while certificate is still" +
          " present. Delete keypair and try again.");
      return FAILURE;
    case PUBLIC_KEY:
      getLogger().error("Found public key but private key and certificate " +
          "missing.");
      return FAILURE;
    case PRIVATE_KEY:
      getLogger().info("Found private key but public key and certificate " +
          "is missing.");
      // TODO: Recovering public key from private might be possible in some
      //  cases.
      return FAILURE;
    case PUBLICKEY_CERT:
      getLogger().error("Found public key and certificate but private " +
          "key is missing.");
      return FAILURE;
    case PRIVATEKEY_CERT:
      getLogger().info("Found private key and certificate but public key" +
          " missing.");
      if (recoverPublicKey()) {
        return SUCCESS;
      } else {
        getLogger().error("Public key recovery failed.");
        return FAILURE;
      }
    case PUBLICKEY_PRIVATEKEY:
      getLogger().info("Found private and public key but certificate is" +
          " missing.");
      if (validateKeyPair(getPublicKey())) {
        return GETCERT;
      } else {
        getLogger().info("Keypair validation failed.");
        return FAILURE;
      }
    case ALL:
      getLogger().info("Found certificate file along with KeyPair.");
      if (validateKeyPairAndCertificate()) {
        return SUCCESS;
      } else {
        return FAILURE;
      }
    default:
      getLogger().error("Unexpected case: {} (private/public/cert)",
          Integer.toBinaryString(init.ordinal()));

      return FAILURE;
    }
  }

  /**
   * Validate keypair and certificate.
   * */
  protected boolean validateKeyPairAndCertificate() throws
      CertificateException {
    if (validateKeyPair(getPublicKey())) {
      getLogger().info("Keypair validated.");
      // TODO: Certificates cryptographic validity can be checked as well.
      if (validateKeyPair(getCertificate().getPublicKey())) {
        getLogger().info("Keypair validated with certificate.");
      } else {
        getLogger().error("Stored certificate is generated with different " +
            "private key.");
        return false;
      }
    } else {
      getLogger().error("Keypair validation failed.");
      return false;
    }
    return true;
  }

  /**
   * Tries to recover public key from certificate. Also validates recovered
   * public key.
   * */
  protected boolean recoverPublicKey() throws CertificateException {
    PublicKey pubKey = getCertificate().getPublicKey();
    try {

      if(validateKeyPair(pubKey)){
        keyCodec.writePublicKey(pubKey);
        publicKey = pubKey;
      } else {
        getLogger().error("Can't recover public key " +
            "corresponding to private key.", BOOTSTRAP_ERROR);
        return false;
      }
    } catch (IOException e) {
      throw new CertificateException("Error while trying to recover " +
          "public key.", e, BOOTSTRAP_ERROR);
    }
    return true;
  }

  /**
   * Validates public and private key of certificate client.
   *
   * @param pubKey
   * */
  protected boolean validateKeyPair(PublicKey pubKey)
      throws CertificateException {
    byte[] challenge = RandomStringUtils.random(1000).getBytes(
        StandardCharsets.UTF_8);
    byte[]  sign = signDataStream(new ByteArrayInputStream(challenge));
    return verifySignature(challenge, sign, pubKey);
  }

  /**
   * Bootstrap the client by creating keypair and storing it in configured
   * location.
   * */
  protected void bootstrapClientKeys() throws CertificateException {
    Path keyPath = securityConfig.getKeyLocation();
    if (Files.notExists(keyPath)) {
      try {
        Files.createDirectories(keyPath);
      } catch (IOException e) {
        throw new CertificateException("Error while creating directories " +
            "for certificate storage.", BOOTSTRAP_ERROR);
      }
    }
    KeyPair keyPair = createKeyPair();
    privateKey = keyPair.getPrivate();
    publicKey = keyPair.getPublic();
  }

  protected KeyPair createKeyPair() throws CertificateException {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = null;
    try {
      keyPair = keyGenerator.generateKey();
      keyCodec.writePublicKey(keyPair.getPublic());
      keyCodec.writePrivateKey(keyPair.getPrivate());
    } catch (NoSuchProviderException | NoSuchAlgorithmException
        | IOException e) {
      getLogger().error("Error while bootstrapping certificate client.", e);
      throw new CertificateException("Error while bootstrapping certificate.",
          BOOTSTRAP_ERROR);
    }
    return keyPair;
  }

  public Logger getLogger() {
    return logger;
  }

  /**
   * Create a scm security client, used to get SCM signed certificate.
   *
   * @return {@link SCMSecurityProtocol}
   */
  private static SCMSecurityProtocol getScmSecurityClient(
      OzoneConfiguration conf) throws IOException {
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress scmSecurityProtoAdd =
        HddsUtils.getScmAddressForSecurityProtocol(conf);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        new SCMSecurityProtocolClientSideTranslatorPB(
            RPC.getProxy(SCMSecurityProtocolPB.class, scmVersion,
                scmSecurityProtoAdd, UserGroupInformation.getCurrentUser(),
                conf, NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmSecurityClient;
  }
}
