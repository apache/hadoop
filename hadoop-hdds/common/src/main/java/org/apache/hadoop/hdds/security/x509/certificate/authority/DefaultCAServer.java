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

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.*;

/**
 * The default CertificateServer used by SCM. This has no dependencies on any
 * external system, this allows us to bootstrap a CertificateServer from
 * Scratch.
 * <p>
 * Details =======
 * <p>
 * The Default CA server is one of the many possible implementations of an SCM
 * Certificate Authority.
 * <p>
 * A certificate authority needs the Root Certificates and its private key to
 * operate.  The init function of the DefaultCA Server detects four possible
 * states the System can be in.
 * <p>
 * 1.  Success - This means that the expected Certificates and Keys are in
 * place, and the CA was able to read those files into memory.
 * <p>
 * 2. Missing Keys - This means that private keys are missing. This is an error
 * state which SCM CA cannot recover from. The cluster might have been
 * initialized earlier and for some reason, we are not able to find the private
 * keys for the CA. Eventually we will have 2 ways to recover from this state,
 * first one is to copy the SCM CA private keys from a backup. Second one is to
 * rekey the whole cluster. Both of these are improvements we will support in
 * future.
 * <p>
 * 3. Missing Certificate - Similar to Missing Keys, but the root certificates
 * are missing.
 * <p>
 * 4. Initialize - We don't have keys or certificates. DefaultCA assumes that
 * this is a system bootup and will generate the keys and certificates
 * automatically.
 * <p>
 * The init() follows the following logic,
 * <p>
 * 1. Compute the Verification Status -- Success, Missing Keys, Missing Certs or
 * Initialize.
 * <p>
 * 2. ProcessVerificationStatus - Returns a Lambda, based on the Verification
 * Status.
 * <p>
 * 3. Invoke the Lambda function.
 * <p>
 * At the end of the init function, we have functional CA. This function can be
 * invoked as many times since we will regenerate the keys and certs only if
 * both of them are missing.
 */
public class DefaultCAServer implements CertificateServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultCAServer.class);
  private final String subject;
  private final String clusterID;
  private final String scmID;
  private String componentName = Paths.get("scm", "ca").toString();
  private Path caKeysPath;
  private Path caRootX509Path;
  private SecurityConfig config;
  /**
   * TODO: We will make these configurable in the future.
   */
  private PKIProfile profile;
  private CertificateApprover approver;
  private CertificateStore store;

  /**
   * Create an Instance of DefaultCAServer.
   *  @param subject - String Subject
   * @param clusterID - String ClusterID
   * @param scmID - String SCMID.
   * @param certificateStore - A store used to persist Certificates.
   */
  public DefaultCAServer(String subject, String clusterID, String scmID,
                         CertificateStore certificateStore) {
    this.subject = subject;
    this.clusterID = clusterID;
    this.scmID = scmID;
    this.store = certificateStore;
  }

  @Override
  public void init(SecurityConfig securityConfig, CAType type)
      throws SCMSecurityException {
    caKeysPath = securityConfig.getKeyLocation(componentName);
    caRootX509Path = securityConfig.getCertificateLocation(componentName);
    this.config = securityConfig;

    // TODO: Make these configurable and load different profiles based on
    // config.
    profile = new DefaultProfile();
    this.approver = new DefaultApprover(profile, this.config);

    /* In future we will spilt this code to have different kind of CAs.
     * Right now, we have only self-signed CertificateServer.
     */

    if (type == CAType.SELF_SIGNED_CA) {
      VerificationStatus status = verifySelfSignedCA(securityConfig);
      Consumer<SecurityConfig> caInitializer =
          processVerificationStatus(status);
      caInitializer.accept(securityConfig);
      return;
    }

    LOG.error("We support only Self-Signed CAs for now.");
    throw new IllegalStateException("Not implemented functionality requested.");
  }

  @Override
  public X509CertificateHolder getCACertificate() throws IOException {
    CertificateCodec certificateCodec =
        new CertificateCodec(config, componentName);
    try {
      return certificateCodec.readCertificate();
    } catch (CertificateException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns the Certificate corresponding to given certificate serial id if
   * exist. Return null if it doesn't exist.
   *
   * @param certSerialId         - Certificate for this CA.
   * @return X509CertificateHolder
   * @throws CertificateException - usually thrown if this CA is not
   * initialized.
   * @throws IOException - on Error.
   */
  @Override
  public X509Certificate getCertificate(String certSerialId) throws
      IOException {
    return store.getCertificateByID(new BigInteger(certSerialId),
        CertificateStore.CertType.VALID_CERTS);
  }

  private KeyPair getCAKeys() throws IOException {
    KeyCodec keyCodec = new KeyCodec(config, componentName);
    try {
      return new KeyPair(keyCodec.readPublicKey(), keyCodec.readPrivateKey());
    } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Future<X509CertificateHolder> requestCertificate(
      PKCS10CertificationRequest csr,
      CertificateApprover.ApprovalType approverType) {
    LocalDate beginDate = LocalDate.now().atStartOfDay().toLocalDate();
    LocalDateTime temp = LocalDateTime.of(beginDate, LocalTime.MIDNIGHT);
    LocalDate endDate =
        temp.plus(config.getDefaultCertDuration()).toLocalDate();

    CompletableFuture<X509CertificateHolder> xcertHolder =
        approver.inspectCSR(csr);

    if(xcertHolder.isCompletedExceptionally()) {
      // This means that approver told us there are things which it disagrees
      // with in this Certificate Request. Since the first set of sanity
      // checks failed, we just return the future object right here.
      return xcertHolder;
    }
    try {
      switch (approverType) {
      case MANUAL:
        xcertHolder.completeExceptionally(new SCMSecurityException("Manual " +
            "approval is not yet implemented."));
        break;
      case KERBEROS_TRUSTED:
      case TESTING_AUTOMATIC:
        X509CertificateHolder xcert;
        try {
          xcert = signAndStoreCertificate(beginDate, endDate, csr);
        } catch (SCMSecurityException e) {
          // Certificate with conflicting serial id, retry again may resolve
          // this issue.
          LOG.error("Certificate storage failed, retrying one more time.", e);
          xcert = signAndStoreCertificate(beginDate, endDate, csr);
        }

        xcertHolder.complete(xcert);
        break;
      default:
        return null; // cannot happen, keeping checkstyle happy.
      }
    } catch (CertificateException | IOException | OperatorCreationException e) {
      LOG.error("Unable to issue a certificate. {}", e);
      xcertHolder.completeExceptionally(new SCMSecurityException(e));
    }
    return xcertHolder;
  }

  private X509CertificateHolder signAndStoreCertificate(LocalDate beginDate,
      LocalDate endDate, PKCS10CertificationRequest csr) throws IOException,
      OperatorCreationException, CertificateException {
    X509CertificateHolder xcert = approver.sign(config,
        getCAKeys().getPrivate(),
        getCACertificate(), java.sql.Date.valueOf(beginDate),
        java.sql.Date.valueOf(endDate), csr, scmID, clusterID);
    store.storeValidCertificate(xcert.getSerialNumber(),
        CertificateCodec.getX509Certificate(xcert));
    return xcert;
  }

  @Override
  public Future<X509CertificateHolder> requestCertificate(String csr,
      CertificateApprover.ApprovalType type) throws IOException {
    PKCS10CertificationRequest request =
        getCertificationRequest(csr);
    return requestCertificate(request, type);
  }

  @Override
  public Future<Boolean> revokeCertificate(X509Certificate certificate,
      CertificateApprover.ApprovalType approverType)
      throws SCMSecurityException {
    CompletableFuture<Boolean> revoked = new CompletableFuture<>();
    if (certificate == null) {
      revoked.completeExceptionally(new SCMSecurityException(
          "Certificate cannot be null"));
      return revoked;
    }
    try {
      store.revokeCertificate(certificate.getSerialNumber());
    } catch (IOException ex) {
      LOG.error("Revoking the certificate failed. {}", ex.getCause());
      throw new SCMSecurityException(ex);
    }
    return revoked;
  }

  /**
   * Generates a Self Signed CertificateServer. These are the steps in
   * generating a Self-Signed CertificateServer.
   * <p>
   * 1. Generate a Private/Public Key Pair. 2. Persist to a protected location.
   * 3. Generate a SelfSigned Root CertificateServer certificate.
   *
   * @param securityConfig - Config.
   */
  private void generateSelfSignedCA(SecurityConfig securityConfig) throws
      NoSuchAlgorithmException, NoSuchProviderException, IOException {
    KeyPair keyPair = generateKeys(securityConfig);
    generateRootCertificate(securityConfig, keyPair);
  }

  /**
   * Verify Self-Signed CertificateServer. 1. Check if the Certificate exist. 2.
   * Check if the key pair exists.
   *
   * @param securityConfig -- Config
   * @return Verification Status
   */
  private VerificationStatus verifySelfSignedCA(SecurityConfig securityConfig) {
    /*
    The following is the truth table for the States.
    True means we have that file False means it is missing.
    +--------------+--------+--------+--------------+
    | Certificates |  Keys  | Result |   Function   |
    +--------------+--------+--------+--------------+
    | True         | True   | True   | Success      |
    | False        | False  | True   | Initialize   |
    | True         | False  | False  | Missing Key  |
    | False        | True   | False  | Missing Cert |
    +--------------+--------+--------+--------------+

    This truth table maps to ~(certs xor keys) or certs == keys
     */
    boolean keyStatus = checkIfKeysExist();
    boolean certStatus = checkIfCertificatesExist();

    if ((certStatus == keyStatus) && (certStatus)) {
      return VerificationStatus.SUCCESS;
    }

    if ((certStatus == keyStatus) && (!certStatus)) {
      return VerificationStatus.INITIALIZE;
    }

    // At this point certStatus is not equal to keyStatus.
    if (certStatus) {
      return VerificationStatus.MISSING_KEYS;
    }

    return VerificationStatus.MISSING_CERTIFICATE;
  }

  /**
   * Returns Keys status.
   *
   * @return True if the key files exist.
   */
  private boolean checkIfKeysExist() {
    if (!Files.exists(caKeysPath)) {
      return false;
    }

    return Files.exists(Paths.get(caKeysPath.toString(),
        this.config.getPrivateKeyFileName()));
  }

  /**
   * Returns certificate Status.
   *
   * @return True if the Certificate files exist.
   */
  private boolean checkIfCertificatesExist() {
    if (!Files.exists(caRootX509Path)) {
      return false;
    }
    return Files.exists(Paths.get(caRootX509Path.toString(),
        this.config.getCertificateFileName()));
  }

  /**
   * Based on the Status of the verification, we return a lambda that gets
   * executed by the init function of the CA.
   *
   * @param status - Verification Status.
   */
  @VisibleForTesting
  Consumer<SecurityConfig> processVerificationStatus(
      VerificationStatus status) {
    Consumer<SecurityConfig> consumer = null;
    switch (status) {
    case SUCCESS:
      consumer = (arg) -> LOG.info("CertificateServer validation is " +
          "successful");
      break;
    case MISSING_KEYS:
      consumer = (arg) -> {
        LOG.error("We have found the Certificate for this CertificateServer, " +
            "but keys used by this CertificateServer is missing. This is a " +
            "non-recoverable error. Please restart the system after locating " +
            "the Keys used by the CertificateServer.");
        LOG.error("Exiting due to unrecoverable CertificateServer error.");
        throw new IllegalStateException("Missing Keys, cannot continue.");
      };
      break;
    case MISSING_CERTIFICATE:
      consumer = (arg) -> {
        LOG.error("We found the keys, but the root certificate for this " +
            "CertificateServer is missing. Please restart SCM after locating " +
            "the " +
            "Certificates.");
        LOG.error("Exiting due to unrecoverable CertificateServer error.");
        throw new IllegalStateException("Missing Root Certs, cannot continue.");
      };
      break;
    case INITIALIZE:
      consumer = (arg) -> {
        try {
          generateSelfSignedCA(arg);
        } catch (NoSuchProviderException | NoSuchAlgorithmException
            | IOException e) {
          LOG.error("Unable to initialize CertificateServer.", e);
        }
        VerificationStatus newStatus = verifySelfSignedCA(arg);
        if (newStatus != VerificationStatus.SUCCESS) {
          LOG.error("Unable to initialize CertificateServer, failed in " +
              "verification.");
        }
      };
      break;
    default:
      /* Make CheckStyle happy */
      break;
    }
    return consumer;
  }

  /**
   * Generates a KeyPair for the Certificate.
   *
   * @param securityConfig - SecurityConfig.
   * @return Key Pair.
   * @throws NoSuchProviderException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws IOException              - on Error.
   */
  private KeyPair generateKeys(SecurityConfig securityConfig)
      throws NoSuchProviderException, NoSuchAlgorithmException, IOException {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keys = keyGenerator.generateKey();
    KeyCodec keyPEMWriter = new KeyCodec(securityConfig,
        componentName);
    keyPEMWriter.writeKey(keys);
    return keys;
  }

  /**
   * Generates a self-signed Root Certificate for CA.
   *
   * @param securityConfig - SecurityConfig
   * @param key - KeyPair.
   * @throws IOException          - on Error.
   * @throws SCMSecurityException - on Error.
   */
  private void generateRootCertificate(SecurityConfig securityConfig,
      KeyPair key) throws IOException, SCMSecurityException {
    Preconditions.checkNotNull(this.config);
    LocalDate beginDate = LocalDate.now().atStartOfDay().toLocalDate();
    LocalDateTime temp = LocalDateTime.of(beginDate, LocalTime.MIDNIGHT);
    LocalDate endDate =
        temp.plus(securityConfig.getMaxCertificateDuration()).toLocalDate();
    X509CertificateHolder selfSignedCertificate =
        SelfSignedCertificate
            .newBuilder()
            .setSubject(this.subject)
            .setScmID(this.scmID)
            .setClusterID(this.clusterID)
            .setBeginDate(beginDate)
            .setEndDate(endDate)
            .makeCA()
            .setConfiguration(securityConfig.getConfiguration())
            .setKey(key)
            .build();

    CertificateCodec certCodec =
        new CertificateCodec(config, componentName);
    certCodec.writeCertificate(selfSignedCertificate);
  }

  /**
   * This represents the verification status of the CA. Based on this enum
   * appropriate action is taken in the Init.
   */
  @VisibleForTesting
  enum VerificationStatus {
    SUCCESS, /* All artifacts needed by CertificateServer is present */
    MISSING_KEYS, /* Private key is missing, certificate Exists.*/
    MISSING_CERTIFICATE, /* Keys exist, but root certificate missing.*/
    INITIALIZE /* All artifacts are missing, we should init the system. */
  }
}
