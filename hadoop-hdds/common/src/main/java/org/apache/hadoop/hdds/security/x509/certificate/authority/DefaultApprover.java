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

import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.PKIProfile;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.apache.hadoop.util.Time;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * Default Approver used the by the DefaultCA.
 */
public class DefaultApprover extends BaseApprover {

  /**
   * Constructs the Default Approver.
   *
   * @param pkiProfile - PKI Profile to use.
   * @param config - Security Config
   */
  public DefaultApprover(PKIProfile pkiProfile, SecurityConfig config) {
    super(pkiProfile, config);
  }

  /**
   * Sign function signs a Certificate.
   * @param config - Security Config.
   * @param caPrivate - CAs private Key.
   * @param caCertificate - CA Certificate.
   * @param validFrom - Begin Da te
   * @param validTill - End Date
   * @param certificationRequest - Certification Request.
   * @param scmId - SCM id.
   * @param clusterId - Cluster id.
   * @return Signed Certificate.
   * @throws IOException - On Error
   * @throws OperatorCreationException - on Error.
   */
  @SuppressWarnings("ParameterNumber")
  public  X509CertificateHolder sign(
      SecurityConfig config,
      PrivateKey caPrivate,
      X509CertificateHolder caCertificate,
      Date validFrom,
      Date validTill,
      PKCS10CertificationRequest certificationRequest,
      String scmId,
      String clusterId) throws IOException, OperatorCreationException {

    AlgorithmIdentifier sigAlgId = new
        DefaultSignatureAlgorithmIdentifierFinder().find(
        config.getSignatureAlgo());
    AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder()
        .find(sigAlgId);

    AsymmetricKeyParameter asymmetricKP = PrivateKeyFactory.createKey(caPrivate
        .getEncoded());
    SubjectPublicKeyInfo keyInfo =
        certificationRequest.getSubjectPublicKeyInfo();

    // Get scmId and cluster Id from subject name.
    X500Name x500Name = certificationRequest.getSubject();
    String csrScmId = x500Name.getRDNs(BCStyle.OU)[0].getFirst().getValue().
        toASN1Primitive().toString();
    String csrClusterId = x500Name.getRDNs(BCStyle.O)[0].getFirst().getValue().
        toASN1Primitive().toString();

    if (!scmId.equals(csrScmId) || !clusterId.equals(csrClusterId)) {
      if (csrScmId.equalsIgnoreCase("null") &&
          csrClusterId.equalsIgnoreCase("null")) {
        // Special case to handle DN certificate generation as DN might not know
        // scmId and clusterId before registration. In secure mode registration
        // will succeed only after datanode has a valid certificate.
        String cn = x500Name.getRDNs(BCStyle.CN)[0].getFirst().getValue()
            .toASN1Primitive().toString();
        x500Name = SecurityUtil.getDistinguishedName(cn, scmId, clusterId);
      } else {
        // Throw exception if scmId and clusterId doesn't match.
        throw new SCMSecurityException("ScmId and ClusterId in CSR subject" +
            " are incorrect.");
      }
    }

    RSAKeyParameters rsa =
        (RSAKeyParameters) PublicKeyFactory.createKey(keyInfo);
    if (rsa.getModulus().bitLength() < config.getSize()) {
      throw new SCMSecurityException("Key size is too small in certificate " +
          "signing request");
    }
    X509v3CertificateBuilder certificateGenerator =
        new X509v3CertificateBuilder(
            caCertificate.getSubject(),
            // Serial is not sequential but it is monotonically increasing.
            BigInteger.valueOf(Time.monotonicNowNanos()),
            validFrom,
            validTill,
            x500Name, keyInfo);

    ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
        .build(asymmetricKP);

    return certificateGenerator.build(sigGen);

  }

  @Override
  public CompletableFuture<X509CertificateHolder> inspectCSR(String csr)
      throws IOException {
    return super.inspectCSR(csr);
  }

  @Override
  public CompletableFuture<X509CertificateHolder>
      inspectCSR(PKCS10CertificationRequest csr) {
    return super.inspectCSR(csr);
  }
}
