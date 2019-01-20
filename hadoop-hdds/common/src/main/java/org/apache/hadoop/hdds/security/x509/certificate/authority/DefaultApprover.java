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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.PKIProfile;
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
   * @return Signed Certificate.
   * @throws IOException - On Error
   * @throws OperatorCreationException - on Error.
   */
  public  X509CertificateHolder sign(
      SecurityConfig config,
      PrivateKey caPrivate,
      X509CertificateHolder caCertificate,
      Date validFrom,
      Date validTill,
      PKCS10CertificationRequest certificationRequest)
      throws IOException, OperatorCreationException {

    AlgorithmIdentifier sigAlgId = new
        DefaultSignatureAlgorithmIdentifierFinder().find(
        config.getSignatureAlgo());
    AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder()
        .find(sigAlgId);

    AsymmetricKeyParameter asymmetricKP = PrivateKeyFactory.createKey(caPrivate
        .getEncoded());
    SubjectPublicKeyInfo keyInfo =
        certificationRequest.getSubjectPublicKeyInfo();

    RSAKeyParameters rsa =
        (RSAKeyParameters) PublicKeyFactory.createKey(keyInfo);
    if (rsa.getModulus().bitLength() < config.getSize()) {
      throw new SCMSecurityException("Key size is too small in certificate " +
          "signing request");
    }
    X509v3CertificateBuilder certificateGenerator =
        new X509v3CertificateBuilder(
            caCertificate.getSubject(),
            // When we do persistence we will check if the certificate number
            // is a duplicate.
            new BigInteger(RandomUtils.nextBytes(8)),
            validFrom,
            validTill,
            certificationRequest.getSubject(), keyInfo);

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
