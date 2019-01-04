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
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.operator.ContentVerifierProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A base approver class for certificate approvals.
 */
public abstract class BaseApprover implements CertificateApprover {
  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateApprover.class);
  private final PKIProfile profile;
  private final SecurityConfig securityConfig;

  public BaseApprover(PKIProfile pkiProfile, SecurityConfig config) {
    this.profile = Objects.requireNonNull(pkiProfile);
    this.securityConfig = Objects.requireNonNull(config);
  }

  /**
   * Returns the Security config.
   *
   * @return SecurityConfig
   */
  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  /**
   * Returns the Attribute array that encodes extensions.
   *
   * @param request - Certificate Request
   * @return - An Array of Attributes that encode various extensions requested
   * in this certificate.
   */
  Attribute[] getAttributes(PKCS10CertificationRequest request) {
    Objects.requireNonNull(request);
    return
        request.getAttributes(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest);
  }

  /**
   * Returns a list of Extensions encoded in a given attribute.
   *
   * @param attribute - Attribute to decode.
   * @return - List of Extensions.
   */
  List<Extensions> getExtensionsList(Attribute attribute) {
    Objects.requireNonNull(attribute);
    List<Extensions> extensionsList = new ArrayList<>();
    for (ASN1Encodable value : attribute.getAttributeValues()) {
      if(value != null) {
        Extensions extensions = Extensions.getInstance(value);
        extensionsList.add(extensions);
      }
    }
    return extensionsList;
  }

  /**
   * Returns the Extension decoded into a Java Collection.
   * @param extensions - A set of Extensions in ASN.1.
   * @return List of Decoded Extensions.
   */
  List<Extension> getIndividualExtension(Extensions extensions) {
    Objects.requireNonNull(extensions);
    List<Extension> extenList = new ArrayList<>();
    for (ASN1ObjectIdentifier id : extensions.getExtensionOIDs()) {
      if (id != null) {
        Extension ext = extensions.getExtension(id);
        if (ext != null) {
          extenList.add(ext);
        }
      }
    }
    return extenList;
  }



  /**
   * This function verifies all extensions in the certificate.
   *
   * @param request - CSR
   * @return - true if the extensions are acceptable by the profile, false
   * otherwise.
   */
  boolean verfiyExtensions(PKCS10CertificationRequest request) {
    Objects.requireNonNull(request);
    /*
     * Inside a CSR we have
     *  1. A list of Attributes
     *    2. Inside each attribute a list of extensions.
     *      3. We need to walk thru the each extension and verify they
     *      are expected and we can put that into a certificate.
     */

    for (Attribute attr : getAttributes(request)) {
      for (Extensions extensionsList : getExtensionsList(attr)) {
        for (Extension extension : getIndividualExtension(extensionsList)) {
          if (!profile.validateExtension(extension)) {
            LOG.error("Failed to verify extension. {}",
                extension.getExtnId().getId());
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Verifies the Signature on the CSR is valid.
   *
   * @param pkcs10Request - PCKS10 Request.
   * @return True if it is valid, false otherwise.
   * @throws OperatorCreationException - On Error.
   * @throws PKCSException             - on Error.
   */
  boolean verifyPkcs10Request(PKCS10CertificationRequest pkcs10Request)
      throws OperatorCreationException, PKCSException {
    ContentVerifierProvider verifierProvider = new
        JcaContentVerifierProviderBuilder()
        .setProvider(this.securityConfig.getProvider())
        .build(pkcs10Request.getSubjectPublicKeyInfo());
    return
        pkcs10Request.isSignatureValid(verifierProvider);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<X509CertificateHolder> inspectCSR(String csr)
      throws IOException {
    return inspectCSR(CertificateSignRequest.getCertificationRequest(csr));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<X509CertificateHolder>
        inspectCSR(PKCS10CertificationRequest csr) {
    /**
     * The base approver executes the following algorithm to verify that a
     * CSR meets the PKI Profile criteria.
     *
     * 0. For time being (Until we have SCM HA) we will deny all request to
     * become an intermediary CA. So we will not need to verify using CA
     * profile, right now.
     *
     * 1. We verify the proof of possession. That is we verify the entity
     * that sends us the CSR indeed has the private key for the said public key.
     *
     * 2. Then we will verify the RDNs meet the format and the Syntax that
     * PKI profile dictates.
     *
     * 3. Then we decode each and every extension and  ask if the PKI profile
     * approves of these extension requests.
     *
     * 4. If all of these pass, We will return a Future which will point to
     * the Certificate when finished.
     */

    CompletableFuture<X509CertificateHolder> response =
        new CompletableFuture<>();
    try {
      // Step 0: Verify this is not a CA Certificate.
      // Will be done by the Ozone PKI profile for time being.
      // If there are any basicConstraints, they will flagged as not
      // supported for time being.

      // Step 1: Let us verify that Certificate is indeed signed by someone
      // who has access to the private key.
      if (!verifyPkcs10Request(csr)) {
        LOG.error("Failed to verify the signature in CSR.");
        response.completeExceptionally(new SCMSecurityException("Failed to " +
            "verify the CSR."));
      }

      // Step 2: Verify the RDNs are in the correct format.
      // TODO: Ozone Profile does not verify RDN now, so this call will pass.
      for (RDN rdn : csr.getSubject().getRDNs()) {
        if (!profile.validateRDN(rdn)) {
          LOG.error("Failed in verifying RDNs");
          response.completeExceptionally(new SCMSecurityException("Failed to " +
              "verify the RDNs. Please check the subject name."));
        }
      }

      // Step 3: Verify the Extensions.
      if (!verfiyExtensions(csr)) {
        LOG.error("failed in verification of extensions.");
        response.completeExceptionally(new SCMSecurityException("Failed to " +
            "verify extensions."));
      }

    } catch (OperatorCreationException | PKCSException e) {
      LOG.error("Approval Failure.", e);
      response.completeExceptionally(new SCMSecurityException(e));
    }
    return response;
  }


}
