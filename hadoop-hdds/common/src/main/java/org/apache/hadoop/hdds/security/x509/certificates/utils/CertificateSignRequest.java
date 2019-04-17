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
package org.apache.hadoop.hdds.security.x509.certificates.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.apache.logging.log4j.util.Strings;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A certificate sign request object that wraps operations to build a
 * PKCS10CertificationRequest to CertificateServer.
 */
public final class CertificateSignRequest {
  private final KeyPair keyPair;
  private final SecurityConfig config;
  private final Extensions extensions;
  private String subject;
  private String clusterID;
  private String scmID;

  /**
   * Private Ctor for CSR.
   *
   * @param subject - Subject
   * @param scmID - SCM ID
   * @param clusterID - Cluster ID
   * @param keyPair - KeyPair
   * @param config - SCM Config
   * @param extensions - CSR extensions
   */
  private CertificateSignRequest(String subject, String scmID, String clusterID,
                                 KeyPair keyPair, SecurityConfig config,
                                 Extensions extensions) {
    this.subject = subject;
    this.clusterID = clusterID;
    this.scmID = scmID;
    this.keyPair = keyPair;
    this.config = config;
    this.extensions = extensions;
  }

  private PKCS10CertificationRequest generateCSR() throws
      OperatorCreationException {
    X500Name dnName = SecurityUtil.getDistinguishedName(subject, scmID,
        clusterID);
    PKCS10CertificationRequestBuilder p10Builder =
        new JcaPKCS10CertificationRequestBuilder(dnName, keyPair.getPublic());

    ContentSigner contentSigner =
        new JcaContentSignerBuilder(config.getSignatureAlgo())
            .setProvider(config.getProvider())
            .build(keyPair.getPrivate());

    if (extensions != null) {
      p10Builder.addAttribute(
          PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extensions);
    }
    return p10Builder.build(contentSigner);
  }
  public static String getEncodedString(PKCS10CertificationRequest request)
      throws IOException {
    PemObject pemObject =
        new PemObject("CERTIFICATE REQUEST", request.getEncoded());
    StringWriter str = new StringWriter();
    try(JcaPEMWriter pemWriter = new JcaPEMWriter(str)) {
      pemWriter.writeObject(pemObject);
    }
    return str.toString();
  }


  /**
   * Gets a CertificateRequest Object from PEM encoded CSR.
   *
   * @param csr - PEM Encoded Certificate Request String.
   * @return PKCS10CertificationRequest
   * @throws IOException - On Error.
   */
  public static PKCS10CertificationRequest getCertificationRequest(String csr)
      throws IOException {
    try (PemReader reader = new PemReader(new StringReader(csr))) {
      PemObject pemObject = reader.readPemObject();
      if(pemObject.getContent() == null) {
        throw new SCMSecurityException("Invalid Certificate signing request");
      }
      return new PKCS10CertificationRequest(pemObject.getContent());
    }
  }

  /**
   * Builder class for Certificate Sign Request.
   */
  public static class Builder {
    private String subject;
    private String clusterID;
    private String scmID;
    private KeyPair key;
    private SecurityConfig config;
    private List<GeneralName> altNames;
    private Boolean ca = false;
    private boolean digitalSignature;
    private boolean digitalEncryption;

    public CertificateSignRequest.Builder setConfiguration(
        Configuration configuration) {
      this.config = new SecurityConfig(configuration);
      return this;
    }

    public CertificateSignRequest.Builder setKey(KeyPair keyPair) {
      this.key = keyPair;
      return this;
    }

    public CertificateSignRequest.Builder setSubject(String subjectString) {
      this.subject = subjectString;
      return this;
    }

    public CertificateSignRequest.Builder setClusterID(String s) {
      this.clusterID = s;
      return this;
    }

    public CertificateSignRequest.Builder setScmID(String s) {
      this.scmID = s;
      return this;
    }

    public Builder setDigitalSignature(boolean dSign) {
      this.digitalSignature = dSign;
      return this;
    }

    public Builder setDigitalEncryption(boolean dEncryption) {
      this.digitalEncryption = dEncryption;
      return this;
    }

    // Support SAN extenion with DNS and RFC822 Name
    // other name type will be added as needed.
    public CertificateSignRequest.Builder addDnsName(String dnsName) {
      Preconditions.checkNotNull(dnsName, "dnsName cannot be null");
      this.addAltName(GeneralName.dNSName, dnsName);
      return this;
    }

    // IP address is subject to change which is optional for now.
    public CertificateSignRequest.Builder addIpAddress(String ip) {
      Preconditions.checkNotNull(ip, "Ip address cannot be null");
      this.addAltName(GeneralName.iPAddress, ip);
      return this;
    }

    private CertificateSignRequest.Builder addAltName(int tag, String name) {
      if (altNames == null) {
        altNames = new ArrayList<>();
      }
      altNames.add(new GeneralName(tag, name));
      return this;
    }

    public CertificateSignRequest.Builder setCA(Boolean isCA) {
      this.ca = isCA;
      return this;
    }

    private Extension getKeyUsageExtension() throws IOException {
      int keyUsageFlag = KeyUsage.keyAgreement;
      if(digitalEncryption){
        keyUsageFlag |= KeyUsage.keyEncipherment | KeyUsage.dataEncipherment;
      }
      if(digitalSignature) {
        keyUsageFlag |= KeyUsage.digitalSignature;
      }

      if (ca) {
        keyUsageFlag |= KeyUsage.keyCertSign | KeyUsage.cRLSign;
      }
      KeyUsage keyUsage = new KeyUsage(keyUsageFlag);
      return new Extension(Extension.keyUsage, true,
          new DEROctetString(keyUsage));
    }

    private Optional<Extension> getSubjectAltNameExtension() throws
        IOException {
      if (altNames != null) {
        return Optional.of(new Extension(Extension.subjectAlternativeName,
            false, new DEROctetString(new GeneralNames(
            altNames.toArray(new GeneralName[altNames.size()])))));
      }
      return Optional.empty();
    }

    private Extension getBasicExtension() throws IOException {
      // We don't set pathLenConstraint means no limit is imposed.
      return new Extension(Extension.basicConstraints,
          true, new DEROctetString(new BasicConstraints(ca)));
    }

    private Extensions createExtensions() throws IOException {
      List<Extension> extensions = new ArrayList<>();

      // Add basic extension
      if(ca) {
        extensions.add(getBasicExtension());
      }

      // Add key usage extension
      extensions.add(getKeyUsageExtension());

      // Add subject alternate name extension
      Optional<Extension> san = getSubjectAltNameExtension();
      if (san.isPresent()) {
        extensions.add(san.get());
      }

      return new Extensions(
          extensions.toArray(new Extension[extensions.size()]));
    }

    public PKCS10CertificationRequest build() throws SCMSecurityException {
      Preconditions.checkNotNull(key, "KeyPair cannot be null");
      Preconditions.checkArgument(Strings.isNotBlank(subject), "Subject " +
          "cannot be blank");

      try {
        CertificateSignRequest csr = new CertificateSignRequest(subject, scmID,
            clusterID, key, config, createExtensions());
        return csr.generateCSR();
      } catch (IOException ioe) {
        throw new CertificateException(String.format("Unable to create " +
            "extension for certificate sign request for %s.", SecurityUtil
            .getDistinguishedName(subject, scmID, clusterID)), ioe.getCause());
      } catch (OperatorCreationException ex) {
        throw new CertificateException(String.format("Unable to create " +
            "certificate sign request for %s.", SecurityUtil
            .getDistinguishedName(subject, scmID, clusterID)),
            ex.getCause());
      }
    }
  }
}
