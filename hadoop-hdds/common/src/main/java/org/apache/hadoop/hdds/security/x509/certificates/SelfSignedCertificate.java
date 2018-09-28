/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
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

package org.apache.hadoop.hdds.security.x509.certificates;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.exceptions.SCMSecurityException;
import org.apache.hadoop.util.Time;
import org.apache.logging.log4j.util.Strings;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Date;

/**
 * A Self Signed Certificate with CA basic constraint can be used to boot-strap
 * a certificate infra-structure, if no external certificate is provided.
 */
public final class SelfSignedCertificate {
  private static final String NAME_FORMAT = "CN=%s,OU=%s,O=%s";
  private String subject;
  private String clusterID;
  private String scmID;
  private Date beginDate;
  private Date endDate;
  private KeyPair key;
  private SecurityConfig config;
  private boolean isCA;

  /**
   * Private Ctor invoked only via Builder Interface.
   * @param subject - Subject
   * @param scmID - SCM ID
   * @param clusterID - Cluster ID
   * @param beginDate - NotBefore
   * @param endDate - Not After
   * @param configuration - SCM Config
   * @param keyPair - KeyPair
   * @param ca - isCA?
   */
  private SelfSignedCertificate(String subject, String scmID, String clusterID,
      Date beginDate, Date endDate, SecurityConfig configuration,
      KeyPair keyPair, boolean ca) {
    this.subject = subject;
    this.clusterID = clusterID;
    this.scmID = scmID;
    this.beginDate = beginDate;
    this.endDate = endDate;
    config = configuration;
    this.key = keyPair;
    this.isCA = ca;
  }

  @VisibleForTesting
  public static String getNameFormat() {
    return NAME_FORMAT;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private X509CertificateHolder generateCertificate()
      throws OperatorCreationException, CertIOException {
    // For the Root Certificate we form the name from Subject, SCM ID and
    // Cluster ID.
    String dnName = String.format(getNameFormat(), subject, scmID, clusterID);
    X500Name name = new X500Name(dnName);
    byte[] encoded = key.getPublic().getEncoded();
    SubjectPublicKeyInfo publicKeyInfo =
        SubjectPublicKeyInfo.getInstance(encoded);


    ContentSigner contentSigner =
        new JcaContentSignerBuilder(
            config.getSignatureAlgo()).build(key.getPrivate());

    // Please note: Since this is a root certificate we use "ONE" as the
    // serial number. Also note that skip enforcing locale or UTC. We are
    // trying to operate at the Days level, hence Time zone is also skipped for
    // now.
    BigInteger serial = BigInteger.ONE;
    if (!isCA) {
      serial = new BigInteger(Long.toString(Time.monotonicNow()));
    }

    X509v3CertificateBuilder builder = new X509v3CertificateBuilder(name,
        serial, beginDate, endDate, name, publicKeyInfo);

    if (isCA) {
      builder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(true));
    }
    return builder.build(contentSigner);
  }

  /**
   * Builder class for Root Certificates.
   */
  public static class Builder {
    private String subject;
    private String clusterID;
    private String scmID;
    private Date beginDate;
    private Date endDate;
    private KeyPair key;
    private SecurityConfig config;
    private boolean isCA;

    public Builder setConfiguration(Configuration configuration) {
      this.config = new SecurityConfig(configuration);
      return this;
    }

    public Builder setKey(KeyPair keyPair) {
      this.key = keyPair;
      return this;
    }

    public Builder setSubject(String subjectString) {
      this.subject = subjectString;
      return this;
    }

    public Builder setClusterID(String s) {
      this.clusterID = s;
      return this;
    }

    public Builder setScmID(String s) {
      this.scmID = s;
      return this;
    }

    public Builder setBeginDate(Date date) {
      this.beginDate = new Date(date.toInstant().toEpochMilli());
      return this;
    }

    public Builder setEndDate(Date date) {
      this.endDate = new Date(date.toInstant().toEpochMilli());
      return this;
    }

    public Builder makeCA() {
      isCA = true;
      return this;
    }

    public X509CertificateHolder build() throws SCMSecurityException {
      Preconditions.checkNotNull(key, "Key cannot be null");
      Preconditions.checkArgument(Strings.isNotBlank(subject), "Subject " +
          "cannot be blank");
      Preconditions.checkArgument(Strings.isNotBlank(clusterID), "Cluster ID " +
          "cannot be blank");
      Preconditions.checkArgument(Strings.isNotBlank(scmID), "SCM ID cannot " +
          "be blank");

      Preconditions.checkArgument(beginDate.before(endDate), "Certificate " +
          "begin date should be before end date");

      Duration certDuration = Duration.between(beginDate.toInstant(),
          endDate.toInstant());
      Preconditions.checkArgument(
          certDuration.compareTo(config.getMaxCertificateDuration()) < 0,
          "Certificate life time cannot be greater than max configured value.");


      SelfSignedCertificate rootCertificate =
          new SelfSignedCertificate(this.subject,
          this.scmID, this.clusterID, this.beginDate, this.endDate,
          this.config, key, isCA);
      try {
        return rootCertificate.generateCertificate();
      } catch (OperatorCreationException | CertIOException e) {
        throw new CertificateException("Unable to create root certificate.",
            e.getCause());
      }
    }
  }
}
