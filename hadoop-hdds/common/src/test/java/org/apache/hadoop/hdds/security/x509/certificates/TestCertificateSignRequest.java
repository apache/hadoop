package org.apache.hadoop.hdds.security.x509.certificates;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.exceptions.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.operator.ContentVerifierProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCSException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;

public class TestCertificateSignRequest {

  private SecurityConfig securityConfig;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    conf.set(OZONE_METADATA_DIRS, temporaryFolder.newFolder().toString());
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testGenerateCSR() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException,
      OperatorCreationException, PKCSException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);
    PKCS10CertificationRequest csr = builder.build();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(SecurityUtil.getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    Assert.assertEquals(csr.getSubject().toString(), dnName);

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    Assert.assertEquals(csrPublicKeyInfo, subjectPublicKeyInfo);

    // Verify CSR with attribute for extensions
    Assert.assertEquals(1, csr.getAttributes().length);
    Extensions extensions = SecurityUtil.getPkcs9Extensions(csr);

    // Verify basic constraints extension
    Extension basicExt = extensions.getExtension(Extension
        .basicConstraints);
    Assert.assertEquals(true, basicExt.isCritical());

    // Verify key usage extension
    Extension keyUsageExt = extensions.getExtension(Extension.keyUsage);
    Assert.assertEquals(true, keyUsageExt.isCritical());


    // Verify San extension not set
    Assert.assertEquals(null,
        extensions.getExtension(Extension.subjectAlternativeName));

    // Verify signature in CSR
    ContentVerifierProvider verifierProvider =
        new JcaContentVerifierProviderBuilder().setProvider(securityConfig
            .getProvider()).build(csr.getSubjectPublicKeyInfo());
    Assert.assertEquals(true, csr.isSignatureValid(verifierProvider));
  }

  @Test
  public void testGenerateCSRwithSan() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException,
      OperatorCreationException, PKCSException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);

    // Multi-home
    builder.addIpAddress("192.168.1.1");
    builder.addIpAddress("192.168.2.1");

    builder.addDnsName("dn1.abc.com");
    builder.addRfc822Name("test@abc.com");

    PKCS10CertificationRequest csr = builder.build();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(SecurityUtil.getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    Assert.assertEquals(csr.getSubject().toString(), dnName);

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    Assert.assertEquals(csrPublicKeyInfo, subjectPublicKeyInfo);

    // Verify CSR with attribute for extensions
    Assert.assertEquals(1, csr.getAttributes().length);
    Extensions extensions = SecurityUtil.getPkcs9Extensions(csr);

    // Verify key usage extension
    Extension sanExt = extensions.getExtension(Extension.keyUsage);
    Assert.assertEquals(true, sanExt.isCritical());


    // Verify signature in CSR
    ContentVerifierProvider verifierProvider =
        new JcaContentVerifierProviderBuilder().setProvider(securityConfig
            .getProvider()).build(csr.getSubjectPublicKeyInfo());
    Assert.assertEquals(true, csr.isSignatureValid(verifierProvider));
  }

  @Test
  public void testGenerateCSRWithInvalidParams() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);

    try {
      builder.setKey(null);
      builder.build();
      Assert.fail("Null Key should have failed.");
    } catch (NullPointerException | IllegalArgumentException e) {
      builder.setKey(keyPair);
    }

    // Now try with blank/null Subject.
    try {
      builder.setSubject(null);
      builder.build();
      Assert.fail("Null/Blank Subject should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setSubject(subject);
    }

    try {
      builder.setSubject("");
      builder.build();
      Assert.fail("Null/Blank Subject should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setSubject(subject);
    }

    // Now try with blank/null SCM ID
    try {
      builder.setScmID(null);
      builder.build();
      Assert.fail("Null/Blank SCM ID should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setScmID(scmID);
    }

    // Now try with blank/null SCM ID
    try {
      builder.setClusterID(null);
      builder.build();
      Assert.fail("Null/Blank Cluster ID should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setClusterID(clusterID);
    }

    // Now try with invalid IP address
    try {
      builder.addIpAddress("255.255.255.*");
      builder.build();
      Assert.fail("Invalid ip address");
    } catch (IllegalArgumentException e) {
    }

    PKCS10CertificationRequest csr = builder.build();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(SecurityUtil.getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    Assert.assertEquals(csr.getSubject().toString(), dnName);

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    Assert.assertEquals(csrPublicKeyInfo, subjectPublicKeyInfo);

    // Verify CSR with attribute for extensions
    Assert.assertEquals(1, csr.getAttributes().length);
  }
  
  @Test
  public void testCsrSerialization() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException, IOException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);
    PKCS10CertificationRequest csr = builder.build();
    byte[] csrBytes = csr.getEncoded();

    // Verify de-serialized CSR matches with the original CSR
    PKCS10CertificationRequest dsCsr = new PKCS10CertificationRequest(csrBytes);
    Assert.assertEquals(csr, dsCsr);
  }
}