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
package org.apache.hadoop.hdds.security.x509.keys;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

/**
 * Utility functions for Security modules for Ozone.
 */
public final class SecurityUtil {

  // Ozone Certificate distinguished format: (CN=Subject,OU=ScmID,O=ClusterID).
  private static final String DISTINGUISHED_NAME_FORMAT = "CN=%s,OU=%s,O=%s";

  private SecurityUtil() {
  }

  public static String getDistinguishedNameFormat() {
    return DISTINGUISHED_NAME_FORMAT;
  }

  public static X500Name getDistinguishedName(String subject, String scmID,
      String clusterID) {
    return new X500Name(String.format(getDistinguishedNameFormat(), subject,
        scmID, clusterID));
  }

  // TODO: move the PKCS10CSRValidator class
  public static Extensions getPkcs9Extensions(PKCS10CertificationRequest csr)
      throws CertificateException {
    ASN1Set pkcs9ExtReq = getPkcs9ExtRequest(csr);
    Object extReqElement = pkcs9ExtReq.getObjects().nextElement();
    if (extReqElement instanceof Extensions) {
      return (Extensions) extReqElement;
    } else {
      if (extReqElement instanceof ASN1Sequence) {
        return Extensions.getInstance((ASN1Sequence) extReqElement);
      } else {
        throw new CertificateException("Unknown element type :" + extReqElement
            .getClass().getSimpleName());
      }
    }
  }

  public static ASN1Set getPkcs9ExtRequest(PKCS10CertificationRequest csr)
      throws CertificateException {
    for (Attribute attr : csr.getAttributes()) {
      ASN1ObjectIdentifier oid = attr.getAttrType();
      if (oid.equals(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest)) {
        return attr.getAttrValues();
      }
    }
    throw new CertificateException("No PKCS#9 extension found in CSR");
  }

  /*
   * Returns private key created from encoded key.
   * @return private key if successful else returns null.
   */
  public static PrivateKey getPrivateKey(byte[] encodedKey,
      SecurityConfig secureConfig) {
    PrivateKey pvtKey = null;
    if (encodedKey == null || encodedKey.length == 0) {
      return null;
    }

    try {
      KeyFactory kf = null;

      kf = KeyFactory.getInstance(secureConfig.getKeyAlgo(),
          secureConfig.getProvider());
      pvtKey = kf.generatePrivate(new PKCS8EncodedKeySpec(encodedKey));

    } catch (NoSuchAlgorithmException | InvalidKeySpecException |
        NoSuchProviderException e) {
      return null;
    }
    return pvtKey;
  }

  /*
   * Returns public key created from encoded key.
   * @return public key if successful else returns null.
   */
  public static PublicKey getPublicKey(byte[] encodedKey,
      SecurityConfig secureConfig) {
    PublicKey key = null;
    if (encodedKey == null || encodedKey.length == 0) {
      return null;
    }

    try {
      KeyFactory kf = null;
      kf = KeyFactory.getInstance(secureConfig.getKeyAlgo(),
          secureConfig.getProvider());
      key = kf.generatePublic(new X509EncodedKeySpec(encodedKey));

    } catch (NoSuchAlgorithmException | InvalidKeySpecException |
        NoSuchProviderException e) {
      return null;
    }
    return key;
  }

}
