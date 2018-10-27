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
}
