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

package org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;

import java.net.UnknownHostException;

/**
 * Base class for profile rules. Generally profiles are documents that define
 * the PKI policy. In HDDS/Ozone world, we have chosen to make PKIs
 * executable code. So if an end-user wants to use a custom profile or one of
 * the existing profile like the list below, they are free to implement a
 * custom profile.
 *
 *     PKIX - Internet PKI profile.
 *     FPKI - (US) Federal PKI profile.
 *     MISSI - US DoD profile.
 *     ISO 15782 - Banking - Certificate Management Part 1: Public Key
 *         Certificates.
 *     TeleTrust/MailTrusT - German MailTrusT profile for TeleTrusT (it
 *     really is
 *         capitalised that way).
 *     German SigG Profile - Profile to implement the German digital
 *     signature law
 *     ISIS Profile - Another German profile.
 *     Australian Profile - Profile for the Australian PKAF
 *     SS 61 43 31 Electronic ID Certificate - Swedish profile.
 *     FINEID S3 - Finnish profile.
 *     ANX Profile - Automotive Network Exchange profile.
 *     Microsoft Profile - This isn't a real profile, but windows uses this.
 */
public interface PKIProfile {

  /**
   * Returns the list of General Names  supported by this profile.
   * @return - an Array of supported General Names by this certificate profile.
   */
  int[] getGeneralNames();

  /**
   * Checks if a given General Name is permitted in this profile.
   * @param generalName - General name.
   * @return true if it is allowed, false otherwise.
   */
  boolean isSupportedGeneralName(int generalName);

  /**
   * Allows the profile to dictate what value ranges are valid.
   * @param type - Type of the General Name.
   * @param value - Value of the General Name.
   * @return - true if the value is permitted, false otherwise.
   * @throws UnknownHostException - on Error in IP validation.
   */
  boolean validateGeneralName(int type, String value)
      throws UnknownHostException;

  /**
   * Returns an array of Object identifiers for extensions supported by this
   * profile.
   * @return an Array of ASN1ObjectIdentifier for the supported extensions.
   */
  ASN1ObjectIdentifier[] getSupportedExtensions();

  /**
   * Checks if the this extension is permitted in this profile.
   * @param extension - Extension to check for.
   * @return - true if this extension is supported, false otherwise.
   */
  boolean isSupportedExtension(Extension extension);

  /**
   * Checks if the extension has the value which this profile approves.
   * @param extension - Extension to validate.
   * @return - True if the extension is acceptable, false otherwise.
   */
  boolean validateExtension(Extension extension);

  /**
   * Validate the Extended Key Usage.
   * @param id - KeyPurpose ID
   * @return true, if this is a supported Purpose, false otherwise.
   */
  boolean validateExtendedKeyUsage(KeyPurposeId id);

  /**
   * Returns the permitted Key usage mask while using this profile.
   * @return KeyUsage
   */
  KeyUsage getKeyUsage();

  /**
   * Gets the supported list of RDNs supported by this profile.
   * @return Array of RDNs.
   */
  RDN[] getRDNs();

  /**
   * Returns true if this Relative Distinguished Name component is allowed in
   * this profile.
   * @param distinguishedName - RDN to check.
   * @return boolean, True if this RDN is allowed, false otherwise.
   */
  boolean isValidRDN(RDN distinguishedName);

  /**
   * Allows the profile to control the value set of the RDN. Profile can
   * reject a RDN name if needed.
   * @param name - RDN.
   * @return true if the name is acceptable to this profile, false otherwise.
   */
  boolean validateRDN(RDN name);

  /**
   * True if the profile we are checking is for issuing a CA certificate.
   * @return  True, if the profile used is for CA, false otherwise.
   */
  boolean isCA();
}
