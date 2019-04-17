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

import com.google.common.base.Preconditions;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.validator.routines.DomainValidator;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Boolean.TRUE;
import static org.bouncycastle.asn1.x509.KeyPurposeId.id_kp_clientAuth;
import static org.bouncycastle.asn1.x509.KeyPurposeId.id_kp_serverAuth;

/**
 * Ozone PKI profile.
 * <p>
 * This PKI profile is invoked by SCM CA to make sure that certificates issued
 * by SCM CA are constrained
 */
public class DefaultProfile implements PKIProfile {
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_KEY_USAGE = DefaultProfile::validateKeyUsage;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_AUTHORITY_KEY_IDENTIFIER = (e, b) -> TRUE;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_LOGO_TYPE = (e, b) -> TRUE;
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultProfile.class);
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_SAN = DefaultProfile::validateSubjectAlternativeName;
  static final BiFunction<Extension, PKIProfile, Boolean>
      VALIDATE_EXTENDED_KEY_USAGE = DefaultProfile::validateExtendedKeyUsage;
  // If we decide to add more General Names, we should add those here and
  // also update the logic in validateGeneralName function.
  private static final int[] GENERAL_NAMES = {
      GeneralName.dNSName,
      GeneralName.iPAddress,
  };
  // Map that handles all the Extensions lookup and validations.
  private static final Map<ASN1ObjectIdentifier, BiFunction<Extension,
      PKIProfile, Boolean>> EXTENSIONS_MAP = Stream.of(
      new SimpleEntry<>(Extension.keyUsage, VALIDATE_KEY_USAGE),
      new SimpleEntry<>(Extension.subjectAlternativeName, VALIDATE_SAN),
      new SimpleEntry<>(Extension.authorityKeyIdentifier,
          VALIDATE_AUTHORITY_KEY_IDENTIFIER),
      new SimpleEntry<>(Extension.extendedKeyUsage,
          VALIDATE_EXTENDED_KEY_USAGE),
      // Ozone certs are issued only for the use of Ozone.
      // However, some users will discover that this is a full scale CA
      // and decide to mis-use these certs for other purposes.
      // To discourage usage of these certs for other purposes, we can leave
      // the Ozone Logo inside these certs. So if a browser is used to
      // connect these logos will show up.
      // https://www.ietf.org/rfc/rfc3709.txt
      new SimpleEntry<>(Extension.logoType, VALIDATE_LOGO_TYPE))
      .collect(Collectors.toMap(SimpleEntry::getKey,
          SimpleEntry::getValue));
  // If we decide to add more General Names, we should add those here and
  // also update the logic in validateGeneralName function.
  private static final KeyPurposeId[] EXTENDED_KEY_USAGE = {
      id_kp_serverAuth, // TLS Web server authentication
      id_kp_clientAuth, // TLS Web client authentication

  };
  private final Set<KeyPurposeId> extendKeyPurposeSet;
  private Set<Integer> generalNameSet;

  /**
   * Construct DefaultProfile.
   */
  public DefaultProfile() {
    generalNameSet = new HashSet<>();
    for (int val : GENERAL_NAMES) {
      generalNameSet.add(val);
    }
    extendKeyPurposeSet =
        new HashSet<>(Arrays.asList(EXTENDED_KEY_USAGE));

  }

  /**
   * This function validates that the KeyUsage Bits are subset of the Bits
   * permitted by the ozone profile.
   *
   * @param ext - KeyUsage Extension.
   * @param profile - PKI Profile - In this case this profile.
   * @return True, if the request key usage is a subset, false otherwise.
   */
  private static Boolean validateKeyUsage(Extension ext, PKIProfile profile) {
    KeyUsage keyUsage = profile.getKeyUsage();
    KeyUsage requestedUsage = KeyUsage.getInstance(ext.getParsedValue());
    BitSet profileBitSet = BitSet.valueOf(keyUsage.getBytes());
    BitSet requestBitSet = BitSet.valueOf(requestedUsage.getBytes());
    // Check if the requestBitSet is a subset of profileBitSet
    //  p & r == r should be equal if it is a subset.
    profileBitSet.and(requestBitSet);
    return profileBitSet.equals(requestBitSet);
  }

  /**
   * Validates the SubjectAlternative names in the Certificate.
   *
   * @param ext - Extension - SAN, which allows us to get the SAN names.
   * @param profile - This profile.
   * @return - True if the request contains only SANs, General names that we
   * support. False otherwise.
   */
  private static Boolean validateSubjectAlternativeName(Extension ext,
      PKIProfile profile) {
    if (ext.isCritical()) {
      // SAN extensions should not be marked as critical under ozone profile.
      LOG.error("SAN extension marked as critical in the Extension. {}",
          GeneralNames.getInstance(ext.getParsedValue()).toString());
      return false;
    }
    GeneralNames generalNames = GeneralNames.getInstance(ext.getParsedValue());
    for (GeneralName name : generalNames.getNames()) {
      try {
        if (!profile.validateGeneralName(name.getTagNo(),
            name.getName().toString())) {
          return false;
        }
      } catch (UnknownHostException e) {
        LOG.error("IP address validation failed."
            + name.getName().toString(), e);
        return false;
      }
    }
    return true;
  }

  /**
   * This function validates that the KeyUsage Bits are subset of the Bits
   * permitted by the ozone profile.
   *
   * @param ext - KeyUsage Extension.
   * @param profile - PKI Profile - In this case this profile.
   * @return True, if the request key usage is a subset, false otherwise.
   */
  private static Boolean validateExtendedKeyUsage(Extension ext,
      PKIProfile profile) {
    if (ext.isCritical()) {
      // https://tools.ietf.org/html/rfc5280#section-4.2.1.12
      // Ozone profile opts to mark this extension as non-critical.
      LOG.error("Extended Key usage marked as critical.");
      return false;
    }
    ExtendedKeyUsage extendedKeyUsage =
        ExtendedKeyUsage.getInstance(ext.getParsedValue());
    for (KeyPurposeId id : extendedKeyUsage.getUsages()) {
      if (!profile.validateExtendedKeyUsage(id)) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] getGeneralNames() {
    return Arrays.copyOfRange(GENERAL_NAMES, 0, GENERAL_NAMES.length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSupportedGeneralName(int generalName) {
    return generalNameSet.contains(generalName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean validateGeneralName(int type, String value) {
    // TODO : We should add more validation for IP address, for example
    //  it matches the local network, and domain matches where the cluster
    //  exits.
    if (!isSupportedGeneralName(type)) {
      return false;
    }
    switch (type) {
    case GeneralName.iPAddress:

      // We need DatatypeConverter conversion, since the original CSR encodes
      // an IP address int a Hex String, for example 8.8.8.8 is encoded as
      // #08080808. Value string is always preceded by "#", we will strip
      // that before passing it on.

      // getByAddress call converts the IP address to hostname/ipAddress format.
      // if the hostname cannot determined then it will be /ipAddress.

      // TODO: Fail? if we cannot resolve the Hostname?
      try {
        final InetAddress byAddress = InetAddress.getByAddress(
            Hex.decodeHex(value.substring(1)));
        LOG.debug("Host Name/IP Address : {}", byAddress.toString());
        return true;
      } catch (UnknownHostException | DecoderException e) {
        return false;
      }
    case GeneralName.dNSName:
      return DomainValidator.getInstance().isValid(value);
    default:
      // This should not happen, since it guarded via isSupportedGeneralName.
      LOG.error("Unexpected type in General Name (int value) : " + type);
      return false;
    }
  }

  @Override
  public boolean validateExtendedKeyUsage(KeyPurposeId id) {
    return extendKeyPurposeSet.contains(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ASN1ObjectIdentifier[] getSupportedExtensions() {
    return EXTENSIONS_MAP.keySet().toArray(new ASN1ObjectIdentifier[0]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSupportedExtension(Extension extension) {
    return EXTENSIONS_MAP.containsKey(extension.getExtnId());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean validateExtension(Extension extension) {
    Preconditions.checkNotNull(extension, "Extension cannot be null");

    if (!isSupportedExtension(extension)) {
      LOG.error("Unsupported Extension found: {} ",
          extension.getExtnId().getId());
      return false;
    }

    BiFunction<Extension, PKIProfile, Boolean> func =
        EXTENSIONS_MAP.get(extension.getExtnId());

    if (func != null) {
      return func.apply(extension, this);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KeyUsage getKeyUsage() {
    return new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment
        | KeyUsage.dataEncipherment | KeyUsage.keyAgreement);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RDN[] getRDNs() {
    return new RDN[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isValidRDN(RDN distinguishedName) {
    // TODO: Right now we just approve all strings.
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean validateRDN(RDN name) {
    return true;
  }

  @Override
  public boolean isCA() {
    return false;
  }
}
