/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.sasTokenGenerator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;

import javax.crypto.*;
import javax.crypto.spec.*;
import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

import static org.apache.hadoop.fs.azurebfs.sasTokenGenerator.SASTokenConstants.*;
import org.apache.hadoop.fs.azurebfs.services.*;

public class DelegationSASGenerator {

  private static final String HMAC_SHA256 = "HmacSHA256";
  private static final String BASE_64_CHARS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  public static final DateTimeFormatter ISO_8601_UTC_DATE_FORMATTER =
      DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT)
      .withZone(ZoneId.of("UTC"));
  private static Map<String, String> authorizerPermissionMap =
      new Hashtable<>();
  private Mac hmacSha256;
  private String delegationKey;
  private String skoid;
  private String sktid;
  private String skt;
  private String ske;
  private String sks;
  private String skv;

  /**
   * Constructor for DelegationSASGenerator.
   *
   * @param delegationKey Azure storage returned delegationKey
   * @param skoid         signed key Object ID [AAD]
   * @param sktid         signed key Tenant ID [AAD]
   * @param skt           signed key start time
   * @param ske           signed key expiry time
   * @param sks           signed key service (b for blob service)
   * @param skv           signed key version
   */
  public DelegationSASGenerator(String delegationKey, String skoid,
      String sktid, String skt, String ske, String sks, String skv) {

    this.delegationKey = delegationKey;
    this.skoid = skoid;
    this.sktid = sktid;
    this.skt = skt;
    this.ske = ske;
    this.sks = sks;
    this.skv = skv;
    initializeMac(delegationKey);
  }

  private static String encode(final byte[] data) {
    final StringBuilder builder = new StringBuilder();
    final int dataRemainder = data.length % 3;

    int j = 0;
    int n = 0;
    for (; j < data.length; j += 3) {

      if (j < data.length - dataRemainder) {
        n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8) + (
            data[j + 2] & 0xFF);
      } else {
        if (dataRemainder == 1) {
          n = (data[j] & 0xFF) << 16;
        } else if (dataRemainder == 2) {
          n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8);
        }
      }

      // Left here for readability
      // byte char1 = (byte) ((n >>> 18) & 0x3F);
      // byte char2 = (byte) ((n >>> 12) & 0x3F);
      // byte char3 = (byte) ((n >>> 6) & 0x3F);
      // byte char4 = (byte) (n & 0x3F);
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 18) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 12) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 6) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) (n & 0x3F)));
    }

    final int bLength = builder.length();

    // append '=' to pad
    if (data.length % 3 == 1) {
      builder.replace(bLength - 2, bLength, "==");
    } else if (data.length % 3 == 2) {
      builder.replace(bLength - 1, bLength, "=");
    }

    return builder.toString();
  }

  public String generateSAS(boolean isUserAuthorized, String authorizerAction,
      URI storePathURI, Instant startTime, Instant expiryTime,
      String correlationID, String version, String authorizedUserOID,
      String unauthorizedUserOID, String ip)
      throws AbfsAuthorizerUnhandledException, InvalidUriException,
      AbfsAuthorizationException, UnsupportedEncodingException {

    if (!isUserAuthorized) {
      throw new AbfsAuthorizationException(String
          .format("User is not authorized to perform %s on path %s",
              authorizerAction, storePathURI.toString()));
    }

    if (!VALID_SAS_REST_API_VERSIONS.contains(version)) {
      throw new AbfsAuthorizerUnhandledException(new InvalidRequestException(
          String.format("SAS REST API version provided is not valid : %s",
              version)));
    }

    if ((authorizedUserOID != null) && (unauthorizedUserOID != null)) {
      throw new AbfsAuthorizationException(String.format(
          "Delegation attempt with both authorized and "
              + "unauthorized user OID made. Authorized UserOID: %s "
              + "Unauthorized UserOID: %s. Action: %s Path: %s",
          authorizedUserOID, unauthorizedUserOID, authorizerAction,
          storePathURI.toString()));
    }

    if (version.equalsIgnoreCase("2018-11-09")) {
      authorizerPermissionMap = Collections.unmodifiableMap(AbfsAuthorizerActionsToPermissionsMap_PreDec19Version);
    } else
    {
      authorizerPermissionMap = Collections.unmodifiableMap(AbfsAuthorizerActionsToPermissionsMap);
    }

    String sp = getRequiredSASPermission(authorizerAction, version);
    String st = ISO_8601_UTC_DATE_FORMATTER.format(startTime);
    String se = ISO_8601_UTC_DATE_FORMATTER.format(expiryTime);

    String signature = computeSignatureForSAS(storePathURI, sp, st, se, skoid,
        sktid, skt, ske, sks, skv, authorizedUserOID,
        unauthorizedUserOID,
        correlationID, ip, version, Blob_SignedResource);

    AbfsUriQueryBuilder qp = new AbfsUriQueryBuilder();

    if (sp != null) {
      qp.addQuery(SASTokenConstants.SignedPermission, sp);
    }

    if (st != null) {
      qp.addQuery(SASTokenConstants.SignedStart, st);
    }

    if (se != null) {
      qp.addQuery(SASTokenConstants.SignedExpiry, se);
    }

    if (skoid != null) {
      qp.addQuery(SASTokenConstants.SignedKeyOid, skoid);
    }

    if (sktid != null) {
      qp.addQuery(SASTokenConstants.SignedKeyTid, sktid);
    }

    if (skt != null) {
      qp.addQuery(SASTokenConstants.SignedKeyStart, skt);
    }

    if (ske != null) {
      qp.addQuery(SASTokenConstants.SignedKeyExpiry, ske);
    }

    if (sks != null) {
      qp.addQuery(SASTokenConstants.SignedKeyService, sks);
    }

    if (skv != null) {
      qp.addQuery(SASTokenConstants.SignedKeyVersion, skv);
    }

    if (authorizedUserOID != null) {
      qp.addQuery(SASTokenConstants.SignedAuthorizedAgentOid, authorizedUserOID);
    }

    if (unauthorizedUserOID != null) {
      qp.addQuery(SASTokenConstants.SignedUnauthorizedAgentOid, unauthorizedUserOID);
    }

    if (!version.equalsIgnoreCase("2018-11-09")) {
      if (correlationID != null) {
        qp.addQuery(SASTokenConstants.SignedCorrelationId, correlationID);
      }
    }

    if (ip != null) {
      qp.addQuery(SASTokenConstants.SignedIP, ip);
    }

    qp.addQuery(SASTokenConstants.SignedVersion, version);

    // TODO: Add directory SAS support
    qp.addQuery(SASTokenConstants.SignedResource, Blob_SignedResource);

    if (signature != null) {
      qp.addQuery(SASTokenConstants.Signature, signature);
    }

    return qp.toString();
  }

  private String getRequiredSASPermission(final String authorizerAction,
      final String version)
      throws AbfsAuthorizerUnhandledException {

    if (!authorizerPermissionMap.containsKey(authorizerAction.toLowerCase())) {
      throw new AbfsAuthorizerUnhandledException(new InvalidRequestException(
          String.format("Unknown Authorizer action %s",
              authorizerAction.toLowerCase())));
    }

    return authorizerPermissionMap.get(authorizerAction.toLowerCase());
  }

  /**
   * Computes Signed signature of SAS token params.
   *
   * @param storePathURI
   * @param sp           Permissions
   * @param st           start time
   * @param se           expiry time
   * @param skoid        signing key OID
   * @param sktid        signing key TID
   * @param skt          signing key start time
   * @param ske          signing key expiry time
   * @param sks          signing key service
   * @param skv          signing key REST API version
   * @param saoid        authotized user OID
   * @param suoid        unauthorized user OID
   * @param scid         correlation id
   * @param sip          IP address
   * @param sv           version
   * @param sr           resource type and scope
   * @return signed signature
   * @throws AbfsAuthorizerUnhandledException
   * @throws InvalidUriException
   */
  private String computeSignatureForSAS(URI storePathURI, String sp, String st,
      String se, String skoid, String sktid, String skt, String ske, String sks,
      String skv, String saoid, String suoid, String scid, String sip,
      String sv, String sr)
      throws AbfsAuthorizerUnhandledException, InvalidUriException {
    if (sr == null) {
      throw new AbfsAuthorizerUnhandledException(
          new Exception("Resource Type missing to generate SAS token"));
    }

    StringBuilder sb = new StringBuilder();
    sb.append((sp == null) ? "" : sp);
    sb.append("\n");
    sb.append((st == null) ? "" : st);
    sb.append("\n");
    sb.append((se == null) ? "" : se);
    sb.append("\n");
    sb.append(Blob_Canonicalized_Resource_Prefix);
    AbfsPathUriParts uriParts = new AbfsPathUriParts(storePathURI);

    sb.append("/" + uriParts.getAccountName() + "/" + uriParts
        .getAbfsFileSystemName());

    // TODO: Add directory SAS support
    if (sr.equalsIgnoreCase(Blob_SignedResource)) {
      sb.append(uriParts.getPathRelativeToAbfsFileSystem());
    } else {
      throw new AbfsAuthorizerUnhandledException(new Exception(String.format(
          "Unexpected signed resource while " + "generating SAS token %s",
          sr)));
    }

    sb.append("\n");
    sb.append((skoid));
    sb.append("\n");
    sb.append((sktid));
    sb.append("\n");
    sb.append((skt));
    sb.append("\n");
    sb.append((ske));
    sb.append("\n");
    sb.append((sks));
    sb.append("\n");
    sb.append((skv));
    sb.append("\n");

    if (!sv.equalsIgnoreCase("2018-11-09")) {

    sb.append((saoid == null) ? "" : saoid);
    sb.append("\n");
    sb.append((suoid == null) ? "" : suoid);
    sb.append("\n");

      sb.append((scid == null) ? "" : scid);
      sb.append("\n");
    }

    sb.append((sip == null) ? "" : sip);
    sb.append("\n");
    sb.append("\n"); // - For optional : spr - ProtocolResource
    sb.append((sv == null) ? "" : sv);
    sb.append("\n");
    sb.append((sr == null) ? "" : sr);
    sb.append("\n");
    sb.append("\n"); // - For optional : rscc - ResponseCacheControl
    sb.append("\n"); // - For optional : rscd - ResponseContentDisposition
    sb.append("\n"); // - For optional : rsce - ResponseContentEncoding
    sb.append("\n"); // - For optional : rscl - ResponseContentLanguage
    sb.append("\n"); // - For optional : rsct - ResponseContentType

    String stringToSign = sb.toString();
    System.out.println("stringToSign = " + stringToSign);
    return computeHmac256(stringToSign);
  }

  private void initializeMac(String delegationKey) {
    // Initializes the HMAC-SHA256 Mac and SecretKey.
    try {
      hmacSha256 = Mac.getInstance(HMAC_SHA256);
      byte[] key = Base64.getDecoder().decode(delegationKey);
      //System.out.println(new String(Base64.getDecoder().decode
      // (delegationKey)));
      hmacSha256.init(new SecretKeySpec(key, HMAC_SHA256));
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String computeHmac256(final String stringToSign) {
    byte[] utf8Bytes;
    utf8Bytes = stringToSign.getBytes(StandardCharsets.UTF_8);
    byte[] hmac;
    synchronized (this) {
      hmac = hmacSha256.doFinal(utf8Bytes);
    }

    String encodeStr = encode(hmac);
    System.out.println("encode : " + encodeStr);
    String base64Str = Base64.getEncoder().encodeToString(hmac);
    System.out.println("base64 : " + base64Str);
    String inhouse = org.apache.hadoop.fs.azurebfs.utils.Base64.encode(hmac);
    System.out.println("inhouse : " + inhouse);

    System.out.println("decode - encode: " + Base64.getDecoder().decode(encodeStr));
    System.out.println("decode - base64: " + Base64.getDecoder().decode(base64Str));
    System.out.println("decode - inhouse: " + Base64.getDecoder().decode(inhouse));

    return org.apache.hadoop.fs.azurebfs.utils.Base64.encode(hmac);
    //return Base64.getEncoder().encodeToString(hmac);
  }

  public class AbfsPathUriParts {
    private String accountName;
    private String abfsFileSystemName;
    private String pathRelativeToAbfsFileSystem;

    public AbfsPathUriParts(URI abfsPathUri) throws InvalidUriException {
      // abfsPath will be of format
      // abfs(s)://abfsFileSystemName@AccountName/path
      String authorityPart = abfsPathUri.getAuthority();
      int indexOfContainerNameEnd = authorityPart.indexOf("@");

      if (indexOfContainerNameEnd == -1) {
        throw new InvalidUriException(
            "Unable to obtain AbfsFileSystemName " + "from URI: " + abfsPathUri
                .toString());
      }

      String accountNameWithDomain =
          authorityPart.substring(indexOfContainerNameEnd + 1);

      this.accountName = accountNameWithDomain.substring(0,
          accountNameWithDomain.indexOf("."));
      this.abfsFileSystemName = authorityPart
          .substring(0, indexOfContainerNameEnd);
      this.pathRelativeToAbfsFileSystem = abfsPathUri.getPath();
    }

    public String getAccountName() {
      return accountName;
    }

    public String getAbfsFileSystemName() {
      return abfsFileSystemName;
    }

    public String getPathRelativeToAbfsFileSystem() {
      return pathRelativeToAbfsFileSystem;
    }
  }

}
