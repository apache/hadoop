/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.time.Instant;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsUriQueryBuilder;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;

/**
 * Test Delegation SAS generator.
 */
public class DelegationSASGeneratorVersionJuly5 extends SASGenerator {
  private final String skoid;
  private final String sktid;
  private final String skt;
  private final String ske;
  private final String sks = "b";
  private final String skv;
  private final String skdutid;
  private final String sduoid;

  /**
   * Initializes all the fields required for generating delegation SAS.
   *
   * @param userDelegationKey the user delegation key as a byte array
   * @param skoid Azure AD delegator app's object ID
   * @param sktid Azure AD delegator app's tenant ID
   * @param skt The start time for the delegation key
   * @param ske The expiry time for the delegation key
   * @param skv the API version
   * @param skdutid Azure AD delegated app's tenant ID
   * @param sduoid Azure AD delegated app's user object ID
   */
  public DelegationSASGeneratorVersionJuly5(byte[] userDelegationKey,
      String skoid,
      String sktid,
      String skt,
      String ske,
      String skv,
      String skdutid,
      String sduoid) {
    super(userDelegationKey);
    this.skoid = skoid;
    this.sktid = sktid;
    this.skt = skt;
    this.ske = ske;
    this.skv = skv;
    this.skdutid = skdutid;
    this.sduoid = sduoid;
  }

  /**
   * Generates a delegation SAS token for the specified resource and operation.
   *
   * @param accountName The storage account name
   * @param containerName The container name
   * @param path The path to the resource
   * @param operation The operation to authorize
   * @param saoid The Azure AD object ID of the application
   * @param suoid The Azure AD object ID of the user
   * @param scid The correlation ID
   * @return The generated SAS token as a query string
   * @throws IllegalArgumentException if the operation is not recognized
   */
  public String getDelegationSAS(String accountName, String containerName, String path, String operation,
                                 String saoid, String suoid, String scid) {

    // The params for signature computation (particularly the string-to-sign) are different based on the SAS version (sv)
    // They might need to be changed if using a different version
    //Ref: https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas

    // SAS version (sv) used here is 2025-07-05
    final String sv = AuthenticationVersion.Jul5.toString();

    final String st = ISO_8601_FORMATTER.format(Instant.now().minus(FIVE_MINUTES));
    final String se = ISO_8601_FORMATTER.format(Instant.now().plus(ONE_DAY));
    String sr = "b";
    String sdd = null;
    String sp;

    switch (operation) {
      case SASTokenProvider.CREATE_FILE_OPERATION:
      case SASTokenProvider.CREATE_DIRECTORY_OPERATION:
      case SASTokenProvider.WRITE_OPERATION:
      case SASTokenProvider.SET_PROPERTIES_OPERATION:
      case SASTokenProvider.LEASE_BLOB_OPERATION:
      case SASTokenProvider.COPY_BLOB_DST_OPERATION:
        sp = "w";
        break;
      case SASTokenProvider.DELETE_OPERATION:
        sp = "d";
        break;
      case SASTokenProvider.DELETE_RECURSIVE_OPERATION:
        sp = "d";
        sr = "d";
        sdd = path.equals(ROOT_PATH)? "0": Integer.toString(StringUtils.countMatches(path, "/"));
        break;
      case SASTokenProvider.CHECK_ACCESS_OPERATION:
      case SASTokenProvider.GET_ACL_OPERATION:
      case SASTokenProvider.GET_STATUS_OPERATION:
        sp = "e";
        break;
      case SASTokenProvider.LIST_OPERATION_BLOB:
        sp = "l";
        sr = "c";
        break;
      case SASTokenProvider.LIST_OPERATION:
        sp = "l";
        sr = "d";
        sdd = path.equals(ROOT_PATH)? "0": Integer.toString(StringUtils.countMatches(path, "/"));
        break;
      case SASTokenProvider.GET_PROPERTIES_OPERATION:
      case SASTokenProvider.READ_OPERATION:
      case SASTokenProvider.COPY_BLOB_SRC_OPERATION:
        sp = "r";
        break;
      case SASTokenProvider.RENAME_DESTINATION_OPERATION:
      case SASTokenProvider.RENAME_SOURCE_OPERATION:
        sp = "m";
        break;
      case SASTokenProvider.SET_ACL_OPERATION:
      case SASTokenProvider.SET_PERMISSION_OPERATION:
        sp = "p";
        break;
      case SASTokenProvider.SET_OWNER_OPERATION:
        sp = "o";
        break;
      default:
        throw new IllegalArgumentException(operation);
    }

    String signature = computeSignatureForSAS(sp, st, se, sv, sr, accountName, containerName,
        path, saoid, suoid, scid);

    AbfsUriQueryBuilder qb = new AbfsUriQueryBuilder();
    qb.addQuery("skoid", skoid);
    qb.addQuery("sktid", sktid);
    qb.addQuery("skt", skt);
    qb.addQuery("ske", ske);
    qb.addQuery("sks", sks);
    qb.addQuery("skv", skv);

    //skdutid and sduoid are required for user bound SAS only
    if (!Objects.equals(skdutid, EMPTY_STRING)) {
      qb.addQuery("skdutid", skdutid);
    }
    if (!Objects.equals(sduoid, EMPTY_STRING)) {
      qb.addQuery("sduoid", sduoid);
    }

    if (saoid != null) {
      qb.addQuery("saoid", saoid);
    }
    if (suoid != null) {
      qb.addQuery("suoid", suoid);
    }
    if (scid != null) {
      qb.addQuery("scid", scid);
    }
    qb.addQuery("sp", sp);
    qb.addQuery("st", st);
    qb.addQuery("se", se);
    qb.addQuery("sv", sv);
    qb.addQuery("sr", sr);
    if (sdd != null) {
      qb.addQuery("sdd", sdd);
    }
    qb.addQuery("sig", signature);
    return qb.toString().substring(1);
  }

  /**
   * Computes the signature for the SAS token based on the provided parameters.
   *
   * @param sp Signed permissions
   * @param st Signed start time
   * @param se Signed expiry time
   * @param sv Signed version
   * @param sr Signed resource
   * @param accountName The storage account name
   * @param containerName The container name
   * @param path The path to the resource
   * @param saoid The Azure AD object ID of the application
   * @param suoid The Azure AD object ID of the user
   * @param scid The correlation ID
   * @return The computed HMAC256 signature
   */
  private String computeSignatureForSAS(String sp, String st, String se, String sv,
      String sr, String accountName, String containerName,
      String path, String saoid, String suoid, String scid) {

    StringBuilder sb = new StringBuilder();
    sb.append(sp);
    sb.append("\n");
    sb.append(st);
    sb.append("\n");
    sb.append(se);
    sb.append("\n");
    // canonicalized resource
    sb.append("/blob/");
    sb.append(accountName);
    sb.append("/");
    sb.append(containerName);
    if (path != null && !sr.equals("c")) {
      sb.append(path);
    }
    sb.append("\n");
    sb.append(skoid);
    sb.append("\n");
    sb.append(sktid);
    sb.append("\n");
    sb.append(skt);
    sb.append("\n");
    sb.append(ske);
    sb.append("\n");
    sb.append(sks);
    sb.append("\n");
    sb.append(skv);
    sb.append("\n");
    if (saoid != null) {
      sb.append(saoid);
    }
    sb.append("\n");
    if (suoid != null) {
      sb.append(suoid);
    }
    sb.append("\n");
    if (scid != null) {
      sb.append(scid);
    }
    sb.append("\n");

    // skdutid, sduoid are sent as empty strings for user-delegation SAS
    // They are only required for user-bound SAS
    if (!Objects.equals(skdutid, EMPTY_STRING)) {
      sb.append(skdutid);
    }
    sb.append("\n");

    if (!Objects.equals(sduoid, EMPTY_STRING)) {
      sb.append(sduoid);
    }
    sb.append("\n");


    sb.append("\n"); // sip
    sb.append("\n"); // spr
    sb.append(sv);
    sb.append("\n");
    sb.append(sr);
    sb.append("\n");
    sb.append("\n"); // - For optional : signedSnapshotTime
    sb.append("\n"); // - For optional :signedEncryptionScope
    sb.append("\n"); // - For optional : rscc - ResponseCacheControl
    sb.append("\n"); // - For optional : rscd - ResponseContentDisposition
    sb.append("\n"); // - For optional : rsce - ResponseContentEncoding
    sb.append("\n"); // - For optional : rscl - ResponseContentLanguage
    //No escape sequence required for optional param rsct - ResponseContentType

    String stringToSign = sb.toString();
    LOG.debug("Delegation SAS stringToSign: " + stringToSign.replace("\n", "."));
    return computeHmac256(stringToSign);
  }
}
