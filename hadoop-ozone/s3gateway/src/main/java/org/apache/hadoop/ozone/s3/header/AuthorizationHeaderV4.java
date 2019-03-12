/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.s3.header;


import com.google.common.base.Preconditions;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Collection;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.apache.commons.lang3.StringUtils.isAllEmpty;
import static org.apache.commons.lang3.StringUtils.isNoneEmpty;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import static org.apache.hadoop.ozone.s3.AWSV4AuthParser.AWS4_SIGNING_ALGORITHM;
import static org.apache.hadoop.ozone.s3.AWSV4AuthParser.DATE_FORMATTER;

/**
 * S3 Authorization header.
 * Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using
 * -authorization-header.html
 */
public class AuthorizationHeaderV4 {

  private final static Logger LOG = LoggerFactory.getLogger(
      AuthorizationHeaderV4.class);

  private final static String CREDENTIAL = "Credential=";
  private final static String SIGNEDHEADERS = "SignedHeaders=";
  private final static String SIGNATURE = "Signature=";

  private String authHeader;
  private String algorithm;
  private String credential;
  private String signedHeadersStr;
  private String signature;
  private Credential credentialObj;
  private Collection<String> signedHeaders;

  /**
   * Construct AuthorizationHeader object.
   * @param header
   */
  public AuthorizationHeaderV4(String header) throws OS3Exception {
    Preconditions.checkNotNull(header);
    this.authHeader = header;
    parseAuthHeader();
  }

  /**
   * This method parses authorization header.
   *
   *  Authorization Header sample:
   *  AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/20181009/us-east-1/s3
   *  /aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date,
   * Signature=db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2
   * @throws OS3Exception
   */
  @SuppressWarnings("StringSplitter")
  public void parseAuthHeader() throws OS3Exception {
    int firstSep = authHeader.indexOf(' ');
    if (firstSep < 0) {
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }

    //split the value parts of the authorization header
    String[] split = authHeader.substring(firstSep + 1).trim().split(", *");

    if (split.length != 3) {
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }

    algorithm = authHeader.substring(0, firstSep);
    validateAlgorithm();
    credential = split[0];
    signedHeadersStr = split[1];
    signature = split[2];
    validateCredentials();
    validateSignedHeaders();
    validateSignature();

  }

  /**
   * Validate Signed headers.
   * */
  private void validateSignedHeaders() throws OS3Exception {
    if (isNoneEmpty(signedHeadersStr)
        && signedHeadersStr.startsWith(SIGNEDHEADERS)) {
      signedHeadersStr = signedHeadersStr.substring(SIGNEDHEADERS.length());
      signedHeaders = StringUtils.getStringCollection(signedHeadersStr, ";");
      if (signedHeaders.size() == 0) {
        LOG.error("No signed headers found. Authheader:{}", authHeader);
        throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
      }
    } else {
      LOG.error("No signed headers found. Authheader:{}", authHeader);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
  }

  /**
   * Validate signature.
   * */
  private void validateSignature() throws OS3Exception {
    if (signature.startsWith(SIGNATURE)) {
      signature = signature.substring(SIGNATURE.length());
      if (!isNoneEmpty(signature)) {
        LOG.error("Signature can't be empty.", signature);
        throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
      }
      try {
        Hex.decodeHex(signature);
      } catch (DecoderException e) {
        LOG.error("Signature:{} should be in hexa-decimal encoding.",
            signature);
        throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
      }
    } else {
      LOG.error("Signature can't be empty.", signature);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
  }

  /**
   * Validate credentials.
   * */
  private void validateCredentials() throws OS3Exception {
    if (isNoneEmpty(credential) && credential.startsWith(CREDENTIAL)) {
      credential = credential.substring(CREDENTIAL.length());
      // Parse credential. Other parts of header are not validated yet. When
      // security comes, it needs to be completed.
      credentialObj = new Credential(credential);
    } else {
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }

    if (credentialObj.getAccessKeyID().isEmpty()) {
      LOG.error("AWS access id shouldn't be empty. credential:{}", credential);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
    if (credentialObj.getAwsRegion().isEmpty()) {
      LOG.error("AWS region shouldn't be empty. credential:{}", credential);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
    if (credentialObj.getAwsRequest().isEmpty()) {
      LOG.error("AWS request shouldn't be empty. credential:{}", credential);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
    if (credentialObj.getAwsService().isEmpty()) {
      LOG.error("AWS service:{} shouldn't be empty. credential:{}",
          credential);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }

    // Date should not be empty and within valid range.
    if (!credentialObj.getDate().isEmpty()) {
      LocalDate date = LocalDate.parse(credentialObj.getDate(), DATE_FORMATTER);
      LocalDate now = LocalDate.now();
      if (date.isBefore(now.minus(1, DAYS)) ||
          date.isAfter(now.plus(1, DAYS))) {
        LOG.error("AWS date not in valid range. Date:{} should not be older " +
                "than 1 day(i.e yesterday) and greater than 1 day(i.e " +
                "tomorrow).",
            getDate());
        throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
      }
    } else {
      LOG.error("AWS date shouldn't be empty. credential:{}", credential);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
  }

  /**
   * Validate if algorithm is in expected format.
   * */
  private void validateAlgorithm() throws OS3Exception {
    if (isAllEmpty(algorithm) || !algorithm.equals(AWS4_SIGNING_ALGORITHM)) {
      LOG.error("Unexpected hash algorithm. Algo:{}", algorithm);
      throw S3ErrorTable.newError(MALFORMED_HEADER, authHeader);
    }
  }

  public String getAuthHeader() {
    return authHeader;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public String getCredential() {
    return credential;
  }

  public String getSignedHeaderString() {
    return signedHeadersStr;
  }

  public String getSignature() {
    return signature;
  }

  public String getAccessKeyID() {
    return credentialObj.getAccessKeyID();
  }

  public String getDate() {
    return credentialObj.getDate();
  }

  public String getAwsRegion() {
    return credentialObj.getAwsRegion();
  }

  public String getAwsService() {
    return credentialObj.getAwsService();
  }

  public String getAwsRequest() {
    return credentialObj.getAwsRequest();
  }

  public Collection<String> getSignedHeaders() {
    return signedHeaders;
  }

  public Credential getCredentialObj() {
    return credentialObj;
  }
}
