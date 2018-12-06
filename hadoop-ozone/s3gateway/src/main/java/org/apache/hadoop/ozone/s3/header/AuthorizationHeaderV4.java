/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.header;


import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * S3 Authorization header.
 * Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using
 * -authorization-header.html
 */
public class AuthorizationHeaderV4 {

  private final static String CREDENTIAL = "Credential=";
  private final static String SIGNEDHEADERS= "SignedHeaders=";
  private final static String SIGNATURE = "Signature=";

  private String authHeader;
  private String algorithm;
  private String credential;
  private String signedHeaders;
  private String signature;
  private Credential credentialObj;

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
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    //split the value parts of the authorization header
    String[] split = authHeader.substring(firstSep + 1).trim().split(", *");

    if (split.length != 3) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    algorithm = authHeader.substring(0, firstSep);
    credential = split[0];
    signedHeaders = split[1];
    signature = split[2];

    if (credential.startsWith(CREDENTIAL)) {
      credential = credential.substring(CREDENTIAL.length());
    } else {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    if (signedHeaders.startsWith(SIGNEDHEADERS)) {
      signedHeaders = signedHeaders.substring(SIGNEDHEADERS.length());
    } else {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    if (signature.startsWith(SIGNATURE)) {
      signature = signature.substring(SIGNATURE.length());
    } else {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    // Parse credential. Other parts of header are not validated yet. When
    // security comes, it needs to be completed.
    credentialObj = new Credential(credential);

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

  public String getSignedHeaders() {
    return signedHeaders;
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

}
