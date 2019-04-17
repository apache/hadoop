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

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Authorization Header v2.
 */
public class AuthorizationHeaderV2 {

  private final static String IDENTIFIER = "AWS";
  private String authHeader;
  private String identifier;
  private String accessKeyID;
  private String signature;

  public AuthorizationHeaderV2(String auth) throws OS3Exception {
    Preconditions.checkNotNull(auth);
    this.authHeader = auth;
    parseHeader();
  }

  /**
   * This method parses the authorization header.
   *
   * Authorization header sample:
   * AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg=
   *
   * @throws OS3Exception
   */
  @SuppressWarnings("StringSplitter")
  public void parseHeader() throws OS3Exception {
    String[] split = authHeader.split(" ");
    if (split.length != 2) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    identifier = split[0];
    if (!IDENTIFIER.equals(identifier)) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    String[] remainingSplit = split[1].split(":");

    if (remainingSplit.length != 2) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    accessKeyID = remainingSplit[0];
    signature = remainingSplit[1];
    if (isBlank(accessKeyID) || isBlank(signature)) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }
  }

  public String getAuthHeader() {
    return authHeader;
  }

  public void setAuthHeader(String authHeader) {
    this.authHeader = authHeader;
  }

  public String getIdentifier() {
    return identifier;
  }

  public String getAccessKeyID() {
    return accessKeyID;
  }

  public String getSignature() {
    return signature;
  }

}
