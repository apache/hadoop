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

import org.apache.hadoop.classification.InterfaceAudience;

import java.time.format.DateTimeFormatter;

/**
 * AWS constants.
 */
@InterfaceAudience.Private
public final class AWSConstants {

  private AWSConstants() {
  }

  public static final String LINE_SEPARATOR = "\n";

  public static final String AWS4_TERMINATOR = "aws4_request";

  public static final String AWS4_SIGNING_ALGORITHM = "AWS4-HMAC-SHA256";

  /**
   * Seconds in a week, which is the max expiration time Sig-v4 accepts.
   */
  public static final long PRESIGN_URL_MAX_EXPIRATION_SECONDS =
      60 * 60 * 24 * 7;

  public static final String X_AMZ_SECURITY_TOKEN = "X-Amz-Security-Token";

  public static final String X_AMZ_CREDENTIAL = "X-Amz-Credential";

  public static final String X_AMZ_DATE = "X-Amz-Date";

  public static final String X_AMZ_EXPIRES = "X-Amz-Expires";

  public static final String X_AMZ_SIGNED_HEADER = "X-Amz-SignedHeaders";

  public static final String X_AMZ_CONTENT_SHA256 = "x-amz-content-sha256";

  public static final String X_AMZ_SIGNATURE = "X-Amz-Signature";

  public static final String X_AMZ_ALGORITHM = "X-Amz-Algorithm";

  public static final String AUTHORIZATION = "Authorization";

  public static final String HOST = "Host";

  public static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd");

}
