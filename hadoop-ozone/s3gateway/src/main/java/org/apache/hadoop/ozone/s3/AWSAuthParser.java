/*
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
package org.apache.hadoop.ozone.s3;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/*
 * Parser to request auth parser for http request.
 * */
interface AWSAuthParser {

  String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
  String NEWLINE = "\n";
  String CONTENT_TYPE = "content-type";
  String X_AMAZ_DATE = "X-Amz-Date";
  String CONTENT_MD5 = "content-md5";
  String AUTHORIZATION_HEADER = "Authorization";
  Charset UTF_8 = Charset.forName("utf-8");
  String X_AMZ_CONTENT_SHA256 = "X-Amz-Content-SHA256";
  String HOST = "host";

  String AWS4_TERMINATOR = "aws4_request";

  String AWS4_SIGNING_ALGORITHM = "AWS4-HMAC-SHA256";

  /**
   * Seconds in a week, which is the max expiration time Sig-v4 accepts.
   */
  long PRESIGN_URL_MAX_EXPIRATION_SECONDS =
      60 * 60 * 24 * 7;

  String X_AMZ_SECURITY_TOKEN = "X-Amz-Security-Token";

  String X_AMZ_CREDENTIAL = "X-Amz-Credential";

  String X_AMZ_DATE = "X-Amz-Date";

  String X_AMZ_EXPIRES = "X-Amz-Expires";

  String X_AMZ_SIGNED_HEADER = "X-Amz-SignedHeaders";

  String X_AMZ_SIGNATURE = "X-Amz-Signature";

  String X_AMZ_ALGORITHM = "X-Amz-Algorithm";

  String AUTHORIZATION = "Authorization";

  String HOST_HEADER = "Host";

  DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd");

  DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
          .withZone(ZoneOffset.UTC);

  /**
   * API to return string to sign.
   */
  String getStringToSign() throws Exception;
}
