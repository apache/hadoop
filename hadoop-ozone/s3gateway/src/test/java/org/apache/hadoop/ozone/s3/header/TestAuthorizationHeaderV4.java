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

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.apache.hadoop.ozone.s3.AWSV4AuthParser.DATE_FORMATTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class tests Authorization header format v2.
 */

public class TestAuthorizationHeaderV4 {
  private String curDate;

  @Before
  public void setup() {
    LocalDate now = LocalDate.now();
    curDate = DATE_FORMATTER.format(now);
  }

  @Test
  public void testV4HeaderWellFormed() throws Exception {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);
    assertEquals("AWS4-HMAC-SHA256", v4.getAlgorithm());
    assertEquals("ozone", v4.getAccessKeyID());
    assertEquals(curDate, v4.getDate());
    assertEquals("us-east-1", v4.getAwsRegion());
    assertEquals("aws4_request", v4.getAwsRequest());
    assertEquals("host;range;x-amz-date", v4.getSignedHeaderString());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", v4.getSignature());
  }

  @Test
  public void testV4HeaderMissingParts() {
    try {
      String auth = "AWS4-HMAC-SHA256 " +
          "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
          "SignedHeaders=host;range;x-amz-date,";
      AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);
      fail("Exception is expected in case of malformed header");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testV4HeaderInvalidCredential() {
    try {
      String auth = "AWS4-HMAC-SHA256 " +
          "Credential=" + curDate + "/us-east-1/s3/aws4_request, " +
          "SignedHeaders=host;range;x-amz-date, " +
          "Signature=fe5f80f77d5fa3beca038a248ff027";
      AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);
      fail("Exception is expected in case of malformed header");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testV4HeaderWithoutSpace() throws OS3Exception {

    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);

    assertEquals("AWS4-HMAC-SHA256", v4.getAlgorithm());
    assertEquals("ozone", v4.getAccessKeyID());
    assertEquals(curDate, v4.getDate());
    assertEquals("us-east-1", v4.getAwsRegion());
    assertEquals("aws4_request", v4.getAwsRequest());
    assertEquals("host;x-amz-content-sha256;x-amz-date",
        v4.getSignedHeaderString());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", v4.getSignature());

  }

  @Test
  public void testV4HeaderDateValidationSuccess() throws OS3Exception {
    // Case 1: valid date within range.
    LocalDate now = LocalDate.now();
    String dateStr = DATE_FORMATTER.format(now);
    validateResponse(dateStr);

    // Case 2: Valid date with in range.
    dateStr = DATE_FORMATTER.format(now.plus(1, DAYS));
    validateResponse(dateStr);

    // Case 3: Valid date with in range.
    dateStr = DATE_FORMATTER.format(now.minus(1, DAYS));
    validateResponse(dateStr);
  }

  @Test
  public void testV4HeaderDateValidationFailure() throws Exception {
    // Case 1: Empty date.
    LocalDate now = LocalDate.now();
    String dateStr = "";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> validateResponse(dateStr));

    // Case 2: Date after yesterday.
    String dateStr2 = DATE_FORMATTER.format(now.plus(2, DAYS));
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> validateResponse(dateStr2));

    // Case 3: Date before yesterday.
    String dateStr3 = DATE_FORMATTER.format(now.minus(2, DAYS));
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> validateResponse(dateStr3));
  }

  private void validateResponse(String dateStr) throws OS3Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + dateStr + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);

    assertEquals("AWS4-HMAC-SHA256", v4.getAlgorithm());
    assertEquals("ozone", v4.getAccessKeyID());
    assertEquals(dateStr, v4.getDate());
    assertEquals("us-east-1", v4.getAwsRegion());
    assertEquals("aws4_request", v4.getAwsRequest());
    assertEquals("host;x-amz-content-sha256;x-amz-date",
        v4.getSignedHeaderString());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", v4.getSignature());
  }

  @Test
  public void testV4HeaderRegionValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "//s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));
    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));
  }

  @Test
  public void testV4HeaderServiceValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1" +
            "//aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));
  }

  @Test
  public void testV4HeaderRequestValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/   ,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            ","
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth3));
  }

  @Test
  public void testV4HeaderSignedHeaderValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=;;,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "=x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth3));

    String auth4 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "=,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth4));
  }

  @Test
  public void testV4HeaderSignatureValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027%";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));

    String auth2 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));

    String auth3 =
        "AWS4-HMAC-SHA256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + ""
            + "=";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth3));
  }

  @Test
  public void testV4HeaderHashAlgoValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));

    String auth2 =
        "SHA-256 Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));

    String auth3 =
        " Credential=ozone/" + curDate + "/us-east-1/s3" +
            "/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth3));
  }

  @Test
  public void testV4HeaderCredentialValidationFailure() throws Exception {
    String auth =
        "AWS4-HMAC-SHA Credential=/" + curDate + "//" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth));

    String auth2 =
        "AWS4-HMAC-SHA =/" + curDate + "//" +
            "/,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    LambdaTestUtils.intercept(OS3Exception.class, "",
        () -> new AuthorizationHeaderV4(auth2));
  }

}
