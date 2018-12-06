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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class tests Authorization header format v2.
 */

public class TestAuthorizationHeaderV4 {

  @Test
  public void testV4HeaderWellFormed() throws Exception {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=ozone/20130524/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);
    assertEquals("AWS4-HMAC-SHA256", v4.getAlgorithm());
    assertEquals("ozone", v4.getAccessKeyID());
    assertEquals("20130524", v4.getDate());
    assertEquals("us-east-1", v4.getAwsRegion());
    assertEquals("aws4_request", v4.getAwsRequest());
    assertEquals("host;range;x-amz-date", v4.getSignedHeaders());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", v4.getSignature());
  }

  @Test
  public void testV4HeaderMissingParts() {
    try {
      String auth = "AWS4-HMAC-SHA256 " +
          "Credential=ozone/20130524/us-east-1/s3/aws4_request, " +
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
          "Credential=20130524/us-east-1/s3/aws4_request, " +
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
        "AWS4-HMAC-SHA256 Credential=ozone/20130524/us-east-1/s3/aws4_request,"
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            + "Signature"
            + "=fe5f80f77d5fa3beca038a248ff027";
    AuthorizationHeaderV4 v4 = new AuthorizationHeaderV4(auth);

    assertEquals("AWS4-HMAC-SHA256", v4.getAlgorithm());
    assertEquals("ozone", v4.getAccessKeyID());
    assertEquals("20130524", v4.getDate());
    assertEquals("us-east-1", v4.getAwsRegion());
    assertEquals("aws4_request", v4.getAwsRequest());
    assertEquals("host;x-amz-content-sha256;x-amz-date",
        v4.getSignedHeaders());
    assertEquals("fe5f80f77d5fa3beca038a248ff027", v4.getSignature());

  }

}
