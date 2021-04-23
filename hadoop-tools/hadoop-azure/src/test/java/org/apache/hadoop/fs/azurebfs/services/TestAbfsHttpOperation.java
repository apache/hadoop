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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

public class TestAbfsHttpOperation {

  @Test
  public void testMaskingAndEncoding()
      throws MalformedURLException, UnsupportedEncodingException {
    testIfMaskAndEncodeSuccessful("Where sig is the only query param",
        "http://www.testurl.net?sig=abcd", "http://www.testurl.net?sig=XXXXX");

    testIfMaskAndEncodeSuccessful("Where oid is the only query param",
        "http://www.testurl.net?saoid=abcdef",
        "http://www.testurl.net?saoid=abcXXX");

    testIfMaskAndEncodeSuccessful("Where sig is the first query param, oid is last",
        "http://www.testurl.net?sig=abcd&abc=xyz&saoid=pqrs456",
        "http://www.testurl.net?sig=XXXXX&abc=xyz&saoid=pqrsXXX");

    testIfMaskAndEncodeSuccessful(
        "Where sig/oid are neither first nor last query param",
        "http://www.testurl.net?lmn=abc&sig=abcd&suoid=mnop789&abc=xyz",
        "http://www.testurl.net?lmn=abc&sig=XXXXX&suoid=mnopXXX&abc=xyz");

    testIfMaskAndEncodeSuccessful("Where sig is the last query param, oid is first",
        "http://www.testurl.net?skoid=pqrs123&abc=xyz&sig=abcd",
        "http://www.testurl.net?skoid=pqrsXXX&abc=xyz&sig=XXXXX");

    testIfMaskAndEncodeSuccessful("Where sig/oid query param are not present",
        "http://www.testurl.net?abc=xyz", "http://www.testurl.net?abc=xyz");

    testIfMaskAndEncodeSuccessful(
        "Where sig/oid query param are not present but mysig and myoid",
        "http://www.testurl.net?abc=xyz&mysig=qwerty&mysaoid=uvw",
        "http://www.testurl.net?abc=xyz&mysig=qwerty&mysaoid=uvw");

    testIfMaskAndEncodeSuccessful(
        "Where sig/oid query param is not present but sigmy and oidmy",
        "http://www.testurl.net?abc=xyz&sigmy=qwerty&skoidmy=uvw",
        "http://www.testurl.net?abc=xyz&sigmy=qwerty&skoidmy=uvw");

    testIfMaskAndEncodeSuccessful(
        "Where sig/oid query param is not present but values sig and oid",
        "http://www.testurl.net?abc=xyz&mnop=sig&pqr=saoid",
        "http://www.testurl.net?abc=xyz&mnop=sig&pqr=saoid");

    testIfMaskAndEncodeSuccessful(
        "Where sig/oid query param is not present but a value ends with sig/oid",
        "http://www.testurl.net?abc=xyzsaoid&mnop=abcsig",
        "http://www.testurl.net?abc=xyzsaoid&mnop=abcsig");

    testIfMaskAndEncodeSuccessful(
        "Where sig/oid query param is not present but a value starts with sig/oid",
        "http://www.testurl.net?abc=saoidxyz&mnop=sigabc",
        "http://www.testurl.net?abc=saoidxyz&mnop=sigabc");
  }

  @Test
  public void testUrlWithNullValues()
      throws MalformedURLException, UnsupportedEncodingException {
    testIfMaskAndEncodeSuccessful("Where param to be masked has null value",
        "http://www.testurl.net?abc=xyz&saoid=&mnop=abcsig",
        "http://www.testurl.net?abc=xyz&saoid=&mnop=abcsig");
    testIfMaskAndEncodeSuccessful("Where visible param has null value",
        "http://www.testurl.net?abc=xyz&pqr=&mnop=abcd",
        "http://www.testurl.net?abc=xyz&pqr=&mnop=abcd");
    testIfMaskAndEncodeSuccessful("Where last param has null value",
        "http://www.testurl.net?abc=xyz&pqr=&mnop=",
        "http://www.testurl.net?abc=xyz&pqr=&mnop=");
  }

  private void testIfMaskAndEncodeSuccessful(final String scenario,
      final String url, final String expectedMaskedUrl)
      throws UnsupportedEncodingException, MalformedURLException {

    Assertions.assertThat(UriUtils.getMaskedUrl(new URL(url)))
        .describedAs(url + " (" + scenario + ") after masking should be: "
            + expectedMaskedUrl).isEqualTo(expectedMaskedUrl);

    final String expectedMaskedEncodedUrl = URLEncoder
        .encode(expectedMaskedUrl, "UTF-8");
    Assertions.assertThat(UriUtils.encodedUrlStr(expectedMaskedUrl))
        .describedAs(
            url + " (" + scenario + ") after masking and encoding should "
                + "be: " + expectedMaskedEncodedUrl)
        .isEqualTo(expectedMaskedEncodedUrl);
  }

}
