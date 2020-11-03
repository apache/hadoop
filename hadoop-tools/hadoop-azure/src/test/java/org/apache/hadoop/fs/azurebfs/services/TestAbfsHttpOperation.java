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
import java.net.URLEncoder;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestAbfsHttpOperation {

  @Test
  public void testMaskingAndEncoding()
      throws MalformedURLException, UnsupportedEncodingException {
    testIfMaskAndEncodeSuccessful("Where sig is the only query param",
        "http://www.testurl.net?sig=abcd", "http://www.testurl.net?sig=XXXX");

    testIfMaskAndEncodeSuccessful("Where sig is the first query param",
        "http://www.testurl.net?sig=abcd&abc=xyz",
        "http://www.testurl.net?sig=XXXX&abc=xyz");

    testIfMaskAndEncodeSuccessful(
        "Where sig is neither first nor last query param",
        "http://www.testurl.net?lmn=abc&sig=abcd&abc=xyz",
        "http://www.testurl.net?lmn=abc&sig=XXXX&abc=xyz");

    testIfMaskAndEncodeSuccessful("Where sig is the last query param",
        "http://www.testurl.net?abc=xyz&sig=abcd",
        "http://www.testurl.net?abc=xyz&sig=XXXX");

    testIfMaskAndEncodeSuccessful("Where sig query param is not present",
        "http://www.testurl.net?abc=xyz", "http://www.testurl.net?abc=xyz");

    testIfMaskAndEncodeSuccessful(
        "Where sig query param is not present but mysig",
        "http://www.testurl.net?abc=xyz&mysig=qwerty",
        "http://www.testurl.net?abc=xyz&mysig=qwerty");

    testIfMaskAndEncodeSuccessful(
        "Where sig query param is not present but sigmy",
        "http://www.testurl.net?abc=xyz&sigmy=qwerty",
        "http://www.testurl.net?abc=xyz&sigmy=qwerty");

    testIfMaskAndEncodeSuccessful(
        "Where sig query param is not present but a " + "value sig",
        "http://www.testurl.net?abc=xyz&mnop=sig",
        "http://www.testurl.net?abc=xyz&mnop=sig");

    testIfMaskAndEncodeSuccessful(
        "Where sig query param is not present but a " + "value ends with sig",
        "http://www.testurl.net?abc=xyz&mnop=abcsig",
        "http://www.testurl.net?abc=xyz&mnop=abcsig");

    testIfMaskAndEncodeSuccessful(
        "Where sig query param is not present but a " + "value starts with sig",
        "http://www.testurl.net?abc=xyz&mnop=sigabc",
        "http://www.testurl.net?abc=xyz&mnop=sigabc");
  }

  private void testIfMaskAndEncodeSuccessful(final String scenario,
      final String url, final String expectedMaskedUrl)
      throws UnsupportedEncodingException {

    Assertions.assertThat(AbfsHttpOperation.getSignatureMaskedUrl(url))
        .describedAs(url + " (" + scenario + ") after masking should be: "
            + expectedMaskedUrl).isEqualTo(expectedMaskedUrl);

    final String expectedMaskedEncodedUrl = URLEncoder
        .encode(expectedMaskedUrl, "UTF-8");
    Assertions.assertThat(AbfsHttpOperation.encodedUrlStr(expectedMaskedUrl))
        .describedAs(
            url + " (" + scenario + ") after masking and encoding should "
                + "be: " + expectedMaskedEncodedUrl)
        .isEqualTo(expectedMaskedEncodedUrl);
  }

}
