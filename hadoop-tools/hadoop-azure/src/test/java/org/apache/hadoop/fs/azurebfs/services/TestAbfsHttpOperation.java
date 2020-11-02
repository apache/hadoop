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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation.getAbfsHttpOperationWithFixedResult;

public class TestAbfsHttpOperation {

  @Test
  public void testForURLs()
      throws MalformedURLException, UnsupportedEncodingException {
    testIfMaskedSuccessfully("Where sig is the only query param"
        ,"http://www.testurl.net?sig=abcd"
        ,"http://www.testurl.net?sig=XXXX");

    testIfMaskedSuccessfully("Where sig is the first query param"
        ,"http://www.testurl.net?sig=abcd&abc=xyz"
        ,"http://www.testurl.net?sig=XXXX&abc=xyz");

    testIfMaskedSuccessfully("Where sig is neither first nor last query param"
        ,"http://www.testurl.net?lmn=abc&sig=abcd&abc=xyz"
        ,"http://www.testurl.net?lmn=abc&sig=XXXX&abc=xyz");

    testIfMaskedSuccessfully("Where sig is the last query param"
        ,"http://www.testurl.net?abc=xyz&sig=abcd"
        ,"http://www.testurl.net?abc=xyz&sig=XXXX");

    testIfMaskedSuccessfully("Where sig query param is not present"
        ,"http://www.testurl.net?abc=xyz"
        ,"http://www.testurl.net?abc=xyz");
  }

  private void testIfMaskedSuccessfully(String scenario, String url,
      String expectedMaskedUrl)
      throws MalformedURLException, UnsupportedEncodingException {
    AbfsHttpOperation abfsHttpOperation = getAbfsHttpOperationWithFixedResult(
        new URL(url), "GET", 200);

    Assertions.assertThat(abfsHttpOperation.getSignatureMaskedUrlStr())
        .describedAs(url+" ("+scenario+") after masking should be: "+expectedMaskedUrl)
        .isEqualToIgnoringCase(expectedMaskedUrl);

    final String expectedMaskedEncodedUrl =
        URLEncoder.encode(expectedMaskedUrl, "UTF-8");
    Assertions.assertThat(abfsHttpOperation.getSignatureMaskedEncodedUrlStr())
        .describedAs(url+" ("+scenario+") after masking and encoding should "
            + "be: "+expectedMaskedEncodedUrl)
        .isEqualToIgnoringCase(expectedMaskedEncodedUrl);
  }

}
