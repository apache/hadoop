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

package org.apache.hadoop.fs.azurebfs.utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test ABFS UriUtils.
 */
public final class TestUriUtils {
  @Test
  public void testIfUriContainsAbfs() throws Exception {
    Assert.assertTrue(UriUtils.containsAbfsUrl("abfs.dfs.core.windows.net"));
    Assert.assertTrue(UriUtils.containsAbfsUrl("abfs.dfs.preprod.core.windows.net"));
    Assert.assertFalse(UriUtils.containsAbfsUrl("abfs.dfs.cores.windows.net"));
    Assert.assertFalse(UriUtils.containsAbfsUrl(""));
    Assert.assertFalse(UriUtils.containsAbfsUrl(null));
    Assert.assertFalse(UriUtils.containsAbfsUrl("abfs.dfs.cores.windows.net"));
    Assert.assertFalse(UriUtils.containsAbfsUrl("xhdfs.blob.core.windows.net"));
  }

  @Test
  public void testExtractRawAccountName() throws Exception {
    Assert.assertEquals("abfs", UriUtils.extractAccountNameFromHostName("abfs.dfs.core.windows.net"));
    Assert.assertEquals("abfs", UriUtils.extractAccountNameFromHostName("abfs.dfs.preprod.core.windows.net"));
    Assert.assertEquals(null, UriUtils.extractAccountNameFromHostName("abfs.dfs.cores.windows.net"));
    Assert.assertEquals(null, UriUtils.extractAccountNameFromHostName(""));
    Assert.assertEquals(null, UriUtils.extractAccountNameFromHostName(null));
    Assert.assertEquals(null, UriUtils.extractAccountNameFromHostName("abfs.dfs.cores.windows.net"));
  }

  @Test
  public void testMaskUrlQueryParameters() throws Exception {
    ArrayList<String> fullMask = new ArrayList<>(Arrays.asList("abc=", "bcd="));
    ArrayList<String> partialMask = new ArrayList<>(
        Arrays.asList("pqr=", "xyz="));

    //Partial and full masking test
    List<NameValuePair> keyValueList = URLEncodedUtils
        .parse("abc=123&pqr=456&def=789&bcd=012&xyz=678", StandardCharsets.UTF_8);
    Assert.assertEquals("Incorrect masking",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask),
        "abc=XXXX&pqr=456XXXX&def=789&bcd=XXXX&xyz=678XXXX");

    //For params entered for both full and partial masks, full mask applies
    partialMask.add("abc=");
    Assert.assertEquals("Full mask should apply",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask),
        "abc=XXXX&pqr=456XXXX&def=789&bcd=XXXX&xyz=678XXXX");

    //Duplicate key (to be masked) with different values
    keyValueList = URLEncodedUtils
        .parse("abc=123&pqr=456&abc=789", StandardCharsets.UTF_8);
    Assert.assertEquals("Duplicate key: Both values should get masked",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask),
        "abc=XXXX&pqr=456XXXX&abc=XXXX");

    //Duplicate key (not to be masked) with different values
    keyValueList = URLEncodedUtils
        .parse("abc=123&def=456&pqr=789&def=000&s=1", StandardCharsets.UTF_8);
    Assert.assertEquals("Duplicate key: no value should get masked",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask),
        "abc=XXXX&def=456&pqr=789XXXX&def=000&s=1");

    //Empty param value
    keyValueList = URLEncodedUtils
        .parse("abc=123&def=&pqr=789&s=1", StandardCharsets.UTF_8);
    Assert.assertEquals("Incorrect url with empty query value",
        "abc=XXXX&def=&pqr=789XXXX&s=1",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Empty param key
    keyValueList = URLEncodedUtils
        .parse("def=2&pqr=789&s=1", StandardCharsets.UTF_8);
    keyValueList.add(new BasicNameValuePair("", "m1"));
    List<NameValuePair> finalKeyValueList = keyValueList;
    intercept(IllegalArgumentException.class, () -> UriUtils
        .maskUrlQueryParameters(finalKeyValueList, fullMask, partialMask));

    //Param (not to be masked) with null value
    keyValueList = URLEncodedUtils
        .parse("abc=123&pqr=789&s=1", StandardCharsets.UTF_8);
    keyValueList.add(new BasicNameValuePair("null1", null));
    Assert.assertEquals("Null value, incorrect query construction",
        "abc=XXXX&pqr=789XXXX&s=1&null1=",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Param (to be masked) with null value
    keyValueList.add(new BasicNameValuePair("null2", null));
    fullMask.add("null2");
    Assert.assertEquals("No mask should be added for null value",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask),
        "abc=XXXX&pqr=789XXXX&s=1&null1=&null2="); //no mask
  }
}
