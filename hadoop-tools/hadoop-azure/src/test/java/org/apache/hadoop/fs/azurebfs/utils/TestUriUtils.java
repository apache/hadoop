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

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;

import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.changeUrlFromBlobToDfs;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.changeUrlFromDfsToBlob;
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
  // If a config for partial masking is introduced, this test will have to be
  // modified for the config-controlled partial mask length
  public void testMaskUrlQueryParameters() throws Exception {
    Set<String> fullMask = new HashSet<>(Arrays.asList("abc", "bcd"));
    Set<String> partialMask = new HashSet<>(Arrays.asList("pqr", "xyz"));

    //Partial and full masking test
    List<NameValuePair> keyValueList = URLEncodedUtils
        .parse("abc=123&pqr=45678&def=789&bcd=012&xyz=678",
            StandardCharsets.UTF_8);
    Assert.assertEquals("Incorrect masking",
        "abc=XXXXX&pqr=456XX&def=789&bcd=XXXXX&xyz=67X",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Mask GUIDs
    keyValueList = URLEncodedUtils
        .parse("abc=123&pqr=256877f2-c094-48c8-83df-ddb5825694fd&def=789",
            StandardCharsets.UTF_8);
    Assert.assertEquals("Incorrect partial masking for guid",
        "abc=XXXXX&pqr=256877f2-c094-48c8XXXXXXXXXXXXXXXXXX&def=789",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //For params entered for both full and partial masks, full mask applies
    partialMask.add("abc");
    Assert.assertEquals("Full mask should apply",
        "abc=XXXXX&pqr=256877f2-c094-48c8XXXXXXXXXXXXXXXXXX&def=789",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Duplicate key (to be masked) with different values
    keyValueList = URLEncodedUtils
        .parse("abc=123&pqr=4561234&abc=789", StandardCharsets.UTF_8);
    Assert.assertEquals("Duplicate key: Both values should get masked",
        "abc=XXXXX&pqr=4561XXX&abc=XXXXX",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Duplicate key (not to be masked) with different values
    keyValueList = URLEncodedUtils
        .parse("abc=123&def=456&pqrs=789&def=000", StandardCharsets.UTF_8);
    Assert.assertEquals("Duplicate key: Values should not get masked",
        "abc=XXXXX&def=456&pqrs=789&def=000",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Empty param value
    keyValueList = URLEncodedUtils
        .parse("abc=123&def=&pqr=789&s=1", StandardCharsets.UTF_8);
    Assert.assertEquals("Incorrect url with empty query value",
        "abc=XXXXX&def=&pqr=78X&s=1",
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
        .parse("abc=123&s=1", StandardCharsets.UTF_8);
    keyValueList.add(new BasicNameValuePair("null1", null));
    Assert.assertEquals("Null value, incorrect query construction",
        "abc=XXXXX&s=1&null1=",
        UriUtils.maskUrlQueryParameters(keyValueList, fullMask, partialMask));

    //Param (to be masked) with null value
    keyValueList.add(new BasicNameValuePair("null2", null));
    fullMask.add("null2");
    Assert.assertEquals("No mask should be added for null value",
        "abc=XXXXX&s=1&null1=&null2=", UriUtils
            .maskUrlQueryParameters(keyValueList, fullMask,
                partialMask)); //no mask
  }

  @Test
  public void testConvertUrlFromDfsToBlob() throws Exception{
    List<String> inputUrls = Arrays.asList(
        "https://accountName.dfs.core.windows.net/containerName",
        "https://accountName.blob.core.windows.net/containerName", // Already blob will remain blob
        "https:/accountName.dfs.core.windows.net/containerName", // Invalid URL will be returned as it is
        "https://accountNamedfs.dfs.core.windows.net/containerName",
        "https://accountNameblob.dfs.core.windows.net/containerName",
        "https://accountName.dfs.core.windows.net/dfsContainer",
        "https://accountName.dfs.core.windows.net/blobcontainerName",
        "https://accountName.dfs.core.windows.net/dfs.Container",
        "https://accountName.dfs.core.windows.net/blob.containerName");
    List<String> expectedUrls = Arrays.asList(
        "https://accountName.blob.core.windows.net/containerName",
        "https://accountName.blob.core.windows.net/containerName", // Already blob will remain blob
        "https:/accountName.dfs.core.windows.net/containerName", // Invalid URL will be returned as it is
        "https://accountNamedfs.blob.core.windows.net/containerName",
        "https://accountNameblob.blob.core.windows.net/containerName",
        "https://accountName.blob.core.windows.net/dfsContainer",
        "https://accountName.blob.core.windows.net/blobcontainerName",
        "https://accountName.blob.core.windows.net/dfs.Container",
        "https://accountName.blob.core.windows.net/blob.containerName");

    for (int i = 0; i < inputUrls.size(); i++) {
      Assertions.assertThat(changeUrlFromDfsToBlob(new URL(inputUrls.get(i))).toString())
          .describedAs("URL conversion not as expected").isEqualTo(expectedUrls.get(i));
    }
  }

  @Test
  public void testConvertUrlFromBlobToDfs() throws Exception{
    List<String> inputUrls = Arrays.asList(
        "https://accountName.blob.core.windows.net/containerName",
        "https://accountName.dfs.core.windows.net/containerName",
        "https://accountNamedfs.blob.core.windows.net/containerName",
        "https://accountNameblob.blob.core.windows.net/containerName",
        "https://accountName.blob.core.windows.net/dfsContainer",
        "https://accountName.blob.core.windows.net/blobcontainerName",
        "https://accountName.blob.core.windows.net/dfs.Container",
        "https://accountName.blob.core.windows.net/blob.containerName");
    List<String> expectedUrls = Arrays.asList(
        "https://accountName.dfs.core.windows.net/containerName",
        "https://accountName.dfs.core.windows.net/containerName",
        "https://accountNamedfs.dfs.core.windows.net/containerName",
        "https://accountNameblob.dfs.core.windows.net/containerName",
        "https://accountName.dfs.core.windows.net/dfsContainer",
        "https://accountName.dfs.core.windows.net/blobcontainerName",
        "https://accountName.dfs.core.windows.net/dfs.Container",
        "https://accountName.dfs.core.windows.net/blob.containerName");

    for (int i = 0; i < inputUrls.size(); i++) {
      Assertions.assertThat(changeUrlFromBlobToDfs(new URL(inputUrls.get(i))).toString())
          .describedAs("Url Conversion Not as Expected").isEqualTo(expectedUrls.get(i));
    }
  }
}
