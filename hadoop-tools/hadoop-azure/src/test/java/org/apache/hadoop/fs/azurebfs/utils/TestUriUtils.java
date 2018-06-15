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

import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals("abfs", UriUtils.extractRawAccountFromAccountName("abfs.dfs.core.windows.net"));
    Assert.assertEquals("abfs", UriUtils.extractRawAccountFromAccountName("abfs.dfs.preprod.core.windows.net"));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName("abfs.dfs.cores.windows.net"));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName(""));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName(null));
    Assert.assertEquals(null, UriUtils.extractRawAccountFromAccountName("abfs.dfs.cores.windows.net"));
  }
}
