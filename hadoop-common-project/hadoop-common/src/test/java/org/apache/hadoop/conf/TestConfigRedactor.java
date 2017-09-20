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

package org.apache.hadoop.conf;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests the tool (and the default expression) for deciding which config
 * redact.
 */
public class TestConfigRedactor {
  private static final String REDACTED_TEXT = "<redacted>";

  private static final String ORIGINAL_VALUE = "Hello, World!";

  @Test
  public void testRedactWithCoreDefault() throws Exception {
    Configuration conf = new Configuration();
    testRedact(conf);
  }

  @Test
  public void testRedactNoCoreDefault() throws Exception {
    Configuration conf = new Configuration(false);
    testRedact(conf);
  }

  private void testRedact(Configuration conf) throws Exception {
    ConfigRedactor redactor = new ConfigRedactor(conf);
    String processedText;

    List<String> sensitiveKeys = Arrays.asList(
        "fs.s3a.secret.key",
        "fs.s3a.bucket.BUCKET.secret.key",
        "fs.s3a.server-side-encryption.key",
        "fs.s3a.bucket.engineering.server-side-encryption.key",
        "fs.azure.account.key.abcdefg.blob.core.windows.net",
        "fs.adl.oauth2.refresh.token",
        "fs.adl.oauth2.credential",
        "dfs.adls.oauth2.refresh.token",
        "dfs.adls.oauth2.credential",
        "dfs.webhdfs.oauth2.access.token",
        "dfs.webhdfs.oauth2.refresh.token",
        "ssl.server.keystore.keypassword",
        "ssl.server.keystore.password",
        "httpfs.ssl.keystore.pass",
        "hadoop.security.sensitive-config-keys"
    );
    for (String key : sensitiveKeys) {
      processedText = redactor.redact(key, ORIGINAL_VALUE);
      Assert.assertEquals(
          "Config parameter wasn't redacted and should be: " + key,
          REDACTED_TEXT, processedText);
    }

    List<String> normalKeys = Arrays.asList(
        "fs.defaultFS",
        "dfs.replication",
        "ssl.server.keystore.location",
        "httpfs.config.dir",
        "hadoop.security.credstore.java-keystore-provider.password-file",
        "fs.s3a.bucket.engineering.server-side-encryption-algorithm"
    );
    for (String key : normalKeys) {
      processedText = redactor.redact(key, ORIGINAL_VALUE);
      Assert.assertEquals(
          "Config parameter was redacted and shouldn't be: " + key,
          ORIGINAL_VALUE, processedText);
    }
  }
}
