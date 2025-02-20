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
package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestSharedKeyAuth extends AbstractAbfsIntegrationTest {

  public ITestSharedKeyAuth() throws Exception {
    super();
  }

  @Test
  public void testWithWrongSharedKey() throws Exception {
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
    Configuration config = this.getRawConfiguration();
    config.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
        true);
    String accountName = this.getAccountName();
    String configkKey = FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME + "." + accountName;
    // a wrong sharedKey
    String secret = "XjUjsGherkDpljuyThd7RpljhR6uhsFjhlxRpmhgD12lnj7lhfRn8kgPt5"
        + "+MJHS7UJNDER+jn6KP6Jnm2ONQlm==";
    config.set(configkKey, secret);
    intercept(IOException.class,
        "\"Server failed to authenticate the request. Make sure the value of "
            + "Authorization header is formed correctly including the "
            + "signature.\", 403",
        () -> {
          FileSystem.newInstance(config);
        });
  }
}
