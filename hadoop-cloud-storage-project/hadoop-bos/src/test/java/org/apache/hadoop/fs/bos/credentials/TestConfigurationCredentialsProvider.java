/*
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

package org.apache.hadoop.fs.bos.credentials;

import com.baidubce.auth.DefaultBceSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ConfigurationCredentialsProvider}.
 */
public class TestConfigurationCredentialsProvider {
  private static Configuration conf = new Configuration();

  /**
    * Set up test configuration with BOS credentials.
    */
  @BeforeAll
  public static void beforeTest() {
    conf.set("fs.bos.access.key", "ak");
    conf.set("fs.bos.secret.access.key", "sk");
    conf.set("fs.bos.session.token.key", "token");
    conf.set("fs.bos.credentials.provider",
        "org.apache.hadoop.fs.bos.credentials"
            + ".ConfigurationCredentialsProvider");
  }

  /**
    * Test that the provider returns correct credentials.
    */
  @Test
  public void testProvider() {
    BceCredentialsProvider provider =
        BceCredentialsProvider
            .getBceCredentialsProviderImpl(conf);
    if (provider instanceof HadoopCredentialsProvider) {
      ((HadoopCredentialsProvider) provider).setConf(conf);
    }
    DefaultBceSessionCredentials token =
        provider.getCredentials(null, "hdfs");
    assertEquals("ak", token.getAccessKeyId());
    assertEquals("sk", token.getSecretKey());
    assertEquals("token", token.getSessionToken());
  }
}
