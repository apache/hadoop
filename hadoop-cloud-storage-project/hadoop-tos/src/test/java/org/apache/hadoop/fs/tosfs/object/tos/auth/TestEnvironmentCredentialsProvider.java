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

package org.apache.hadoop.fs.tosfs.object.tos.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestEnvironmentCredentialsProvider extends TestAbstractCredentialsProvider {

  @BeforeEach
  public void setUp() {
    saveOsCredEnv();
  }

  @Test
  public void testLoadAkSkFromEnvProvider() {
    TestUtility.setSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID, "AccessKeyId");
    TestUtility.setSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, "SecretAccessKey");

    EnvironmentCredentialsProvider provider = new EnvironmentCredentialsProvider();
    provider.initialize(new Configuration(), null);

    ExpireableCredential oldCred = provider.credential();
    assertEquals(oldCred.getAccessKeyId(), "AccessKeyId", "provider ak must be equals to env ak");
    assertEquals(oldCred.getAccessKeySecret(), "SecretAccessKey",
        "provider sk must be equals to env sk");

    TestUtility.setSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID, "newAccessKeyId");
    TestUtility.setSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, "newSecretAccessKey");
    TestUtility.setSystemEnv(TOS.ENV_TOS_SESSION_TOKEN, "newSessionToken");

    assertFalse(oldCred.isExpired());

    ExpireableCredential newCred = provider.credential();
    assertEquals(newCred.getAccessKeyId(), "AccessKeyId", "provider ak must be equals to env ak");
    assertEquals(newCred.getAccessKeySecret(), "SecretAccessKey",
        "provider sk must be equals to env sk");

    assertFalse(newCred.isExpired());
  }

  @AfterEach
  public void resetEnv() {
    resetOsCredEnv();
  }
}
