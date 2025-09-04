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

import com.volcengine.tos.TosException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.tosfs.util.TestUtility.removeSystemEnv;
import static org.apache.hadoop.fs.tosfs.util.TestUtility.setSystemEnv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDefaultCredentialsProviderChain extends TestAbstractCredentialsProvider {

  private static final String MOCK_TEST_AK = "AK";
  private static final String MOCK_TEST_SK = "SK";
  private static final String MOCK_TEST_TST_TOKEN = "STS_TOKEN";

  private static final String MOCK_TEST_AK_WITH_BUCKET = "AK_WITH_BUCKET";
  private static final String MOCK_TEST_SK_WITH_BUCKET = "SK_WITH_BUCKET";
  private static final String MOCK_TEST_STS_TOKEN_WITH_BUCKET = "STS_TOKEN_WITH_BUCKET";

  private static final String MOCK_TEST_ENV_AK = "ENV_AK";
  private static final String MOCK_TEST_ENV_SK = "ENV_SK";
  private static final String MOCK_TEST_ENV_STS_TOKEN = "ENV_STS_TOKEN";

  private static final String MOCK_TEST_BUCKET = "test";
  private static final String MOCK_TEST_ROLE_NAME = "roleName";
  private static final String MOCK_PATH = "/volcstack/latest/iam/security_credentials/";
  private static final String API_ENDPOINT = MOCK_PATH + MOCK_TEST_ROLE_NAME;
  private static final String EXPIRED_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX";

  @Override
  public Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key("test"), MOCK_TEST_AK_WITH_BUCKET);
    conf.set(TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key("test"), MOCK_TEST_SK_WITH_BUCKET);
    conf.set(TosKeys.FS_TOS_BUCKET_SESSION_TOKEN.key("test"), MOCK_TEST_STS_TOKEN_WITH_BUCKET);
    conf.set(TosKeys.FS_TOS_ACCESS_KEY_ID, MOCK_TEST_AK);
    conf.set(TosKeys.FS_TOS_SECRET_ACCESS_KEY, MOCK_TEST_SK);
    conf.set(TosKeys.FS_TOS_SESSION_TOKEN, MOCK_TEST_TST_TOKEN);
    return conf;
  }

  @BeforeEach
  public void setUp() {
    saveOsCredEnv();
  }

  @Test
  public void testLoadCredFromEnvProvider() {
    Configuration conf = getConf();
    setSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID, MOCK_TEST_ENV_AK);
    setSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, MOCK_TEST_ENV_SK);
    setSystemEnv(TOS.ENV_TOS_SESSION_TOKEN, MOCK_TEST_ENV_STS_TOKEN);
    DefaultCredentialsProviderChain chain = new DefaultCredentialsProviderChain();
    chain.initialize(conf, null);

    assertEquals(chain.credential().getAccessKeyId(),
        MOCK_TEST_ENV_AK, String.format("expect %s", MOCK_TEST_ENV_AK));
    assertEquals(chain.credential().getAccessKeySecret(), MOCK_TEST_ENV_SK,
        String.format("expect %s", MOCK_TEST_ENV_SK));
    assertEquals(chain.credential().getSecurityToken(), MOCK_TEST_ENV_STS_TOKEN,
        String.format("expect %s", MOCK_TEST_ENV_STS_TOKEN));
    assertTrue(chain.lastUsedProvider() instanceof EnvironmentCredentialsProvider);
  }

  @Test
  public void testLoadCredFromSimpleProviderWithBucket() {
    Configuration conf = getConf();
    removeSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID);
    removeSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY);
    removeSystemEnv(TOS.ENV_TOS_SESSION_TOKEN);
    DefaultCredentialsProviderChain chain = new DefaultCredentialsProviderChain();
    chain.initialize(conf, MOCK_TEST_BUCKET);

    assertEquals(chain.credential().getAccessKeyId(), MOCK_TEST_AK_WITH_BUCKET,
        String.format("expect %s", MOCK_TEST_AK_WITH_BUCKET));
    assertEquals(chain.credential().getAccessKeySecret(), MOCK_TEST_SK_WITH_BUCKET,
        String.format("expect %s", MOCK_TEST_SK_WITH_BUCKET));
    assertEquals(chain.credential().getSecurityToken(), MOCK_TEST_STS_TOKEN_WITH_BUCKET,
        String.format("expect %s", MOCK_TEST_STS_TOKEN_WITH_BUCKET));
    assertTrue(chain.lastUsedProvider() instanceof SimpleCredentialsProvider);
  }

  @Test
  public void testLoadCredFromSimpleProvider() {
    Configuration conf = getConf();
    removeSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID);
    removeSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY);
    DefaultCredentialsProviderChain chain = new DefaultCredentialsProviderChain();
    chain.initialize(conf, "test-bucket");

    assertEquals(chain.credential().getAccessKeyId(), MOCK_TEST_AK,
        String.format("expect %s", MOCK_TEST_AK));
    assertEquals(chain.credential().getAccessKeySecret(), MOCK_TEST_SK,
        String.format("expect %s", MOCK_TEST_SK));
    assertTrue(chain.lastUsedProvider() instanceof SimpleCredentialsProvider);
  }

  @Test
  public void testNotFoundAnyProvider() {
    removeSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID);
    removeSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY);
    DefaultCredentialsProviderChain chain = new DefaultCredentialsProviderChain();
    chain.initialize(new Configuration(), MOCK_TEST_BUCKET);
    assertThrows(RuntimeException.class, chain::credential);
  }

  @AfterEach
  public void after() {
    resetOsCredEnv();
  }

  @Test
  public void testShouldReturnAKSKFollowByProviderSequence() {
    setSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID, "ENV_ACCESS_KEY");
    setSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, "ENV_SECRET_KEY");

    // use the simple credential provider at first.
    String providerClassesStr = SimpleCredentialsProvider.class.getName() + ','
        + EnvironmentCredentialsProvider.class.getName();

    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES, providerClassesStr);
    conf.set(TosKeys.FS_TOS_ACCESS_KEY_ID, MOCK_TEST_AK);
    conf.set(TosKeys.FS_TOS_SECRET_ACCESS_KEY, MOCK_TEST_SK);
    conf.set(TosKeys.FS_TOS_SESSION_TOKEN, MOCK_TEST_TST_TOKEN);

    DefaultCredentialsProviderChain provider = new DefaultCredentialsProviderChain();
    provider.initialize(conf, MOCK_TEST_BUCKET);

    ExpireableCredential cred = provider.createCredential();
    assertEquals(MOCK_TEST_AK, cred.getAccessKeyId());
    assertEquals(MOCK_TEST_SK, cred.getAccessKeySecret());

    assertFalse(cred.isExpired());

    // use the env credential provider at first.
    providerClassesStr = EnvironmentCredentialsProvider.class.getName() + ','
        + SimpleCredentialsProvider.class.getName();
    conf.set(TosKeys.FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES, providerClassesStr);

    provider = new DefaultCredentialsProviderChain();
    provider.initialize(conf, MOCK_TEST_BUCKET);
    cred = provider.createCredential();
    assertEquals("ENV_ACCESS_KEY", cred.getAccessKeyId());
    assertEquals("ENV_SECRET_KEY", cred.getAccessKeySecret());
    assertFalse(cred.isExpired());

    removeSystemEnv(TOS.ENV_TOS_ACCESS_KEY_ID);
    removeSystemEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY);
  }

  @Test
  public void testShouldThrowExceptionWhenCustomClassNotFound() {
    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES,
        SimpleCredentialsProvider.class.getName() + "NotExist");

    DefaultCredentialsProviderChain provider = new DefaultCredentialsProviderChain();
    TosException tosException =
        assertThrows(TosException.class, () -> provider.initialize(conf, null));
    assertTrue(tosException.getCause() instanceof ClassNotFoundException);
  }

  @Test
  public void testShouldThrowExceptionIfNoDefaultConstructorFound() {
    Configuration conf = new Configuration();
    conf.set(TosKeys.FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES,
        TestCredentialProviderNoDefaultConstructor.class.getName());
    DefaultCredentialsProviderChain provider = new DefaultCredentialsProviderChain();
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> provider.initialize(conf, null));
    assertTrue(exception.getMessage().contains("java.lang.NoSuchMethodException"));
  }

  static class TestCredentialProviderNoDefaultConstructor extends AbstractCredentialsProvider {

    TestCredentialProviderNoDefaultConstructor(String fake) {
    }

    @Override
    protected ExpireableCredential createCredential() {
      return null;
    }
  }
}
