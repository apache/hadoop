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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConfigurationPropertyNotFoundException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests correct precedence of various configurations that might be returned.
 * Configuration can be specified with the account name as a suffix to the
 * config key, or without one. Account-specific values should be returned
 * whenever they exist. Account-agnostic values are returned if they do not.
 * Default values are returned if neither exists.
 *
 * These tests are in 2 main groups: tests of methods that allow default values
 * (such as get and getPasswordString) are of one form, while tests of methods
 * that do allow default values (all others) follow another form.
 */
public class TestAccountConfiguration {
  private static final String TEST_OAUTH_PROVIDER_CLASS_CONFIG = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";
  private static final String TEST_CUSTOM_PROVIDER_CLASS_CONFIG = "org.apache.hadoop.fs.azurebfs.oauth2.RetryTestTokenProvider";
  private static final String TEST_SAS_PROVIDER_CLASS_CONFIG_1 = "org.apache.hadoop.fs.azurebfs.extensions.MockErrorSASTokenProvider";
  private static final String TEST_SAS_PROVIDER_CLASS_CONFIG_2 = "org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider";

  private static final String TEST_OAUTH_ENDPOINT = "oauthEndpoint";
  private static final String TEST_CLIENT_ID = "clientId";
  private static final String TEST_CLIENT_SECRET = "clientSecret";

  private static final List<String> CONFIG_KEYS =
      Collections.unmodifiableList(Arrays.asList(
          FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT,
          FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID,
          FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET));

  @Test
  public void testStringPrecedence()
      throws IllegalAccessException, IOException, InvalidConfigurationValueException {
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();

    final String accountName1 = "account1";
    final String accountName2 = "account2";
    final String accountName3 = "account3";

    final String globalKey = "fs.azure.configuration";
    final String accountKey1 = globalKey + "." + accountName1;
    final String accountKey2 = globalKey + "." + accountName2;
    final String accountKey3 = globalKey + "." + accountName3;

    final String globalValue = "global";
    final String accountValue1 = "one";
    final String accountValue2 = "two";

    conf.set(accountKey1, accountValue1);
    conf.set(accountKey2, accountValue2);
    conf.set(globalKey, globalValue);

    abfsConf = new AbfsConfiguration(conf, accountName1);
    assertEquals("Wrong value returned when account-specific value was requested",
        abfsConf.get(accountKey1), accountValue1);
    assertEquals("Account-specific value was not returned when one existed",
        abfsConf.get(globalKey), accountValue1);

    abfsConf = new AbfsConfiguration(conf, accountName2);
    assertEquals("Wrong value returned when a different account-specific value was requested",
        abfsConf.get(accountKey1), accountValue1);
    assertEquals("Wrong value returned when account-specific value was requested",
        abfsConf.get(accountKey2), accountValue2);
    assertEquals("Account-agnostic value return even though account-specific value was set",
        abfsConf.get(globalKey), accountValue2);

    abfsConf = new AbfsConfiguration(conf, accountName3);
    assertNull("Account-specific value returned when none was set",
        abfsConf.get(accountKey3));
    assertEquals("Account-agnostic value not returned when no account-specific value was set",
        abfsConf.get(globalKey), globalValue);
  }

  @Test
  public void testPasswordPrecedence()
      throws IllegalAccessException, IOException, InvalidConfigurationValueException {
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();

    final String accountName1 = "account1";
    final String accountName2 = "account2";
    final String accountName3 = "account3";

    final String globalKey = "fs.azure.password";
    final String accountKey1 = globalKey + "." + accountName1;
    final String accountKey2 = globalKey + "." + accountName2;
    final String accountKey3 = globalKey + "." + accountName3;

    final String globalValue = "global";
    final String accountValue1 = "one";
    final String accountValue2 = "two";

    conf.set(accountKey1, accountValue1);
    conf.set(accountKey2, accountValue2);
    conf.set(globalKey, globalValue);

    abfsConf = new AbfsConfiguration(conf, accountName1);
    assertEquals("Wrong value returned when account-specific value was requested",
        abfsConf.getPasswordString(accountKey1), accountValue1);
    assertEquals("Account-specific value was not returned when one existed",
        abfsConf.getPasswordString(globalKey), accountValue1);

    abfsConf = new AbfsConfiguration(conf, accountName2);
    assertEquals("Wrong value returned when a different account-specific value was requested",
        abfsConf.getPasswordString(accountKey1), accountValue1);
    assertEquals("Wrong value returned when account-specific value was requested",
        abfsConf.getPasswordString(accountKey2), accountValue2);
    assertEquals("Account-agnostic value return even though account-specific value was set",
        abfsConf.getPasswordString(globalKey), accountValue2);

    abfsConf = new AbfsConfiguration(conf, accountName3);
    assertNull("Account-specific value returned when none was set",
        abfsConf.getPasswordString(accountKey3));
    assertEquals("Account-agnostic value not returned when no account-specific value was set",
        abfsConf.getPasswordString(globalKey), globalValue);
  }

  @Test
  public void testBooleanPrecedence()
        throws IllegalAccessException, IOException, InvalidConfigurationValueException {

    final String accountName = "account";
    final String globalKey = "fs.azure.bool";
    final String accountKey = globalKey + "." + accountName;

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    conf.setBoolean(globalKey, false);
    assertEquals("Default value returned even though account-agnostic config was set",
        abfsConf.getBoolean(globalKey, true), false);
    conf.unset(globalKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getBoolean(globalKey, true), true);

    conf.setBoolean(accountKey, false);
    assertEquals("Default value returned even though account-specific config was set",
        abfsConf.getBoolean(globalKey, true), false);
    conf.unset(accountKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getBoolean(globalKey, true), true);

    conf.setBoolean(accountKey, true);
    conf.setBoolean(globalKey, false);
    assertEquals("Account-agnostic or default value returned even though account-specific config was set",
        abfsConf.getBoolean(globalKey, false), true);
  }

  @Test
  public void testLongPrecedence()
        throws IllegalAccessException, IOException, InvalidConfigurationValueException {

    final String accountName = "account";
    final String globalKey = "fs.azure.long";
    final String accountKey = globalKey + "." + accountName;

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    conf.setLong(globalKey, 0);
    assertEquals("Default value returned even though account-agnostic config was set",
        abfsConf.getLong(globalKey, 1), 0);
    conf.unset(globalKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getLong(globalKey, 1), 1);

    conf.setLong(accountKey, 0);
    assertEquals("Default value returned even though account-specific config was set",
        abfsConf.getLong(globalKey, 1), 0);
    conf.unset(accountKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getLong(globalKey, 1), 1);

    conf.setLong(accountKey, 1);
    conf.setLong(globalKey, 0);
    assertEquals("Account-agnostic or default value returned even though account-specific config was set",
        abfsConf.getLong(globalKey, 0), 1);
  }

  /**
   * Dummy type used for testing handling of enums in configuration.
   */
  public enum GetEnumType {
    TRUE, FALSE
  }

  @Test
  public void testEnumPrecedence()
        throws IllegalAccessException, IOException, InvalidConfigurationValueException {

    final String accountName = "account";
    final String globalKey = "fs.azure.enum";
    final String accountKey = globalKey + "." + accountName;

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    conf.setEnum(globalKey, GetEnumType.FALSE);
    assertEquals("Default value returned even though account-agnostic config was set",
        abfsConf.getEnum(globalKey, GetEnumType.TRUE), GetEnumType.FALSE);
    conf.unset(globalKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getEnum(globalKey, GetEnumType.TRUE), GetEnumType.TRUE);

    conf.setEnum(accountKey, GetEnumType.FALSE);
    assertEquals("Default value returned even though account-specific config was set",
        abfsConf.getEnum(globalKey, GetEnumType.TRUE), GetEnumType.FALSE);
    conf.unset(accountKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getEnum(globalKey, GetEnumType.TRUE), GetEnumType.TRUE);

    conf.setEnum(accountKey, GetEnumType.TRUE);
    conf.setEnum(globalKey, GetEnumType.FALSE);
    assertEquals("Account-agnostic or default value returned even though account-specific config was set",
        abfsConf.getEnum(globalKey, GetEnumType.FALSE), GetEnumType.TRUE);
  }

  /**
   * Dummy type used for testing handling of classes in configuration.
   */
  interface GetClassInterface {
  }

  /**
   * Dummy type used for testing handling of classes in configuration.
   */
  private class GetClassImpl0 implements GetClassInterface {
  }

  /**
   * Dummy type used for testing handling of classes in configuration.
   */
  private class GetClassImpl1 implements GetClassInterface {
  }

  @Test
  public void testClass()
        throws IllegalAccessException, IOException, InvalidConfigurationValueException {

    final String accountName = "account";
    final String globalKey = "fs.azure.class";
    final String accountKey = globalKey + "." + accountName;

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    final Class class0 = GetClassImpl0.class;
    final Class class1 = GetClassImpl1.class;
    final Class xface = GetClassInterface.class;

    conf.setClass(globalKey, class0, xface);
    assertEquals("Default value returned even though account-agnostic config was set",
        abfsConf.getAccountAgnosticClass(globalKey, class1, xface), class0);
    conf.unset(globalKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getAccountAgnosticClass(globalKey, class1, xface), class1);

    conf.setClass(accountKey, class0, xface);
    assertEquals("Default value returned even though account-specific config was set",
        abfsConf.getAccountSpecificClass(globalKey, class1, xface), class0);
    conf.unset(accountKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getAccountSpecificClass(globalKey, class1, xface), class1);

    conf.setClass(accountKey, class1, xface);
    conf.setClass(globalKey, class0, xface);
    assertEquals("Account-agnostic or default value returned even though account-specific config was set",
        abfsConf.getAccountSpecificClass(globalKey, class0, xface), class1);
  }

  @Test
  public void testSASProviderPrecedence()
      throws IOException, IllegalAccessException {
    final String accountName = "account";

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    // AccountSpecific: SAS with provider set as SAS_Provider_1
    abfsConf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + accountName,
        "SAS");
    abfsConf.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE + "." + accountName,
        TEST_SAS_PROVIDER_CLASS_CONFIG_1);

    // Global: SAS with provider set as SAS_Provider_2
    abfsConf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
        AuthType.SAS.toString());
    abfsConf.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        TEST_SAS_PROVIDER_CLASS_CONFIG_2);

    Assertions.assertThat(
        abfsConf.getSASTokenProvider().getClass().getName())
        .describedAs(
            "Account-specific SAS token provider should be in effect.")
        .isEqualTo(TEST_SAS_PROVIDER_CLASS_CONFIG_1);
  }

  @Test
  public void testAccessTokenProviderPrecedence()
      throws IllegalAccessException, IOException {
    final String accountName = "account";

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    // Global: Custom , AccountSpecific: OAuth
    testGlobalAndAccountOAuthPrecedence(abfsConf, AuthType.Custom,
        AuthType.OAuth);

    // Global: OAuth , AccountSpecific: Custom
    testGlobalAndAccountOAuthPrecedence(abfsConf, AuthType.OAuth,
        AuthType.Custom);

    // Global: (non-oAuth) SAS , AccountSpecific: Custom
    testGlobalAndAccountOAuthPrecedence(abfsConf, AuthType.SAS,
        AuthType.Custom);

    // Global: Custom , AccountSpecific: -
    testGlobalAndAccountOAuthPrecedence(abfsConf, AuthType.Custom, null);

    // Global: OAuth , AccountSpecific: -
    testGlobalAndAccountOAuthPrecedence(abfsConf, AuthType.OAuth, null);

    // Global: - , AccountSpecific: Custom
    testGlobalAndAccountOAuthPrecedence(abfsConf, null, AuthType.Custom);

    // Global: - , AccountSpecific: OAuth
    testGlobalAndAccountOAuthPrecedence(abfsConf, null, AuthType.OAuth);
  }

  @Test
  public void testConfigPropNotFound() throws Throwable {
    final String accountName = "account";

    final Configuration conf = new Configuration();
    final AbfsConfiguration abfsConf = new AbfsConfiguration(conf, accountName);

    for (String key : CONFIG_KEYS) {
      setAuthConfig(abfsConf, true, AuthType.OAuth);
      abfsConf.unset(key);
      abfsConf.unset(key + "." + accountName);
      testMissingConfigKey(abfsConf, key);
    }

    unsetAuthConfig(abfsConf, false);
    unsetAuthConfig(abfsConf, true);
  }

  private static void testMissingConfigKey(final AbfsConfiguration abfsConf,
      final String confKey) throws Throwable {
    GenericTestUtils.assertExceptionContains("Configuration property "
            + confKey + " not found.",
        LambdaTestUtils.verifyCause(
            ConfigurationPropertyNotFoundException.class,
            LambdaTestUtils.intercept(TokenAccessProviderException.class,
                () -> abfsConf.getTokenProvider().getClass().getTypeName())));
  }

  public void testGlobalAndAccountOAuthPrecedence(AbfsConfiguration abfsConf,
      AuthType globalAuthType,
      AuthType accountSpecificAuthType)
      throws IOException {
    if (globalAuthType == null) {
      unsetAuthConfig(abfsConf, false);
    } else {
      setAuthConfig(abfsConf, false, globalAuthType);
    }

    if (accountSpecificAuthType == null) {
      unsetAuthConfig(abfsConf, true);
    } else {
      setAuthConfig(abfsConf, true, accountSpecificAuthType);
    }

    // If account specific AuthType is present, precedence is always for it.
    AuthType expectedEffectiveAuthType;
    if (accountSpecificAuthType != null) {
      expectedEffectiveAuthType = accountSpecificAuthType;
    } else {
      expectedEffectiveAuthType = globalAuthType;
    }

    Class<?> expectedEffectiveTokenProviderClassType =
        (expectedEffectiveAuthType == AuthType.OAuth)
            ? ClientCredsTokenProvider.class
            : CustomTokenProviderAdapter.class;

    Assertions.assertThat(
        abfsConf.getTokenProvider().getClass().getTypeName())
        .describedAs(
            "Account-specific settings takes precendence to global"
                + " settings. In absence of Account settings, global settings "
                + "should take effect.")
        .isEqualTo(expectedEffectiveTokenProviderClassType.getTypeName());


    unsetAuthConfig(abfsConf, false);
    unsetAuthConfig(abfsConf, true);
  }

  public void setAuthConfig(AbfsConfiguration abfsConf,
      boolean isAccountSetting,
      AuthType authType) {
    final String accountNameSuffix = "." + abfsConf.getAccountName();
    String authKey = FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME
        + (isAccountSetting ? accountNameSuffix : "");
    String providerClassKey = "";
    String providerClassValue = "";

    switch (authType) {
    case OAuth:
      providerClassKey = FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME
          + (isAccountSetting ? accountNameSuffix : "");
      providerClassValue = TEST_OAUTH_PROVIDER_CLASS_CONFIG;

      abfsConf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT
          + ((isAccountSetting) ? accountNameSuffix : ""),
          TEST_OAUTH_ENDPOINT);
      abfsConf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID
          + ((isAccountSetting) ? accountNameSuffix : ""),
          TEST_CLIENT_ID);
      abfsConf.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET
          + ((isAccountSetting) ? accountNameSuffix : ""),
          TEST_CLIENT_SECRET);
      break;

    case Custom:
      providerClassKey = FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME
          + (isAccountSetting ? accountNameSuffix : "");
      providerClassValue = TEST_CUSTOM_PROVIDER_CLASS_CONFIG;
      break;

    case SAS:
      providerClassKey = FS_AZURE_SAS_TOKEN_PROVIDER_TYPE
          + (isAccountSetting ? accountNameSuffix : "");
      providerClassValue = TEST_SAS_PROVIDER_CLASS_CONFIG_1;
      break;

    default: // set nothing
    }

    abfsConf.set(authKey, authType.toString());
    abfsConf.set(providerClassKey, providerClassValue);
  }

  private void unsetAuthConfig(AbfsConfiguration abfsConf, boolean isAccountSettings) {
    String accountNameSuffix =
        isAccountSettings ? ("." + abfsConf.getAccountName()) : "";

    abfsConf.unset(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + accountNameSuffix);
    abfsConf.unset(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + accountNameSuffix);
    abfsConf.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE + accountNameSuffix);

    abfsConf.unset(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT + accountNameSuffix);
    abfsConf.unset(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + accountNameSuffix);
    abfsConf.unset(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET + accountNameSuffix);
  }

}
