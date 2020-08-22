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
package org.apache.hadoop.fs.azurebfs.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AccountType;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_HNS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_NONHNS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

public class AbfsTestStatement extends Statement {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsTestStatement.class);

  private static final String COMBINATION_SEPARATOR = "\\|";
  private static final String CONFIGS_SEPARATOR = "-";
  private static final int ACCOUNT_TYPE_INDEX = 0;
  private static final int AUTH_TYPE_INDEX = 1;
  private static final List<List<String>> TEST_COMBINATIONS;
  private static final List<String> SUPPORTED_AUTH_TYPES;
  private static final List<String> SUPPORTED_ACCOUNT_TYPES;

  static {
    SUPPORTED_AUTH_TYPES = new ArrayList<>();
    SUPPORTED_AUTH_TYPES.add(AuthType.OAuth.name());
    SUPPORTED_AUTH_TYPES.add(AuthType.SharedKey.name());
    SUPPORTED_ACCOUNT_TYPES = new ArrayList<>();
    SUPPORTED_ACCOUNT_TYPES.add(AccountType.HNS.name());
    SUPPORTED_ACCOUNT_TYPES.add(AccountType.NonHNS.name());

    TEST_COMBINATIONS = new ArrayList<>();
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);
    String testConfigCombinationsStr = rawConfig
        .get(TestConfigurationKeys.FS_AZURE_TEST_COMBINATIONS);
    String[] confCombinations = testConfigCombinationsStr.trim()
        .split(COMBINATION_SEPARATOR);
    for (String confCombinationStr : confCombinations) {
      String[] confs = confCombinationStr.trim().split(CONFIGS_SEPARATOR);
      String accountType = confs[ACCOUNT_TYPE_INDEX];
      String authType = confs[AUTH_TYPE_INDEX];
      if (SUPPORTED_AUTH_TYPES.contains(authType) && SUPPORTED_ACCOUNT_TYPES
          .contains(accountType)) {
        TEST_COMBINATIONS.add(Arrays.asList(accountType, authType));
      }
    }
  }

  private final Statement base;
  private final AbfsTestable testObj;
  private final List<String> authTypesToTest;
  private final List<String> accountTypesToTest;
  private final List<String> authTypesToExclude;

  private String accountType;
  private String authType;

  public AbfsTestStatement(final Statement base, final Description description,
      final AbfsTestable testObj) {
    this.base = base;
    this.testObj = testObj;
    this.authTypesToExclude = new ArrayList<>();
    List<AuthType> authTypesToExclude = testObj.excludeAuthTypes();
    if (!authTypesToExclude.isEmpty()) {
      for (AuthType authType : authTypesToExclude) {
        this.authTypesToExclude.add(authType.name());
      }
    }
    AbfsConfigsToTest abfsConfigsToTest = description
        .getAnnotation(AbfsConfigsToTest.class);
    if (abfsConfigsToTest == null) {
      this.accountTypesToTest = SUPPORTED_ACCOUNT_TYPES;
      this.authTypesToTest = SUPPORTED_AUTH_TYPES;
      return;
    }
    AuthType[] authTypesToTestArr = abfsConfigsToTest.authTypes();
    if (authTypesToTestArr != null && authTypesToTestArr.length > 0) {
      this.authTypesToTest = new ArrayList<>();
      for (AuthType authType : authTypesToTestArr) {
        this.authTypesToTest.add(authType.name());
      }
    } else {
      this.authTypesToTest = SUPPORTED_AUTH_TYPES;
    }
    AccountType[] accountTypesToTestArr = abfsConfigsToTest.accountTypes();
    if (accountTypesToTestArr != null && accountTypesToTestArr.length > 0) {
      this.accountTypesToTest = new ArrayList<>();
      for (AccountType accountType : accountTypesToTestArr) {
        this.accountTypesToTest.add(accountType.name());
      }
    } else {
      this.accountTypesToTest = SUPPORTED_ACCOUNT_TYPES;
    }
  }

  @Override
  public void evaluate() throws Throwable {
    for (List<String> testConfigCombination : getTestConfigCombinations()) {
      this.accountType = testConfigCombination.get(ACCOUNT_TYPE_INDEX);
      this.authType = testConfigCombination.get(AUTH_TYPE_INDEX);
      LOG.error("Test : {}", testConfigCombination);
      setAccountTypeConfigs();
      setAuthTypeConfigs();
      base.evaluate();
    }
  }

  private List<List<String>> getTestConfigCombinations() {
    if (accountTypesToTest.isEmpty() && authTypesToTest.isEmpty()
        && authTypesToExclude.isEmpty()) {
      return TEST_COMBINATIONS;
    }
    List<List<String>> result = new ArrayList<>();
    for (List<String> confCombination : TEST_COMBINATIONS) {
      String accountType = confCombination.get(ACCOUNT_TYPE_INDEX);
      String authType = confCombination.get(AUTH_TYPE_INDEX);
      if (this.accountTypesToTest.contains(accountType) && this.authTypesToTest
          .contains(authType) && !this.authTypesToExclude.contains(authType)) {
        result.add(confCombination);
      }
    }
    return result;
  }

  private void setAuthTypeConfigs() {
    Configuration conf = testObj.getInitialConfiguration();
    conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, this.authType);
  }

  private void setAccountTypeConfigs() {
    String accountName;
    boolean isHNSEnabled;
    Configuration conf = testObj.getInitialConfiguration();
    if (this.accountType.equalsIgnoreCase(AccountType.HNS.name())) {
      accountName = conf.get(FS_AZURE_ABFS_HNS_ACCOUNT_NAME);
      isHNSEnabled = true;
    } else {
      accountName = conf.get(FS_AZURE_ABFS_NONHNS_ACCOUNT_NAME);
      isHNSEnabled = false;
    }
    conf.set(FS_AZURE_ABFS_ACCOUNT_NAME, accountName);
    conf.set(FS_AZURE_ACCOUNT_NAME, accountName);
    conf.setBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, isHNSEnabled);
  }

}
