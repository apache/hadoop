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
import java.util.stream.Collectors;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.junit.Assume;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AccountType;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_HNS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_NONHNS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;

public class AbfsTestStatement extends Statement {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsTestStatement.class);

  private final Statement base;
  private final Description description;
  private final AbfsTestable testObj;

  private String accountType;
  private String authType;

  public AbfsTestStatement(Statement base, Description description,
      AbfsTestable testObj) {
    this.base = base;
    this.description = description;
    this.testObj = testObj;
  }

  @Override
  public void evaluate() throws Throwable {
    String testMethod =
        description.getTestClass() + "#" + description.getMethodName() + "-";

    List<List<String>> testsConfigs = new ArrayList<>();
    testsConfigs.add(accountTypesToTest());
    testsConfigs.add(authTypesToTest());
    List<List<String>> testConfigCombinations = cartesianProduct(testsConfigs);

    String test = "";
    try {
      for (List<String> testConfigCombination : testConfigCombinations) {
        this.accountType = testConfigCombination.get(0);
        this.authType = testConfigCombination.get(1);
        if(isValidConfigCombination()) {
          test = testMethod + testConfigCombination;
          LOG.error("\n\nTest : {}", test);
          setAccountTypeConfigs(this.accountType);
          setAuthTypeConfigs(this.authType);
          base.evaluate();
        }
      }
    } catch (Exception e) {
      LOG.debug(test + " failed. ", e);
      throw e;
    }
  }

  private boolean isValidConfigCombination() {
    return !(AccountType.NonHNS.name().equalsIgnoreCase(accountType)
        && AuthType.SAS.name().equalsIgnoreCase(authType));
  }

  private List<String> authTypesToTest() {
    AbfsConfigsToTest abfsConfigsToTest = description
        .getAnnotation(AbfsConfigsToTest.class);
    List<AuthType> authTypes = new ArrayList();
    if (abfsConfigsToTest != null) {
      AuthType[] values = abfsConfigsToTest.authTypes();
      if (values != null && values.length > 0) {
        authTypes = Arrays.asList(values);
      }
    }
    if (authTypes != null && authTypes.size() < 1) {
      authTypes.add(AuthType.OAuth);
      authTypes.add(AuthType.SharedKey);//authTypes.add(AuthType.SAS);
    }
    return authTypes.stream()
        .filter(authType -> !testObj.excludeAuthTypes().contains(authType))
        .map(authType -> authType.name())
        .collect(Collectors.toList());
  }

  private List<String> accountTypesToTest() {
    AbfsConfigsToTest abfsConfigsToTest = description
        .getAnnotation(AbfsConfigsToTest.class);
    List<AccountType> accountTypes = new ArrayList();
    if (abfsConfigsToTest != null) {
      AccountType[] values = abfsConfigsToTest.accountTypes();
      if (values != null && values.length > 0) {
        accountTypes = Arrays.asList(values);
      }
    }
    if (accountTypes != null && accountTypes.size() < 1) {
      accountTypes.add(AccountType.HNS);
      accountTypes.add(AccountType.NonHNS);
    }
    return accountTypes.stream().map(accountType -> accountType.name())
        .collect(Collectors.toList());
  }

  private void setAuthTypeConfigs(final String authType) throws Exception {
    Configuration conf = testObj.getInitialConfiguration();
    conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, authType);
  }

  private void setAccountTypeConfigs(final String accountType) {
    String accountName;
    boolean isHNSEnabled;
    Configuration conf = testObj.getInitialConfiguration();
    if (accountType.equalsIgnoreCase(AccountType.HNS.name())) {
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

  private static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
    List<List<T>> result = Arrays.asList(Arrays.asList());
    for (List<T> currentList : lists) {
      result = appendElements(result, currentList);
    }
    return result;
  }

  private static <T> List<List<T>> appendElements(List<List<T>> tempResult,
      final List<T> currentList) {
    List<List<T>> newCombinations = new ArrayList<>();
    for (List<T> combination : tempResult) {
      for (T currListElement : currentList) {
        List<T> combinationWithElementFromCurrList = new ArrayList<>(
            combination);
        combinationWithElementFromCurrList.add(currListElement);
        newCombinations.add(combinationWithElementFromCurrList);
      }
    }
    return newCombinations;
  }

}
