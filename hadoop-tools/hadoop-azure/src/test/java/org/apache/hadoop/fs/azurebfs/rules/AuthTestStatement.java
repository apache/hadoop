package org.apache.hadoop.fs.azurebfs.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_HNS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_NONHNS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;

public class AuthTestStatement extends Statement {

  private static final Logger LOG = LoggerFactory
      .getLogger(AuthTestStatement.class);

  private final Statement base;
  private final Description description;
  private final AuthTypesTestable testObj;

  public AuthTestStatement(Statement base, Description description,
      AuthTypesTestable testObj) {
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
        test = testMethod + testConfigCombination;
        System.out.println(test);
        setAccountTypeConfigs(testConfigCombination.get(0));
        setAuthTypeConfigs(testConfigCombination.get(1));
        testObj.initFSEndpointForNewFS();
        base.evaluate();
      }
    } catch (Exception e) {
      LOG.debug(test + " failed. ", e);
      throw e;
    }
  }

  private List<String> authTypesToTest() {
    AuthTypesToTest authTypesToTest = description
        .getAnnotation(AuthTypesToTest.class);
    List<AuthType> authTypes = new ArrayList();
    if (authTypesToTest != null) {
      AuthType[] values = authTypesToTest.authTypes();
      if (values != null && values.length > 0) {
        authTypes = Arrays.asList(values);
      }
    }
    if (authTypes != null && authTypes.size() < 1) {
      authTypes.add(AuthType.OAuth);
      authTypes.add(AuthType.SharedKey);
      //authTypes.add(AuthType.SAS);
    }
    return authTypes.stream().map(authType -> authType.name())
        .collect(Collectors.toList());
  }

  private List<String> accountTypesToTest() {
    AuthTypesToTest authTypesToTest = description
        .getAnnotation(AuthTypesToTest.class);
    List<AccountType> accountTypes = new ArrayList();
    if (authTypesToTest != null) {
      AccountType[] values = authTypesToTest.accountTypes();
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

  private void setAuthTypeConfigs(final String authType) {
    Configuration conf = testObj.getInitialConfiguration();
    testObj.setAuthType(AuthType.valueOf(authType));
    conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, authType);
  }

  private void setAccountTypeConfigs(final String accountType) {
    String accountName;
    boolean isHNSEnabled;
    Configuration conf = testObj.getInitialConfiguration();
    if (accountType == AccountType.HNS.name()) {
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

