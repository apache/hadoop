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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

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

  interface GetClassInterface {
  }

  private class GetClassImpl0 implements GetClassInterface {
  }

  private class GetClassImpl1 implements GetClassInterface {
  }

  @Test
  public void testClassPrecedence()
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
        abfsConf.getClass(globalKey, class1, xface), class0);
    conf.unset(globalKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getClass(globalKey, class1, xface), class1);

    conf.setClass(accountKey, class0, xface);
    assertEquals("Default value returned even though account-specific config was set",
        abfsConf.getClass(globalKey, class1, xface), class0);
    conf.unset(accountKey);
    assertEquals("Default value not returned even though config was unset",
        abfsConf.getClass(globalKey, class1, xface), class1);

    conf.setClass(accountKey, class1, xface);
    conf.setClass(globalKey, class0, xface);
    assertEquals("Account-agnostic or default value returned even though account-specific config was set",
        abfsConf.getClass(globalKey, class0, xface), class1);
  }

}
