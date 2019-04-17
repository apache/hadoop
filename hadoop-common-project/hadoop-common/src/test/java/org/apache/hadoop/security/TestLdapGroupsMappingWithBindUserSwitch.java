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

package org.apache.hadoop.security;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.security.LdapGroupsMapping.BIND_PASSWORD_ALIAS_SUFFIX;
import static org.apache.hadoop.security.LdapGroupsMapping.BIND_PASSWORD_FILE_SUFFIX;
import static org.apache.hadoop.security.LdapGroupsMapping.BIND_PASSWORD_SUFFIX;
import static org.apache.hadoop.security.LdapGroupsMapping.BIND_USERS_KEY;
import static org.apache.hadoop.security.LdapGroupsMapping.BIND_USER_SUFFIX;
import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_NUM_ATTEMPTS_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test functionality for switching bind user information if
 * AuthenticationExceptions are encountered.
 */
public class TestLdapGroupsMappingWithBindUserSwitch
    extends TestLdapGroupsMappingBase {

  private static final String TEST_USER_NAME = "some_user";

  @Test
  public void testIncorrectConfiguration() {
    // No bind user configured for user2
    Configuration conf = getBaseConf();
    conf.set(BIND_USERS_KEY, "user1,user2");
    conf.set(BIND_USERS_KEY + ".user1" + BIND_USER_SUFFIX, "bindUsername1");

    LdapGroupsMapping groupsMapping = new LdapGroupsMapping();
    try {
      groupsMapping.setConf(conf);
      groupsMapping.getGroups(TEST_USER_NAME);
      fail("Should have failed with RuntimeException");
    } catch (RuntimeException e) {
      GenericTestUtils.assertExceptionContains(
          "Bind username or password not configured for user: user2", e);
    }
  }

  @Test
  public void testBindUserSwitchPasswordPlaintext() throws Exception {
    Configuration conf = getBaseConf();
    conf.set(BIND_USERS_KEY, "user1,user2");
    conf.set(BIND_USERS_KEY + ".user1" + BIND_USER_SUFFIX, "bindUsername1");
    conf.set(BIND_USERS_KEY + ".user2" + BIND_USER_SUFFIX, "bindUsername2");

    conf.set(BIND_USERS_KEY + ".user1" + BIND_PASSWORD_SUFFIX, "bindPassword1");
    conf.set(BIND_USERS_KEY + ".user2" + BIND_PASSWORD_SUFFIX, "bindPassword2");

    doTestBindUserSwitch(conf, 1,
        Arrays.asList("bindUsername1", "bindUsername2"),
        Arrays.asList("bindPassword1", "bindPassword2"));
  }

  @Test
  public void testBindUserSwitchPasswordFromAlias() throws Exception {
    Configuration conf = getBaseConf();
    conf.set(BIND_USERS_KEY, "joe,lukas");
    conf.set(BIND_USERS_KEY + ".joe" + BIND_USER_SUFFIX, "joeBindUsername");
    conf.set(BIND_USERS_KEY + ".lukas" + BIND_USER_SUFFIX, "lukasBindUsername");

    conf.set(BIND_USERS_KEY + ".joe" + BIND_PASSWORD_ALIAS_SUFFIX,
        "joeBindPasswordAlias");
    conf.set(BIND_USERS_KEY + ".lukas" + BIND_PASSWORD_ALIAS_SUFFIX,
        "lukasBindPasswordAlias");

    setupCredentialProvider(conf);
    createCredentialForAlias(conf, "joeBindPasswordAlias", "joeBindPassword");
    createCredentialForAlias(conf, "lukasBindPasswordAlias",
        "lukasBindPassword");

    // Simulate 2 failures to test cycling through the bind users
    List<String> expectedBindUsers = Arrays.asList("joeBindUsername",
        "lukasBindUsername", "joeBindUsername");
    List<String> expectedBindPasswords = Arrays.asList("joeBindPassword",
        "lukasBindPassword", "joeBindPassword");

    doTestBindUserSwitch(conf, 2, expectedBindUsers, expectedBindPasswords);
  }

  @Test
  public void testBindUserSwitchPasswordFromFile() throws Exception {
    Configuration conf = getBaseConf();
    conf.setInt(LDAP_NUM_ATTEMPTS_KEY, 10);

    conf.set(BIND_USERS_KEY, "bob,alice");
    conf.set(BIND_USERS_KEY + ".bob" + BIND_USER_SUFFIX, "bobUsername");
    conf.set(BIND_USERS_KEY + ".alice" + BIND_USER_SUFFIX, "aliceUsername");

    conf.set(BIND_USERS_KEY + ".bob" + BIND_PASSWORD_FILE_SUFFIX,
        createPasswordFile("bobPasswordFile1.txt", "bobBindPassword"));
    conf.set(BIND_USERS_KEY + ".alice" + BIND_PASSWORD_FILE_SUFFIX,
        createPasswordFile("alicePasswordFile2.txt", "aliceBindPassword"));

    // Simulate 4 failures to test cycling through the bind users
    List<String> expectedBindUsers = Arrays.asList("bobUsername",
        "aliceUsername", "bobUsername", "aliceUsername", "bobUsername");
    List<String> expectedBindPasswords = Arrays.asList("bobBindPassword",
        "aliceBindPassword", "bobBindPassword", "aliceBindPassword",
        "bobBindPassword");

    doTestBindUserSwitch(conf, 4, expectedBindUsers, expectedBindPasswords);
  }

  private void setupCredentialProvider(Configuration conf) {
    File testDir = GenericTestUtils.getTestDir();
    final Path jksPath = new Path(testDir.toString(), "test.jks");
    final String ourUrl =
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(testDir, "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);
  }

  private void createCredentialForAlias(
      Configuration conf, String alias, String password) throws Exception {
    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    char[] bindpass = password.toCharArray();

    // Ensure that we get null when the key isn't there
    assertNull(provider.getCredentialEntry(alias));

    // Create credential for the alias
    provider.createCredentialEntry(alias, bindpass);
    provider.flush();

    // Make sure we get back the right key
    assertArrayEquals(bindpass, provider.getCredentialEntry(
        alias).getCredential());
  }

  private String createPasswordFile(String filename, String password)
      throws Exception {
    File testDir = GenericTestUtils.getTestDir();
    testDir.mkdirs();
    File secretFile = new File(testDir, filename);
    Writer writer = new FileWriter(secretFile);
    writer.write(password);
    writer.close();
    return secretFile.getPath();
  }

  private void doTestBindUserSwitch(
      Configuration conf, Integer numFailures,
      List<String> expectedBindUsers,
      List<String> expectedBindPasswords) throws NamingException {
    doTestBindUserSwitch(conf, numFailures, Iterators.cycle(expectedBindUsers),
        Iterators.cycle(expectedBindPasswords));
  }

  /**
   *
   * @param conf Configuration to be used
   * @param numFailures number of AuthenticationException failures to simulate
   * @param expectedBindUsers expected sequence of distinguished user names
   *                          when binding to LDAP
   * @param expectedBindPasswords expected sequence of passwords to be used when
   *                              binding to LDAP
   * @throws NamingException from DirContext.search()
   */
  private void doTestBindUserSwitch(
      Configuration conf, Integer numFailures,
      Iterator<String> expectedBindUsers,
      Iterator<String> expectedBindPasswords) throws NamingException {

    DummyLdapCtxFactory.setExpectedBindUser(expectedBindUsers.next());
    DummyLdapCtxFactory.setExpectedBindPassword(expectedBindPasswords.next());

    final AtomicInteger failuresLeft = new AtomicInteger(numFailures);

    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class))).thenAnswer(invocationOnMock -> {
          if (failuresLeft.get() > 0) {
            DummyLdapCtxFactory.setExpectedBindUser(expectedBindUsers.next());
            DummyLdapCtxFactory.setExpectedBindPassword(
                expectedBindPasswords.next());
            failuresLeft.decrementAndGet();
            throw new AuthenticationException();
          }
          // Return userNames for the first successful search()
          if (failuresLeft.getAndDecrement() == 0) {
            return getUserNames();
          }
          // Return groupNames for the second successful search()
          return getGroupNames();
        });

    LdapGroupsMapping groupsMapping = new LdapGroupsMapping();
    groupsMapping.setConf(conf);

    List<String> groups = groupsMapping.getGroups(TEST_USER_NAME);
    assertEquals(Arrays.asList("group1", "group2"), groups);

    // There will be one search() call for each failure and
    // 2 calls for the successful case; one for retrieving the
    // user and one for retrieving their groups.
    int numExpectedSearchCalls = numFailures + 2;
    verify(getContext(), times(numExpectedSearchCalls)).search(anyString(),
        anyString(), any(Object[].class), any(SearchControls.class));
  }
}
