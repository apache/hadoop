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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.Stubber;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test LdapGroupsMapping with one-query lookup enabled.
 * Mockito is used to simulate the LDAP server response.
 */
@SuppressWarnings("unchecked")
public class TestLdapGroupsMappingWithOneQuery
    extends TestLdapGroupsMappingBase {

  public void setupMocks(List<String> listOfDNs) throws NamingException {
    Attribute groupDN = mock(Attribute.class);

    NamingEnumeration<SearchResult> groupNames = getGroupNames();
    doReturn(groupNames).when(groupDN).getAll();
    buildListOfGroupDNs(listOfDNs).when(groupNames).next();
    when(groupNames.hasMore()).
      thenReturn(true).thenReturn(true).
      thenReturn(true).thenReturn(false);

    when(getAttributes().get(eq("memberOf"))).thenReturn(groupDN);
  }

  /**
   * Build and return a list of individually added group DNs such
   * that calls to .next() will result in a single value each time.
   *
   * @param listOfDNs
   * @return the stubber to use for the .when().next() call
   */
  private Stubber buildListOfGroupDNs(List<String> listOfDNs) {
    Stubber stubber = null;
    for (String s : listOfDNs) {
      if (stubber != null) {
        stubber.doReturn(s);
      } else {
        stubber = doReturn(s);
      }
    }
    return stubber;
  }

  @Test
  public void testGetGroups() throws NamingException {
    // given a user whose ldap query returns a user object with three "memberOf"
    // properties, return an array of strings representing its groups.
    String[] testGroups = new String[] {"abc", "xyz", "sss"};
    doTestGetGroups(Arrays.asList(testGroups));

    // test fallback triggered by NamingException
    doTestGetGroupsWithFallback();
  }

  private void doTestGetGroups(List<String> expectedGroups)
      throws NamingException {
    List<String> groupDns = new ArrayList<>();
    groupDns.add("CN=abc,DC=foo,DC=bar,DC=com");
    groupDns.add("CN=xyz,DC=foo,DC=bar,DC=com");
    groupDns.add("CN=sss,DC=foo,DC=bar,DC=com");

    setupMocks(groupDns);
    String ldapUrl = "ldap://test";
    Configuration conf = getBaseConf(ldapUrl);
    // enable single-query lookup
    conf.set(LdapGroupsMapping.MEMBEROF_ATTR_KEY, "memberOf");

    TestLdapGroupsMapping groupsMapping = new TestLdapGroupsMapping();
    groupsMapping.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = groupsMapping.getGroups("some_user");

    Assert.assertEquals(expectedGroups, groups);
    Assert.assertFalse("Second LDAP query should NOT have been called.",
            groupsMapping.isSecondaryQueryCalled());

    // We should have only made one query because single-query lookup is enabled
    verify(getContext(), times(1)).search(anyString(), anyString(),
        any(Object[].class), any(SearchControls.class));
  }

  private void doTestGetGroupsWithFallback()
          throws NamingException {
    List<String> groupDns = new ArrayList<>();
    groupDns.add("CN=abc,DC=foo,DC=bar,DC=com");
    groupDns.add("CN=xyz,DC=foo,DC=bar,DC=com");
    groupDns.add("ipaUniqueID=e4a9a634-bb24-11ec-aec1-06ede52b5fe1," +
            "CN=sudo,DC=foo,DC=bar,DC=com");
    setupMocks(groupDns);
    String ldapUrl = "ldap://test";
    Configuration conf = getBaseConf(ldapUrl);
    // enable single-query lookup
    conf.set(LdapGroupsMapping.MEMBEROF_ATTR_KEY, "memberOf");
    conf.set(LdapGroupsMapping.LDAP_NUM_ATTEMPTS_KEY, "1");

    TestLdapGroupsMapping groupsMapping = new TestLdapGroupsMapping();
    groupsMapping.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = groupsMapping.getGroups("some_user");

    // expected to be empty due to invalid memberOf
    Assert.assertEquals(0, groups.size());

    // expect secondary query to be called: getGroups()
    Assert.assertTrue("Second LDAP query should have been called.",
            groupsMapping.isSecondaryQueryCalled());

    // We should have fallen back to the second query because first threw
    // NamingException expected count is 3 since testGetGroups calls
    // doTestGetGroups and doTestGetGroupsWithFallback in succession and
    // the count is across both test scenarios.
    verify(getContext(), times(3)).search(anyString(), anyString(),
            any(Object[].class), any(SearchControls.class));
  }

  private static final class TestLdapGroupsMapping extends LdapGroupsMapping {
    private boolean secondaryQueryCalled = false;
    public boolean isSecondaryQueryCalled() {
      return secondaryQueryCalled;
    }
    Set<String> lookupGroup(SearchResult result, DirContext c,
                                    int goUpHierarchy) throws NamingException {
      secondaryQueryCalled = true;
      return super.lookupGroup(result, c, goUpHierarchy);
    }
  }
}
