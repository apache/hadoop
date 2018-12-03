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

import java.util.Arrays;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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

  @Before
  public void setupMocks() throws NamingException {
    Attribute groupDN = mock(Attribute.class);

    NamingEnumeration<SearchResult> groupNames = getGroupNames();
    doReturn(groupNames).when(groupDN).getAll();
    String groupName1 = "CN=abc,DC=foo,DC=bar,DC=com";
    String groupName2 = "CN=xyz,DC=foo,DC=bar,DC=com";
    String groupName3 = "CN=sss,CN=foo,DC=bar,DC=com";
    doReturn(groupName1).doReturn(groupName2).doReturn(groupName3).
        when(groupNames).next();
    when(groupNames.hasMore()).thenReturn(true).thenReturn(true).
        thenReturn(true).thenReturn(false);

    when(getAttributes().get(eq("memberOf"))).thenReturn(groupDN);
  }

  @Test
  public void testGetGroups() throws NamingException {
    // given a user whose ldap query returns a user object with three "memberOf"
    // properties, return an array of strings representing its groups.
    String[] testGroups = new String[] {"abc", "xyz", "sss"};
    doTestGetGroups(Arrays.asList(testGroups));
  }

  private void doTestGetGroups(List<String> expectedGroups)
      throws NamingException {
    String ldapUrl = "ldap://test";
    Configuration conf = getBaseConf(ldapUrl);
    // enable single-query lookup
    conf.set(LdapGroupsMapping.MEMBEROF_ATTR_KEY, "memberOf");

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = groupsMapping.getGroups("some_user");

    Assert.assertEquals(expectedGroups, groups);

    // We should have only made one query because single-query lookup is enabled
    verify(getContext(), times(1)).search(anyString(), anyString(),
        any(Object[].class), any(SearchControls.class));
  }
}