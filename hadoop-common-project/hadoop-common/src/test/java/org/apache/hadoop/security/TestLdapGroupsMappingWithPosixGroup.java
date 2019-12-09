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

import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestLdapGroupsMappingWithPosixGroup
  extends TestLdapGroupsMappingBase {

  @Before
  public void setupMocks() throws NamingException {
    Attribute uidNumberAttr = mock(Attribute.class);
    Attribute gidNumberAttr = mock(Attribute.class);
    Attribute uidAttr = mock(Attribute.class);
    Attributes attributes = getAttributes();

    when(uidAttr.get()).thenReturn("some_user");
    when(uidNumberAttr.get()).thenReturn("700");
    when(gidNumberAttr.get()).thenReturn("600");
    when(attributes.get(eq("uid"))).thenReturn(uidAttr);
    when(attributes.get(eq("uidNumber"))).thenReturn(uidNumberAttr);
    when(attributes.get(eq("gidNumber"))).thenReturn(gidNumberAttr);
  }

  @Test
  public void testGetGroups() throws NamingException {
    // The search functionality of the mock context is reused, so we will
    // return the user NamingEnumeration first, and then the group
    when(getContext().search(anyString(), contains("posix"),
        any(Object[].class), any(SearchControls.class)))
        .thenReturn(getUserNames(), getGroupNames());

    doTestGetGroups(Arrays.asList(getTestGroups()), 2);
  }

  private void doTestGetGroups(List<String> expectedGroups, int searchTimes)
      throws NamingException {
    String ldapUrl = "ldap://test";
    Configuration conf = getBaseConf(ldapUrl);
    conf.set(LdapGroupsMapping.GROUP_SEARCH_FILTER_KEY,
        "(objectClass=posixGroup)(cn={0})");
    conf.set(LdapGroupsMapping.USER_SEARCH_FILTER_KEY,
        "(objectClass=posixAccount)");
    conf.set(LdapGroupsMapping.GROUP_MEMBERSHIP_ATTR_KEY, "memberUid");
    conf.set(LdapGroupsMapping.POSIX_UID_ATTR_KEY, "uidNumber");
    conf.set(LdapGroupsMapping.POSIX_GID_ATTR_KEY, "gidNumber");
    conf.set(LdapGroupsMapping.GROUP_NAME_ATTR_KEY, "cn");

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = groupsMapping.getGroups("some_user");

    Assert.assertEquals(expectedGroups, groups);

    groupsMapping.getConf().set(LdapGroupsMapping.POSIX_UID_ATTR_KEY, "uid");

    Assert.assertEquals(expectedGroups, groups);

    // We should have searched for a user, and then two groups
    verify(getContext(), times(searchTimes)).search(anyString(),
        anyString(),
        any(Object[].class),
        any(SearchControls.class));
  }
}
