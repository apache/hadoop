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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

public class TestLdapGroupsMappingBase {
  @Mock
  private DirContext context;
  @Mock
  private NamingEnumeration<SearchResult> userNames;
  @Mock
  private NamingEnumeration<SearchResult> groupNames;
  @Mock
  private NamingEnumeration<SearchResult> parentGroupNames;
  @Mock
  private SearchResult userSearchResult;
  @Mock
  private Attributes attributes;
  @Spy
  private LdapGroupsMapping groupsMapping = new LdapGroupsMapping();

  private String[] testGroups = new String[] {"group1", "group2"};
  private String[] testParentGroups =
      new String[] {"group1", "group2", "group1_1"};

  @Before
  public void setupMocksBase() throws NamingException {
    MockitoAnnotations.initMocks(this);
    DirContext ctx = getContext();
    doReturn(ctx).when(groupsMapping).getDirContext();

    when(ctx.search(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(Object[].class), Mockito.any(SearchControls.class))).
        thenReturn(userNames);
    // We only ever call hasMoreElements once for the user NamingEnum, so
    // we can just have one return value
    when(userNames.hasMoreElements()).thenReturn(true);

    SearchResult groupSearchResult = mock(SearchResult.class);
    // We're going to have to define the loop here. We want two iterations,
    // to get both the groups
    when(groupNames.hasMoreElements()).thenReturn(true, true, false);
    when(groupNames.nextElement()).thenReturn(groupSearchResult);

    // Define the attribute for the name of the first group
    Attribute group1Attr = new BasicAttribute("cn");
    group1Attr.add(testGroups[0]);
    Attributes group1Attrs = new BasicAttributes();
    group1Attrs.put(group1Attr);

    // Define the attribute for the name of the second group
    Attribute group2Attr = new BasicAttribute("cn");
    group2Attr.add(testGroups[1]);
    Attributes group2Attrs = new BasicAttributes();
    group2Attrs.put(group2Attr);

    // This search result gets reused, so return group1, then group2
    when(groupSearchResult.getAttributes()).
        thenReturn(group1Attrs, group2Attrs);

    when(getUserNames().nextElement()).
        thenReturn(getUserSearchResult());

    when(getUserSearchResult().getAttributes()).thenReturn(getAttributes());
    // Define results for groups 1 level up
    SearchResult parentGroupResult = mock(SearchResult.class);

    // only one parent group
    when(parentGroupNames.hasMoreElements()).thenReturn(true, false);
    when(parentGroupNames.nextElement()).
        thenReturn(parentGroupResult);

    // Define the attribute for the parent group
    Attribute parentGroup1Attr = new BasicAttribute("cn");
    parentGroup1Attr.add(testParentGroups[2]);
    Attributes parentGroup1Attrs = new BasicAttributes();
    parentGroup1Attrs.put(parentGroup1Attr);

    // attach the attributes to the result
    when(parentGroupResult.getAttributes()).thenReturn(parentGroup1Attrs);
    when(parentGroupResult.getNameInNamespace()).
        thenReturn("CN=some_group,DC=test,DC=com");
  }

  protected DirContext getContext() {
    return context;
  }
  protected NamingEnumeration<SearchResult> getUserNames() {
    return userNames;
  }

  protected NamingEnumeration<SearchResult> getGroupNames() {
    return groupNames;
  }

  protected SearchResult getUserSearchResult() {
    return userSearchResult;
  }

  protected Attributes getAttributes() {
    return attributes;
  }

  protected LdapGroupsMapping getGroupsMapping() {
    return groupsMapping;
  }
  protected String[] getTestGroups() {
    return testGroups;
  }
  protected NamingEnumeration getParentGroupNames() {
    return parentGroupNames;
  }
  protected String[] getTestParentGroups() {
    return testParentGroups;
  }
}
