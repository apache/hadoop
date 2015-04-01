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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;

import org.junit.Before;

public class TestLdapGroupsMappingBase {
  protected DirContext mockContext;

  protected LdapGroupsMapping mappingSpy = spy(new LdapGroupsMapping());
  protected NamingEnumeration mockUserNamingEnum =
      mock(NamingEnumeration.class);
  protected NamingEnumeration mockGroupNamingEnum =
      mock(NamingEnumeration.class);
  protected String[] testGroups = new String[] {"group1", "group2"};

  @Before
  public void setupMocksBase() throws NamingException {
    mockContext = mock(DirContext.class);
    doReturn(mockContext).when(mappingSpy).getDirContext();

    // We only ever call hasMoreElements once for the user NamingEnum, so
    // we can just have one return value
    when(mockUserNamingEnum.hasMoreElements()).thenReturn(true);

    SearchResult mockGroupResult = mock(SearchResult.class);
    // We're going to have to define the loop here. We want two iterations,
    // to get both the groups
    when(mockGroupNamingEnum.hasMoreElements()).thenReturn(true, true, false);
    when(mockGroupNamingEnum.nextElement()).thenReturn(mockGroupResult);

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
    when(mockGroupResult.getAttributes()).thenReturn(group1Attrs, group2Attrs);
  }
}
