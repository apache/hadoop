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

import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_CTX_FACTORY_CLASS_DEFAULT;
import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_CTX_FACTORY_CLASS_KEY;
import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_URL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.spi.InitialContextFactory;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Hashtable;

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
    DummyLdapCtxFactory.reset();
    MockitoAnnotations.initMocks(this);
    DirContext ctx = getContext();

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

  protected Configuration getBaseConf() {
    return getBaseConf("ldap://test");
  }

  protected Configuration getBaseConf(String ldapUrl) {
    return getBaseConf(ldapUrl, getContext());
  }

  protected Configuration getBaseConf(
      String ldapUrl, DirContext contextToReturn) {
    DummyLdapCtxFactory.setContextToReturn(contextToReturn);
    DummyLdapCtxFactory.setExpectedLdapUrl(ldapUrl);

    Configuration conf = new Configuration();
    conf.set(LDAP_URL_KEY, ldapUrl);
    conf.setClass(LDAP_CTX_FACTORY_CLASS_KEY, DummyLdapCtxFactory.class,
        InitialContextFactory.class);
    return conf;
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

  /**
   * Ldap Context Factory implementation to be used for testing to check
   * contexts are requested for the expected LDAP server URLs etc.
   */
  public static class DummyLdapCtxFactory implements InitialContextFactory {

    private static DirContext contextToReturn;
    private static String expectedLdapUrl;
    private static String expectedBindUser;
    private static String expectedBindPassword;

    public DummyLdapCtxFactory() {
    }

    protected static void setContextToReturn(DirContext ctx) {
      contextToReturn = ctx;
    }

    protected static void setExpectedLdapUrl(String url) {
      expectedLdapUrl = url;
    }

    public static void setExpectedBindUser(String bindUser) {
      expectedBindUser = bindUser;
    }

    public static void setExpectedBindPassword(String bindPassword) {
      expectedBindPassword = bindPassword;
    }

    public static void reset() {
      expectedLdapUrl = null;
      expectedBindUser = null;
      expectedBindPassword = null;
    }

    @Override
    public Context getInitialContext(Hashtable<?, ?> env)
        throws NamingException {
      if (expectedLdapUrl != null) {
        String actualLdapUrl = (String) env.get(Context.PROVIDER_URL);
        assertEquals(expectedLdapUrl, actualLdapUrl);
      }
      if (expectedBindUser != null) {
        String actualBindUser = (String) env.get(Context.SECURITY_PRINCIPAL);
        assertEquals(expectedBindUser, actualBindUser);
      }
      if (expectedBindPassword != null) {
        String actualBindPassword = (String) env.get(
            Context.SECURITY_CREDENTIALS);
        assertEquals(expectedBindPassword, actualBindPassword);
      }
      if (contextToReturn == null) {
        InitialContextFactory defaultFactory = null;
        try {
          defaultFactory = LDAP_CTX_FACTORY_CLASS_DEFAULT.newInstance();
        } catch (ReflectiveOperationException e) {
          fail("Could not initialize the default factory");
        }
        return defaultFactory.getInitialContext(env);
      }
      return contextToReturn;
    }
  }
}
