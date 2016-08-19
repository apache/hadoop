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

import static org.apache.hadoop.security.LdapGroupsMapping.CONNECTION_TIMEOUT;
import static org.apache.hadoop.security.LdapGroupsMapping.READ_TIMEOUT;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.HashSet;

import javax.naming.CommunicationException;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class TestLdapGroupsMapping extends TestLdapGroupsMappingBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestLdapGroupsMapping.class);

  /**
   * To construct a LDAP InitialDirContext object, it will firstly initiate a
   * protocol session to server for authentication. After a session is
   * established, a method of authentication is negotiated between the server
   * and the client. When the client is authenticated, the LDAP server will send
   * a bind response, whose message contents are bytes as the
   * {@link #AUTHENTICATE_SUCCESS_MSG}. After receiving this bind response
   * message, the LDAP context is considered connected to the server and thus
   * can issue query requests for determining group membership.
   */
  private static final byte[] AUTHENTICATE_SUCCESS_MSG =
      {48, 12, 2, 1, 1, 97, 7, 10, 1, 0, 4, 0, 4, 0};

  @Before
  public void setupMocks() throws NamingException {
    when(getUserSearchResult().getNameInNamespace()).
        thenReturn("CN=some_user,DC=test,DC=com");
  }
  
  @Test
  public void testGetGroups() throws IOException, NamingException {
    // The search functionality of the mock context is reused, so we will
    // return the user NamingEnumeration first, and then the group
    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class)))
        .thenReturn(getUserNames(), getGroupNames());
    doTestGetGroups(Arrays.asList(getTestGroups()), 2);
  }

  @Test
  public void testGetGroupsWithHierarchy() throws IOException, NamingException {
    // The search functionality of the mock context is reused, so we will
    // return the user NamingEnumeration first, and then the group
    // The parent search is run once for each level, and is a different search
    // The parent group is returned once for each group, yet the final list
    // should be unique
    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class)))
        .thenReturn(getUserNames(), getGroupNames());
    when(getContext().search(anyString(), anyString(),
        any(SearchControls.class)))
        .thenReturn(getParentGroupNames());
    doTestGetGroupsWithParent(Arrays.asList(getTestParentGroups()), 2, 1);
  }

  @Test
  public void testGetGroupsWithConnectionClosed() throws IOException, NamingException {
    // The case mocks connection is closed/gc-ed, so the first search call throws CommunicationException,
    // then after reconnected return the user NamingEnumeration first, and then the group
    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class)))
        .thenThrow(new CommunicationException("Connection is closed"))
        .thenReturn(getUserNames(), getGroupNames());
    
    // Although connection is down but after reconnected
    // it still should retrieve the result groups
    // 1 is the first failure call
    doTestGetGroups(Arrays.asList(getTestGroups()), 1 + 2);
  }

  @Test
  public void testGetGroupsWithLdapDown() throws IOException, NamingException {
    // This mocks the case where Ldap server is down, and always throws CommunicationException 
    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class)))
        .thenThrow(new CommunicationException("Connection is closed"));
    
    // Ldap server is down, no groups should be retrieved
    doTestGetGroups(Arrays.asList(new String[] {}), 
        LdapGroupsMapping.RECONNECT_RETRY_COUNT);
  }
  
  private void doTestGetGroups(List<String> expectedGroups, int searchTimes) throws IOException, NamingException {  
    Configuration conf = new Configuration();
    // Set this, so we don't throw an exception
    conf.set(LdapGroupsMapping.LDAP_URL_KEY, "ldap://test");

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = groupsMapping.getGroups("some_user");
    
    Assert.assertEquals(expectedGroups, groups);
    
    // We should have searched for a user, and then two groups
    verify(getContext(), times(searchTimes)).search(anyString(),
                                         anyString(),
                                         any(Object[].class),
                                         any(SearchControls.class));
  }

  private void doTestGetGroupsWithParent(List<String> expectedGroups,
      int searchTimesGroup, int searchTimesParentGroup)
          throws IOException, NamingException {
    Configuration conf = new Configuration();
    // Set this, so we don't throw an exception
    conf.set(LdapGroupsMapping.LDAP_URL_KEY, "ldap://test");
    // Set the config to get parents 1 level up
    conf.setInt(LdapGroupsMapping.GROUP_HIERARCHY_LEVELS_KEY, 1);

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = groupsMapping.getGroups("some_user");

    // compare lists, ignoring the order
    Assert.assertEquals(new HashSet<String>(expectedGroups),
        new HashSet<String>(groups));

    // We should have searched for a user, and group
    verify(getContext(), times(searchTimesGroup)).search(anyString(),
                                         anyString(),
                                         any(Object[].class),
                                         any(SearchControls.class));
    // One groups search for the parent group should have been done
    verify(getContext(), times(searchTimesParentGroup)).search(anyString(),
                                         anyString(),
                                         any(SearchControls.class));
  }

  @Test
  public void testExtractPassword() throws IOException {
    File testDir = GenericTestUtils.getTestDir();
    testDir.mkdirs();
    File secretFile = new File(testDir, "secret.txt");
    Writer writer = new FileWriter(secretFile);
    writer.write("hadoop");
    writer.close();
    
    LdapGroupsMapping mapping = new LdapGroupsMapping();
    Assert.assertEquals("hadoop",
        mapping.extractPassword(secretFile.getPath()));
  }

  @Test
  public void testConfGetPassword() throws Exception {
    File testDir = GenericTestUtils.getTestDir();
    Configuration conf = new Configuration();
    final Path jksPath = new Path(testDir.toString(), "test.jks");
    final String ourUrl =
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(testDir, "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    char[] bindpass = {'b', 'i', 'n', 'd', 'p', 'a', 's', 's'};
    char[] storepass = {'s', 't', 'o', 'r', 'e', 'p', 'a', 's', 's'};

    // ensure that we get nulls when the key isn't there
    assertEquals(null, provider.getCredentialEntry(
        LdapGroupsMapping.BIND_PASSWORD_KEY));
    assertEquals(null, provider.getCredentialEntry
        (LdapGroupsMapping.LDAP_KEYSTORE_PASSWORD_KEY));

    // create new aliases
    try {
      provider.createCredentialEntry(
          LdapGroupsMapping.BIND_PASSWORD_KEY, bindpass);

      provider.createCredentialEntry(
          LdapGroupsMapping.LDAP_KEYSTORE_PASSWORD_KEY, storepass);
      provider.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // make sure we get back the right key
    assertArrayEquals(bindpass, provider.getCredentialEntry(
        LdapGroupsMapping.BIND_PASSWORD_KEY).getCredential());
    assertArrayEquals(storepass, provider.getCredentialEntry(
        LdapGroupsMapping.LDAP_KEYSTORE_PASSWORD_KEY).getCredential());

    LdapGroupsMapping mapping = new LdapGroupsMapping();
    Assert.assertEquals("bindpass",
        mapping.getPassword(conf, LdapGroupsMapping.BIND_PASSWORD_KEY, ""));
    Assert.assertEquals("storepass",
        mapping.getPassword(conf, LdapGroupsMapping.LDAP_KEYSTORE_PASSWORD_KEY,
           ""));
    // let's make sure that a password that doesn't exist returns an
    // empty string as currently expected and used to trigger a call to
    // extract password
    Assert.assertEquals("", mapping.getPassword(conf,"invalid-alias", ""));
  }

  /**
   * Test that if the {@link LdapGroupsMapping#CONNECTION_TIMEOUT} is set in the
   * configuration, the LdapGroupsMapping connection will timeout by this value
   * if it does not get a LDAP response from the server.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout = 30000)
  public void testLdapConnectionTimeout()
      throws IOException, InterruptedException {
    final int connectionTimeoutMs = 3 * 1000; // 3s
    try (ServerSocket serverSock = new ServerSocket(0)) {
      final CountDownLatch finLatch = new CountDownLatch(1);

      // Below we create a LDAP server which will accept a client request;
      // but it will never reply to the bind (connect) request.
      // Client of this LDAP server is expected to get a connection timeout.
      final Thread ldapServer = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            try (Socket ignored = serverSock.accept()) {
              finLatch.await();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
      ldapServer.start();

      final LdapGroupsMapping mapping = new LdapGroupsMapping();
      final Configuration conf = new Configuration();
      conf.set(LdapGroupsMapping.LDAP_URL_KEY,
          "ldap://localhost:" + serverSock.getLocalPort());
      conf.setInt(CONNECTION_TIMEOUT, connectionTimeoutMs);
      mapping.setConf(conf);

      try {
        mapping.doGetGroups("hadoop", 1);
        fail("The LDAP query should have timed out!");
      } catch (NamingException ne) {
        LOG.debug("Got the exception while LDAP querying: ", ne);
        assertExceptionContains("LDAP response read timed out, timeout used:" +
            connectionTimeoutMs + "ms", ne);
        assertFalse(ne.getMessage().contains("remaining name"));
      } finally {
        finLatch.countDown();
      }
      ldapServer.join();
    }
  }

  /**
   * Test that if the {@link LdapGroupsMapping#READ_TIMEOUT} is set in the
   * configuration, the LdapGroupsMapping query will timeout by this value if
   * it does not get a LDAP response from the server.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(timeout = 30000)
  public void testLdapReadTimeout() throws IOException, InterruptedException {
    final int readTimeoutMs = 4 * 1000; // 4s
    try (ServerSocket serverSock = new ServerSocket(0)) {
      final CountDownLatch finLatch = new CountDownLatch(1);

      // Below we create a LDAP server which will accept a client request,
      // authenticate it successfully; but it will never reply to the following
      // query request.
      // Client of this LDAP server is expected to get a read timeout.
      final Thread ldapServer = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            try (Socket clientSock = serverSock.accept()) {
              IOUtils.skipFully(clientSock.getInputStream(), 1);
              clientSock.getOutputStream().write(AUTHENTICATE_SUCCESS_MSG);
              finLatch.await();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
      ldapServer.start();

      final LdapGroupsMapping mapping = new LdapGroupsMapping();
      final Configuration conf = new Configuration();
      conf.set(LdapGroupsMapping.LDAP_URL_KEY,
          "ldap://localhost:" + serverSock.getLocalPort());
      conf.setInt(READ_TIMEOUT, readTimeoutMs);
      mapping.setConf(conf);

      try {
        mapping.doGetGroups("hadoop", 1);
        fail("The LDAP query should have timed out!");
      } catch (NamingException ne) {
        LOG.debug("Got the exception while LDAP querying: ", ne);
        assertExceptionContains("LDAP response read timed out, timeout used:" +
            readTimeoutMs + "ms", ne);
        assertExceptionContains("remaining name", ne);
      } finally {
        finLatch.countDown();
      }
      ldapServer.join();
    }
  }

  /**
   * Make sure that when
   * {@link Configuration#getPassword(String)} throws an IOException,
   * {@link LdapGroupsMapping#setConf(Configuration)} does not throw an NPE.
   *
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testSetConf() throws Exception {
    Configuration conf = new Configuration();
    Configuration mockConf = Mockito.spy(conf);
    when(mockConf.getPassword(anyString()))
        .thenThrow(new IOException("injected IOException"));
    // Set a dummy LDAP server URL.
    mockConf.set(LdapGroupsMapping.LDAP_URL_KEY, "ldap://test");

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(mockConf);
  }

}
