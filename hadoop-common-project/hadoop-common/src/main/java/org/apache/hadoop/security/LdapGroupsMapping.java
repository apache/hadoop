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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of {@link GroupMappingServiceProvider} which
 * connects directly to an LDAP server for determining group membership.
 * 
 * This provider should be used only if it is necessary to map users to
 * groups that reside exclusively in an Active Directory or LDAP installation.
 * The common case for a Hadoop installation will be that LDAP users and groups
 * materialized on the Unix servers, and for an installation like that,
 * ShellBasedUnixGroupsMapping is preferred. However, in cases where
 * those users and groups aren't materialized in Unix, but need to be used for
 * access control, this class may be used to communicate directly with the LDAP
 * server.
 * 
 * It is important to note that resolving group mappings will incur network
 * traffic, and may cause degraded performance, although user-group mappings
 * will be cached via the infrastructure provided by {@link Groups}.
 * 
 * This implementation does not support configurable search limits. If a filter
 * is used for searching users or groups which returns more results than are
 * allowed by the server, an exception will be thrown.
 * 
 * The implementation also does not attempt to resolve group hierarchies. In
 * order to be considered a member of a group, the user must be an explicit
 * member in LDAP.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class LdapGroupsMapping
    implements GroupMappingServiceProvider, Configurable {
  
  public static final String LDAP_CONFIG_PREFIX = "hadoop.security.group.mapping.ldap";

  /*
   * URL of the LDAP server
   */
  public static final String LDAP_URL_KEY = LDAP_CONFIG_PREFIX + ".url";
  public static final String LDAP_URL_DEFAULT = "";

  /*
   * Should SSL be used to connect to the server
   */
  public static final String LDAP_USE_SSL_KEY = LDAP_CONFIG_PREFIX + ".ssl";
  public static final Boolean LDAP_USE_SSL_DEFAULT = false;

  /*
   * File path to the location of the SSL keystore to use
   */
  public static final String LDAP_KEYSTORE_KEY = LDAP_CONFIG_PREFIX + ".ssl.keystore";
  public static final String LDAP_KEYSTORE_DEFAULT = "";

  /*
   * Password for the keystore
   */
  public static final String LDAP_KEYSTORE_PASSWORD_KEY = LDAP_CONFIG_PREFIX + ".ssl.keystore.password";
  public static final String LDAP_KEYSTORE_PASSWORD_DEFAULT = "";
  
  public static final String LDAP_KEYSTORE_PASSWORD_FILE_KEY = LDAP_KEYSTORE_PASSWORD_KEY + ".file";
  public static final String LDAP_KEYSTORE_PASSWORD_FILE_DEFAULT = "";


  /**
   * File path to the location of the SSL truststore to use
   */
  public static final String LDAP_TRUSTSTORE_KEY = LDAP_CONFIG_PREFIX +
      ".ssl.truststore";

  /**
   * The key of the credential entry containing the password for
   * the LDAP SSL truststore
   */
  public static final String LDAP_TRUSTSTORE_PASSWORD_KEY =
      LDAP_CONFIG_PREFIX +".ssl.truststore.password";

  /**
   * The path to a file containing the password for
   * the LDAP SSL truststore
   */
  public static final String LDAP_TRUSTSTORE_PASSWORD_FILE_KEY =
      LDAP_TRUSTSTORE_PASSWORD_KEY + ".file";

  /*
   * User to bind to the LDAP server with
   */
  public static final String BIND_USER_KEY = LDAP_CONFIG_PREFIX + ".bind.user";
  public static final String BIND_USER_DEFAULT = "";

  /*
   * Password for the bind user
   */
  public static final String BIND_PASSWORD_KEY = LDAP_CONFIG_PREFIX + ".bind.password";
  public static final String BIND_PASSWORD_DEFAULT = "";
  
  public static final String BIND_PASSWORD_FILE_KEY = BIND_PASSWORD_KEY + ".file";
  public static final String BIND_PASSWORD_FILE_DEFAULT = "";

  /*
   * Base distinguished name to use for searches
   */
  public static final String BASE_DN_KEY = LDAP_CONFIG_PREFIX + ".base";
  public static final String BASE_DN_DEFAULT = "";

  /*
   * Any additional filters to apply when searching for users
   */
  public static final String USER_SEARCH_FILTER_KEY = LDAP_CONFIG_PREFIX + ".search.filter.user";
  public static final String USER_SEARCH_FILTER_DEFAULT = "(&(objectClass=user)(sAMAccountName={0}))";

  /*
   * Any additional filters to apply when finding relevant groups
   */
  public static final String GROUP_SEARCH_FILTER_KEY = LDAP_CONFIG_PREFIX + ".search.filter.group";
  public static final String GROUP_SEARCH_FILTER_DEFAULT = "(objectClass=group)";

  /*
   * LDAP attribute to use for determining group membership
   */
  public static final String GROUP_MEMBERSHIP_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.member";
  public static final String GROUP_MEMBERSHIP_ATTR_DEFAULT = "member";

  /*
   * LDAP attribute to use for identifying a group's name
   */
  public static final String GROUP_NAME_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.group.name";
  public static final String GROUP_NAME_ATTR_DEFAULT = "cn";

  /*
   * LDAP attribute names to use when doing posix-like lookups
   */
  public static final String POSIX_UID_ATTR_KEY = LDAP_CONFIG_PREFIX + ".posix.attr.uid.name";
  public static final String POSIX_UID_ATTR_DEFAULT = "uidNumber";

  public static final String POSIX_GID_ATTR_KEY = LDAP_CONFIG_PREFIX + ".posix.attr.gid.name";
  public static final String POSIX_GID_ATTR_DEFAULT = "gidNumber";

  /*
   * Posix attributes
   */
  public static final String POSIX_GROUP = "posixGroup";
  public static final String POSIX_ACCOUNT = "posixAccount";

  /*
   * LDAP {@link SearchControls} attribute to set the time limit
   * for an invoked directory search. Prevents infinite wait cases.
   */
  public static final String DIRECTORY_SEARCH_TIMEOUT =
    LDAP_CONFIG_PREFIX + ".directory.search.timeout";
  public static final int DIRECTORY_SEARCH_TIMEOUT_DEFAULT = 10000; // 10s

  public static final String CONNECTION_TIMEOUT =
      LDAP_CONFIG_PREFIX + ".connection.timeout.ms";
  public static final int CONNECTION_TIMEOUT_DEFAULT = 60 * 1000; // 60 seconds
  public static final String READ_TIMEOUT =
      LDAP_CONFIG_PREFIX + ".read.timeout.ms";
  public static final int READ_TIMEOUT_DEFAULT = 60 * 1000; // 60 seconds

  private static final Log LOG = LogFactory.getLog(LdapGroupsMapping.class);

  private static final SearchControls SEARCH_CONTROLS = new SearchControls();
  static {
    SEARCH_CONTROLS.setSearchScope(SearchControls.SUBTREE_SCOPE);
  }

  private DirContext ctx;
  private Configuration conf;
  
  private String ldapUrl;
  private boolean useSsl;
  private String keystore;
  private String keystorePass;
  private String truststore;
  private String truststorePass;
  private String bindUser;
  private String bindPassword;
  private String baseDN;
  private String groupSearchFilter;
  private String userSearchFilter;
  private String groupMemberAttr;
  private String groupNameAttr;
  private String posixUidAttr;
  private String posixGidAttr;
  private boolean isPosix;

  public static final int RECONNECT_RETRY_COUNT = 3;
  
  /**
   * Returns list of groups for a user.
   * 
   * The LdapCtx which underlies the DirContext object is not thread-safe, so
   * we need to block around this whole method. The caching infrastructure will
   * ensure that performance stays in an acceptable range.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public synchronized List<String> getGroups(String user) {
    /*
     * Normal garbage collection takes care of removing Context instances when they are no longer in use. 
     * Connections used by Context instances being garbage collected will be closed automatically.
     * So in case connection is closed and gets CommunicationException, retry some times with new new DirContext/connection. 
     */
    for(int retry = 0; retry < RECONNECT_RETRY_COUNT; retry++) {
      try {
        return doGetGroups(user);
      } catch (NamingException e) {
        LOG.warn("Failed to get groups for user " + user + " (retry=" + retry
            + ") by " + e);
        LOG.trace("TRACE", e);
      }

      //reset ctx so that new DirContext can be created with new connection
      this.ctx = null;
    }
    
    return Collections.emptyList();
  }
  
  List<String> doGetGroups(String user) throws NamingException {
    List<String> groups = new ArrayList<String>();

    DirContext ctx = getDirContext();

    // Search for the user. We'll only ever need to look at the first result
    NamingEnumeration<SearchResult> results = ctx.search(baseDN,
        userSearchFilter,
        new Object[]{user},
        SEARCH_CONTROLS);
    if (results.hasMoreElements()) {
      SearchResult result = results.nextElement();
      String userDn = result.getNameInNamespace();

      NamingEnumeration<SearchResult> groupResults = null;

      if (isPosix) {
        String gidNumber = null;
        String uidNumber = null;
        Attribute gidAttribute = result.getAttributes().get(posixGidAttr);
        Attribute uidAttribute = result.getAttributes().get(posixUidAttr);
        if (gidAttribute != null) {
          gidNumber = gidAttribute.get().toString();
        }
        if (uidAttribute != null) {
          uidNumber = uidAttribute.get().toString();
        }
        if (uidNumber != null && gidNumber != null) {
          groupResults =
              ctx.search(baseDN,
                  "(&"+ groupSearchFilter + "(|(" + posixGidAttr + "={0})" +
                      "(" + groupMemberAttr + "={1})))",
                  new Object[] { gidNumber, uidNumber },
                  SEARCH_CONTROLS);
        }
      } else {
        groupResults =
            ctx.search(baseDN,
                "(&" + groupSearchFilter + "(" + groupMemberAttr + "={0}))",
                new Object[]{userDn},
                SEARCH_CONTROLS);
      }
      if (groupResults != null) {
        while (groupResults.hasMoreElements()) {
          SearchResult groupResult = groupResults.nextElement();
          Attribute groupName = groupResult.getAttributes().get(groupNameAttr);
          groups.add(groupName.get().toString());
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("doGetGroups(" + user + ") return " + groups);
    }
    return groups;
  }

  DirContext getDirContext() throws NamingException {
    if (ctx == null) {
      // Set up the initial environment for LDAP connectivity
      Hashtable<String, String> env = new Hashtable<String, String>();
      env.put(Context.INITIAL_CONTEXT_FACTORY,
          com.sun.jndi.ldap.LdapCtxFactory.class.getName());
      env.put(Context.PROVIDER_URL, ldapUrl);
      env.put(Context.SECURITY_AUTHENTICATION, "simple");

      // Set up SSL security, if necessary
      if (useSsl) {
        env.put(Context.SECURITY_PROTOCOL, "ssl");
        if (!keystore.isEmpty()) {
          System.setProperty("javax.net.ssl.keyStore", keystore);
        }
        if (!keystorePass.isEmpty()) {
          System.setProperty("javax.net.ssl.keyStorePassword", keystorePass);
        }
        if (!truststore.isEmpty()) {
          System.setProperty("javax.net.ssl.trustStore", truststore);
        }
        if (!truststorePass.isEmpty()) {
          System.setProperty("javax.net.ssl.trustStorePassword",
              truststorePass);
        }
      }

      env.put(Context.SECURITY_PRINCIPAL, bindUser);
      env.put(Context.SECURITY_CREDENTIALS, bindPassword);

      env.put("com.sun.jndi.ldap.connect.timeout", conf.get(CONNECTION_TIMEOUT,
          String.valueOf(CONNECTION_TIMEOUT_DEFAULT)));
      env.put("com.sun.jndi.ldap.read.timeout", conf.get(READ_TIMEOUT,
          String.valueOf(READ_TIMEOUT_DEFAULT)));

      ctx = new InitialDirContext(env);
    }

    return ctx;
  }
  
  /**
   * Caches groups, no need to do that for this provider
   */
  @Override
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Adds groups to cache, no need to do that for this provider
   *
   * @param groups unused
   */
  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    ldapUrl = conf.get(LDAP_URL_KEY, LDAP_URL_DEFAULT);
    if (ldapUrl == null || ldapUrl.isEmpty()) {
      throw new RuntimeException("LDAP URL is not configured");
    }

    useSsl = conf.getBoolean(LDAP_USE_SSL_KEY, LDAP_USE_SSL_DEFAULT);
    if (useSsl) {
      loadSslConf(conf);
    }
    
    bindUser = conf.get(BIND_USER_KEY, BIND_USER_DEFAULT);
    bindPassword = getPassword(conf, BIND_PASSWORD_KEY, BIND_PASSWORD_DEFAULT);
    if (bindPassword.isEmpty()) {
      bindPassword = extractPassword(
          conf.get(BIND_PASSWORD_FILE_KEY, BIND_PASSWORD_FILE_DEFAULT));
    }
    
    baseDN = conf.get(BASE_DN_KEY, BASE_DN_DEFAULT);
    groupSearchFilter =
        conf.get(GROUP_SEARCH_FILTER_KEY, GROUP_SEARCH_FILTER_DEFAULT);
    userSearchFilter =
        conf.get(USER_SEARCH_FILTER_KEY, USER_SEARCH_FILTER_DEFAULT);
    isPosix = groupSearchFilter.contains(POSIX_GROUP) && userSearchFilter
        .contains(POSIX_ACCOUNT);
    groupMemberAttr =
        conf.get(GROUP_MEMBERSHIP_ATTR_KEY, GROUP_MEMBERSHIP_ATTR_DEFAULT);
    groupNameAttr =
        conf.get(GROUP_NAME_ATTR_KEY, GROUP_NAME_ATTR_DEFAULT);
    posixUidAttr =
        conf.get(POSIX_UID_ATTR_KEY, POSIX_UID_ATTR_DEFAULT);
    posixGidAttr =
        conf.get(POSIX_GID_ATTR_KEY, POSIX_GID_ATTR_DEFAULT);

    int dirSearchTimeout = conf.getInt(DIRECTORY_SEARCH_TIMEOUT, DIRECTORY_SEARCH_TIMEOUT_DEFAULT);
    SEARCH_CONTROLS.setTimeLimit(dirSearchTimeout);
    // Limit the attributes returned to only those required to speed up the search.
    // See HADOOP-10626 and HADOOP-12001 for more details.
    SEARCH_CONTROLS.setReturningAttributes(
        new String[] {groupNameAttr, posixUidAttr, posixGidAttr});

    this.conf = conf;
  }

  private void loadSslConf(Configuration sslConf) {
    keystore = sslConf.get(LDAP_KEYSTORE_KEY, LDAP_KEYSTORE_DEFAULT);
    keystorePass = getPassword(sslConf, LDAP_KEYSTORE_PASSWORD_KEY,
        LDAP_KEYSTORE_PASSWORD_DEFAULT);
    if (keystorePass.isEmpty()) {
      keystorePass = extractPassword(sslConf.get(
          LDAP_KEYSTORE_PASSWORD_FILE_KEY,
          LDAP_KEYSTORE_PASSWORD_FILE_DEFAULT));
    }

    truststore = sslConf.get(LDAP_TRUSTSTORE_KEY, "");
    truststorePass = getPasswordFromCredentialProviders(
        sslConf, LDAP_TRUSTSTORE_PASSWORD_KEY, "");
    if (truststorePass.isEmpty()) {
      truststorePass = extractPassword(
          sslConf.get(LDAP_TRUSTSTORE_PASSWORD_FILE_KEY, ""));
    }
  }

  String getPasswordFromCredentialProviders(
      Configuration conf, String alias, String defaultPass) {
    String password = defaultPass;
    try {
      char[] passchars = conf.getPasswordFromCredentialProviders(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    } catch (IOException ioe) {
      LOG.warn("Exception while trying to get password for alias " + alias
          + ": ", ioe);
    }
    return password;
  }

  /**
   * Passwords should not be stored in configuration. Use
   * {@link #getPasswordFromCredentialProviders(
   *            Configuration, String, String)}
   * to avoid reading passwords from a configuration file.
   */
  @Deprecated
  String getPassword(Configuration conf, String alias, String defaultPass) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
      else {
        password = defaultPass;
      }
    }
    catch (IOException ioe) {
      LOG.warn("Exception while trying to password for alias " + alias + ": "
          + ioe.getMessage());
    }
    return password;
  }

  String extractPassword(String pwFile) {
    if (pwFile.isEmpty()) {
      // If there is no password file defined, we'll assume that we should do
      // an anonymous bind
      return "";
    }

    StringBuilder password = new StringBuilder();
    try (Reader reader = new InputStreamReader(
        new FileInputStream(pwFile), Charsets.UTF_8)) {
      int c = reader.read();
      while (c > -1) {
        password.append((char)c);
        c = reader.read();
      }
      return password.toString().trim();
    } catch (IOException ioe) {
      throw new RuntimeException("Could not read password file: " + pwFile, ioe);
    }
  }
}
