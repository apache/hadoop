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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Collection;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.naming.spi.InitialContextFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * The implementation attempts to resolve group hierarchies,
 * to a configurable limit.
 * If the limit is 0, in order to be considered a member of a group,
 * the user must be an explicit member in LDAP.  Otherwise, it will traverse the
 * group hierarchy n levels up.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class LdapGroupsMapping
    implements GroupMappingServiceProvider, Configurable {
  
  public static final String LDAP_CONFIG_PREFIX = "hadoop.security.group.mapping.ldap";

  /*
   * URL of the LDAP server(s)
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
   * User aliases to bind to the LDAP server with. Each alias will have
   * to have its username and password configured, see core-default.xml
   * and GroupsMapping.md for details.
   */
  public static final String BIND_USERS_KEY = LDAP_CONFIG_PREFIX +
      ".bind.users";

  /*
   * User to bind to the LDAP server with
   */
  public static final String BIND_USER_SUFFIX = ".bind.user";
  public static final String BIND_USER_KEY = LDAP_CONFIG_PREFIX +
      BIND_USER_SUFFIX;
  public static final String BIND_USER_DEFAULT = "";

  /*
   * Password for the bind user
   */
  public static final String BIND_PASSWORD_SUFFIX = ".bind.password";
  public static final String BIND_PASSWORD_KEY = LDAP_CONFIG_PREFIX +
      BIND_PASSWORD_SUFFIX;
  public static final String BIND_PASSWORD_DEFAULT = "";

  public static final String BIND_PASSWORD_FILE_SUFFIX =
      BIND_PASSWORD_SUFFIX + ".file";
  public static final String BIND_PASSWORD_FILE_KEY = LDAP_CONFIG_PREFIX +
      BIND_PASSWORD_FILE_SUFFIX;
  public static final String BIND_PASSWORD_FILE_DEFAULT = "";

  public static final String BIND_PASSWORD_ALIAS_SUFFIX =
      BIND_PASSWORD_SUFFIX + ".alias";
  public static final String BIND_PASSWORD_ALIAS_KEY =
      LDAP_CONFIG_PREFIX + BIND_PASSWORD_ALIAS_SUFFIX;
  public static final String BIND_PASSWORD_ALIAS_DEFAULT = "";

  /*
   * Base distinguished name to use for searches
   */
  public static final String BASE_DN_KEY = LDAP_CONFIG_PREFIX + ".base";
  public static final String BASE_DN_DEFAULT = "";

  /*
   * Base DN used in user search.
   */
  public static final String USER_BASE_DN_KEY =
          LDAP_CONFIG_PREFIX + ".userbase";

  /*
   * Base DN used in group search.
   */
  public static final String GROUP_BASE_DN_KEY =
          LDAP_CONFIG_PREFIX + ".groupbase";


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
  public static final String MEMBEROF_ATTR_KEY =
      LDAP_CONFIG_PREFIX + ".search.attr.memberof";
  public static final String MEMBEROF_ATTR_DEFAULT = "";

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
   * How many levels to traverse when checking for groups in the org hierarchy
   */
  public static final String GROUP_HIERARCHY_LEVELS_KEY =
        LDAP_CONFIG_PREFIX + ".search.group.hierarchy.levels";
  public static final int GROUP_HIERARCHY_LEVELS_DEFAULT = 0;

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

  public static final String LDAP_NUM_ATTEMPTS_KEY =
      LDAP_CONFIG_PREFIX + ".num.attempts";
  public static final int LDAP_NUM_ATTEMPTS_DEFAULT = 3;

  public static final String LDAP_NUM_ATTEMPTS_BEFORE_FAILOVER_KEY =
      LDAP_CONFIG_PREFIX + ".num.attempts.before.failover";
  public static final int LDAP_NUM_ATTEMPTS_BEFORE_FAILOVER_DEFAULT =
      LDAP_NUM_ATTEMPTS_DEFAULT;

  public static final String LDAP_CTX_FACTORY_CLASS_KEY =
      LDAP_CONFIG_PREFIX + ".ctx.factory.class";

  public static final String LDAP_CTX_FACTORY_CLASS_DEFAULT =
      "com.sun.jndi.ldap.LdapCtxFactory";

  /**
   * The env key used for specifying a custom socket factory to be used for
   * creating connections to the LDAP server. This is not a Hadoop conf key.
   */
  private static final String LDAP_SOCKET_FACTORY_ENV_KEY =
      "java.naming.ldap.factory.socket";

  private static final Logger LOG =
      LoggerFactory.getLogger(LdapGroupsMapping.class);

  static final SearchControls SEARCH_CONTROLS = new SearchControls();
  static {
    SEARCH_CONTROLS.setSearchScope(SearchControls.SUBTREE_SCOPE);
  }

  private DirContext ctx;
  private Configuration conf;

  private Iterator<String> ldapUrls;
  private String currentLdapUrl;

  private boolean useSsl;
  private String keystore;
  private String keystorePass;
  private String truststore;
  private String truststorePass;

  /*
   * Users to bind to when connecting to LDAP. This will be a rotating
   * iterator, cycling back to the first user if necessary.
   */
  private Iterator<BindUserInfo> bindUsers;
  private BindUserInfo currentBindUser;

  private String userbaseDN;
  private String groupbaseDN;
  private String groupSearchFilter;
  private String userSearchFilter;
  private String memberOfAttr;
  private String groupMemberAttr;
  private String groupNameAttr;
  private int groupHierarchyLevels;
  private String posixUidAttr;
  private String posixGidAttr;
  private boolean isPosix;
  private boolean useOneQuery;
  private int numAttempts;
  private int numAttemptsBeforeFailover;
  private String ldapCtxFactoryClassName;

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
     * Normal garbage collection takes care of removing Context instances when
     * they are no longer in use. Connections used by Context instances being
     * garbage collected will be closed automatically. So in case connection is
     * closed and gets CommunicationException, retry some times with new new
     * DirContext/connection.
     */

    // Tracks the number of attempts made using the same LDAP server
    int atemptsBeforeFailover = 1;

    for (int attempt = 1; attempt <= numAttempts; attempt++,
        atemptsBeforeFailover++) {
      try {
        return doGetGroups(user, groupHierarchyLevels);
      } catch (AuthenticationException e) {
        switchBindUser(e);
      } catch (NamingException e) {
        LOG.warn("Failed to get groups for user {} (attempt={}/{}) using {}. " +
            "Exception: ", user, attempt, numAttempts, currentLdapUrl, e);
        LOG.trace("TRACE", e);

        if (failover(atemptsBeforeFailover, numAttemptsBeforeFailover)) {
          atemptsBeforeFailover = 0;
        }
      }

      // Reset ctx so that new DirContext can be created with new connection
      this.ctx = null;
    }
    
    return Collections.emptyList();
  }

  /**
   * A helper method to get the Relative Distinguished Name (RDN) from
   * Distinguished name (DN). According to Active Directory documentation,
   * a group object's RDN is a CN.
   *
   * @param distinguishedName A string representing a distinguished name.
   * @throws NamingException if the DN is malformed.
   * @return a string which represents the RDN
   */
  private String getRelativeDistinguishedName(String distinguishedName)
      throws NamingException {
    LdapName ldn = new LdapName(distinguishedName);
    List<Rdn> rdns = ldn.getRdns();
    if (rdns.isEmpty()) {
      throw new NamingException("DN is empty");
    }
    Rdn rdn = rdns.get(rdns.size()-1);
    if (rdn.getType().equalsIgnoreCase(groupNameAttr)) {
      String groupName = (String)rdn.getValue();
      return groupName;
    }
    throw new NamingException("Unable to find RDN: The DN " +
    distinguishedName + " is malformed.");
  }

  /**
   * Look up groups using posixGroups semantics. Use posix gid/uid to find
   * groups of the user.
   *
   * @param result the result object returned from the prior user lookup.
   * @param c the context object of the LDAP connection.
   * @return an object representing the search result.
   *
   * @throws NamingException if the server does not support posixGroups
   * semantics.
   */
  private NamingEnumeration<SearchResult> lookupPosixGroup(SearchResult result,
      DirContext c) throws NamingException {
    String gidNumber = null;
    String uidNumber = null;
    Attribute gidAttribute = result.getAttributes().get(posixGidAttr);
    Attribute uidAttribute = result.getAttributes().get(posixUidAttr);
    String reason = "";
    if (gidAttribute == null) {
      reason = "Can't find attribute '" + posixGidAttr + "'.";
    } else {
      gidNumber = gidAttribute.get().toString();
    }
    if (uidAttribute == null) {
      reason = "Can't find attribute '" + posixUidAttr + "'.";
    } else {
      uidNumber = uidAttribute.get().toString();
    }
    if (uidNumber != null && gidNumber != null) {
      return c.search(groupbaseDN,
              "(&"+ groupSearchFilter + "(|(" + posixGidAttr + "={0})" +
                  "(" + groupMemberAttr + "={1})))",
              new Object[] {gidNumber, uidNumber},
              SEARCH_CONTROLS);
    }
    throw new NamingException("The server does not support posixGroups " +
        "semantics. Reason: " + reason +
        " Returned user object: " + result.toString());
  }

  /**
   * Perform the second query to get the groups of the user.
   *
   * If posixGroups is enabled, use use posix gid/uid to find.
   * Otherwise, use the general group member attribute to find it.
   *
   * @param result the result object returned from the prior user lookup.
   * @param c the context object of the LDAP connection.
   * @return a list of strings representing group names of the user.
   * @throws NamingException if unable to find group names
   */
  private List<String> lookupGroup(SearchResult result, DirContext c,
      int goUpHierarchy)
      throws NamingException {
    List<String> groups = new ArrayList<>();
    Set<String> groupDNs = new HashSet<>();

    NamingEnumeration<SearchResult> groupResults;
    // perform the second LDAP query
    if (isPosix) {
      groupResults = lookupPosixGroup(result, c);
    } else {
      String userDn = result.getNameInNamespace();
      groupResults =
          c.search(groupbaseDN,
              "(&" + groupSearchFilter + "(" + groupMemberAttr + "={0}))",
              new Object[]{userDn},
              SEARCH_CONTROLS);
    }
    // if the second query is successful, group objects of the user will be
    // returned. Get group names from the returned objects.
    if (groupResults != null) {
      while (groupResults.hasMoreElements()) {
        SearchResult groupResult = groupResults.nextElement();
        getGroupNames(groupResult, groups, groupDNs, goUpHierarchy > 0);
      }
      if (goUpHierarchy > 0 && !isPosix) {
        // convert groups to a set to ensure uniqueness
        Set<String> groupset = new HashSet<>(groups);
        goUpGroupHierarchy(groupDNs, goUpHierarchy, groupset);
        // convert set back to list for compatibility
        groups = new ArrayList<>(groupset);
      }
    }
    return groups;
  }

  /**
   * Perform LDAP queries to get group names of a user.
   *
   * Perform the first LDAP query to get the user object using the user's name.
   * If one-query is enabled, retrieve the group names from the user object.
   * If one-query is disabled, or if it failed, perform the second query to
   * get the groups.
   *
   * @param user user name
   * @return a list of group names for the user. If the user can not be found,
   * return an empty string array.
   * @throws NamingException if unable to get group names
   */
  List<String> doGetGroups(String user, int goUpHierarchy)
      throws NamingException {
    DirContext c = getDirContext();

    // Search for the user. We'll only ever need to look at the first result
    NamingEnumeration<SearchResult> results = c.search(userbaseDN,
        userSearchFilter, new Object[]{user}, SEARCH_CONTROLS);
    // return empty list if the user can not be found.
    if (!results.hasMoreElements()) {
      LOG.debug("doGetGroups({}) returned no groups because the " +
          "user is not found.", user);
      return new ArrayList<>();
    }
    SearchResult result = results.nextElement();

    List<String> groups = null;
    if (useOneQuery) {
      try {
        /**
         * For Active Directory servers, the user object has an attribute
         * 'memberOf' that represents the DNs of group objects to which the
         * user belongs. So the second query may be skipped.
         */
        Attribute groupDNAttr = result.getAttributes().get(memberOfAttr);
        if (groupDNAttr == null) {
          throw new NamingException("The user object does not have '" +
              memberOfAttr + "' attribute." +
              "Returned user object: " + result.toString());
        }
        groups = new ArrayList<>();
        NamingEnumeration groupEnumeration = groupDNAttr.getAll();
        while (groupEnumeration.hasMore()) {
          String groupDN = groupEnumeration.next().toString();
          groups.add(getRelativeDistinguishedName(groupDN));
        }
      } catch (NamingException e) {
        // If the first lookup failed, fall back to the typical scenario.
        LOG.info("Failed to get groups from the first lookup. Initiating " +
                "the second LDAP query using the user's DN.", e);
      }
    }
    if (groups == null || groups.isEmpty() || goUpHierarchy > 0) {
      groups = lookupGroup(result, c, goUpHierarchy);
    }
    LOG.debug("doGetGroups({}) returned {}", user, groups);
    return groups;
  }

  /* Helper function to get group name from search results.
  */
  void getGroupNames(SearchResult groupResult, Collection<String> groups,
                     Collection<String> groupDNs, boolean doGetDNs)
                     throws NamingException {
    Attribute groupName = groupResult.getAttributes().get(groupNameAttr);
    if (groupName == null) {
      throw new NamingException("The group object does not have " +
        "attribute '" + groupNameAttr + "'.");
    }
    groups.add(groupName.get().toString());
    if (doGetDNs) {
      groupDNs.add(groupResult.getNameInNamespace());
    }
  }

  /* Implementation for walking up the ldap hierarchy
   * This function will iteratively find the super-group memebership of
   *    groups listed in groupDNs and add them to
   * the groups set.  It will walk up the hierarchy goUpHierarchy levels.
   * Note: This is an expensive operation and settings higher than 1
   *    are NOT recommended as they will impact both the speed and
   *    memory usage of all operations.
   * The maximum time for this function will be bounded by the ldap query
   * timeout and the number of ldap queries that it will make, which is
   * max(Recur Depth in LDAP, goUpHierarcy) * DIRECTORY_SEARCH_TIMEOUT
   *
   * @param ctx - The context for contacting the ldap server
   * @param groupDNs - the distinguished name of the groups whose parents we
   *    want to look up
   * @param goUpHierarchy - the number of levels to go up,
   * @param groups - Output variable to store all groups that will be added
  */
  void goUpGroupHierarchy(Set<String> groupDNs,
                          int goUpHierarchy,
                          Set<String> groups)
      throws NamingException {
    if (goUpHierarchy <= 0 || groups.isEmpty()) {
      return;
    }
    DirContext context = getDirContext();
    Set<String> nextLevelGroups = new HashSet<>();
    StringBuilder filter = new StringBuilder();
    filter.append("(&").append(groupSearchFilter).append("(|");
    for (String dn : groupDNs) {
      filter.append("(").append(groupMemberAttr).append("=")
        .append(dn).append(")");
    }
    filter.append("))");
    LOG.debug("Ldap group query string: " + filter.toString());
    NamingEnumeration<SearchResult> groupResults =
        context.search(groupbaseDN,
           filter.toString(),
           SEARCH_CONTROLS);
    while (groupResults.hasMoreElements()) {
      SearchResult groupResult = groupResults.nextElement();
      getGroupNames(groupResult, groups, nextLevelGroups, true);
    }
    goUpGroupHierarchy(nextLevelGroups, goUpHierarchy - 1, groups);
  }

  /**
   * Check whether we should fail over to the next LDAP server.
   * @param attemptsMadeWithSameLdap current number of attempts made
   *                                 with using same LDAP instance
   * @param maxAttemptsBeforeFailover maximum number of attempts
   *                                  before failing over
   * @return true if we should fail over to the next LDAP server
   */
  protected boolean failover(
      int attemptsMadeWithSameLdap, int maxAttemptsBeforeFailover) {
    if (attemptsMadeWithSameLdap >= maxAttemptsBeforeFailover) {
      String previousLdapUrl = currentLdapUrl;
      currentLdapUrl = ldapUrls.next();
      LOG.info("Reached {} attempts on {}, failing over to {}",
          attemptsMadeWithSameLdap, previousLdapUrl, currentLdapUrl);
      return true;
    }
    return false;
  }

  /**
   * Switch to the next available user to bind to.
   * @param e AuthenticationException encountered when contacting LDAP
   */
  protected void switchBindUser(AuthenticationException e) {
    BindUserInfo oldBindUser = this.currentBindUser;
    currentBindUser = this.bindUsers.next();
    if (!oldBindUser.equals(currentBindUser)) {
      LOG.info("Switched from {} to {} after an AuthenticationException: {}",
          oldBindUser, currentBindUser, e.getMessage());
    }
  }

  private DirContext getDirContext() throws NamingException {
    if (ctx == null) {
      // Set up the initial environment for LDAP connectivity
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, ldapCtxFactoryClassName);
      env.put(Context.PROVIDER_URL, currentLdapUrl);
      env.put(Context.SECURITY_AUTHENTICATION, "simple");

      // Set up SSL security, if necessary
      if (useSsl) {
        env.put(Context.SECURITY_PROTOCOL, "ssl");
        // It is necessary to use a custom socket factory rather than setting
        // system properties to configure these options to avoid interfering
        // with other SSL factories throughout the system
        LdapSslSocketFactory.setConfigurations(keystore, keystorePass,
            truststore, truststorePass);
        env.put("java.naming.ldap.factory.socket",
            LdapSslSocketFactory.class.getName());
      }

      env.put(Context.SECURITY_PRINCIPAL, currentBindUser.username);
      env.put(Context.SECURITY_CREDENTIALS, currentBindUser.password);

      env.put("com.sun.jndi.ldap.connect.timeout", conf.get(CONNECTION_TIMEOUT,
          String.valueOf(CONNECTION_TIMEOUT_DEFAULT)));
      env.put("com.sun.jndi.ldap.read.timeout", conf.get(READ_TIMEOUT,
          String.valueOf(READ_TIMEOUT_DEFAULT)));

      // See HADOOP-17675 for details TLDR:
      // From a native thread the thread's context classloader is null.
      // jndi internally in the InitialDirContext specifies the context
      // classloader for Class.forName, and as it is null, jndi will use the
      // bootstrap classloader in this case to laod the socket factory
      // implementation.
      // BUT
      // Bootstrap classloader does not have it in its classpath, so throws a
      // ClassNotFoundException.
      // This affects Impala for example when it uses LdapGroupsMapping.
      ClassLoader currentContextLoader =
          Thread.currentThread().getContextClassLoader();
      if (currentContextLoader == null) {
        try {
          Thread.currentThread().setContextClassLoader(
              this.getClass().getClassLoader());
          ctx = new InitialDirContext(env);
        } finally {
          Thread.currentThread().setContextClassLoader(null);
        }
      } else {
        ctx = new InitialDirContext(env);
      }
    }
    return ctx;
  }
  
  /**
   * Caches groups, no need to do that for this provider
   */
  @Override
  public void cacheGroupsRefresh() {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Adds groups to cache, no need to do that for this provider
   *
   * @param groups unused
   */
  @Override
  public void cacheGroupsAdd(List<String> groups) {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
    String[] urls = conf.getStrings(LDAP_URL_KEY, LDAP_URL_DEFAULT);
    if (urls == null || urls.length == 0) {
      throw new RuntimeException("LDAP URL(s) are not configured");
    }
    ldapUrls = Iterators.cycle(urls);
    currentLdapUrl = ldapUrls.next();

    useSsl = conf.getBoolean(LDAP_USE_SSL_KEY, LDAP_USE_SSL_DEFAULT);
    if (useSsl) {
      loadSslConf(conf);
    }

    initializeBindUsers();

    String baseDN = conf.getTrimmed(BASE_DN_KEY, BASE_DN_DEFAULT);

    // User search base which defaults to base dn.
    userbaseDN = conf.getTrimmed(USER_BASE_DN_KEY, baseDN);
    LOG.debug("Usersearch baseDN: {}", userbaseDN);

    // Group search base which defaults to base dn.
    groupbaseDN = conf.getTrimmed(GROUP_BASE_DN_KEY, baseDN);
    LOG.debug("Groupsearch baseDN: {}", groupbaseDN);

    groupSearchFilter =
        conf.get(GROUP_SEARCH_FILTER_KEY, GROUP_SEARCH_FILTER_DEFAULT);
    userSearchFilter =
        conf.get(USER_SEARCH_FILTER_KEY, USER_SEARCH_FILTER_DEFAULT);
    isPosix = groupSearchFilter.contains(POSIX_GROUP) && userSearchFilter
        .contains(POSIX_ACCOUNT);
    memberOfAttr =
        conf.get(MEMBEROF_ATTR_KEY, MEMBEROF_ATTR_DEFAULT);
    // if memberOf attribute is set, resolve group names from the attribute
    // of user objects.
    useOneQuery = !memberOfAttr.isEmpty();
    groupMemberAttr =
        conf.get(GROUP_MEMBERSHIP_ATTR_KEY, GROUP_MEMBERSHIP_ATTR_DEFAULT);
    groupNameAttr =
        conf.get(GROUP_NAME_ATTR_KEY, GROUP_NAME_ATTR_DEFAULT);
    groupHierarchyLevels =
        conf.getInt(GROUP_HIERARCHY_LEVELS_KEY, GROUP_HIERARCHY_LEVELS_DEFAULT);
    posixUidAttr =
        conf.get(POSIX_UID_ATTR_KEY, POSIX_UID_ATTR_DEFAULT);
    posixGidAttr =
        conf.get(POSIX_GID_ATTR_KEY, POSIX_GID_ATTR_DEFAULT);

    int dirSearchTimeout = conf.getInt(DIRECTORY_SEARCH_TIMEOUT,
        DIRECTORY_SEARCH_TIMEOUT_DEFAULT);
    SEARCH_CONTROLS.setTimeLimit(dirSearchTimeout);
    // Limit the attributes returned to only those required to speed up the search.
    // See HADOOP-10626 and HADOOP-12001 for more details.
    String[] returningAttributes;
    if (useOneQuery) {
      returningAttributes = new String[] {
          groupNameAttr, posixUidAttr, posixGidAttr, memberOfAttr};
    } else {
      returningAttributes = new String[] {
          groupNameAttr, posixUidAttr, posixGidAttr};
    }
    SEARCH_CONTROLS.setReturningAttributes(returningAttributes);

    // LDAP_CTX_FACTORY_CLASS_DEFAULT is not open to unnamed modules
    // in Java 11+, so the default value is set to null to avoid
    // creating the instance for now.
    Class<? extends InitialContextFactory> ldapCtxFactoryClass =
        conf.getClass(LDAP_CTX_FACTORY_CLASS_KEY, null,
        InitialContextFactory.class);
    if (ldapCtxFactoryClass != null) {
      ldapCtxFactoryClassName = ldapCtxFactoryClass.getName();
    } else {
      // The default value is set afterwards.
      ldapCtxFactoryClassName = LDAP_CTX_FACTORY_CLASS_DEFAULT;
    }

    this.numAttempts = conf.getInt(LDAP_NUM_ATTEMPTS_KEY,
        LDAP_NUM_ATTEMPTS_DEFAULT);
    this.numAttemptsBeforeFailover = conf.getInt(
        LDAP_NUM_ATTEMPTS_BEFORE_FAILOVER_KEY,
        LDAP_NUM_ATTEMPTS_BEFORE_FAILOVER_DEFAULT);
  }

  /**
   * Get URLs of configured LDAP servers.
   * @return URLs of LDAP servers being used.
   */
  public Iterator<String> getLdapUrls() {
    return ldapUrls;
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
      Configuration config, String alias, String defaultPass) {
    String password = defaultPass;
    try {
      char[] passchars = config.getPasswordFromCredentialProviders(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    } catch (IOException ioe) {
      LOG.warn("Exception while trying to get password for alias {}: {}",
          alias, ioe);
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
    String password = defaultPass;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    } catch (IOException ioe) {
      LOG.warn("Exception while trying to get password for alias {}:",
          alias, ioe);
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
        Files.newInputStream(Paths.get(pwFile)), StandardCharsets.UTF_8)) {
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

  private void initializeBindUsers() {
    List<BindUserInfo> bindUsersConfigured = new ArrayList<>();

    String[] bindUserAliases = conf.getStrings(BIND_USERS_KEY);
    if (bindUserAliases != null && bindUserAliases.length > 0) {

      for (String bindUserAlias : bindUserAliases) {
        String userConfPrefix = BIND_USERS_KEY + "." + bindUserAlias;
        String bindUsername = conf.get(userConfPrefix + BIND_USER_SUFFIX);
        String bindPassword = getPasswordForBindUser(userConfPrefix);

        if (bindUsername == null || bindPassword == null) {
          throw new RuntimeException("Bind username or password not " +
              "configured for user: " + bindUserAlias);
        }
        bindUsersConfigured.add(new BindUserInfo(bindUsername, bindPassword));
      }
    } else {
      String bindUsername = conf.get(BIND_USER_KEY, BIND_USER_DEFAULT);
      String bindPassword = getPasswordForBindUser(LDAP_CONFIG_PREFIX);
      bindUsersConfigured.add(new BindUserInfo(bindUsername, bindPassword));
    }

    this.bindUsers = Iterators.cycle(bindUsersConfigured);
    this.currentBindUser = this.bindUsers.next();
  }

  private String getPasswordForBindUser(String keyPrefix) {
    String password;
    String alias = conf.get(keyPrefix + BIND_PASSWORD_ALIAS_SUFFIX,
        BIND_PASSWORD_ALIAS_DEFAULT);
    password = getPasswordFromCredentialProviders(conf, alias, "");
    if (password.isEmpty()) {
      password = getPassword(conf, keyPrefix + BIND_PASSWORD_SUFFIX,
          BIND_PASSWORD_DEFAULT);
      if (password.isEmpty()) {
        password = extractPassword(conf.get(
            keyPrefix + BIND_PASSWORD_FILE_SUFFIX, BIND_PASSWORD_FILE_DEFAULT));
      }
    }
    return password;
  }

  private final static class BindUserInfo {
    private final String username;
    private final String password;

    private BindUserInfo(String username, String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof BindUserInfo)) {
        return false;
      }
      return this.username.equals(((BindUserInfo) o).username);
    }

    @Override
    public int hashCode() {
      return this.username.hashCode();
    }

    @Override
    public String toString() {
      return this.username;
    }
  }

  /**
   * An private internal socket factory used to create SSL sockets with custom
   * configuration. There is no way to pass a specific instance of a factory to
   * the Java naming services, and the instantiated socket factory is not
   * passed any contextual information, so all information must be encapsulated
   * directly in the class. Static fields are used here to achieve this. This is
   * safe since the only usage of {@link LdapGroupsMapping} is within
   * {@link Groups}, which is a singleton (see the GROUPS field).
   * <p>
   * This has nearly the same behavior as an {@link SSLSocketFactory}. The only
   * additional logic is to configure the key store and trust store.
   * <p>
   * This is public only to be accessible by the Java naming services.
   */
  @InterfaceAudience.Private
  public static class LdapSslSocketFactory extends SocketFactory {

    /** Cached value lazy-loaded by {@link #getDefault()}. */
    private static LdapSslSocketFactory defaultSslFactory;

    private static String keyStoreLocation;
    private static String keyStorePassword;
    private static String trustStoreLocation;
    private static String trustStorePassword;

    private final SSLSocketFactory socketFactory;

    LdapSslSocketFactory(SSLSocketFactory wrappedSocketFactory) {
      this.socketFactory = wrappedSocketFactory;
    }

    public static synchronized SocketFactory getDefault() {
      if (defaultSslFactory == null) {
        try {
          SSLContext context = SSLContext.getInstance("TLS");
          context.init(createKeyManagers(), createTrustManagers(), null);
          defaultSslFactory =
              new LdapSslSocketFactory(context.getSocketFactory());
          LOG.info("Successfully instantiated LdapSslSocketFactory with "
                  + "keyStoreLocation = {} and trustStoreLocation = {}",
              keyStoreLocation, trustStoreLocation);
        } catch (IOException | GeneralSecurityException e) {
          throw new RuntimeException("Unable to create SSLSocketFactory", e);
        }
      }
      return defaultSslFactory;
    }

    static synchronized void setConfigurations(String newKeyStoreLocation,
        String newKeyStorePassword, String newTrustStoreLocation,
        String newTrustStorePassword) {
      LdapSslSocketFactory.keyStoreLocation = newKeyStoreLocation;
      LdapSslSocketFactory.keyStorePassword = newKeyStorePassword;
      LdapSslSocketFactory.trustStoreLocation = newTrustStoreLocation;
      LdapSslSocketFactory.trustStorePassword = newTrustStorePassword;
    }

    private static KeyManager[] createKeyManagers()
        throws IOException, GeneralSecurityException {
      if (keyStoreLocation.isEmpty()) {
        return null;
      }
      KeyManagerFactory keyMgrFactory = KeyManagerFactory
          .getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyMgrFactory.init(createKeyStore(keyStoreLocation, keyStorePassword),
          getPasswordCharArray(keyStorePassword));
      return keyMgrFactory.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers()
        throws IOException, GeneralSecurityException {
      if (trustStoreLocation.isEmpty()) {
        return null;
      }
      TrustManagerFactory trustMgrFactory = TrustManagerFactory
          .getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustMgrFactory.init(
          createKeyStore(trustStoreLocation, trustStorePassword));
      return trustMgrFactory.getTrustManagers();
    }

    private static KeyStore createKeyStore(String location, String password)
        throws IOException, GeneralSecurityException {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      try (InputStream keyStoreInput = new FileInputStream(location)) {
        keyStore.load(keyStoreInput, getPasswordCharArray(password));
      }
      return keyStore;
    }

    private static char[] getPasswordCharArray(String password) {
      if (password == null || password.isEmpty()) {
        return null;
      }
      return password.toCharArray();
    }

    @Override
    public Socket createSocket() throws IOException {
      return socketFactory.createSocket();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost,
        int localPort) throws IOException {
      return socketFactory.createSocket(host, port, localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
      return socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress address, int port,
        InetAddress localAddress, int localPort) throws IOException {
      return socketFactory.createSocket(address, port, localAddress, localPort);
    }
  }

}
