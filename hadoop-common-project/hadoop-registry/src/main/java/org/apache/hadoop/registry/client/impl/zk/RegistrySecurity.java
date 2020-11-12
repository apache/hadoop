/*
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

package org.apache.hadoop.registry.client.impl.zk;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions.*;
import static org.apache.hadoop.registry.client.api.RegistryConstants.*;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

/**
 * Implement the registry security ... a self contained service for
 * testability.
 * <p>
 * This class contains:
 * <ol>
 *   <li>
 *     The registry security policy implementation, configuration reading, ACL
 * setup and management
 *   </li>
 *   <li>Lots of static helper methods to aid security setup and debugging</li>
 * </ol>
 */

public class RegistrySecurity extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistrySecurity.class);

  public static final String E_UNKNOWN_AUTHENTICATION_MECHANISM =
      "Unknown/unsupported authentication mechanism; ";

  /**
   * there's no default user to add with permissions, so it would be
   * impossible to create nodes with unrestricted user access
   */
  public static final String E_NO_USER_DETERMINED_FOR_ACLS =
      "No user for ACLs determinable from current user or registry option "
      + KEY_REGISTRY_USER_ACCOUNTS;

  /**
   * Error raised when the registry is tagged as secure but this
   * process doesn't have hadoop security enabled.
   */
  public static final String E_NO_KERBEROS =
      "Registry security is enabled -but Hadoop security is not enabled";

  /**
   * Access policy options
   */
  private enum AccessPolicy {
    anon, sasl, digest, simple
  }

  /**
   * Access mechanism
   */
  private AccessPolicy access;

  /**
   * User used for digest auth
   */

  private String digestAuthUser;

  /**
   * Password used for digest auth
   */

  private String digestAuthPassword;

  /**
   * Auth data used for digest auth
   */
  private byte[] digestAuthData;

  /**
   * flag set to true if the registry has security enabled.
   */
  private boolean secureRegistry;

  /**
   * An ACL with read-write access for anyone
   */
  public static final ACL ALL_READWRITE_ACCESS =
      new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE);

  /**
   * An ACL with read access for anyone
   */
  public static final ACL ALL_READ_ACCESS =
      new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE);

  /**
   * An ACL list containing the {@link #ALL_READWRITE_ACCESS} entry.
   * It is copy on write so can be shared without worry
   */
  public static final List<ACL> WorldReadWriteACL;

  static {
    List<ACL> acls = new ArrayList<ACL>();
    acls.add(ALL_READWRITE_ACCESS);
    WorldReadWriteACL = new CopyOnWriteArrayList<ACL>(acls);
  }

  /**
   * the list of system ACLs
   */
  private final List<ACL> systemACLs = new ArrayList<ACL>();

  private boolean usesRealm = true;

  /**
   * A list of digest ACLs which can be added to permissions
   * —and cleared later.
   */
  private final List<ACL> digestACLs = new ArrayList<ACL>();

  /**
   * the default kerberos realm
   */
  private String kerberosRealm;

  /**
   * Client context
   */
  private String jaasClientEntry;

  /**
   * Client identity
   */
  private String jaasClientIdentity;

  private String principal;

  private String keytab;

  /**
   * Create an instance
   * @param name service name
   */
  public RegistrySecurity(String name) {
    super(name);
  }

  /**
   * Init the service: this sets up security based on the configuration
   * @param conf configuration
   * @throws Exception
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String auth = conf.getTrimmed(KEY_REGISTRY_CLIENT_AUTH,
        REGISTRY_CLIENT_AUTH_ANONYMOUS);

    switch (auth) {
    case REGISTRY_CLIENT_AUTH_KERBEROS:
      access = AccessPolicy.sasl;
      break;
    case REGISTRY_CLIENT_AUTH_DIGEST:
      access = AccessPolicy.digest;
      break;
    case REGISTRY_CLIENT_AUTH_ANONYMOUS:
      access = AccessPolicy.anon;
      break;
    case REGISTRY_CLIENT_AUTH_SIMPLE:
      access = AccessPolicy.simple;
      break;
    default:
      throw new ServiceStateException(E_UNKNOWN_AUTHENTICATION_MECHANISM
                                      + "\"" + auth + "\"");
    }
    initSecurity();
  }

  /**
   * Init security.
   *
   * After this operation, the {@link #systemACLs} list is valid.
   * @throws IOException
   */
  private void initSecurity() throws IOException {

    secureRegistry =
        getConfig().getBoolean(KEY_REGISTRY_SECURE, DEFAULT_REGISTRY_SECURE);
    systemACLs.clear();
    if (secureRegistry) {
      addSystemACL(ALL_READ_ACCESS);

      // determine the kerberos realm from JVM and settings
      kerberosRealm = getConfig().get(KEY_REGISTRY_KERBEROS_REALM,
          getDefaultRealmInJVM());

      // System Accounts
      String system = getOrFail(KEY_REGISTRY_SYSTEM_ACCOUNTS,
                                DEFAULT_REGISTRY_SYSTEM_ACCOUNTS);
      usesRealm = system.contains("@");

      systemACLs.addAll(buildACLs(system, kerberosRealm, ZooDefs.Perms.ALL));

      LOG.info("Registry default system acls: " + System.lineSeparator() +
          systemACLs);
      // user accounts (may be empty, but for digest one user AC must
      // be built up
      String user = getConfig().get(KEY_REGISTRY_USER_ACCOUNTS,
                              DEFAULT_REGISTRY_USER_ACCOUNTS);
      List<ACL> userACLs = buildACLs(user, kerberosRealm, ZooDefs.Perms.ALL);

      // add self if the current user can be determined
      ACL self;
      if (UserGroupInformation.isSecurityEnabled()) {
        self = createSaslACLFromCurrentUser(ZooDefs.Perms.ALL);
        if (self != null) {
          userACLs.add(self);
        }
      }
      LOG.info("Registry User ACLs " + System.lineSeparator()+ userACLs);

      // here check for UGI having secure on or digest + ID
      switch (access) {
        case sasl:
          // secure + SASL => has to be authenticated
          if (!UserGroupInformation.isSecurityEnabled()) {
            throw new IOException("Kerberos required for secure registry access");
          }
          UserGroupInformation currentUser =
              UserGroupInformation.getCurrentUser();
          jaasClientEntry = getOrFail(KEY_REGISTRY_CLIENT_JAAS_CONTEXT,
              DEFAULT_REGISTRY_CLIENT_JAAS_CONTEXT);
          jaasClientIdentity = currentUser.getShortUserName();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Auth is SASL user=\"{}\" JAAS context=\"{}\"",
                jaasClientIdentity, jaasClientEntry);
          }
          break;

        case digest:
          String id = getOrFail(KEY_REGISTRY_CLIENT_AUTHENTICATION_ID, "");
          String pass = getOrFail(KEY_REGISTRY_CLIENT_AUTHENTICATION_PASSWORD, "");
          if (userACLs.isEmpty()) {
            //
            throw new ServiceStateException(E_NO_USER_DETERMINED_FOR_ACLS);
          }
          digest(id, pass);
          ACL acl = new ACL(ZooDefs.Perms.ALL, toDigestId(id, pass));
          userACLs.add(acl);
          digestAuthUser = id;
          digestAuthPassword = pass;
          String authPair = id + ":" + pass;
          digestAuthData = authPair.getBytes("UTF-8");
          if (LOG.isDebugEnabled()) {
            LOG.debug("Auth is Digest ACL: {}", aclToString(acl));
          }
          break;

        case anon:
        case simple:
          // nothing is needed; account is read only.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Auth is anonymous");
          }
          userACLs = new ArrayList<ACL>(0);
          break;
      }
      systemACLs.addAll(userACLs);

    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Registry has no security");
      }
      // wide open cluster, adding system acls
      systemACLs.addAll(WorldReadWriteACL);
    }
  }

  /**
   * Add another system ACL
   * @param acl add ACL
   */
  public void addSystemACL(ACL acl) {
    systemACLs.add(acl);
  }

  /**
   * Add a digest ACL
   * @param acl add ACL
   */
  public boolean addDigestACL(ACL acl) {
    if (secureRegistry) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added ACL {}", aclToString(acl));
      }
      digestACLs.add(acl);
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring added ACL - registry is insecure{}",
            aclToString(acl));
      }
      return false;
    }
  }

  /**
   * Reset the digest ACL list
   */
  public void resetDigestACLs() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleared digest ACLs");
    }
    digestACLs.clear();
  }

  /**
   * Flag to indicate the cluster is secure
   * @return true if the config enabled security
   */
  public boolean isSecureRegistry() {
    return secureRegistry;
  }

  /**
   * Get the system principals
   * @return the system principals
   */
  public List<ACL> getSystemACLs() {
    Preconditions.checkNotNull(systemACLs, "registry security is uninitialized");
    return Collections.unmodifiableList(systemACLs);
  }

  /**
   * Get all ACLs needed for a client to use when writing to the repo.
   * That is: system ACLs, its own ACL, any digest ACLs
   * @return the client ACLs
   */
  public List<ACL> getClientACLs() {
    List<ACL> clientACLs = new ArrayList<ACL>(systemACLs);
    clientACLs.addAll(digestACLs);
    return clientACLs;
  }

  /**
   * Create a SASL ACL for the user
   * @param perms permissions
   * @return an ACL for the current user or null if they aren't a kerberos user
   * @throws IOException
   */
  public ACL createSaslACLFromCurrentUser(int perms) throws IOException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    if (currentUser.hasKerberosCredentials()) {
      return createSaslACL(currentUser, perms);
    } else {
      return null;
    }
  }

  /**
   * Given a UGI, create a SASL ACL from it
   * @param ugi UGI
   * @param perms permissions
   * @return a new ACL
   */
  public ACL createSaslACL(UserGroupInformation ugi, int perms) {
    String userName = null;
    if (usesRealm) {
      userName = ugi.getUserName();
    } else {
      userName = ugi.getShortUserName();
    }
    return new ACL(perms, new Id(SCHEME_SASL, userName));
  }

  /**
   * Get a conf option, throw an exception if it is null/empty
   * @param key key
   * @param defval default value
   * @return the value
   * @throws IOException if missing
   */
  private String getOrFail(String key, String defval) throws IOException {
    String val = getConfig().get(key, defval);
    if (StringUtils.isEmpty(val)) {
      throw new IOException("Missing value for configuration option " + key);
    }
    return val;
  }

  /**
   * Check for an id:password tuple being valid.
   * This test is stricter than that in {@link DigestAuthenticationProvider},
   * which splits the string, but doesn't check the contents of each
   * half for being non-"".
   * @param idPasswordPair id:pass pair
   * @return true if the pass is considered valid.
   */
  public boolean isValid(String idPasswordPair) {
    String[] parts = idPasswordPair.split(":");
    return parts.length == 2
           && !StringUtils.isEmpty(parts[0])
           && !StringUtils.isEmpty(parts[1]);
  }

  /**
   * Get the derived kerberos realm.
   * @return this is built from the JVM realm, or the configuration if it
   * overrides it. If "", it means "don't know".
   */
  public String getKerberosRealm() {
    return kerberosRealm;
  }

  /**
   * Generate a base-64 encoded digest of the idPasswordPair pair
   * @param idPasswordPair id:password
   * @return a string that can be used for authentication
   */
  public String digest(String idPasswordPair) throws IOException {
    if (StringUtils.isEmpty(idPasswordPair) || !isValid(idPasswordPair)) {
      throw new IOException("Invalid id:password");
    }
    try {
      return DigestAuthenticationProvider.generateDigest(idPasswordPair);
    } catch (NoSuchAlgorithmException e) {
      // unlikely since it is standard to the JVM, but maybe JCE restrictions
      // could trigger it
      throw new IOException(e.toString(), e);
    }
  }

  /**
   * Generate a base-64 encoded digest of the idPasswordPair pair
   * @param id ID
   * @param password pass
   * @return a string that can be used for authentication
   * @throws IOException
   */
  public String digest(String id, String password) throws IOException {
    return digest(id + ":" + password);
  }

  /**
   * Given a digest, create an ID from it
   * @param digest digest
   * @return ID
   */
  public Id toDigestId(String digest) {
    return new Id(SCHEME_DIGEST, digest);
  }

  /**
   * Create a Digest ID from an id:pass pair
   * @param id ID
   * @param password password
   * @return an ID
   * @throws IOException
   */
  public Id toDigestId(String id, String password) throws IOException {
    return toDigestId(digest(id, password));
  }

  /**
   * Split up a list of the form
   * <code>sasl:mapred@,digest:5f55d66, sasl@yarn@EXAMPLE.COM</code>
   * into a list of possible ACL values, trimming as needed
   *
   * The supplied realm is added to entries where
   * <ol>
   *   <li>the string begins "sasl:"</li>
   *   <li>the string ends with "@"</li>
   * </ol>
   * No attempt is made to validate any of the acl patterns.
   *
   * @param aclString list of 0 or more ACLs
   * @param realm realm to add
   * @return a list of split and potentially patched ACL pairs.
   *
   */
  public List<String> splitAclPairs(String aclString, String realm) {
    List<String> list = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
                .split(aclString));
    ListIterator<String> listIterator = list.listIterator();
    while (listIterator.hasNext()) {
      String next = listIterator.next();
      if (next.startsWith(SCHEME_SASL +":") && next.endsWith("@")) {
        listIterator.set(next + realm);
      }
    }
    return list;
  }

  /**
   * Parse a string down to an ID, adding a realm if needed
   * @param idPair id:data tuple
   * @param realm realm to add
   * @return the ID.
   * @throws IllegalArgumentException if the idPair is invalid
   */
  public Id parse(String idPair, String realm) {
    int firstColon = idPair.indexOf(':');
    int lastColon = idPair.lastIndexOf(':');
    if (firstColon == -1 || lastColon == -1 || firstColon != lastColon) {
      throw new IllegalArgumentException(
          "ACL '" + idPair + "' not of expected form scheme:id");
    }
    String scheme = idPair.substring(0, firstColon);
    String id = idPair.substring(firstColon + 1);
    if (id.endsWith("@")) {
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(realm),
          "@ suffixed account but no realm %s", id);
      id = id + realm;
    }
    return new Id(scheme, id);
  }

  /**
   * Parse the IDs, adding a realm if needed, setting the permissions
   * @param principalList id string
   * @param realm realm to add
   * @param perms permissions
   * @return the relevant ACLs
   * @throws IOException
   */
  public List<ACL> buildACLs(String principalList, String realm, int perms)
      throws IOException {
    List<String> aclPairs = splitAclPairs(principalList, realm);
    List<ACL> ids = new ArrayList<ACL>(aclPairs.size());
    for (String aclPair : aclPairs) {
      ACL newAcl = new ACL();
      newAcl.setId(parse(aclPair, realm));
      newAcl.setPerms(perms);
      ids.add(newAcl);
    }
    return ids;
  }

  /**
   * Parse an ACL list. This includes configuration indirection
   * {@link ZKUtil#resolveConfIndirection(String)}
   * @param zkAclConf configuration string
   * @return an ACL list
   * @throws IOException on a bad ACL parse
   */
  public List<ACL> parseACLs(String zkAclConf) throws IOException {
    try {
      return ZKUtil.parseACLs(ZKUtil.resolveConfIndirection(zkAclConf));
    } catch (ZKUtil.BadAclFormatException e) {
      throw new IOException("Parsing " + zkAclConf + " :" + e, e);
    }
  }

  /**
   * Get the appropriate Kerberos Auth module for JAAS entries
   * for this JVM.
   * @return a JVM-specific kerberos login module classname.
   */
  public static String getKerberosAuthModuleForJVM() {
    if (System.getProperty("java.vendor").contains("IBM")) {
      return "com.ibm.security.auth.module.Krb5LoginModule";
    } else {
      return "com.sun.security.auth.module.Krb5LoginModule";
    }
  }

  /**
   * JAAS template: {@value}
   * Note the semicolon on the last entry
   */
  private static final String JAAS_ENTRY =
      (IBM_JAVA ?
      "%s { %n"
      + " %s required%n"
      + " useKeytab=\"%s\"%n"
      + " debug=true%n"
      + " principal=\"%s\"%n"
      + " credsType=both%n"
      + " refreshKrb5Config=true;%n"
      + "}; %n"
      :
      "%s { %n"
      + " %s required%n"
      // kerberos module
      + " keyTab=\"%s\"%n"
      + " debug=true%n"
      + " principal=\"%s\"%n"
      + " useKeyTab=true%n"
      + " useTicketCache=false%n"
      + " doNotPrompt=true%n"
      + " storeKey=true;%n"
      + "}; %n"
     );

  /**
   * Create a JAAS entry for insertion
   * @param context context of the entry
   * @param principal kerberos principal
   * @param keytab keytab
   * @return a context
   */
  public String createJAASEntry(
      String context,
      String principal,
      File keytab) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(principal),
        "invalid principal");
    Preconditions.checkArgument(StringUtils.isNotEmpty(context),
        "invalid context");
    Preconditions.checkArgument(keytab != null && keytab.isFile(),
        "Keytab null or missing: ");
    String keytabpath = keytab.getAbsolutePath();
    // fix up for windows; no-op on unix
    keytabpath =  keytabpath.replace('\\', '/');
    return String.format(
        Locale.ENGLISH,
        JAAS_ENTRY,
        context,
        getKerberosAuthModuleForJVM(),
        keytabpath,
        principal);
  }

  /**
   * Bind the JVM JAS setting to the specified JAAS file.
   *
   * <b>Important:</b> once a file has been loaded the JVM doesn't pick up
   * changes
   * @param jaasFile the JAAS file
   */
  public static void bindJVMtoJAASFile(File jaasFile) {
    String path = jaasFile.getAbsolutePath();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Binding {} to {}", Environment.JAAS_CONF_KEY, path);
    }
    System.setProperty(Environment.JAAS_CONF_KEY, path);
  }

  /**
   * Set the Zookeeper server property
   * {@link ZookeeperConfigOptions#PROP_ZK_SERVER_SASL_CONTEXT}
   * to the SASL context. When the ZK server starts, this is the context
   * which it will read in
   * @param contextName the name of the context
   */
  public static void bindZKToServerJAASContext(String contextName) {
    System.setProperty(PROP_ZK_SERVER_SASL_CONTEXT, contextName);
  }

  /**
   * Reset any system properties related to JAAS
   */
  public static void clearJaasSystemProperties() {
    System.clearProperty(Environment.JAAS_CONF_KEY);
  }

  /**
   * Resolve the context of an entry. This is an effective test of
   * JAAS setup, because it will relay detected problems up
   * @param context context name
   * @return the entry
   * @throws RuntimeException if there is no context entry found
   */
  public static AppConfigurationEntry[] validateContext(String context)  {
    if (context == null) {
      throw new RuntimeException("Null context argument");
    }
    if (context.isEmpty()) {
      throw new RuntimeException("Empty context argument");
    }
    javax.security.auth.login.Configuration configuration =
        javax.security.auth.login.Configuration.getConfiguration();
    AppConfigurationEntry[] entries =
        configuration.getAppConfigurationEntry(context);
    if (entries == null) {
      throw new RuntimeException(
          String.format("Entry \"%s\" not found; " +
                        "JAAS config = %s",
              context,
              describeProperty(Environment.JAAS_CONF_KEY) ));
    }
    return entries;
  }

  /**
   * Apply the security environment to this curator instance. This
   * may include setting up the ZK system properties for SASL
   * @param builder curator builder
   * @throws IOException if jaas configuration can't be generated or found
   */
  public void applySecurityEnvironment(CuratorFrameworkFactory.Builder
      builder) throws IOException {

    if (isSecureRegistry()) {
      switch (access) {
        case anon:
          clearZKSaslClientProperties();
          break;

        case digest:
          // no SASL
          clearZKSaslClientProperties();
          builder.authorization(SCHEME_DIGEST, digestAuthData);
          break;

        case sasl:
          String existingJaasConf = System.getProperty(
              "java.security.auth.login.config");
          if (existingJaasConf == null || existingJaasConf.isEmpty()) {
            if (principal == null || keytab == null) {
              throw new IOException("SASL is configured for registry, " +
                  "but neither keytab/principal nor java.security.auth.login" +
                  ".config system property are specified");
            }
            // in this case, keytab and principal are specified and no jaas
            // config is specified, so we will create one
            LOG.info(
                "Enabling ZK sasl client: jaasClientEntry = " + jaasClientEntry
                    + ", principal = " + principal + ", keytab = " + keytab);
            JaasConfiguration jconf =
                new JaasConfiguration(jaasClientEntry, principal, keytab);
            javax.security.auth.login.Configuration.setConfiguration(jconf);
            setSystemPropertyIfUnset(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
                                     "true");
            setSystemPropertyIfUnset(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                                     jaasClientEntry);
          } else {
            // in this case, jaas config is specified so we will not change it
            LOG.info("Using existing ZK sasl configuration: " +
              "jaasClientEntry = " + System.getProperty(
              ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, "Client") +
              ", sasl client = " + System.getProperty(
              ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
              ZKClientConfig.ENABLE_CLIENT_SASL_DEFAULT) +
              ", jaas = " + existingJaasConf);
          }
          break;

        default:
          clearZKSaslClientProperties();
          break;
      }
    }
  }

  public void setKerberosPrincipalAndKeytab(String principal, String keytab) {
    this.principal = principal;
    this.keytab = keytab;
  }

  /**
   * Creates a programmatic version of a jaas.conf file. This can be used
   * instead of writing a jaas.conf file and setting the system property,
   * "java.security.auth.login.config", to point to that file. It is meant to be
   * used for connecting to ZooKeeper.
   */
  @InterfaceAudience.Private
  public static class JaasConfiguration extends
      javax.security.auth.login.Configuration {

    private final javax.security.auth.login.Configuration baseConfig =
        javax.security.auth.login.Configuration.getConfiguration();
    private static AppConfigurationEntry[] entry;
    private String entryName;

    /**
     * Add an entry to the jaas configuration with the passed in name,
     * principal, and keytab. The other necessary options will be set for you.
     *
     * @param entryName The name of the entry (e.g. "Client")
     * @param principal The principal of the user
     * @param keytab The location of the keytab
     */
    public JaasConfiguration(String entryName, String principal, String keytab) {
      this.entryName = entryName;
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("useTicketCache", "false");
      options.put("refreshKrb5Config", "true");
      String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
      if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
        options.put("debug", "true");
      }
      entry = new AppConfigurationEntry[]{
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      return (entryName.equals(name)) ? entry : ((baseConfig != null)
          ? baseConfig.getAppConfigurationEntry(name) : null);
    }

    private String getKrb5LoginModuleName() {
      String krb5LoginModuleName;
      if (System.getProperty("java.vendor").contains("IBM")) {
        krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
      } else {
        krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
      }
      return krb5LoginModuleName;
    }
  }

  /**
   * Set the client properties. This forces the ZK client into
   * failing if it can't auth.
   * <b>Important:</b>This is JVM-wide.
   * @param username username
   * @param context login context
   * @throws RuntimeException if the context cannot be found in the current
   * JAAS context
   */
  public static void setZKSaslClientProperties(String username,
      String context)  {
    RegistrySecurity.validateContext(context);
    enableZookeeperClientSASL();
    setSystemPropertyIfUnset(PROP_ZK_SASL_CLIENT_USERNAME, username);
    setSystemPropertyIfUnset(PROP_ZK_SASL_CLIENT_CONTEXT, context);
  }

  private static void setSystemPropertyIfUnset(String name, String value) {
    String existingValue = System.getProperty(name);
    if (existingValue == null || existingValue.isEmpty()) {
      System.setProperty(name, value);
    }
  }

  /**
   * Clear all the ZK SASL Client properties
   * <b>Important:</b>This is JVM-wide
   */
  public static void clearZKSaslClientProperties() {
    disableZookeeperClientSASL();
    System.clearProperty(PROP_ZK_SASL_CLIENT_CONTEXT);
    System.clearProperty(PROP_ZK_SASL_CLIENT_USERNAME);
  }

  /**
   * Turn ZK SASL on
   * <b>Important:</b>This is JVM-wide
   */
  protected static void enableZookeeperClientSASL() {
    System.setProperty(PROP_ZK_ENABLE_SASL_CLIENT, "true");
  }

  /**
   * Force disable ZK SASL bindings.
   * <b>Important:</b>This is JVM-wide
   */
  public static void disableZookeeperClientSASL() {
    System.setProperty(ZookeeperConfigOptions.PROP_ZK_ENABLE_SASL_CLIENT, "false");
  }

  /**
   * Is the system property enabling the SASL client set?
   * @return true if the SASL client system property is set.
   */
  public static boolean isClientSASLEnabled() {
    return Boolean.parseBoolean(System.getProperty(
        ZookeeperConfigOptions.PROP_ZK_ENABLE_SASL_CLIENT, "true"));
  }

  /**
   * Log details about the current Hadoop user at INFO.
   * Robust against IOEs when trying to get the current user
   */
  public void logCurrentHadoopUser() {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      LOG.info("Current user = {}",currentUser);
      UserGroupInformation realUser = currentUser.getRealUser();
      LOG.info("Real User = {}" , realUser);
    } catch (IOException e) {
      LOG.warn("Failed to get current user, {}", e);
    }
  }

  /**
   * Stringify a list of ACLs for logging. Digest ACLs have their
   * digest values stripped for security.
   * @param acls ACL list
   * @return a string for logs, exceptions, ...
   */
  public static String aclsToString(List<ACL> acls) {
    StringBuilder builder = new StringBuilder();
    if (acls == null) {
      builder.append("null ACL");
    } else {
      builder.append('\n');
      for (ACL acl : acls) {
        builder.append(aclToString(acl))
               .append(" ");
      }
    }
    return builder.toString();
  }

  /**
   * Convert an ACL to a string, with any obfuscation needed
   * @param acl ACL
   * @return ACL string value
   */
  public static String aclToString(ACL acl) {
    return String.format(Locale.ENGLISH,
        "0x%02x: %s",
        acl.getPerms(),
        idToString(acl.getId())
    );
  }

  /**
   * Convert an ID to a string, stripping out all but the first few characters
   * of any digest auth hash for security reasons
   * @param id ID
   * @return a string description of a Zookeeper ID
   */
  public static String idToString(Id id) {
    String s;
    if (id.getScheme().equals(SCHEME_DIGEST)) {
      String ids = id.getId();
      int colon = ids.indexOf(':');
      if (colon > 0) {
        ids = ids.substring(colon + 3);
      }
      s = SCHEME_DIGEST + ": " + ids;
    } else {
      s = id.toString();
    }
    return s;
  }

  /**
   * Build up low-level security diagnostics to aid debugging
   * @return a string to use in diagnostics
   */
  public String buildSecurityDiagnostics() {
    StringBuilder builder = new StringBuilder();
    builder.append(secureRegistry ? "secure registry; "
                          : "insecure registry; ");
    builder.append("Curator service access policy: ").append(access);

    builder.append("; System ACLs: ").append(aclsToString(systemACLs));
    builder.append("User: ").append(UgiInfo.fromCurrentUser());
    builder.append("; Kerberos Realm: ").append(kerberosRealm);
    builder.append(describeProperty(Environment.JAAS_CONF_KEY));
    String sasl =
        System.getProperty(PROP_ZK_ENABLE_SASL_CLIENT,
            DEFAULT_ZK_ENABLE_SASL_CLIENT);
    boolean saslEnabled = Boolean.parseBoolean(sasl);
    builder.append(describeProperty(PROP_ZK_ENABLE_SASL_CLIENT,
        DEFAULT_ZK_ENABLE_SASL_CLIENT));
    if (saslEnabled) {
      builder.append("; JAAS Client Identity")
             .append("=")
             .append(jaasClientIdentity)
             .append("; ");
      builder.append(KEY_REGISTRY_CLIENT_JAAS_CONTEXT)
             .append("=")
             .append(jaasClientEntry)
             .append("; ");
      builder.append(describeProperty(PROP_ZK_SASL_CLIENT_USERNAME));
      builder.append(describeProperty(PROP_ZK_SASL_CLIENT_CONTEXT));
    }
    builder.append(describeProperty(PROP_ZK_ALLOW_FAILED_SASL_CLIENTS,
        "(undefined but defaults to true)"));
    builder.append(describeProperty(
        PROP_ZK_SERVER_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE));
    return builder.toString();
  }

  private static String describeProperty(String name) {
    return describeProperty(name, "(undefined)");
  }

  private static String describeProperty(String name, String def) {
    return "; " + name + "=" + System.getProperty(name, def);
  }

  /**
   * Get the default kerberos realm —returning "" if there
   * is no realm or other problem
   * @return the default realm of the system if it
   * could be determined
   */
  public static String getDefaultRealmInJVM() {
    String realm = KerberosUtil.getDefaultRealmProtected();
    if (realm == null) {
      realm = "";
    }
    return realm;
  }

  /**
   * Create an ACL For a user.
   * @param ugi User identity
   * @return the ACL For the specified user. Ifthe username doesn't end
   * in "@" then the realm is added
   */
  public ACL createACLForUser(UserGroupInformation ugi, int perms) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating ACL For ", new UgiInfo(ugi));
    }
    if (!secureRegistry) {
      return ALL_READWRITE_ACCESS;
    } else {
      return createACLfromUsername(ugi.getUserName(), perms);
    }
  }

  /**
   * Given a user name (short or long), create a SASL ACL
   * @param username user name; if it doesn't contain an "@" symbol, the
   * service's kerberos realm is added
   * @param perms permissions
   * @return an ACL for the user
   */
  public ACL createACLfromUsername(String username, int perms) {
    if (usesRealm && !username.contains("@")) {
      username = username + "@" + kerberosRealm;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Appending kerberos realm to make {}", username);
      }
    }
    return new ACL(perms, new Id(SCHEME_SASL, username));
  }

  /**
   * On demand string-ifier for UGI with extra details
   */
  public static class UgiInfo {

    public static UgiInfo fromCurrentUser() {
      try {
        return new UgiInfo(UserGroupInformation.getCurrentUser());
      } catch (IOException e) {
        LOG.info("Failed to get current user {}", e, e);
        return new UgiInfo(null);
      }
    }

    private final UserGroupInformation ugi;

    public UgiInfo(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    @Override
    public String toString() {
      if (ugi==null) {
        return "(null ugi)";
      }
      StringBuilder builder = new StringBuilder();
      builder.append(ugi.getUserName()).append(": ");
      builder.append(ugi.toString());
      builder.append(" hasKerberosCredentials=").append(
          ugi.hasKerberosCredentials());
      builder.append(" isFromKeytab=").append(ugi.isFromKeytab());
      builder.append(" kerberos is enabled in Hadoop =").append(UserGroupInformation.isSecurityEnabled());
      return builder.toString();
    }

  }

  /**
   * on-demand stringifier for a list of ACLs
   */
  public static class AclListInfo {
    public final List<ACL> acls;

    public AclListInfo(List<ACL> acls) {
      this.acls = acls;
    }

    @Override
    public String toString() {
      return aclsToString(acls);
    }
  }
}
