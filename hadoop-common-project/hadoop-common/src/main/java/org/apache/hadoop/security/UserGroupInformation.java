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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_TOKEN_FILES;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_TOKENS;
import static org.apache.hadoop.security.UGIExceptionMessages.*;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;
import static org.apache.hadoop.util.StringUtils.getTrimmedStringCollection;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration.Parameters;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User and group information for Hadoop.
 * This class wraps around a JAAS Subject and provides methods to determine the
 * user's username and groups. It supports both the Windows, Unix and Kerberos 
 * login modules.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UserGroupInformation {
  @VisibleForTesting
  static final Logger LOG = LoggerFactory.getLogger(
      UserGroupInformation.class);

  /**
   * Percentage of the ticket window to use before we renew ticket.
   */
  private static final float TICKET_RENEW_WINDOW = 0.80f;
  private static boolean shouldRenewImmediatelyForTests = false;
  static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  static final String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";

  /**
   * For the purposes of unit tests, we want to test login
   * from keytab and don't want to wait until the renew
   * window (controlled by TICKET_RENEW_WINDOW).
   * @param immediate true if we should login without waiting for ticket window
   */
  @VisibleForTesting
  public static void setShouldRenewImmediatelyForTests(boolean immediate) {
    shouldRenewImmediatelyForTests = immediate;
  }

  /** 
   * UgiMetrics maintains UGI activity statistics
   * and publishes them through the metrics interfaces.
   */
  @Metrics(about="User and group related metrics", context="ugi")
  static class UgiMetrics {
    final MetricsRegistry registry = new MetricsRegistry("UgiMetrics");

    @Metric("Rate of successful kerberos logins and latency (milliseconds)")
    MutableRate loginSuccess;
    @Metric("Rate of failed kerberos logins and latency (milliseconds)")
    MutableRate loginFailure;
    @Metric("GetGroups") MutableRate getGroups;
    MutableQuantiles[] getGroupsQuantiles;
    @Metric("Renewal failures since startup")
    private MutableGaugeLong renewalFailuresTotal;
    @Metric("Renewal failures since last successful login")
    private MutableGaugeInt renewalFailures;

    static UgiMetrics create() {
      return DefaultMetricsSystem.instance().register(new UgiMetrics());
    }

    static void reattach() {
      metrics = UgiMetrics.create();
    }

    void addGetGroups(long latency) {
      getGroups.add(latency);
      if (getGroupsQuantiles != null) {
        for (MutableQuantiles q : getGroupsQuantiles) {
          q.add(latency);
        }
      }
    }

    MutableGaugeInt getRenewalFailures() {
      return renewalFailures;
    }
  }
  
  /**
   * A login module that looks at the Kerberos, Unix, or Windows principal and
   * adds the corresponding UserName.
   */
  @InterfaceAudience.Private
  public static class HadoopLoginModule implements LoginModule {
    private Subject subject;

    @Override
    public boolean abort() throws LoginException {
      return true;
    }

    private <T extends Principal> T getCanonicalUser(Class<T> cls) {
      for(T user: subject.getPrincipals(cls)) {
        return user;
      }
      return null;
    }

    @Override
    public boolean commit() throws LoginException {
      LOG.debug("hadoop login commit");
      // if we already have a user, we are done.
      if (!subject.getPrincipals(User.class).isEmpty()) {
        LOG.debug("Using existing subject: {}", subject.getPrincipals());
        return true;
      }
      Principal user = getCanonicalUser(KerberosPrincipal.class);
      if (user != null) {
        LOG.debug("Using kerberos user: {}", user);
      }
      //If we don't have a kerberos user and security is disabled, check
      //if user is specified in the environment or properties
      if (!isSecurityEnabled() && (user == null)) {
        String envUser = System.getenv(HADOOP_USER_NAME);
        if (envUser == null) {
          envUser = System.getProperty(HADOOP_USER_NAME);
        }
        user = envUser == null ? null : new User(envUser);
      }
      // use the OS user
      if (user == null) {
        user = getCanonicalUser(OS_PRINCIPAL_CLASS);
        LOG.debug("Using local user: {}", user);
      }
      // if we found the user, add our principal
      if (user != null) {
        LOG.debug("Using user: \"{}\" with name: {}", user, user.getName());

        User userEntry = null;
        try {
          // LoginContext will be attached later unless it's an external
          // subject.
          AuthenticationMethod authMethod = (user instanceof KerberosPrincipal)
            ? AuthenticationMethod.KERBEROS : AuthenticationMethod.SIMPLE;
          userEntry = new User(user.getName(), authMethod, null);
        } catch (Exception e) {
          throw (LoginException)(new LoginException(e.toString()).initCause(e));
        }
        LOG.debug("User entry: \"{}\"", userEntry);

        subject.getPrincipals().add(userEntry);
        return true;
      }
      throw new LoginException("Failed to find user in name " + subject);
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
      this.subject = subject;
    }

    @Override
    public boolean login() throws LoginException {
      LOG.debug("Hadoop login");
      return true;
    }

    @Override
    public boolean logout() throws LoginException {
      LOG.debug("Hadoop logout");
      return true;
    }
  }

  /**
   * Reattach the class's metrics to a new metric system.
   */
  public static void reattachMetrics() {
    UgiMetrics.reattach();
  }

  /** Metrics to track UGI activity */
  static UgiMetrics metrics = UgiMetrics.create();
  /** The auth method to use */
  private static AuthenticationMethod authenticationMethod;
  /** Server-side groups fetching service */
  private static Groups groups;
  /** Min time (in seconds) before relogin for Kerberos */
  private static long kerberosMinSecondsBeforeRelogin;
  /** Boolean flag to enable auto-renewal for keytab based loging. */
  private static boolean kerberosKeyTabLoginRenewalEnabled;
  /** A reference to Kerberos login auto renewal thread. */
  private static Optional<ExecutorService> kerberosLoginRenewalExecutor =
          Optional.empty();
  /** The configuration to use */

  private static Configuration conf;

  
  /**Environment variable pointing to the token cache file*/
  public static final String HADOOP_TOKEN_FILE_LOCATION = 
      "HADOOP_TOKEN_FILE_LOCATION";
  /** Environment variable pointing to the base64 tokens. */
  public static final String HADOOP_TOKEN = "HADOOP_TOKEN";
  
  public static boolean isInitialized() {
    return conf != null;
  }

  /** 
   * A method to initialize the fields that depend on a configuration.
   * Must be called before useKerberos or groups is used.
   */
  private static void ensureInitialized() {
    if (!isInitialized()) {
      synchronized(UserGroupInformation.class) {
        if (!isInitialized()) { // someone might have beat us
          initialize(new Configuration(), false);
        }
      }
    }
  }

  /**
   * Initialize UGI and related classes.
   * @param conf the configuration to use
   */
  private static synchronized void initialize(Configuration conf,
                                              boolean overrideNameRules) {
    authenticationMethod = SecurityUtil.getAuthenticationMethod(conf);
    if (overrideNameRules || !HadoopKerberosName.hasRulesBeenSet()) {
      try {
        HadoopKerberosName.setConfiguration(conf);
      } catch (IOException ioe) {
        throw new RuntimeException(
            "Problem with Kerberos auth_to_local name configuration", ioe);
      }
    }
    try {
        kerberosMinSecondsBeforeRelogin = 1000L * conf.getLong(
                HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN,
                HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN_DEFAULT);
    }
    catch(NumberFormatException nfe) {
        throw new IllegalArgumentException("Invalid attribute value for " +
                HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN + " of " +
                conf.get(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN));
    }

    kerberosKeyTabLoginRenewalEnabled = conf.getBoolean(
            HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED,
            HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED_DEFAULT);

    // If we haven't set up testing groups, use the configuration to find it
    if (!(groups instanceof TestingGroups)) {
      groups = Groups.getUserToGroupsMappingService(conf);
    }
    UserGroupInformation.conf = conf;

    if (metrics.getGroupsQuantiles == null) {
      int[] intervals = conf.getInts(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS);
      if (intervals != null && intervals.length > 0) {
        final int length = intervals.length;
        MutableQuantiles[] getGroupsQuantiles = new MutableQuantiles[length];
        for (int i = 0; i < length; i++) {
          getGroupsQuantiles[i] = metrics.registry.newQuantiles(
            "getGroups" + intervals[i] + "s",
            "Get groups", "ops", "latency", intervals[i]);
        }
        metrics.getGroupsQuantiles = getGroupsQuantiles;
      }
    }
  }

  /**
   * Set the static configuration for UGI.
   * In particular, set the security authentication mechanism and the
   * group look up service.
   * @param conf the configuration to use
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void setConfiguration(Configuration conf) {
    initialize(conf, true);
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public static void reset() {
    authenticationMethod = null;
    conf = null;
    groups = null;
    kerberosMinSecondsBeforeRelogin = 0;
    kerberosKeyTabLoginRenewalEnabled = false;
    kerberosLoginRenewalExecutor = Optional.empty();
    setLoginUser(null);
    HadoopKerberosName.setRules(null);
  }
  
  /**
   * Determine if UserGroupInformation is using Kerberos to determine
   * user identities or is relying on simple authentication
   * 
   * @return true if UGI is working in a secure environment
   */
  public static boolean isSecurityEnabled() {
    return !isAuthenticationMethodEnabled(AuthenticationMethod.SIMPLE);
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  private static boolean isAuthenticationMethodEnabled(AuthenticationMethod method) {
    ensureInitialized();
    return (authenticationMethod == method);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  @VisibleForTesting
  static boolean isKerberosKeyTabLoginRenewalEnabled() {
    ensureInitialized();
    return kerberosKeyTabLoginRenewalEnabled;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  @VisibleForTesting
  static Optional<ExecutorService> getKerberosLoginRenewalExecutor() {
    ensureInitialized();
    return kerberosLoginRenewalExecutor;
  }

  /**
   * Information about the logged in user.
   */
  private static final AtomicReference<UserGroupInformation> loginUserRef =
    new AtomicReference<>();

  private final Subject subject;
  // All non-static fields must be read-only caches that come from the subject.
  private final User user;

  private static String OS_LOGIN_MODULE_NAME;
  private static Class<? extends Principal> OS_PRINCIPAL_CLASS;
  
  private static final boolean windows =
      System.getProperty("os.name").startsWith("Windows");

  /* Return the OS login module class name */
  /* For IBM JDK, use the common OS login module class name for all platforms */
  private static String getOSLoginModuleName() {
    if (IBM_JAVA) {
      return "com.ibm.security.auth.module.JAASLoginModule";
    } else {
      return windows ? "com.sun.security.auth.module.NTLoginModule"
        : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  /* Return the OS principal class */
  /* For IBM JDK, use the common OS principal class for all platforms */
  @SuppressWarnings("unchecked")
  private static Class<? extends Principal> getOsPrincipalClass() {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try {
      String principalClass = null;
      if (IBM_JAVA) {
        principalClass = "com.ibm.security.auth.UsernamePrincipal";
      } else {
        principalClass = windows ? "com.sun.security.auth.NTUserPrincipal"
            : "com.sun.security.auth.UnixPrincipal";
      }
      return (Class<? extends Principal>) cl.loadClass(principalClass);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find JAAS classes:" + e.getMessage());
    }
    return null;
  }
  static {
    OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
    OS_PRINCIPAL_CLASS = getOsPrincipalClass();
  }

  private static class RealUser implements Principal {
    private final UserGroupInformation realUser;
    
    RealUser(UserGroupInformation realUser) {
      this.realUser = realUser;
    }
    
    @Override
    public String getName() {
      return realUser.getUserName();
    }
    
    public UserGroupInformation getRealUser() {
      return realUser;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o == null || getClass() != o.getClass()) {
        return false;
      } else {
        return realUser.equals(((RealUser) o).realUser);
      }
    }
    
    @Override
    public int hashCode() {
      return realUser.hashCode();
    }
    
    @Override
    public String toString() {
      return realUser.toString();
    }
  }

  private static HadoopLoginContext
  newLoginContext(String appName, Subject subject,
                  HadoopConfiguration loginConf)
      throws LoginException {
    // Temporarily switch the thread's ContextClassLoader to match this
    // class's classloader, so that we can properly load HadoopLoginModule
    // from the JAAS libraries.
    Thread t = Thread.currentThread();
    ClassLoader oldCCL = t.getContextClassLoader();
    t.setContextClassLoader(HadoopLoginModule.class.getClassLoader());
    try {
      return new HadoopLoginContext(appName, subject, loginConf);
    } finally {
      t.setContextClassLoader(oldCCL);
    }
  }

  // return the LoginContext only if it's managed by the ugi.  externally
  // managed login contexts will be ignored.
  private HadoopLoginContext getLogin() {
    LoginContext login = user.getLogin();
    return (login instanceof HadoopLoginContext)
      ? (HadoopLoginContext)login : null;
  }

  private void setLogin(LoginContext login) {
    user.setLogin(login);
  }

  /**
   * Set the last login time for logged in user
   * @param loginTime the number of milliseconds since the beginning of time
   */
  private void setLastLogin(long loginTime) {
    user.setLastLogin(loginTime);
  }

  /**
   * Create a UserGroupInformation for the given subject.
   * This does not change the subject or acquire new credentials.
   *
   * The creator of subject is responsible for renewing credentials.
   * @param subject the user's subject
   */
  UserGroupInformation(Subject subject) {
    this.subject = subject;
    // do not access ANY private credentials since they are mutable
    // during a relogin.  no principal locking necessary since
    // relogin/logout does not remove User principal.
    this.user = subject.getPrincipals(User.class).iterator().next();
    if (user == null || user.getName() == null) {
      throw new IllegalStateException("Subject does not contain a valid User");
    }
  }

  /**
   * checks if logged in using kerberos
   * @return true if the subject logged via keytab or has a Kerberos TGT
   */
  public boolean hasKerberosCredentials() {
    return user.getAuthenticationMethod() == AuthenticationMethod.KERBEROS;
  }

  /**
   * Return the current user, including any doAs in the current stack.
   * @return the current user
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation getCurrentUser() throws IOException {
    ensureInitialized();
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    if (subject == null || subject.getPrincipals(User.class).isEmpty()) {
      return getLoginUser();
    } else {
      return new UserGroupInformation(subject);
    }
  }

  /**
   * Find the most appropriate UserGroupInformation to use
   *
   * @param ticketCachePath    The Kerberos ticket cache path, or NULL
   *                           if none is specfied
   * @param user               The user name, or NULL if none is specified.
   *
   * @return                   The most appropriate UserGroupInformation
   */ 
  public static UserGroupInformation getBestUGI(
      String ticketCachePath, String user) throws IOException {
    if (ticketCachePath != null) {
      return getUGIFromTicketCache(ticketCachePath, user);
    } else if (user == null) {
      return getCurrentUser();
    } else {
      return createRemoteUser(user);
    }    
  }

  /**
   * Create a UserGroupInformation from a Kerberos ticket cache.
   * 
   * @param user                The principal name to load from the ticket
   *                            cache
   * @param ticketCache     the path to the ticket cache file
   *
   * @throws IOException        if the kerberos login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation getUGIFromTicketCache(
            String ticketCache, String user) throws IOException {
    if (!isAuthenticationMethodEnabled(AuthenticationMethod.KERBEROS)) {
      return getBestUGI(null, user);
    }
    LoginParams params = new LoginParams();
    params.put(LoginParam.PRINCIPAL, user);
    params.put(LoginParam.CCACHE, ticketCache);
    return doSubjectLogin(null, params);
  }

  /**
   * Create a UserGroupInformation from a Subject with Kerberos principal.
   *
   * @param subject             The KerberosPrincipal to use in UGI.
   *                            The creator of subject is responsible for
   *                            renewing credentials.
   *
   * @throws IOException
   * @throws KerberosAuthException if the kerberos login fails
   */
  public static UserGroupInformation getUGIFromSubject(Subject subject)
      throws IOException {
    if (subject == null) {
      throw new KerberosAuthException(SUBJECT_MUST_NOT_BE_NULL);
    }

    if (subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
      throw new KerberosAuthException(SUBJECT_MUST_CONTAIN_PRINCIPAL);
    }

    // null params indicate external subject login.  no login context will
    // be attached.
    return doSubjectLogin(subject, null);
  }

  /**
   * Get the currently logged in user.  If no explicit login has occurred,
   * the user will automatically be logged in with either kerberos credentials
   * if available, or as the local OS user, based on security settings.
   * @return the logged in user
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation getLoginUser() throws IOException {
    ensureInitialized();
    UserGroupInformation loginUser = loginUserRef.get();
    // a potential race condition exists only for the initial creation of
    // the login user.  there's no need to penalize all subsequent calls
    // with sychronization overhead so optimistically create a login user
    // and discard if we lose the race.
    if (loginUser == null) {
      UserGroupInformation newLoginUser = createLoginUser(null);
      do {
        // it's extremely unlikely that the login user will be non-null
        // (lost CAS race), but be nulled before the subsequent get, but loop
        // for correctness.
        if (loginUserRef.compareAndSet(null, newLoginUser)) {
          loginUser = newLoginUser;
          // only spawn renewal if this login user is the winner.
          loginUser.spawnAutoRenewalThreadForUserCreds(false);
        } else {
          loginUser = loginUserRef.get();
        }
      } while (loginUser == null);
    }
    return loginUser;
  }

  /**
   * remove the login method that is followed by a space from the username
   * e.g. "jack (auth:SIMPLE)" {@literal ->} "jack"
   *
   * @param userName
   * @return userName without login method
   */
  public static String trimLoginMethod(String userName) {
    int spaceIndex = userName.indexOf(' ');
    if (spaceIndex >= 0) {
      userName = userName.substring(0, spaceIndex);
    }
    return userName;
  }

  /**
   * Log in a user using the given subject
   * @param subject the subject to use when logging in a user, or null to
   * create a new subject.
   *
   * If subject is not null, the creator of subject is responsible for renewing
   * credentials.
   *
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void loginUserFromSubject(Subject subject) throws IOException {
    setLoginUser(createLoginUser(subject));
  }

  private static
  UserGroupInformation createLoginUser(Subject subject) throws IOException {
    UserGroupInformation realUser = doSubjectLogin(subject, null);
    UserGroupInformation loginUser = null;
    try {
      // If the HADOOP_PROXY_USER environment variable or property
      // is specified, create a proxy user as the logged in user.
      String proxyUser = System.getenv(HADOOP_PROXY_USER);
      if (proxyUser == null) {
        proxyUser = System.getProperty(HADOOP_PROXY_USER);
      }
      loginUser = proxyUser == null ? realUser : createProxyUser(proxyUser, realUser);

      // Load tokens from files
      final Collection<String> tokenFileLocations = new LinkedHashSet<>();
      tokenFileLocations.addAll(getTrimmedStringCollection(
          System.getProperty(HADOOP_TOKEN_FILES)));
      tokenFileLocations.addAll(getTrimmedStringCollection(
          conf.get(HADOOP_TOKEN_FILES)));
      tokenFileLocations.addAll(getTrimmedStringCollection(
          System.getenv(HADOOP_TOKEN_FILE_LOCATION)));
      for (String tokenFileLocation : tokenFileLocations) {
        if (tokenFileLocation != null && tokenFileLocation.length() > 0) {
          File tokenFile = new File(tokenFileLocation);
          LOG.debug("Reading credentials from location {}",
              tokenFile.getCanonicalPath());
          if (tokenFile.exists() && tokenFile.isFile()) {
            Credentials cred = Credentials.readTokenStorageFile(
                tokenFile, conf);
            LOG.debug("Loaded {} tokens from {}", cred.numberOfTokens(),
                tokenFile.getCanonicalPath());
            loginUser.addCredentials(cred);
          } else {
            LOG.info("Token file {} does not exist",
                tokenFile.getCanonicalPath());
          }
        }
      }

      // Load tokens from base64 encoding
      final Collection<String> tokensBase64 = new LinkedHashSet<>();
      tokensBase64.addAll(getTrimmedStringCollection(
          System.getProperty(HADOOP_TOKENS)));
      tokensBase64.addAll(getTrimmedStringCollection(
          conf.get(HADOOP_TOKENS)));
      tokensBase64.addAll(getTrimmedStringCollection(
          System.getenv(HADOOP_TOKEN)));
      int numTokenBase64 = 0;
      for (String tokenBase64 : tokensBase64) {
        if (tokenBase64 != null && tokenBase64.length() > 0) {
          try {
            Token<TokenIdentifier> token = new Token<>();
            token.decodeFromUrlString(tokenBase64);
            Credentials cred = new Credentials();
            cred.addToken(token.getService(), token);
            loginUser.addCredentials(cred);
            numTokenBase64++;
          } catch (IOException ioe) {
            LOG.error("Cannot add token {}: {}",
                tokenBase64, ioe.getMessage());
          }
        }
      }
      if (numTokenBase64 > 0) {
        LOG.debug("Loaded {} base64 tokens", numTokenBase64);
      }
    } catch (IOException ioe) {
      LOG.debug("Failure to load login credentials", ioe);
      throw ioe;
    }
    LOG.debug("UGI loginUser: {}", loginUser);
    return loginUser;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  public static void setLoginUser(UserGroupInformation ugi) {
    // if this is to become stable, should probably logout the currently
    // logged in ugi if it's different
    loginUserRef.set(ugi);
  }
  
  private String getKeytab() {
    HadoopLoginContext login = getLogin();
    return (login != null)
      ? login.getConfiguration().getParameters().get(LoginParam.KEYTAB)
      : null;
  }

  /**
   * Is the ugi managed by the UGI or an external subject?
   * @return true if managed by UGI.
   */
  private boolean isHadoopLogin() {
    // checks if the private hadoop login context is managing the ugi.
    return getLogin() != null;
  }

  /**
   * Is this user logged in from a keytab file managed by the UGI?
   * @return true if the credentials are from a keytab file.
   */
  public boolean isFromKeytab() {
    // can't simply check if keytab is present since a relogin failure will
    // have removed the keytab from priv creds.  instead, check login params.
    return hasKerberosCredentials() && isHadoopLogin() && getKeytab() != null;
  }
  
  /**
   *  Is this user logged in from a ticket (but no keytab) managed by the UGI?
   * @return true if the credentials are from a ticket cache.
   */
  private boolean isFromTicket() {
    return hasKerberosCredentials() && isHadoopLogin() && getKeytab() == null;
  }

  /**
   * Get the Kerberos TGT
   * @return the user's TGT or null if none was found
   */
  private KerberosTicket getTGT() {
    Set<KerberosTicket> tickets = subject
        .getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket ticket : tickets) {
      if (SecurityUtil.isOriginalTGT(ticket)) {
        return ticket;
      }
    }
    return null;
  }
  
  private long getRefreshTime(KerberosTicket tgt) {
    long start = tgt.getStartTime().getTime();
    long end = tgt.getEndTime().getTime();
    return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public boolean shouldRelogin() {
    return hasKerberosCredentials() && isHadoopLogin();
  }

  /**
   * Spawn a thread to do periodic renewals of kerberos credentials. NEVER
   * directly call this method. This method should only be used for ticket cache
   * based kerberos credentials.
   *
   * @param force - used by tests to forcibly spawn thread
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  void spawnAutoRenewalThreadForUserCreds(boolean force) {
    if (!force && (!shouldRelogin() || isFromKeytab())) {
      return;
    }

    //spawn thread only if we have kerb credentials
    KerberosTicket tgt = getTGT();
    if (tgt == null) {
      return;
    }
    String cmd = conf.get("hadoop.kerberos.kinit.command", "kinit");
    long nextRefresh = getRefreshTime(tgt);
    executeAutoRenewalTask(getUserName(),
            new TicketCacheRenewalRunnable(tgt, cmd, nextRefresh));
  }

  /**
   * Spawn a thread to do periodic renewals of kerberos credentials from a
   * keytab file.
   */
  private void spawnAutoRenewalThreadForKeytab() {
    if (!shouldRelogin() || isFromTicket()) {
      return;
    }

    // spawn thread only if we have kerb credentials
    KerberosTicket tgt = getTGT();
    if (tgt == null) {
      return;
    }
    long nextRefresh = getRefreshTime(tgt);
    executeAutoRenewalTask(getUserName(),
            new KeytabRenewalRunnable(tgt, nextRefresh));
  }

  /**
   * Spawn a thread to do periodic renewals of kerberos credentials from a
   * keytab file. NEVER directly call this method.
   *
   * @param userName Name of the user for which login needs to be renewed.
   * @param task  The reference of the login renewal task.
   */
  private void executeAutoRenewalTask(final String userName,
                                      AutoRenewalForUserCredsRunnable task) {
    kerberosLoginRenewalExecutor = Optional.of(
            Executors.newSingleThreadExecutor(
                  new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                      Thread t = new Thread(r);
                      t.setDaemon(true);
                      t.setName("TGT Renewer for " + userName);
                      return t;
                    }
                  }
            ));
    kerberosLoginRenewalExecutor.get().submit(task);
  }

  /**
   * An abstract class which encapsulates the functionality required to
   * auto renew Kerbeors TGT. The concrete implementations of this class
   * are expected to provide implementation required to perform actual
   * TGT renewal (see {@code TicketCacheRenewalRunnable} and
   * {@code KeytabRenewalRunnable}).
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  abstract class AutoRenewalForUserCredsRunnable implements Runnable {
    private KerberosTicket tgt;
    private RetryPolicy rp;
    private long nextRefresh;
    private boolean runRenewalLoop = true;

    AutoRenewalForUserCredsRunnable(KerberosTicket tgt, long nextRefresh) {
      this.tgt = tgt;
      this.nextRefresh = nextRefresh;
      this.rp = null;
    }

    public void setRunRenewalLoop(boolean runRenewalLoop) {
      this.runRenewalLoop = runRenewalLoop;
    }

    /**
     * This method is used to perform renewal of kerberos login ticket.
     * The concrete implementations of this class should provide specific
     * logic required to perform renewal as part of this method.
     */
    protected abstract void relogin() throws IOException;

    @Override
    public void run() {
      do {
        try {
          long now = Time.now();
          LOG.debug("Current time is {}, next refresh is {}", now, nextRefresh);
          if (now < nextRefresh) {
            Thread.sleep(nextRefresh - now);
          }
          relogin();
          tgt = getTGT();
          if (tgt == null) {
            LOG.warn("No TGT after renewal. Aborting renew thread for " +
                getUserName());
            return;
          }
          nextRefresh = Math.max(getRefreshTime(tgt),
              now + kerberosMinSecondsBeforeRelogin);
          metrics.renewalFailures.set(0);
          rp = null;
        } catch (InterruptedException ie) {
          LOG.warn("Terminating renewal thread");
          return;
        } catch (IOException ie) {
          metrics.renewalFailuresTotal.incr();
          final long now = Time.now();

          if (tgt.isDestroyed()) {
            LOG.error(String.format("TGT is destroyed. " +
                    "Aborting renew thread for %s.", getUserName()), ie);
            return;
          }

          long tgtEndTime;
          // As described in HADOOP-15593 we need to handle the case when
          // tgt.getEndTime() throws NPE because of JDK issue JDK-8147772
          // NPE is only possible if this issue is not fixed in the JDK
          // currently used
          try {
            tgtEndTime = tgt.getEndTime().getTime();
          } catch (NullPointerException npe) {
            LOG.error("NPE thrown while getting KerberosTicket endTime. "
                + "Aborting renew thread for {}.", getUserName(), ie);
            return;
          }

          LOG.warn(
              "Exception encountered while running the "
                  + "renewal command for {}. "
                  + "(TGT end time:{}, renewalFailures: {}, "
                  + "renewalFailuresTotal: {})",
              getUserName(), tgtEndTime, metrics.renewalFailures.value(),
              metrics.renewalFailuresTotal.value(), ie);
          if (rp == null) {
            // Use a dummy maxRetries to create the policy. The policy will
            // only be used to get next retry time with exponential back-off.
            // The final retry time will be later limited within the
            // tgt endTime in getNextTgtRenewalTime.
            rp = RetryPolicies.exponentialBackoffRetry(Long.SIZE - 2,
                kerberosMinSecondsBeforeRelogin, TimeUnit.MILLISECONDS);
          }
          try {
            nextRefresh = getNextTgtRenewalTime(tgtEndTime, now, rp);
          } catch (Exception e) {
            LOG.error("Exception when calculating next tgt renewal time", e);
            return;
          }
          metrics.renewalFailures.incr();
          // retry until close enough to tgt endTime.
          if (now > nextRefresh) {
            LOG.error("TGT is expired. Aborting renew thread for {}.",
                getUserName());
            return;
          }
        }
      } while (runRenewalLoop);
    }
  }

  /**
   * A concrete implementation of {@code AutoRenewalForUserCredsRunnable} class
   * which performs TGT renewal using kinit command.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  final class TicketCacheRenewalRunnable
      extends AutoRenewalForUserCredsRunnable {
    private String kinitCmd;

    TicketCacheRenewalRunnable(KerberosTicket tgt, String kinitCmd,
        long nextRefresh) {
      super(tgt, nextRefresh);
      this.kinitCmd = kinitCmd;
    }

    @Override
    public void relogin() throws IOException {
      String output = Shell.execCommand(kinitCmd, "-R");
      LOG.debug("Renewed ticket. kinit output: {}", output);
      reloginFromTicketCache();
    }
  }

  /**
   * A concrete implementation of {@code AutoRenewalForUserCredsRunnable} class
   * which performs TGT renewal using specified keytab.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  final class KeytabRenewalRunnable extends AutoRenewalForUserCredsRunnable {

    KeytabRenewalRunnable(KerberosTicket tgt, long nextRefresh) {
      super(tgt, nextRefresh);
    }

    @Override
    public void relogin() throws IOException {
      reloginFromKeytab();
    }
  }

  /**
   * Get time for next login retry. This will allow the thread to retry with
   * exponential back-off, until tgt endtime.
   * Last retry is {@link #kerberosMinSecondsBeforeRelogin} before endtime.
   *
   * @param tgtEndTime EndTime of the tgt.
   * @param now Current time.
   * @param rp The retry policy.
   * @return Time for next login retry.
   */
  @VisibleForTesting
  static long getNextTgtRenewalTime(final long tgtEndTime, final long now,
      final RetryPolicy rp) throws Exception {
    final long lastRetryTime = tgtEndTime - kerberosMinSecondsBeforeRelogin;
    final RetryPolicy.RetryAction ra = rp.shouldRetry(null,
        metrics.renewalFailures.value(), 0, false);
    return Math.min(lastRetryTime, now + ra.delayMillis);
  }

  /**
   * Log a user in from a keytab file. Loads a user identity from a keytab
   * file and logs them in. They become the currently logged-in user.
   * @param user the principal name to load from the keytab
   * @param path the path to the keytab file
   * @throws IOException
   * @throws KerberosAuthException if it's a kerberos login exception.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public
  static void loginUserFromKeytab(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return;

    UserGroupInformation u = loginUserFromKeytabAndReturnUGI(user, path);
    if (isKerberosKeyTabLoginRenewalEnabled()) {
      u.spawnAutoRenewalThreadForKeytab();
    }

    setLoginUser(u);

    LOG.info(
        "Login successful for user {} using keytab file {}. Keytab auto"
            + " renewal enabled : {}",
        user, new File(path).getName(), isKerberosKeyTabLoginRenewalEnabled());
  }

  /**
   * Log the current user out who previously logged in using keytab.
   * This method assumes that the user logged in by calling
   * {@link #loginUserFromKeytab(String, String)}.
   *
   * @throws IOException
   * @throws KerberosAuthException if a failure occurred in logout,
   * or if the user did not log in by invoking loginUserFromKeyTab() before.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public void logoutUserFromKeytab() throws IOException {
    if (!hasKerberosCredentials()) {
      return;
    }

    // Shutdown the background task performing login renewal.
    if (getKerberosLoginRenewalExecutor().isPresent()) {
      getKerberosLoginRenewalExecutor().get().shutdownNow();
    }

    HadoopLoginContext login = getLogin();
    String keytabFile = getKeytab();
    if (login == null || keytabFile == null) {
      throw new KerberosAuthException(MUST_FIRST_LOGIN_FROM_KEYTAB);
    }

    try {
      LOG.debug("Initiating logout for {}", getUserName());
      // hadoop login context internally locks credentials.
      login.logout();
    } catch (LoginException le) {
      KerberosAuthException kae = new KerberosAuthException(LOGOUT_FAILURE, le);
      kae.setUser(user.toString());
      kae.setKeytabFile(keytabFile);
      throw kae;
    }

    LOG.info("Logout successful for user " + getUserName()
        + " using keytab file " + keytabFile);
  }
  
  /**
   * Re-login a user from keytab if TGT is expired or is close to expiry.
   * 
   * @throws IOException
   * @throws KerberosAuthException if it's a kerberos login exception.
   */
  public void checkTGTAndReloginFromKeytab() throws IOException {
    reloginFromKeytab(true);
  }

  // if the first kerberos ticket is not TGT, then remove and destroy it since
  // the kerberos library of jdk always use the first kerberos ticket as TGT.
  // See HADOOP-13433 for more details.
  @VisibleForTesting
  void fixKerberosTicketOrder() {
    Set<Object> creds = getSubject().getPrivateCredentials();
    synchronized (creds) {
      for (Iterator<Object> iter = creds.iterator(); iter.hasNext();) {
        Object cred = iter.next();
        if (cred instanceof KerberosTicket) {
          KerberosTicket ticket = (KerberosTicket) cred;
          if (ticket.isDestroyed() || ticket.getServer() == null) {
            LOG.warn("Ticket is already destroyed, remove it.");
            iter.remove();
          } else if (!ticket.getServer().getName().startsWith("krbtgt")) {
            LOG.warn(
                "The first kerberos ticket is not TGT"
                    + "(the server principal is {}), remove and destroy it.",
                ticket.getServer());
            iter.remove();
            try {
              ticket.destroy();
            } catch (DestroyFailedException e) {
              LOG.warn("destroy ticket failed", e);
            }
          } else {
            return;
          }
        }
      }
    }
    LOG.warn("Warning, no kerberos ticket found while attempting to renew ticket");
  }

  /**
   * Re-Login a user in from a keytab file. Loads a user identity from a keytab
   * file and logs them in. They become the currently logged-in user. This
   * method assumes that {@link #loginUserFromKeytab(String, String)} had
   * happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException
   * @throws KerberosAuthException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public void reloginFromKeytab() throws IOException {
    reloginFromKeytab(false);
  }

  /**
   * Force re-Login a user in from a keytab file irrespective of the last login
   * time. Loads a user identity from a keytab file and logs them in. They
   * become the currently logged-in user. This method assumes that
   * {@link #loginUserFromKeytab(String, String)} had happened already. The
   * Subject field of this UserGroupInformation object is updated to have the
   * new credentials.
   *
   * @throws IOException
   * @throws KerberosAuthException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public void forceReloginFromKeytab() throws IOException {
    reloginFromKeytab(false, true);
  }

  private void reloginFromKeytab(boolean checkTGT) throws IOException {
    reloginFromKeytab(checkTGT, false);
  }

  private void reloginFromKeytab(boolean checkTGT, boolean ignoreLastLoginTime)
      throws IOException {
    if (!shouldRelogin() || !isFromKeytab()) {
      return;
    }
    HadoopLoginContext login = getLogin();
    if (login == null) {
      throw new KerberosAuthException(MUST_FIRST_LOGIN_FROM_KEYTAB);
    }
    if (checkTGT) {
      KerberosTicket tgt = getTGT();
      if (tgt != null && !shouldRenewImmediatelyForTests &&
        Time.now() < getRefreshTime(tgt)) {
        return;
      }
    }
    relogin(login, ignoreLastLoginTime);
  }

  /**
   * Re-Login a user in from the ticket cache.  This
   * method assumes that login had happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException
   * @throws KerberosAuthException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public void reloginFromTicketCache() throws IOException {
    if (!shouldRelogin() || !isFromTicket()) {
      return;
    }
    HadoopLoginContext login = getLogin();
    if (login == null) {
      throw new KerberosAuthException(MUST_FIRST_LOGIN);
    }
    relogin(login, false);
  }

  private void relogin(HadoopLoginContext login, boolean ignoreLastLoginTime)
      throws IOException {
    // ensure the relogin is atomic to avoid leaving credentials in an
    // inconsistent state.  prevents other ugi instances, SASL, and SPNEGO
    // from accessing or altering credentials during the relogin.
    synchronized(login.getSubjectLock()) {
      // another racing thread may have beat us to the relogin.
      if (login == getLogin()) {
        unprotectedRelogin(login, ignoreLastLoginTime);
      }
    }
  }

  private void unprotectedRelogin(HadoopLoginContext login,
      boolean ignoreLastLoginTime) throws IOException {
    assert Thread.holdsLock(login.getSubjectLock());
    long now = Time.now();
    if (!hasSufficientTimeElapsed(now) && !ignoreLastLoginTime) {
      return;
    }
    // register most recent relogin attempt
    user.setLastLogin(now);
    try {
      LOG.debug("Initiating logout for {}", getUserName());
      //clear up the kerberos state. But the tokens are not cleared! As per 
      //the Java kerberos login module code, only the kerberos credentials
      //are cleared
      login.logout();
      //login and also update the subject field of this instance to 
      //have the new credentials (pass it to the LoginContext constructor)
      login = newLoginContext(
        login.getAppName(), login.getSubject(), login.getConfiguration());
      LOG.debug("Initiating re-login for {}", getUserName());
      login.login();
      // this should be unnecessary.  originally added due to improper locking
      // of the subject during relogin.
      fixKerberosTicketOrder();
      setLogin(login);
    } catch (LoginException le) {
      KerberosAuthException kae = new KerberosAuthException(LOGIN_FAILURE, le);
      kae.setUser(getUserName());
      throw kae;
    }
  }

  /**
   * Log a user in from a keytab file. Loads a user identity from a keytab
   * file and login them in. This new user does not affect the currently
   * logged-in user.
   * @param user the principal name to load from the keytab
   * @param path the path to the keytab file
   * @throws IOException if the keytab file can't be read
   */
  public
  static UserGroupInformation loginUserFromKeytabAndReturnUGI(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return UserGroupInformation.getCurrentUser();

    LoginParams params = new LoginParams();
    params.put(LoginParam.PRINCIPAL, user);
    params.put(LoginParam.KEYTAB, path);
    return doSubjectLogin(null, params);
  }

  private boolean hasSufficientTimeElapsed(long now) {
    if (!shouldRenewImmediatelyForTests &&
        now - user.getLastLogin() < kerberosMinSecondsBeforeRelogin ) {
      LOG.warn("Not attempting to re-login since the last re-login was " +
          "attempted less than " + (kerberosMinSecondsBeforeRelogin/1000) +
          " seconds before. Last Login=" + user.getLastLogin());
      return false;
    }
    return true;
  }
  
  /**
   * Did the login happen via keytab
   * @return true or false
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static boolean isLoginKeytabBased() throws IOException {
    return getLoginUser().isFromKeytab();
  }

  /**
   * Did the login happen via ticket cache
   * @return true or false
   */
  public static boolean isLoginTicketBased()  throws IOException {
    return getLoginUser().isFromTicket();
  }

  /**
   * Create a user from a login name. It is intended to be used for remote
   * users in RPC, since it won't have any credentials.
   * @param user the full user principal name, must not be empty or null
   * @return the UserGroupInformation for the remote user.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createRemoteUser(String user) {
    return createRemoteUser(user, AuthMethod.SIMPLE);
  }
  
  /**
   * Create a user from a login name. It is intended to be used for remote
   * users in RPC, since it won't have any credentials.
   * @param user the full user principal name, must not be empty or null
   * @return the UserGroupInformation for the remote user.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createRemoteUser(String user, AuthMethod authMethod) {
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException("Null user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    UserGroupInformation result = new UserGroupInformation(subject);
    result.setAuthenticationMethod(authMethod);
    return result;
  }

  /**
   * existing types of authentications' methods
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum AuthenticationMethod {
    // currently we support only one auth per method, but eventually a 
    // subtype is needed to differentiate, ex. if digest is token or ldap
    SIMPLE(AuthMethod.SIMPLE,
        HadoopConfiguration.SIMPLE_CONFIG_NAME),
    KERBEROS(AuthMethod.KERBEROS,
        HadoopConfiguration.KERBEROS_CONFIG_NAME),
    TOKEN(AuthMethod.TOKEN),
    CERTIFICATE(null),
    KERBEROS_SSL(null),
    PROXY(null);
    
    private final AuthMethod authMethod;
    private final String loginAppName;
    
    private AuthenticationMethod(AuthMethod authMethod) {
      this(authMethod, null);
    }
    private AuthenticationMethod(AuthMethod authMethod, String loginAppName) {
      this.authMethod = authMethod;
      this.loginAppName = loginAppName;
    }
    
    public AuthMethod getAuthMethod() {
      return authMethod;
    }
    
    String getLoginAppName() {
      if (loginAppName == null) {
        throw new UnsupportedOperationException(
            this + " login authentication is not supported");
      }
      return loginAppName;
    }
    
    public static AuthenticationMethod valueOf(AuthMethod authMethod) {
      for (AuthenticationMethod value : values()) {
        if (value.getAuthMethod() == authMethod) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "no authentication method for " + authMethod);
    }
  };

  /**
   * Create a proxy user using username of the effective user and the ugi of the
   * real user.
   * @param user
   * @param realUser
   * @return proxyUser ugi
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createProxyUser(String user,
      UserGroupInformation realUser) {
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException("Null user");
    }
    if (realUser == null) {
      throw new IllegalArgumentException("Null real user");
    }
    Subject subject = new Subject();
    Set<Principal> principals = subject.getPrincipals();
    principals.add(new User(user, AuthenticationMethod.PROXY, null));
    principals.add(new RealUser(realUser));
    return new UserGroupInformation(subject);
  }

  /**
   * get RealUser (vs. EffectiveUser)
   * @return realUser running over proxy user
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public UserGroupInformation getRealUser() {
    for (RealUser p: subject.getPrincipals(RealUser.class)) {
      return p.getRealUser();
    }
    return null;
  }


  
  /**
   * This class is used for storing the groups for testing. It stores a local
   * map that has the translation of usernames to groups.
   */
  private static class TestingGroups extends Groups {
    private final Map<String, List<String>> userToGroupsMapping = 
      new HashMap<String,List<String>>();
    private Groups underlyingImplementation;
    
    private TestingGroups(Groups underlyingImplementation) {
      super(new org.apache.hadoop.conf.Configuration());
      this.underlyingImplementation = underlyingImplementation;
    }
    
    @Override
    public List<String> getGroups(String user) throws IOException {
      List<String> result = userToGroupsMapping.get(user);
      
      if (result == null) {
        result = underlyingImplementation.getGroups(user);
      }

      return result;
    }

    private void setUserGroups(String user, String[] groups) {
      userToGroupsMapping.put(user, Arrays.asList(groups));
    }
  }

  /**
   * Create a UGI for testing HDFS and MapReduce
   * @param user the full user principal name
   * @param userGroups the names of the groups that the user belongs to
   * @return a fake user for running unit tests
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static UserGroupInformation createUserForTesting(String user, 
                                                          String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createRemoteUser(user);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups(groups);
    }
    // add the user groups
    ((TestingGroups) groups).setUserGroups(ugi.getShortUserName(), userGroups);
    return ugi;
  }


  /**
   * Create a proxy user UGI for testing HDFS and MapReduce
   * 
   * @param user
   *          the full user principal name for effective user
   * @param realUser
   *          UGI of the real user
   * @param userGroups
   *          the names of the groups that the user belongs to
   * @return a fake user for running unit tests
   */
  public static UserGroupInformation createProxyUserForTesting(String user,
      UserGroupInformation realUser, String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createProxyUser(user, realUser);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups(groups);
    }
    // add the user groups
    ((TestingGroups) groups).setUserGroups(ugi.getShortUserName(), userGroups);
    return ugi;
  }
  
  /**
   * Get the user's login name.
   * @return the user's name up to the first '/' or '@'.
   */
  public String getShortUserName() {
    return user.getShortName();
  }

  public String getPrimaryGroupName() throws IOException {
    List<String> groups = getGroups();
    if (groups.isEmpty()) {
      throw new IOException("There is no primary group for UGI " + this);
    }
    return groups.get(0);
  }

  /**
   * Get the user's full principal name.
   * @return the user's full principal name.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public String getUserName() {
    return user.getName();
  }

  /**
   * Add a TokenIdentifier to this UGI. The TokenIdentifier has typically been
   * authenticated by the RPC layer as belonging to the user represented by this
   * UGI.
   * 
   * @param tokenId
   *          tokenIdentifier to be added
   * @return true on successful add of new tokenIdentifier
   */
  public synchronized boolean addTokenIdentifier(TokenIdentifier tokenId) {
    return subject.getPublicCredentials().add(tokenId);
  }

  /**
   * Get the set of TokenIdentifiers belonging to this UGI
   * 
   * @return the set of TokenIdentifiers belonging to this UGI
   */
  public synchronized Set<TokenIdentifier> getTokenIdentifiers() {
    return subject.getPublicCredentials(TokenIdentifier.class);
  }
  
  /**
   * Add a token to this UGI
   * 
   * @param token Token to be added
   * @return true on successful add of new token
   */
  public boolean addToken(Token<? extends TokenIdentifier> token) {
    return (token != null) ? addToken(token.getService(), token) : false;
  }

  /**
   * Add a named token to this UGI
   * 
   * @param alias Name of the token
   * @param token Token to be added
   * @return true on successful add of new token
   */
  public boolean addToken(Text alias, Token<? extends TokenIdentifier> token) {
    synchronized (subject) {
      getCredentialsInternal().addToken(alias, token);
      return true;
    }
  }
  
  /**
   * Obtain the collection of tokens associated with this user.
   * 
   * @return an unmodifiable collection of tokens associated with user
   */
  public Collection<Token<? extends TokenIdentifier>> getTokens() {
    synchronized (subject) {
      return Collections.unmodifiableCollection(
          new ArrayList<Token<?>>(getCredentialsInternal().getAllTokens()));
    }
  }

  /**
   * Obtain the tokens in credentials form associated with this user.
   * 
   * @return Credentials of tokens associated with this user
   */
  public Credentials getCredentials() {
    synchronized (subject) {
      Credentials creds = new Credentials(getCredentialsInternal());
      Iterator<Token<?>> iter = creds.getAllTokens().iterator();
      while (iter.hasNext()) {
        if (iter.next().isPrivate()) {
          iter.remove();
        }
      }
      return creds;
    }
  }
  
  /**
   * Add the given Credentials to this user.
   * @param credentials of tokens and secrets
   */
  public void addCredentials(Credentials credentials) {
    synchronized (subject) {
      getCredentialsInternal().addAll(credentials);
    }
  }

  private synchronized Credentials getCredentialsInternal() {
    final Credentials credentials;
    final Set<Credentials> credentialsSet =
      subject.getPrivateCredentials(Credentials.class);
    if (!credentialsSet.isEmpty()){
      credentials = credentialsSet.iterator().next();
    } else {
      credentials = new Credentials();
      subject.getPrivateCredentials().add(credentials);
    }
    return credentials;
  }

  /**
   * Get the group names for this user. {@link #getGroups()} is less
   * expensive alternative when checking for a contained element.
   * @return the list of users with the primary group first. If the command
   *    fails, it returns an empty list.
   */
  public String[] getGroupNames() {
    List<String> groups = getGroups();
    return groups.toArray(new String[groups.size()]);
  }

  /**
   * Get the group names for this user.
   * @return the list of users with the primary group first. If the command
   *    fails, it returns an empty list.
   */
  public List<String> getGroups() {
    ensureInitialized();
    try {
      return groups.getGroups(getShortUserName());
    } catch (IOException ie) {
      LOG.debug("Failed to get groups for user {}", getShortUserName(), ie);
      return Collections.emptyList();
    }
  }

  /**
   * Return the username.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getUserName());
    sb.append(" (auth:"+getAuthenticationMethod()+")");
    if (getRealUser() != null) {
      sb.append(" via ").append(getRealUser().toString());
    }
    return sb.toString();
  }

  /**
   * Sets the authentication method in the subject
   * 
   * @param authMethod
   */
  public synchronized 
  void setAuthenticationMethod(AuthenticationMethod authMethod) {
    user.setAuthenticationMethod(authMethod);
  }

  /**
   * Sets the authentication method in the subject
   * 
   * @param authMethod
   */
  public void setAuthenticationMethod(AuthMethod authMethod) {
    user.setAuthenticationMethod(AuthenticationMethod.valueOf(authMethod));
  }

  /**
   * Get the authentication method from the subject
   * 
   * @return AuthenticationMethod in the subject, null if not present.
   */
  public synchronized AuthenticationMethod getAuthenticationMethod() {
    return user.getAuthenticationMethod();
  }

  /**
   * Get the authentication method from the real user's subject.  If there
   * is no real user, return the given user's authentication method.
   * 
   * @return AuthenticationMethod in the subject, null if not present.
   */
  public synchronized AuthenticationMethod getRealAuthenticationMethod() {
    UserGroupInformation ugi = getRealUser();
    if (ugi == null) {
      ugi = this;
    }
    return ugi.getAuthenticationMethod();
  }

  /**
   * Returns the authentication method of a ugi. If the authentication method is
   * PROXY, returns the authentication method of the real user.
   * 
   * @param ugi
   * @return AuthenticationMethod
   */
  public static AuthenticationMethod getRealAuthenticationMethod(
      UserGroupInformation ugi) {
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  /**
   * Compare the subjects to see if they are equal to each other.
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return subject == ((UserGroupInformation) o).subject;
    }
  }

  /**
   * Return the hash of the subject.
   */
  @Override
  public int hashCode() {
    return System.identityHashCode(subject);
  }

  /**
   * Get the underlying subject from this ugi.
   * @return the subject that represents this user.
   */
  protected Subject getSubject() {
    return subject;
  }

  /**
   * Run the given action as the user.
   * @param <T> the return type of the run method
   * @param action the method to execute
   * @return the value from the run method
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public <T> T doAs(PrivilegedAction<T> action) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PrivilegedAction [as: {}][action: {}]", this, action,
          new Exception());
    }
    return Subject.doAs(subject, action);
  }
  
  /**
   * Run the given action as the user, potentially throwing an exception.
   * @param <T> the return type of the run method
   * @param action the method to execute
   * @return the value from the run method
   * @throws IOException if the action throws an IOException
   * @throws Error if the action throws an Error
   * @throws RuntimeException if the action throws a RuntimeException
   * @throws InterruptedException if the action throws an InterruptedException
   * @throws UndeclaredThrowableException if the action throws something else
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public <T> T doAs(PrivilegedExceptionAction<T> action
                    ) throws IOException, InterruptedException {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("PrivilegedAction [as: {}][action: {}]", this, action,
            new Exception());
      }
      return Subject.doAs(subject, action);
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      LOG.debug("PrivilegedActionException as: {}", this, cause);
      if (cause == null) {
        throw new RuntimeException("PrivilegedActionException with no " +
                "underlying cause. UGI [" + this + "]" +": " + pae, pae);
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else {
        throw new UndeclaredThrowableException(cause);
      }
    }
  }

  /**
   * Log current UGI and token information into specified log.
   * @param ugi - UGI
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "KMS"})
  @InterfaceStability.Unstable
  public static void logUserInfo(Logger log, String caption,
      UserGroupInformation ugi) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug(caption + " UGI: " + ugi);
      for (Token<?> token : ugi.getTokens()) {
        log.debug("+token:" + token);
      }
    }
  }

  /**
   * Log all (current, real, login) UGI and token info into specified log.
   * @param ugi - UGI
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "KMS"})
  @InterfaceStability.Unstable
  public static void logAllUserInfo(Logger log, UserGroupInformation ugi) throws
      IOException {
    if (log.isDebugEnabled()) {
      logUserInfo(log, "Current", ugi.getCurrentUser());
      if (ugi.getRealUser() != null) {
        logUserInfo(log, "Real", ugi.getRealUser());
      }
      logUserInfo(log, "Login", ugi.getLoginUser());
    }
  }

  /**
   * Log all (current, real, login) UGI and token info into UGI debug log.
   * @param ugi - UGI
   * @throws IOException
   */
  public static void logAllUserInfo(UserGroupInformation ugi) throws
      IOException {
    logAllUserInfo(LOG, ugi);
  }

  private void print() throws IOException {
    System.out.println("User: " + getUserName());
    System.out.print("Group Ids: ");
    System.out.println();
    String[] groups = getGroupNames();
    System.out.print("Groups: ");
    for(int i=0; i < groups.length; i++) {
      System.out.print(groups[i] + " ");
    }
    System.out.println();    
  }

  /**
   * Login a subject with the given parameters.  If the subject is null,
   * the login context used to create the subject will be attached.
   * @param subject to login, null for new subject.
   * @param params for login, null for externally managed ugi.
   * @return UserGroupInformation for subject
   * @throws IOException
   */
  private static UserGroupInformation doSubjectLogin(
      Subject subject, LoginParams params) throws IOException {
    ensureInitialized();
    // initial default login.
    if (subject == null && params == null) {
      params = LoginParams.getDefaults();
    }
    HadoopConfiguration loginConf = new HadoopConfiguration(params);
    try {
      HadoopLoginContext login = newLoginContext(
        authenticationMethod.getLoginAppName(), subject, loginConf);
      login.login();
      UserGroupInformation ugi = new UserGroupInformation(login.getSubject());
      // attach login context for relogin unless this was a pre-existing
      // subject.
      if (subject == null) {
        params.put(LoginParam.PRINCIPAL, ugi.getUserName());
        ugi.setLogin(login);
        ugi.setLastLogin(Time.now());
      }
      return ugi;
    } catch (LoginException le) {
      KerberosAuthException kae =
        new KerberosAuthException(FAILURE_TO_LOGIN, le);
      if (params != null) {
        kae.setPrincipal(params.get(LoginParam.PRINCIPAL));
        kae.setKeytabFile(params.get(LoginParam.KEYTAB));
        kae.setTicketCacheFile(params.get(LoginParam.CCACHE));
      }
      throw kae;
    }
  }

  // parameters associated with kerberos logins.  may be extended to support
  // additional authentication methods.
  enum LoginParam {
    PRINCIPAL,
    KEYTAB,
    CCACHE,
  }

  // explicitly private to prevent external tampering.
  private static class LoginParams extends EnumMap<LoginParam,String>
      implements Parameters {
    LoginParams() {
      super(LoginParam.class);
    }

    // do not add null values, nor allow existing values to be overriden.
    @Override
    public String put(LoginParam param, String val) {
      boolean add = val != null && !containsKey(param);
      return add ? super.put(param, val) : null;
    }

    static LoginParams getDefaults() {
      LoginParams params = new LoginParams();
      params.put(LoginParam.PRINCIPAL, System.getenv("KRB5PRINCIPAL"));
      params.put(LoginParam.KEYTAB, System.getenv("KRB5KEYTAB"));
      params.put(LoginParam.CCACHE, System.getenv("KRB5CCNAME"));
      return params;
    }
  }

  // wrapper to allow access to fields necessary to recreate the same login
  // context for relogin.  explicitly private to prevent external tampering.
  private static class HadoopLoginContext extends LoginContext {
    private final String appName;
    private final HadoopConfiguration conf;
    private AtomicBoolean isLoggedIn = new AtomicBoolean();

    HadoopLoginContext(String appName, Subject subject,
                       HadoopConfiguration conf) throws LoginException {
      super(appName, subject, null, conf);
      this.appName = appName;
      this.conf = conf;
    }

    String getAppName() {
      return appName;
    }

    HadoopConfiguration getConfiguration() {
      return conf;
    }

    // the locking model for logins cannot rely on ugi instance synchronization
    // since a subject will be referenced by multiple ugi instances.
    Object getSubjectLock() {
      Subject subject = getSubject();
      // if subject is null, the login context will create the subject
      // so just lock on this context.
      return (subject == null) ? this : subject.getPrivateCredentials();
    }

    @Override
    public void login() throws LoginException {
      synchronized(getSubjectLock()) {
        MutableRate metric = metrics.loginFailure;
        long start = Time.monotonicNow();
        try {
          super.login();
          isLoggedIn.set(true);
          metric = metrics.loginSuccess;
        } finally {
          metric.add(Time.monotonicNow() - start);
        }
      }
    }

    @Override
    public void logout() throws LoginException {
      synchronized(getSubjectLock()) {
        if (isLoggedIn.compareAndSet(true, false)) {
          super.logout();
        }
      }
    }
  }

  /**
   * A JAAS configuration that defines the login modules that we want
   * to use for login.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  private static class HadoopConfiguration
  extends javax.security.auth.login.Configuration {
    static final String KRB5_LOGIN_MODULE =
        KerberosUtil.getKrb5LoginModuleName();
    static final String SIMPLE_CONFIG_NAME = "hadoop-simple";
    static final String KERBEROS_CONFIG_NAME = "hadoop-kerberos";

    private static final Map<String, String> BASIC_JAAS_OPTIONS =
        new HashMap<String,String>();
    static {
      if ("true".equalsIgnoreCase(System.getenv("HADOOP_JAAS_DEBUG"))) {
        BASIC_JAAS_OPTIONS.put("debug", "true");
      }
    }

    static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
        new AppConfigurationEntry(
            OS_LOGIN_MODULE_NAME,
            LoginModuleControlFlag.REQUIRED,
            BASIC_JAAS_OPTIONS);

    static final AppConfigurationEntry HADOOP_LOGIN =
        new AppConfigurationEntry(
            HadoopLoginModule.class.getName(),
            LoginModuleControlFlag.REQUIRED,
            BASIC_JAAS_OPTIONS);

    private final LoginParams params;

    HadoopConfiguration(LoginParams params) {
      this.params = params;
    }

    @Override
    public LoginParams getParameters() {
      return params;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      ArrayList<AppConfigurationEntry> entries = new ArrayList<>();
      // login of external subject passes no params.  technically only
      // existing credentials should be used but other components expect
      // the login to succeed with local user fallback if no principal.
      if (params == null || appName.equals(SIMPLE_CONFIG_NAME)) {
        entries.add(OS_SPECIFIC_LOGIN);
      } else if (appName.equals(KERBEROS_CONFIG_NAME)) {
        // existing semantics are the initial default login allows local user
        // fallback. this is not allowed when a principal explicitly
        // specified or during a relogin.
        if (!params.containsKey(LoginParam.PRINCIPAL)) {
          entries.add(OS_SPECIFIC_LOGIN);
        }
        entries.add(getKerberosEntry());
      }
      entries.add(HADOOP_LOGIN);
      return entries.toArray(new AppConfigurationEntry[0]);
    }

    private AppConfigurationEntry getKerberosEntry() {
      final Map<String,String> options = new HashMap<>(BASIC_JAAS_OPTIONS);
      LoginModuleControlFlag controlFlag = LoginModuleControlFlag.OPTIONAL;
      // kerberos login is mandatory if principal is specified.  principal
      // will not be set for initial default login, but will always be set
      // for relogins.
      final String principal = params.get(LoginParam.PRINCIPAL);
      if (principal != null) {
        options.put("principal", principal);
        controlFlag = LoginModuleControlFlag.REQUIRED;
      }

      // use keytab if given else fallback to ticket cache.
      if (IBM_JAVA) {
        if (params.containsKey(LoginParam.KEYTAB)) {
          final String keytab = params.get(LoginParam.KEYTAB);
          if (keytab != null) {
            options.put("useKeytab", prependFileAuthority(keytab));
          } else {
            options.put("useDefaultKeytab", "true");
          }
          options.put("credsType", "both");
        } else {
          String ticketCache = params.get(LoginParam.CCACHE);
          if (ticketCache != null) {
            options.put("useCcache", prependFileAuthority(ticketCache));
          } else {
            options.put("useDefaultCcache", "true");
          }
          options.put("renewTGT", "true");
        }
      } else {
        if (params.containsKey(LoginParam.KEYTAB)) {
          options.put("useKeyTab", "true");
          final String keytab = params.get(LoginParam.KEYTAB);
          if (keytab != null) {
            options.put("keyTab", keytab);
          }
          options.put("storeKey", "true");
        } else {
          options.put("useTicketCache", "true");
          String ticketCache = params.get(LoginParam.CCACHE);
          if (ticketCache != null) {
            options.put("ticketCache", ticketCache);
          }
          options.put("renewTGT", "true");
        }
        options.put("doNotPrompt", "true");
      }
      options.put("refreshKrb5Config", "true");

      return new AppConfigurationEntry(
          KRB5_LOGIN_MODULE, controlFlag, options);
    }

    private static String prependFileAuthority(String keytabPath) {
      return keytabPath.startsWith("file://")
          ? keytabPath
          : "file://" + keytabPath;
    }
  }

  /**
   * A test method to print out the current user's UGI.
   * @param args if there are two arguments, read the user from the keytab
   * and print it out.
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
  System.out.println("Getting UGI for current user");
    UserGroupInformation ugi = getCurrentUser();
    ugi.print();
    System.out.println("UGI: " + ugi);
    System.out.println("Auth method " + ugi.user.getAuthenticationMethod());
    System.out.println("Keytab " + ugi.isFromKeytab());
    System.out.println("============================================================");
    
    if (args.length == 2) {
      System.out.println("Getting UGI from keytab....");
      loginUserFromKeytab(args[0], args[1]);
      getCurrentUser().print();
      System.out.println("Keytab: " + ugi);
      UserGroupInformation loginUgi = getLoginUser();
      System.out.println("Auth method " + loginUgi.getAuthenticationMethod());
      System.out.println("Keytab " + loginUgi.isFromKeytab());
    }
  }
}
