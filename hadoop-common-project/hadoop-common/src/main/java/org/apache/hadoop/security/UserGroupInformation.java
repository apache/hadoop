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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_TOKEN_FILES;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User and group information for Hadoop.
 * This class wraps around a JAAS Subject and provides methods to determine the
 * user's username and groups. It supports both the Windows, Unix and Kerberos 
 * login modules.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "HBase", "Hive", "Oozie"})
@InterfaceStability.Evolving
public class UserGroupInformation {
  private static final Logger LOG = LoggerFactory.getLogger(
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("hadoop login commit");
      }
      // if we already have a user, we are done.
      if (!subject.getPrincipals(User.class).isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("using existing subject:"+subject.getPrincipals());
        }
        return true;
      }
      Principal user = null;
      // if we are using kerberos, try it out
      if (isAuthenticationMethodEnabled(AuthenticationMethod.KERBEROS)) {
        user = getCanonicalUser(KerberosPrincipal.class);
        if (LOG.isDebugEnabled()) {
          LOG.debug("using kerberos user:"+user);
        }
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("using local user:"+user);
        }
      }
      // if we found the user, add our principal
      if (user != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using user: \"" + user + "\" with name " + user.getName());
        }

        User userEntry = null;
        try {
          userEntry = new User(user.getName());
        } catch (Exception e) {
          throw (LoginException)(new LoginException(e.toString()).initCause(e));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("User entry: \"" + userEntry.toString() + "\"" );
        }

        subject.getPrincipals().add(userEntry);
        return true;
      }
      LOG.error("Can't find user in " + subject);
      throw new LoginException("Can't find user name");
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
      this.subject = subject;
    }

    @Override
    public boolean login() throws LoginException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("hadoop login");
      }
      return true;
    }

    @Override
    public boolean logout() throws LoginException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("hadoop logout");
      }
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
  /** The configuration to use */
  private static Configuration conf;

  
  /**Environment variable pointing to the token cache file*/
  public static final String HADOOP_TOKEN_FILE_LOCATION = 
    "HADOOP_TOKEN_FILE_LOCATION";
  
  /** 
   * A method to initialize the fields that depend on a configuration.
   * Must be called before useKerberos or groups is used.
   */
  private static void ensureInitialized() {
    if (conf == null) {
      synchronized(UserGroupInformation.class) {
        if (conf == null) { // someone might have beat us
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
  static void reset() {
    authenticationMethod = null;
    conf = null;
    groups = null;
    kerberosMinSecondsBeforeRelogin = 0;
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
  
  /**
   * Information about the logged in user.
   */
  private static UserGroupInformation loginUser = null;
  private static String keytabPrincipal = null;
  private static String keytabFile = null;

  private final Subject subject;
  // All non-static fields must be read-only caches that come from the subject.
  private final User user;
  private final boolean isKeytab;
  private final boolean isKrbTkt;
  
  private static String OS_LOGIN_MODULE_NAME;
  private static Class<? extends Principal> OS_PRINCIPAL_CLASS;
  
  private static final boolean windows =
      System.getProperty("os.name").startsWith("Windows");
  private static final boolean is64Bit =
      System.getProperty("os.arch").contains("64") ||
      System.getProperty("os.arch").contains("s390x");
  private static final boolean aix = System.getProperty("os.name").equals("AIX");

  /* Return the OS login module class name */
  private static String getOSLoginModuleName() {
    if (IBM_JAVA) {
      if (windows) {
        return is64Bit ? "com.ibm.security.auth.module.Win64LoginModule"
            : "com.ibm.security.auth.module.NTLoginModule";
      } else if (aix) {
        return is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule"
            : "com.ibm.security.auth.module.AIXLoginModule";
      } else {
        return "com.ibm.security.auth.module.LinuxLoginModule";
      }
    } else {
      return windows ? "com.sun.security.auth.module.NTLoginModule"
        : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  /* Return the OS principal class */
  @SuppressWarnings("unchecked")
  private static Class<? extends Principal> getOsPrincipalClass() {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try {
      String principalClass = null;
      if (IBM_JAVA) {
        if (is64Bit) {
          principalClass = "com.ibm.security.auth.UsernamePrincipal";
        } else {
          if (windows) {
            principalClass = "com.ibm.security.auth.NTUserPrincipal";
          } else if (aix) {
            principalClass = "com.ibm.security.auth.AIXPrincipal";
          } else {
            principalClass = "com.ibm.security.auth.LinuxPrincipal";
          }
        }
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
  
  /**
   * A JAAS configuration that defines the login modules that we want
   * to use for login.
   */
  private static class HadoopConfiguration 
      extends javax.security.auth.login.Configuration {
    private static final String SIMPLE_CONFIG_NAME = "hadoop-simple";
    private static final String USER_KERBEROS_CONFIG_NAME = 
      "hadoop-user-kerberos";
    private static final String KEYTAB_KERBEROS_CONFIG_NAME = 
      "hadoop-keytab-kerberos";

    private static final Map<String, String> BASIC_JAAS_OPTIONS =
      new HashMap<String,String>();
    static {
      String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
      if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
        BASIC_JAAS_OPTIONS.put("debug", "true");
      }
    }
    
    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
      new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
                                LoginModuleControlFlag.REQUIRED,
                                BASIC_JAAS_OPTIONS);
    private static final AppConfigurationEntry HADOOP_LOGIN =
      new AppConfigurationEntry(HadoopLoginModule.class.getName(),
                                LoginModuleControlFlag.REQUIRED,
                                BASIC_JAAS_OPTIONS);
    private static final Map<String,String> USER_KERBEROS_OPTIONS = 
      new HashMap<String,String>();
    static {
      if (IBM_JAVA) {
        USER_KERBEROS_OPTIONS.put("useDefaultCcache", "true");
      } else {
        USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
        USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
      }
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        if (IBM_JAVA) {
          // The first value searched when "useDefaultCcache" is used.
          System.setProperty("KRB5CCNAME", ticketCache);
        } else {
          USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
        }
      }
      USER_KERBEROS_OPTIONS.put("renewTGT", "true");
      USER_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
    }
    private static final AppConfigurationEntry USER_KERBEROS_LOGIN =
      new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                                LoginModuleControlFlag.OPTIONAL,
                                USER_KERBEROS_OPTIONS);
    private static final Map<String,String> KEYTAB_KERBEROS_OPTIONS = 
      new HashMap<String,String>();
    static {
      if (IBM_JAVA) {
        KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
      } else {
        KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
        KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
        KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
      }
      KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
      KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);      
    }
    private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
      new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                                LoginModuleControlFlag.REQUIRED,
                                KEYTAB_KERBEROS_OPTIONS);
    
    private static final AppConfigurationEntry[] SIMPLE_CONF = 
      new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, HADOOP_LOGIN};
    
    private static final AppConfigurationEntry[] USER_KERBEROS_CONF =
      new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN,
                                  HADOOP_LOGIN};

    private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
      new AppConfigurationEntry[]{KEYTAB_KERBEROS_LOGIN, HADOOP_LOGIN};

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (SIMPLE_CONFIG_NAME.equals(appName)) {
        return SIMPLE_CONF;
      } else if (USER_KERBEROS_CONFIG_NAME.equals(appName)) {
        return USER_KERBEROS_CONF;
      } else if (KEYTAB_KERBEROS_CONFIG_NAME.equals(appName)) {
        if (IBM_JAVA) {
          KEYTAB_KERBEROS_OPTIONS.put("useKeytab",
              prependFileAuthority(keytabFile));
        } else {
          KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
        }
        KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
        return KEYTAB_KERBEROS_CONF;
      }
      return null;
    }
  }

  private static String prependFileAuthority(String keytabPath) {
    return keytabPath.startsWith("file://") ? keytabPath
        : "file://" + keytabPath;
  }

  /**
   * Represents a javax.security configuration that is created at runtime.
   */
  private static class DynamicConfiguration
      extends javax.security.auth.login.Configuration {
    private AppConfigurationEntry[] ace;
    
    DynamicConfiguration(AppConfigurationEntry[] ace) {
      this.ace = ace;
    }
    
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      return ace;
    }
  }

  private static LoginContext
  newLoginContext(String appName, Subject subject,
    javax.security.auth.login.Configuration loginConf)
      throws LoginException {
    // Temporarily switch the thread's ContextClassLoader to match this
    // class's classloader, so that we can properly load HadoopLoginModule
    // from the JAAS libraries.
    Thread t = Thread.currentThread();
    ClassLoader oldCCL = t.getContextClassLoader();
    t.setContextClassLoader(HadoopLoginModule.class.getClassLoader());
    try {
      return new LoginContext(appName, subject, null, loginConf);
    } finally {
      t.setContextClassLoader(oldCCL);
    }
  }

  private LoginContext getLogin() {
    return user.getLogin();
  }
  
  private void setLogin(LoginContext login) {
    user.setLogin(login);
  }

  /**
   * Create a UserGroupInformation for the given subject.
   * This does not change the subject or acquire new credentials.
   * @param subject the user's subject
   */
  UserGroupInformation(Subject subject) {
    this.subject = subject;
    this.user = subject.getPrincipals(User.class).iterator().next();
    this.isKeytab = KerberosUtil.hasKerberosKeyTab(subject);
    this.isKrbTkt = KerberosUtil.hasKerberosTicket(subject);
  }
  
  /**
   * checks if logged in using kerberos
   * @return true if the subject logged via keytab or has a Kerberos TGT
   */
  public boolean hasKerberosCredentials() {
    return isKeytab || isKrbTkt;
  }

  /**
   * Return the current user, including any doAs in the current stack.
   * @return the current user
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized
  static UserGroupInformation getCurrentUser() throws IOException {
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
   * @param ticketCachePath     the path to the ticket cache file
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
    try {
      Map<String,String> krbOptions = new HashMap<String,String>();
      if (IBM_JAVA) {
        krbOptions.put("useDefaultCcache", "true");
        // The first value searched when "useDefaultCcache" is used.
        System.setProperty("KRB5CCNAME", ticketCache);
      } else {
        krbOptions.put("doNotPrompt", "true");
        krbOptions.put("useTicketCache", "true");
        krbOptions.put("useKeyTab", "false");
        krbOptions.put("ticketCache", ticketCache);
      }
      krbOptions.put("renewTGT", "false");
      krbOptions.putAll(HadoopConfiguration.BASIC_JAAS_OPTIONS);
      AppConfigurationEntry ace = new AppConfigurationEntry(
          KerberosUtil.getKrb5LoginModuleName(),
          LoginModuleControlFlag.REQUIRED,
          krbOptions);
      DynamicConfiguration dynConf =
          new DynamicConfiguration(new AppConfigurationEntry[]{ ace });
      LoginContext login = newLoginContext(
          HadoopConfiguration.USER_KERBEROS_CONFIG_NAME, null, dynConf);
      login.login();

      Subject loginSubject = login.getSubject();
      Set<Principal> loginPrincipals = loginSubject.getPrincipals();
      if (loginPrincipals.isEmpty()) {
        throw new RuntimeException("No login principals found!");
      }
      if (loginPrincipals.size() != 1) {
        LOG.warn("found more than one principal in the ticket cache file " +
          ticketCache);
      }
      User ugiUser = new User(loginPrincipals.iterator().next().getName(),
          AuthenticationMethod.KERBEROS, login);
      loginSubject.getPrincipals().add(ugiUser);
      UserGroupInformation ugi = new UserGroupInformation(loginSubject);
      ugi.setLogin(login);
      ugi.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
      return ugi;
    } catch (LoginException le) {
      throw new IOException("failure to login using ticket cache file " +
          ticketCache, le);
    }
  }

  /**
   * Create a UserGroupInformation from a Subject with Kerberos principal.
   *
   * @param user                The KerberosPrincipal to use in UGI
   *
   * @throws IOException        if the kerberos login fails
   */
  public static UserGroupInformation getUGIFromSubject(Subject subject)
      throws IOException {
    if (subject == null) {
      throw new IOException("Subject must not be null");
    }

    if (subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
      throw new IOException("Provided Subject must contain a KerberosPrincipal");
    }

    KerberosPrincipal principal =
        subject.getPrincipals(KerberosPrincipal.class).iterator().next();

    User ugiUser = new User(principal.getName(),
        AuthenticationMethod.KERBEROS, null);
    subject.getPrincipals().add(ugiUser);
    UserGroupInformation ugi = new UserGroupInformation(subject);
    ugi.setLogin(null);
    ugi.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    return ugi;
  }

  /**
   * Get the currently logged in user.
   * @return the logged in user
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized 
  static UserGroupInformation getLoginUser() throws IOException {
    if (loginUser == null) {
      loginUserFromSubject(null);
    }
    return loginUser;
  }

  /**
   * remove the login method that is followed by a space from the username
   * e.g. "jack (auth:SIMPLE)" -> "jack"
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
   * @parma subject the subject to use when logging in a user, or null to 
   * create a new subject.
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized 
  static void loginUserFromSubject(Subject subject) throws IOException {
    ensureInitialized();
    try {
      if (subject == null) {
        subject = new Subject();
      }
      LoginContext login =
          newLoginContext(authenticationMethod.getLoginAppName(), 
                          subject, new HadoopConfiguration());
      login.login();
      UserGroupInformation realUser = new UserGroupInformation(subject);
      realUser.setLogin(login);
      realUser.setAuthenticationMethod(authenticationMethod);
      realUser = new UserGroupInformation(login.getSubject());
      // If the HADOOP_PROXY_USER environment variable or property
      // is specified, create a proxy user as the logged in user.
      String proxyUser = System.getenv(HADOOP_PROXY_USER);
      if (proxyUser == null) {
        proxyUser = System.getProperty(HADOOP_PROXY_USER);
      }
      loginUser = proxyUser == null ? realUser : createProxyUser(proxyUser, realUser);

      String tokenFileLocation = System.getProperty(HADOOP_TOKEN_FILES);
      if (tokenFileLocation == null) {
        tokenFileLocation = conf.get(HADOOP_TOKEN_FILES);
      }
      if (tokenFileLocation != null) {
        for (String tokenFileName:
             StringUtils.getTrimmedStrings(tokenFileLocation)) {
          if (tokenFileName.length() > 0) {
            File tokenFile = new File(tokenFileName);
            if (tokenFile.exists() && tokenFile.isFile()) {
              Credentials cred = Credentials.readTokenStorageFile(
                  tokenFile, conf);
              loginUser.addCredentials(cred);
            } else {
              LOG.info("tokenFile("+tokenFileName+") does not exist");
            }
          }
        }
      }

      String fileLocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
      if (fileLocation != null) {
        // Load the token storage file and put all of the tokens into the
        // user. Don't use the FileSystem API for reading since it has a lock
        // cycle (HADOOP-9212).
        File source = new File(fileLocation);
        LOG.debug("Reading credentials from location set in {}: {}",
            HADOOP_TOKEN_FILE_LOCATION,
            source.getCanonicalPath());
        if (!source.isFile()) {
          throw new FileNotFoundException("Source file "
              + source.getCanonicalPath() + " from "
              + HADOOP_TOKEN_FILE_LOCATION
              + " not found");
        }
        Credentials cred = Credentials.readTokenStorageFile(
            source, conf);
        LOG.debug("Loaded {} tokens", cred.numberOfTokens());
        loginUser.addCredentials(cred);
      }
      loginUser.spawnAutoRenewalThreadForUserCreds();
    } catch (LoginException le) {
      LOG.debug("failure to login", le);
      throw new IOException("failure to login: " + le, le);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("UGI loginUser:"+loginUser);
    } 
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  public synchronized static void setLoginUser(UserGroupInformation ugi) {
    // if this is to become stable, should probably logout the currently
    // logged in ugi if it's different
    loginUser = ugi;
  }
  
  /**
   * Is this user logged in from a keytab file?
   * @return true if the credentials are from a keytab file.
   */
  public boolean isFromKeytab() {
    return isKeytab;
  }
  
  /**
   * Get the Kerberos TGT
   * @return the user's TGT or null if none was found
   */
  private synchronized KerberosTicket getTGT() {
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

  /**Spawn a thread to do periodic renewals of kerberos credentials*/
  private void spawnAutoRenewalThreadForUserCreds() {
    if (isSecurityEnabled()) {
      //spawn thread only if we have kerb credentials
      if (user.getAuthenticationMethod() == AuthenticationMethod.KERBEROS &&
          !isKeytab) {
        Thread t = new Thread(new Runnable() {
          
          @Override
          public void run() {
            String cmd = conf.get("hadoop.kerberos.kinit.command",
                                  "kinit");
            KerberosTicket tgt = getTGT();
            if (tgt == null) {
              return;
            }
            long nextRefresh = getRefreshTime(tgt);
            while (true) {
              try {
                long now = Time.now();
                if(LOG.isDebugEnabled()) {
                  LOG.debug("Current time is " + now);
                  LOG.debug("Next refresh is " + nextRefresh);
                }
                if (now < nextRefresh) {
                  Thread.sleep(nextRefresh - now);
                }
                Shell.execCommand(cmd, "-R");
                if(LOG.isDebugEnabled()) {
                  LOG.debug("renewed ticket");
                }
                reloginFromTicketCache();
                tgt = getTGT();
                if (tgt == null) {
                  LOG.warn("No TGT after renewal. Aborting renew thread for " +
                           getUserName());
                  return;
                }
                nextRefresh = Math.max(getRefreshTime(tgt),
                                       now + kerberosMinSecondsBeforeRelogin);
              } catch (InterruptedException ie) {
                LOG.warn("Terminating renewal thread");
                return;
              } catch (IOException ie) {
                LOG.warn("Exception encountered while running the" +
                    " renewal command. Aborting renew thread. " + ie);
                return;
              }
            }
          }
        });
        t.setDaemon(true);
        t.setName("TGT Renewer for " + getUserName());
        t.start();
      }
    }
  }
  /**
   * Log a user in from a keytab file. Loads a user identity from a keytab
   * file and logs them in. They become the currently logged-in user.
   * @param user the principal name to load from the keytab
   * @param path the path to the keytab file
   * @throws IOException if the keytab file can't be read
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized
  static void loginUserFromKeytab(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return;

    keytabFile = path;
    keytabPrincipal = user;
    Subject subject = new Subject();
    LoginContext login; 
    long start = 0;
    try {
      login = newLoginContext(HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME,
            subject, new HadoopConfiguration());
      start = Time.now();
      login.login();
      metrics.loginSuccess.add(Time.now() - start);
      loginUser = new UserGroupInformation(subject);
      loginUser.setLogin(login);
      loginUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
    } catch (LoginException le) {
      if (start > 0) {
        metrics.loginFailure.add(Time.now() - start);
      }
      throw new IOException("Login failure for " + user + " from keytab " + 
                            path+ ": " + le, le);
    }
    LOG.info("Login successful for user " + keytabPrincipal
        + " using keytab file " + keytabFile);
  }

  /**
   * Log the current user out who previously logged in using keytab.
   * This method assumes that the user logged in by calling
   * {@link #loginUserFromKeytab(String, String)}.
   *
   * @throws IOException if a failure occurred in logout, or if the user did
   * not log in by invoking loginUserFromKeyTab() before.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public void logoutUserFromKeytab() throws IOException {
    if (!isSecurityEnabled() ||
        user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS) {
      return;
    }
    LoginContext login = getLogin();
    if (login == null || keytabFile == null) {
      throw new IOException("loginUserFromKeytab must be done first");
    }

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating logout for " + getUserName());
      }
      synchronized (UserGroupInformation.class) {
        login.logout();
      }
    } catch (LoginException le) {
      throw new IOException("Logout failure for " + user + " from keytab " +
          keytabFile + ": " + le,
          le);
    }

    LOG.info("Logout successful for user " + keytabPrincipal
        + " using keytab file " + keytabFile);
  }
  
  /**
   * Re-login a user from keytab if TGT is expired or is close to expiry.
   * 
   * @throws IOException
   */
  public synchronized void checkTGTAndReloginFromKeytab() throws IOException {
    if (!isSecurityEnabled()
        || user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS
        || !isKeytab)
      return;
    KerberosTicket tgt = getTGT();
    if (tgt != null && !shouldRenewImmediatelyForTests &&
        Time.now() < getRefreshTime(tgt)) {
      return;
    }
    reloginFromKeytab();
  }

  /**
   * Re-Login a user in from a keytab file. Loads a user identity from a keytab
   * file and logs them in. They become the currently logged-in user. This
   * method assumes that {@link #loginUserFromKeytab(String, String)} had 
   * happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized void reloginFromKeytab()
  throws IOException {
    if (!isSecurityEnabled() ||
         user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS ||
         !isKeytab)
      return;
    
    long now = Time.now();
    if (!shouldRenewImmediatelyForTests && !hasSufficientTimeElapsed(now)) {
      return;
    }

    KerberosTicket tgt = getTGT();
    //Return if TGT is valid and is not going to expire soon.
    if (tgt != null && !shouldRenewImmediatelyForTests &&
        now < getRefreshTime(tgt)) {
      return;
    }
    
    LoginContext login = getLogin();
    if (login == null || keytabFile == null) {
      throw new IOException("loginUserFromKeyTab must be done first");
    }
    
    long start = 0;
    // register most recent relogin attempt
    user.setLastLogin(now);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating logout for " + getUserName());
      }
      synchronized (UserGroupInformation.class) {
        // clear up the kerberos state. But the tokens are not cleared! As per
        // the Java kerberos login module code, only the kerberos credentials
        // are cleared
        login.logout();
        // login and also update the subject field of this instance to
        // have the new credentials (pass it to the LoginContext constructor)
        login = newLoginContext(
            HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME, getSubject(),
            new HadoopConfiguration());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initiating re-login for " + keytabPrincipal);
        }
        start = Time.now();
        login.login();
        metrics.loginSuccess.add(Time.now() - start);
        setLogin(login);
      }
    } catch (LoginException le) {
      if (start > 0) {
        metrics.loginFailure.add(Time.now() - start);
      }
      throw new IOException("Login failure for " + keytabPrincipal + 
          " from keytab " + keytabFile + ": " + le, le);
    } 
  }

  /**
   * Re-Login a user in from the ticket cache.  This
   * method assumes that login had happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException on a failure
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public synchronized void reloginFromTicketCache()
  throws IOException {
    if (!isSecurityEnabled() || 
        user.getAuthenticationMethod() != AuthenticationMethod.KERBEROS ||
        !isKrbTkt)
      return;
    LoginContext login = getLogin();
    if (login == null) {
      throw new IOException("login must be done first");
    }
    long now = Time.now();
    if (!hasSufficientTimeElapsed(now)) {
      return;
    }
    // register most recent relogin attempt
    user.setLastLogin(now);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating logout for " + getUserName());
      }
      //clear up the kerberos state. But the tokens are not cleared! As per 
      //the Java kerberos login module code, only the kerberos credentials
      //are cleared
      login.logout();
      //login and also update the subject field of this instance to 
      //have the new credentials (pass it to the LoginContext constructor)
      login = 
        newLoginContext(HadoopConfiguration.USER_KERBEROS_CONFIG_NAME, 
            getSubject(), new HadoopConfiguration());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initiating re-login for " + getUserName());
      }
      login.login();
      setLogin(login);
    } catch (LoginException le) {
      throw new IOException("Login failure for " + getUserName() + ": " + le,
          le);
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
  public synchronized
  static UserGroupInformation loginUserFromKeytabAndReturnUGI(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return UserGroupInformation.getCurrentUser();
    String oldKeytabFile = null;
    String oldKeytabPrincipal = null;

    long start = 0;
    try {
      oldKeytabFile = keytabFile;
      oldKeytabPrincipal = keytabPrincipal;
      keytabFile = path;
      keytabPrincipal = user;
      Subject subject = new Subject();
      
      LoginContext login = newLoginContext(
          HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME, subject,
          new HadoopConfiguration());
       
      start = Time.now();
      login.login();
      metrics.loginSuccess.add(Time.now() - start);
      UserGroupInformation newLoginUser = new UserGroupInformation(subject);
      newLoginUser.setLogin(login);
      newLoginUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
      
      return newLoginUser;
    } catch (LoginException le) {
      if (start > 0) {
        metrics.loginFailure.add(Time.now() - start);
      }
      throw new IOException("Login failure for " + user + " from keytab " + 
                            path + ": " + le, le);
    } finally {
      if(oldKeytabFile != null) keytabFile = oldKeytabFile;
      if(oldKeytabPrincipal != null) keytabPrincipal = oldKeytabPrincipal;
    }
  }

  private boolean hasSufficientTimeElapsed(long now) {
    if (now - user.getLastLogin() < kerberosMinSecondsBeforeRelogin ) {
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
  public synchronized static boolean isLoginKeytabBased() throws IOException {
    return getLoginUser().isKeytab;
  }

  /**
   * Did the login happen via ticket cache
   * @return true or false
   */
  public static boolean isLoginTicketBased()  throws IOException {
    return getLoginUser().isKrbTkt;
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
  public static enum AuthenticationMethod {
    // currently we support only one auth per method, but eventually a 
    // subtype is needed to differentiate, ex. if digest is token or ldap
    SIMPLE(AuthMethod.SIMPLE,
        HadoopConfiguration.SIMPLE_CONFIG_NAME),
    KERBEROS(AuthMethod.KERBEROS,
        HadoopConfiguration.USER_KERBEROS_CONFIG_NAME),
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
    principals.add(new User(user));
    principals.add(new RealUser(realUser));
    UserGroupInformation result =new UserGroupInformation(subject);
    result.setAuthenticationMethod(AuthenticationMethod.PROXY);
    return result;
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
    for (User p: subject.getPrincipals(User.class)) {
      return p.getShortName();
    }
    return null;
  }

  public String getPrimaryGroupName() throws IOException {
    String[] groups = getGroupNames();
    if (groups.length == 0) {
      throw new IOException("There is no primary group for UGI " + this);
    }
    return groups[0];
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
        if (iter.next() instanceof Token.PrivateToken) {
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
   * Get the group names for this user.
   * @return the list of users with the primary group first. If the command
   *    fails, it returns an empty list.
   */
  public synchronized String[] getGroupNames() {
    ensureInitialized();
    try {
      Set<String> result = new LinkedHashSet<String>
        (groups.getGroups(getShortUserName()));
      return result.toArray(new String[result.size()]);
    } catch (IOException ie) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to get groups for user " + getShortUserName()
            + " by " + ie);
        LOG.trace("TRACE", ie);
      }
      return StringUtils.emptyStringArray;
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
    logPrivilegedAction(subject, action);
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
      logPrivilegedAction(subject, action);
      return Subject.doAs(subject, action);
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      if (LOG.isDebugEnabled()) {
        LOG.debug("PrivilegedActionException as:" + this + " cause:" + cause);
      }
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

  private void logPrivilegedAction(Subject subject, Object action) {
    if (LOG.isDebugEnabled()) {
      // would be nice if action included a descriptive toString()
      String where = new Throwable().getStackTrace()[2].toString();
      LOG.debug("PrivilegedAction as:"+this+" from:"+where);
    }
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
    System.out.println("Keytab " + ugi.isKeytab);
    System.out.println("============================================================");
    
    if (args.length == 2) {
      System.out.println("Getting UGI from keytab....");
      loginUserFromKeytab(args[0], args[1]);
      getCurrentUser().print();
      System.out.println("Keytab: " + ugi);
      System.out.println("Auth method " + loginUser.user.getAuthenticationMethod());
      System.out.println("Keytab " + loginUser.isKeytab);
    }
  }
}
