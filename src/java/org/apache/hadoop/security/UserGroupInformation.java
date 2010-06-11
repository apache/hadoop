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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.spi.LoginModule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.sun.security.auth.NTUserPrincipal;
import com.sun.security.auth.UnixPrincipal;
import com.sun.security.auth.module.Krb5LoginModule;

/**
 * User and group information for Hadoop.
 * This class wraps around a JAAS Subject and provides methods to determine the
 * user's username and groups. It supports both the Windows, Unix and Kerberos 
 * login modules.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class UserGroupInformation {
  private static final Log LOG =  LogFactory.getLog(UserGroupInformation.class);
  
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
      Principal user = null;
      // if we are using kerberos, try it out
      if (useKerberos) {
        user = getCanonicalUser(KerberosPrincipal.class);
      }
      // if we don't have a kerberos user, use the OS user
      if (user == null) {
        user = getCanonicalUser(OS_PRINCIPAL_CLASS);
      }
      // if we found the user, add our principal
      if (user != null) {
        subject.getPrincipals().add(new User(user.getName()));
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
      return true;
    }

    @Override
    public boolean logout() throws LoginException {
      return true;
    }
  }

  /** Are the static variables that depend on configuration initialized? */
  private static boolean isInitialized = false;
  /** Should we use Kerberos configuration? */
  private static boolean useKerberos;
  /** Server-side groups fetching service */
  private static Groups groups;
  /** The last authentication time */
  private static long lastUnsuccessfulAuthenticationAttemptTime;
  
  public static final long MIN_TIME_BEFORE_RELOGIN = 10 * 60 * 1000L;
  
  /**Environment variable pointing to the token cache file*/
  public static final String HADOOP_TOKEN_FILE_LOCATION = 
    "HADOOP_TOKEN_FILE_LOCATION";
  
  /** 
   * A method to initialize the fields that depend on a configuration.
   * Must be called before useKerberos or groups is used.
   */
  private static synchronized void ensureInitialized() {
    if (!isInitialized) {
      initialize(new Configuration());
    }
  }

  /**
   * Set the configuration values for UGI.
   * @param conf the configuration to use
   */
  private static synchronized void initialize(Configuration conf) {
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION);
    if (value == null || "simple".equals(value)) {
      useKerberos = false;
    } else if ("kerberos".equals(value)) {
      useKerberos = true;
    } else {
      throw new IllegalArgumentException("Invalid attribute value for " +
                                         HADOOP_SECURITY_AUTHENTICATION + 
                                         " of " + value);
    }
    // If we haven't set up testing groups, use the configuration to find it
    if (!(groups instanceof TestingGroups)) {
      groups = Groups.getUserToGroupsMappingService(conf);
    }
    // Set the configuration for JAAS to be the Hadoop configuration. 
    // This is done here rather than a static initializer to avoid a
    // circular dependence.
    javax.security.auth.login.Configuration.setConfiguration
        (new HadoopConfiguration());
    isInitialized = true;
  }

  /**
   * Set the static configuration for UGI.
   * In particular, set the security authentication mechanism and the
   * group look up service.
   * @param conf the configuration to use
   */
  public static void setConfiguration(Configuration conf) {
    initialize(conf);
  }
  
  /**
   * Determine if UserGroupInformation is using Kerberos to determine
   * user identities or is relying on simple authentication
   * 
   * @return true if UGI is working in a secure environment
   */
  public static boolean isSecurityEnabled() {
    ensureInitialized();
    return useKerberos;
  }
  
  /**
   * Information about the logged in user.
   */
  private static UserGroupInformation loginUser = null;
  private static String keytabPrincipal = null;
  private static String keytabFile = null;

  private final Subject subject;
  
  private static LoginContext login;
  
  private static final String OS_LOGIN_MODULE_NAME;
  private static final Class<? extends Principal> OS_PRINCIPAL_CLASS;
  private static final boolean windows = 
                           System.getProperty("os.name").startsWith("Windows");
  static {
    if (windows) {
      OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.NTLoginModule";
      OS_PRINCIPAL_CLASS = NTUserPrincipal.class;
    } else {
      OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.UnixLoginModule";
      OS_PRINCIPAL_CLASS = UnixPrincipal.class;
    }
  }
  
  private static class RealUser implements Principal {
    private final UserGroupInformation realUser;
    
    RealUser(UserGroupInformation realUser) {
      this.realUser = realUser;
    }
    
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
    
    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
      new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
                                LoginModuleControlFlag.REQUIRED,
                                new HashMap<String,String>());
    private static final AppConfigurationEntry HADOOP_LOGIN =
      new AppConfigurationEntry(HadoopLoginModule.class.getName(),
                                LoginModuleControlFlag.REQUIRED,
                                new HashMap<String,String>());
    private static final Map<String,String> USER_KERBEROS_OPTIONS = 
      new HashMap<String,String>();
    static {
      USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
      USER_KERBEROS_OPTIONS.put("renewTGT", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        USER_KERBEROS_OPTIONS.put("ticketCache", ticketCache);
      }
    }
    private static final AppConfigurationEntry USER_KERBEROS_LOGIN =
      new AppConfigurationEntry(Krb5LoginModule.class.getName(),
                                LoginModuleControlFlag.OPTIONAL,
                                USER_KERBEROS_OPTIONS);
    private static final Map<String,String> KEYTAB_KERBEROS_OPTIONS = 
      new HashMap<String,String>();
    static {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
      KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
    }
    private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
      new AppConfigurationEntry(Krb5LoginModule.class.getName(),
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
        KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
        KEYTAB_KERBEROS_OPTIONS.put("principal", keytabPrincipal);
        return KEYTAB_KERBEROS_CONF;
      }
      return null;
    }
  }

  /**
   * Create a UserGroupInformation for the given subject.
   * This does not change the subject or acquire new credentials.
   * @param subject the user's subject
   */
  UserGroupInformation(Subject subject) {
    this.subject = subject;
  }

  /**
   * Return the current user, including any doAs in the current stack.
   * @return the current user
   * @throws IOException if login fails
   */
  public static UserGroupInformation getCurrentUser() throws IOException {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    return subject == null ? getLoginUser() : new UserGroupInformation(subject);
  }

  /**
   * Get the currently logged in user.
   * @return the logged in user
   * @throws IOException if login fails
   */
  public synchronized 
  static UserGroupInformation getLoginUser() throws IOException {
    if (loginUser == null) {
      try {
        if (isSecurityEnabled()) {
          login = new LoginContext(HadoopConfiguration.USER_KERBEROS_CONFIG_NAME);
        } else {
          login = new LoginContext(HadoopConfiguration.SIMPLE_CONFIG_NAME);
        }
        login.login();
        loginUser = new UserGroupInformation(login.getSubject());
        String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
        if (tokenFile != null && isSecurityEnabled()) {
          TokenStorage.readTokensAndLoadInUGI(tokenFile, new Configuration(), loginUser);
        }
      } catch (LoginException le) {
        throw new IOException("failure to login", le);
      }
    }
    return loginUser;
  }

  /**
   * Log a user in from a keytab file. Loads a user identity from a keytab
   * file and login them in. They become the currently logged-in user.
   * @param user the principal name to load from the keytab
   * @param path the path to the keytab file
   * @throws IOException if the keytab file can't be read
   */
  public synchronized
  static void loginUserFromKeytab(String user,
                                  String path
                                  ) throws IOException {
    if (!isSecurityEnabled())
      return;

    keytabFile = path;
    keytabPrincipal = user;
    try {
      login = 
        new LoginContext(HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME);
      login.login();
      loginUser = new UserGroupInformation(login.getSubject());
    } catch (LoginException le) {
      throw new IOException("Login failure for " + user + " from keytab " + 
                            path, le);
    }
  }
  
  /**
   * Re-Login a user in from a keytab file. Loads a user identity from a keytab
   * file and login them in. They become the currently logged-in user. This
   * method assumes that {@link #loginUserFromKeytab(String, String)} had 
   * happened already.
   * The Subject field of this UserGroupInformation object is updated to have
   * the new credentials.
   * @throws IOException on a failure
   */
  public synchronized void reloginFromKeytab()
  throws IOException {
    if (!isSecurityEnabled())
      return;
    if (login == null || keytabFile == null) {
      throw new IOException("loginUserFromKeyTab must be done first");
    }
    if (System.currentTimeMillis() -lastUnsuccessfulAuthenticationAttemptTime <
          MIN_TIME_BEFORE_RELOGIN) {
      LOG.warn("Not attempting to re-login since the last re-login was " +
          "attempted less than " + (MIN_TIME_BEFORE_RELOGIN/1000) + " seconds"+
          " before.");
      return;
    }
    try {
      LOG.info("Initiating logout for " + getUserName());
      //clear up the kerberos state. But the tokens are not cleared! As per 
      //the Java kerberos login module code, only the kerberos credentials
      //are cleared
      login.logout();
      //login and also update the subject field of this instance to 
      //have the new credentials (pass it to the LoginContext constructor)
      login = 
        new LoginContext(HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME, 
            getSubject());
      LOG.info("Initiating re-login for " + keytabPrincipal);
      login.login();
    } catch (LoginException le) {
      throw new IOException("Login failure for " + keytabPrincipal + 
          " from keytab " + keytabFile, le);
    } 
  }

  public synchronized static void 
    setLastUnsuccessfulAuthenticationAttemptTime(long time) {
    lastUnsuccessfulAuthenticationAttemptTime = time;
  }
  
  public synchronized static boolean isLoginKeytabBased() {
    return keytabFile != null;
  }
  /**
   * Create a user from a login name. It is intended to be used for remote
   * users in RPC, since it won't have any credentials.
   * @param user the full user principal name, must not be empty or null
   * @return the UserGroupInformation for the remote user.
   */
  public static UserGroupInformation createRemoteUser(String user) {
    if (user == null || "".equals(user)) {
      throw new IllegalArgumentException("Null user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return new UserGroupInformation(subject);
  }

  @InterfaceStability.Evolving
  public static enum AuthenticationMethod {
    SIMPLE,
    KERBEROS,
    TOKEN,
    CERTIFICATE,
    KERBEROS_SSL,
    PROXY;
  }

  /* Create a proxy user using username of the effective user and the ugi of the
   * real user.
   *
   * @param effective
   *          user, UGI for real user.
   * @return
   */
  public static UserGroupInformation createProxyUser(String user,
      UserGroupInformation realUser) {
    if (user == null || "".equals(user)) {
      throw new IllegalArgumentException("Null user");
    }
    if (realUser == null) {
      throw new IllegalArgumentException("Null real user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    subject.getPrincipals().add(new RealUser(realUser));
    return new UserGroupInformation(subject);
  }

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
    
    private TestingGroups() {
      super(new org.apache.hadoop.conf.Configuration());
    }
    
    @Override
    public List<String> getGroups(String user) {
      List<String> result = userToGroupsMapping.get(user);
      if (result == null) {
        result = new ArrayList<String>();
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
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public static UserGroupInformation createUserForTesting(String user, 
                                                          String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createRemoteUser(user);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups();
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
  @InterfaceAudience.LimitedPrivate( { "HDFS", "MapReduce" })
  public static UserGroupInformation createProxyUserForTesting(String user,
      UserGroupInformation realUser, String[] userGroups) {
    ensureInitialized();
    UserGroupInformation ugi = createProxyUser(user, realUser);
    // make sure that the testing object is setup
    if (!(groups instanceof TestingGroups)) {
      groups = new TestingGroups();
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

  /**
   * Get the user's full principal name.
   * @return the user's full principal name.
   */
  public String getUserName() {
    for (User p: subject.getPrincipals(User.class)) {
      return p.getName();
    }
    return null;
  }

  /**
   * Add a token to this UGI
   * 
   * @param token Token to be added
   * @return true on successful add of new token
   */
  public synchronized boolean addToken(Token<? extends TokenIdentifier> token) {
    return subject.getPrivateCredentials().add(token);
  }
  
  /**
   * Obtain the collection of tokens associated with this user.
   * 
   * @return an unmodifiable collection of tokens associated with user
   */
  public synchronized
  Collection<Token<? extends TokenIdentifier>> getTokens() {
    Set<Object> creds = subject.getPrivateCredentials();
    List<Token<?>> result = new ArrayList<Token<?>>(creds.size());
    for(Object o: creds) {
      if (o instanceof Token<?>) {
        result.add((Token<?>) o);
      }
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Get the group names for this user.
   * @return the list of users with the primary group first. If the command
   *    fails, it returns an empty list.
   */
  public synchronized String[] getGroupNames() {
    ensureInitialized();
    try {
      List<String> result = groups.getGroups(getShortUserName());
      return result.toArray(new String[result.size()]);
    } catch (IOException ie) {
      LOG.warn("No groups available for user " + getShortUserName());
      return new String[0];
    }
  }
  
  /**
   * Return the username.
   */
  @Override
  public String toString() {
    if (getRealUser() != null) {
      return getUserName() + " via " +  getRealUser().toString();
    } else {
      return getUserName();
    }
  }

  /**
   * Sets the authentication method in the subject
   * 
   * @param authMethod
   */
  public synchronized 
  void setAuthenticationMethod(AuthenticationMethod authMethod) {
    for (User p : subject.getPrincipals(User.class)) {
      p.setAuthenticationMethod(authMethod);
    }
  }

  /**
   * Get the authentication method from the subject
   * 
   * @return AuthenticationMethod in the subject, null if not present.
   */
  public synchronized AuthenticationMethod getAuthenticationMethod() {
    for (User p: subject.getPrincipals(User.class)) {
      return p.getAuthenticationMethod();
    }
    return null;
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
      return subject.equals(((UserGroupInformation) o).subject);
    }
  }

  /**
   * Return the hash of the subject.
   */
  @Override
  public int hashCode() {
    return subject.hashCode();
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
  public <T> T doAs(PrivilegedAction<T> action) {
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
  public <T> T doAs(PrivilegedExceptionAction<T> action
                    ) throws IOException, InterruptedException {
    try {
      return Subject.doAs(subject, action);
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else {
        throw new UndeclaredThrowableException(pae,"Unknown exception in doAs");
      }
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
    System.out.println("============================================================");
    
    if (args.length == 2) {
      System.out.println("Getting UGI from keytab....");
      loginUserFromKeytab(args[0], args[1]);
      getCurrentUser().print();
      System.out.println("Keytab: " + ugi);
    }
  }

}
