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

package org.apache.hadoop.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.hadoop.security.UserGroupInformation.*;
import static org.apache.hadoop.security.authentication.util.KerberosUtil.*;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;

/**
 * Kerberos diagnostics
 * At some point this may move to hadoop core, so please keep use of slider
 * methods and classes to ~0.
 *
 * This operation expands some of the diagnostic output of the security code,
 * but not all. For completeness
 *
 * Set the environment variable {@code HADOOP_JAAS_DEBUG=true}
 * Set the log level for {@code org.apache.hadoop.security=DEBUG}
 */
public class KerberosDiags implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosDiags.class);
  public static final String KRB5_CCNAME = "KRB5CCNAME";
  public static final String JAVA_SECURITY_KRB5_CONF
    = "java.security.krb5.conf";
  public static final String JAVA_SECURITY_KRB5_REALM
    = "java.security.krb5.realm";
  public static final String SUN_SECURITY_KRB5_DEBUG
    = "sun.security.krb5.debug";
  public static final String SUN_SECURITY_SPNEGO_DEBUG
    = "sun.security.spnego.debug";
  public static final String SUN_SECURITY_JAAS_FILE
    = "java.security.auth.login.config";
  public static final String KERBEROS_KINIT_COMMAND
    = "hadoop.kerberos.kinit.command";
  public static final String HADOOP_AUTHENTICATION_IS_DISABLED
      = "Hadoop authentication is disabled";
  public static final String UNSET = "(unset)";
  public static final String NO_DEFAULT_REALM = "Cannot locate default realm";

  private final Configuration conf;
  private final List<String> services;
  private final PrintStream out;
  private final File keytab;
  private final String principal;
  private final long minKeyLength;
  private final boolean securityRequired;

  public static final String CAT_JVM = "JVM";
  public static final String CAT_JAAS = "JAAS";
  public static final String CAT_CONFIG = "CONFIG";
  public static final String CAT_LOGIN = "LOGIN";
  public static final String CAT_KERBEROS = "KERBEROS";
  public static final String CAT_SASL = "SASL";

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public KerberosDiags(Configuration conf,
      PrintStream out,
      List<String> services,
      File keytab,
      String principal,
      long minKeyLength,
      boolean securityRequired) {
    this.conf = conf;
    this.services = services;
    this.keytab = keytab;
    this.principal = principal;
    this.out = out;
    this.minKeyLength = minKeyLength;
    this.securityRequired = securityRequired;
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  /**
   * Execute diagnostics.
   * <p>
   * Things it would be nice if UGI made accessible
   * <ol>
   *   <li>A way to enable JAAS debug programatically</li>
   *   <li>Access to the TGT</li>
   * </ol>
   * @return true if security was enabled and all probes were successful
   * @throws KerberosDiagsFailure explicitly raised failure
   * @throws Exception other security problems
   */
  @SuppressWarnings("deprecation")
  public boolean execute() throws Exception {

    title("Kerberos Diagnostics scan at %s",
        new Date(System.currentTimeMillis()));

    // check that the machine has a name
    println("Hostname: %s",
        InetAddress.getLocalHost().getCanonicalHostName());

    // Fail fast on a JVM without JCE installed.
    validateKeyLength();

    // look at realm
    println("JVM Kerberos Login Module = %s", getKrb5LoginModuleName());
    printDefaultRealm();

    title("System Properties");
    for (String prop : new String[]{
      JAVA_SECURITY_KRB5_CONF,
      JAVA_SECURITY_KRB5_REALM,
      SUN_SECURITY_KRB5_DEBUG,
      SUN_SECURITY_SPNEGO_DEBUG,
      SUN_SECURITY_JAAS_FILE
    }) {
      printSysprop(prop);
    }

    title("Environment Variables");
    for (String env : new String[]{
      "HADOOP_JAAS_DEBUG",
      KRB5_CCNAME,
      HADOOP_USER_NAME,
      HADOOP_PROXY_USER,
      HADOOP_TOKEN_FILE_LOCATION,
    }) {
      printEnv(env);
    }

    for (String prop : new String[]{
      KERBEROS_KINIT_COMMAND,
      HADOOP_SECURITY_AUTHENTICATION,
      HADOOP_SECURITY_AUTHORIZATION,
      "hadoop.kerberos.min.seconds.before.relogin",    // not in 2.6
      "hadoop.security.dns.interface",   // not in 2.6
      "hadoop.security.dns.nameserver",  // not in 2.6
      HADOOP_RPC_PROTECTION,
      HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX,
      HADOOP_SECURITY_GROUP_MAPPING,
      "hadoop.security.impersonation.provider.class",    // not in 2.6
      "dfs.data.transfer.protection" // HDFS
    }) {
      printConfOpt(prop);
    }

    // check that authentication is enabled
    if (SecurityUtil.getAuthenticationMethod(conf)
        .equals(AuthenticationMethod.SIMPLE)) {
      println(HADOOP_AUTHENTICATION_IS_DISABLED);
      failif(securityRequired, CAT_CONFIG, HADOOP_AUTHENTICATION_IS_DISABLED);
      // no security, skip rest of test
      return false;
    }

    validateKrb5File();
    validateSasl(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
    validateSasl("dfs.data.transfer.saslproperties.resolver.class");
    validateKinitExecutable();
    validateJAAS();
    // now the big test: login, then try again
    boolean krb5Debug = getAndSet(SUN_SECURITY_KRB5_DEBUG);
    boolean spnegoDebug = getAndSet(SUN_SECURITY_SPNEGO_DEBUG);
    try {
      title("Logging in");

      if (keytab != null) {
        dumpKeytab(keytab);
        loginFromKeytab();
      } else {
        UserGroupInformation loginUser = getLoginUser();
        dumpUGI("Log in user", loginUser);
        validateUGI("Login user", loginUser);
        println("Ticket based login: %b", isLoginTicketBased());
        println("Keytab based login: %b", isLoginKeytabBased());
      }

      return true;
    } finally {
      // restore original system properties
      System.setProperty(SUN_SECURITY_KRB5_DEBUG,
        Boolean.toString(krb5Debug));
      System.setProperty(SUN_SECURITY_SPNEGO_DEBUG,
        Boolean.toString(spnegoDebug));
    }
  }

  /**
   * Fail fast on a JVM without JCE installed.
   *
   * This is a recurrent problem
   * (that is: it keeps creeping back with JVM updates);
   * a fast failure is the best tactic
   * @throws NoSuchAlgorithmException
   */

  protected void validateKeyLength() throws NoSuchAlgorithmException {
    int aesLen = Cipher.getMaxAllowedKeyLength("AES");
    println("Maximum AES encryption key length %d bits", aesLen);
    failif (aesLen < minKeyLength,
        CAT_JVM,
        "Java Cryptography Extensions are not installed on this JVM."
        +" Maximum supported key length %s - minimum required %d",
        aesLen, minKeyLength);
  }

  /**
   * Get the default realm.
   * <p>
   * Not having a default realm may be harmless, so is noted at info.
   * All other invocation failures are downgraded to warn, as
   * follow-on actions may still work.
   * failure to invoke the method via introspection is rejected,
   * as it's a sign of JVM compatibility issues that may have other
   * consequences
   */
  protected void printDefaultRealm() {
    try {
      println("Default Realm = %s",
          getDefaultRealm());
    } catch (ClassNotFoundException
        | IllegalAccessException
        | NoSuchMethodException e) {

      throw new KerberosDiagsFailure(CAT_JVM, e,
          "Failed to invoke krb5.Config.getDefaultRealm: %s", e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause.toString().contains(NO_DEFAULT_REALM)) {
        // exception raised if there is no default realm. This is not
        // always a problem, so downgrade to a message.
        println("Host has no default realm");
        LOG.debug(cause.toString(), cause);
      } else {
        println("Kerberos.getDefaultRealm() failed: %s\n%s",
            cause,
            org.apache.hadoop.util.StringUtils.stringifyException(cause));
      }
    }
  }

  /**
   * Locate the krb5.conf file and dump it.
   * No-op on windows.
   * @throws IOException
   */
  private void validateKrb5File() throws IOException {
    if (!Shell.WINDOWS) {
      title("Locating Kerberos configuration file");
      String krbPath = "/etc/krb5.conf";
      String jvmKrbPath = System.getProperty(JAVA_SECURITY_KRB5_CONF);
      if (jvmKrbPath != null) {
        println("Setting kerberos path from sysprop %s: %s",
          JAVA_SECURITY_KRB5_CONF, jvmKrbPath);
        krbPath = jvmKrbPath;
      }

      String krb5name = System.getenv(KRB5_CCNAME);
      if (krb5name != null) {
        println("Setting kerberos path from environment variable %s: %s",
          KRB5_CCNAME, krb5name);
        krbPath = krb5name;
        if (jvmKrbPath != null) {
          println("Warning - both %s and %s were set - %s takes priority",
            JAVA_SECURITY_KRB5_CONF, KRB5_CCNAME, KRB5_CCNAME);
        }
      }

      File krbFile = new File(krbPath);
      println("Kerberos configuration file = %s", krbFile);
      failif(!krbFile.exists(),
          CAT_KERBEROS,
          "Kerberos configuration file %s not found", krbFile);
      dump(krbFile);
    }
  }

  /**
   * Dump a keytab: list all principals.
   * @param keytabFile the keytab file
   * @throws IOException IO problems
   */
  public void dumpKeytab(File keytabFile) throws IOException {
    title("Examining keytab %s", keytabFile);
    File kt = keytabFile.getCanonicalFile();
    failif(!kt.exists(), CAT_CONFIG, "Keytab not found: %s", kt);
    failif(!kt.isFile(), CAT_CONFIG, "Keytab is not a valid file: %s", kt);

    String[] names = getPrincipalNames(keytabFile.getCanonicalPath(),
        Pattern.compile(".*"));
    println("keytab entry count: %d", names.length);
    for (String name : names) {
      println("    %s", name);
    }
    println("-----");
  }

  /**
   * Log in from a keytab, dump the UGI, validate it, then try and log in again.
   * That second-time login catches JVM/Hadoop compatibility problems.
   * @throws IOException
   */
  private void loginFromKeytab() throws IOException {
    UserGroupInformation ugi;
    String identity;
    if (keytab != null) {
      File kt = keytab.getCanonicalFile();
      println("Using keytab %s principal %s", kt, principal);
      identity = principal;

      failif(StringUtils.isEmpty(principal), CAT_KERBEROS,
          "No principal defined");
      ugi = loginUserFromKeytabAndReturnUGI(principal, kt.getPath());
      dumpUGI(identity, ugi);
      validateUGI(principal, ugi);

      title("Attempting to log in from keytab again");
      // package scoped -hence the reason why this class must be in the
      // hadoop.security package
      setShouldRenewImmediatelyForTests(true);
      // attempt a new login
      ugi.reloginFromKeytab();
    } else {
      println("No keytab: logging is as current user");
    }
  }

  /**
   * Dump a UGI.
   * @param title title of this section
   * @param ugi UGI to dump
   * @throws IOException
   */
  private void dumpUGI(String title, UserGroupInformation ugi)
    throws IOException {
    title(title);
    println("UGI instance = %s", ugi);
    println("Has kerberos credentials: %b", ugi.hasKerberosCredentials());
    println("Authentication method: %s", ugi.getAuthenticationMethod());
    println("Real Authentication method: %s",
      ugi.getRealAuthenticationMethod());
    title("Group names");
    for (String name : ugi.getGroupNames()) {
      println(name);
    }
    title("Credentials");
    Credentials credentials = ugi.getCredentials();
    List<Text> secretKeys = credentials.getAllSecretKeys();
    title("Secret keys");
    if (!secretKeys.isEmpty()) {
      for (Text secret: secretKeys) {
        println("%s", secret);
      }
    } else {
      println("(none)");
    }

    dumpTokens(ugi);
  }

  /**
   * Validate the UGI: verify it is kerberized.
   * @param messagePrefix message in exceptions
   * @param user user to validate
   */
  private void validateUGI(String messagePrefix, UserGroupInformation user) {
    failif(!user.hasKerberosCredentials(),
        CAT_LOGIN, "%s: No kerberos credentials for %s", messagePrefix, user);
    failif(user.getAuthenticationMethod() == null,
        CAT_LOGIN, "%s: Null AuthenticationMethod for %s", messagePrefix, user);
  }

  /**
   * A cursory look at the {@code kinit} executable.
   * If it is an absolute path: it must exist with a size > 0.
   * If it is just a command, it has to be on the path. There's no check
   * for that -but the PATH is printed out.
   */
  private void validateKinitExecutable() {
    String kinit = conf.getTrimmed(KERBEROS_KINIT_COMMAND, "");
    if (!kinit.isEmpty()) {
      File kinitPath = new File(kinit);
      println("%s = %s", KERBEROS_KINIT_COMMAND, kinitPath);
      if (kinitPath.isAbsolute()) {
        failif(!kinitPath.exists(), CAT_KERBEROS,
            "%s executable does not exist: %s",
            KERBEROS_KINIT_COMMAND, kinitPath);
        failif(!kinitPath.isFile(), CAT_KERBEROS,
            "%s path does not refer to a file: %s",
            KERBEROS_KINIT_COMMAND, kinitPath);
        failif(kinitPath.length() == 0, CAT_KERBEROS,
            "%s file is empty: %s",
            KERBEROS_KINIT_COMMAND, kinitPath);
      } else {
        println("Executable %s is relative -must be on the PATH", kinit);
        printEnv("PATH");
      }
    }
  }

  /**
   * Try to load the SASL resolver.
   * @param saslPropsResolverKey key for the SASL resolver
   */
  private void validateSasl(String saslPropsResolverKey) {
    title("Resolving SASL property %s", saslPropsResolverKey);
    String saslPropsResolver = conf.getTrimmed(saslPropsResolverKey);
    try {
      Class<? extends SaslPropertiesResolver> resolverClass = conf.getClass(
          saslPropsResolverKey,
          SaslPropertiesResolver.class, SaslPropertiesResolver.class);
      println("Resolver is %s", resolverClass);
    } catch (RuntimeException e) {
      throw new KerberosDiagsFailure(CAT_SASL, e,
          "Failed to load %s class %s",
          saslPropsResolverKey, saslPropsResolver);
    }
  }

  /**
   * Validate any JAAS entry referenced in the {@link #SUN_SECURITY_JAAS_FILE}
   * property.
   */
  private void validateJAAS() {
    String jaasFilename = System.getProperty(SUN_SECURITY_JAAS_FILE);
    if (jaasFilename != null) {
      title("JAAS");
      File jaasFile = new File(jaasFilename);
      println("JAAS file is defined in %s: %s",
          SUN_SECURITY_JAAS_FILE, jaasFile);
      failif(!jaasFile.exists(), CAT_JAAS,
          "JAAS file does not exist: %s", jaasFile);
      failif(!jaasFile.isFile(), CAT_JAAS,
          "Specified JAAS file is not a file: %s", jaasFile);
    }
  }

  /**
   * Dump all tokens of a user
   * @param user user
   */
  public void dumpTokens(UserGroupInformation user) {
    Collection<Token<? extends TokenIdentifier>> tokens
      = user.getCredentials().getAllTokens();
    title("Token Count: %d", tokens.size());
    for (Token<? extends TokenIdentifier> token : tokens) {
      println("Token %s", token.getKind());
    }
  }

  /**
   * Set the System property to true; return the old value for caching
   * @param sysprop property
   * @return the previous value
   */
  private boolean getAndSet(String sysprop) {
    boolean old = Boolean.getBoolean(sysprop);
    System.setProperty(sysprop, "true");
    return old;
  }

  /**
   * Flush all active output channels, including {@Code System.err},
   * so as to stay in sync with any JRE log messages.
   */
  private void flush() {
    if (out != null) {
      out.flush();
    } else {
      System.out.flush();
    }
    System.err.flush();
  }

  /**
   * Format and print a line of output.
   * This goes to any output file, or
   * is logged at info. The output is flushed before and after, to
   * try and stay in sync with JRE logging.
   * @param format format string
   * @param args any arguments
   */
  @VisibleForTesting
  public void println(String format, Object... args) {
    println(format(format, args));
  }

  /**
   * Print a line of output. This goes to any output file, or
   * is logged at info. The output is flushed before and after, to
   * try and stay in sync with JRE logging.
   * @param msg message string
   */
  @VisibleForTesting
  private void println(String msg) {
    flush();
    if (out != null) {
      out.println(msg);
    } else {
      LOG.info(msg);
    }
    flush();
  }

  /**
   * Print a title entry
   * @param format format string
   * @param args any arguments
   */
  private void title(String format, Object... args) {
    println("");
    println("");
    String msg = "== " + format(format, args) + " ==";
    println(msg);
    println("");
  }

  /**
   * Print a system property, or {@link #UNSET} if unset.
   * @param property property to print
   */
  private void printSysprop(String property) {
    println("%s = \"%s\"", property,
        System.getProperty(property, UNSET));
  }

  /**
   * Print a configuration option, or {@link #UNSET} if unset.
   * @param option option to print
   */
  private void printConfOpt(String option) {
    println("%s = \"%s\"", option, conf.get(option, UNSET));
  }

  /**
   * Print an environment variable's name and value; printing
   * {@link #UNSET} if it is not set
   * @param variable environment variable
   */
  private void printEnv(String variable) {
    String env = System.getenv(variable);
    println("%s = \"%s\"", variable, env != null ? env : UNSET);
  }

  /**
   * Dump any file to standard out; add a trailing newline
   * @param file file to dump
   * @throws IOException IO problems
   */
  public void dump(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      for (String line : IOUtils.readLines(in)) {
        println("%s", line);
      }
    }
    println("");
  }

  /**
   * Format and raise a failure
   *
   * @param category category for exception
   * @param message string formatting message
   * @param args any arguments for the formatting
   * @throws KerberosDiagsFailure containing the formatted text
   */
  private void fail(String category, String message, Object... args)
    throws KerberosDiagsFailure {
    throw new KerberosDiagsFailure(category, message, args);
  }

  /**
   * Conditional failure with string formatted arguments
   * @param condition failure condition
   * @param category category for exception
   * @param message string formatting message
   * @param args any arguments for the formatting
   * @throws KerberosDiagsFailure containing the formatted text
   *         if the condition was met
   */
  private void failif(boolean condition,
      String category,
      String message,
      Object... args)
    throws KerberosDiagsFailure {
    if (condition) {
      fail(category, message, args);
    }
  }

  /**
   * Format a string, treating a call where there are no varags values
   * as a string to pass through unformatted.
   * @param message message, which is either a format string + args, or
   * a general string
   * @param args argument array
   * @return a string for printing.
   */
  public static String format(String message, Object... args) {
    if (args.length == 0) {
      return message;
    } else {
      return String.format(message, args);
    }
  }

  /**
   * Diagnostics failures return the exit code 41, "unauthorized".
   *
   * They have a category, initially for testing: the category can be
   * validated without having to match on the entire string.
   */
  public static class KerberosDiagsFailure extends ExitUtil.ExitException {
    private final String category;

    public KerberosDiagsFailure(String category, String message) {
      super(41, category + ": " + message);
      this.category = category;
    }

    public KerberosDiagsFailure(String category, String message, Object... args) {
      this(category, format(message, args));
    }

    public KerberosDiagsFailure(String category, Throwable throwable,
        String message, Object... args) {
      this(category, message, args);
      initCause(throwable);
    }

    public String getCategory() {
      return category;
    }
  }
}
