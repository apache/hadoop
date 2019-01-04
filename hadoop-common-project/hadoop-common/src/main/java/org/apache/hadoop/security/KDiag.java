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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionKey;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.security.UserGroupInformation.*;
import static org.apache.hadoop.security.authentication.util.KerberosUtil.*;
import static org.apache.hadoop.util.StringUtils.popOption;
import static org.apache.hadoop.util.StringUtils.popOptionWithArgument;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_TOKEN_FILES;

/**
 * Kerberos diagnostics
 *
 * This operation expands some of the diagnostic output of the security code,
 * but not all. For completeness
 *
 * Set the environment variable {@code HADOOP_JAAS_DEBUG=true}
 * Set the log level for {@code org.apache.hadoop.security=DEBUG}
 */
public class KDiag extends Configured implements Tool, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KDiag.class);
  /**
   * Location of the kerberos ticket cache as passed down via an environment
   * variable. This is what kinit will use by default: {@value}
   */
  public static final String KRB5_CCNAME = "KRB5CCNAME";
  /**
   * Location of main kerberos configuration file as passed down via an
   * environment variable.
   */
  public static final String KRB5_CONFIG = "KRB5_CONFIG";
  public static final String JAVA_SECURITY_KRB5_CONF
    = "java.security.krb5.conf";
  public static final String JAVA_SECURITY_KRB5_REALM
    = "java.security.krb5.realm";
  public static final String JAVA_SECURITY_KRB5_KDC_ADDRESS
    = "java.security.krb5.kdc";
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

  /**
   * String seen in {@code getDefaultRealm()} exceptions if the user has
   * no realm: {@value}.
   */
  public static final String NO_DEFAULT_REALM = "Cannot locate default realm";

  /**
   * The exit code for a failure of the diagnostics: 41 == HTTP 401 == unauth.
   */
  public static final int KDIAG_FAILURE = 41;
  public static final String DFS_DATA_TRANSFER_SASLPROPERTIES_RESOLVER_CLASS
      = "dfs.data.transfer.saslproperties.resolver.class";
  public static final String DFS_DATA_TRANSFER_PROTECTION
      = "dfs.data.transfer.protection";
  public static final String ETC_KRB5_CONF = "/etc/krb5.conf";
  public static final String ETC_NTP = "/etc/ntp.conf";
  public static final String HADOOP_JAAS_DEBUG = "HADOOP_JAAS_DEBUG";

  private PrintWriter out;
  private File keytab;
  private String principal;
  private long minKeyLength = 256;
  private boolean securityRequired;
  private boolean nofail = false;
  private boolean nologin = false;
  private boolean jaas = false;
  private boolean checkShortName = false;

  /**
   * A pattern that recognizes simple/non-simple names. Per KerberosName
   */
  private static final Pattern nonSimplePattern = Pattern.compile("[/@]");

  /**
   * Flag set to true if a {@link #verify(boolean, String, String, Object...)}
   * probe failed.
   */
  private boolean probeHasFailed = false;

  public static final String CAT_CONFIG = "CONFIG";
  public static final String CAT_JAAS = "JAAS";
  public static final String CAT_JVM = "JVM";
  public static final String CAT_KERBEROS = "KERBEROS";
  public static final String CAT_LOGIN = "LOGIN";
  public static final String CAT_OS = "JAAS";
  public static final String CAT_SASL = "SASL";
  public static final String CAT_UGI = "UGI";
  public static final String CAT_TOKEN = "TOKEN";

  public static final String ARG_KEYLEN = "--keylen";
  public static final String ARG_KEYTAB = "--keytab";
  public static final String ARG_JAAS = "--jaas";
  public static final String ARG_NOFAIL = "--nofail";
  public static final String ARG_NOLOGIN = "--nologin";
  public static final String ARG_OUTPUT = "--out";
  public static final String ARG_PRINCIPAL = "--principal";
  public static final String ARG_RESOURCE = "--resource";

  public static final String ARG_SECURE = "--secure";

  public static final String ARG_VERIFYSHORTNAME = "--verifyshortname";

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public KDiag(Configuration conf,
      PrintWriter out,
      File keytab,
      String principal,
      long minKeyLength,
      boolean securityRequired) {
    super(conf);
    this.keytab = keytab;
    this.principal = principal;
    this.out = out;
    this.minKeyLength = minKeyLength;
    this.securityRequired = securityRequired;
  }

  public KDiag() {
  }

  @Override
  public void close() throws IOException {
    flush();
    if (out != null) {
      out.close();
    }
  }

  @Override
  public int run(String[] argv) throws Exception {
    List<String> args = new LinkedList<>(Arrays.asList(argv));
    String keytabName = popOptionWithArgument(ARG_KEYTAB, args);
    if (keytabName != null) {
      keytab = new File(keytabName);
    }
    principal = popOptionWithArgument(ARG_PRINCIPAL, args);
    String outf = popOptionWithArgument(ARG_OUTPUT, args);
    String mkl = popOptionWithArgument(ARG_KEYLEN, args);
    if (mkl != null) {
      minKeyLength = Integer.parseInt(mkl);
    }
    securityRequired = popOption(ARG_SECURE, args);
    nofail = popOption(ARG_NOFAIL, args);
    jaas = popOption(ARG_JAAS, args);
    nologin = popOption(ARG_NOLOGIN, args);
    checkShortName = popOption(ARG_VERIFYSHORTNAME, args);

    // look for list of resources
    String resource;
    while (null != (resource = popOptionWithArgument(ARG_RESOURCE, args))) {
      // loading a resource
      LOG.info("Loading resource {}", resource);
      try (InputStream in =
               getClass().getClassLoader().getResourceAsStream(resource)) {
        if (verify(in != null, CAT_CONFIG, "No resource %s", resource)) {
          Configuration.addDefaultResource(resource);
        }
      }
    }
    // look for any leftovers
    if (!args.isEmpty()) {
      println("Unknown arguments in command:");
      for (String s : args) {
        println("  \"%s\"", s);
      }
      println();
      println(usage());
      return -1;
    }
    if (outf != null) {
      println("Printing output to %s", outf);
      out = new PrintWriter(new File(outf), "UTF-8");
    }
    execute();
    return probeHasFailed ? KDIAG_FAILURE : 0;
  }

  private String usage() {
    return "KDiag: Diagnose Kerberos Problems\n"
      + arg("-D", "key=value", "Define a configuration option")
      + arg(ARG_JAAS, "",
      "Require a JAAS file to be defined in " + SUN_SECURITY_JAAS_FILE)
      + arg(ARG_KEYLEN, "<keylen>",
      "Require a minimum size for encryption keys supported by the JVM."
      + " Default value : "+ minKeyLength)
      + arg(ARG_KEYTAB, "<keytab> " + ARG_PRINCIPAL + " <principal>",
          "Login from a keytab as a specific principal")
      + arg(ARG_NOFAIL, "", "Do not fail on the first problem")
      + arg(ARG_NOLOGIN, "", "Do not attempt to log in")
      + arg(ARG_OUTPUT, "<file>", "Write output to a file")
      + arg(ARG_RESOURCE, "<resource>", "Load an XML configuration resource")
      + arg(ARG_SECURE, "", "Require the hadoop configuration to be secure")
      + arg(ARG_VERIFYSHORTNAME, ARG_PRINCIPAL + " <principal>",
      "Verify the short name of the specific principal does not contain '@' or '/'");
  }

  private String arg(String name, String params, String meaning) {
    return String.format("  [%s%s%s] : %s",
        name, (!params.isEmpty() ? " " : ""), params, meaning) + ".\n";
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
    println("Hostname = %s",
        InetAddress.getLocalHost().getCanonicalHostName());

    println("%s = %d", ARG_KEYLEN, minKeyLength);
    println("%s = %s", ARG_KEYTAB, keytab);
    println("%s = %s", ARG_PRINCIPAL, principal);
    println("%s = %s", ARG_VERIFYSHORTNAME, checkShortName);

    // Fail fast on a JVM without JCE installed.
    validateKeyLength();

    // look at realm
    println("JVM Kerberos Login Module = %s", getKrb5LoginModuleName());

    title("Core System Properties");
    for (String prop : new String[]{
      "user.name",
      "java.version",
      "java.vendor",
      JAVA_SECURITY_KRB5_CONF,
      JAVA_SECURITY_KRB5_REALM,
      JAVA_SECURITY_KRB5_KDC_ADDRESS,
      SUN_SECURITY_KRB5_DEBUG,
      SUN_SECURITY_SPNEGO_DEBUG,
      SUN_SECURITY_JAAS_FILE
    }) {
      printSysprop(prop);
    }
    endln();

    title("All System Properties");
    ArrayList<String> propList = new ArrayList<>(
        System.getProperties().stringPropertyNames());
    Collections.sort(propList, String.CASE_INSENSITIVE_ORDER);
    for (String s : propList) {
      printSysprop(s);
    }
    endln();

    title("Environment Variables");
    for (String env : new String[]{
        HADOOP_JAAS_DEBUG,
        KRB5_CCNAME,
        KRB5_CONFIG,
        HADOOP_USER_NAME,
        HADOOP_PROXY_USER,
        HADOOP_TOKEN_FILE_LOCATION,
        "HADOOP_SECURE_LOG",
        "HADOOP_OPTS",
        "HADOOP_CLIENT_OPTS",
    }) {
      printEnv(env);
    }
    endln();

    title("Configuration Options");
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
      DFS_DATA_TRANSFER_PROTECTION, // HDFS
      DFS_DATA_TRANSFER_SASLPROPERTIES_RESOLVER_CLASS // HDFS
    }) {
      printConfOpt(prop);
    }

    // check that authentication is enabled
    Configuration conf = getConf();
    if (isSimpleAuthentication(conf)) {
      println(HADOOP_AUTHENTICATION_IS_DISABLED);
      failif(securityRequired, CAT_CONFIG, HADOOP_AUTHENTICATION_IS_DISABLED);
      // no security, warn
      LOG.warn("Security is not enabled for the Hadoop cluster");
    } else {
      if (isSimpleAuthentication(new Configuration())) {
        LOG.warn("The default cluster security is insecure");
        failif(securityRequired, CAT_CONFIG, HADOOP_AUTHENTICATION_IS_DISABLED);
      }
    }


    // now the big test: login, then try again
    boolean krb5Debug = getAndSet(SUN_SECURITY_KRB5_DEBUG);
    boolean spnegoDebug = getAndSet(SUN_SECURITY_SPNEGO_DEBUG);

    try {
      UserGroupInformation.setConfiguration(conf);
      validateHadoopTokenFiles(conf);
      validateKrb5File();
      printDefaultRealm();
      validateSasl(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
      if (conf.get(DFS_DATA_TRANSFER_SASLPROPERTIES_RESOLVER_CLASS) != null) {
        validateSasl(DFS_DATA_TRANSFER_SASLPROPERTIES_RESOLVER_CLASS);
      }
      validateKinitExecutable();
      validateJAAS(jaas);
      validateNTPConf();
      if (checkShortName) {
        validateShortName();
      }

      if (!nologin) {
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
   * Is the authentication method of this configuration "simple"?
   * @param conf configuration to check
   * @return true if auth is simple (i.e. not kerberos)
   */
  protected boolean isSimpleAuthentication(Configuration conf) {
    return SecurityUtil.getAuthenticationMethod(conf)
        .equals(AuthenticationMethod.SIMPLE);
  }

  /**
   * Fail fast on a JVM without JCE installed.
   *
   * This is a recurrent problem
   * (that is: it keeps creeping back with JVM updates);
   * a fast failure is the best tactic.
   * @throws NoSuchAlgorithmException
   */

  protected void validateKeyLength() throws NoSuchAlgorithmException {
    int aesLen = Cipher.getMaxAllowedKeyLength("AES");
    println("Maximum AES encryption key length %d bits", aesLen);
    verify(minKeyLength <= aesLen,
        CAT_JVM,
        "Java Cryptography Extensions are not installed on this JVM."
            + " Maximum supported key length %s - minimum required %d",
        aesLen, minKeyLength);
  }

  /**
   * Verify whether auth_to_local rules transform a principal name
   * <p>
   * Having a local user name "bar@foo.com" may be harmless, so it is noted at
   * info. However if what was intended is a transformation to "bar"
   * it can be difficult to debug, hence this check.
   */
  protected void validateShortName() {
    failif(principal == null, CAT_KERBEROS, "No principal defined");

    try {
      KerberosName kn = new KerberosName(principal);
      String result = kn.getShortName();
      if (nonSimplePattern.matcher(result).find()) {
        warn(CAT_KERBEROS, principal + " short name: " + result +
                " still contains @ or /");
      }
    } catch (IOException e) {
      throw new KerberosDiagsFailure(CAT_KERBEROS, e,
              "Failed to get short name for " + principal, e);
    } catch (IllegalArgumentException e) {
      error(CAT_KERBEROS, "KerberosName(" + principal + ") failed: %s\n%s",
              e, StringUtils.stringifyException(e));
    }
  }

  /**
   * Get the default realm.
   * <p>
   * Not having a default realm may be harmless, so is noted at info.
   * All other invocation failures are downgraded to warn, as
   * follow-on actions may still work.
   * Failure to invoke the method via introspection is considered a failure,
   * as it's a sign of JVM compatibility issues that may have other 
   * consequences
   */
  protected void printDefaultRealm() {
    try {
      String defaultRealm = getDefaultRealm();
      println("Default Realm = %s", defaultRealm);
      if (defaultRealm == null) {
        warn(CAT_KERBEROS, "Host has no default realm");
      }
    } catch (ClassNotFoundException
        | IllegalAccessException
        | NoSuchMethodException e) {
      throw new KerberosDiagsFailure(CAT_JVM, e,
          "Failed to invoke krb5.Config.getDefaultRealm: %s: " +e, e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause.toString().contains(NO_DEFAULT_REALM)) {
        // exception raised if there is no default realm. This is not
        // always a problem, so downgrade to a message.
        warn(CAT_KERBEROS, "Host has no default realm");
        LOG.debug(cause.toString(), cause);
      } else {
        error(CAT_KERBEROS, "Kerberos.getDefaultRealm() failed: %s\n%s",
            cause, StringUtils.stringifyException(cause));
      }
    }
  }

  /**
   * Validate that hadoop.token.files (if specified) exist and are valid.
   * @throws ClassNotFoundException
   * @throws SecurityException
   * @throws NoSuchMethodException
   * @throws KerberosDiagsFailure
   */
  private void validateHadoopTokenFiles(Configuration conf)
    throws ClassNotFoundException, KerberosDiagsFailure, NoSuchMethodException,
    SecurityException {
    title("Locating Hadoop token files");

    String tokenFileLocation = System.getProperty(HADOOP_TOKEN_FILES);
    if(tokenFileLocation != null) {
      println("Found " + HADOOP_TOKEN_FILES + " in system properties : "
          + tokenFileLocation);
    }

    if(conf.get(HADOOP_TOKEN_FILES) != null) {
      println("Found " + HADOOP_TOKEN_FILES + " in hadoop configuration : "
          + conf.get(HADOOP_TOKEN_FILES));
      if(System.getProperty(HADOOP_TOKEN_FILES) != null) {
        println(HADOOP_TOKEN_FILES + " in the system properties overrides the"
            + " one specified in hadoop configuration");
      } else {
        tokenFileLocation = conf.get(HADOOP_TOKEN_FILES);
      }
    }

    if (tokenFileLocation != null) {
      for (String tokenFileName:
          StringUtils.getTrimmedStrings(tokenFileLocation)) {
        if (tokenFileName.length() > 0) {
          File tokenFile = new File(tokenFileName);
          verifyFileIsValid(tokenFile, CAT_TOKEN, "token");
          verify(tokenFile, conf, CAT_TOKEN, "token");
        }
      }
    }
  }

  /**
   * Locate the {@code krb5.conf} file and dump it.
   *
   * No-op on windows.
   * @throws IOException problems reading the file.
   */
  private void validateKrb5File() throws IOException {
    if (!Shell.WINDOWS) {
      title("Locating Kerberos configuration file");
      String krbPath = ETC_KRB5_CONF;
      String jvmKrbPath = System.getProperty(JAVA_SECURITY_KRB5_CONF);
      if (jvmKrbPath != null && !jvmKrbPath.isEmpty()) {
        println("Setting kerberos path from sysprop %s: \"%s\"",
          JAVA_SECURITY_KRB5_CONF, jvmKrbPath);
        krbPath = jvmKrbPath;
      }

      String krb5name = System.getenv(KRB5_CONFIG);
      if (krb5name != null) {
        println("Setting kerberos path from environment variable %s: \"%s\"",
            KRB5_CONFIG, krb5name);
        krbPath = krb5name;
        if (jvmKrbPath != null) {
          println("Warning - both %s and %s were set - %s takes priority",
              JAVA_SECURITY_KRB5_CONF, KRB5_CONFIG, KRB5_CONFIG);
        }
      }

      File krbFile = new File(krbPath);
      println("Kerberos configuration file = %s", krbFile);
      dump(krbFile);
      endln();
    }
  }

  /**
   * Dump a keytab: list all principals.
   *
   * @param keytabFile the keytab file
   * @throws IOException IO problems
   */
  private void dumpKeytab(File keytabFile) throws IOException {
    title("Examining keytab %s", keytabFile);
    File kt = keytabFile.getCanonicalFile();
    verifyFileIsValid(kt, CAT_KERBEROS, "keytab");

    Keytab loadKeytab = Keytab.loadKeytab(kt);
    List<PrincipalName> principals = loadKeytab.getPrincipals();
    println("keytab principal count: %d", principals.size());
    int entrySize = 0;
    for (PrincipalName princ : principals) {
      List<KeytabEntry> entries = loadKeytab.getKeytabEntries(princ);
      entrySize = entrySize + entries.size();
      for (KeytabEntry entry : entries) {
        EncryptionKey key = entry.getKey();
        println(" %s: version=%d expires=%s encryption=%s",
                entry.getPrincipal(),
                entry.getKvno(),
                entry.getTimestamp(),
                key.getKeyType());
      }
    }
    println("keytab entry count: %d", entrySize);

    endln();
  }

  /**
   * Log in from a keytab, dump the UGI, validate it, then try and log in again.
   *
   * That second-time login catches JVM/Hadoop compatibility problems.
   * @throws IOException Keytab loading problems
   */
  private void loginFromKeytab() throws IOException {
    UserGroupInformation ugi;
    String identity;
    if (keytab != null) {
      File kt = keytab.getCanonicalFile();
      println("Using keytab %s principal %s", kt, principal);
      identity = principal;

      failif(principal == null, CAT_KERBEROS, "No principal defined");
      ugi = loginUserFromKeytabAndReturnUGI(principal, kt.getPath());
      dumpUGI(identity, ugi);
      validateUGI(principal, ugi);

      title("Attempting to relogin");
      try {
        // package scoped -hence the reason why this class must be in the
        // hadoop.security package
        setShouldRenewImmediatelyForTests(true);
        // attempt a new login
        ugi.reloginFromKeytab();
      } catch (IllegalAccessError e) {
        // if you've built this class into an independent JAR, package-access
        // may fail. Downgrade
        warn(CAT_UGI, "Failed to reset UGI -and so could not try to relogin");
        LOG.debug("Failed to reset UGI: {}", e, e);
      }
    } else {
      println("No keytab: attempting to log in is as current user");
    }
  }

  /**
   * Dump a UGI.
   *
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
    List<Text> secretKeys = ugi.getCredentials().getAllSecretKeys();
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
    if (verify(user.getAuthenticationMethod() == AuthenticationMethod.KERBEROS,
        CAT_LOGIN, "User %s is not authenticated by Kerberos", user)) {
      verify(user.hasKerberosCredentials(),
          CAT_LOGIN, "%s: No kerberos credentials for %s", messagePrefix, user);
      verify(user.getAuthenticationMethod() != null,
          CAT_LOGIN, "%s: Null AuthenticationMethod for %s", messagePrefix,
          user);
    }
  }

  /**
   * A cursory look at the {@code kinit} executable.
   *
   * If it is an absolute path: it must exist with a size > 0.
   * If it is just a command, it has to be on the path. There's no check
   * for that -but the PATH is printed out.
   */
  private void validateKinitExecutable() {
    String kinit = getConf().getTrimmed(KERBEROS_KINIT_COMMAND, "");
    if (!kinit.isEmpty()) {
      File kinitPath = new File(kinit);
      println("%s = %s", KERBEROS_KINIT_COMMAND, kinitPath);
      if (kinitPath.isAbsolute()) {
        verifyFileIsValid(kinitPath, CAT_KERBEROS, KERBEROS_KINIT_COMMAND);
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
    String saslPropsResolver = getConf().getTrimmed(saslPropsResolverKey);
    try {
      Class<? extends SaslPropertiesResolver> resolverClass =
          getConf().getClass(
          saslPropsResolverKey,
          SaslPropertiesResolver.class,
          SaslPropertiesResolver.class);
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
   * @param jaasRequired is JAAS required
   */
  private void validateJAAS(boolean jaasRequired) throws IOException {
    String jaasFilename = System.getProperty(SUN_SECURITY_JAAS_FILE);
    if (jaasRequired) {
      verify(jaasFilename != null, CAT_JAAS,
          "No JAAS file specified in " + SUN_SECURITY_JAAS_FILE);
    }
    if (jaasFilename != null) {
      title("JAAS");
      File jaasFile = new File(jaasFilename);
      println("JAAS file is defined in %s: %s",
          SUN_SECURITY_JAAS_FILE, jaasFile);
      verifyFileIsValid(jaasFile, CAT_JAAS,
          "JAAS file defined in " + SUN_SECURITY_JAAS_FILE);
      dump(jaasFile);
      endln();
    }
  }

  private void validateNTPConf() throws IOException {
    if (!Shell.WINDOWS) {
      File ntpfile = new File(ETC_NTP);
      if (ntpfile.exists()
          && verifyFileIsValid(ntpfile, CAT_OS,
          "NTP file: " + ntpfile)) {
        title("NTP");
        dump(ntpfile);
        endln();
      }
    }
  }


  /**
   * Verify that a file is valid: it is a file, non-empty and readable.
   * @param file file
   * @param category category for exceptions
   * @param text text message
   * @return true if the validation held; false if it did not <i>and</i>
   * {@link #nofail} has disabled raising exceptions.
   */
  private boolean verifyFileIsValid(File file, String category, String text) {
    return verify(file.exists(), category,
        "%s file does not exist: %s",
        text, file)
     && verify(file.isFile(), category,
        "%s path does not refer to a file: %s", text, file)
     && verify(file.length() != 0, category,
        "%s file is empty: %s", text, file)
      && verify(file.canRead(), category,
        "%s file is not readable: %s", text, file);
  }

  /**
   * Dump all tokens of a UGI.
   * @param ugi UGI to examine
   */
  public void dumpTokens(UserGroupInformation ugi) {
    Collection<Token<? extends TokenIdentifier>> tokens
      = ugi.getCredentials().getAllTokens();
    title("Token Count: %d", tokens.size());
    for (Token<? extends TokenIdentifier> token : tokens) {
      println("Token %s", token.getKind());
    }
    endln();
  }

  /**
   * Set the System property to true; return the old value for caching.
   *
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
   * Print a line of output. This goes to any output file, or
   * is logged at info. The output is flushed before and after, to
   * try and stay in sync with JRE logging.
   *
   * @param format format string
   * @param args any arguments
   */
  private void println(String format, Object... args) {
    flush();
    String msg = String.format(format, args);
    if (out != null) {
      out.println(msg);
    } else {
      System.out.println(msg);
    }
    flush();
  }

  /**
   * Print a new line
   */
  private void println() {
    println("");
  }

  /**
   * Print something at the end of a section
   */
  private void endln() {
    println();
    println("-----");
  }

  /**
   * Print a title entry.
   *
   * @param format format string
   * @param args any arguments
   */
  private void title(String format, Object... args) {
    println();
    println();
    println("== " + String.format(format, args) + " ==");
    println();
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
   *
   * @param option option to print
   */
  private void printConfOpt(String option) {
    println("%s = \"%s\"", option, getConf().get(option, UNSET));
  }

  /**
   * Print an environment variable's name and value; printing
   * {@link #UNSET} if it is not set.
   * @param variable environment variable
   */
  private void printEnv(String variable) {
    String env = System.getenv(variable);
    println("%s = \"%s\"", variable, env != null ? env : UNSET);
  }

  /**
   * Dump any file to standard out.
   * @param file file to dump
   * @throws IOException IO problems
   */
  private void dump(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      for (String line : IOUtils.readLines(in)) {
        println("%s", line);
      }
    }
  }

  /**
   * Format and raise a failure.
   *
   * @param category category for exception
   * @param message string formatting message
   * @param args any arguments for the formatting
   * @throws KerberosDiagsFailure containing the formatted text
   */
  private void fail(String category, String message, Object... args)
    throws KerberosDiagsFailure {
    error(category, message, args);
    throw new KerberosDiagsFailure(category, message, args);
  }

  /**
   * Assert that a condition must hold.
   *
   * If not, an exception is raised, or, if {@link #nofail} is set,
   * an error will be logged and the method return false.
   *
   * @param condition condition which must hold
   * @param category category for exception
   * @param message string formatting message
   * @param args any arguments for the formatting
   * @return true if the verification succeeded, false if it failed but
   * an exception was not raised.
   * @throws KerberosDiagsFailure containing the formatted text
   *         if the condition was met
   */
  private boolean verify(boolean condition,
      String category,
      String message,
      Object... args)
    throws KerberosDiagsFailure {
    if (!condition) {
      // condition not met: fail or report
      probeHasFailed = true;
      if (!nofail) {
        fail(category, message, args);
      } else {
        error(category, message, args);
      }
      return false;
    } else {
      // condition is met
      return true;
    }
  }

  /**
   * Verify that tokenFile contains valid Credentials.
   *
   * If not, an exception is raised, or, if {@link #nofail} is set,
   * an error will be logged and the method return false.
   *
   */
  private boolean verify(File tokenFile, Configuration conf, String category,
      String message) throws KerberosDiagsFailure {
    try {
      Credentials.readTokenStorageFile(tokenFile, conf);
    } catch(Exception e) {
      if (!nofail) {
        fail(category, message);
      } else {
        error(category, message);
      }
      return false;
    }
    return true;
  }

  /**
   * Print a message as an error
   * @param category error category
   * @param message format string
   * @param args list of arguments
   */
  private void error(String category, String message, Object...args) {
    println("ERROR: %s: %s", category, String.format(message, args));
  }
  /**
   * Print a message as an warning
   * @param category error category
   * @param message format string
   * @param args list of arguments
   */
  private void warn(String category, String message, Object...args) {
    println("WARNING: %s: %s", category, String.format(message, args));
  }

  /**
   * Conditional failure with string formatted arguments.
   * There is no chek for the {@link #nofail} value.
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
   * Inner entry point, with no logging or system exits.
   *
   * @param conf configuration
   * @param argv argument list
   * @return an exception
   * @throws Exception
   */
  public static int exec(Configuration conf, String... argv) throws Exception {
    try(KDiag kdiag = new KDiag()) {
      return ToolRunner.run(conf, kdiag, argv);
    }
  }

  /**
   * Main entry point.
   * @param argv args list
   */
  public static void main(String[] argv) {
    try {
      ExitUtil.terminate(exec(new Configuration(), argv));
    } catch (ExitUtil.ExitException e) {
      LOG.error(e.toString());
      System.exit(e.status);
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      ExitUtil.halt(-1, e);
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
      super(KDIAG_FAILURE, category + ": " + message);
      this.category = category;
    }

    public KerberosDiagsFailure(String category,
        String message,
        Object... args) {
      this(category, String.format(message, args));
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
