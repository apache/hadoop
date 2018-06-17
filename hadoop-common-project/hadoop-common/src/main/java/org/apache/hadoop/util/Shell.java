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
package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for running a Shell command.
 *
 * <code>Shell</code> can be used to run shell commands like <code>du</code> or
 * <code>df</code>. It also offers facilities to gate commands by
 * time-intervals.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Shell {
  private static final Map<Shell, Object> CHILD_SHELLS =
      Collections.synchronizedMap(new WeakHashMap<Shell, Object>());
  public static final Logger LOG = LoggerFactory.getLogger(Shell.class);

  /**
   * Text to include when there are windows-specific problems.
   * {@value}
   */
  private static final String WINDOWS_PROBLEMS =
      "https://wiki.apache.org/hadoop/WindowsProblems";

  /**
   * Name of the windows utils binary: {@value}.
   */
  static final String WINUTILS_EXE = "winutils.exe";

  /**
   * System property for the Hadoop home directory: {@value}.
   */
  public static final String SYSPROP_HADOOP_HOME_DIR = "hadoop.home.dir";

  /**
   * Environment variable for Hadoop's home dir: {@value}.
   */
  public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

  /**
   * query to see if system is Java 7 or later.
   * Now that Hadoop requires Java 7 or later, this always returns true.
   * @deprecated This call isn't needed any more: please remove uses of it.
   * @return true, always.
   */
  @Deprecated
  public static boolean isJava7OrAbove() {
    return true;
  }

  // "1.8"->8, "9"->9, "10"->10
  private static final int JAVA_SPEC_VER = Math.max(8, Integer.parseInt(
      System.getProperty("java.specification.version").split("\\.")[0]));

  /**
   * Query to see if major version of Java specification of the system
   * is equal or greater than the parameter.
   *
   * @param version 8, 9, 10 etc.
   * @return comparison with system property, always true for 8
   */
  public static boolean isJavaVersionAtLeast(int version) {
    return JAVA_SPEC_VER >= version;
  }

  /**
   * Maximum command line length in Windows
   * KB830473 documents this as 8191
   */
  public static final int WINDOWS_MAX_SHELL_LENGTH = 8191;

  /**
   * mis-spelling of {@link #WINDOWS_MAX_SHELL_LENGTH}.
   * @deprecated use the correctly spelled constant.
   */
  @Deprecated
  public static final int WINDOWS_MAX_SHELL_LENGHT = WINDOWS_MAX_SHELL_LENGTH;

  /**
   * Checks if a given command (String[]) fits in the Windows maximum command
   * line length Note that the input is expected to already include space
   * delimiters, no extra count will be added for delimiters.
   *
   * @param commands command parts, including any space delimiters
   */
  public static void checkWindowsCommandLineLength(String...commands)
      throws IOException {
    int len = 0;
    for (String s: commands) {
      len += s.length();
    }
    if (len > WINDOWS_MAX_SHELL_LENGTH) {
      throw new IOException(String.format(
        "The command line has a length of %d exceeds maximum allowed length" +
            " of %d. Command starts with: %s",
        len, WINDOWS_MAX_SHELL_LENGTH,
        StringUtils.join("", commands).substring(0, 100)));
    }
  }

  /**
   * Quote the given arg so that bash will interpret it as a single value.
   * Note that this quotes it for one level of bash, if you are passing it
   * into a badly written shell script, you need to fix your shell script.
   * @param arg the argument to quote
   * @return the quoted string
   */
  static String bashQuote(String arg) {
    StringBuilder buffer = new StringBuilder(arg.length() + 2);
    buffer.append('\'');
    buffer.append(arg.replace("'", "'\\''"));
    buffer.append('\'');
    return buffer.toString();
  }

  /** a Unix command to get the current user's name: {@value}. */
  public static final String USER_NAME_COMMAND = "whoami";

  /** Windows <code>CreateProcess</code> synchronization object. */
  public static final Object WindowsProcessLaunchLock = new Object();

  // OSType detection

  public enum OSType {
    OS_TYPE_LINUX,
    OS_TYPE_WIN,
    OS_TYPE_SOLARIS,
    OS_TYPE_MAC,
    OS_TYPE_FREEBSD,
    OS_TYPE_OTHER
  }

  /**
   * Get the type of the operating system, as determined from parsing
   * the <code>os.name</code> property.
   */
  public static final OSType osType = getOSType();

  private static OSType getOSType() {
    String osName = System.getProperty("os.name");
    if (osName.startsWith("Windows")) {
      return OSType.OS_TYPE_WIN;
    } else if (osName.contains("SunOS") || osName.contains("Solaris")) {
      return OSType.OS_TYPE_SOLARIS;
    } else if (osName.contains("Mac")) {
      return OSType.OS_TYPE_MAC;
    } else if (osName.contains("FreeBSD")) {
      return OSType.OS_TYPE_FREEBSD;
    } else if (osName.startsWith("Linux")) {
      return OSType.OS_TYPE_LINUX;
    } else {
      // Some other form of Unix
      return OSType.OS_TYPE_OTHER;
    }
  }

  // Helper static vars for each platform
  public static final boolean WINDOWS = (osType == OSType.OS_TYPE_WIN);
  public static final boolean SOLARIS = (osType == OSType.OS_TYPE_SOLARIS);
  public static final boolean MAC     = (osType == OSType.OS_TYPE_MAC);
  public static final boolean FREEBSD = (osType == OSType.OS_TYPE_FREEBSD);
  public static final boolean LINUX   = (osType == OSType.OS_TYPE_LINUX);
  public static final boolean OTHER   = (osType == OSType.OS_TYPE_OTHER);

  public static final boolean PPC_64
                = System.getProperties().getProperty("os.arch").contains("ppc64");

  /** a Unix command to get the current user's groups list. */
  public static String[] getGroupsCommand() {
    return (WINDOWS)? new String[]{"cmd", "/c", "groups"}
                    : new String[]{"groups"};
  }

  /**
   * A command to get a given user's groups list.
   * If the OS is not WINDOWS, the command will get the user's primary group
   * first and finally get the groups list which includes the primary group.
   * i.e. the user's primary group will be included twice.
   */
  public static String[] getGroupsForUserCommand(final String user) {
    //'groups username' command return is inconsistent across different unixes
    if (WINDOWS) {
      return new String[]
          {getWinUtilsPath(), "groups", "-F", "\"" + user + "\""};
    } else {
      String quotedUser = bashQuote(user);
      return new String[] {"bash", "-c", "id -gn " + quotedUser +
                            "; id -Gn " + quotedUser};
    }
  }

  /**
   * A command to get a given user's group id list.
   * The command will get the user's primary group
   * first and finally get the groups list which includes the primary group.
   * i.e. the user's primary group will be included twice.
   * This command does not support Windows and will only return group names.
   */
  public static String[] getGroupsIDForUserCommand(final String user) {
    //'groups username' command return is inconsistent across different unixes
    if (WINDOWS) {
      return new String[]{getWinUtilsPath(), "groups", "-F", "\"" + user +
                           "\""};
    } else {
      String quotedUser = bashQuote(user);
      return new String[] {"bash", "-c", "id -g " + quotedUser + "; id -G " +
                            quotedUser};
    }
  }

  /** A command to get a given netgroup's user list. */
  public static String[] getUsersForNetgroupCommand(final String netgroup) {
    //'groups username' command return is non-consistent across different unixes
    return new String[] {"getent", "netgroup", netgroup};
  }

  /** Return a command to get permission information. */
  public static String[] getGetPermissionCommand() {
    return (WINDOWS) ? new String[] { getWinUtilsPath(), "ls", "-F" }
                     : new String[] { "ls", "-ld" };
  }

  /** Return a command to set permission. */
  public static String[] getSetPermissionCommand(String perm, boolean recursive) {
    if (recursive) {
      return (WINDOWS) ?
          new String[] { getWinUtilsPath(), "chmod", "-R", perm }
          : new String[] { "chmod", "-R", perm };
    } else {
      return (WINDOWS) ?
          new String[] { getWinUtilsPath(), "chmod", perm }
          : new String[] { "chmod", perm };
    }
  }

  /**
   * Return a command to set permission for specific file.
   *
   * @param perm String permission to set
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param file String file to set
   * @return String[] containing command and arguments
   */
  public static String[] getSetPermissionCommand(String perm,
                                                 boolean recursive,
                                                 String file) {
    String[] baseCmd = getSetPermissionCommand(perm, recursive);
    String[] cmdWithFile = Arrays.copyOf(baseCmd, baseCmd.length + 1);
    cmdWithFile[cmdWithFile.length - 1] = file;
    return cmdWithFile;
  }

  /** Return a command to set owner. */
  public static String[] getSetOwnerCommand(String owner) {
    return (WINDOWS) ?
        new String[] { getWinUtilsPath(), "chown", "\"" + owner + "\"" }
        : new String[] { "chown", owner };
  }

  /** Return a command to create symbolic links. */
  public static String[] getSymlinkCommand(String target, String link) {
    return WINDOWS ?
       new String[] { getWinUtilsPath(), "symlink", link, target }
       : new String[] { "ln", "-s", target, link };
  }

  /** Return a command to read the target of the a symbolic link. */
  public static String[] getReadlinkCommand(String link) {
    return WINDOWS ?
        new String[] { getWinUtilsPath(), "readlink", link }
        : new String[] { "readlink", link };
  }

  /**
   * Return a command for determining if process with specified pid is alive.
   * @param pid process ID
   * @return a <code>kill -0</code> command or equivalent
   */
  public static String[] getCheckProcessIsAliveCommand(String pid) {
    return getSignalKillCommand(0, pid);
  }

  /** Return a command to send a signal to a given pid. */
  public static String[] getSignalKillCommand(int code, String pid) {
    // Code == 0 means check alive
    if (Shell.WINDOWS) {
      if (0 == code) {
        return new String[] {Shell.getWinUtilsPath(), "task", "isAlive", pid };
      } else {
        return new String[] {Shell.getWinUtilsPath(), "task", "kill", pid };
      }
    }

    // Use the bash-builtin instead of the Unix kill command (usually
    // /bin/kill) as the bash-builtin supports "--" in all Hadoop supported
    // OSes.
    final String quotedPid = bashQuote(pid);
    if (isSetsidAvailable) {
      return new String[] { "bash", "-c", "kill -" + code + " -- -" +
          quotedPid };
    } else {
      return new String[] { "bash", "-c", "kill -" + code + " " +
          quotedPid };
    }
  }

  /** Regular expression for environment variables: {@value}. */
  public static final String ENV_NAME_REGEX = "[A-Za-z_][A-Za-z0-9_]*";

  /** Return a regular expression string that match environment variables. */
  public static String getEnvironmentVariableRegex() {
    return (WINDOWS)
        ? "%(" + ENV_NAME_REGEX + "?)%"
        : "\\$(" + ENV_NAME_REGEX + ")";
  }

  /**
   * Returns a File referencing a script with the given basename, inside the
   * given parent directory.  The file extension is inferred by platform:
   * <code>".cmd"</code> on Windows, or <code>".sh"</code> otherwise.
   *
   * @param parent File parent directory
   * @param basename String script file basename
   * @return File referencing the script in the directory
   */
  public static File appendScriptExtension(File parent, String basename) {
    return new File(parent, appendScriptExtension(basename));
  }

  /**
   * Returns a script file name with the given basename.
   *
   * The file extension is inferred by platform:
   * <code>".cmd"</code> on Windows, or <code>".sh"</code> otherwise.
   *
   * @param basename String script file basename
   * @return String script file name
   */
  public static String appendScriptExtension(String basename) {
    return basename + (WINDOWS ? ".cmd" : ".sh");
  }

  /**
   * Returns a command to run the given script.  The script interpreter is
   * inferred by platform: cmd on Windows or bash otherwise.
   *
   * @param script File script to run
   * @return String[] command to run the script
   */
  public static String[] getRunScriptCommand(File script) {
    String absolutePath = script.getAbsolutePath();
    return WINDOWS ?
      new String[] {"cmd", "/c", absolutePath }
      : new String[] {"bash", bashQuote(absolutePath) };
  }

  /** a Unix command to set permission: {@value}. */
  public static final String SET_PERMISSION_COMMAND = "chmod";
  /** a Unix command to set owner: {@value}. */
  public static final String SET_OWNER_COMMAND = "chown";

  /** a Unix command to set the change user's groups list: {@value}. */
  public static final String SET_GROUP_COMMAND = "chgrp";
  /** a Unix command to create a link: {@value}. */
  public static final String LINK_COMMAND = "ln";
  /** a Unix command to get a link target: {@value}. */
  public static final String READ_LINK_COMMAND = "readlink";

  /**Time after which the executing script would be timedout. */
  protected long timeOutInterval = 0L;
  /** If or not script timed out*/
  private final AtomicBoolean timedOut = new AtomicBoolean(false);

  /** Indicates if the parent env vars should be inherited or not*/
  protected boolean inheritParentEnv = true;

  /**
   *  Centralized logic to discover and validate the sanity of the Hadoop
   *  home directory.
   *
   *  This does a lot of work so it should only be called
   *  privately for initialization once per process.
   *
   * @return A directory that exists and via was specified on the command line
   * via <code>-Dhadoop.home.dir</code> or the <code>HADOOP_HOME</code>
   * environment variable.
   * @throws FileNotFoundException if the properties are absent or the specified
   * path is not a reference to a valid directory.
   */
  private static File checkHadoopHome() throws FileNotFoundException {

    // first check the Dflag hadoop.home.dir with JVM scope
    String home = System.getProperty(SYSPROP_HADOOP_HOME_DIR);

    // fall back to the system/user-global env variable
    if (home == null) {
      home = System.getenv(ENV_HADOOP_HOME);
    }
    return checkHadoopHomeInner(home);
  }

  /*
  A set of exception strings used to construct error messages;
  these are referred to in tests
  */
  static final String E_DOES_NOT_EXIST = "does not exist";
  static final String E_IS_RELATIVE = "is not an absolute path.";
  static final String E_NOT_DIRECTORY = "is not a directory.";
  static final String E_NO_EXECUTABLE = "Could not locate Hadoop executable";
  static final String E_NOT_EXECUTABLE_FILE = "Not an executable file";
  static final String E_HADOOP_PROPS_UNSET = ENV_HADOOP_HOME + " and "
      + SYSPROP_HADOOP_HOME_DIR + " are unset.";
  static final String E_HADOOP_PROPS_EMPTY = ENV_HADOOP_HOME + " or "
      + SYSPROP_HADOOP_HOME_DIR + " set to an empty string";
  static final String E_NOT_A_WINDOWS_SYSTEM = "Not a Windows system";

  /**
   *  Validate the accessibility of the Hadoop home directory.
   *
   * @return A directory that is expected to be the hadoop home directory
   * @throws FileNotFoundException if the specified
   * path is not a reference to a valid directory.
   */
  @VisibleForTesting
  static File checkHadoopHomeInner(String home) throws FileNotFoundException {
    // couldn't find either setting for hadoop's home directory
    if (home == null) {
      throw new FileNotFoundException(E_HADOOP_PROPS_UNSET);
    }
    // strip off leading and trailing double quotes
    while (home.startsWith("\"")) {
      home = home.substring(1);
    }
    while (home.endsWith("\"")) {
      home = home.substring(0, home.length() - 1);
    }

    // after stripping any quotes, check for home dir being non-empty
    if (home.isEmpty()) {
      throw new FileNotFoundException(E_HADOOP_PROPS_EMPTY);
    }

    // check that the hadoop home dir value
    // is an absolute reference to a directory
    File homedir = new File(home);
    if (!homedir.isAbsolute()) {
      throw new FileNotFoundException("Hadoop home directory " + homedir
          + " " + E_IS_RELATIVE);
    }
    if (!homedir.exists()) {
      throw new FileNotFoundException("Hadoop home directory " + homedir
          + " " + E_DOES_NOT_EXIST);
    }
    if (!homedir.isDirectory()) {
      throw new FileNotFoundException("Hadoop home directory " + homedir
          + " "+ E_NOT_DIRECTORY);
    }
    return homedir;
  }

  /**
   * The Hadoop home directory.
   */
  private static final File HADOOP_HOME_FILE;

  /**
   * Rethrowable cause for the failure to determine the hadoop
   * home directory
   */
  private static final IOException HADOOP_HOME_DIR_FAILURE_CAUSE;

  static {
    File home;
    IOException ex;
    try {
      home = checkHadoopHome();
      ex = null;
    } catch (IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to detect a valid hadoop home directory", ioe);
      }
      ex = ioe;
      home = null;
    }
    HADOOP_HOME_FILE = home;
    HADOOP_HOME_DIR_FAILURE_CAUSE = ex;
  }

  /**
   * Optionally extend an error message with some OS-specific text.
   * @param message core error message
   * @return error message, possibly with some extra text
   */
  private static String addOsText(String message) {
    return WINDOWS ? (message + " -see " + WINDOWS_PROBLEMS) : message;
  }

  /**
   * Create a {@code FileNotFoundException} with the inner nested cause set
   * to the given exception. Compensates for the fact that FNFE doesn't
   * have an initializer that takes an exception.
   * @param text error text
   * @param ex inner exception
   * @return a new exception to throw.
   */
  private static FileNotFoundException fileNotFoundException(String text,
      Exception ex) {
    return (FileNotFoundException) new FileNotFoundException(text)
        .initCause(ex);
  }

  /**
   * Get the Hadoop home directory. Raises an exception if not found
   * @return the home dir
   * @throws IOException if the home directory cannot be located.
   */
  public static String getHadoopHome() throws IOException {
    return getHadoopHomeDir().getCanonicalPath();
  }

  /**
   * Get the Hadoop home directory. If it is invalid,
   * throw an exception.
   * @return a path referring to hadoop home.
   * @throws FileNotFoundException if the directory doesn't exist.
   */
  private static File getHadoopHomeDir() throws FileNotFoundException {
    if (HADOOP_HOME_DIR_FAILURE_CAUSE != null) {
      throw fileNotFoundException(
          addOsText(HADOOP_HOME_DIR_FAILURE_CAUSE.toString()),
          HADOOP_HOME_DIR_FAILURE_CAUSE);
    }
    return HADOOP_HOME_FILE;
  }

  /**
   *  Fully qualify the path to a binary that should be in a known hadoop
   *  bin location. This is primarily useful for disambiguating call-outs
   *  to executable sub-components of Hadoop to avoid clashes with other
   *  executables that may be in the path.  Caveat:  this call doesn't
   *  just format the path to the bin directory.  It also checks for file
   *  existence of the composed path. The output of this call should be
   *  cached by callers.
   *
   * @param executable executable
   * @return executable file reference
   * @throws FileNotFoundException if the path does not exist
   */
  public static File getQualifiedBin(String executable)
      throws FileNotFoundException {
    // construct hadoop bin path to the specified executable
    return getQualifiedBinInner(getHadoopHomeDir(), executable);
  }

  /**
   * Inner logic of {@link #getQualifiedBin(String)}, accessible
   * for tests.
   * @param hadoopHomeDir home directory (assumed to be valid)
   * @param executable executable
   * @return path to the binary
   * @throws FileNotFoundException if the executable was not found/valid
   */
  static File getQualifiedBinInner(File hadoopHomeDir, String executable)
      throws FileNotFoundException {
    String binDirText = "Hadoop bin directory ";
    File bin = new File(hadoopHomeDir, "bin");
    if (!bin.exists()) {
      throw new FileNotFoundException(addOsText(binDirText + E_DOES_NOT_EXIST
          + ": " + bin));
    }
    if (!bin.isDirectory()) {
      throw new FileNotFoundException(addOsText(binDirText + E_NOT_DIRECTORY
          + ": " + bin));
    }

    File exeFile = new File(bin, executable);
    if (!exeFile.exists()) {
      throw new FileNotFoundException(
          addOsText(E_NO_EXECUTABLE + ": " + exeFile));
    }
    if (!exeFile.isFile()) {
      throw new FileNotFoundException(
          addOsText(E_NOT_EXECUTABLE_FILE + ": " + exeFile));
    }
    try {
      return exeFile.getCanonicalFile();
    } catch (IOException e) {
      // this isn't going to happen, because of all the upfront checks.
      // so if it does, it gets converted to a FNFE and rethrown
      throw fileNotFoundException(e.toString(), e);
    }
  }

  /**
   *  Fully qualify the path to a binary that should be in a known hadoop
   *  bin location. This is primarily useful for disambiguating call-outs
   *  to executable sub-components of Hadoop to avoid clashes with other
   *  executables that may be in the path.  Caveat:  this call doesn't
   *  just format the path to the bin directory.  It also checks for file
   *  existence of the composed path. The output of this call should be
   *  cached by callers.
   *
   * @param executable executable
   * @return executable file reference
   * @throws FileNotFoundException if the path does not exist
   * @throws IOException on path canonicalization failures
   */
  public static String getQualifiedBinPath(String executable)
      throws IOException {
    return getQualifiedBin(executable).getCanonicalPath();
  }

  /**
   * Location of winutils as a string; null if not found.
   * <p>
   * <i>Important: caller must check for this value being null</i>.
   * The lack of such checks has led to many support issues being raised.
   * <p>
   * @deprecated use one of the exception-raising getter methods,
   * specifically {@link #getWinUtilsPath()} or {@link #getWinUtilsFile()}
   */
  @Deprecated
  public static final String WINUTILS;

  /** Canonical path to winutils, private to Shell. */
  private static final String WINUTILS_PATH;

  /** file reference to winutils. */
  private static final File WINUTILS_FILE;

  /** the exception raised on a failure to init the WINUTILS fields. */
  private static final IOException WINUTILS_FAILURE;

  /*
   * Static WINUTILS_* field initializer.
   * On non-Windows systems sets the paths to null, and
   * adds a specific exception to the failure cause, so
   * that on any attempt to resolve the paths will raise
   * a meaningful exception.
   */
  static {
    IOException ioe = null;
    String path = null;
    File file = null;
    // invariant: either there's a valid file and path,
    // or there is a cached IO exception.
    if (WINDOWS) {
      try {
        file = getQualifiedBin(WINUTILS_EXE);
        path = file.getCanonicalPath();
        ioe = null;
      } catch (IOException e) {
        LOG.warn("Did not find {}: {}", WINUTILS_EXE, e);
        // stack trace comes at debug level
        LOG.debug("Failed to find " + WINUTILS_EXE, e);
        file = null;
        path = null;
        ioe = e;
      }
    } else {
      // on a non-windows system, the invariant is kept
      // by adding an explicit exception.
      ioe = new FileNotFoundException(E_NOT_A_WINDOWS_SYSTEM);
    }
    WINUTILS_PATH = path;
    WINUTILS_FILE = file;

    WINUTILS = path;
    WINUTILS_FAILURE = ioe;
  }

  /**
   * Predicate to indicate whether or not the path to winutils is known.
   *
   * If true, then {@link #WINUTILS} is non-null, and both
   * {@link #getWinUtilsPath()} and {@link #getWinUtilsFile()}
   * will successfully return this value. Always false on non-windows systems.
   * @return true if there is a valid path to the binary
   */
  public static boolean hasWinutilsPath() {
    return WINUTILS_PATH != null;
  }

  /**
   * Locate the winutils binary, or fail with a meaningful
   * exception and stack trace as an RTE.
   * This method is for use in methods which don't explicitly throw
   * an <code>IOException</code>.
   * @return the path to {@link #WINUTILS_EXE}
   * @throws RuntimeException if the path is not resolvable
   */
  public static String getWinUtilsPath() {
    if (WINUTILS_FAILURE == null) {
      return WINUTILS_PATH;
    } else {
      throw new RuntimeException(WINUTILS_FAILURE.toString(),
          WINUTILS_FAILURE);
    }
  }

  /**
   * Get a file reference to winutils.
   * Always raises an exception if there isn't one
   * @return the file instance referring to the winutils bin.
   * @throws FileNotFoundException on any failure to locate that file.
   */
  public static File getWinUtilsFile() throws FileNotFoundException {
    if (WINUTILS_FAILURE == null) {
      return WINUTILS_FILE;
    } else {
      // raise a new exception to generate a new stack trace
      throw fileNotFoundException(WINUTILS_FAILURE.toString(),
          WINUTILS_FAILURE);
    }
  }

  public static boolean checkIsBashSupported() throws InterruptedIOException {
    if (Shell.WINDOWS) {
      return false;
    }

    ShellCommandExecutor shexec;
    boolean supported = true;
    try {
      String[] args = {"bash", "-c", "echo 1000"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (InterruptedIOException iioe) {
      LOG.warn("Interrupted, unable to determine if bash is supported", iioe);
      throw iioe;
    } catch (IOException ioe) {
      LOG.warn("Bash is not supported by the OS", ioe);
      supported = false;
    } catch (SecurityException se) {
      LOG.info("Bash execution is not allowed by the JVM " +
          "security manager.Considering it not supported.");
      supported = false;
    }

    return supported;
  }

  /**
   * Flag which is true if setsid exists.
   */
  public static final boolean isSetsidAvailable = isSetsidSupported();

  /**
   * Look for <code>setsid</code>.
   * @return true if <code>setsid</code> was present
   */
  private static boolean isSetsidSupported() {
    if (Shell.WINDOWS) {
      return false;
    }
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = {"setsid", "bash", "-c", "echo $$"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.debug("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } catch (SecurityException se) {
      LOG.debug("setsid is not allowed to run by the JVM "+
          "security manager. So not using it.");
      setsidSupported = false;
    } catch (Error err) {
      if (err.getMessage() != null
          && err.getMessage().contains("posix_spawn is not " +
          "a supported process launch mechanism")
          && (Shell.FREEBSD || Shell.MAC)) {
        // HADOOP-11924: This is a workaround to avoid failure of class init
        // by JDK issue on TR locale(JDK-8047340).
        LOG.info("Avoiding JDK-8047340 on BSD-based systems.", err);
        setsidSupported = false;
      }
    }  finally { // handle the exit code
      if (LOG.isDebugEnabled()) {
        LOG.debug("setsid exited with exit code "
                 + (shexec != null ? shexec.getExitCode() : "(null executor)"));
      }
    }
    return setsidSupported;
  }

  /** Token separator regex used to parse Shell tool outputs. */
  public static final String TOKEN_SEPARATOR_REGEX
                = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

  private long interval;   // refresh interval in msec
  private long lastTime;   // last time the command was performed
  private final boolean redirectErrorStream; // merge stdout and stderr
  private Map<String, String> environment; // env for the command execution
  private File dir;
  private Process process; // sub process used to execute the command
  private int exitCode;
  private Thread waitingThread;

  /** Flag to indicate whether or not the script has finished executing. */
  private final AtomicBoolean completed = new AtomicBoolean(false);

  /**
   * Create an instance with no minimum interval between runs; stderr is
   * not merged with stdout.
   */
  protected Shell() {
    this(0L);
  }

  /**
   * Create an instance with a minimum interval between executions; stderr is
   * not merged with stdout.
   * @param interval interval in milliseconds between command executions.
   */
  protected Shell(long interval) {
    this(interval, false);
  }

  /**
   * Create a shell instance which can be re-executed when the {@link #run()}
   * method is invoked with a given elapsed time between calls.
   *
   * @param interval the minimum duration in milliseconds to wait before
   *        re-executing the command. If set to 0, there is no minimum.
   * @param redirectErrorStream should the error stream be merged with
   *        the normal output stream?
   */
  protected Shell(long interval, boolean redirectErrorStream) {
    this.interval = interval;
    this.lastTime = (interval < 0) ? 0 : -interval;
    this.redirectErrorStream = redirectErrorStream;
  }

  /**
   * Set the environment for the command.
   * @param env Mapping of environment variables
   */
  protected void setEnvironment(Map<String, String> env) {
    this.environment = env;
  }

  /**
   * Set the working directory.
   * @param dir The directory where the command will be executed
   */
  protected void setWorkingDirectory(File dir) {
    this.dir = dir;
  }

  /** Check to see if a command needs to be executed and execute if needed. */
  protected void run() throws IOException {
    if (lastTime + interval > Time.monotonicNow()) {
      return;
    }
    exitCode = 0; // reset for next run
    if (Shell.MAC) {
      System.setProperty("jdk.lang.Process.launchMechanism", "POSIX_SPAWN");
    }
    runCommand();
  }

  /** Run the command. */
  private void runCommand() throws IOException {
    ProcessBuilder builder = new ProcessBuilder(getExecString());
    Timer timeOutTimer = null;
    ShellTimeoutTimerTask timeoutTimerTask = null;
    timedOut.set(false);
    completed.set(false);

    // Remove all env vars from the Builder to prevent leaking of env vars from
    // the parent process.
    if (!inheritParentEnv) {
      builder.environment().clear();
    }

    if (environment != null) {
      builder.environment().putAll(this.environment);
    }

    if (dir != null) {
      builder.directory(this.dir);
    }

    builder.redirectErrorStream(redirectErrorStream);

    if (Shell.WINDOWS) {
      synchronized (WindowsProcessLaunchLock) {
        // To workaround the race condition issue with child processes
        // inheriting unintended handles during process launch that can
        // lead to hangs on reading output and error streams, we
        // serialize process creation. More info available at:
        // http://support.microsoft.com/kb/315939
        process = builder.start();
      }
    } else {
      process = builder.start();
    }

    waitingThread = Thread.currentThread();
    CHILD_SHELLS.put(this, null);

    if (timeOutInterval > 0) {
      timeOutTimer = new Timer("Shell command timeout");
      timeoutTimerTask = new ShellTimeoutTimerTask(
          this);
      //One time scheduling.
      timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
    }
    final BufferedReader errReader =
            new BufferedReader(new InputStreamReader(
                process.getErrorStream(), Charset.defaultCharset()));
    BufferedReader inReader =
            new BufferedReader(new InputStreamReader(
                process.getInputStream(), Charset.defaultCharset()));
    final StringBuffer errMsg = new StringBuffer();

    // read error and input streams as this would free up the buffers
    // free the error stream buffer
    Thread errThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            errMsg.append(line);
            errMsg.append(System.getProperty("line.separator"));
            line = errReader.readLine();
          }
        } catch(IOException ioe) {
          // Its normal to observe a "Stream closed" I/O error on
          // command timeouts destroying the underlying process
          // so only log a WARN if the command didn't time out
          if (!isTimedOut()) {
            LOG.warn("Error reading the error stream", ioe);
          } else {
            LOG.debug("Error reading the error stream due to shell "
                + "command timeout", ioe);
          }
        }
      }
    };
    try {
      errThread.start();
    } catch (IllegalStateException ise) {
    } catch (OutOfMemoryError oe) {
      LOG.error("Caught " + oe + ". One possible reason is that ulimit"
          + " setting of 'max user processes' is too low. If so, do"
          + " 'ulimit -u <largerNum>' and try again.");
      throw oe;
    }
    try {
      parseExecResult(inReader); // parse the output
      // clear the input stream buffer
      String line = inReader.readLine();
      while(line != null) {
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      exitCode  = process.waitFor();
      // make sure that the error thread exits
      joinThread(errThread);
      completed.set(true);
      //the timeout thread handling
      //taken care in finally block
      if (exitCode != 0) {
        throw new ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (InterruptedException ie) {
      InterruptedIOException iie = new InterruptedIOException(ie.toString());
      iie.initCause(ie);
      throw iie;
    } finally {
      if (timeOutTimer != null) {
        timeOutTimer.cancel();
      }
      // close the input stream
      try {
        // JDK 7 tries to automatically drain the input streams for us
        // when the process exits, but since close is not synchronized,
        // it creates a race if we close the stream first and the same
        // fd is recycled.  the stream draining thread will attempt to
        // drain that fd!!  it may block, OOM, or cause bizarre behavior
        // see: https://bugs.openjdk.java.net/browse/JDK-8024521
        //      issue is fixed in build 7u60
        InputStream stdout = process.getInputStream();
        synchronized (stdout) {
          inReader.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      if (!completed.get()) {
        errThread.interrupt();
        joinThread(errThread);
      }
      try {
        InputStream stderr = process.getErrorStream();
        synchronized (stderr) {
          errReader.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      process.destroy();
      waitingThread = null;
      CHILD_SHELLS.remove(this);
      lastTime = Time.monotonicNow();
    }
  }

  private static void joinThread(Thread t) {
    while (t.isAlive()) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Interrupted while joining on: " + t, ie);
        }
        t.interrupt(); // propagate interrupt
      }
    }
  }

  /** return an array containing the command name and its parameters. */
  protected abstract String[] getExecString();

  /** Parse the execution result */
  protected abstract void parseExecResult(BufferedReader lines)
  throws IOException;

  /**
   * Get an environment variable.
   * @param env the environment var
   * @return the value or null if it was unset.
   */
  public String getEnvironment(String env) {
    return environment.get(env);
  }

  /** get the current sub-process executing the given command.
   * @return process executing the command
   */
  public Process getProcess() {
    return process;
  }

  /** get the exit code.
   * @return the exit code of the process
   */
  public int getExitCode() {
    return exitCode;
  }

  /** get the thread that is waiting on this instance of <code>Shell</code>.
   * @return the thread that ran runCommand() that spawned this shell
   * or null if no thread is waiting for this shell to complete
   */
  public Thread getWaitingThread() {
    return waitingThread;
  }


  /**
   * This is an IOException with exit code added.
   */
  public static class ExitCodeException extends IOException {
    private final int exitCode;

    public ExitCodeException(int exitCode, String message) {
      super(message);
      this.exitCode = exitCode;
    }

    public int getExitCode() {
      return exitCode;
    }

    @Override
    public String toString() {
      final StringBuilder sb =
          new StringBuilder("ExitCodeException ");
      sb.append("exitCode=").append(exitCode)
        .append(": ");
      sb.append(super.getMessage());
      return sb.toString();
    }
  }

  public interface CommandExecutor {

    void execute() throws IOException;

    int getExitCode() throws IOException;

    String getOutput() throws IOException;

    void close();

  }

  /**
   * A simple shell command executor.
   *
   * <code>ShellCommandExecutor</code>should be used in cases where the output
   * of the command needs no explicit parsing and where the command, working
   * directory and the environment remains unchanged. The output of the command
   * is stored as-is and is expected to be small.
   */
  public static class ShellCommandExecutor extends Shell
      implements CommandExecutor {

    private String[] command;
    private StringBuffer output;


    public ShellCommandExecutor(String[] execString) {
      this(execString, null);
    }

    public ShellCommandExecutor(String[] execString, File dir) {
      this(execString, dir, null);
    }

    public ShellCommandExecutor(String[] execString, File dir,
                                 Map<String, String> env) {
      this(execString, dir, env , 0L);
    }

    public ShellCommandExecutor(String[] execString, File dir,
                                Map<String, String> env, long timeout) {
      this(execString, dir, env , timeout, true);
    }

    /**
     * Create a new instance of the ShellCommandExecutor to execute a command.
     *
     * @param execString The command to execute with arguments
     * @param dir If not-null, specifies the directory which should be set
     *            as the current working directory for the command.
     *            If null, the current working directory is not modified.
     * @param env If not-null, environment of the command will include the
     *            key-value pairs specified in the map. If null, the current
     *            environment is not modified.
     * @param timeout Specifies the time in milliseconds, after which the
     *                command will be killed and the status marked as timed-out.
     *                If 0, the command will not be timed out.
     * @param inheritParentEnv Indicates if the process should inherit the env
     *                         vars from the parent process or not.
     */
    public ShellCommandExecutor(String[] execString, File dir,
        Map<String, String> env, long timeout, boolean inheritParentEnv) {
      command = execString.clone();
      if (dir != null) {
        setWorkingDirectory(dir);
      }
      if (env != null) {
        setEnvironment(env);
      }
      timeOutInterval = timeout;
      this.inheritParentEnv = inheritParentEnv;
    }

    /**
     * Returns the timeout value set for the executor's sub-commands.
     * @return The timeout value in milliseconds
     */
    @VisibleForTesting
    public long getTimeoutInterval() {
      return timeOutInterval;
    }

    /**
     * Execute the shell command.
     * @throws IOException if the command fails, or if the command is
     * not well constructed.
     */
    public void execute() throws IOException {
      for (String s : command) {
        if (s == null) {
          throw new IOException("(null) entry in command string: "
              + StringUtils.join(" ", command));
        }
      }
      this.run();
    }

    @Override
    public String[] getExecString() {
      return command;
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      output = new StringBuffer();
      char[] buf = new char[512];
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        output.append(buf, 0, nRead);
      }
    }

    /** Get the output of the shell command. */
    public String getOutput() {
      return (output == null) ? "" : output.toString();
    }

    /**
     * Returns the commands of this instance.
     * Arguments with spaces in are presented with quotes round; other
     * arguments are presented raw
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      String[] args = getExecString();
      for (String s : args) {
        if (s.indexOf(' ') >= 0) {
          builder.append('"').append(s).append('"');
        } else {
          builder.append(s);
        }
        builder.append(' ');
      }
      return builder.toString();
    }

    @Override
    public void close() {
    }
  }

  /**
   * To check if the passed script to shell command executor timed out or
   * not.
   *
   * @return if the script timed out.
   */
  public boolean isTimedOut() {
    return timedOut.get();
  }

  /**
   * Declare that the command has timed out.
   *
   */
  private void setTimedOut() {
    this.timedOut.set(true);
  }

  /**
   * Static method to execute a shell command.
   * Covers most of the simple cases without requiring the user to implement
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(String ... cmd) throws IOException {
    return execCommand(null, cmd, 0L);
  }

  /**
   * Static method to execute a shell command.
   * Covers most of the simple cases without requiring the user to implement
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @param timeout time in milliseconds after which script should be marked timeout
   * @return the output of the executed command.
   * @throws IOException on any problem.
   */

  public static String execCommand(Map<String, String> env, String[] cmd,
      long timeout) throws IOException {
    ShellCommandExecutor exec = new ShellCommandExecutor(cmd, null, env,
                                                          timeout);
    exec.execute();
    return exec.getOutput();
  }

  /**
   * Static method to execute a shell command.
   * Covers most of the simple cases without requiring the user to implement
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   * @throws IOException on any problem.
   */
  public static String execCommand(Map<String,String> env, String ... cmd)
  throws IOException {
    return execCommand(env, cmd, 0L);
  }

  /**
   * Timer which is used to timeout scripts spawned off by shell.
   */
  private static class ShellTimeoutTimerTask extends TimerTask {

    private final Shell shell;

    public ShellTimeoutTimerTask(Shell shell) {
      this.shell = shell;
    }

    @Override
    public void run() {
      Process p = shell.getProcess();
      try {
        p.exitValue();
      } catch (Exception e) {
        //Process has not terminated.
        //So check if it has completed
        //if not just destroy it.
        if (p != null && !shell.completed.get()) {
          shell.setTimedOut();
          p.destroy();
        }
      }
    }
  }

  /**
   * Static method to destroy all running <code>Shell</code> processes.
   * Iterates through a map of all currently running <code>Shell</code>
   * processes and destroys them one by one. This method is thread safe
   */
  public static void destroyAllShellProcesses() {
    synchronized (CHILD_SHELLS) {
      for (Shell shell : CHILD_SHELLS.keySet()) {
        if (shell.getProcess() != null) {
          shell.getProcess().destroy();
        }
      }
      CHILD_SHELLS.clear();
    }
  }

  /**
   * Static method to return a Set of all <code>Shell</code> objects.
   */
  public static Set<Shell> getAllShells() {
    synchronized (CHILD_SHELLS) {
      return new HashSet<>(CHILD_SHELLS.keySet());
    }
  }

  /**
   * Static method to return the memory lock limit for datanode.
   * @param ulimit max value at which memory locked should be capped.
   * @return long value specifying the memory lock limit.
   */
  public static Long getMemlockLimit(Long ulimit) {
    if (WINDOWS) {
      // HDFS-13560: if ulimit is too large on Windows, Windows will complain
      // "1450: Insufficient system resources exist to complete the requested
      // service". Thus, cap Windows memory lock limit at Integer.MAX_VALUE.
      return Math.min(Integer.MAX_VALUE, ulimit);
    }
    return ulimit;
  }
}
