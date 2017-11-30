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
package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.GlobalStorageStatistics.StorageStatisticsProvider;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.HandleOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TraceScope;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;

/****************************************************************
 * An abstract base class for a fairly generic filesystem.  It
 * may be implemented as a distributed filesystem, or as a "local"
 * one that reflects the locally-connected disk.  The local version
 * exists for small Hadoop instances and for testing.
 *
 * <p>
 *
 * All user code that may potentially use the Hadoop Distributed
 * File System should be written to use a FileSystem object or its
 * successor, {@link FileContext}.
 *
 * <p>
 * The local implementation is {@link LocalFileSystem} and distributed
 * implementation is DistributedFileSystem. There are other implementations
 * for object stores and (outside the Apache Hadoop codebase),
 * third party filesystems.
 * <p>
 * Notes
 * <ol>
 * <li>The behaviour of the filesystem is
 * <a href="https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html">
 * specified in the Hadoop documentation. </a>
 * However, the normative specification of the behavior of this class is
 * actually HDFS: if HDFS does not behave the way these Javadocs or
 * the specification in the Hadoop documentations define, assume that
 * the documentation is incorrect.
 * </li>
 * <li>The term {@code FileSystem} refers to an instance of this class.</li>
 * <li>The acronym "FS" is used as an abbreviation of FileSystem.</li>
 * <li>The term {@code filesystem} refers to the distributed/local filesystem
 * itself, rather than the class used to interact with it.</li>
 * <li>The term "file" refers to a file in the remote filesystem,
 * rather than instances of {@code java.io.File}.</li>
 * </ol>
 *****************************************************************/
@SuppressWarnings("DeprecatedIsStillUsed")
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileSystem extends Configured implements Closeable {
  public static final String FS_DEFAULT_NAME_KEY =
                   CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
  public static final String DEFAULT_FS =
                   CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT;

  /**
   * This log is widely used in the org.apache.hadoop.fs code and tests,
   * so must be considered something to only be changed with care.
   */
  @InterfaceAudience.Private
  public static final Log LOG = LogFactory.getLog(FileSystem.class);

  /**
   * The SLF4J logger to use in logging within the FileSystem class itself.
   */
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileSystem.class);

  /**
   * Priority of the FileSystem shutdown hook: {@value}.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  /**
   * Prefix for trash directory: {@value}.
   */
  public static final String TRASH_PREFIX = ".Trash";
  public static final String USER_HOME_PREFIX = "/user";

  /** FileSystem cache. */
  static final Cache CACHE = new Cache();

  /** The key this instance is stored under in the cache. */
  private Cache.Key key;

  /** Recording statistics per a FileSystem class. */
  private static final Map<Class<? extends FileSystem>, Statistics>
      statisticsTable = new IdentityHashMap<>();

  /**
   * The statistics for this file system.
   */
  protected Statistics statistics;

  /**
   * A cache of files that should be deleted when the FileSystem is closed
   * or the JVM is exited.
   */
  private final Set<Path> deleteOnExit = new TreeSet<>();

  /**
   * Should symbolic links be resolved by {@link FileSystemLinkResolver}.
   * Set to the value of
   * {@link CommonConfigurationKeysPublic#FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY}
   */
  boolean resolveSymlinks;

  /**
   * This method adds a FileSystem instance to the cache so that it can
   * be retrieved later. It is only for testing.
   * @param uri the uri to store it under
   * @param conf the configuration to store it under
   * @param fs the FileSystem to store
   * @throws IOException if the current user cannot be determined.
   */
  @VisibleForTesting
  static void addFileSystemForTesting(URI uri, Configuration conf,
      FileSystem fs) throws IOException {
    CACHE.map.put(new Cache.Key(uri, conf), fs);
  }

  /**
   * Get a FileSystem instance based on the uri, the passed in
   * configuration and the user.
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return the filesystem instance
   * @throws IOException failure to load
   * @throws InterruptedException If the {@code UGI.doAs()} call was
   * somehow interrupted.
   */
  public static FileSystem get(final URI uri, final Configuration conf,
        final String user) throws IOException, InterruptedException {
    String ticketCachePath =
      conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
    UserGroupInformation ugi =
        UserGroupInformation.getBestUGI(ticketCachePath, user);
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws IOException {
        return get(uri, conf);
      }
    });
  }

  /**
   * Returns the configured FileSystem implementation.
   * @param conf the configuration to use
   */
  public static FileSystem get(Configuration conf) throws IOException {
    return get(getDefaultUri(conf), conf);
  }

  /**
   * Get the default FileSystem URI from a configuration.
   * @param conf the configuration to use
   * @return the uri of the default filesystem
   */
  public static URI getDefaultUri(Configuration conf) {
    URI uri = URI.create(fixName(conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)));
    if (uri.getScheme() == null) {
      throw new IllegalArgumentException("No scheme in default FS: " + uri);
    }
    return uri;
  }

  /**
   * Set the default FileSystem URI in a configuration.
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  public static void setDefaultUri(Configuration conf, URI uri) {
    conf.set(FS_DEFAULT_NAME_KEY, uri.toString());
  }

  /** Set the default FileSystem URI in a configuration.
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  public static void setDefaultUri(Configuration conf, String uri) {
    setDefaultUri(conf, URI.create(fixName(uri)));
  }

  /**
   * Initialize a FileSystem.
   *
   * Called after the new FileSystem instance is constructed, and before it
   * is ready for use.
   *
   * FileSystem implementations overriding this method MUST forward it to
   * their superclass, though the order in which it is done, and whether
   * to alter the configuration before the invocation are options of the
   * subclass.
   * @param name a URI whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   * @throws IOException on any failure to initialize this instance.
   * @throws IllegalArgumentException if the URI is considered invalid.
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    final String scheme;
    if (name.getScheme() == null || name.getScheme().isEmpty()) {
      scheme = getDefaultUri(conf).getScheme();
    } else {
      scheme = name.getScheme();
    }
    statistics = getStatistics(scheme, getClass());
    resolveSymlinks = conf.getBoolean(
        CommonConfigurationKeysPublic.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY,
        CommonConfigurationKeysPublic.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT);
  }

  /**
   * Return the protocol scheme for this FileSystem.
   * <p>
   * This implementation throws an <code>UnsupportedOperationException</code>.
   *
   * @return the protocol scheme for this FileSystem.
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public String getScheme() {
    throw new UnsupportedOperationException("Not implemented by the "
        + getClass().getSimpleName() + " FileSystem implementation");
  }

  /**
   * Returns a URI which identifies this FileSystem.
   *
   * @return the URI of this filesystem.
   */
  public abstract URI getUri();

  /**
   * Return a canonicalized form of this FileSystem's URI.
   *
   * The default implementation simply calls {@link #canonicalizeUri(URI)}
   * on the filesystem's own URI, so subclasses typically only need to
   * implement that method.
   *
   * @see #canonicalizeUri(URI)
   */
  protected URI getCanonicalUri() {
    return canonicalizeUri(getUri());
  }

  /**
   * Canonicalize the given URI.
   *
   * This is implementation-dependent, and may for example consist of
   * canonicalizing the hostname using DNS and adding the default
   * port if not specified.
   *
   * The default implementation simply fills in the default port if
   * not specified and if {@link #getDefaultPort()} returns a
   * default port.
   *
   * @return URI
   * @see NetUtils#getCanonicalUri(URI, int)
   */
  protected URI canonicalizeUri(URI uri) {
    if (uri.getPort() == -1 && getDefaultPort() > 0) {
      // reconstruct the uri with the default port set
      try {
        uri = new URI(uri.getScheme(), uri.getUserInfo(),
            uri.getHost(), getDefaultPort(),
            uri.getPath(), uri.getQuery(), uri.getFragment());
      } catch (URISyntaxException e) {
        // Should never happen!
        throw new AssertionError("Valid URI became unparseable: " +
            uri);
      }
    }

    return uri;
  }

  /**
   * Get the default port for this FileSystem.
   * @return the default port or 0 if there isn't one
   */
  protected int getDefaultPort() {
    return 0;
  }

  protected static FileSystem getFSofPath(final Path absOrFqPath,
      final Configuration conf)
      throws UnsupportedFileSystemException, IOException {
    absOrFqPath.checkNotSchemeWithRelative();
    absOrFqPath.checkNotRelative();

    // Uses the default FileSystem if not fully qualified
    return get(absOrFqPath.toUri(), conf);
  }

  /**
   * Get a canonical service name for this FileSystem.
   * The token cache is the only user of the canonical service name,
   * and uses it to lookup this FileSystem's service tokens.
   * If the file system provides a token of its own then it must have a
   * canonical name, otherwise the canonical name can be null.
   *
   * Default implementation: If the FileSystem has child file systems
   * (such as an embedded file system) then it is assumed that the FS has no
   * tokens of its own and hence returns a null name; otherwise a service
   * name is built using Uri and port.
   *
   * @return a service string that uniquely identifies this file system, null
   *         if the filesystem does not implement tokens
   * @see SecurityUtil#buildDTServiceName(URI, int)
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public String getCanonicalServiceName() {
    return (getChildFileSystems() == null)
      ? SecurityUtil.buildDTServiceName(getUri(), getDefaultPort())
      : null;
  }

  /** @deprecated call {@link #getUri()} instead.*/
  @Deprecated
  public String getName() { return getUri().toString(); }

  /** @deprecated call {@link #get(URI, Configuration)} instead. */
  @Deprecated
  public static FileSystem getNamed(String name, Configuration conf)
    throws IOException {
    return get(URI.create(fixName(name)), conf);
  }

  /** Update old-format filesystem names, for back-compatibility.  This should
   * eventually be replaced with a checkName() method that throws an exception
   * for old-format names.
   */
  private static String fixName(String name) {
    // convert old-format name to new-format name
    if (name.equals("local")) {         // "local" is now "file:///".
      LOGGER.warn("\"local\" is a deprecated filesystem name."
               +" Use \"file:///\" instead.");
      name = "file:///";
    } else if (name.indexOf('/')==-1) {   // unqualified is "hdfs://"
      LOGGER.warn("\""+name+"\" is a deprecated filesystem name."
               +" Use \"hdfs://"+name+"/\" instead.");
      name = "hdfs://"+name;
    }
    return name;
  }

  /**
   * Get the local FileSystem.
   * @param conf the configuration to configure the FileSystem with
   * if it is newly instantiated.
   * @return a LocalFileSystem
   * @throws IOException if somehow the local FS cannot be instantiated.
   */
  public static LocalFileSystem getLocal(Configuration conf)
    throws IOException {
    return (LocalFileSystem)get(LocalFileSystem.NAME, conf);
  }

  /**
   * Get a FileSystem for this URI's scheme and authority.
   * <ol>
   * <li>
   *   If the configuration has the property
   *   {@code "fs.$SCHEME.impl.disable.cache"} set to true,
   *   a new instance will be created, initialized with the supplied URI and
   *   configuration, then returned without being cached.
   * </li>
   * <li>
   *   If the there is a cached FS instance matching the same URI, it will
   *   be returned.
   * </li>
   * <li>
   *   Otherwise: a new FS instance will be created, initialized with the
   *   configuration and URI, cached and returned to the caller.
   * </li>
   * </ol>
   * @throws IOException if the FileSystem cannot be instantiated.
   */
  public static FileSystem get(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null && authority == null) {     // use default FS
      return get(conf);
    }

    if (scheme != null && authority == null) {     // no authority
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return get(defaultUri, conf);              // return default
      }
    }
    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    if (conf.getBoolean(disableCacheName, false)) {
      LOGGER.debug("Bypassing cache to create filesystem {}", uri);
      return createFileSystem(uri, conf);
    }

    return CACHE.get(uri, conf);
  }

  /**
   * Returns the FileSystem for this URI's scheme and authority and the
   * given user. Internally invokes {@link #newInstance(URI, Configuration)}
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return filesystem instance
   * @throws IOException if the FileSystem cannot be instantiated.
   * @throws InterruptedException If the {@code UGI.doAs()} call was
   *         somehow interrupted.
   */
  public static FileSystem newInstance(final URI uri, final Configuration conf,
      final String user) throws IOException, InterruptedException {
    String ticketCachePath =
      conf.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
    UserGroupInformation ugi =
        UserGroupInformation.getBestUGI(ticketCachePath, user);
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws IOException {
        return newInstance(uri, conf);
      }
    });
  }

  /**
   * Returns the FileSystem for this URI's scheme and authority.
   * The entire URI is passed to the FileSystem instance's initialize method.
   * This always returns a new FileSystem object.
   * @param uri FS URI
   * @param config configuration to use
   * @return the new FS instance
   * @throws IOException FS creation or initialization failure.
   */
  public static FileSystem newInstance(URI uri, Configuration config)
      throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null) {                       // no scheme: use default FS
      return newInstance(config);
    }

    if (authority == null) {                       // no authority
      URI defaultUri = getDefaultUri(config);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return newInstance(defaultUri, config);              // return default
      }
    }
    return CACHE.getUnique(uri, config);
  }

  /**
   * Returns a unique configured FileSystem implementation for the default
   * filesystem of the supplied configuration.
   * This always returns a new FileSystem object.
   * @param conf the configuration to use
   * @return the new FS instance
   * @throws IOException FS creation or initialization failure.
   */
  public static FileSystem newInstance(Configuration conf) throws IOException {
    return newInstance(getDefaultUri(conf), conf);
  }

  /**
   * Get a unique local FileSystem object.
   * @param conf the configuration to configure the FileSystem with
   * @return a new LocalFileSystem object.
   * @throws IOException FS creation or initialization failure.
   */
  public static LocalFileSystem newInstanceLocal(Configuration conf)
    throws IOException {
    return (LocalFileSystem)newInstance(LocalFileSystem.NAME, conf);
  }

  /**
   * Close all cached FileSystem instances. After this operation, they
   * may not be used in any operations.
   *
   * @throws IOException a problem arose closing one or more filesystem.
   */
  public static void closeAll() throws IOException {
    CACHE.closeAll();
  }

  /**
   * Close all cached FileSystem instances for a given UGI.
   * Be sure those filesystems are not used anymore.
   * @param ugi user group info to close
   * @throws IOException a problem arose closing one or more filesystem.
   */
  public static void closeAllForUGI(UserGroupInformation ugi)
  throws IOException {
    CACHE.closeAll(ugi);
  }

  /**
   * Qualify a path to one which uses this FileSystem and, if relative,
   * made absolute.
   * @param path to qualify.
   * @return this path if it contains a scheme and authority and is absolute, or
   * a new path that includes a path and authority and is fully qualified
   * @see Path#makeQualified(URI, Path)
   * @throws IllegalArgumentException if the path has a schema/URI different
   * from this FileSystem.
   */
  public Path makeQualified(Path path) {
    checkPath(path);
    return path.makeQualified(this.getUri(), this.getWorkingDirectory());
  }

  /**
   * Get a new delegation token for this FileSystem.
   * This is an internal method that should have been declared protected
   * but wasn't historically.
   * Callers should use {@link #addDelegationTokens(String, Credentials)}
   *
   * @param renewer the account name that is allowed to renew the token.
   * @return a new delegation token or null if the FS does not support tokens.
   * @throws IOException on any problem obtaining a token
   */
  @InterfaceAudience.Private()
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return null;
  }

  /**
   * Obtain all delegation tokens used by this FileSystem that are not
   * already present in the given Credentials. Existing tokens will neither
   * be verified as valid nor having the given renewer.  Missing tokens will
   * be acquired and added to the given Credentials.
   *
   * Default Impl: works for simple FS with its own token
   * and also for an embedded FS whose tokens are those of its
   * child FileSystems (i.e. the embedded FS has no tokens of its own).
   *
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add new delegation tokens
   * @return list of new delegation tokens
   * @throws IOException problems obtaining a token
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public Token<?>[] addDelegationTokens(
      final String renewer, Credentials credentials) throws IOException {
    if (credentials == null) {
      credentials = new Credentials();
    }
    final List<Token<?>> tokens = new ArrayList<>();
    collectDelegationTokens(renewer, credentials, tokens);
    return tokens.toArray(new Token<?>[tokens.size()]);
  }

  /**
   * Recursively obtain the tokens for this FileSystem and all descendant
   * FileSystems as determined by {@link #getChildFileSystems()}.
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add the new delegation tokens
   * @param tokens list in which to add acquired tokens
   * @throws IOException problems obtaining a token
   */
  private void collectDelegationTokens(final String renewer,
                                       final Credentials credentials,
                                       final List<Token<?>> tokens)
                                           throws IOException {
    final String serviceName = getCanonicalServiceName();
    // Collect token of the this filesystem and then of its embedded children
    if (serviceName != null) { // fs has token, grab it
      final Text service = new Text(serviceName);
      Token<?> token = credentials.getToken(service);
      if (token == null) {
        token = getDelegationToken(renewer);
        if (token != null) {
          tokens.add(token);
          credentials.addToken(service, token);
        }
      }
    }
    // Now collect the tokens from the children
    final FileSystem[] children = getChildFileSystems();
    if (children != null) {
      for (final FileSystem fs : children) {
        fs.collectDelegationTokens(renewer, credentials, tokens);
      }
    }
  }

  /**
   * Get all the immediate child FileSystems embedded in this FileSystem.
   * It does not recurse and get grand children.  If a FileSystem
   * has multiple child FileSystems, then it must return a unique list
   * of those FileSystems.  Default is to return null to signify no children.
   *
   * @return FileSystems that are direct children of this FileSystem,
   *         or null for "no children"
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS" })
  @VisibleForTesting
  public FileSystem[] getChildFileSystems() {
    return null;
  }

  /**
   * Create a file with the provided permission.
   *
   * The permission of the file is set to be the provided permission as in
   * setPermission, not permission&~umask
   *
   * The HDFS implementation is implemented using two RPCs.
   * It is understood that it is inefficient,
   * but the implementation is thread-safe. The other option is to change the
   * value of umask in configuration to be 0, but it is not thread-safe.
   *
   * @param fs FileSystem
   * @param file the name of the file to be created
   * @param permission the permission of the file
   * @return an output stream
   * @throws IOException IO failure
   */
  public static FSDataOutputStream create(FileSystem fs,
      Path file, FsPermission permission) throws IOException {
    // create the file with default permission
    FSDataOutputStream out = fs.create(file);
    // set its permission to the supplied one
    fs.setPermission(file, permission);
    return out;
  }

  /**
   * Create a directory with the provided permission.
   * The permission of the directory is set to be the provided permission as in
   * setPermission, not permission&~umask
   *
   * @see #create(FileSystem, Path, FsPermission)
   *
   * @param fs FileSystem handle
   * @param dir the name of the directory to be created
   * @param permission the permission of the directory
   * @return true if the directory creation succeeds; false otherwise
   * @throws IOException A problem creating the directories.
   */
  public static boolean mkdirs(FileSystem fs, Path dir, FsPermission permission)
      throws IOException {
    // create the directory using the default permission
    boolean result = fs.mkdirs(dir);
    // set its permission to be the supplied one
    fs.setPermission(dir, permission);
    return result;
  }

  ///////////////////////////////////////////////////////////////
  // FileSystem
  ///////////////////////////////////////////////////////////////

  protected FileSystem() {
    super(null);
  }

  /**
   * Check that a Path belongs to this FileSystem.
   *
   * The base implementation performs case insensitive equality checks
   * of the URIs' schemes and authorities. Subclasses may implement slightly
   * different checks.
   * @param path to check
   * @throws IllegalArgumentException if the path is not considered to be
   * part of this FileSystem.
   *
   */
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String thatScheme = uri.getScheme();
    if (thatScheme == null)                // fs is relative
      return;
    URI thisUri = getCanonicalUri();
    String thisScheme = thisUri.getScheme();
    //authority and scheme are not case sensitive
    if (thisScheme.equalsIgnoreCase(thatScheme)) {// schemes match
      String thisAuthority = thisUri.getAuthority();
      String thatAuthority = uri.getAuthority();
      if (thatAuthority == null &&                // path's authority is null
          thisAuthority != null) {                // fs has an authority
        URI defaultUri = getDefaultUri(getConf());
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme())) {
          uri = defaultUri; // schemes match, so use this uri instead
        } else {
          uri = null; // can't determine auth of the path
        }
      }
      if (uri != null) {
        // canonicalize uri before comparing with this fs
        uri = canonicalizeUri(uri);
        thatAuthority = uri.getAuthority();
        if (thisAuthority == thatAuthority ||       // authorities match
            (thisAuthority != null &&
             thisAuthority.equalsIgnoreCase(thatAuthority)))
          return;
      }
    }
    throw new IllegalArgumentException("Wrong FS: " + path +
                                       ", expected: " + this.getUri());
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file.  For nonexistent
   * file or regions, {@code null} is returned.
   *
   * <pre>
   *   if f == null :
   *     result = null
   *   elif f.getLen() <= start:
   *     result = []
   *   else result = [ locations(FS, b) for b in blocks(FS, p, s, s+l)]
   * </pre>
   * This call is most helpful with and distributed filesystem
   * where the hostnames of machines that contain blocks of the given file
   * can be determined.
   *
   * The default implementation returns an array containing one element:
   * <pre>
   * BlockLocation( { "localhost:9866" },  { "localhost" }, 0, file.getLen())
   * </pre>
   *
   * In HDFS, if file is three-replicated, the returned array contains
   * elements like:
   * <pre>
   * BlockLocation(offset: 0, length: BLOCK_SIZE,
   *   hosts: {"host1:9866", "host2:9866, host3:9866"})
   * BlockLocation(offset: BLOCK_SIZE, length: BLOCK_SIZE,
   *   hosts: {"host2:9866", "host3:9866, host4:9866"})
   * </pre>
   *
   * And if a file is erasure-coded, the returned BlockLocation are logical
   * block groups.
   *
   * Suppose we have a RS_3_2 coded file (3 data units and 2 parity units).
   * 1. If the file size is less than one stripe size, say 2 * CELL_SIZE, then
   * there will be one BlockLocation returned, with 0 offset, actual file size
   * and 4 hosts (2 data blocks and 2 parity blocks) hosting the actual blocks.
   * 3. If the file size is less than one group size but greater than one
   * stripe size, then there will be one BlockLocation returned, with 0 offset,
   * actual file size with 5 hosts (3 data blocks and 2 parity blocks) hosting
   * the actual blocks.
   * 4. If the file size is greater than one group size, 3 * BLOCK_SIZE + 123
   * for example, then the result will be like:
   * <pre>
   * BlockLocation(offset: 0, length: 3 * BLOCK_SIZE, hosts: {"host1:9866",
   *   "host2:9866","host3:9866","host4:9866","host5:9866"})
   * BlockLocation(offset: 3 * BLOCK_SIZE, length: 123, hosts: {"host1:9866",
   *   "host4:9866", "host5:9866"})
   * </pre>
   *
   * @param file FilesStatus to get data from
   * @param start offset into the given file
   * @param len length for which to get locations for
   * @throws IOException IO failure
   */
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) throws IOException {
    if (file == null) {
      return null;
    }

    if (start < 0 || len < 0) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() <= start) {
      return new BlockLocation[0];

    }
    String[] name = {"localhost:9866"};
    String[] host = {"localhost"};
    return new BlockLocation[] {
      new BlockLocation(name, host, 0, file.getLen()) };
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file.  For a nonexistent
   * file or regions, {@code null} is returned.
   *
   * This call is most helpful with location-aware distributed
   * filesystems, where it returns hostnames of machines that
   * contain the given file.
   *
   * A FileSystem will normally return the equivalent result
   * of passing the {@code FileStatus} of the path to
   * {@link #getFileBlockLocations(FileStatus, long, long)}
   *
   * @param p path is used to identify an FS since an FS could have
   *          another FS that it could be delegating the call to
   * @param start offset into the given file
   * @param len length for which to get locations for
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException IO failure
   */
  public BlockLocation[] getFileBlockLocations(Path p,
      long start, long len) throws IOException {
    if (p == null) {
      throw new NullPointerException();
    }
    FileStatus file = getFileStatus(p);
    return getFileBlockLocations(file, start, len);
  }

  /**
   * Return a set of server default configuration values.
   * @return server default configuration values
   * @throws IOException IO failure
   * @deprecated use {@link #getServerDefaults(Path)} instead
   */
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    Configuration config = getConf();
    // CRC32 is chosen as default as it is available in all
    // releases that support checksum.
    // The client trash configuration is ignored.
    return new FsServerDefaults(getDefaultBlockSize(),
        config.getInt("io.bytes.per.checksum", 512),
        64 * 1024,
        getDefaultReplication(),
        config.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
        false,
        FS_TRASH_INTERVAL_DEFAULT,
        DataChecksum.Type.CRC32,
        "");
  }

  /**
   * Return a set of server default configuration values.
   * @param p path is used to identify an FS since an FS could have
   *          another FS that it could be delegating the call to
   * @return server default configuration values
   * @throws IOException IO failure
   */
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return getServerDefaults();
  }

  /**
   * Return the fully-qualified path of path, resolving the path
   * through any symlinks or mount point.
   * @param p path to be resolved
   * @return fully qualified path
   * @throws FileNotFoundException if the path is not present
   * @throws IOException for any other error
   */
   public Path resolvePath(final Path p) throws IOException {
     checkPath(p);
     return getFileStatus(p).getPath();
   }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   */
  public abstract FSDataInputStream open(Path f, int bufferSize)
    throws IOException;

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file to open
   * @throws IOException IO failure
   */
  public FSDataInputStream open(Path f) throws IOException {
    return open(f, getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
        IO_FILE_BUFFER_SIZE_DEFAULT));
  }

  /**
   * Open an FSDataInputStream matching the PathHandle instance. The
   * implementation may encode metadata in PathHandle to address the
   * resource directly and verify that the resource referenced
   * satisfies constraints specified at its construciton.
   * @param fd PathHandle object returned by the FS authority.
   * @param bufferSize the size of the buffer to use
   * @throws IOException IO failure
   * @throws UnsupportedOperationException If not overridden by subclass
   */
  public FSDataInputStream open(PathHandle fd, int bufferSize)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a durable, serializable handle to the referent of the given
   * entity.
   * @param stat Referent in the target FileSystem
   * @param opt If absent, assume {@link HandleOpt#path()}.
   * @throws IllegalArgumentException If the FileStatus does not belong to
   *         this FileSystem
   * @throws UnsupportedOperationException If {@link #createPathHandle}
   *         not overridden by subclass.
   * @throws UnsupportedOperationException If this FileSystem cannot enforce
   *         the specified constraints.
   */
  public final PathHandle getPathHandle(FileStatus stat, HandleOpt... opt) {
    if (null == opt || 0 == opt.length) {
      return createPathHandle(stat, HandleOpt.path());
    }
    return createPathHandle(stat, opt);
  }

  /**
   * Hook to implement support for {@link PathHandle} operations.
   * @param stat Referent in the target FileSystem
   * @param opt Constraints that determine the validity of the
   *            {@link PathHandle} reference.
   */
  protected PathHandle createPathHandle(FileStatus stat, HandleOpt... opt) {
    throw new UnsupportedOperationException();
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f) throws IOException {
    return create(f, true);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an exception will be thrown.
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f, boolean overwrite)
      throws IOException {
    return create(f, overwrite,
                  getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
                      IO_FILE_BUFFER_SIZE_DEFAULT),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param progress to report progress
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f, Progressable progress)
      throws IOException {
    return create(f, true,
                  getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
                      IO_FILE_BUFFER_SIZE_DEFAULT),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f, short replication)
      throws IOException {
    return create(f, true,
                  getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
                      IO_FILE_BUFFER_SIZE_DEFAULT),
                  replication,
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   * @param progress to report progress
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f, short replication,
      Progressable progress) throws IOException {
    return create(f, true,
                  getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
                      IO_FILE_BUFFER_SIZE_DEFAULT),
                  replication, getDefaultBlockSize(f), progress);
  }


  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file to create
   * @param overwrite if a path with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize,
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }

  /**
   * Create an {@link FSDataOutputStream} at the indicated Path
   * with write-progress reporting.
   *
   * The frequency of callbacks is implementation-specific; it may be "none".
   * @param f the path of the file to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize,
                                   Progressable progress
                                   ) throws IOException {
    return create(f, overwrite, bufferSize,
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }


  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize) throws IOException {
    return create(f, overwrite, bufferSize, replication, blockSize, null);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @throws IOException IO failure
   */
  public FSDataOutputStream create(Path f,
                                            boolean overwrite,
                                            int bufferSize,
                                            short replication,
                                            long blockSize,
                                            Progressable progress
                                            ) throws IOException {
    return this.create(f, FsCreateModes.applyUMask(
        FsPermission.getFileDefault(), FsPermission.getUMask(getConf())),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission file permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size
   * @param progress the progress reporter
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
  public abstract FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException;

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission file permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size
   * @param progress the progress reporter
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, flags, bufferSize, replication,
        blockSize, progress, null);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with a custom
   * checksum option.
   * @param f the file name to open
   * @param permission file permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size
   * @param progress the progress reporter
   * @param checksumOpt checksum parameter. If null, the values
   *        found in conf will be used.
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {
    // Checksum options are ignored by default. The file systems that
    // implement checksum need to override this method. The full
    // support is currently only available in DFS.
    return create(f, permission, flags.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
  }

  /**
   * This create has been added to support the FileContext that processes
   * the permission with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   * @throws IOException IO failure
   */
  @Deprecated
  protected FSDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission,
      EnumSet<CreateFlag> flag,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      ChecksumOpt checksumOpt) throws IOException {

    boolean pathExists = exists(f);
    CreateFlag.validate(f, pathExists, flag);

    // Default impl  assumes that permissions do not matter and
    // nor does the bytesPerChecksum  hence
    // calling the regular create is good enough.
    // FSs that implement permissions should override this.

    if (pathExists && flag.contains(CreateFlag.APPEND)) {
      return append(f, bufferSize, progress);
    }

    return this.create(f, absolutePermission,
        flag.contains(CreateFlag.OVERWRITE), bufferSize, replication,
        blockSize, progress);
  }

  /**
   * This version of the mkdirs method assumes that the permission is absolute.
   * It has been added to support the FileContext that processes the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   * @param f path
   * @param absolutePermission permissions
   * @return true if the directory was actually created.
   * @throws IOException IO failure
   * @see #mkdirs(Path, FsPermission)
   */
  @Deprecated
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
    throws IOException {
   return this.mkdirs(f, absolutePermission);
  }


  /**
   * This version of the mkdirs method assumes that the permission is absolute.
   * It has been added to support the FileContext that processes the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected void primitiveMkdir(Path f, FsPermission absolutePermission,
                    boolean createParent)
    throws IOException {

    if (!createParent) { // parent must exist.
      // since the this.mkdirs makes parent dirs automatically
      // we must throw exception if parent does not exist.
      final FileStatus stat = getFileStatus(f.getParent());
      if (stat == null) {
        throw new FileNotFoundException("Missing parent:" + f);
      }
      if (!stat.isDirectory()) {
        throw new ParentNotDirectoryException("parent is not a dir");
      }
      // parent does exist - go ahead with mkdir of leaf
    }
    // Default impl is to assume that permissions do not matter and hence
    // calling the regular mkdirs is good enough.
    // FSs that implement permissions should override this.
    if (!this.mkdirs(f, absolutePermission)) {
      throw new IOException("mkdir of "+ f + " failed");
    }
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting. Same as create(), except fails if parent directory doesn't
   * already exist.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size
   * @param progress the progress reporter
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream createNonRecursive(Path f,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting. Same as create(), except fails if parent directory doesn't
   * already exist.
   * @param f the file name to open
   * @param permission file permission
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size
   * @param progress the progress reporter
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
   public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
       boolean overwrite, int bufferSize, short replication, long blockSize,
       Progressable progress) throws IOException {
     return createNonRecursive(f, permission,
         overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
             : EnumSet.of(CreateFlag.CREATE), bufferSize,
             replication, blockSize, progress);
   }

   /**
    * Opens an FSDataOutputStream at the indicated Path with write-progress
    * reporting. Same as create(), except fails if parent directory doesn't
    * already exist.
    * @param f the file name to open
    * @param permission file permission
    * @param flags {@link CreateFlag}s to use for this stream.
    * @param bufferSize the size of the buffer to be used.
    * @param replication required block replication for the file.
    * @param blockSize block size
    * @param progress the progress reporter
    * @throws IOException IO failure
    * @see #setPermission(Path, FsPermission)
    */
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      throw new IOException("createNonRecursive unsupported for this filesystem "
          + this.getClass());
    }

  /**
   * Creates the given Path as a brand-new zero-length file.  If
   * create fails, or if it already existed, return false.
   * <i>Important: the default implementation is not atomic</i>
   * @param f path to use for create
   * @throws IOException IO failure
   */
  public boolean createNewFile(Path f) throws IOException {
    if (exists(f)) {
      return false;
    } else {
      create(f, false, getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
          IO_FILE_BUFFER_SIZE_DEFAULT)).close();
      return true;
    }
  }

  /**
   * Append to an existing file (optional operation).
   * Same as
   * {@code append(f, getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
   *     IO_FILE_BUFFER_SIZE_DEFAULT), null)}
   * @param f the existing file to be appended.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public FSDataOutputStream append(Path f) throws IOException {
    return append(f, getConf().getInt(IO_FILE_BUFFER_SIZE_KEY,
        IO_FILE_BUFFER_SIZE_DEFAULT), null);
  }

  /**
   * Append to an existing file (optional operation).
   * Same as append(f, bufferSize, null).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return append(f, bufferSize, null);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public abstract FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException;

  /**
   * Concat existing files together.
   * @param trg the path to the target destination.
   * @param psrcs the paths to the sources to use for the concatenation.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public void concat(final Path trg, final Path [] psrcs) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " +
        getClass().getSimpleName() + " FileSystem implementation");
  }

 /**
   * Get the replication factor.
   *
   * @deprecated Use {@link #getFileStatus(Path)} instead
   * @param src file name
   * @return file replication
   * @throws FileNotFoundException if the path does not resolve.
   * @throws IOException an IO failure
   */
  @Deprecated
  public short getReplication(Path src) throws IOException {
    return getFileStatus(src).getReplication();
  }

  /**
   * Set the replication for an existing file.
   * If a filesystem does not support replication, it will always
   * return true: the check for a file existing may be bypassed.
   * This is the default behavior.
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful, or the feature in unsupported;
   *         false if replication is supported but the file does not exist,
   *         or is a directory
   */
  public boolean setReplication(Path src, short replication)
    throws IOException {
    return true;
  }

  /**
   * Renames Path src to Path dst.
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   * @return true if rename is successful
   */
  public abstract boolean rename(Path src, Path dst) throws IOException;

  /**
   * Renames Path src to Path dst
   * <ul>
   *   <li>Fails if src is a file and dst is a directory.</li>
   *   <li>Fails if src is a directory and dst is a file.</li>
   *   <li>Fails if the parent of dst does not exist or is a file.</li>
   * </ul>
   * <p>
   * If OVERWRITE option is not passed as an argument, rename fails
   * if the dst already exists.
   * <p>
   * If OVERWRITE option is passed as an argument, rename overwrites
   * the dst if it is a file or an empty directory. Rename fails if dst is
   * a non-empty directory.
   * <p>
   * Note that atomicity of rename is dependent on the file system
   * implementation. Please refer to the file system documentation for
   * details. This default implementation is non atomic.
   * <p>
   * This method is deprecated since it is a temporary method added to
   * support the transition from FileSystem to FileContext for user
   * applications.
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws FileNotFoundException src path does not exist, or the parent
   * path of dst does not exist.
   * @throws FileAlreadyExistsException dest path exists and is a file
   * @throws ParentNotDirectoryException if the parent path of dest is not
   * a directory
   * @throws IOException on failure
   */
  @Deprecated
  protected void rename(final Path src, final Path dst,
      final Rename... options) throws IOException {
    // Default implementation
    final FileStatus srcStatus = getFileLinkStatus(src);
    if (srcStatus == null) {
      throw new FileNotFoundException("rename source " + src + " not found.");
    }

    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }

    FileStatus dstStatus;
    try {
      dstStatus = getFileLinkStatus(dst);
    } catch (IOException e) {
      dstStatus = null;
    }
    if (dstStatus != null) {
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new IOException("Source " + src + " Destination " + dst
            + " both should be either file or directory");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException("rename destination " + dst
            + " already exists.");
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDirectory()) {
        FileStatus[] list = listStatus(dst);
        if (list != null && list.length != 0) {
          throw new IOException(
              "rename cannot overwrite non empty destination directory " + dst);
        }
      }
      delete(dst, false);
    } else {
      final Path parent = dst.getParent();
      final FileStatus parentStatus = getFileStatus(parent);
      if (parentStatus == null) {
        throw new FileNotFoundException("rename destination parent " + parent
            + " not found.");
      }
      if (!parentStatus.isDirectory()) {
        throw new ParentNotDirectoryException("rename destination parent " + parent
            + " is a file.");
      }
    }
    if (!rename(src, dst)) {
      throw new IOException("rename from " + src + " to " + dst + " failed.");
    }
  }

  /**
   * Truncate the file in the indicated path to the indicated size.
   * <ul>
   *   <li>Fails if path is a directory.</li>
   *   <li>Fails if path does not exist.</li>
   *   <li>Fails if path is not closed.</li>
   *   <li>Fails if new size is greater than current size.</li>
   * </ul>
   * @param f The path to the file to be truncated
   * @param newLength The size the file is to be truncated to
   *
   * @return <code>true</code> if the file has been truncated to the desired
   * <code>newLength</code> and is immediately available to be reused for
   * write operations such as <code>append</code>, or
   * <code>false</code> if a background process of adjusting the length of
   * the last block has been started, and clients should wait for it to
   * complete before proceeding with further file updates.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public boolean truncate(Path f, long newLength) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " +
        getClass().getSimpleName() + " FileSystem implementation");
  }

  /**
   * Delete a file/directory.
   * @deprecated Use {@link #delete(Path, boolean)} instead.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  /** Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException IO failure
   */
  public abstract boolean delete(Path f, boolean recursive) throws IOException;

  /**
   * Mark a path to be deleted when its FileSystem is closed.
   * When the JVM shuts down cleanly, all cached FileSystem objects will be
   * closed automatically —these the marked paths will be deleted as a result.
   *
   * If a FileSystem instance is not cached, i.e. has been created with
   * {@link #createFileSystem(URI, Configuration)}, then the paths will
   * be deleted in when {@link #close()} is called on that instance.
   *
   * The path must exist in the filesystem at the time of the method call;
   * it does not have to exist at the time of JVM shutdown.
   *
   * Notes
   * <ol>
   *   <li>Clean shutdown of the JVM cannot be guaranteed.</li>
   *   <li>The time to shut down a FileSystem will depends on the number of
   *   files to delete. For filesystems where the cost of checking
   *   for the existence of a file/directory and the actual delete operation
   *   (for example: object stores) is high, the time to shutdown the JVM can be
   *   significantly extended by over-use of this feature.</li>
   *   <li>Connectivity problems with a remote filesystem may delay shutdown
   *   further, and may cause the files to not be deleted.</li>
   * </ol>
   * @param f the path to delete.
   * @return  true if deleteOnExit is successful, otherwise false.
   * @throws IOException IO failure
   */
  public boolean deleteOnExit(Path f) throws IOException {
    if (!exists(f)) {
      return false;
    }
    synchronized (deleteOnExit) {
      deleteOnExit.add(f);
    }
    return true;
  }

  /**
   * Cancel the scheduled deletion of the path when the FileSystem is closed.
   * @param f the path to cancel deletion
   * @return true if the path was found in the delete-on-exit list.
   */
  public boolean cancelDeleteOnExit(Path f) {
    synchronized (deleteOnExit) {
      return deleteOnExit.remove(f);
    }
  }

  /**
   * Delete all paths that were marked as delete-on-exit. This recursively
   * deletes all files and directories in the specified paths.
   *
   * The time to process this operation is {@code O(paths)}, with the actual
   * time dependent on the time for existence and deletion operations to
   * complete, successfully or not.
   */
  protected void processDeleteOnExit() {
    synchronized (deleteOnExit) {
      for (Iterator<Path> iter = deleteOnExit.iterator(); iter.hasNext();) {
        Path path = iter.next();
        try {
          if (exists(path)) {
            delete(path, true);
          }
        }
        catch (IOException e) {
          LOGGER.info("Ignoring failure to deleteOnExit for path {}", path);
        }
        iter.remove();
      }
    }
  }

  /** Check if a path exists.
   *
   * It is highly discouraged to call this method back to back with other
   * {@link #getFileStatus(Path)} calls, as this will involve multiple redundant
   * RPC calls in HDFS.
   *
   * @param f source path
   * @return true if the path exists
   * @throws IOException IO failure
   */
  public boolean exists(Path f) throws IOException {
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /** True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus
   * returned by getFileStatus() or listStatus() methods.
   *
   * @param f path to check
   * @throws IOException IO failure
   * @deprecated Use {@link #getFileStatus(Path)} instead
   */
  @Deprecated
  public boolean isDirectory(Path f) throws IOException {
    try {
      return getFileStatus(f).isDirectory();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /** True iff the named path is a regular file.
   * Note: Avoid using this method. Instead reuse the FileStatus
   * returned by {@link #getFileStatus(Path)} or listStatus() methods.
   *
   * @param f path to check
   * @throws IOException IO failure
   * @deprecated Use {@link #getFileStatus(Path)} instead
   */
  @Deprecated
  public boolean isFile(Path f) throws IOException {
    try {
      return getFileStatus(f).isFile();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /**
   * The number of bytes in a file.
   * @return the number of bytes; 0 for a directory
   * @deprecated Use {@link #getFileStatus(Path)} instead.
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException IO failure
   */
  @Deprecated
  public long getLength(Path f) throws IOException {
    return getFileStatus(f).getLen();
  }

  /** Return the {@link ContentSummary} of a given {@link Path}.
   * @param f path to use
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException IO failure
   */
  public ContentSummary getContentSummary(Path f) throws IOException {
    FileStatus status = getFileStatus(f);
    if (status.isFile()) {
      // f is a file
      long length = status.getLen();
      return new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(length).build();
    }
    // f is a directory
    long[] summary = {0, 0, 1};
    for(FileStatus s : listStatus(f)) {
      long length = s.getLen();
      ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath()) :
          new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(length).build();
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
    }
    return new ContentSummary.Builder().length(summary[0]).
        fileCount(summary[1]).directoryCount(summary[2]).
        spaceConsumed(summary[0]).build();
  }

  /** Return the {@link QuotaUsage} of a given {@link Path}.
   * @param f path to use
   * @return the quota usage
   * @throws IOException IO failure
   */
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    return getContentSummary(f);
  }

  /**
   * The default filter accepts all paths.
   */
  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return true;
      }
    };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   * <p>
   * Will not return null. Expect IOException upon access error.
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public abstract FileStatus[] listStatus(Path f) throws FileNotFoundException,
                                                         IOException;

  /**
   * Represents a batch of directory entries when iteratively listing a
   * directory. This is a private API not meant for use by end users.
   * <p>
   * For internal use by FileSystem subclasses that override
   * {@link FileSystem#listStatusBatch(Path, byte[])} to implement iterative
   * listing.
   */
  @InterfaceAudience.Private
  public static class DirectoryEntries {
    private final FileStatus[] entries;
    private final byte[] token;
    private final boolean hasMore;

    public DirectoryEntries(FileStatus[] entries, byte[] token, boolean
        hasMore) {
      this.entries = entries;
      if (token != null) {
        this.token = token.clone();
      } else {
        this.token = null;
      }
      this.hasMore = hasMore;
    }

    public FileStatus[] getEntries() {
      return entries;
    }

    public byte[] getToken() {
      return token;
    }

    public boolean hasMore() {
      return hasMore;
    }
  }

  /**
   * Given an opaque iteration token, return the next batch of entries in a
   * directory. This is a private API not meant for use by end users.
   * <p>
   * This method should be overridden by FileSystem subclasses that want to
   * use the generic {@link FileSystem#listStatusIterator(Path)} implementation.
   * @param f Path to list
   * @param token opaque iteration token returned by previous call, or null
   *              if this is the first call.
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  @InterfaceAudience.Private
  protected DirectoryEntries listStatusBatch(Path f, byte[] token) throws
      FileNotFoundException, IOException {
    // The default implementation returns the entire listing as a single batch.
    // Thus, there is never a second batch, and no need to respect the passed
    // token or set a token in the returned DirectoryEntries.
    FileStatus[] listing = listStatus(f);
    return new DirectoryEntries(listing, null, false);
  }

  /**
   * Filter files/directories in the given path using the user-supplied path
   * filter. Results are added to the given array <code>results</code>.
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  private void listStatus(ArrayList<FileStatus> results, Path f,
      PathFilter filter) throws FileNotFoundException, IOException {
    FileStatus listing[] = listStatus(f);
    Preconditions.checkNotNull(listing, "listStatus should not return NULL");
    for (int i = 0; i < listing.length; i++) {
      if (filter.accept(listing[i].getPath())) {
        results.add(listing[i]);
      }
    }
  }

  /**
   * List corrupted file blocks.
   * @return an iterator over the corrupt files under the given path
   * (may contain duplicates if a file has more than one corrupt block)
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   * @throws IOException IO failure
   */
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    throw new UnsupportedOperationException(getClass().getCanonicalName() +
        " does not support listCorruptFileBlocks");
  }

  /**
   * Filter files/directories in the given path using the user-supplied path
   * filter.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   *
   * @param f
   *          a path name
   * @param filter
   *          the user-supplied path filter
   * @return an array of FileStatus objects for the files under the given path
   *         after applying the filter
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f, PathFilter filter)
                                   throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<>();
    listStatus(results, f, filter);
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * Filter files/directories in the given list of paths using default
   * path filter.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   *
   * @param files
   *          a list of paths
   * @return a list of statuses for the files under the given paths after
   *         applying the filter default Path filter
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files)
      throws FileNotFoundException, IOException {
    return listStatus(files, DEFAULT_FILTER);
  }

  /**
   * Filter files/directories in the given list of paths using user-supplied
   * path filter.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   *
   * @param files
   *          a list of paths
   * @param filter
   *          the user-supplied path filter
   * @return a list of statuses for the files under the given paths after
   *         applying the filter
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files, PathFilter filter)
      throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    for (int i = 0; i < files.length; i++) {
      listStatus(results, files[i], filter);
    }
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * <p>Return all the files that match filePattern and are not checksum
   * files. Results are sorted by their names.
   *
   * <p>
   * A filename pattern is composed of <i>regular</i> characters and
   * <i>special pattern matching</i> characters, which are:
   *
   * <dl>
   *  <dd>
   *   <dl>
   *    <p>
   *    <dt> <tt> ? </tt>
   *    <dd> Matches any single character.
   *
   *    <p>
   *    <dt> <tt> * </tt>
   *    <dd> Matches zero or more characters.
   *
   *    <p>
   *    <dt> <tt> [<i>abc</i>] </tt>
   *    <dd> Matches a single character from character set
   *     <tt>{<i>a,b,c</i>}</tt>.
   *
   *    <p>
   *    <dt> <tt> [<i>a</i>-<i>b</i>] </tt>
   *    <dd> Matches a single character from the character range
   *     <tt>{<i>a...b</i>}</tt>.  Note that character <tt><i>a</i></tt> must be
   *     lexicographically less than or equal to character <tt><i>b</i></tt>.
   *
   *    <p>
   *    <dt> <tt> [^<i>a</i>] </tt>
   *    <dd> Matches a single character that is not from character set or range
   *     <tt>{<i>a</i>}</tt>.  Note that the <tt>^</tt> character must occur
   *     immediately to the right of the opening bracket.
   *
   *    <p>
   *    <dt> <tt> \<i>c</i> </tt>
   *    <dd> Removes (escapes) any special meaning of character <i>c</i>.
   *
   *    <p>
   *    <dt> <tt> {ab,cd} </tt>
   *    <dd> Matches a string from the string set <tt>{<i>ab, cd</i>} </tt>
   *
   *    <p>
   *    <dt> <tt> {ab,c{de,fh}} </tt>
   *    <dd> Matches a string from the string set <tt>{<i>ab, cde, cfh</i>}</tt>
   *
   *   </dl>
   *  </dd>
   * </dl>
   *
   * @param pathPattern a regular expression specifying a pth pattern

   * @return an array of paths that match the path pattern
   * @throws IOException IO failure
   */
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return new Globber(this, pathPattern, DEFAULT_FILTER).glob();
  }

  /**
   * Return an array of {@link FileStatus} objects whose path names match
   * {@code pathPattern} and is accepted by the user-supplied path filter.
   * Results are sorted by their path names.
   *
   * @param pathPattern a regular expression specifying the path pattern
   * @param filter a user-supplied path filter
   * @return null if {@code pathPattern} has no glob and the path does not exist
   *         an empty array if {@code pathPattern} has a glob and no path
   *         matches it else an array of {@link FileStatus} objects matching the
   *         pattern
   * @throws IOException if any I/O error occurs when fetching file status
   */
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    return new Globber(this, pathPattern, filter).glob();
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * Return the file's status and block locations If the path is a file.
   *
   * If a returned status is a file, it contains the file's block locations.
   *
   * @param f is the path
   *
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   *
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws IOException If an I/O error occurred
   */
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
  throws FileNotFoundException, IOException {
    return listLocatedStatus(f, DEFAULT_FILTER);
  }

  /**
   * List a directory.
   * The returned results include its block location if it is a file
   * The results are filtered by the given path filter
   * @param f a path
   * @param filter a path filter
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if <code>f</code> does not exist
   * @throws IOException if any I/O error occurred
   */
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
  throws FileNotFoundException, IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private final FileStatus[] stats = listStatus(f, filter);
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i<stats.length;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException("No more entries in " + f);
        }
        FileStatus result = stats[i++];
        // for files, use getBlockLocations(FileStatus, int, int) to avoid
        // calling getFileStatus(Path) to load the FileStatus again
        BlockLocation[] locs = result.isFile() ?
            getFileBlockLocations(result, 0, result.getLen()) :
            null;
        return new LocatedFileStatus(result, locs);
      }
    };
  }

  /**
   * Generic iterator for implementing {@link #listStatusIterator(Path)}.
   */
  private class DirListingIterator<T extends FileStatus> implements
      RemoteIterator<T> {

    private final Path path;
    private DirectoryEntries entries;
    private int i = 0;

    DirListingIterator(Path path) {
      this.path = path;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (entries == null) {
        fetchMore();
      }
      return i < entries.getEntries().length ||
          entries.hasMore();
    }

    private void fetchMore() throws IOException {
      byte[] token = null;
      if (entries != null) {
        token = entries.getToken();
      }
      entries = listStatusBatch(path, token);
      i = 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() throws IOException {
      Preconditions.checkState(hasNext(), "No more items in iterator");
      if (i == entries.getEntries().length) {
        fetchMore();
      }
      return (T)entries.getEntries()[i++];
    }
  }

  /**
   * Returns a remote iterator so that followup calls are made on demand
   * while consuming the entries. Each FileSystem implementation should
   * override this method and provide a more efficient implementation, if
   * possible.
   *
   * Does not guarantee to return the iterator that traverses statuses
   * of the files in a sorted order.
   *
   * @param p target path
   * @return remote iterator
   * @throws FileNotFoundException if <code>p</code> does not exist
   * @throws IOException if any I/O error occurred
   */
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
  throws FileNotFoundException, IOException {
    return new DirListingIterator<>(p);
  }

  /**
   * List the statuses and block locations of the files in the given path.
   * Does not guarantee to return the iterator that traverses statuses
   * of the files in a sorted order.
   *
   * <pre>
   * If the path is a directory,
   *   if recursive is false, returns files in the directory;
   *   if recursive is true, return files in the subtree rooted at the path.
   * If the path is a file, return the file's status and block locations.
   * </pre>
   * @param f is the path
   * @param recursive if the subdirectories need to be traversed recursively
   *
   * @return an iterator that traverses statuses of the files
   *
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException see specific implementation
   */
  public RemoteIterator<LocatedFileStatus> listFiles(
      final Path f, final boolean recursive)
  throws FileNotFoundException, IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private Stack<RemoteIterator<LocatedFileStatus>> itors = new Stack<>();
      private RemoteIterator<LocatedFileStatus> curItor =
        listLocatedStatus(f);
      private LocatedFileStatus curFile;

      @Override
      public boolean hasNext() throws IOException {
        while (curFile == null) {
          if (curItor.hasNext()) {
            handleFileStat(curItor.next());
          } else if (!itors.empty()) {
            curItor = itors.pop();
          } else {
            return false;
          }
        }
        return true;
      }

      /**
       * Process the input stat.
       * If it is a file, return the file stat.
       * If it is a directory, traverse the directory if recursive is true;
       * ignore it if recursive is false.
       * @param stat input status
       * @throws IOException if any IO error occurs
       */
      private void handleFileStat(LocatedFileStatus stat) throws IOException {
        if (stat.isFile()) { // file
          curFile = stat;
        } else if (recursive) { // directory
          itors.push(curItor);
          curItor = listLocatedStatus(stat.getPath());
        }
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (hasNext()) {
          LocatedFileStatus result = curFile;
          curFile = null;
          return result;
        }
        throw new java.util.NoSuchElementException("No more entry in " + f);
      }
    };
  }

  /** Return the current user's home directory in this FileSystem.
   * The default implementation returns {@code "/user/$USER/"}.
   */
  public Path getHomeDirectory() {
    return this.makeQualified(
        new Path(USER_HOME_PREFIX + "/" + System.getProperty("user.name")));
  }


  /**
   * Set the current working directory for the given FileSystem. All relative
   * paths will be resolved relative to it.
   *
   * @param new_dir Path of new working directory
   */
  public abstract void setWorkingDirectory(Path new_dir);

  /**
   * Get the current working directory for the given FileSystem
   * @return the directory pathname
   */
  public abstract Path getWorkingDirectory();

  /**
   * Note: with the new FileContext class, getWorkingDirectory()
   * will be removed.
   * The working directory is implemented in FileContext.
   *
   * Some FileSystems like LocalFileSystem have an initial workingDir
   * that we use as the starting workingDir. For other file systems
   * like HDFS there is no built in notion of an initial workingDir.
   *
   * @return if there is built in notion of workingDir then it
   * is returned; else a null is returned.
   */
  protected Path getInitialWorkingDirectory() {
    return null;
  }

  /**
   * Call {@link #mkdirs(Path, FsPermission)} with default permission.
   * @param f path
   * @return true if the directory was created
   * @throws IOException IO failure
   */
  public boolean mkdirs(Path f) throws IOException {
    return mkdirs(f, FsPermission.getDirDefault());
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has roughly the semantics of Unix @{code mkdir -p}.
   * Existence of the directory hierarchy is not an error.
   * @param f path to create
   * @param permission to apply to f
   * @throws IOException IO failure
   */
  public abstract boolean mkdirs(Path f, FsPermission permission
      ) throws IOException;

  /**
   * The src file is on the local disk.  Add it to filesystem at
   * the given dst name and the source is kept intact afterwards
   * @param src path
   * @param dst path
   * @throws IOException IO failure
   */
  public void copyFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(false, src, dst);
  }

  /**
   * The src files is on the local disk.  Add it to filesystem at
   * the given dst name, removing the source afterwards.
   * @param srcs source paths
   * @param dst path
   * @throws IOException IO failure
   */
  public void moveFromLocalFile(Path[] srcs, Path dst)
    throws IOException {
    copyFromLocalFile(true, true, srcs, dst);
  }

  /**
   * The src file is on the local disk.  Add it to the filesystem at
   * the given dst name, removing the source afterwards.
   * @param src local path
   * @param dst path
   * @throws IOException IO failure
   */
  public void moveFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(true, src, dst);
  }

  /**
   * The src file is on the local disk.  Add it to the filesystem at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    copyFromLocalFile(delSrc, true, src, dst);
  }

  /**
   * The src files are on the local disk.  Add it to the filesystem at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param srcs array of paths which are source
   * @param dst path
   * @throws IOException IO failure
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
  }

  /**
   * The src file is on the local disk.  Add it to the filesystem at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO failure
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, overwrite, conf);
  }

  /**
   * Copy it a file from the remote filesystem to the local one.
   * @param src path src file in the remote filesystem
   * @param dst path local destination
   * @throws IOException IO failure
   */
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    copyToLocalFile(false, src, dst);
  }

  /**
   * Copy a file to the local filesystem, then delete it from the
   * remote filesystem (if successfully copied).
   * @param src path src file in the remote filesystem
   * @param dst path local destination
   * @throws IOException IO failure
   */
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    copyToLocalFile(true, src, dst);
  }

  /**
   * Copy it a file from a remote filesystem to the local one.
   * delSrc indicates if the src will be removed or not.
   * @param delSrc whether to delete the src
   * @param src path src file in the remote filesystem
   * @param dst path local destination
   * @throws IOException IO failure
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    copyToLocalFile(delSrc, src, dst, false);
  }

  /**
   * The src file is under this filesystem, and the dst is on the local disk.
   * Copy it from the remote filesystem to the local dst name.
   * delSrc indicates if the src will be removed
   * or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem
   * as the local file system or not. RawLocalFileSystem is non checksumming,
   * So, It will not create any crc files at local.
   *
   * @param delSrc
   *          whether to delete the src
   * @param src
   *          path
   * @param dst
   *          path
   * @param useRawLocalFileSystem
   *          whether to use RawLocalFileSystem as local file system or not.
   *
   * @throws IOException for any IO error
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst,
      boolean useRawLocalFileSystem) throws IOException {
    Configuration conf = getConf();
    FileSystem local = null;
    if (useRawLocalFileSystem) {
      local = getLocal(conf).getRawFileSystem();
    } else {
      local = getLocal(conf);
    }
    FileUtil.copy(this, src, local, dst, delSrc, conf);
  }

  /**
   * Returns a local file that the user can write output to.  The caller
   * provides both the eventual target name in this FileSystem
   * and the local working file path.
   * If this FileSystem is local, we write directly into the target.  If
   * the FileSystem is not local, we write into the tmp local area.
   * @param fsOutputFile path of output file
   * @param tmpLocalFile path of local tmp file
   * @throws IOException IO failure
   */
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  /**
   * Called when we're all done writing to the target.
   * A local FS will do nothing, because we've written to exactly the
   * right place.
   * A remote FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   * @param fsOutputFile path of output file
   * @param tmpLocalFile path to local tmp file
   * @throws IOException IO failure
   */
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * Close this FileSystem instance.
   * Will release any held locks, delete all files queued for deletion
   * through calls to {@link #deleteOnExit(Path)}, and remove this FS instance
   * from the cache, if cached.
   *
   * After this operation, the outcome of any method call on this FileSystem
   * instance, or any input/output stream created by it is <i>undefined</i>.
   * @throws IOException IO failure
   */
  @Override
  public void close() throws IOException {
    // delete all files that were marked as delete-on-exit.
    processDeleteOnExit();
    CACHE.remove(this.key, this);
  }

  /**
   * Return the total size of all files in the filesystem.
   * @throws IOException IO failure
   */
  public long getUsed() throws IOException {
    Path path = new Path("/");
    return getUsed(path);
  }

  /**
   * Return the total size of all files from a specified path.
   * @throws IOException IO failure
   */
  public long getUsed(Path path) throws IOException {
    return getContentSummary(path).getLength();
  }

  /**
   * Get the block size for a particular file.
   * @param f the filename
   * @return the number of bytes in a block
   * @deprecated Use {@link #getFileStatus(Path)} instead
   * @throws FileNotFoundException if the path is not present
   * @throws IOException IO failure
   */
  @Deprecated
  public long getBlockSize(Path f) throws IOException {
    return getFileStatus(f).getBlockSize();
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    // default to 32MB: large enough to minimize the impact of seeks
    return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.  The given path will be used to
   * locate the actual filesystem.  The full path does not have to exist.
   * @param f path of file
   * @return the default block size for the path's filesystem
   */
  public long getDefaultBlockSize(Path f) {
    return getDefaultBlockSize();
  }

  /**
   * Get the default replication.
   * @return the replication; the default value is "1".
   * @deprecated use {@link #getDefaultReplication(Path)} instead
   */
  @Deprecated
  public short getDefaultReplication() { return 1; }

  /**
   * Get the default replication for a path.
   * The given path will be used to locate the actual FileSystem to query.
   * The full path does not have to exist.
   * @param path of the file
   * @return default replication for the path's filesystem
   */
  public short getDefaultReplication(Path path) {
    return getDefaultReplication();
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public abstract FileStatus getFileStatus(Path f) throws IOException;

  /**
   * Checks if the user can access a path.  The mode specifies which access
   * checks to perform.  If the requested permissions are granted, then the
   * method returns normally.  If access is denied, then the method throws an
   * {@link AccessControlException}.
   * <p>
   * The default implementation calls {@link #getFileStatus(Path)}
   * and checks the returned permissions against the requested permissions.
   *
   * Note that the {@link #getFileStatus(Path)} call will be subject to
   * authorization checks.
   * Typically, this requires search (execute) permissions on each directory in
   * the path's prefix, but this is implementation-defined.  Any file system
   * that provides a richer authorization model (such as ACLs) may override the
   * default implementation so that it checks against that model instead.
   * <p>
   * In general, applications should avoid using this method, due to the risk of
   * time-of-check/time-of-use race conditions.  The permissions on a file may
   * change immediately after the access call returns.  Most applications should
   * prefer running specific file system actions as the desired user represented
   * by a {@link UserGroupInformation}.
   *
   * @param path Path to check
   * @param mode type of access to check
   * @throws AccessControlException if access is denied
   * @throws FileNotFoundException if the path does not exist
   * @throws IOException see specific implementation
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "Hive"})
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, IOException {
    checkAccessPermissions(this.getFileStatus(path), mode);
  }

  /**
   * This method provides the default implementation of
   * {@link #access(Path, FsAction)}.
   *
   * @param stat FileStatus to check
   * @param mode type of access to check
   * @throws AccessControlException if access is denied
   * @throws IOException for any error
   */
  @InterfaceAudience.Private
  static void checkAccessPermissions(FileStatus stat, FsAction mode)
      throws AccessControlException, IOException {
    FsPermission perm = stat.getPermission();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user = ugi.getShortUserName();
    if (user.equals(stat.getOwner())) {
      if (perm.getUserAction().implies(mode)) {
        return;
      }
    } else if (ugi.getGroups().contains(stat.getGroup())) {
      if (perm.getGroupAction().implies(mode)) {
        return;
      }
    } else {
      if (perm.getOtherAction().implies(mode)) {
        return;
      }
    }
    throw new AccessControlException(String.format(
      "Permission denied: user=%s, path=\"%s\":%s:%s:%s%s", user, stat.getPath(),
      stat.getOwner(), stat.getGroup(), stat.isDirectory() ? "d" : "-", perm));
  }

  /**
   * See {@link FileContext#fixRelativePart}.
   */
  protected Path fixRelativePart(Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new Path(getWorkingDirectory(), p);
    }
  }

  /**
   * See {@link FileContext#createSymlink(Path, Path, boolean)}.
   */
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException,
      IOException {
    // Supporting filesystems should override this method
    throw new UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * See {@link FileContext#getFileLinkStatus(Path)}.
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException see specific implementation
   */
  public FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    // Supporting filesystems should override this method
    return getFileStatus(f);
  }

  /**
   * See {@link AbstractFileSystem#supportsSymlinks()}.
   */
  public boolean supportsSymlinks() {
    return false;
  }

  /**
   * See {@link FileContext#getLinkTarget(Path)}.
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public Path getLinkTarget(Path f) throws IOException {
    // Supporting filesystems should override this method
    throw new UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * See {@link AbstractFileSystem#getLinkTarget(Path)}.
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  protected Path resolveLink(Path f) throws IOException {
    // Supporting filesystems should override this method
    throw new UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * Get the checksum of a file, if the FS supports checksums.
   *
   * @param f The file path
   * @return The file checksum.  The default return value is null,
   *  which indicates that no checksum algorithm is implemented
   *  in the corresponding FileSystem.
   * @throws IOException IO failure
   */
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return getFileChecksum(f, Long.MAX_VALUE);
  }

  /**
   * Get the checksum of a file, from the beginning of the file till the
   * specific length.
   * @param f The file path
   * @param length The length of the file range for checksum calculation
   * @return The file checksum or null if checksums are not supported.
   * @throws IOException IO failure
   */
  public FileChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    return null;
  }

  /**
   * Set the verify checksum flag. This is only applicable if the
   * corresponding filesystem supports checksums.
   * By default doesn't do anything.
   * @param verifyChecksum Verify checksum flag
   */
  public void setVerifyChecksum(boolean verifyChecksum) {
    //doesn't do anything
  }

  /**
   * Set the write checksum flag. This is only applicable if the
   * corresponding filesystem supports checksums.
   * By default doesn't do anything.
   * @param writeChecksum Write checksum flag
   */
  public void setWriteChecksum(boolean writeChecksum) {
    //doesn't do anything
  }

  /**
   * Returns a status object describing the use and capacity of the
   * filesystem. If the filesystem has multiple partitions, the
   * use and capacity of the root partition is reflected.
   *
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public FsStatus getStatus() throws IOException {
    return getStatus(null);
  }

  /**
   * Returns a status object describing the use and capacity of the
   * filesystem. If the filesystem has multiple partitions, the
   * use and capacity of the partition pointed to by the specified
   * path is reflected.
   * @param p Path for which status should be obtained. null means
   * the default partition.
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public FsStatus getStatus(Path p) throws IOException {
    return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
  }

  /**
   * Set permission of a path.
   * @param p The path
   * @param permission permission
   * @throws IOException IO failure
   */
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
  }

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   * @throws IOException IO failure
   */
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
  }

  /**
   * Set access time of a file.
   * @param p The path
   * @param mtime Set the modification time of this file.
   *              The number of milliseconds since Jan 1, 1970.
   *              A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   *              The number of milliseconds since Jan 1, 1970.
   *              A value of -1 means that this call should not set access time.
   * @throws IOException IO failure
   */
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
  }

  /**
   * Create a snapshot with a default name.
   * @param path The directory where snapshots will be taken.
   * @return the snapshot path.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   */
  public final Path createSnapshot(Path path) throws IOException {
    return createSnapshot(path, null);
  }

  /**
   * Create a snapshot.
   * @param path The directory where snapshots will be taken.
   * @param snapshotName The name of the snapshot
   * @return the snapshot path.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   */
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support createSnapshot");
  }

  /**
   * Rename a snapshot.
   * @param path The directory path where the snapshot was taken
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support renameSnapshot");
  }

  /**
   * Delete a snapshot of a directory.
   * @param path  The directory that the to-be-deleted snapshot belongs to
   * @param snapshotName The name of the snapshot
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support deleteSnapshot");
  }

  /**
   * Modifies ACL entries of files and directories.  This method can add new ACL
   * entries or modify the permissions on existing ACL entries.  All existing
   * ACL entries that are not specified in this call are retained without
   * changes.  (Modifications are merged into the current ACL.)
   *
   * @param path Path to modify
   * @param aclSpec List<AclEntry> describing modifications
   * @throws IOException if an ACL could not be modified
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support modifyAclEntries");
  }

  /**
   * Removes ACL entries from files and directories.  Other ACL entries are
   * retained.
   *
   * @param path Path to modify
   * @param aclSpec List describing entries to remove
   * @throws IOException if an ACL could not be modified
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support removeAclEntries");
  }

  /**
   * Removes all default ACL entries from files and directories.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be modified
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void removeDefaultAcl(Path path)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support removeDefaultAcl");
  }

  /**
   * Removes all but the base ACL entries of files and directories.  The entries
   * for user, group, and others are retained for compatibility with permission
   * bits.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be removed
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void removeAcl(Path path)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support removeAcl");
  }

  /**
   * Fully replaces ACL of files and directories, discarding all existing
   * entries.
   *
   * @param path Path to modify
   * @param aclSpec List describing modifications, which must include entries
   *   for user, group, and others for compatibility with permission bits.
   * @throws IOException if an ACL could not be modified
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support setAcl");
  }

  /**
   * Gets the ACL of a file or directory.
   *
   * @param path Path to get
   * @return AclStatus describing the ACL of the file or directory
   * @throws IOException if an ACL could not be read
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public AclStatus getAclStatus(Path path) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getAclStatus");
  }

  /**
   * Set an xattr of a file or directory.
   * The name must be prefixed with the namespace followed by ".". For example,
   * "user.attr".
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to modify
   * @param name xattr name.
   * @param value xattr value.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void setXAttr(Path path, String name, byte[] value)
      throws IOException {
    setXAttr(path, name, value, EnumSet.of(XAttrSetFlag.CREATE,
        XAttrSetFlag.REPLACE));
  }

  /**
   * Set an xattr of a file or directory.
   * The name must be prefixed with the namespace followed by ".". For example,
   * "user.attr".
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to modify
   * @param name xattr name.
   * @param value xattr value.
   * @param flag xattr set flag
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support setXAttr");
  }

  /**
   * Get an xattr name and value for a file or directory.
   * The name must be prefixed with the namespace followed by ".". For example,
   * "user.attr".
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attribute
   * @param name xattr name.
   * @return byte[] xattr value.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public byte[] getXAttr(Path path, String name) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getXAttr");
  }

  /**
   * Get all of the xattr name/value pairs for a file or directory.
   * Only those xattrs which the logged-in user has permissions to view
   * are returned.
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @return Map describing the XAttrs of the file or directory
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getXAttrs");
  }

  /**
   * Get all of the xattrs name/value pairs for a file or directory.
   * Only those xattrs which the logged-in user has permissions to view
   * are returned.
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @param names XAttr names.
   * @return Map describing the XAttrs of the file or directory
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getXAttrs");
  }

  /**
   * Get all of the xattr names for a file or directory.
   * Only those xattr names which the logged-in user has permissions to view
   * are returned.
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @return List<String> of the XAttr names of the file or directory
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public List<String> listXAttrs(Path path) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
            + " doesn't support listXAttrs");
  }

  /**
   * Remove an xattr of a file or directory.
   * The name must be prefixed with the namespace followed by ".". For example,
   * "user.attr".
   * <p>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to remove extended attribute
   * @param name xattr name
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void removeXAttr(Path path, String name) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support removeXAttr");
  }

  /**
   * Set the storage policy for a given file or directory.
   *
   * @param src file or directory path.
   * @param policyName the name of the target storage policy. The list
   *                   of supported Storage policies can be retrieved
   *                   via {@link #getAllStoragePolicies}.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support setStoragePolicy");
  }

  /**
   * Unset the storage policy set for a given file or directory.
   * @param src file or directory path.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public void unsetStoragePolicy(final Path src) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support unsetStoragePolicy");
  }

  /**
   * Query the effective storage policy ID for the given file or directory.
   *
   * @param src file or directory path.
   * @return storage policy for give file.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public BlockStoragePolicySpi getStoragePolicy(final Path src)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getStoragePolicy");
  }

  /**
   * Retrieve all the storage policies supported by this file system.
   *
   * @return all storage policies supported by this filesystem.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default outcome).
   */
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getAllStoragePolicies");
  }

  /**
   * Get the root directory of Trash for current user when the path specified
   * is deleted.
   *
   * @param path the trash root of the path to be determined.
   * @return the default implementation returns {@code /user/$USER/.Trash}
   */
  public Path getTrashRoot(Path path) {
    return this.makeQualified(new Path(getHomeDirectory().toUri().getPath(),
        TRASH_PREFIX));
  }

  /**
   * Get all the trash roots for current user or all users.
   *
   * @param allUsers return trash roots for all users if true.
   * @return all the trash root directories.
   *         Default FileSystem returns .Trash under users' home directories if
   *         {@code /user/$USER/.Trash} exists.
   */
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    Path userHome = new Path(getHomeDirectory().toUri().getPath());
    List<FileStatus> ret = new ArrayList<>();
    try {
      if (!allUsers) {
        Path userTrash = new Path(userHome, TRASH_PREFIX);
        if (exists(userTrash)) {
          ret.add(getFileStatus(userTrash));
        }
      } else {
        Path homeParent = userHome.getParent();
        if (exists(homeParent)) {
          FileStatus[] candidates = listStatus(homeParent);
          for (FileStatus candidate : candidates) {
            Path userTrash = new Path(candidate.getPath(), TRASH_PREFIX);
            if (exists(userTrash)) {
              candidate.setPath(userTrash);
              ret.add(candidate);
            }
          }
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Cannot get all trash roots", e);
    }
    return ret;
  }

  // making it volatile to be able to do a double checked locking
  private volatile static boolean FILE_SYSTEMS_LOADED = false;

  /**
   * Filesystems listed as services.
   */
  private static final Map<String, Class<? extends FileSystem>>
      SERVICE_FILE_SYSTEMS = new HashMap<>();

  /**
   * Load the filesystem declarations from service resources.
   * This is a synchronized operation.
   */
  private static void loadFileSystems() {
    LOGGER.debug("Loading filesystems");
    synchronized (FileSystem.class) {
      if (!FILE_SYSTEMS_LOADED) {
        ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
        Iterator<FileSystem> it = serviceLoader.iterator();
        while (it.hasNext()) {
          FileSystem fs;
          try {
            fs = it.next();
            try {
              SERVICE_FILE_SYSTEMS.put(fs.getScheme(), fs.getClass());
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}:// = {} from {}",
                    fs.getScheme(), fs.getClass(),
                    ClassUtil.findContainingJar(fs.getClass()));
              }
            } catch (Exception e) {
              LOGGER.warn("Cannot load: {} from {}", fs,
                  ClassUtil.findContainingJar(fs.getClass()));
              LOGGER.info("Full exception loading: {}", fs, e);
            }
          } catch (ServiceConfigurationError ee) {
            LOG.warn("Cannot load filesystem: " + ee);
            Throwable cause = ee.getCause();
            // print all the nested exception messages
            while (cause != null) {
              LOG.warn(cause.toString());
              cause = cause.getCause();
            }
            // and at debug: the full stack
            LOG.debug("Stack Trace", ee);
          }
        }
        FILE_SYSTEMS_LOADED = true;
      }
    }
  }

  /**
   * Get the FileSystem implementation class of a filesystem.
   * This triggers a scan and load of all FileSystem implementations listed as
   * services and discovered via the {@link ServiceLoader}
   * @param scheme URL scheme of FS
   * @param conf configuration: can be null, in which case the check for
   * a filesystem binding declaration in the configuration is skipped.
   * @return the filesystem
   * @throws UnsupportedFileSystemException if there was no known implementation
   *         for the scheme.
   * @throws IOException if the filesystem could not be loaded
   */
  public static Class<? extends FileSystem> getFileSystemClass(String scheme,
      Configuration conf) throws IOException {
    if (!FILE_SYSTEMS_LOADED) {
      loadFileSystems();
    }
    LOGGER.debug("Looking for FS supporting {}", scheme);
    Class<? extends FileSystem> clazz = null;
    if (conf != null) {
      String property = "fs." + scheme + ".impl";
      LOGGER.debug("looking for configuration option {}", property);
      clazz = (Class<? extends FileSystem>) conf.getClass(
          property, null);
    } else {
      LOGGER.debug("No configuration: skipping check for fs.{}.impl", scheme);
    }
    if (clazz == null) {
      LOGGER.debug("Looking in service filesystems for implementation class");
      clazz = SERVICE_FILE_SYSTEMS.get(scheme);
    } else {
      LOGGER.debug("Filesystem {} defined in configuration option", scheme);
    }
    if (clazz == null) {
      throw new UnsupportedFileSystemException("No FileSystem for scheme "
          + "\"" + scheme + "\"");
    }
    LOGGER.debug("FS for {} is {}", scheme, clazz);
    return clazz;
  }

  /**
   * Create and initialize a new instance of a FileSystem.
   * @param uri URI containing the FS schema and FS details
   * @param conf configuration to use to look for the FS instance declaration
   * and to pass to the {@link FileSystem#initialize(URI, Configuration)}.
   * @return the initialized filesystem.
   * @throws IOException problems loading or initializing the FileSystem
   */
  private static FileSystem createFileSystem(URI uri, Configuration conf)
      throws IOException {
    Tracer tracer = FsTracer.get(conf);
    try(TraceScope scope = tracer.newScope("FileSystem#createFileSystem")) {
      scope.addKVAnnotation("scheme", uri.getScheme());
      Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
      FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(uri, conf);
      return fs;
    }
  }

  /** Caching FileSystem objects. */
  static class Cache {
    private final ClientFinalizer clientFinalizer = new ClientFinalizer();

    private final Map<Key, FileSystem> map = new HashMap<>();
    private final Set<Key> toAutoClose = new HashSet<>();

    /** A variable that makes all objects in the cache unique. */
    private static AtomicLong unique = new AtomicLong(1);

    FileSystem get(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf);
      return getInternal(uri, conf, key);
    }

    /** The objects inserted into the cache using this method are all unique. */
    FileSystem getUnique(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf, unique.getAndIncrement());
      return getInternal(uri, conf, key);
    }

    /**
     * Get the FS instance if the key maps to an instance, creating and
     * initializing the FS if it is not found.
     * If this is the first entry in the map and the JVM is not shutting down,
     * this registers a shutdown hook to close filesystems, and adds this
     * FS to the {@code toAutoClose} set if {@code "fs.automatic.close"}
     * is set in the configuration (default: true).
     * @param uri filesystem URI
     * @param conf configuration
     * @param key key to store/retrieve this FileSystem in the cache
     * @return a cached or newly instantiated FileSystem.
     * @throws IOException
     */
    private FileSystem getInternal(URI uri, Configuration conf, Key key)
        throws IOException{
      FileSystem fs;
      synchronized (this) {
        fs = map.get(key);
      }
      if (fs != null) {
        return fs;
      }

      fs = createFileSystem(uri, conf);
      synchronized (this) { // refetch the lock again
        FileSystem oldfs = map.get(key);
        if (oldfs != null) { // a file system is created while lock is releasing
          fs.close(); // close the new file system
          return oldfs;  // return the old file system
        }

        // now insert the new file system into the map
        if (map.isEmpty()
                && !ShutdownHookManager.get().isShutdownInProgress()) {
          ShutdownHookManager.get().addShutdownHook(clientFinalizer, SHUTDOWN_HOOK_PRIORITY);
        }
        fs.key = key;
        map.put(key, fs);
        if (conf.getBoolean(
            FS_AUTOMATIC_CLOSE_KEY, FS_AUTOMATIC_CLOSE_DEFAULT)) {
          toAutoClose.add(key);
        }
        return fs;
      }
    }

    synchronized void remove(Key key, FileSystem fs) {
      FileSystem cachedFs = map.remove(key);
      if (fs == cachedFs) {
        toAutoClose.remove(key);
      } else if (cachedFs != null) {
        map.put(key, cachedFs);
      }
    }

    /**
     * Close all FileSystems in the cache, whether they are marked for
     * automatic closing or not.
     * @throws IOException a problem arose closing one or more FileSystem.
     */
    synchronized void closeAll() throws IOException {
      closeAll(false);
    }

    /**
     * Close all FileSystem instances in the Cache.
     * @param onlyAutomatic only close those that are marked for automatic closing
     * @throws IOException a problem arose closing one or more FileSystem.
     */
    synchronized void closeAll(boolean onlyAutomatic) throws IOException {
      List<IOException> exceptions = new ArrayList<>();

      // Make a copy of the keys in the map since we'll be modifying
      // the map while iterating over it, which isn't safe.
      List<Key> keys = new ArrayList<>();
      keys.addAll(map.keySet());

      for (Key key : keys) {
        final FileSystem fs = map.get(key);

        if (onlyAutomatic && !toAutoClose.contains(key)) {
          continue;
        }

        //remove from cache
        map.remove(key);
        toAutoClose.remove(key);

        if (fs != null) {
          try {
            fs.close();
          }
          catch(IOException ioe) {
            exceptions.add(ioe);
          }
        }
      }

      if (!exceptions.isEmpty()) {
        throw MultipleIOException.createIOException(exceptions);
      }
    }

    private class ClientFinalizer implements Runnable {
      @Override
      public synchronized void run() {
        try {
          closeAll(true);
        } catch (IOException e) {
          LOGGER.info("FileSystem.Cache.closeAll() threw an exception:\n" + e);
        }
      }
    }

    synchronized void closeAll(UserGroupInformation ugi) throws IOException {
      List<FileSystem> targetFSList = new ArrayList<>(map.entrySet().size());
      //Make a pass over the list and collect the FileSystems to close
      //we cannot close inline since close() removes the entry from the Map
      for (Map.Entry<Key, FileSystem> entry : map.entrySet()) {
        final Key key = entry.getKey();
        final FileSystem fs = entry.getValue();
        if (ugi.equals(key.ugi) && fs != null) {
          targetFSList.add(fs);
        }
      }
      List<IOException> exceptions = new ArrayList<>();
      //now make a pass over the target list and close each
      for (FileSystem fs : targetFSList) {
        try {
          fs.close();
        }
        catch(IOException ioe) {
          exceptions.add(ioe);
        }
      }
      if (!exceptions.isEmpty()) {
        throw MultipleIOException.createIOException(exceptions);
      }
    }

    /** FileSystem.Cache.Key */
    static class Key {
      final String scheme;
      final String authority;
      final UserGroupInformation ugi;
      final long unique;   // an artificial way to make a key unique

      Key(URI uri, Configuration conf) throws IOException {
        this(uri, conf, 0);
      }

      Key(URI uri, Configuration conf, long unique) throws IOException {
        scheme = uri.getScheme()==null ?
            "" : StringUtils.toLowerCase(uri.getScheme());
        authority = uri.getAuthority()==null ?
            "" : StringUtils.toLowerCase(uri.getAuthority());
        this.unique = unique;

        this.ugi = UserGroupInformation.getCurrentUser();
      }

      @Override
      public int hashCode() {
        return (scheme + authority).hashCode() + ugi.hashCode() + (int)unique;
      }

      static boolean isEqual(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj instanceof Key) {
          Key that = (Key)obj;
          return isEqual(this.scheme, that.scheme)
                 && isEqual(this.authority, that.authority)
                 && isEqual(this.ugi, that.ugi)
                 && (this.unique == that.unique);
        }
        return false;
      }

      @Override
      public String toString() {
        return "("+ugi.toString() + ")@" + scheme + "://" + authority;
      }
    }
  }

  /**
   * Tracks statistics about how many reads, writes, and so forth have been
   * done in a FileSystem.
   *
   * Since there is only one of these objects per FileSystem, there will
   * typically be many threads writing to this object.  Almost every operation
   * on an open file will involve a write to this object.  In contrast, reading
   * statistics is done infrequently by most programs, and not at all by others.
   * Hence, this is optimized for writes.
   *
   * Each thread writes to its own thread-local area of memory.  This removes
   * contention and allows us to scale up to many, many threads.  To read
   * statistics, the reader thread totals up the contents of all of the
   * thread-local data areas.
   */
  public static final class Statistics {
    /**
     * Statistics data.
     *
     * There is only a single writer to thread-local StatisticsData objects.
     * Hence, volatile is adequate here-- we do not need AtomicLong or similar
     * to prevent lost updates.
     * The Java specification guarantees that updates to volatile longs will
     * be perceived as atomic with respect to other threads, which is all we
     * need.
     */
    public static class StatisticsData {
      private volatile long bytesRead;
      private volatile long bytesWritten;
      private volatile int readOps;
      private volatile int largeReadOps;
      private volatile int writeOps;
      private volatile long bytesReadLocalHost;
      private volatile long bytesReadDistanceOfOneOrTwo;
      private volatile long bytesReadDistanceOfThreeOrFour;
      private volatile long bytesReadDistanceOfFiveOrLarger;

      /**
       * Add another StatisticsData object to this one.
       */
      void add(StatisticsData other) {
        this.bytesRead += other.bytesRead;
        this.bytesWritten += other.bytesWritten;
        this.readOps += other.readOps;
        this.largeReadOps += other.largeReadOps;
        this.writeOps += other.writeOps;
        this.bytesReadLocalHost += other.bytesReadLocalHost;
        this.bytesReadDistanceOfOneOrTwo += other.bytesReadDistanceOfOneOrTwo;
        this.bytesReadDistanceOfThreeOrFour +=
            other.bytesReadDistanceOfThreeOrFour;
        this.bytesReadDistanceOfFiveOrLarger +=
            other.bytesReadDistanceOfFiveOrLarger;
      }

      /**
       * Negate the values of all statistics.
       */
      void negate() {
        this.bytesRead = -this.bytesRead;
        this.bytesWritten = -this.bytesWritten;
        this.readOps = -this.readOps;
        this.largeReadOps = -this.largeReadOps;
        this.writeOps = -this.writeOps;
        this.bytesReadLocalHost = -this.bytesReadLocalHost;
        this.bytesReadDistanceOfOneOrTwo = -this.bytesReadDistanceOfOneOrTwo;
        this.bytesReadDistanceOfThreeOrFour =
            -this.bytesReadDistanceOfThreeOrFour;
        this.bytesReadDistanceOfFiveOrLarger =
            -this.bytesReadDistanceOfFiveOrLarger;
      }

      @Override
      public String toString() {
        return bytesRead + " bytes read, " + bytesWritten + " bytes written, "
            + readOps + " read ops, " + largeReadOps + " large read ops, "
            + writeOps + " write ops";
      }

      public long getBytesRead() {
        return bytesRead;
      }

      public long getBytesWritten() {
        return bytesWritten;
      }

      public int getReadOps() {
        return readOps;
      }

      public int getLargeReadOps() {
        return largeReadOps;
      }

      public int getWriteOps() {
        return writeOps;
      }

      public long getBytesReadLocalHost() {
        return bytesReadLocalHost;
      }

      public long getBytesReadDistanceOfOneOrTwo() {
        return bytesReadDistanceOfOneOrTwo;
      }

      public long getBytesReadDistanceOfThreeOrFour() {
        return bytesReadDistanceOfThreeOrFour;
      }

      public long getBytesReadDistanceOfFiveOrLarger() {
        return bytesReadDistanceOfFiveOrLarger;
      }
    }

    private interface StatisticsAggregator<T> {
      void accept(StatisticsData data);
      T aggregate();
    }

    private final String scheme;

    /**
     * rootData is data that doesn't belong to any thread, but will be added
     * to the totals.  This is useful for making copies of Statistics objects,
     * and for storing data that pertains to threads that have been garbage
     * collected.  Protected by the Statistics lock.
     */
    private final StatisticsData rootData;

    /**
     * Thread-local data.
     */
    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<StatisticsData> threadData;

    /**
     * Set of all thread-local data areas.  Protected by the Statistics lock.
     * The references to the statistics data are kept using weak references
     * to the associated threads. Proper clean-up is performed by the cleaner
     * thread when the threads are garbage collected.
     */
    private final Set<StatisticsDataReference> allData;

    /**
     * Global reference queue and a cleaner thread that manage statistics data
     * references from all filesystem instances.
     */
    private static final ReferenceQueue<Thread> STATS_DATA_REF_QUEUE;
    private static final Thread STATS_DATA_CLEANER;

    static {
      STATS_DATA_REF_QUEUE = new ReferenceQueue<>();
      // start a single daemon cleaner thread
      STATS_DATA_CLEANER = new Thread(new StatisticsDataReferenceCleaner());
      STATS_DATA_CLEANER.
          setName(StatisticsDataReferenceCleaner.class.getName());
      STATS_DATA_CLEANER.setDaemon(true);
      STATS_DATA_CLEANER.start();
    }

    public Statistics(String scheme) {
      this.scheme = scheme;
      this.rootData = new StatisticsData();
      this.threadData = new ThreadLocal<>();
      this.allData = new HashSet<>();
    }

    /**
     * Copy constructor.
     *
     * @param other    The input Statistics object which is cloned.
     */
    public Statistics(Statistics other) {
      this.scheme = other.scheme;
      this.rootData = new StatisticsData();
      other.visitAll(new StatisticsAggregator<Void>() {
        @Override
        public void accept(StatisticsData data) {
          rootData.add(data);
        }

        public Void aggregate() {
          return null;
        }
      });
      this.threadData = new ThreadLocal<>();
      this.allData = new HashSet<>();
    }

    /**
     * A weak reference to a thread that also includes the data associated
     * with that thread. On the thread being garbage collected, it is enqueued
     * to the reference queue for clean-up.
     */
    private final class StatisticsDataReference extends WeakReference<Thread> {
      private final StatisticsData data;

      private StatisticsDataReference(StatisticsData data, Thread thread) {
        super(thread, STATS_DATA_REF_QUEUE);
        this.data = data;
      }

      public StatisticsData getData() {
        return data;
      }

      /**
       * Performs clean-up action when the associated thread is garbage
       * collected.
       */
      public void cleanUp() {
        // use the statistics lock for safety
        synchronized (Statistics.this) {
          /*
           * If the thread that created this thread-local data no longer exists,
           * remove the StatisticsData from our list and fold the values into
           * rootData.
           */
          rootData.add(data);
          allData.remove(this);
        }
      }
    }

    /**
     * Background action to act on references being removed.
     */
    private static class StatisticsDataReferenceCleaner implements Runnable {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          try {
            StatisticsDataReference ref =
                (StatisticsDataReference)STATS_DATA_REF_QUEUE.remove();
            ref.cleanUp();
          } catch (InterruptedException ie) {
            LOGGER.warn("Cleaner thread interrupted, will stop", ie);
            Thread.currentThread().interrupt();
          } catch (Throwable th) {
            LOGGER.warn("Exception in the cleaner thread but it will" +
                " continue to run", th);
          }
        }
      }
    }

    /**
     * Get or create the thread-local data associated with the current thread.
     */
    public StatisticsData getThreadStatistics() {
      StatisticsData data = threadData.get();
      if (data == null) {
        data = new StatisticsData();
        threadData.set(data);
        StatisticsDataReference ref =
            new StatisticsDataReference(data, Thread.currentThread());
        synchronized(this) {
          allData.add(ref);
        }
      }
      return data;
    }

    /**
     * Increment the bytes read in the statistics.
     * @param newBytes the additional bytes read
     */
    public void incrementBytesRead(long newBytes) {
      getThreadStatistics().bytesRead += newBytes;
    }

    /**
     * Increment the bytes written in the statistics.
     * @param newBytes the additional bytes written
     */
    public void incrementBytesWritten(long newBytes) {
      getThreadStatistics().bytesWritten += newBytes;
    }

    /**
     * Increment the number of read operations.
     * @param count number of read operations
     */
    public void incrementReadOps(int count) {
      getThreadStatistics().readOps += count;
    }

    /**
     * Increment the number of large read operations.
     * @param count number of large read operations
     */
    public void incrementLargeReadOps(int count) {
      getThreadStatistics().largeReadOps += count;
    }

    /**
     * Increment the number of write operations.
     * @param count number of write operations
     */
    public void incrementWriteOps(int count) {
      getThreadStatistics().writeOps += count;
    }

    /**
     * Increment the bytes read by the network distance in the statistics
     * In the common network topology setup, distance value should be an even
     * number such as 0, 2, 4, 6. To make it more general, we group distance
     * by {1, 2}, {3, 4} and {5 and beyond} for accounting.
     * @param distance the network distance
     * @param newBytes the additional bytes read
     */
    public void incrementBytesReadByDistance(int distance, long newBytes) {
      switch (distance) {
      case 0:
        getThreadStatistics().bytesReadLocalHost += newBytes;
        break;
      case 1:
      case 2:
        getThreadStatistics().bytesReadDistanceOfOneOrTwo += newBytes;
        break;
      case 3:
      case 4:
        getThreadStatistics().bytesReadDistanceOfThreeOrFour += newBytes;
        break;
      default:
        getThreadStatistics().bytesReadDistanceOfFiveOrLarger += newBytes;
        break;
      }
    }

    /**
     * Apply the given aggregator to all StatisticsData objects associated with
     * this Statistics object.
     *
     * For each StatisticsData object, we will call accept on the visitor.
     * Finally, at the end, we will call aggregate to get the final total.
     *
     * @param         visitor to use.
     * @return        The total.
     */
    private synchronized <T> T visitAll(StatisticsAggregator<T> visitor) {
      visitor.accept(rootData);
      for (StatisticsDataReference ref: allData) {
        StatisticsData data = ref.getData();
        visitor.accept(data);
      }
      return visitor.aggregate();
    }

    /**
     * Get the total number of bytes read.
     * @return the number of bytes
     */
    public long getBytesRead() {
      return visitAll(new StatisticsAggregator<Long>() {
        private long bytesRead = 0;

        @Override
        public void accept(StatisticsData data) {
          bytesRead += data.bytesRead;
        }

        public Long aggregate() {
          return bytesRead;
        }
      });
    }

    /**
     * Get the total number of bytes written.
     * @return the number of bytes
     */
    public long getBytesWritten() {
      return visitAll(new StatisticsAggregator<Long>() {
        private long bytesWritten = 0;

        @Override
        public void accept(StatisticsData data) {
          bytesWritten += data.bytesWritten;
        }

        public Long aggregate() {
          return bytesWritten;
        }
      });
    }

    /**
     * Get the number of file system read operations such as list files.
     * @return number of read operations
     */
    public int getReadOps() {
      return visitAll(new StatisticsAggregator<Integer>() {
        private int readOps = 0;

        @Override
        public void accept(StatisticsData data) {
          readOps += data.readOps;
          readOps += data.largeReadOps;
        }

        public Integer aggregate() {
          return readOps;
        }
      });
    }

    /**
     * Get the number of large file system read operations such as list files
     * under a large directory.
     * @return number of large read operations
     */
    public int getLargeReadOps() {
      return visitAll(new StatisticsAggregator<Integer>() {
        private int largeReadOps = 0;

        @Override
        public void accept(StatisticsData data) {
          largeReadOps += data.largeReadOps;
        }

        public Integer aggregate() {
          return largeReadOps;
        }
      });
    }

    /**
     * Get the number of file system write operations such as create, append
     * rename etc.
     * @return number of write operations
     */
    public int getWriteOps() {
      return visitAll(new StatisticsAggregator<Integer>() {
        private int writeOps = 0;

        @Override
        public void accept(StatisticsData data) {
          writeOps += data.writeOps;
        }

        public Integer aggregate() {
          return writeOps;
        }
      });
    }

    /**
     * In the common network topology setup, distance value should be an even
     * number such as 0, 2, 4, 6. To make it more general, we group distance
     * by {1, 2}, {3, 4} and {5 and beyond} for accounting. So if the caller
     * ask for bytes read for distance 2, the function will return the value
     * for group {1, 2}.
     * @param distance the network distance
     * @return the total number of bytes read by the network distance
     */
    public long getBytesReadByDistance(int distance) {
      long bytesRead;
      switch (distance) {
      case 0:
        bytesRead = getData().getBytesReadLocalHost();
        break;
      case 1:
      case 2:
        bytesRead = getData().getBytesReadDistanceOfOneOrTwo();
        break;
      case 3:
      case 4:
        bytesRead = getData().getBytesReadDistanceOfThreeOrFour();
        break;
      default:
        bytesRead = getData().getBytesReadDistanceOfFiveOrLarger();
        break;
      }
      return bytesRead;
    }

    /**
     * Get all statistics data.
     * MR or other frameworks can use the method to get all statistics at once.
     * @return the StatisticsData
     */
    public StatisticsData getData() {
      return visitAll(new StatisticsAggregator<StatisticsData>() {
        private StatisticsData all = new StatisticsData();

        @Override
        public void accept(StatisticsData data) {
          all.add(data);
        }

        public StatisticsData aggregate() {
          return all;
        }
      });
    }

    @Override
    public String toString() {
      return visitAll(new StatisticsAggregator<String>() {
        private StatisticsData total = new StatisticsData();

        @Override
        public void accept(StatisticsData data) {
          total.add(data);
        }

        public String aggregate() {
          return total.toString();
        }
      });
    }

    /**
     * Resets all statistics to 0.
     *
     * In order to reset, we add up all the thread-local statistics data, and
     * set rootData to the negative of that.
     *
     * This may seem like a counterintuitive way to reset the statistics.  Why
     * can't we just zero out all the thread-local data?  Well, thread-local
     * data can only be modified by the thread that owns it.  If we tried to
     * modify the thread-local data from this thread, our modification might get
     * interleaved with a read-modify-write operation done by the thread that
     * owns the data.  That would result in our update getting lost.
     *
     * The approach used here avoids this problem because it only ever reads
     * (not writes) the thread-local data.  Both reads and writes to rootData
     * are done under the lock, so we're free to modify rootData from any thread
     * that holds the lock.
     */
    public void reset() {
      visitAll(new StatisticsAggregator<Void>() {
        private StatisticsData total = new StatisticsData();

        @Override
        public void accept(StatisticsData data) {
          total.add(data);
        }

        public Void aggregate() {
          total.negate();
          rootData.add(total);
          return null;
        }
      });
    }

    /**
     * Get the uri scheme associated with this statistics object.
     * @return the schema associated with this set of statistics
     */
    public String getScheme() {
      return scheme;
    }

    @VisibleForTesting
    synchronized int getAllThreadLocalDataSize() {
      return allData.size();
    }
  }

  /**
   * Get the Map of Statistics object indexed by URI Scheme.
   * @return a Map having a key as URI scheme and value as Statistics object
   * @deprecated use {@link #getGlobalStorageStatistics()}
   */
  @Deprecated
  public static synchronized Map<String, Statistics> getStatistics() {
    Map<String, Statistics> result = new HashMap<>();
    for(Statistics stat: statisticsTable.values()) {
      result.put(stat.getScheme(), stat);
    }
    return result;
  }

  /**
   * Return the FileSystem classes that have Statistics.
   * @deprecated use {@link #getGlobalStorageStatistics()}
   */
  @Deprecated
  public static synchronized List<Statistics> getAllStatistics() {
    return new ArrayList<>(statisticsTable.values());
  }

  /**
   * Get the statistics for a particular file system.
   * @param cls the class to lookup
   * @return a statistics object
   * @deprecated use {@link #getGlobalStorageStatistics()}
   */
  @Deprecated
  public static synchronized Statistics getStatistics(final String scheme,
      Class<? extends FileSystem> cls) {
    checkArgument(scheme != null,
        "No statistics is allowed for a file system with null scheme!");
    Statistics result = statisticsTable.get(cls);
    if (result == null) {
      final Statistics newStats = new Statistics(scheme);
      statisticsTable.put(cls, newStats);
      result = newStats;
      GlobalStorageStatistics.INSTANCE.put(scheme,
          new StorageStatisticsProvider() {
            @Override
            public StorageStatistics provide() {
              return new FileSystemStorageStatistics(scheme, newStats);
            }
          });
    }
    return result;
  }

  /**
   * Reset all statistics for all file systems.
   */
  public static synchronized void clearStatistics() {
    GlobalStorageStatistics.INSTANCE.reset();
  }

  /**
   * Print all statistics for all file systems to {@code System.out}
   */
  public static synchronized
  void printStatistics() throws IOException {
    for (Map.Entry<Class<? extends FileSystem>, Statistics> pair:
            statisticsTable.entrySet()) {
      System.out.println("  FileSystem " + pair.getKey().getName() +
                         ": " + pair.getValue());
    }
  }

  // Symlinks are temporarily disabled - see HADOOP-10020 and HADOOP-10052
  private static boolean symlinksEnabled = false;

  @VisibleForTesting
  public static boolean areSymlinksEnabled() {
    return symlinksEnabled;
  }

  @VisibleForTesting
  public static void enableSymlinks() {
    symlinksEnabled = true;
  }

  /**
   * Get the StorageStatistics for this FileSystem object.  These statistics are
   * per-instance.  They are not shared with any other FileSystem object.
   *
   * <p>This is a default method which is intended to be overridden by
   * subclasses. The default implementation returns an empty storage statistics
   * object.</p>
   *
   * @return    The StorageStatistics for this FileSystem instance.
   *            Will never be null.
   */
  public StorageStatistics getStorageStatistics() {
    return new EmptyStorageStatistics(getUri().toString());
  }

  /**
   * Get the global storage statistics.
   */
  public static GlobalStorageStatistics getGlobalStorageStatistics() {
    return GlobalStorageStatistics.INSTANCE;
  }

  private static final class FileSystemDataOutputStreamBuilder extends
      FSDataOutputStreamBuilder<FSDataOutputStream,
        FileSystemDataOutputStreamBuilder> {

    /**
     * Constructor.
     */
    protected FileSystemDataOutputStreamBuilder(FileSystem fileSystem, Path p) {
      super(fileSystem, p);
    }

    @Override
    public FSDataOutputStream build() throws IOException {
      if (getFlags().contains(CreateFlag.CREATE) ||
          getFlags().contains(CreateFlag.OVERWRITE)) {
        if (isRecursive()) {
          return getFS().create(getPath(), getPermission(), getFlags(),
              getBufferSize(), getReplication(), getBlockSize(), getProgress(),
              getChecksumOpt());
        } else {
          return getFS().createNonRecursive(getPath(), getPermission(),
              getFlags(), getBufferSize(), getReplication(), getBlockSize(),
              getProgress());
        }
      } else if (getFlags().contains(CreateFlag.APPEND)) {
        return getFS().append(getPath(), getBufferSize(), getProgress());
      }
      throw new IOException("Must specify either create, overwrite or append");
    }

    @Override
    protected FileSystemDataOutputStreamBuilder getThisBuilder() {
      return this;
    }
  }

  /**
   * Create a new FSDataOutputStreamBuilder for the file with path.
   * Files are overwritten by default.
   *
   * @param path file path
   * @return a FSDataOutputStreamBuilder object to build the file
   *
   * HADOOP-14384. Temporarily reduce the visibility of method before the
   * builder interface becomes stable.
   */
  public FSDataOutputStreamBuilder createFile(Path path) {
    return new FileSystemDataOutputStreamBuilder(this, path)
        .create().overwrite(true);
  }

  /**
   * Create a Builder to append a file.
   * @param path file path.
   * @return a {@link FSDataOutputStreamBuilder} to build file append request.
   */
  public FSDataOutputStreamBuilder appendFile(Path path) {
    return new FileSystemDataOutputStreamBuilder(this, path).append();
  }
}
