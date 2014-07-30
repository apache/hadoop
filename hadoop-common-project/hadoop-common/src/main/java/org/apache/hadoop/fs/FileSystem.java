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
package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;

import com.google.common.annotations.VisibleForTesting;

/****************************************************************
 * An abstract base class for a fairly generic filesystem.  It
 * may be implemented as a distributed filesystem, or as a "local"
 * one that reflects the locally-connected disk.  The local version
 * exists for small Hadoop instances and for testing.
 *
 * <p>
 *
 * All user code that may potentially use the Hadoop Distributed
 * File System should be written to use a FileSystem object.  The
 * Hadoop DFS is a multi-machine system that appears as a single
 * disk.  It's useful because of its fault tolerance and potentially
 * very large capacity.
 * 
 * <p>
 * The local implementation is {@link LocalFileSystem} and distributed
 * implementation is DistributedFileSystem.
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileSystem extends Configured implements Closeable {
  public static final String FS_DEFAULT_NAME_KEY = 
                   CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
  public static final String DEFAULT_FS = 
                   CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT;

  public static final Log LOG = LogFactory.getLog(FileSystem.class);

  /**
   * Priority of the FileSystem shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  /** FileSystem cache */
  static final Cache CACHE = new Cache();

  /** The key this instance is stored under in the cache. */
  private Cache.Key key;

  /** Recording statistics per a FileSystem class */
  private static final Map<Class<? extends FileSystem>, Statistics> 
    statisticsTable =
      new IdentityHashMap<Class<? extends FileSystem>, Statistics>();
  
  /**
   * The statistics for this file system.
   */
  protected Statistics statistics;

  /**
   * A cache of files that should be deleted when filsystem is closed
   * or the JVM is exited.
   */
  private Set<Path> deleteOnExit = new TreeSet<Path>();
  
  boolean resolveSymlinks;
  /**
   * This method adds a file system for testing so that we can find it later. It
   * is only for testing.
   * @param uri the uri to store it under
   * @param conf the configuration to store it under
   * @param fs the file system to store
   * @throws IOException
   */
  static void addFileSystemForTesting(URI uri, Configuration conf,
      FileSystem fs) throws IOException {
    CACHE.map.put(new Cache.Key(uri, conf), fs);
  }

  /**
   * Get a filesystem instance based on the uri, the passed
   * configuration and the user
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return the filesystem instance
   * @throws IOException
   * @throws InterruptedException
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
   * Returns the configured filesystem implementation.
   * @param conf the configuration to use
   */
  public static FileSystem get(Configuration conf) throws IOException {
    return get(getDefaultUri(conf), conf);
  }
  
  /** Get the default filesystem URI from a configuration.
   * @param conf the configuration to use
   * @return the uri of the default filesystem
   */
  public static URI getDefaultUri(Configuration conf) {
    return URI.create(fixName(conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)));
  }

  /** Set the default filesystem URI in a configuration.
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  public static void setDefaultUri(Configuration conf, URI uri) {
    conf.set(FS_DEFAULT_NAME_KEY, uri.toString());
  }

  /** Set the default filesystem URI in a configuration.
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  public static void setDefaultUri(Configuration conf, String uri) {
    setDefaultUri(conf, URI.create(fixName(uri)));
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    statistics = getStatistics(name.getScheme(), getClass());    
    resolveSymlinks = conf.getBoolean(
        CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY,
        CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT);
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   * This implementation throws an <code>UnsupportedOperationException</code>.
   *
   * @return the protocol scheme for the FileSystem.
   */
  public String getScheme() {
    throw new UnsupportedOperationException("Not implemented by the " + getClass().getSimpleName() + " FileSystem implementation");
  }

  /** Returns a URI whose scheme and authority identify this FileSystem.*/
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
   * This is filesystem-dependent, but may for example consist of
   * canonicalizing the hostname using DNS and adding the default
   * port if not specified.
   * 
   * The default implementation simply fills in the default port if
   * not specified and if the filesystem has a default port.
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
   * Get the default port for this file system.
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

    // Uses the default file system if not fully qualified
    return get(absOrFqPath.toUri(), conf);
  }

  /**
   * Get a canonical service name for this file system.  The token cache is
   * the only user of the canonical service name, and uses it to lookup this
   * filesystem's service tokens.
   * If file system provides a token of its own then it must have a canonical
   * name, otherwise canonical name can be null.
   * 
   * Default Impl: If the file system has child file systems 
   * (such as an embedded file system) then it is assumed that the fs has no
   * tokens of its own and hence returns a null name; otherwise a service
   * name is built using Uri and port.
   * 
   * @return a service string that uniquely identifies this file system, null
   *         if the filesystem does not implement tokens
   * @see SecurityUtil#buildDTServiceName(URI, int) 
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
  public String getCanonicalServiceName() {
    return (getChildFileSystems() == null)
      ? SecurityUtil.buildDTServiceName(getUri(), getDefaultPort())
      : null;
  }

  /** @deprecated call #getUri() instead.*/
  @Deprecated
  public String getName() { return getUri().toString(); }

  /** @deprecated call #get(URI,Configuration) instead. */
  @Deprecated
  public static FileSystem getNamed(String name, Configuration conf)
    throws IOException {
    return get(URI.create(fixName(name)), conf);
  }
  
  /** Update old-format filesystem names, for back-compatibility.  This should
   * eventually be replaced with a checkName() method that throws an exception
   * for old-format names. */ 
  private static String fixName(String name) {
    // convert old-format name to new-format name
    if (name.equals("local")) {         // "local" is now "file:///".
      LOG.warn("\"local\" is a deprecated filesystem name."
               +" Use \"file:///\" instead.");
      name = "file:///";
    } else if (name.indexOf('/')==-1) {   // unqualified is "hdfs://"
      LOG.warn("\""+name+"\" is a deprecated filesystem name."
               +" Use \"hdfs://"+name+"/\" instead.");
      name = "hdfs://"+name;
    }
    return name;
  }

  /**
   * Get the local file system.
   * @param conf the configuration to configure the file system with
   * @return a LocalFileSystem
   */
  public static LocalFileSystem getLocal(Configuration conf)
    throws IOException {
    return (LocalFileSystem)get(LocalFileSystem.NAME, conf);
  }

  /** Returns the FileSystem for this URI's scheme and authority.  The scheme
   * of the URI determines a configuration property name,
   * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
   * The entire URI is passed to the FileSystem instance's initialize method.
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
      return createFileSystem(uri, conf);
    }

    return CACHE.get(uri, conf);
  }

  /**
   * Returns the FileSystem for this URI's scheme and authority and the 
   * passed user. Internally invokes {@link #newInstance(URI, Configuration)}
   * @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
   * @return filesystem instance
   * @throws IOException
   * @throws InterruptedException
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
        return newInstance(uri,conf); 
      }
    });
  }
  /** Returns the FileSystem for this URI's scheme and authority.  The scheme
   * of the URI determines a configuration property name,
   * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
   * The entire URI is passed to the FileSystem instance's initialize method.
   * This always returns a new FileSystem object.
   */
  public static FileSystem newInstance(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null) {                       // no scheme: use default FS
      return newInstance(conf);
    }

    if (authority == null) {                       // no authority
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return newInstance(defaultUri, conf);              // return default
      }
    }
    return CACHE.getUnique(uri, conf);
  }

  /** Returns a unique configured filesystem implementation.
   * This always returns a new FileSystem object.
   * @param conf the configuration to use
   */
  public static FileSystem newInstance(Configuration conf) throws IOException {
    return newInstance(getDefaultUri(conf), conf);
  }

  /**
   * Get a unique local file system object
   * @param conf the configuration to configure the file system with
   * @return a LocalFileSystem
   * This always returns a new FileSystem object.
   */
  public static LocalFileSystem newInstanceLocal(Configuration conf)
    throws IOException {
    return (LocalFileSystem)newInstance(LocalFileSystem.NAME, conf);
  }

  /**
   * Close all cached filesystems. Be sure those filesystems are not
   * used anymore.
   * 
   * @throws IOException
   */
  public static void closeAll() throws IOException {
    CACHE.closeAll();
  }

  /**
   * Close all cached filesystems for a given UGI. Be sure those filesystems 
   * are not used anymore.
   * @param ugi user group info to close
   * @throws IOException
   */
  public static void closeAllForUGI(UserGroupInformation ugi) 
  throws IOException {
    CACHE.closeAll(ugi);
  }

  /** 
   * Make sure that a path specifies a FileSystem.
   * @param path to use
   */
  public Path makeQualified(Path path) {
    checkPath(path);
    return path.makeQualified(this.getUri(), this.getWorkingDirectory());
  }
    
  /**
   * Get a new delegation token for this file system.
   * This is an internal method that should have been declared protected
   * but wasn't historically.
   * Callers should use {@link #addDelegationTokens(String, Credentials)}
   * 
   * @param renewer the account name that is allowed to renew the token.
   * @return a new delegation token
   * @throws IOException
   */
  @InterfaceAudience.Private()
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return null;
  }
  
  /**
   * Obtain all delegation tokens used by this FileSystem that are not
   * already present in the given Credentials.  Existing tokens will neither
   * be verified as valid nor having the given renewer.  Missing tokens will
   * be acquired and added to the given Credentials.
   * 
   * Default Impl: works for simple fs with its own token
   * and also for an embedded fs whose tokens are those of its
   * children file system (i.e. the embedded fs has not tokens of its
   * own).
   * 
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add new delegation tokens
   * @return list of new delegation tokens
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
  public Token<?>[] addDelegationTokens(
      final String renewer, Credentials credentials) throws IOException {
    if (credentials == null) {
      credentials = new Credentials();
    }
    final List<Token<?>> tokens = new ArrayList<Token<?>>();
    collectDelegationTokens(renewer, credentials, tokens);
    return tokens.toArray(new Token<?>[tokens.size()]);
  }
  
  /**
   * Recursively obtain the tokens for this FileSystem and all descended
   * FileSystems as determined by getChildFileSystems().
   * @param renewer the user allowed to renew the delegation tokens
   * @param credentials cache in which to add the new delegation tokens
   * @param tokens list in which to add acquired tokens
   * @throws IOException
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
   * has multiple child FileSystems, then it should return a unique list
   * of those FileSystems.  Default is to return null to signify no children.
   * 
   * @return FileSystems used by this FileSystem
   */
  @InterfaceAudience.LimitedPrivate({ "HDFS" })
  @VisibleForTesting
  public FileSystem[] getChildFileSystems() {
    return null;
  }
  
  /** create a file with the provided permission
   * The permission of the file is set to be the provided permission as in
   * setPermission, not permission&~umask
   * 
   * It is implemented using two RPCs. It is understood that it is inefficient,
   * but the implementation is thread-safe. The other option is to change the
   * value of umask in configuration to be 0, but it is not thread-safe.
   * 
   * @param fs file system handle
   * @param file the name of the file to be created
   * @param permission the permission of the file
   * @return an output stream
   * @throws IOException
   */
  public static FSDataOutputStream create(FileSystem fs,
      Path file, FsPermission permission) throws IOException {
    // create the file with default permission
    FSDataOutputStream out = fs.create(file);
    // set its permission to the supplied one
    fs.setPermission(file, permission);
    return out;
  }

  /** create a directory with the provided permission
   * The permission of the directory is set to be the provided permission as in
   * setPermission, not permission&~umask
   * 
   * @see #create(FileSystem, Path, FsPermission)
   * 
   * @param fs file system handle
   * @param dir the name of the directory to be created
   * @param permission the permission of the directory
   * @return true if the directory creation succeeds; false otherwise
   * @throws IOException
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
   * @param path to check
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
    throw new IllegalArgumentException("Wrong FS: "+path+
                                       ", expected: "+this.getUri());
  }

  /**
   * Return an array containing hostnames, offset and size of 
   * portions of the given file.  For a nonexistent 
   * file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns 
   * hostnames of machines that contain the given file.
   *
   * The FileSystem will simply return an elt containing 'localhost'.
   *
   * @param file FilesStatus to get data from
   * @param start offset into the given file
   * @param len length for which to get locations for
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
    String[] name = { "localhost:50010" };
    String[] host = { "localhost" };
    return new BlockLocation[] {
      new BlockLocation(name, host, 0, file.getLen()) };
  }
 

  /**
   * Return an array containing hostnames, offset and size of 
   * portions of the given file.  For a nonexistent 
   * file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns 
   * hostnames of machines that contain the given file.
   *
   * The FileSystem will simply return an elt containing 'localhost'.
   *
   * @param p path is used to identify an FS since an FS could have
   *          another FS that it could be delegating the call to
   * @param start offset into the given file
   * @param len length for which to get locations for
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
   * Return a set of server default configuration values
   * @return server default configuration values
   * @throws IOException
   * @deprecated use {@link #getServerDefaults(Path)} instead
   */
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    Configuration conf = getConf();
    // CRC32 is chosen as default as it is available in all 
    // releases that support checksum.
    // The client trash configuration is ignored.
    return new FsServerDefaults(getDefaultBlockSize(), 
        conf.getInt("io.bytes.per.checksum", 512), 
        64 * 1024, 
        getDefaultReplication(),
        conf.getInt("io.file.buffer.size", 4096),
        false,
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT,
        DataChecksum.Type.CRC32);
  }

  /**
   * Return a set of server default configuration values
   * @param p path is used to identify an FS since an FS could have
   *          another FS that it could be delegating the call to
   * @return server default configuration values
   * @throws IOException
   */
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    return getServerDefaults();
  }

  /**
   * Return the fully-qualified path of path f resolving the path
   * through any symlinks or mount point
   * @param p path to be resolved
   * @return fully qualified path 
   * @throws FileNotFoundException
   */
   public Path resolvePath(final Path p) throws IOException {
     checkPath(p);
     return getFileStatus(p).getPath();
   }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public abstract FSDataInputStream open(Path f, int bufferSize)
    throws IOException;
    
  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file to open
   */
  public FSDataInputStream open(Path f) throws IOException {
    return open(f, getConf().getInt("io.file.buffer.size", 4096));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   */
  public FSDataOutputStream create(Path f) throws IOException {
    return create(f, true);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an exception will be thrown.
   */
  public FSDataOutputStream create(Path f, boolean overwrite)
      throws IOException {
    return create(f, overwrite, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   * @param f the file to create
   * @param progress to report progress
   */
  public FSDataOutputStream create(Path f, Progressable progress) 
      throws IOException {
    return create(f, true, 
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(f),
                  getDefaultBlockSize(f), progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   * @param f the file to create
   * @param replication the replication factor
   */
  public FSDataOutputStream create(Path f, short replication)
      throws IOException {
    return create(f, true, 
                  getConf().getInt("io.file.buffer.size", 4096),
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
   */
  public FSDataOutputStream create(Path f, short replication, 
      Progressable progress) throws IOException {
    return create(f, true, 
                  getConf().getInt(
                      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
                  replication,
                  getDefaultBlockSize(f), progress);
  }

    
  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to create
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
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
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the path of the file to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
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
   */
  public FSDataOutputStream create(Path f, 
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize
                                   ) throws IOException {
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
   */
  public FSDataOutputStream create(Path f,
                                            boolean overwrite,
                                            int bufferSize,
                                            short replication,
                                            long blockSize,
                                            Progressable progress
                                            ) throws IOException {
    return this.create(f, FsPermission.getFileDefault().applyUMask(
        FsPermission.getUMask(getConf())), overwrite, bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
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
   * @param permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
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
   * checksum option
   * @param f the file name to open
   * @param permission
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @param checksumOpt checksum parameter. If null, the values
   *        found in conf will be used.
   * @throws IOException
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

  /*.
   * This create has been added to support the FileContext that processes
   * the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected FSDataOutputStream primitiveCreate(Path f,
     FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
     short replication, long blockSize, Progressable progress,
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
   */
  @Deprecated
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
    throws IOException {
    // Default impl is to assume that permissions do not matter and hence
    // calling the regular mkdirs is good enough.
    // FSs that implement permissions should override this.
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
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
  @Deprecated
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
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
   @Deprecated
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
    * @param permission
    * @param flags {@link CreateFlag}s to use for this stream.
    * @param bufferSize the size of the buffer to be used.
    * @param replication required block replication for the file.
    * @param blockSize
    * @param progress
    * @throws IOException
    * @see #setPermission(Path, FsPermission)
    * @deprecated API only for 0.20-append
    */
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      throw new IOException("createNonRecursive unsupported for this filesystem "
          + this.getClass());
    }

  /**
   * Creates the given Path as a brand-new zero-length file.  If
   * create fails, or if it already existed, return false.
   *
   * @param f path to use for create
   */
  public boolean createNewFile(Path f) throws IOException {
    if (exists(f)) {
      return false;
    } else {
      create(f, false, getConf().getInt("io.file.buffer.size", 4096)).close();
      return true;
    }
  }

  /**
   * Append to an existing file (optional operation).
   * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
   * @param f the existing file to be appended.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f) throws IOException {
    return append(f, getConf().getInt("io.file.buffer.size", 4096), null);
  }
  /**
   * Append to an existing file (optional operation).
   * Same as append(f, bufferSize, null).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return append(f, bufferSize, null);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException
   */
  public abstract FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException;

  /**
   * Concat existing files together.
   * @param trg the path to the target destination.
   * @param psrcs the paths to the sources to use for the concatenation.
   * @throws IOException
   */
  public void concat(final Path trg, final Path [] psrcs) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }

 /**
   * Get replication.
   * 
   * @deprecated Use getFileStatus() instead
   * @param src file name
   * @return file replication
   * @throws IOException
   */ 
  @Deprecated
  public short getReplication(Path src) throws IOException {
    return getFileStatus(src).getReplication();
  }

  /**
   * Set replication for an existing file.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(Path src, short replication)
    throws IOException {
    return true;
  }

  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   * @return true if rename is successful
   */
  public abstract boolean rename(Path src, Path dst) throws IOException;

  /**
   * Renames Path src to Path dst
   * <ul>
   * <li
   * <li>Fails if src is a file and dst is a directory.
   * <li>Fails if src is a directory and dst is a file.
   * <li>Fails if the parent of dst does not exist or is a file.
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
   * Delete a file 
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
   * @throws IOException
   */
  public abstract boolean delete(Path f, boolean recursive) throws IOException;

  /**
   * Mark a path to be deleted when FileSystem is closed.
   * When the JVM shuts down,
   * all FileSystem objects will be closed automatically.
   * Then,
   * the marked path will be deleted as a result of closing the FileSystem.
   *
   * The path has to exist in the file system.
   * 
   * @param f the path to delete.
   * @return  true if deleteOnExit is successful, otherwise false.
   * @throws IOException
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
   * Cancel the deletion of the path when the FileSystem is closed
   * @param f the path to cancel deletion
   */
  public boolean cancelDeleteOnExit(Path f) {
    synchronized (deleteOnExit) {
      return deleteOnExit.remove(f);
    }
  }

  /**
   * Delete all files that were marked as delete-on-exit. This recursively
   * deletes all files in the specified paths.
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
          LOG.info("Ignoring failure to deleteOnExit for path " + path);
        }
        iter.remove();
      }
    }
  }
  
  /** Check if exists.
   * @param f source file
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
   * @param f path to check
   */
  public boolean isDirectory(Path f) throws IOException {
    try {
      return getFileStatus(f).isDirectory();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /** True iff the named path is a regular file.
   * Note: Avoid using this method. Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   * @param f path to check
   */
  public boolean isFile(Path f) throws IOException {
    try {
      return getFileStatus(f).isFile();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }
  
  /** The number of bytes in a file. */
  /** @deprecated Use getFileStatus() instead */
  @Deprecated
  public long getLength(Path f) throws IOException {
    return getFileStatus(f).getLen();
  }
    
  /** Return the {@link ContentSummary} of a given {@link Path}.
  * @param f path to use
  */
  public ContentSummary getContentSummary(Path f) throws IOException {
    FileStatus status = getFileStatus(f);
    if (status.isFile()) {
      // f is a file
      return new ContentSummary(status.getLen(), 1, 0);
    }
    // f is a directory
    long[] summary = {0, 0, 1};
    for(FileStatus s : listStatus(f)) {
      ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath()) :
                                     new ContentSummary(s.getLen(), 1, 0);
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
    }
    return new ContentSummary(summary[0], summary[1], summary[2]);
  }

  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return true;
      }     
    };
    
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public abstract FileStatus[] listStatus(Path f) throws FileNotFoundException, 
                                                         IOException;
    
  /*
   * Filter files/directories in the given path using the user-supplied path
   * filter. Results are added to the given array <code>results</code>.
   */
  private void listStatus(ArrayList<FileStatus> results, Path f,
      PathFilter filter) throws FileNotFoundException, IOException {
    FileStatus listing[] = listStatus(f);
    if (listing == null) {
      throw new IOException("Error accessing " + f);
    }

    for (int i = 0; i < listing.length; i++) {
      if (filter.accept(listing[i].getPath())) {
        results.add(listing[i]);
      }
    }
  }

  /**
   * @return an iterator over the corrupt files under the given path
   * (may contain duplicates if a file has more than one corrupt block)
   * @throws IOException
   */
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    throw new UnsupportedOperationException(getClass().getCanonicalName() +
                                            " does not support" +
                                            " listCorruptFileBlocks");
  }

  /**
   * Filter files/directories in the given path using the user-supplied path
   * filter.
   * 
   * @param f
   *          a path name
   * @param filter
   *          the user-supplied path filter
   * @return an array of FileStatus objects for the files under the given path
   *         after applying the filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation   
   */
  public FileStatus[] listStatus(Path f, PathFilter filter) 
                                   throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    listStatus(results, f, filter);
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * Filter files/directories in the given list of paths using default
   * path filter.
   * 
   * @param files
   *          a list of paths
   * @return a list of statuses for the files under the given paths after
   *         applying the filter default Path filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files)
      throws FileNotFoundException, IOException {
    return listStatus(files, DEFAULT_FILTER);
  }

  /**
   * Filter files/directories in the given list of paths using user-supplied
   * path filter.
   * 
   * @param files
   *          a list of paths
   * @param filter
   *          the user-supplied path filter
   * @return a list of statuses for the files under the given paths after
   *         applying the filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
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
   * @throws IOException
   */
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return new Globber(this, pathPattern, DEFAULT_FILTER).glob();
  }
  
  /**
   * Return an array of FileStatus objects whose path names match pathPattern
   * and is accepted by the user-supplied path filter. Results are sorted by
   * their path names.
   * Return null if pathPattern has no glob and the path does not exist.
   * Return an empty array if pathPattern has a glob and no path matches it. 
   * 
   * @param pathPattern
   *          a regular expression specifying the path pattern
   * @param filter
   *          a user-supplied path filter
   * @return an array of FileStatus objects
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
   * Listing a directory
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
          throw new NoSuchElementException("No more entry in " + f);
        }
        FileStatus result = stats[i++];
        BlockLocation[] locs = result.isFile() ?
            getFileBlockLocations(result.getPath(), 0, result.getLen()) :
            null;
        return new LocatedFileStatus(result, locs);
      }
    };
  }

  /**
   * List the statuses and block locations of the files in the given path.
   * 
   * If the path is a directory, 
   *   if recursive is false, returns files in the directory;
   *   if recursive is true, return files in the subtree rooted at the path.
   * If the path is a file, return the file's status and block locations.
   * 
   * @param f is the path
   * @param recursive if the subdirectories need to be traversed recursively
   *
   * @return an iterator that traverses statuses of the files
   *
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public RemoteIterator<LocatedFileStatus> listFiles(
      final Path f, final boolean recursive)
  throws FileNotFoundException, IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private Stack<RemoteIterator<LocatedFileStatus>> itors = 
        new Stack<RemoteIterator<LocatedFileStatus>>();
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
  
  /** Return the current user's home directory in this filesystem.
   * The default implementation returns "/user/$USER/".
   */
  public Path getHomeDirectory() {
    return this.makeQualified(
        new Path("/user/"+System.getProperty("user.name")));
  }


  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   * 
   * @param new_dir
   */
  public abstract void setWorkingDirectory(Path new_dir);
    
  /**
   * Get the current working directory for the given file system
   * @return the directory pathname
   */
  public abstract Path getWorkingDirectory();
  
  
  /**
   * Note: with the new FilesContext class, getWorkingDirectory()
   * will be removed. 
   * The working directory is implemented in FilesContext.
   * 
   * Some file systems like LocalFileSystem have an initial workingDir
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
   */
  public boolean mkdirs(Path f) throws IOException {
    return mkdirs(f, FsPermission.getDirDefault());
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   * @param f path to create
   * @param permission to apply to f
   */
  public abstract boolean mkdirs(Path f, FsPermission permission
      ) throws IOException;

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name and the source is kept intact afterwards
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(false, src, dst);
  }

  /**
   * The src files is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   * @param srcs path
   * @param dst path
   */
  public void moveFromLocalFile(Path[] srcs, Path dst)
    throws IOException {
    copyFromLocalFile(true, true, srcs, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   * @param src path
   * @param dst path
   */
  public void moveFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(true, src, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
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
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param srcs array of paths which are source
   * @param dst path
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path[] srcs, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
  }
  
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, overwrite, conf);
  }
    
  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * @param src path
   * @param dst path
   */
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    copyToLocalFile(false, src, dst);
  }
    
  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * Remove the source afterwards
   * @param src path
   * @param dst path
   */
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    copyToLocalFile(true, src, dst);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   * @param delSrc whether to delete the src
   * @param src path
   * @param dst path
   */   
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    copyToLocalFile(delSrc, src, dst, false);
  }
  
    /**
   * The src file is under FS, and the dst is on the local disk. Copy it from FS
   * control to the local dst name. delSrc indicates if the src will be removed
   * or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem
   * as local file system or not. RawLocalFileSystem is non crc file system.So,
   * It will not create any crc files at local.
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
   * @throws IOException
   *           - if any IO error
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
   * Returns a local File that the user can write output to.  The caller
   * provides both the eventual FS target name and the local working
   * file.  If the FS is local, we write directly into the target.  If
   * the FS is remote, we write into the tmp local area.
   * @param fsOutputFile path of output file
   * @param tmpLocalFile path of local tmp file
   */
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   * @param fsOutputFile path of output file
   * @param tmpLocalFile path to local tmp file
   */
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * No more filesystem operations are needed.  Will
   * release any held locks.
   */
  @Override
  public void close() throws IOException {
    // delete all files that were marked as delete-on-exit.
    processDeleteOnExit();
    CACHE.remove(this.key, this);
  }

  /** Return the total size of all files in the filesystem.*/
  public long getUsed() throws IOException{
    long used = 0;
    FileStatus[] files = listStatus(new Path("/"));
    for(FileStatus file:files){
      used += file.getLen();
    }
    return used;
  }
  
  /**
   * Get the block size for a particular file.
   * @param f the filename
   * @return the number of bytes in a block
   */
  /** @deprecated Use getFileStatus() instead */
  @Deprecated
  public long getBlockSize(Path f) throws IOException {
    return getFileStatus(f).getBlockSize();
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    // default to 32MB: large enough to minimize the impact of seeks
    return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
  }
    
  /** Return the number of bytes that large input files should be optimally
   * be split into to minimize i/o time.  The given path will be used to
   * locate the actual filesystem.  The full path does not have to exist.
   * @param f path of file
   * @return the default block size for the path's filesystem
   */
  public long getDefaultBlockSize(Path f) {
    return getDefaultBlockSize();
  }

  /**
   * Get the default replication.
   * @deprecated use {@link #getDefaultReplication(Path)} instead
   */
  @Deprecated
  public short getDefaultReplication() { return 1; }

  /**
   * Get the default replication for a path.   The given path will be used to
   * locate the actual filesystem.  The full path does not have to exist.
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
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public abstract FileStatus getFileStatus(Path f) throws IOException;

  /**
   * Checks if the user can access a path.  The mode specifies which access
   * checks to perform.  If the requested permissions are granted, then the
   * method returns normally.  If access is denied, then the method throws an
   * {@link AccessControlException}.
   * <p/>
   * The default implementation of this method calls {@link #getFileStatus(Path)}
   * and checks the returned permissions against the requested permissions.
   * Note that the getFileStatus call will be subject to authorization checks.
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
   * @throws IOException for any error
   */
  @InterfaceAudience.Private
  static void checkAccessPermissions(FileStatus stat, FsAction mode)
      throws IOException {
    FsPermission perm = stat.getPermission();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user = ugi.getShortUserName();
    List<String> groups = Arrays.asList(ugi.getGroupNames());
    if (user.equals(stat.getOwner())) {
      if (perm.getUserAction().implies(mode)) {
        return;
      }
    } else if (groups.contains(stat.getGroup())) {
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
   * See {@link FileContext#fixRelativePart}
   */
  protected Path fixRelativePart(Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new Path(getWorkingDirectory(), p);
    }
  }

  /**
   * See {@link FileContext#createSymlink(Path, Path, boolean)}
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
   * See {@link FileContext#getFileLinkStatus(Path)}
   */
  public FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    // Supporting filesystems should override this method
    return getFileStatus(f);
  }

  /**
   * See {@link AbstractFileSystem#supportsSymlinks()}
   */
  public boolean supportsSymlinks() {
    return false;
  }

  /**
   * See {@link FileContext#getLinkTarget(Path)}
   */
  public Path getLinkTarget(Path f) throws IOException {
    // Supporting filesystems should override this method
    throw new UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * See {@link AbstractFileSystem#getLinkTarget(Path)}
   */
  protected Path resolveLink(Path f) throws IOException {
    // Supporting filesystems should override this method
    throw new UnsupportedOperationException(
        "Filesystem does not support symlinks!");
  }

  /**
   * Get the checksum of a file.
   *
   * @param f The file path
   * @return The file checksum.  The default return value is null,
   *  which indicates that no checksum algorithm is implemented
   *  in the corresponding FileSystem.
   */
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return getFileChecksum(f, Long.MAX_VALUE);
  }

  /**
   * Get the checksum of a file, from the beginning of the file till the
   * specific length.
   * @param f The file path
   * @param length The length of the file range for checksum calculation
   * @return The file checksum.
   */
  public FileChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    return null;
  }

  /**
   * Set the verify checksum flag. This is only applicable if the 
   * corresponding FileSystem supports checksum. By default doesn't do anything.
   * @param verifyChecksum
   */
  public void setVerifyChecksum(boolean verifyChecksum) {
    //doesn't do anything
  }

  /**
   * Set the write checksum flag. This is only applicable if the 
   * corresponding FileSystem supports checksum. By default doesn't do anything.
   * @param writeChecksum
   */
  public void setWriteChecksum(boolean writeChecksum) {
    //doesn't do anything
  }

  /**
   * Returns a status object describing the use and capacity of the
   * file system. If the file system has multiple partitions, the
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
   * file system. If the file system has multiple partitions, the
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
   * @param p
   * @param permission
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
   */
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
  }

  /**
   * Set access time of a file
   * @param p The path
   * @param mtime Set the modification time of this file.
   *              The number of milliseconds since Jan 1, 1970. 
   *              A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   *              The number of milliseconds since Jan 1, 1970. 
   *              A value of -1 means that this call should not set access time.
   */
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
  }

  /**
   * Create a snapshot with a default name.
   * @param path The directory where snapshots will be taken.
   * @return the snapshot path.
   */
  public final Path createSnapshot(Path path) throws IOException {
    return createSnapshot(path, null);
  }

  /**
   * Create a snapshot
   * @param path The directory where snapshots will be taken.
   * @param snapshotName The name of the snapshot
   * @return the snapshot path.
   */
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support createSnapshot");
  }
  
  /**
   * Rename a snapshot
   * @param path The directory path where the snapshot was taken
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   * @throws IOException
   */
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support renameSnapshot");
  }
  
  /**
   * Delete a snapshot of a directory
   * @param path  The directory that the to-be-deleted snapshot belongs to
   * @param snapshotName The name of the snapshot
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
   * @param aclSpec List<AclEntry> describing entries to remove
   * @throws IOException if an ACL could not be modified
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
   * @param aclSpec List<AclEntry> describing modifications, must include entries
   *   for user, group, and others for compatibility with permission bits.
   * @throws IOException if an ACL could not be modified
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
   */
  public AclStatus getAclStatus(Path path) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getAclStatus");
  }

  /**
   * Set an xattr of a file or directory.
   * The name must be prefixed with the namespace followed by ".". For example,
   * "user.attr".
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to modify
   * @param name xattr name.
   * @param value xattr value.
   * @throws IOException
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
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to modify
   * @param name xattr name.
   * @param value xattr value.
   * @param flag xattr set flag
   * @throws IOException
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
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attribute
   * @param name xattr name.
   * @return byte[] xattr value.
   * @throws IOException
   */
  public byte[] getXAttr(Path path, String name) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getXAttr");
  }

  /**
   * Get all of the xattr name/value pairs for a file or directory.
   * Only those xattrs which the logged-in user has permissions to view
   * are returned.
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @return Map<String, byte[]> describing the XAttrs of the file or directory
   * @throws IOException
   */
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getXAttrs");
  }

  /**
   * Get all of the xattrs name/value pairs for a file or directory.
   * Only those xattrs which the logged-in user has permissions to view
   * are returned.
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @param names XAttr names.
   * @return Map<String, byte[]> describing the XAttrs of the file or directory
   * @throws IOException
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
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @return List<String> of the XAttr names of the file or directory
   * @throws IOException
   */
  public List<String> listXAttrs(Path path) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
            + " doesn't support listXAttrs");
  }

  /**
   * Remove an xattr of a file or directory.
   * The name must be prefixed with the namespace followed by ".". For example,
   * "user.attr".
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to remove extended attribute
   * @param name xattr name
   * @throws IOException
   */
  public void removeXAttr(Path path, String name) throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support removeXAttr");
  }

  // making it volatile to be able to do a double checked locking
  private volatile static boolean FILE_SYSTEMS_LOADED = false;

  private static final Map<String, Class<? extends FileSystem>>
    SERVICE_FILE_SYSTEMS = new HashMap<String, Class<? extends FileSystem>>();

  private static void loadFileSystems() {
    synchronized (FileSystem.class) {
      if (!FILE_SYSTEMS_LOADED) {
        ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
        for (FileSystem fs : serviceLoader) {
          SERVICE_FILE_SYSTEMS.put(fs.getScheme(), fs.getClass());
        }
        FILE_SYSTEMS_LOADED = true;
      }
    }
  }

  public static Class<? extends FileSystem> getFileSystemClass(String scheme,
      Configuration conf) throws IOException {
    if (!FILE_SYSTEMS_LOADED) {
      loadFileSystems();
    }
    Class<? extends FileSystem> clazz = null;
    if (conf != null) {
      clazz = (Class<? extends FileSystem>) conf.getClass("fs." + scheme + ".impl", null);
    }
    if (clazz == null) {
      clazz = SERVICE_FILE_SYSTEMS.get(scheme);
    }
    if (clazz == null) {
      throw new IOException("No FileSystem for scheme: " + scheme);
    }
    return clazz;
  }

  private static FileSystem createFileSystem(URI uri, Configuration conf
      ) throws IOException {
    Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
    if (clazz == null) {
      throw new IOException("No FileSystem for scheme: " + uri.getScheme());
    }
    FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
    fs.initialize(uri, conf);
    return fs;
  }

  /** Caching FileSystem objects */
  static class Cache {
    private final ClientFinalizer clientFinalizer = new ClientFinalizer();

    private final Map<Key, FileSystem> map = new HashMap<Key, FileSystem>();
    private final Set<Key> toAutoClose = new HashSet<Key>();

    /** A variable that makes all objects in the cache unique */
    private static AtomicLong unique = new AtomicLong(1);

    FileSystem get(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf);
      return getInternal(uri, conf, key);
    }

    /** The objects inserted into the cache using this method are all unique */
    FileSystem getUnique(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf, unique.getAndIncrement());
      return getInternal(uri, conf, key);
    }

    private FileSystem getInternal(URI uri, Configuration conf, Key key) throws IOException{
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
        if (conf.getBoolean("fs.automatic.close", true)) {
          toAutoClose.add(key);
        }
        return fs;
      }
    }

    synchronized void remove(Key key, FileSystem fs) {
      if (map.containsKey(key) && fs == map.get(key)) {
        map.remove(key);
        toAutoClose.remove(key);
        }
    }

    synchronized void closeAll() throws IOException {
      closeAll(false);
    }

    /**
     * Close all FileSystem instances in the Cache.
     * @param onlyAutomatic only close those that are marked for automatic closing
     */
    synchronized void closeAll(boolean onlyAutomatic) throws IOException {
      List<IOException> exceptions = new ArrayList<IOException>();

      // Make a copy of the keys in the map since we'll be modifying
      // the map while iterating over it, which isn't safe.
      List<Key> keys = new ArrayList<Key>();
      keys.addAll(map.keySet());

      for (Key key : keys) {
        final FileSystem fs = map.get(key);

        if (onlyAutomatic && !toAutoClose.contains(key)) {
          continue;
        }

        //remove from cache
        remove(key, fs);

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
          LOG.info("FileSystem.Cache.closeAll() threw an exception:\n" + e);
        }
      }
    }

    synchronized void closeAll(UserGroupInformation ugi) throws IOException {
      List<FileSystem> targetFSList = new ArrayList<FileSystem>();
      //Make a pass over the list and collect the filesystems to close
      //we cannot close inline since close() removes the entry from the Map
      for (Map.Entry<Key, FileSystem> entry : map.entrySet()) {
        final Key key = entry.getKey();
        final FileSystem fs = entry.getValue();
        if (ugi.equals(key.ugi) && fs != null) {
          targetFSList.add(fs);   
        }
      }
      List<IOException> exceptions = new ArrayList<IOException>();
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
        scheme = uri.getScheme()==null?"":uri.getScheme().toLowerCase();
        authority = uri.getAuthority()==null?"":uri.getAuthority().toLowerCase();
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
        if (obj != null && obj instanceof Key) {
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
      volatile long bytesRead;
      volatile long bytesWritten;
      volatile int readOps;
      volatile int largeReadOps;
      volatile int writeOps;
      /**
       * Stores a weak reference to the thread owning this StatisticsData.
       * This allows us to remove StatisticsData objects that pertain to
       * threads that no longer exist.
       */
      final WeakReference<Thread> owner;

      StatisticsData(WeakReference<Thread> owner) {
        this.owner = owner;
      }

      /**
       * Add another StatisticsData object to this one.
       */
      void add(StatisticsData other) {
        this.bytesRead += other.bytesRead;
        this.bytesWritten += other.bytesWritten;
        this.readOps += other.readOps;
        this.largeReadOps += other.largeReadOps;
        this.writeOps += other.writeOps;
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
    private final ThreadLocal<StatisticsData> threadData;
    
    /**
     * List of all thread-local data areas.  Protected by the Statistics lock.
     */
    private LinkedList<StatisticsData> allData;

    public Statistics(String scheme) {
      this.scheme = scheme;
      this.rootData = new StatisticsData(null);
      this.threadData = new ThreadLocal<StatisticsData>();
      this.allData = null;
    }

    /**
     * Copy constructor.
     * 
     * @param other    The input Statistics object which is cloned.
     */
    public Statistics(Statistics other) {
      this.scheme = other.scheme;
      this.rootData = new StatisticsData(null);
      other.visitAll(new StatisticsAggregator<Void>() {
        @Override
        public void accept(StatisticsData data) {
          rootData.add(data);
        }

        public Void aggregate() {
          return null;
        }
      });
      this.threadData = new ThreadLocal<StatisticsData>();
    }

    /**
     * Get or create the thread-local data associated with the current thread.
     */
    public StatisticsData getThreadStatistics() {
      StatisticsData data = threadData.get();
      if (data == null) {
        data = new StatisticsData(
            new WeakReference<Thread>(Thread.currentThread()));
        threadData.set(data);
        synchronized(this) {
          if (allData == null) {
            allData = new LinkedList<StatisticsData>();
          }
          allData.add(data);
        }
      }
      return data;
    }

    /**
     * Increment the bytes read in the statistics
     * @param newBytes the additional bytes read
     */
    public void incrementBytesRead(long newBytes) {
      getThreadStatistics().bytesRead += newBytes;
    }
    
    /**
     * Increment the bytes written in the statistics
     * @param newBytes the additional bytes written
     */
    public void incrementBytesWritten(long newBytes) {
      getThreadStatistics().bytesWritten += newBytes;
    }
    
    /**
     * Increment the number of read operations
     * @param count number of read operations
     */
    public void incrementReadOps(int count) {
      getThreadStatistics().readOps += count;
    }

    /**
     * Increment the number of large read operations
     * @param count number of large read operations
     */
    public void incrementLargeReadOps(int count) {
      getThreadStatistics().largeReadOps += count;
    }

    /**
     * Increment the number of write operations
     * @param count number of write operations
     */
    public void incrementWriteOps(int count) {
      getThreadStatistics().writeOps += count;
    }

    /**
     * Apply the given aggregator to all StatisticsData objects associated with
     * this Statistics object.
     *
     * For each StatisticsData object, we will call accept on the visitor.
     * Finally, at the end, we will call aggregate to get the final total. 
     *
     * @param         The visitor to use.
     * @return        The total.
     */
    private synchronized <T> T visitAll(StatisticsAggregator<T> visitor) {
      visitor.accept(rootData);
      if (allData != null) {
        for (Iterator<StatisticsData> iter = allData.iterator();
            iter.hasNext(); ) {
          StatisticsData data = iter.next();
          visitor.accept(data);
          if (data.owner.get() == null) {
            /*
             * If the thread that created this thread-local data no
             * longer exists, remove the StatisticsData from our list
             * and fold the values into rootData.
             */
            rootData.add(data);
            iter.remove();
          }
        }
      }
      return visitor.aggregate();
    }

    /**
     * Get the total number of bytes read
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
     * Get the total number of bytes written
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
     * Get the number of file system read operations such as list files
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
     * under a large directory
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


    @Override
    public String toString() {
      return visitAll(new StatisticsAggregator<String>() {
        private StatisticsData total = new StatisticsData(null);

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
     * This may seem like a counterintuitive way to reset the statsitics.  Why
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
        private StatisticsData total = new StatisticsData(null);

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
  }
  
  /**
   * Get the Map of Statistics object indexed by URI Scheme.
   * @return a Map having a key as URI scheme and value as Statistics object
   * @deprecated use {@link #getAllStatistics} instead
   */
  @Deprecated
  public static synchronized Map<String, Statistics> getStatistics() {
    Map<String, Statistics> result = new HashMap<String, Statistics>();
    for(Statistics stat: statisticsTable.values()) {
      result.put(stat.getScheme(), stat);
    }
    return result;
  }

  /**
   * Return the FileSystem classes that have Statistics
   */
  public static synchronized List<Statistics> getAllStatistics() {
    return new ArrayList<Statistics>(statisticsTable.values());
  }
  
  /**
   * Get the statistics for a particular file system
   * @param cls the class to lookup
   * @return a statistics object
   */
  public static synchronized 
  Statistics getStatistics(String scheme, Class<? extends FileSystem> cls) {
    Statistics result = statisticsTable.get(cls);
    if (result == null) {
      result = new Statistics(scheme);
      statisticsTable.put(cls, result);
    }
    return result;
  }
  
  /**
   * Reset all statistics for all file systems
   */
  public static synchronized void clearStatistics() {
    for(Statistics stat: statisticsTable.values()) {
      stat.reset();
    }
  }

  /**
   * Print all statistics for all file systems
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

  private static Configuration conf = null;

  @VisibleForTesting
  public static boolean areSymlinksEnabled() {
    return symlinksEnabled;
  }

  @VisibleForTesting
  public static void enableSymlinks() {
    symlinksEnabled = true;
  }
}
