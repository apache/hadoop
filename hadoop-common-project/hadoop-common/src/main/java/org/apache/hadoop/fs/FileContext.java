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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RpcClientException;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.ipc.UnexpectedServerException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ShutdownHookManager;

/**
 * The FileContext class provides an interface to the application writer for
 * using the Hadoop file system.
 * It provides a set of methods for the usual operation: create, open, 
 * list, etc 
 * 
 * <p>
 * <b> *** Path Names *** </b>
 * <p>
 * 
 * The Hadoop file system supports a URI name space and URI names.
 * It offers a forest of file systems that can be referenced using fully
 * qualified URIs.
 * Two common Hadoop file systems implementations are
 * <ul>
 * <li> the local file system: file:///path
 * <li> the hdfs file system hdfs://nnAddress:nnPort/path
 * </ul>
 * 
 * While URI names are very flexible, it requires knowing the name or address
 * of the server. For convenience one often wants to access the default system
 * in one's environment without knowing its name/address. This has an
 * additional benefit that it allows one to change one's default fs
 *  (e.g. admin moves application from cluster1 to cluster2).
 * <p>
 * 
 * To facilitate this, Hadoop supports a notion of a default file system.
 * The user can set his default file system, although this is
 * typically set up for you in your environment via your default config.
 * A default file system implies a default scheme and authority; slash-relative
 * names (such as /for/bar) are resolved relative to that default FS.
 * Similarly a user can also have working-directory-relative names (i.e. names
 * not starting with a slash). While the working directory is generally in the
 * same default FS, the wd can be in a different FS.
 * <p>
 *  Hence Hadoop path names can be one of:
 *  <ul>
 *  <li> fully qualified URI: scheme://authority/path
 *  <li> slash relative names: /path relative to the default file system
 *  <li> wd-relative names: path  relative to the working dir
 *  </ul>   
 *  Relative paths with scheme (scheme:foo/bar) are illegal.
 *  
 *  <p>
 *  <b>****The Role of the FileContext and configuration defaults****</b>
 *  <p>
 *  The FileContext provides file namespace context for resolving file names;
 *  it also contains the umask for permissions, In that sense it is like the
 *  per-process file-related state in Unix system.
 *  These two properties
 *  <ul> 
 *  <li> default file system i.e your slash)
 *  <li> umask
 *  </ul>
 *  in general, are obtained from the default configuration file
 *  in your environment,  (@see {@link Configuration}).
 *  
 *  No other configuration parameters are obtained from the default config as 
 *  far as the file context layer is concerned. All file system instances
 *  (i.e. deployments of file systems) have default properties; we call these
 *  server side (SS) defaults. Operation like create allow one to select many 
 *  properties: either pass them in as explicit parameters or use
 *  the SS properties.
 *  <p>
 *  The file system related SS defaults are
 *  <ul>
 *  <li> the home directory (default is "/user/userName")
 *  <li> the initial wd (only for local fs)
 *  <li> replication factor
 *  <li> block size
 *  <li> buffer size
 *  <li> encryptDataTransfer 
 *  <li> checksum option. (checksumType and  bytesPerChecksum)
 *  </ul>
 *
 * <p>
 * <b> *** Usage Model for the FileContext class *** </b>
 * <p>
 * Example 1: use the default config read from the $HADOOP_CONFIG/core.xml.
 *   Unspecified values come from core-defaults.xml in the release jar.
 *  <ul>  
 *  <li> myFContext = FileContext.getFileContext(); // uses the default config
 *                                                // which has your default FS 
 *  <li>  myFContext.create(path, ...);
 *  <li>  myFContext.setWorkingDir(path)
 *  <li>  myFContext.open (path, ...);  
 *  </ul>  
 * Example 2: Get a FileContext with a specific URI as the default FS
 *  <ul>  
 *  <li> myFContext = FileContext.getFileContext(URI)
 *  <li> myFContext.create(path, ...);
 *   ...
 * </ul> 
 * Example 3: FileContext with local file system as the default
 *  <ul> 
 *  <li> myFContext = FileContext.getLocalFSFileContext()
 *  <li> myFContext.create(path, ...);
 *  <li> ...
 *  </ul> 
 * Example 4: Use a specific config, ignoring $HADOOP_CONFIG
 *  Generally you should not need use a config unless you are doing
 *   <ul> 
 *   <li> configX = someConfigSomeOnePassedToYou.
 *   <li> myFContext = getFileContext(configX); // configX is not changed,
 *                                              // is passed down 
 *   <li> myFContext.create(path, ...);
 *   <li>...
 *  </ul>                                          
 *    
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public final class FileContext {
  
  public static final Log LOG = LogFactory.getLog(FileContext.class);
  /**
   * Default permission for directory and symlink
   * In previous versions, this default permission was also used to
   * create files, so files created end up with ugo+x permission.
   * See HADOOP-9155 for detail. 
   * Two new constants are added to solve this, please use 
   * {@link FileContext#DIR_DEFAULT_PERM} for directory, and use
   * {@link FileContext#FILE_DEFAULT_PERM} for file.
   * This constant is kept for compatibility.
   */
  public static final FsPermission DEFAULT_PERM = FsPermission.getDefault();
  /**
   * Default permission for directory
   */
  public static final FsPermission DIR_DEFAULT_PERM = FsPermission.getDirDefault();
  /**
   * Default permission for file
   */
  public static final FsPermission FILE_DEFAULT_PERM = FsPermission.getFileDefault();

  /**
   * Priority of the FileContext shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 20;

  /**
   * List of files that should be deleted on JVM shutdown.
   */
  static final Map<FileContext, Set<Path>> DELETE_ON_EXIT = 
    new IdentityHashMap<FileContext, Set<Path>>();

  /** JVM shutdown hook thread. */
  static final FileContextFinalizer FINALIZER = 
    new FileContextFinalizer();
  
  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(final Path file) {
      return true;
    }
  };
  
  /**
   * The FileContext is defined by.
   *  1) defaultFS (slash)
   *  2) wd
   *  3) umask
   */   
  private final AbstractFileSystem defaultFS; //default FS for this FileContext.
  private Path workingDir;          // Fully qualified
  private FsPermission umask;
  private final Configuration conf;
  private final UserGroupInformation ugi;

  private FileContext(final AbstractFileSystem defFs,
    final FsPermission theUmask, final Configuration aConf) {
    defaultFS = defFs;
    umask = FsPermission.getUMask(aConf);
    conf = aConf;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      LOG.error("Exception in getCurrentUser: ",e);
      throw new RuntimeException("Failed to get the current user " +
      		"while creating a FileContext", e);
    }
    /*
     * Init the wd.
     * WorkingDir is implemented at the FileContext layer 
     * NOT at the AbstractFileSystem layer. 
     * If the DefaultFS, such as localFilesystem has a notion of
     *  builtin WD, we use that as the initial WD.
     *  Otherwise the WD is initialized to the home directory.
     */
    workingDir = defaultFS.getInitialWorkingDirectory();
    if (workingDir == null) {
      workingDir = defaultFS.getHomeDirectory();
    }
    util = new Util(); // for the inner class
  }
 
  /* 
   * Remove relative part - return "absolute":
   * If input is relative path ("foo/bar") add wd: ie "/<workingDir>/foo/bar"
   * A fully qualified uri ("hdfs://nn:p/foo/bar") or a slash-relative path
   * ("/foo/bar") are returned unchanged.
   * 
   * Applications that use FileContext should use #makeQualified() since
   * they really want a fully qualified URI.
   * Hence this method is not called makeAbsolute() and 
   * has been deliberately declared private.
   */
  private Path fixRelativePart(Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new Path(workingDir, p);
    }
  }

  /**
   * Delete all the paths that were marked as delete-on-exit.
   */
  static void processDeleteOnExit() {
    synchronized (DELETE_ON_EXIT) {
      Set<Entry<FileContext, Set<Path>>> set = DELETE_ON_EXIT.entrySet();
      for (Entry<FileContext, Set<Path>> entry : set) {
        FileContext fc = entry.getKey();
        Set<Path> paths = entry.getValue();
        for (Path path : paths) {
          try {
            fc.delete(path, true);
          } catch (IOException e) {
            LOG.warn("Ignoring failure to deleteOnExit for path " + path);
          }
        }
      }
      DELETE_ON_EXIT.clear();
    }
  }
  
  /**
   * Pathnames with scheme and relative path are illegal.
   * @param path to be checked
   */
  private static void checkNotSchemeWithRelative(final Path path) {
    if (path.toUri().isAbsolute() && !path.isUriPathAbsolute()) {
      throw new HadoopIllegalArgumentException(
          "Unsupported name: has scheme but relative path-part");
    }
  }

  /**
   * Get the file system of supplied path.
   * 
   * @param absOrFqPath - absolute or fully qualified path
   * @return the file system of the path
   * 
   * @throws UnsupportedFileSystemException If the file system for
   *           <code>absOrFqPath</code> is not supported.
   * @throws IOExcepton If the file system for <code>absOrFqPath</code> could
   *         not be instantiated.
   */
  private AbstractFileSystem getFSofPath(final Path absOrFqPath)
      throws UnsupportedFileSystemException, IOException {
    checkNotSchemeWithRelative(absOrFqPath);
    if (!absOrFqPath.isAbsolute() && absOrFqPath.toUri().getScheme() == null) {
      throw new HadoopIllegalArgumentException(
          "FileContext Bug: path is relative");
    }

    try { 
      // Is it the default FS for this FileContext?
      defaultFS.checkPath(absOrFqPath);
      return defaultFS;
    } catch (Exception e) { // it is different FileSystem
      return getAbstractFileSystem(ugi, absOrFqPath.toUri(), conf);
    }
  }
  
  private static AbstractFileSystem getAbstractFileSystem(
      UserGroupInformation user, final URI uri, final Configuration conf)
      throws UnsupportedFileSystemException, IOException {
    try {
      return user.doAs(new PrivilegedExceptionAction<AbstractFileSystem>() {
        @Override
        public AbstractFileSystem run() throws UnsupportedFileSystemException {
          return AbstractFileSystem.get(uri, conf);
        }
      });
    } catch (InterruptedException ex) {
      LOG.error(ex);
      throw new IOException("Failed to get the AbstractFileSystem for path: "
          + uri, ex);
    }
  }
  
  /**
   * Protected Static Factory methods for getting a FileContexts
   * that take a AbstractFileSystem as input. To be used for testing.
   */

  /**
   * Create a FileContext with specified FS as default using the specified
   * config.
   * 
   * @param defFS
   * @param aConf
   * @return new FileContext with specifed FS as default.
   */
  public static FileContext getFileContext(final AbstractFileSystem defFS,
                    final Configuration aConf) {
    return new FileContext(defFS, FsPermission.getUMask(aConf), aConf);
  }
  
  /**
   * Create a FileContext for specified file system using the default config.
   * 
   * @param defaultFS
   * @return a FileContext with the specified AbstractFileSystem
   *                 as the default FS.
   */
  protected static FileContext getFileContext(
    final AbstractFileSystem defaultFS) {
    return getFileContext(defaultFS, new Configuration());
  }
 
  /**
   * Static Factory methods for getting a FileContext.
   * Note new file contexts are created for each call.
   * The only singleton is the local FS context using the default config.
   * 
   * Methods that use the default config: the default config read from the
   * $HADOOP_CONFIG/core.xml,
   * Unspecified key-values for config are defaulted from core-defaults.xml
   * in the release jar.
   * 
   * The keys relevant to the FileContext layer are extracted at time of
   * construction. Changes to the config after the call are ignore
   * by the FileContext layer. 
   * The conf is passed to lower layers like AbstractFileSystem and HDFS which
   * pick up their own config variables.
   */

  /**
   * Create a FileContext using the default config read from the
   * $HADOOP_CONFIG/core.xml, Unspecified key-values for config are defaulted
   * from core-defaults.xml in the release jar.
   * 
   * @throws UnsupportedFileSystemException If the file system from the default
   *           configuration is not supported
   */
  public static FileContext getFileContext()
      throws UnsupportedFileSystemException {
    return getFileContext(new Configuration());
  }

  /**
   * @return a FileContext for the local file system using the default config.
   * @throws UnsupportedFileSystemException If the file system for
   *           {@link FsConstants#LOCAL_FS_URI} is not supported.
   */
  public static FileContext getLocalFSFileContext()
      throws UnsupportedFileSystemException {
    return getFileContext(FsConstants.LOCAL_FS_URI);
  }

  /**
   * Create a FileContext for specified URI using the default config.
   * 
   * @param defaultFsUri
   * @return a FileContext with the specified URI as the default FS.
   * 
   * @throws UnsupportedFileSystemException If the file system for
   *           <code>defaultFsUri</code> is not supported
   */
  public static FileContext getFileContext(final URI defaultFsUri)
      throws UnsupportedFileSystemException {
    return getFileContext(defaultFsUri, new Configuration());
  }

  /**
   * Create a FileContext for specified default URI using the specified config.
   * 
   * @param defaultFsUri
   * @param aConf
   * @return new FileContext for specified uri
   * @throws UnsupportedFileSystemException If the file system with specified is
   *           not supported
   * @throws RuntimeException If the file system specified is supported but
   *         could not be instantiated, or if login fails.
   */
  public static FileContext getFileContext(final URI defaultFsUri,
      final Configuration aConf) throws UnsupportedFileSystemException {
    UserGroupInformation currentUser = null;
    AbstractFileSystem defaultAfs = null;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
      defaultAfs = getAbstractFileSystem(currentUser, defaultFsUri, aConf);
    } catch (UnsupportedFileSystemException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }
    return getFileContext(defaultAfs, aConf);
  }

  /**
   * Create a FileContext using the passed config. Generally it is better to use
   * {@link #getFileContext(URI, Configuration)} instead of this one.
   * 
   * 
   * @param aConf
   * @return new FileContext
   * @throws UnsupportedFileSystemException If file system in the config
   *           is not supported
   */
  public static FileContext getFileContext(final Configuration aConf)
      throws UnsupportedFileSystemException {
    return getFileContext(
      URI.create(aConf.get(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT)), 
      aConf);
  }

  /**
   * @param aConf - from which the FileContext is configured
   * @return a FileContext for the local file system using the specified config.
   * 
   * @throws UnsupportedFileSystemException If default file system in the config
   *           is not supported
   * 
   */
  public static FileContext getLocalFSFileContext(final Configuration aConf)
      throws UnsupportedFileSystemException {
    return getFileContext(FsConstants.LOCAL_FS_URI, aConf);
  }

  /* This method is needed for tests. */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable /* return type will change to AFS once
                                  HADOOP-6223 is completed */
  public AbstractFileSystem getDefaultFileSystem() {
    return defaultFS;
  }
  
  /**
   * Set the working directory for wd-relative names (such a "foo/bar"). Working
   * directory feature is provided by simply prefixing relative names with the
   * working dir. Note this is different from Unix where the wd is actually set
   * to the inode. Hence setWorkingDir does not follow symlinks etc. This works
   * better in a distributed environment that has multiple independent roots.
   * {@link #getWorkingDirectory()} should return what setWorkingDir() set.
   * 
   * @param newWDir new working directory
   * @throws IOException 
   * <br>
   *           NewWdir can be one of:
   *           <ul>
   *           <li>relative path: "foo/bar";</li>
   *           <li>absolute without scheme: "/foo/bar"</li>
   *           <li>fully qualified with scheme: "xx://auth/foo/bar"</li>
   *           </ul>
   * <br>
   *           Illegal WDs:
   *           <ul>
   *           <li>relative with scheme: "xx:foo/bar"</li>
   *           <li>non existent directory</li>
   *           </ul>
   */
  public void setWorkingDirectory(final Path newWDir) throws IOException {
    checkNotSchemeWithRelative(newWDir);
    /* wd is stored as a fully qualified path. We check if the given 
     * path is not relative first since resolve requires and returns 
     * an absolute path.
     */  
    final Path newWorkingDir = new Path(workingDir, newWDir);
    FileStatus status = getFileStatus(newWorkingDir);
    if (status.isFile()) {
      throw new FileNotFoundException("Cannot setWD to a file");
    }
    workingDir = newWorkingDir;
  }
  
  /**
   * Gets the working directory for wd-relative names (such a "foo/bar").
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  /**
   * Gets the ugi in the file-context
   * @return UserGroupInformation
   */
  public UserGroupInformation getUgi() {
    return ugi;
  }
  
  /**
   * Return the current user's home directory in this file system.
   * The default implementation returns "/user/$USER/".
   * @return the home directory
   */
  public Path getHomeDirectory() {
    return defaultFS.getHomeDirectory();
  }
  
  /**
   * 
   * @return the umask of this FileContext
   */
  public FsPermission getUMask() {
    return umask;
  }
  
  /**
   * Set umask to the supplied parameter.
   * @param newUmask  the new umask
   */
  public void setUMask(final FsPermission newUmask) {
    umask = newUmask;
  }
  
  
  /**
   * Resolve the path following any symlinks or mount points
   * @param f to be resolved
   * @return fully qualified resolved path
   * 
   * @throws FileNotFoundException  If <code>f</code> does not exist
   * @throws AccessControlException if access denied
   * @throws IOException If an IO Error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   * 
   * RuntimeExceptions:
   * @throws InvalidPathException If path <code>f</code> is not valid
   */
  public Path resolvePath(final Path f) throws FileNotFoundException,
      UnresolvedLinkException, AccessControlException, IOException {
    return resolve(f);
  }
  
  /**
   * Make the path fully qualified if it is isn't. 
   * A Fully-qualified path has scheme and authority specified and an absolute
   * path.
   * Use the default file system and working dir in this FileContext to qualify.
   * @param path
   * @return qualified path
   */
  public Path makeQualified(final Path path) {
    return path.makeQualified(defaultFS.getUri(), getWorkingDirectory());
  }

  /**
   * Create or overwrite file on indicated path and returns an output stream for
   * writing into the file.
   * 
   * @param f the file name to open
   * @param createFlag gives the semantics of create; see {@link CreateFlag}
   * @param opts file creation options; see {@link Options.CreateOpts}.
   *          <ul>
   *          <li>Progress - to report progress on the operation - default null
   *          <li>Permission - umask is applied against permisssion: default is
   *          FsPermissions:getDefault()
   * 
   *          <li>CreateParent - create missing parent path; default is to not
   *          to create parents
   *          <li>The defaults for the following are SS defaults of the file
   *          server implementing the target path. Not all parameters make sense
   *          for all kinds of file system - eg. localFS ignores Blocksize,
   *          replication, checksum
   *          <ul>
   *          <li>BufferSize - buffersize used in FSDataOutputStream
   *          <li>Blocksize - block size for file blocks
   *          <li>ReplicationFactor - replication for blocks
   *          <li>ChecksumParam - Checksum parameters. server default is used
   *          if not specified.
   *          </ul>
   *          </ul>
   * 
   * @return {@link FSDataOutputStream} for created file
   * 
   * @throws AccessControlException If access is denied
   * @throws FileAlreadyExistsException If file <code>f</code> already exists
   * @throws FileNotFoundException If parent of <code>f</code> does not exist
   *           and <code>createParent</code> is false
   * @throws ParentNotDirectoryException If parent of <code>f</code> is not a
   *           directory.
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   * 
   * RuntimeExceptions:
   * @throws InvalidPathException If path <code>f</code> is not valid
   */
  public FSDataOutputStream create(final Path f,
      final EnumSet<CreateFlag> createFlag, Options.CreateOpts... opts)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    Path absF = fixRelativePart(f);

    // If one of the options is a permission, extract it & apply umask
    // If not, add a default Perms and apply umask;
    // AbstractFileSystem#create

    CreateOpts.Perms permOpt = 
      (CreateOpts.Perms) CreateOpts.getOpt(CreateOpts.Perms.class, opts);
    FsPermission permission = (permOpt != null) ? permOpt.getValue() :
                                      FILE_DEFAULT_PERM;
    permission = permission.applyUMask(umask);

    final CreateOpts[] updatedOpts = 
                      CreateOpts.setOpt(CreateOpts.perms(permission), opts);
    return new FSLinkResolver<FSDataOutputStream>() {
      @Override
      public FSDataOutputStream next(final AbstractFileSystem fs, final Path p) 
        throws IOException {
        return fs.create(p, createFlag, updatedOpts);
      }
    }.resolve(this, absF);
  }

  /**
   * Make(create) a directory and all the non-existent parents.
   * 
   * @param dir - the dir to make
   * @param permission - permissions is set permission&~umask
   * @param createParent - if true then missing parent dirs are created if false
   *          then parent must exist
   * 
   * @throws AccessControlException If access is denied
   * @throws FileAlreadyExistsException If directory <code>dir</code> already
   *           exists
   * @throws FileNotFoundException If parent of <code>dir</code> does not exist
   *           and <code>createParent</code> is false
   * @throws ParentNotDirectoryException If parent of <code>dir</code> is not a
   *           directory
   * @throws UnsupportedFileSystemException If file system for <code>dir</code>
   *         is not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   * 
   * RuntimeExceptions:
   * @throws InvalidPathException If path <code>dir</code> is not valid
   */
  public void mkdir(final Path dir, final FsPermission permission,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException {
    final Path absDir = fixRelativePart(dir);
    final FsPermission absFerms = (permission == null ? 
          FsPermission.getDirDefault() : permission).applyUMask(umask);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        fs.mkdir(p, absFerms, createParent);
        return null;
      }
    }.resolve(this, absDir);
  }

  /**
   * Delete a file.
   * @param f the path to delete.
   * @param recursive if path is a directory and set to 
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   * 
   * RuntimeExceptions:
   * @throws InvalidPathException If path <code>f</code> is invalid
   */
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    Path absF = fixRelativePart(f);
    return new FSLinkResolver<Boolean>() {
      @Override
      public Boolean next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return Boolean.valueOf(fs.delete(p, recursive));
      }
    }.resolve(this, absF);
  }
 
  /**
   * Opens an FSDataInputStream at the indicated Path using
   * default buffersize.
   * @param f the file name to open
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If file <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code>
   *         is not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public FSDataInputStream open(final Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<FSDataInputStream>() {
      @Override
      public FSDataInputStream next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.open(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * 
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   * 
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If file <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public FSDataInputStream open(final Path f, final int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<FSDataInputStream>() {
      @Override
      public FSDataInputStream next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.open(p, bufferSize);
      }
    }.resolve(this, absF);
  }

  /**
   * Set replication for an existing file.
   * 
   * @param f file name
   * @param replication new replication
   *
   * @return true if successful
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If file <code>f</code> does not exist
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException,
      IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<Boolean>() {
      @Override
      public Boolean next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return Boolean.valueOf(fs.setReplication(p, replication));
      }
    }.resolve(this, absF);
  }

  /**
   * Renames Path src to Path dst
   * <ul>
   * <li
   * <li>Fails if src is a file and dst is a directory.
   * <li>Fails if src is a directory and dst is a file.
   * <li>Fails if the parent of dst does not exist or is a file.
   * </ul>
   * <p>
   * If OVERWRITE option is not passed as an argument, rename fails if the dst
   * already exists.
   * <p>
   * If OVERWRITE option is passed as an argument, rename overwrites the dst if
   * it is a file or an empty directory. Rename fails if dst is a non-empty
   * directory.
   * <p>
   * Note that atomicity of rename is dependent on the file system
   * implementation. Please refer to the file system documentation for details
   * <p>
   * 
   * @param src path to be renamed
   * @param dst new path after rename
   * 
   * @throws AccessControlException If access is denied
   * @throws FileAlreadyExistsException If <code>dst</code> already exists and
   *           <code>options</options> has {@link Options.Rename#OVERWRITE} 
   *           option false.
   * @throws FileNotFoundException If <code>src</code> does not exist
   * @throws ParentNotDirectoryException If parent of <code>dst</code> is not a
   *           directory
   * @throws UnsupportedFileSystemException If file system for <code>src</code>
   *           and <code>dst</code> is not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   */
  public void rename(final Path src, final Path dst,
      final Options.Rename... options) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException,
      IOException {
    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);
    AbstractFileSystem srcFS = getFSofPath(absSrc);
    AbstractFileSystem dstFS = getFSofPath(absDst);
    if(!srcFS.getUri().equals(dstFS.getUri())) {
      throw new IOException("Renames across AbstractFileSystems not supported");
    }
    try {
      srcFS.rename(absSrc, absDst, options);
    } catch (UnresolvedLinkException e) {
      /* We do not know whether the source or the destination path
       * was unresolved. Resolve the source path up until the final
       * path component, then fully resolve the destination. 
       */
      final Path source = resolveIntermediate(absSrc);    
      new FSLinkResolver<Void>() {
        @Override
        public Void next(final AbstractFileSystem fs, final Path p) 
          throws IOException, UnresolvedLinkException {
          fs.rename(source, p, options);
          return null;
        }
      }.resolve(this, absDst);
    }
  }
  
  /**
   * Set permission of a path.
   * @param f
   * @param permission - the new absolute permission (umask is not applied)
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code>
   *         is not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        fs.setPermission(p, permission);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Set owner of a path (i.e. a file or a directory). The parameters username
   * and groupname cannot both be null.
   * 
   * @param f The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   * 
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   * 
   * RuntimeExceptions:
   * @throws HadoopIllegalArgumentException If <code>username</code> or
   *           <code>groupname</code> is invalid.
   */
  public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      UnsupportedFileSystemException, FileNotFoundException,
      IOException {
    if ((username == null) && (groupname == null)) {
      throw new HadoopIllegalArgumentException(
          "username and groupname cannot both be null");
    }
    final Path absF = fixRelativePart(f);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        fs.setOwner(p, username, groupname);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Set access time of a file.
   * @param f The path
   * @param mtime Set the modification time of this file.
   *        The number of milliseconds since epoch (Jan 1, 1970). 
   *        A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   *        The number of milliseconds since Jan 1, 1970. 
   *        A value of -1 means that this call should not set access time.
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public void setTimes(final Path f, final long mtime, final long atime)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        fs.setTimes(p, mtime, atime);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Get the checksum of a file.
   *
   * @param f file path
   *
   * @return The file checksum.  The default return value is null,
   *  which indicates that no checksum algorithm is implemented
   *  in the corresponding FileSystem.
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<FileChecksum>() {
      @Override
      public FileChecksum next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.getFileChecksum(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Set the verify checksum flag for the  file system denoted by the path.
   * This is only applicable if the 
   * corresponding FileSystem supports checksum. By default doesn't do anything.
   * @param verifyChecksum
   * @param f set the verifyChecksum for the Filesystem containing this path
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public void setVerifyChecksum(final boolean verifyChecksum, final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = resolve(fixRelativePart(f));
    getFSofPath(absF).setVerifyChecksum(verifyChecksum);
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   *
   * @return a FileStatus object
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public FileStatus getFileStatus(final Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<FileStatus>() {
      @Override
      public FileStatus next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.getFileStatus(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Return a fully qualified version of the given symlink target if it
   * has no scheme and authority. Partially and fully qualified paths 
   * are returned unmodified.
   * @param pathFS The AbstractFileSystem of the path
   * @param pathWithLink Path that contains the symlink
   * @param target The symlink's absolute target
   * @return Fully qualified version of the target.
   */
  private Path qualifySymlinkTarget(final AbstractFileSystem pathFS,
    Path pathWithLink, Path target) {
    // NB: makeQualified uses the target's scheme and authority, if
    // specified, and the scheme and authority of pathFS, if not.
    final String scheme = target.toUri().getScheme();
    final String auth   = target.toUri().getAuthority();
    return (scheme == null && auth == null)
      ? target.makeQualified(pathFS.getUri(), pathWithLink.getParent())
      : target;
  }
  
  /**
   * Return a file status object that represents the path. If the path 
   * refers to a symlink then the FileStatus of the symlink is returned.
   * The behavior is equivalent to #getFileStatus() if the underlying
   * file system does not support symbolic links.
   * @param  f The path we want information from.
   * @return A FileStatus object
   * 
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   */
  public FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<FileStatus>() {
      @Override
      public FileStatus next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        FileStatus fi = fs.getFileLinkStatus(p);
        if (fi.isSymlink()) {
          fi.setSymlink(qualifySymlinkTarget(fs, p, fi.getSymlink()));
        }
        return fi;
      }
    }.resolve(this, absF);
  }
  
  /**
   * Returns the target of the given symbolic link as it was specified
   * when the link was created.  Links in the path leading up to the
   * final path component are resolved transparently.
   *
   * @param f the path to return the target of
   * @return The un-interpreted target of the symbolic link.
   * 
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If path <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If the given path does not refer to a symlink
   *           or an I/O error occurred
   */
  public Path getLinkTarget(final Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<Path>() {
      @Override
      public Path next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        FileStatus fi = fs.getFileLinkStatus(p);
        return fi.getSymlink();
      }
    }.resolve(this, absF);
  }
  
  /**
   * Return blockLocation of the given file for the given offset and len.
   *  For a nonexistent file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns 
   * hostnames of machines that contain the given file.
   * 
   * @param f - get blocklocations of this file
   * @param start position (byte offset)
   * @param len (in bytes)
   *
   * @return block locations for given file at specified offset of len
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   * 
   * RuntimeExceptions:
   * @throws InvalidPathException If path <code>f</code> is invalid
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public BlockLocation[] getFileBlockLocations(final Path f, final long start,
      final long len) throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<BlockLocation[]>() {
      @Override
      public BlockLocation[] next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.getFileBlockLocations(p, start, len);
      }
    }.resolve(this, absF);
  }
  
  /**
   * Returns a status object describing the use and capacity of the
   * file system denoted by the Parh argument p.
   * If the file system has multiple partitions, the
   * use and capacity of the partition pointed to by the specified
   * path is reflected.
   * 
   * @param f Path for which status should be obtained. null means the
   * root partition of the default file system. 
   *
   * @return a FsStatus object
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public FsStatus getFsStatus(final Path f) throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    if (f == null) {
      return defaultFS.getFsStatus();
    }
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<FsStatus>() {
      @Override
      public FsStatus next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.getFsStatus(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Creates a symbolic link to an existing file. An exception is thrown if 
   * the symlink exits, the user does not have permission to create symlink,
   * or the underlying file system does not support symlinks.
   * 
   * Symlink permissions are ignored, access to a symlink is determined by
   * the permissions of the symlink target.
   * 
   * Symlinks in paths leading up to the final path component are resolved 
   * transparently. If the final path component refers to a symlink some 
   * functions operate on the symlink itself, these are:
   * - delete(f) and deleteOnExit(f) - Deletes the symlink.
   * - rename(src, dst) - If src refers to a symlink, the symlink is 
   *   renamed. If dst refers to a symlink, the symlink is over-written.
   * - getLinkTarget(f) - Returns the target of the symlink. 
   * - getFileLinkStatus(f) - Returns a FileStatus object describing
   *   the symlink.
   * Some functions, create() and mkdir(), expect the final path component
   * does not exist. If they are given a path that refers to a symlink that 
   * does exist they behave as if the path referred to an existing file or 
   * directory. All other functions fully resolve, ie follow, the symlink. 
   * These are: open, setReplication, setOwner, setTimes, setWorkingDirectory,
   * setPermission, getFileChecksum, setVerifyChecksum, getFileBlockLocations,
   * getFsStatus, getFileStatus, exists, and listStatus.
   * 
   * Symlink targets are stored as given to createSymlink, assuming the 
   * underlying file system is capable of storing a fully qualified URI.
   * Dangling symlinks are permitted. FileContext supports four types of 
   * symlink targets, and resolves them as follows
   * <pre>
   * Given a path referring to a symlink of form:
   * 
   *   <---X---> 
   *   fs://host/A/B/link 
   *   <-----Y----->
   * 
   * In this path X is the scheme and authority that identify the file system,
   * and Y is the path leading up to the final path component "link". If Y is
   * a symlink  itself then let Y' be the target of Y and X' be the scheme and
   * authority of Y'. Symlink targets may:
   * 
   * 1. Fully qualified URIs
   * 
   * fs://hostX/A/B/file  Resolved according to the target file system.
   * 
   * 2. Partially qualified URIs (eg scheme but no host)
   * 
   * fs:///A/B/file  Resolved according to the target file sytem. Eg resolving
   *                 a symlink to hdfs:///A results in an exception because
   *                 HDFS URIs must be fully qualified, while a symlink to 
   *                 file:///A will not since Hadoop's local file systems 
   *                 require partially qualified URIs.
   * 
   * 3. Relative paths
   * 
   * path  Resolves to [Y'][path]. Eg if Y resolves to hdfs://host/A and path 
   *       is "../B/file" then [Y'][path] is hdfs://host/B/file
   * 
   * 4. Absolute paths
   * 
   * path  Resolves to [X'][path]. Eg if Y resolves hdfs://host/A/B and path
   *       is "/file" then [X][path] is hdfs://host/file
   * </pre>
   * 
   * @param target the target of the symbolic link
   * @param link the path to be created that points to target
   * @param createParent if true then missing parent dirs are created if 
   *                     false then parent must exist
   *
   *
   * @throws AccessControlException If access is denied
   * @throws FileAlreadyExistsException If file <code>linkcode> already exists
   * @throws FileNotFoundException If <code>target</code> does not exist
   * @throws ParentNotDirectoryException If parent of <code>link</code> is not a
   *           directory.
   * @throws UnsupportedFileSystemException If file system for 
   *           <code>target</code> or <code>link</code> is not supported
   * @throws IOException If an I/O error occurred
   */
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException { 
    final Path nonRelLink = fixRelativePart(link);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        fs.createSymlink(target, p, createParent);
        return null;
      }
    }.resolve(this, nonRelLink);
  }
  
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f is the path
   *
   * @return an iterator that traverses statuses of the files/directories 
   *         in the given path
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public RemoteIterator<FileStatus> listStatus(final Path f) throws
      AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<RemoteIterator<FileStatus>>() {
      @Override
      public RemoteIterator<FileStatus> next(
          final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.listStatusIterator(p);
      }
    }.resolve(this, absF);
  }

  /**
   * @return an iterator over the corrupt files under the given path
   * (may contain duplicates if a file has more than one corrupt block)
   * @throws IOException
   */
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<RemoteIterator<Path>>() {
      @Override
      public RemoteIterator<Path> next(final AbstractFileSystem fs,
                                       final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.listCorruptFileBlocks(p);
      }
    }.resolve(this, absF);
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
   * If any IO exception (for example the input directory gets deleted while
   * listing is being executed), next() or hasNext() of the returned iterator
   * may throw a RuntimeException with the io exception as the cause.
   *
   * @throws AccessControlException If access is denied
   * @throws FileNotFoundException If <code>f</code> does not exist
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(
      final Path f) throws
      AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<RemoteIterator<LocatedFileStatus>>() {
      @Override
      public RemoteIterator<LocatedFileStatus> next(
          final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.listLocatedStatus(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Mark a path to be deleted on JVM shutdown.
   * 
   * @param f the existing path to delete.
   *
   * @return  true if deleteOnExit is successful, otherwise false.
   *
   * @throws AccessControlException If access is denied
   * @throws UnsupportedFileSystemException If file system for <code>f</code> is
   *           not supported
   * @throws IOException If an I/O error occurred
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  public boolean deleteOnExit(Path f) throws AccessControlException,
      IOException {
    if (!this.util().exists(f)) {
      return false;
    }
    synchronized (DELETE_ON_EXIT) {
      if (DELETE_ON_EXIT.isEmpty()) {
        ShutdownHookManager.get().addShutdownHook(FINALIZER, SHUTDOWN_HOOK_PRIORITY);
      }
      
      Set<Path> set = DELETE_ON_EXIT.get(this);
      if (set == null) {
        set = new TreeSet<Path>();
        DELETE_ON_EXIT.put(this, set);
      }
      set.add(f);
    }
    return true;
  }
  
  private final Util util;
  public Util util() {
    return util;
  }
  
  
  /**
   * Utility/library methods built over the basic FileContext methods.
   * Since this are library functions, the oprtation are not atomic
   * and some of them may partially complete if other threads are making
   * changes to the same part of the name space.
   */
  public class Util {
    /**
     * Does the file exist?
     * Note: Avoid using this method if you already have FileStatus in hand.
     * Instead reuse the FileStatus 
     * @param f the  file or dir to be checked
     *
     * @throws AccessControlException If access is denied
     * @throws IOException If an I/O error occurred
     * @throws UnsupportedFileSystemException If file system for <code>f</code> is
     *           not supported
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public boolean exists(final Path f) throws AccessControlException,
      UnsupportedFileSystemException, IOException {
      try {
        FileStatus fs = FileContext.this.getFileStatus(f);
        assert fs != null;
        return true;
      } catch (FileNotFoundException e) {
        return false;
      }
    }
    
    /**
     * Return a list of file status objects that corresponds to supplied paths
     * excluding those non-existent paths.
     * 
     * @param paths list of paths we want information from
     *
     * @return a list of FileStatus objects
     *
     * @throws AccessControlException If access is denied
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    private FileStatus[] getFileStatus(Path[] paths)
        throws AccessControlException, IOException {
      if (paths == null) {
        return null;
      }
      ArrayList<FileStatus> results = new ArrayList<FileStatus>(paths.length);
      for (int i = 0; i < paths.length; i++) {
        try {
          results.add(FileContext.this.getFileStatus(paths[i]));
        } catch (FileNotFoundException fnfe) {
          // ignoring 
        }
      }
      return results.toArray(new FileStatus[results.size()]);
    }
    
    
    /**
     * Return the {@link ContentSummary} of path f.
     * @param f path
     *
     * @return the {@link ContentSummary} of path f.
     *
     * @throws AccessControlException If access is denied
     * @throws FileNotFoundException If <code>f</code> does not exist
     * @throws UnsupportedFileSystemException If file system for 
     *         <code>f</code> is not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public ContentSummary getContentSummary(Path f)
        throws AccessControlException, FileNotFoundException,
        UnsupportedFileSystemException, IOException {
      FileStatus status = FileContext.this.getFileStatus(f);
      if (status.isFile()) {
        return new ContentSummary(status.getLen(), 1, 0);
      }
      long[] summary = {0, 0, 1};
      RemoteIterator<FileStatus> statusIterator = 
        FileContext.this.listStatus(f);
      while(statusIterator.hasNext()) {
        FileStatus s = statusIterator.next();
        ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath()) :
                                       new ContentSummary(s.getLen(), 1, 0);
        summary[0] += c.getLength();
        summary[1] += c.getFileCount();
        summary[2] += c.getDirectoryCount();
      }
      return new ContentSummary(summary[0], summary[1], summary[2]);
    }
    
    /**
     * See {@link #listStatus(Path[], PathFilter)}
     */
    public FileStatus[] listStatus(Path[] files) throws AccessControlException,
        FileNotFoundException, IOException {
      return listStatus(files, DEFAULT_FILTER);
    }
     
    /**
     * Filter files/directories in the given path using the user-supplied path
     * filter.
     * 
     * @param f is the path name
     * @param filter is the user-supplied path filter
     *
     * @return an array of FileStatus objects for the files under the given path
     *         after applying the filter
     *
     * @throws AccessControlException If access is denied
     * @throws FileNotFoundException If <code>f</code> does not exist
     * @throws UnsupportedFileSystemException If file system for 
     *         <code>pathPattern</code> is not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public FileStatus[] listStatus(Path f, PathFilter filter)
        throws AccessControlException, FileNotFoundException,
        UnsupportedFileSystemException, IOException {
      ArrayList<FileStatus> results = new ArrayList<FileStatus>();
      listStatus(results, f, filter);
      return results.toArray(new FileStatus[results.size()]);
    }
    
    /**
     * Filter files/directories in the given list of paths using user-supplied
     * path filter.
     * 
     * @param files is a list of paths
     * @param filter is the filter
     *
     * @return a list of statuses for the files under the given paths after
     *         applying the filter
     *
     * @throws AccessControlException If access is denied
     * @throws FileNotFoundException If a file in <code>files</code> does not 
     *           exist
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public FileStatus[] listStatus(Path[] files, PathFilter filter)
        throws AccessControlException, FileNotFoundException, IOException {
      ArrayList<FileStatus> results = new ArrayList<FileStatus>();
      for (int i = 0; i < files.length; i++) {
        listStatus(results, files[i], filter);
      }
      return results.toArray(new FileStatus[results.size()]);
    }
  
    /*
     * Filter files/directories in the given path using the user-supplied path
     * filter. Results are added to the given array <code>results</code>.
     */
    private void listStatus(ArrayList<FileStatus> results, Path f,
        PathFilter filter) throws AccessControlException,
        FileNotFoundException, IOException {
      FileStatus[] listing = listStatus(f);
      if (listing != null) {
        for (int i = 0; i < listing.length; i++) {
          if (filter.accept(listing[i].getPath())) {
            results.add(listing[i]);
          }
        }
      }
    }

    /**
     * List the statuses of the files/directories in the given path 
     * if the path is a directory.
     * 
     * @param f is the path
     *
     * @return an array that contains statuses of the files/directories 
     *         in the given path
     *
     * @throws AccessControlException If access is denied
     * @throws FileNotFoundException If <code>f</code> does not exist
     * @throws UnsupportedFileSystemException If file system for <code>f</code> is
     *           not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public FileStatus[] listStatus(final Path f) throws AccessControlException,
        FileNotFoundException, UnsupportedFileSystemException,
        IOException {
      final Path absF = fixRelativePart(f);
      return new FSLinkResolver<FileStatus[]>() {
        @Override
        public FileStatus[] next(final AbstractFileSystem fs, final Path p) 
          throws IOException, UnresolvedLinkException {
          return fs.listStatus(p);
        }
      }.resolve(FileContext.this, absF);
    }

    /**
     * List the statuses and block locations of the files in the given path.
     * 
     * If the path is a directory, 
     *   if recursive is false, returns files in the directory;
     *   if recursive is true, return files in the subtree rooted at the path.
     *   The subtree is traversed in the depth-first order.
     * If the path is a file, return the file's status and block locations.
     * Files across symbolic links are also returned.
     * 
     * @param f is the path
     * @param recursive if the subdirectories need to be traversed recursively
     *
     * @return an iterator that traverses statuses of the files
     * If any IO exception (for example a sub-directory gets deleted while
     * listing is being executed), next() or hasNext() of the returned iterator
     * may throw a RuntimeException with the IO exception as the cause.
     *
     * @throws AccessControlException If access is denied
     * @throws FileNotFoundException If <code>f</code> does not exist
     * @throws UnsupportedFileSystemException If file system for <code>f</code>
     *         is not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public RemoteIterator<LocatedFileStatus> listFiles(
        final Path f, final boolean recursive) throws AccessControlException,
        FileNotFoundException, UnsupportedFileSystemException, 
        IOException {
      return new RemoteIterator<LocatedFileStatus>() {
        private Stack<RemoteIterator<LocatedFileStatus>> itors = 
          new Stack<RemoteIterator<LocatedFileStatus>>();
        RemoteIterator<LocatedFileStatus> curItor = listLocatedStatus(f);
        LocatedFileStatus curFile;

        /**
         * Returns <tt>true</tt> if the iterator has more files.
         *
         * @return <tt>true</tt> if the iterator has more files.
         * @throws AccessControlException if not allowed to access next
         *                                file's status or locations
         * @throws FileNotFoundException if next file does not exist any more
         * @throws UnsupportedFileSystemException if next file's 
         *                                        fs is unsupported
         * @throws IOException for all other IO errors
         *                     for example, NameNode is not avaialbe or
         *                     NameNode throws IOException due to an error
         *                     while getting the status or block locations
         */
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
         * If it is a symlink, resolve the symlink first and then process it
         * depending on if it is a file or directory.
         * @param stat input status
         * @throws AccessControlException if access is denied
         * @throws FileNotFoundException if file is not found
         * @throws UnsupportedFileSystemException if fs is not supported
         * @throws IOException for all other IO errors
         */
        private void handleFileStat(LocatedFileStatus stat)
        throws IOException {
          if (stat.isFile()) { // file
            curFile = stat;
          } else if (stat.isSymlink()) { // symbolic link
            // resolve symbolic link
            FileStatus symstat = FileContext.this.getFileStatus(
                stat.getSymlink());
            if (symstat.isFile() || (recursive && symstat.isDirectory())) {
              itors.push(curItor);
              curItor = listLocatedStatus(stat.getPath());
            }
          } else if (recursive) { // directory
            itors.push(curItor);
            curItor = listLocatedStatus(stat.getPath());
          }
        }

        /**
         * Returns the next file's status with its block locations
         *
         * @throws AccessControlException if not allowed to access next
         *                                file's status or locations
         * @throws FileNotFoundException if next file does not exist any more
         * @throws UnsupportedFileSystemException if next file's 
         *                                        fs is unsupported
         * @throws IOException for all other IO errors
         *                     for example, NameNode is not avaialbe or
         *                     NameNode throws IOException due to an error
         *                     while getting the status or block locations
         */
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
     *     <tt>{<i>a...b</i>}</tt>. Note: character <tt><i>a</i></tt> must be
     *     lexicographically less than or equal to character <tt><i>b</i></tt>.
     *
     *    <p>
     *    <dt> <tt> [^<i>a</i>] </tt>
     *    <dd> Matches a single char that is not from character set or range
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
     *    <dd> Matches a string from string set <tt>{<i>ab, cde, cfh</i>}</tt>
     *
     *   </dl>
     *  </dd>
     * </dl>
     *
     * @param pathPattern a regular expression specifying a pth pattern
     *
     * @return an array of paths that match the path pattern
     *
     * @throws AccessControlException If access is denied
     * @throws UnsupportedFileSystemException If file system for 
     *         <code>pathPattern</code> is not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public FileStatus[] globStatus(Path pathPattern)
        throws AccessControlException, UnsupportedFileSystemException,
        IOException {
      return globStatus(pathPattern, DEFAULT_FILTER);
    }
    
    /**
     * Return an array of FileStatus objects whose path names match pathPattern
     * and is accepted by the user-supplied path filter. Results are sorted by
     * their path names.
     * Return null if pathPattern has no glob and the path does not exist.
     * Return an empty array if pathPattern has a glob and no path matches it. 
     * 
     * @param pathPattern regular expression specifying the path pattern
     * @param filter user-supplied path filter
     *
     * @return an array of FileStatus objects
     *
     * @throws AccessControlException If access is denied
     * @throws UnsupportedFileSystemException If file system for 
     *         <code>pathPattern</code> is not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     */
    public FileStatus[] globStatus(final Path pathPattern,
        final PathFilter filter) throws AccessControlException,
        UnsupportedFileSystemException, IOException {
      URI uri = getFSofPath(fixRelativePart(pathPattern)).getUri();

      String filename = pathPattern.toUri().getPath();

      List<String> filePatterns = GlobExpander.expand(filename);
      if (filePatterns.size() == 1) {
        Path absPathPattern = fixRelativePart(pathPattern);
        return globStatusInternal(uri, new Path(absPathPattern.toUri()
            .getPath()), filter);
      } else {
        List<FileStatus> results = new ArrayList<FileStatus>();
        for (String iFilePattern : filePatterns) {
          Path iAbsFilePattern = fixRelativePart(new Path(iFilePattern));
          FileStatus[] files = globStatusInternal(uri, iAbsFilePattern, filter);
          for (FileStatus file : files) {
            results.add(file);
          }
        }
        return results.toArray(new FileStatus[results.size()]);
      }
    }

    /**
     * 
     * @param uri for all the inPathPattern
     * @param inPathPattern - without the scheme & authority (take from uri)
     * @param filter
     *
     * @return an array of FileStatus objects
     *
     * @throws AccessControlException If access is denied
     * @throws IOException If an I/O error occurred
     */
    private FileStatus[] globStatusInternal(final URI uri,
        final Path inPathPattern, final PathFilter filter)
        throws AccessControlException, IOException
      {
      Path[] parents = new Path[1];
      int level = 0;
      
      assert(inPathPattern.toUri().getScheme() == null &&
          inPathPattern.toUri().getAuthority() == null && 
          inPathPattern.isUriPathAbsolute());

      
      String filename = inPathPattern.toUri().getPath();
      
      // path has only zero component
      if ("".equals(filename) || Path.SEPARATOR.equals(filename)) {
        Path p = inPathPattern.makeQualified(uri, null);
        return getFileStatus(new Path[]{p});
      }

      // path has at least one component
      String[] components = filename.split(Path.SEPARATOR);
      
      // Path is absolute, first component is "/" hence first component
      // is the uri root
      parents[0] = new Path(new Path(uri), new Path("/"));
      level = 1;

      // glob the paths that match the parent path, ie. [0, components.length-1]
      boolean[] hasGlob = new boolean[]{false};
      Path[] relParentPaths = 
        globPathsLevel(parents, components, level, hasGlob);
      FileStatus[] results;
      
      if (relParentPaths == null || relParentPaths.length == 0) {
        results = null;
      } else {
        // fix the pathes to be abs
        Path[] parentPaths = new Path [relParentPaths.length]; 
        for(int i=0; i<relParentPaths.length; i++) {
          parentPaths[i] = relParentPaths[i].makeQualified(uri, null);
        }
        
        // Now work on the last component of the path
        GlobFilter fp = 
                    new GlobFilter(components[components.length - 1], filter);
        if (fp.hasPattern()) { // last component has a pattern
          // list parent directories and then glob the results
          try {
            results = listStatus(parentPaths, fp);
          } catch (FileNotFoundException e) {
            results = null;
          }
          hasGlob[0] = true;
        } else { // last component does not have a pattern
          // get all the path names
          ArrayList<Path> filteredPaths = 
                                      new ArrayList<Path>(parentPaths.length);
          for (int i = 0; i < parentPaths.length; i++) {
            parentPaths[i] = new Path(parentPaths[i],
              components[components.length - 1]);
            if (fp.accept(parentPaths[i])) {
              filteredPaths.add(parentPaths[i]);
            }
          }
          // get all their statuses
          results = getFileStatus(
              filteredPaths.toArray(new Path[filteredPaths.size()]));
        }
      }

      // Decide if the pathPattern contains a glob or not
      if (results == null) {
        if (hasGlob[0]) {
          results = new FileStatus[0];
        }
      } else {
        if (results.length == 0) {
          if (!hasGlob[0]) {
            results = null;
          }
        } else {
          Arrays.sort(results);
        }
      }
      return results;
    }

    /*
     * For a path of N components, return a list of paths that match the
     * components [<code>level</code>, <code>N-1</code>].
     */
    private Path[] globPathsLevel(Path[] parents, String[] filePattern,
        int level, boolean[] hasGlob) throws AccessControlException,
        FileNotFoundException, IOException {
      if (level == filePattern.length - 1) {
        return parents;
      }
      if (parents == null || parents.length == 0) {
        return null;
      }
      GlobFilter fp = new GlobFilter(filePattern[level]);
      if (fp.hasPattern()) {
        try {
          parents = FileUtil.stat2Paths(listStatus(parents, fp));
        } catch (FileNotFoundException e) {
          parents = null;
        }
        hasGlob[0] = true;
      } else {
        for (int i = 0; i < parents.length; i++) {
          parents[i] = new Path(parents[i], filePattern[level]);
        }
      }
      return globPathsLevel(parents, filePattern, level + 1, hasGlob);
    }

    /**
     * Copy file from src to dest. See
     * {@link #copy(Path, Path, boolean, boolean)}
     */
    public boolean copy(final Path src, final Path dst)
        throws AccessControlException, FileAlreadyExistsException,
        FileNotFoundException, ParentNotDirectoryException,
        UnsupportedFileSystemException, IOException {
      return copy(src, dst, false, false);
    }
    
    /**
     * Copy from src to dst, optionally deleting src and overwriting dst.
     * @param src
     * @param dst
     * @param deleteSource - delete src if true
     * @param overwrite  overwrite dst if true; throw IOException if dst exists
     *         and overwrite is false.
     *
     * @return true if copy is successful
     *
     * @throws AccessControlException If access is denied
     * @throws FileAlreadyExistsException If <code>dst</code> already exists
     * @throws FileNotFoundException If <code>src</code> does not exist
     * @throws ParentNotDirectoryException If parent of <code>dst</code> is not
     *           a directory
     * @throws UnsupportedFileSystemException If file system for 
     *         <code>src</code> or <code>dst</code> is not supported
     * @throws IOException If an I/O error occurred
     * 
     * Exceptions applicable to file systems accessed over RPC:
     * @throws RpcClientException If an exception occurred in the RPC client
     * @throws RpcServerException If an exception occurred in the RPC server
     * @throws UnexpectedServerException If server implementation throws 
     *           undeclared exception to RPC server
     * 
     * RuntimeExceptions:
     * @throws InvalidPathException If path <code>dst</code> is invalid
     */
    public boolean copy(final Path src, final Path dst, boolean deleteSource,
        boolean overwrite) throws AccessControlException,
        FileAlreadyExistsException, FileNotFoundException,
        ParentNotDirectoryException, UnsupportedFileSystemException, 
	IOException {
      checkNotSchemeWithRelative(src);
      checkNotSchemeWithRelative(dst);
      Path qSrc = makeQualified(src);
      Path qDst = makeQualified(dst);
      checkDest(qSrc.getName(), qDst, overwrite);
      FileStatus fs = FileContext.this.getFileStatus(qSrc);
      if (fs.isDirectory()) {
        checkDependencies(qSrc, qDst);
        mkdir(qDst, FsPermission.getDirDefault(), true);
        FileStatus[] contents = listStatus(qSrc);
        for (FileStatus content : contents) {
          copy(makeQualified(content.getPath()), makeQualified(new Path(qDst,
              content.getPath().getName())), deleteSource, overwrite);
        }
      } else {
        InputStream in=null;
        OutputStream out = null;
        try {
          in = open(qSrc);
          EnumSet<CreateFlag> createFlag = overwrite ? EnumSet.of(
              CreateFlag.CREATE, CreateFlag.OVERWRITE) : 
                EnumSet.of(CreateFlag.CREATE);
          out = create(qDst, createFlag);
          IOUtils.copyBytes(in, out, conf, true);
        } catch (IOException e) {
          IOUtils.closeStream(out);
          IOUtils.closeStream(in);
          throw e;
        }
      }
      if (deleteSource) {
        return delete(qSrc, true);
      } else {
        return true;
      }
    }
  }

  /**
   * Check if copying srcName to dst would overwrite an existing 
   * file or directory.
   * @param srcName File or directory to be copied.
   * @param dst Destination to copy srcName to.
   * @param overwrite Whether it's ok to overwrite an existing file. 
   * @throws AccessControlException If access is denied.
   * @throws IOException If dst is an existing directory, or dst is an 
   * existing file and the overwrite option is not passed.
   */
  private void checkDest(String srcName, Path dst, boolean overwrite)
      throws AccessControlException, IOException {
    try {
      FileStatus dstFs = getFileStatus(dst);
      if (dstFs.isDirectory()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        // Recurse to check if dst/srcName exists.
        checkDest(null, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + new Path(dst, srcName)
            + " already exists");
      }
    } catch (FileNotFoundException e) {
      // dst does not exist - OK to copy.
    }
  }
   
  //
  // If the destination is a subdirectory of the source, then
  // generate exception
  //
  private static void checkDependencies(Path qualSrc, Path qualDst)
    throws IOException {
    if (isSameFS(qualSrc, qualDst)) {
      String srcq = qualSrc.toString() + Path.SEPARATOR;
      String dstq = qualDst.toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + qualSrc + " to itself.");
        } else {
          throw new IOException("Cannot copy " + qualSrc +
                             " to its subdirectory " + qualDst);
        }
      }
    }
  }
  
  /**
   * Are qualSrc and qualDst of the same file system?
   * @param qualPath1 - fully qualified path
   * @param qualPath2 - fully qualified path
   * @return
   */
  private static boolean isSameFS(Path qualPath1, Path qualPath2) {
    URI srcUri = qualPath1.toUri();
    URI dstUri = qualPath2.toUri();
    return (srcUri.getScheme().equals(dstUri.getScheme()) && 
        !(srcUri.getAuthority() != null && dstUri.getAuthority() != null && srcUri
        .getAuthority().equals(dstUri.getAuthority())));
  }

  /**
   * Deletes all the paths in deleteOnExit on JVM shutdown.
   */
  static class FileContextFinalizer implements Runnable {
    @Override
    public synchronized void run() {
      processDeleteOnExit();
    }
  }

  /**
   * Resolves all symbolic links in the specified path.
   * Returns the new path object.
   */
  protected Path resolve(final Path f) throws FileNotFoundException,
      UnresolvedLinkException, AccessControlException, IOException {
    return new FSLinkResolver<Path>() {
      @Override
      public Path next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.resolvePath(p);
      }
    }.resolve(this, f);
  }

  /**
   * Resolves all symbolic links in the specified path leading up 
   * to, but not including the final path component.
   * @param f path to resolve
   * @return the new path object.
   */
  protected Path resolveIntermediate(final Path f) throws IOException {
    return new FSLinkResolver<FileStatus>() {
      @Override
      public FileStatus next(final AbstractFileSystem fs, final Path p) 
        throws IOException, UnresolvedLinkException {
        return fs.getFileLinkStatus(p);
      }
    }.resolve(this, f).getPath();
  }

  /**
   * Returns the list of AbstractFileSystems accessed in the path. The list may
   * contain more than one AbstractFileSystems objects in case of symlinks.
   * 
   * @param f
   *          Path which needs to be resolved
   * @return List of AbstractFileSystems accessed in the path
   * @throws IOException
   */
  Set<AbstractFileSystem> resolveAbstractFileSystems(final Path f)
      throws IOException {
    final Path absF = fixRelativePart(f);
    final HashSet<AbstractFileSystem> result 
      = new HashSet<AbstractFileSystem>();
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException, UnresolvedLinkException {
        result.add(fs);
        fs.getFileStatus(p);
        return null;
      }
    }.resolve(this, absF);
    return result;
  }
  
  /**
   * Class used to perform an operation on and resolve symlinks in a
   * path. The operation may potentially span multiple file systems.  
   */
  protected abstract class FSLinkResolver<T> {
    // The maximum number of symbolic link components in a path
    private static final int MAX_PATH_LINKS = 32;

    /**
     * Generic helper function overridden on instantiation to perform a 
     * specific operation on the given file system using the given path
     * which may result in an UnresolvedLinkException. 
     * @param fs AbstractFileSystem to perform the operation on.
     * @param p Path given the file system.
     * @return Generic type determined by the specific implementation.
     * @throws UnresolvedLinkException If symbolic link <code>path</code> could 
     *           not be resolved
     * @throws IOException an I/O error occured
     */
    public abstract T next(final AbstractFileSystem fs, final Path p) 
      throws IOException, UnresolvedLinkException;  
        
    /**
     * Performs the operation specified by the next function, calling it
     * repeatedly until all symlinks in the given path are resolved.
     * @param fc FileContext used to access file systems.
     * @param p The path to resolve symlinks in.
     * @return Generic type determined by the implementation of next.
     * @throws IOException
     */
    public T resolve(final FileContext fc, Path p) throws IOException {
      int count = 0;
      T in = null;
      Path first = p;
      // NB: More than one AbstractFileSystem can match a scheme, eg 
      // "file" resolves to LocalFs but could have come by RawLocalFs.
      AbstractFileSystem fs = fc.getFSofPath(p);      

      // Loop until all symlinks are resolved or the limit is reached
      for (boolean isLink = true; isLink;) {
        try {
          in = next(fs, p);
          isLink = false;
        } catch (UnresolvedLinkException e) {
          if (count++ > MAX_PATH_LINKS) {
            throw new IOException("Possible cyclic loop while " +
                                  "following symbolic link " + first);
          }
          // Resolve the first unresolved path component
          p = qualifySymlinkTarget(fs, p, fs.getLinkTarget(p));
          fs = fc.getFSofPath(p);
        }
      }
      return in;
    }
  }
  
  /**
   * Get the statistics for a particular file system
   * 
   * @param uri
   *          the uri to lookup the statistics. Only scheme and authority part
   *          of the uri are used as the key to store and lookup.
   * @return a statistics object
   */
  public static Statistics getStatistics(URI uri) {
    return AbstractFileSystem.getStatistics(uri);
  }

  /**
   * Clears all the statistics stored in AbstractFileSystem, for all the file
   * systems.
   */
  public static void clearStatistics() {
    AbstractFileSystem.clearStatistics();
  }

  /**
   * Prints the statistics to standard output. File System is identified by the
   * scheme and authority.
   */
  public static void printStatistics() {
    AbstractFileSystem.printStatistics();
  }

  /**
   * @return Map of uri and statistics for each filesystem instantiated. The uri
   *         consists of scheme and authority for the filesystem.
   */
  public static Map<URI, Statistics> getAllStatistics() {
    return AbstractFileSystem.getAllStatistics();
  }
  
  /**
   * Get delegation tokens for the file systems accessed for a given
   * path.
   * @param p Path for which delegations tokens are requested.
   * @param renewer the account name that is allowed to renew the token.
   * @return List of delegation tokens.
   * @throws IOException
   */
  @InterfaceAudience.LimitedPrivate( { "HDFS", "MapReduce" })
  public List<Token<?>> getDelegationTokens(
      Path p, String renewer) throws IOException {
    Set<AbstractFileSystem> afsSet = resolveAbstractFileSystems(p);
    List<Token<?>> tokenList = 
        new ArrayList<Token<?>>();
    for (AbstractFileSystem afs : afsSet) {
      List<Token<?>> afsTokens = afs.getDelegationTokens(renewer);
      tokenList.addAll(afsTokens);
    }
    return tokenList;
  }
}
