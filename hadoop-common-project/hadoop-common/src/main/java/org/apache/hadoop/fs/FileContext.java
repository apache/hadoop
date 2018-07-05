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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsCreateModes;
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

import com.google.common.base.Preconditions;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FileContext class provides an interface for users of the Hadoop
 * file system. It exposes a number of file system operations, e.g. create,
 * open, list.
 * 
 * <h2>Path Names</h2>
 * 
 * The Hadoop file system supports a URI namespace and URI names. This enables
 * multiple types of file systems to be referenced using fully-qualified URIs.
 * Two common Hadoop file system implementations are
 * <ul>
 * <li>the local file system: file:///path
 * <li>the HDFS file system: hdfs://nnAddress:nnPort/path
 * </ul>
 * 
 * The Hadoop file system also supports additional naming schemes besides URIs.
 * Hadoop has the concept of a <i>default file system</i>, which implies a
 * default URI scheme and authority. This enables <i>slash-relative names</i>
 * relative to the default FS, which are more convenient for users and
 * application writers. The default FS is typically set by the user's
 * environment, though it can also be manually specified.
 * <p>
 * 
 * Hadoop also supports <i>working-directory-relative</i> names, which are paths
 * relative to the current working directory (similar to Unix). The working
 * directory can be in a different file system than the default FS.
 * <p>
 * Thus, Hadoop path names can be specified as one of the following:
 * <ul>
 * <li>a fully-qualified URI: scheme://authority/path (e.g.
 * hdfs://nnAddress:nnPort/foo/bar)
 * <li>a slash-relative name: path relative to the default file system (e.g.
 * /foo/bar)
 * <li>a working-directory-relative name: path relative to the working dir (e.g.
 * foo/bar)
 * </ul>
 *  Relative paths with scheme (scheme:foo/bar) are illegal.
 *  
 * <h2>Role of FileContext and Configuration Defaults</h2>
 *
 * The FileContext is the analogue of per-process file-related state in Unix. It
 * contains two properties:
 * 
 * <ul>
 * <li>the default file system (for resolving slash-relative names)
 * <li>the umask (for file permissions)
 * </ul>
 * In general, these properties are obtained from the default configuration file
 * in the user's environment (see {@link Configuration}).
 * 
 * Further file system properties are specified on the server-side. File system
 * operations default to using these server-side defaults unless otherwise
 * specified.
 * <p>
 * The file system related server-side defaults are:
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
 * <h2>Example Usage</h2>
 *
 * Example 1: use the default config read from the $HADOOP_CONFIG/core.xml.
 *   Unspecified values come from core-defaults.xml in the release jar.
 *  <ul>  
 *  <li> myFContext = FileContext.getFileContext(); // uses the default config
 *                                                // which has your default FS 
 *  <li>  myFContext.create(path, ...);
 *  <li>  myFContext.setWorkingDir(path);
 *  <li>  myFContext.open (path, ...);  
 *  <li>...
 *  </ul>  
 * Example 2: Get a FileContext with a specific URI as the default FS
 *  <ul>  
 *  <li> myFContext = FileContext.getFileContext(URI);
 *  <li> myFContext.create(path, ...);
 *  <li>...
 * </ul>
 * Example 3: FileContext with local file system as the default
 *  <ul> 
 *  <li> myFContext = FileContext.getLocalFSFileContext();
 *  <li> myFContext.create(path, ...);
 *  <li> ...
 *  </ul> 
 * Example 4: Use a specific config, ignoring $HADOOP_CONFIG
 *  Generally you should not need use a config unless you are doing
 *   <ul> 
 *   <li> configX = someConfigSomeOnePassedToYou;
 *   <li> myFContext = getFileContext(configX); // configX is not changed,
 *                                              // is passed down 
 *   <li> myFContext.create(path, ...);
 *   <li>...
 *  </ul>                                          
 *    
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileContext {
  
  public static final Logger LOG = LoggerFactory.getLogger(FileContext.class);
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
   *  3) umask (explicitly set via setUMask(),
   *      falling back to FsPermission.getUMask(conf))
   */   
  private final AbstractFileSystem defaultFS; //default FS for this FileContext.
  private Path workingDir;          // Fully qualified
  private FsPermission umask;
  private final Configuration conf;
  private final UserGroupInformation ugi;
  final boolean resolveSymlinks;
  private final Tracer tracer;

  private FileContext(final AbstractFileSystem defFs,
                      final Configuration aConf) {
    defaultFS = defFs;
    conf = aConf;
    tracer = FsTracer.get(aConf);
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
    resolveSymlinks = conf.getBoolean(
        CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY,
        CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT);
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
  Path fixRelativePart(Path p) {
    Preconditions.checkNotNull(p, "path cannot be null");
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
   * Get the file system of supplied path.
   * 
   * @param absOrFqPath - absolute or fully qualified path
   * @return the file system of the path
   * 
   * @throws UnsupportedFileSystemException If the file system for
   *           <code>absOrFqPath</code> is not supported.
   * @throws IOException If the file system for <code>absOrFqPath</code> could
   *         not be instantiated.
   */
  protected AbstractFileSystem getFSofPath(final Path absOrFqPath)
      throws UnsupportedFileSystemException, IOException {
    absOrFqPath.checkNotSchemeWithRelative();
    absOrFqPath.checkNotRelative();

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
    } catch (RuntimeException ex) {
      // RTEs can wrap other exceptions; if there is an IOException inner,
      // throw it direct.
      Throwable cause = ex.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw ex;
      }
    } catch (InterruptedException ex) {
      LOG.error(ex.toString());
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
   * @return new FileContext with specified FS as default.
   */
  public static FileContext getFileContext(final AbstractFileSystem defFS,
                    final Configuration aConf) {
    return new FileContext(defFS, aConf);
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
    if (defaultFsUri.getScheme() == null) {
      return getFileContext(aConf);
    }
    try {
      currentUser = UserGroupInformation.getCurrentUser();
      defaultAfs = getAbstractFileSystem(currentUser, defaultFsUri, aConf);
    } catch (UnsupportedFileSystemException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error(ex.toString());
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
    final URI defaultFsUri = URI.create(aConf.get(FS_DEFAULT_NAME_KEY,
        FS_DEFAULT_NAME_DEFAULT));
    if (   defaultFsUri.getScheme() != null
        && !defaultFsUri.getScheme().trim().isEmpty()) {
      return getFileContext(defaultFsUri, aConf);
    }
    throw new UnsupportedFileSystemException(String.format(
        "%s: URI configured via %s carries no scheme",
        defaultFsUri, FS_DEFAULT_NAME_KEY));
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
    newWDir.checkNotSchemeWithRelative();
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
    return (umask != null ? umask : FsPermission.getUMask(conf));
  }
  
  /**
   * Set umask to the supplied parameter.
   * @param newUmask  the new umask
   */
  public void setUMask(final FsPermission newUmask) {
    this.umask = newUmask;
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
   *          <li>Permission - umask is applied against permission: default is
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

    CreateOpts.Perms permOpt = CreateOpts.getOpt(CreateOpts.Perms.class, opts);
    FsPermission permission = (permOpt != null) ? permOpt.getValue() :
                                      FILE_DEFAULT_PERM;
    permission = FsCreateModes.applyUMask(permission, getUMask());

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
   * {@link FSDataOutputStreamBuilder} for {@liink FileContext}.
   */
  private static final class FCDataOutputStreamBuilder extends
      FSDataOutputStreamBuilder<
        FSDataOutputStream, FCDataOutputStreamBuilder> {
    private final FileContext fc;

    private FCDataOutputStreamBuilder(
        @Nonnull FileContext fc, @Nonnull Path p) throws IOException {
      super(fc, p);
      this.fc = fc;
      Preconditions.checkNotNull(fc);
    }

    @Override
    protected FCDataOutputStreamBuilder getThisBuilder() {
      return this;
    }

    @Override
    public FSDataOutputStream build() throws IOException {
      final EnumSet<CreateFlag> flags = getFlags();
      List<CreateOpts> createOpts = new ArrayList<>(Arrays.asList(
          CreateOpts.blockSize(getBlockSize()),
          CreateOpts.bufferSize(getBufferSize()),
          CreateOpts.repFac(getReplication()),
          CreateOpts.perms(getPermission())
      ));
      if (getChecksumOpt() != null) {
        createOpts.add(CreateOpts.checksumParam(getChecksumOpt()));
      }
      if (getProgress() != null) {
        createOpts.add(CreateOpts.progress(getProgress()));
      }
      if (isRecursive()) {
        createOpts.add(CreateOpts.createParent());
      }
      return fc.create(getPath(), flags,
          createOpts.toArray(new CreateOpts[0]));
    }
  }

  /**
   * Create a {@link FSDataOutputStreamBuilder} for creating or overwriting
   * a file on indicated path.
   *
   * @param f the file path to create builder for.
   * @return {@link FSDataOutputStreamBuilder} to build a
   *         {@link FSDataOutputStream}.
   *
   * Upon {@link FSDataOutputStreamBuilder#build()} being invoked,
   * builder parameters will be verified by {@link FileContext} and
   * {@link AbstractFileSystem#create}. And filesystem states will be modified.
   *
   * Client should expect {@link FSDataOutputStreamBuilder#build()} throw the
   * same exceptions as create(Path, EnumSet, CreateOpts...).
   */
  public FSDataOutputStreamBuilder<FSDataOutputStream, ?> create(final Path f)
      throws IOException {
    return new FCDataOutputStreamBuilder(this, f).create();
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
    final FsPermission absFerms = FsCreateModes.applyUMask(
        permission == null ?
            FsPermission.getDirDefault() : permission, getUMask());
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
        return fs.delete(p, recursive);
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
   * Truncate the file in the indicated path to the indicated size.
   * <ul>
   * <li>Fails if path is a directory.
   * <li>Fails if path does not exist.
   * <li>Fails if path is not closed.
   * <li>Fails if new size is greater than current size.
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
  public boolean truncate(final Path f, final long newLength)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absF = fixRelativePart(f);
    return new FSLinkResolver<Boolean>() {
      @Override
      public Boolean next(final AbstractFileSystem fs, final Path p)
          throws IOException, UnresolvedLinkException {
        return fs.truncate(p, newLength);
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
        return fs.setReplication(p, replication);
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
   * @throws UnsupportedFileSystemException if file system for <code>path</code>
   *   is not supported
   * @throws IOException see specific implementation
   * 
   * Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws 
   *           undeclared exception to RPC server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "Hive"})
  public void access(final Path path, final FsAction mode)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    final Path absPath = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(AbstractFileSystem fs, Path p) throws IOException,
          UnresolvedLinkException {
        fs.access(p, mode);
        return null;
      }
    }.resolve(this, absPath);
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
          fi.setSymlink(FSLinkResolver.qualifySymlinkTarget(fs.getUri(), p,
              fi.getSymlink()));
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
   * fs:///A/B/file  Resolved according to the target file system. Eg resolving
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
  @SuppressWarnings("deprecation")
  public void createSymlink(final Path target, final Path link,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException { 
    if (!FileSystem.areSymlinksEnabled()) {
      throw new UnsupportedOperationException("Symlinks not supported");
    }
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
        long length = status.getLen();
        return new ContentSummary.Builder().length(length).
            fileCount(1).directoryCount(0).spaceConsumed(length).
            build();
      }
      long[] summary = {0, 0, 1};
      RemoteIterator<FileStatus> statusIterator =
        FileContext.this.listStatus(f);
      while(statusIterator.hasNext()) {
        FileStatus s = statusIterator.next();
        long length = s.getLen();
        ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath()) :
            new ContentSummary.Builder().length(length).fileCount(1).
            directoryCount(0).spaceConsumed(length).build();
        summary[0] += c.getLength();
        summary[1] += c.getFileCount();
        summary[2] += c.getDirectoryCount();
      }
      return new ContentSummary.Builder().length(summary[0]).
          fileCount(summary[1]).directoryCount(summary[2]).
          spaceConsumed(summary[0]).build();
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
     * @param pathPattern a glob specifying a path pattern
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
      return new Globber(FileContext.this, pathPattern, DEFAULT_FILTER).glob();
    }
    
    /**
     * Return an array of FileStatus objects whose path names match pathPattern
     * and is accepted by the user-supplied path filter. Results are sorted by
     * their path names.
     * Return null if pathPattern has no glob and the path does not exist.
     * Return an empty array if pathPattern has a glob and no path matches it. 
     * 
     * @param pathPattern glob specifying the path pattern
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
      return new Globber(FileContext.this, pathPattern, filter).glob();
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
      src.checkNotSchemeWithRelative();
      dst.checkNotSchemeWithRelative();
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
        EnumSet<CreateFlag> createFlag = overwrite ? EnumSet.of(
            CreateFlag.CREATE, CreateFlag.OVERWRITE) :
            EnumSet.of(CreateFlag.CREATE);
        InputStream in = open(qSrc);
        try (OutputStream out = create(qDst, createFlag)) {
          IOUtils.copyBytes(in, out, conf, true);
        } finally {
          IOUtils.closeStream(in);
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
  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.modifyAclEntries(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Removes ACL entries from files and directories.  Other ACL entries are
   * retained.
   *
   * @param path Path to modify
   * @param aclSpec List<AclEntry> describing entries to remove
   * @throws IOException if an ACL could not be modified
   */
  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.removeAclEntries(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Removes all default ACL entries from files and directories.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be modified
   */
  public void removeDefaultAcl(Path path)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.removeDefaultAcl(p);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Removes all but the base ACL entries of files and directories.  The entries
   * for user, group, and others are retained for compatibility with permission
   * bits.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be removed
   */
  public void removeAcl(Path path) throws IOException {
    Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.removeAcl(p);
        return null;
      }
    }.resolve(this, absF);
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
  public void setAcl(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.setAcl(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Gets the ACLs of files and directories.
   *
   * @param path Path to get
   * @return RemoteIterator<AclStatus> which returns each AclStatus
   * @throws IOException if an ACL could not be read
   */
  public AclStatus getAclStatus(Path path) throws IOException {
    Path absF = fixRelativePart(path);
    return new FSLinkResolver<AclStatus>() {
      @Override
      public AclStatus next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.getAclStatus(p);
      }
    }.resolve(this, absF);
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
  public void setXAttr(Path path, final String name, final byte[] value,
      final EnumSet<XAttrSetFlag> flag) throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.setXAttr(p, name, value, flag);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Get an xattr for a file or directory.
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
  public byte[] getXAttr(Path path, final String name) throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<byte[]>() {
      @Override
      public byte[] next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.getXAttr(p, name);
      }
    }.resolve(this, absF);
  }

  /**
   * Get all of the xattrs for a file or directory.
   * Only those xattrs for which the logged-in user has permissions to view
   * are returned.
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @return Map<String, byte[]> describing the XAttrs of the file or directory
   * @throws IOException
   */
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<Map<String, byte[]>>() {
      @Override
      public Map<String, byte[]> next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.getXAttrs(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Get all of the xattrs for a file or directory.
   * Only those xattrs for which the logged-in user has permissions to view
   * are returned.
   * <p/>
   * Refer to the HDFS extended attributes user documentation for details.
   *
   * @param path Path to get extended attributes
   * @param names XAttr names.
   * @return Map<String, byte[]> describing the XAttrs of the file or directory
   * @throws IOException
   */
  public Map<String, byte[]> getXAttrs(Path path, final List<String> names)
      throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<Map<String, byte[]>>() {
      @Override
      public Map<String, byte[]> next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.getXAttrs(p, names);
      }
    }.resolve(this, absF);
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
  public void removeXAttr(Path path, final String name) throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.removeXAttr(p, name);
        return null;
      }
    }.resolve(this, absF);
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
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<List<String>>() {
      @Override
      public List<String> next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.listXAttrs(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Create a snapshot with a default name.
   *
   * @param path The directory where snapshots will be taken.
   * @return the snapshot path.
   *
   * @throws IOException If an I/O error occurred
   *
   * <p>Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   */
  public final Path createSnapshot(Path path) throws IOException {
    return createSnapshot(path, null);
  }

  /**
   * Create a snapshot.
   *
   * @param path The directory where snapshots will be taken.
   * @param snapshotName The name of the snapshot
   * @return the snapshot path.
   *
   * @throws IOException If an I/O error occurred
   *
   * <p>Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   */
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<Path>() {

      @Override
      public Path next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        return fs.createSnapshot(p, snapshotName);
      }
    }.resolve(this, absF);
  }

  /**
   * Rename a snapshot.
   *
   * @param path The directory path where the snapshot was taken
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   *
   * @throws IOException If an I/O error occurred
   *
   * <p>Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   */
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.renameSnapshot(p, snapshotOldName, snapshotNewName);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Delete a snapshot of a directory.
   *
   * @param path The directory that the to-be-deleted snapshot belongs to
   * @param snapshotName The name of the snapshot
   *
   * @throws IOException If an I/O error occurred
   *
   * <p>Exceptions applicable to file systems accessed over RPC:
   * @throws RpcClientException If an exception occurred in the RPC client
   * @throws RpcServerException If an exception occurred in the RPC server
   * @throws UnexpectedServerException If server implementation throws
   *           undeclared exception to RPC server
   */
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.deleteSnapshot(p, snapshotName);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Set the storage policy for a given file or directory.
   *
   * @param path file or directory path.
   * @param policyName the name of the target storage policy. The list
   *                   of supported Storage policies can be retrieved
   *                   via {@link #getAllStoragePolicies}.
   */
  public void setStoragePolicy(final Path path, final String policyName)
      throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.setStoragePolicy(path, policyName);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Unset the storage policy set for a given file or directory.
   * @param src file or directory path.
   * @throws IOException
   */
  public void unsetStoragePolicy(final Path src) throws IOException {
    final Path absF = fixRelativePart(src);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.unsetStoragePolicy(src);
        return null;
      }
    }.resolve(this, absF);
  }

  /**
   * Query the effective storage policy ID for the given file or directory.
   *
   * @param path file or directory path.
   * @return storage policy for give file.
   * @throws IOException
   */
  public BlockStoragePolicySpi getStoragePolicy(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    return new FSLinkResolver<BlockStoragePolicySpi>() {
      @Override
      public BlockStoragePolicySpi next(final AbstractFileSystem fs,
          final Path p)
          throws IOException {
        return fs.getStoragePolicy(p);
      }
    }.resolve(this, absF);
  }

  /**
   * Retrieve all the storage policies supported by this file system.
   *
   * @return all storage policies supported by this filesystem.
   * @throws IOException
   */
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return defaultFS.getAllStoragePolicies();
  }

  Tracer getTracer() {
    return tracer;
  }
}
