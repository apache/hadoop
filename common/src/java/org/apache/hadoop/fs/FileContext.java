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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate.Project;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * The FileContext class provides an interface to the application writer for
 * using the Hadoop filesystem.
 * It provides a set of methods for the usual operation: create, open, 
 * list, etc 
 * 
 * *** Path Names ***
 * 
 * The Hadoop filesystem supports a URI name space and URI names.
 * It offers a a forest of filesystems that can be referenced using fully
 * qualified URIs.
 * 
 * Two common Hadoop filesystems implementations are
 *   the local filesystem: file:///path
 *   the hdfs filesystem hdfs://nnAddress:nnPort/path
 *   
 * While URI names are very flexible, it requires knowing the name or address
 * of the server. For convenience one often wants to access the default system
 * in your environment without knowing its name/address. This has an
 * additional benefit that it allows one to change one's default fs
 *  (say your admin moves you from cluster1 to cluster2).
 *  
 * Too facilitate this Hadoop supports a notion of a default filesystem.
 * The user can set his default filesystem, although this is
 * typically set up for you in your environment in your default config.
 * A default filesystem implies a default scheme and authority; slash-relative
 * names (such as /for/bar) are resolved relative to that default FS.
 * Similarly a user can also have working-directory-relative names (i.e. names
 * not starting with a slash). While the working directory is generally in the
 * same default FS, the wd can be in a different FS; in particular, changing
 * the default filesystem DOES NOT change the working directory,
 * 
 *  Hence Hadoop path names can be one of:
 *       fully qualified URI:  scheme://authority/path
 *       slash relative names: /path    - relative to the default filesystem
 *       wd-relative names:    path        - relative to the working dir
 *       
 *       Relative paths with scheme (scheme:foo/bar) are illegal
 *  
 *  ****The Role of the FileContext and configuration defaults****
 *  The FileContext provides file namespace context for resolving file names;
 *  it also contains the umask for permissions, In that sense it is like the
 *  per-process file-related state in Unix system.
 *  These, in general, are obtained from the default configuration file
 *  in your environment,  (@see {@link Configuration}
 *  
 *  No other configuration parameters are obtained from the default config as 
 *  far as the file context layer is concerned. All filesystem instances
 *  (i.e. deployments of filesystems) have default properties; we call these
 *  server side (SS) defaults. Operation like create allow one to select many 
 *  properties: either pass them in as explicit parameters or 
 *  one can choose to used the SS properties.
 *  
 *  The filesystem related SS defaults are
 *    - the home directory (default is "/user/<userName>")
 *    - the initial wd (only for local fs)
 *    - replication factor
 *    - block size
 *    - buffer size
 *    - bytesPerChecksum (if used).
 *
 * 
 * *** Usage Model for the FileContext class ***
 * 
 * Example 1: use the default config read from the $HADOOP_CONFIG/core.xml.
 *            Unspecified values come from core-defaults.xml in the release jar.
 *            
 *     myFiles = getFileContext(); // uses the default config 
 *     myFiles.create(path, ...);
 *     myFiles.setWorkingDir(path)
 *     myFiles.open (path, ...);   
 *     
 * Example 2: Use a specific config, ignoring $HADOOP_CONFIG
 *    configX = someConfigSomeOnePassedToYou.
 *    myFContext = getFileContext(configX); //configX not changed but passeddown
 *    myFContext.create(path, ...);
 *    myFContext.setWorkingDir(path)
 *                                             
 * Other ways of creating new FileContexts:
 *   getLocalFSFileContext(...)  // local filesystem is the default FS
 *   getLocalFileContext(URI, ...) // where specified URI is default FS.
 *    
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */

public final class FileContext {
  
  public static final Log LOG = LogFactory.getLog(FileContext.class);
  
  /**
   * List of files that should be deleted on JVM shutdown
   */
  final static Map<FileContext, Set<Path>> deleteOnExit = 
    new IdentityHashMap<FileContext, Set<Path>>();

  /** JVM shutdown hook thread */
  final static FileContextFinalizer finalizer = 
    new FileContextFinalizer();
  
  /**
   * The FileContext is defined by.
   *  1) defaultFS (slash)
   *  2) wd
   *  3) umask
   *
   */   
  private final FileSystem defaultFS; // the default FS for this FileContext.
  private Path workingDir;          // Fully qualified
  private FsPermission umask;
  private final Configuration conf; // passed to the filesystem below
                                  // When we move to new AbstractFileSystem
                                  // then it is not really needed except for
                                  // undocumented config vars;

  private FileContext(final FileSystem defFs, final FsPermission theUmask, 
      final Configuration aConf) {
    defaultFS = defFs;
    umask = FsPermission.getUMask(aConf);
    conf = aConf;
    /*
     * Init the wd.
     * WorkingDir is implemented at the FileContext layer 
     * NOT at the FileSystem layer. 
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
   * Hence this method os not called makeAbsolute() and 
   * has been deliberately declared private.
   */

  private Path fixRelativePart(Path f) {
    if (f.isUriPathAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }

  /**
   * Delete all the paths that were marked as delete-on-exit.
   */
  static void processDeleteOnExit() {
    synchronized (deleteOnExit) {
      Set<Entry<FileContext, Set<Path>>> set = deleteOnExit.entrySet();
      for (Entry<FileContext, Set<Path>> entry : set) {
        FileContext fc = entry.getKey();
        Set<Path> paths = entry.getValue();
        for (Path path : paths) {
          try {
            fc.delete(path, true);
          }
          catch (IOException e) {
            LOG.warn("Ignoring failure to deleteOnExit for path " + path);
          }
        }
      }
      deleteOnExit.clear();
    }
  }
  
  /**
   * Pathnames with scheme and relative path are illegal.
   * @param path to be checked
   * @throws IllegalArgumentException if of type scheme:foo/bar
   */
  private static void checkNotSchemeWithRelative(final Path path) {
    if (path.toUri().isAbsolute() && !path.isUriPathAbsolute()) {
      throw new IllegalArgumentException(
          "Unsupported name: has scheme but relative path-part");
    }
  }
  
  /**
   * Get the filesystem of supplied path.
   * @param absOrFqPath - absolute or fully qualified path
   * @return the filesystem of the path
   * @throws IOException
   */
  private FileSystem getFSofPath(final Path absOrFqPath) throws IOException {
    checkNotSchemeWithRelative(absOrFqPath);
    if (!absOrFqPath.isAbsolute() && absOrFqPath.toUri().getScheme() == null) {
      throw new IllegalArgumentException(
          "FileContext Bug: path is relative");
    }
    
    // TBD cleanup this impl once we create a new FileSystem to replace current
    // one - see HADOOP-6223.
    try { 
      // Is it the default FS for this FileContext?
      defaultFS.checkPath(absOrFqPath);
      return defaultFS;
    } catch (Exception e) { // it is different FileSystem
      return FileSystem.get(absOrFqPath.toUri(), conf);
    }
  }
  
  
  /**
   * Protected Static Factory methods for getting a FileContexts
   * that take a FileSystem as input. To be used for testing.
   * Protected since new FileSystem will be protected.
   * Note new file contexts are created for each call.
   */
  
  /**
   * Create a FileContext with specified FS as default 
   * using the specified config.
   * 
   * @param defFS
   * @param aConf
   * @return new FileContext with specifed FS as default.
   * @throws IOException if the filesystem with specified cannot be created
   */
  protected static FileContext getFileContext(final FileSystem defFS,
                    final Configuration aConf) throws IOException {
    return new FileContext(defFS, FsPermission.getUMask(aConf), aConf);
  }
  
  /**
   * Create a FileContext for specified FileSystem using the default config.
   * 
   * @param defaultFS
   * @return a FileSystem for the specified URI
   * @throws IOException if the filesysem with specified cannot be created
   */
  protected static FileContext getFileContext(final FileSystem defaultFS)
    throws IOException {
    return getFileContext(defaultFS, new Configuration());
  }
 
 
  public static final URI LOCAL_FS_URI = URI.create("file:///");
  public static final FsPermission DEFAULT_PERM = FsPermission.getDefault();
  
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
   * The conf is passed to lower layers like FileSystem and HDFS which
   * pick up their own config variables.
   */
   
  /**
   * Create a FileContext using the default config read from the
   * $HADOOP_CONFIG/core.xml,
   * Unspecified key-values for config are defaulted from core-defaults.xml
   * in the release jar.
   * 
   * @throws IOException if default FileSystem in the config  cannot be created
   */
  public static FileContext getFileContext() throws IOException {
    return getFileContext(new Configuration());
  } 
  
  private static FileContext localFsSingleton = null;
  /**
   * 
   * @return a FileContext for the local filesystem using the default config.
   * @throws IOException 
   */
  public static FileContext getLocalFSFileContext() throws IOException {
    if (localFsSingleton == null) {
      localFsSingleton = getFileContext(LOCAL_FS_URI); 
    }
    return localFsSingleton;
  }

  
  /**
   * Create a FileContext for specified URI using the default config.
   * 
   * @param defaultFsUri
   * @return a FileSystem for the specified URI
   * @throws IOException if the filesysem with specified cannot be created
   */
  public static FileContext getFileContext(final URI defaultFsUri)
    throws IOException {
    return getFileContext(defaultFsUri, new Configuration());
  }
  
  /**
   * Create a FileContext for specified default URI using the specified config.
   * 
   * @param defaultFsUri
   * @param aConf
   * @return new FileContext for specified uri
   * @throws IOException if the filesysem with specified cannot be created
   */
  public static FileContext getFileContext(final URI defaultFsUri,
                    final Configuration aConf) throws IOException {
    return getFileContext(FileSystem.get(defaultFsUri,  aConf), aConf);
  }

  /**
   * Create a FileContext using the passed config.
   * 
   * @param aConf
   * @return new FileContext
   * @throws IOException  if default FileSystem in the config  cannot be created
   */
  public static FileContext getFileContext(final Configuration aConf)
    throws IOException {
    return getFileContext(URI.create(FsConfig.getDefaultFsURI(aConf)), aConf);
  }
  
  
  /**
   * @param aConf - from which the FileContext is configured
   * @return a FileContext for the local filesystem using the specified config.
   * @throws IOException 
   */
  public static FileContext getLocalFSFileContext(final Configuration aConf)
    throws IOException {
    return getFileContext(LOCAL_FS_URI, aConf);
  }

  /* This method is needed for tests. */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable /* return type will change to AFS once
                                  HADOOP-6223 is completed */
  protected FileSystem getDefaultFileSystem() {
    return defaultFS;
  }
  
  /**
   * Set the working directory for wd-relative names (such a "foo/bar").
   * @param newWDir
   * @throws IOException
   * 
   * newWdir can be one of 
   *     - relative path:  "foo/bar";
   *     - absolute without scheme: "/foo/bar"
   *     - fully qualified with scheme: "xx://auth/foo/bar"
   *  Illegal WDs:
   *      - relative with scheme: "xx:foo/bar" 
   */
  public void setWorkingDirectory(final Path newWDir) throws IOException {
    checkNotSchemeWithRelative(newWDir);
    // wd is stored as fully qualified path. 

    final Path newWorkingDir =  new Path(workingDir, newWDir);
    FileStatus status = getFileStatus(newWorkingDir);
    if (!status.isDir()) {
      throw new FileNotFoundException(" Cannot setWD to a file");
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
   * Make the path fully qualified if it is isn't. 
   * A Fully-qualified path has scheme and authority specified and an absolute
   * path.
   * Use the default filesystem and working dir in this FileContext to qualify.
   * @param path
   * @return qualified path
   */
  public Path makeQualified(final Path path) {
    return path.makeQualified(defaultFS.getUri(), getWorkingDirectory());
  } 

  
  /**
   * Create or overwrite file on indicated path and returns an output stream
   * for writing into the file.
   * @param f the file name to open
   * @param createFlag gives the semantics  of create: overwrite, append etc.
   * @param opts  - varargs of CreateOpt:
   *     Progress - to report progress on the operation - default null
   *     Permission - umask is applied against permisssion:
   *                  default FsPermissions:getDefault()
   *                  @see #setPermission(Path, FsPermission)
   *     CreateParent - create missing parent path
   *                  default is to not create parents
   *     
   *                The defaults for the following are  SS defaults of the
   *                file server implementing the tart path.
   *                Not all parameters make sense for all kinds of filesystem
   *                - eg. localFS ignores Blocksize, replication, checksum
   *    BufferSize - buffersize used in FSDataOutputStream
   *    Blocksize - block size for file blocks
   *    ReplicationFactor - replication for blocks
   *    BytesPerChecksum - bytes per checksum
   *                     
   *    
   * @throws IOException
   */
  @SuppressWarnings("deprecation") // call to primitiveCreate
  public FSDataOutputStream create(final Path f,
                                    final EnumSet<CreateFlag> createFlag,
                                    CreateOpts... opts)
    throws IOException {
    Path absF = fixRelativePart(f);
    FileSystem fsOfAbsF = getFSofPath(absF);

    // If one of the options is a permission, extract it & apply umask
    // If not, add a default Perms and apply umask;
    // FileSystem#create

    FsPermission permission = null;

    if (opts != null) {
      for (int i = 0; i < opts.length; ++i) {
        if (opts[i] instanceof CreateOpts.Perms) {
          if (permission != null) 
            throw new IllegalArgumentException("multiple permissions varargs");
          permission = ((CreateOpts.Perms) opts[i]).getValue();
          opts[i] = CreateOpts.perms(permission.applyUMask(umask));
        }
      }
    }

    CreateOpts[] theOpts = opts;
    if (permission == null) { // no permission was set
      CreateOpts[] newOpts = new CreateOpts[opts.length + 1];
      System.arraycopy(opts, 0, newOpts, 0, opts.length);
      newOpts[opts.length] = 
        CreateOpts.perms(FsPermission.getDefault().applyUMask(umask));
      theOpts = newOpts;
    }
    return fsOfAbsF.primitiveCreate(absF, createFlag, theOpts);
  }
  
  /**
   * Make the given file and all non-existent parents into
   * directories.
   * 
   * @param dir - the dir to make
   * @param permission - permissions is set permission&~umask
   * @param createParent - if true then missing parent dirs are created
   *                       if false then parent must exist
   * @throws IOException when operation fails not authorized or 
   *   if parent does not exist and createParent is false.
   */
  @SuppressWarnings("deprecation") // call to primitiveMkdir
  public void mkdir(final Path dir, final FsPermission permission,
      final boolean createParent)
    throws IOException {
    Path absDir = fixRelativePart(dir);
    FsPermission absFerms = (permission == null ? 
          FsPermission.getDefault() : permission).applyUMask(umask);
    getFSofPath(absDir).primitiveMkdir(absDir, absFerms, createParent);
  }

  /**
   * Delete a file.
   * @param f the path to delete.
   * @param recursive if path is a directory and set to 
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false. 
   * @return  true if delete is successful else false. 
   * @throws IOException
   */
  public boolean delete(final Path f, final boolean recursive) 
    throws IOException {
    Path absF = fixRelativePart(f);
    return getFSofPath(absF).delete(absF, recursive);
  }
 
  /**
   * Opens an FSDataInputStream at the indicated Path using
   * default buffersize.
   * @param f the file name to open
   */
  public FSDataInputStream open(final Path f) throws IOException {
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).open(absF);
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(final Path f, int bufferSize)
    throws IOException {
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).open(absF, bufferSize);
  }

 /**
  * Set replication for an existing file.
  * 
  * @param f file name
  * @param replication new replication
  * @throws IOException
  * @return true if successful;
  *         false if file does not exist or is a directory
  */
  public boolean setReplication(final Path f, final short replication)
    throws IOException {
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).setReplication(absF, replication);
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
   * If OVERWRITE option is not passed as an argument, rename fails
   * if the dst already exists.
   * <p>
   * If OVERWRITE option is passed as an argument, rename overwrites
   * the dst if it is a file or an empty directory. Rename fails if dst is
   * a non-empty directory.
   * <p>
   * Note that atomicity of rename is dependent on the file system
   * implementation. Please refer to the file system documentation for
   * details
   * <p>
   * 
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on failure
   */
  @SuppressWarnings("deprecation")
  public void rename(final Path src, final Path dst, final Rename... options)
    throws IOException {
    final Path absSrc  = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);
    FileSystem srcFS = getFSofPath(absSrc);
    FileSystem dstFS = getFSofPath(absDst);
    if(!srcFS.getUri().equals(dstFS.getUri())) {
      throw new IOException("Renames across FileSystems not supported");
    }
    srcFS.rename(absSrc, absDst, options);
  }
  
  /**
   * Set permission of a path.
   * @param f
   * @param permission - the new absolute permission (umask is not applied)
   */
  public void setPermission(final Path f, final FsPermission permission)
    throws IOException {
    final Path absF = fixRelativePart(f);
    getFSofPath(absF).setPermission(absF, permission);
  }

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   * @param f The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  public void setOwner(final Path f, final String username,
                        final String groupname) throws IOException {
    if ((username == null) && (groupname == null)) {
      throw new IllegalArgumentException(
          "usernme and groupname cannot both be null");
    }
    final Path absF = fixRelativePart(f);
    getFSofPath(absF).setOwner(absF, username, groupname);
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
   */
  public void setTimes(final Path f, final long mtime, final long atime)
    throws IOException {
    final Path absF = fixRelativePart(f);
    getFSofPath(absF).setTimes(absF, mtime, atime);
  }

  /**
   * Get the checksum of a file.
   *
   * @param f The file path
   * @return The file checksum.  The default return value is null,
   *  which indicates that no checksum algorithm is implemented
   *  in the corresponding FileSystem.
   */
  public FileChecksum getFileChecksum(final Path f) throws IOException {
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).getFileChecksum(absF);
  }

  /**
   * Set the verify checksum flag for the  filesystem denoted by the path.
   * This is only applicable if the 
   * corresponding FileSystem supports checksum. By default doesn't do anything.
   * @param verifyChecksum
   * @param f - set the verifyChecksum for the Filesystem containing this path
   * @throws IOException 
   */

  public void setVerifyChecksum(final boolean verifyChecksum, final Path f)
    throws IOException {
    final Path absF = fixRelativePart(f);
    getFSofPath(absF).setVerifyChecksum(verifyChecksum);
    
    //TBD need to be changed when we add symlinks.
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus getFileStatus(final Path f) throws IOException {
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).getFileStatus(absF);
  }
  
  /**
   * Return blockLocation of the given file for the given offset and len.
   *  For a nonexistent file or regions, null will be returned.
   *
   * This call is most helpful with DFS, where it returns 
   * hostnames of machines that contain the given file.
   * 
   * @param p - get blocklocations of this file
   * @param start position (byte offset)
   * @param len (in bytes)
   * @return block locations for given file at specified offset of len
   * @throws IOException
   */
  
  @InterfaceAudience.LimitedPrivate({Project.HDFS, Project.MAPREDUCE})
  @InterfaceStability.Evolving
  public BlockLocation[] getFileBlockLocations(final Path p, 
    final long start, final long len) throws IOException {
    return getFSofPath(p).getFileBlockLocations(p, start, len);
  }
  
  /**
   * Returns a status object describing the use and capacity of the
   * filesystem denoted by the Parh argument p.
   * If the filesystem has multiple partitions, the
   * use and capacity of the partition pointed to by the specified
   * path is reflected.
   * 
   * @param f Path for which status should be obtained. null means the
   * root partition of the default filesystem. 
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public FsStatus getFsStatus(final Path f) throws IOException {
    if (f == null) {
      return defaultFS.getStatus(null);
    }
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).getStatus(absF);
  }
  
  /**
   * Does the file exist?
   * Note: Avoid using this method if you already have FileStatus in hand.
   * Instead reuse the FileStatus 
   * @param f the  file or dir to be checked
   */
  public boolean exists(final Path f) throws IOException {
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Is a directory?
   * Note: Avoid using this method if you already have FileStatus in hand.
   * Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   * 
   * @param f Path to evaluate
   * @return True iff the named path is a directory.
   * @throws IOException
   */
  public boolean isDirectory(final Path f) throws IOException {
    try {
      final Path absF = fixRelativePart(f);
      return getFileStatus(absF).isDir();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /** True iff the named path is a regular file.
   * Note: Avoid using this method  if you already have FileStatus in hand
   * Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   */
  public boolean isFile(final Path f) throws IOException {
    try {
      final Path absF = fixRelativePart(f);
      return !getFileStatus(absF).isDir();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }
  
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f is the path
   * @return the statuses of the files/directories in the given path
   * @throws IOException
   */
  public FileStatus[] listStatus(final Path f) throws IOException {
    final Path absF = fixRelativePart(f);
    return getFSofPath(absF).listStatus(absF);
  }

  /**
   * Mark a path to be deleted on JVM shutdown.
   * 
   * @param f the existing path to delete.
   * @return  true if deleteOnExit is successful, otherwise false.
   * @throws IOException
   */
  public boolean deleteOnExit(Path f) throws IOException {
    if (!exists(f)) {
      return false;
    }
    synchronized (deleteOnExit) {
      if (deleteOnExit.isEmpty() && !finalizer.isAlive()) {
        Runtime.getRuntime().addShutdownHook(finalizer);
      }
      
      Set<Path> set = deleteOnExit.get(this);
      if (set == null) {
        set = new TreeSet<Path>();
        deleteOnExit.put(this, set);
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
     * Return a list of file status objects that corresponds to supplied paths
     * excluding those non-existent paths.
     * 
     * @param paths are the list of paths we want information from
     * @return a list of FileStatus objects
     * @throws IOException
     */
    private FileStatus[] getFileStatus(Path[] paths) throws IOException {
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
     * Filter files/directories in the given list of paths using default
     * path filter.
     * 
     * @param files is the list of paths
     * @return a list of statuses for the files under the given paths after
     *         applying the filter default Path filter
     * @exception IOException
     */
    public FileStatus[] listStatus(Path[] files)
      throws IOException {
      return listStatus(files, DEFAULT_FILTER);
    }
     
    /**
     * Filter files/directories in the given path using the user-supplied path
     * filter.
     * 
     * @param f is the path name
     * @param filter is the user-supplied path filter
     * @return an array of FileStatus objects for the files under the given path
     *         after applying the filter
     * @throws IOException
     *           if encounter any problem while fetching the status
     */
    public FileStatus[] listStatus(Path f, PathFilter filter)
      throws IOException {
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
     * @return a list of statuses for the files under the given paths after
     *         applying the filter
     * @exception IOException
     */
    public FileStatus[] listStatus(Path[] files, PathFilter filter)
      throws IOException {
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
        PathFilter filter) throws IOException {
      FileStatus[] listing = FileContext.this.listStatus(f);
      if (listing != null) {
        for (int i = 0; i < listing.length; i++) {
          if (filter.accept(listing[i].getPath())) {
            results.add(listing[i]);
          }
        }
      }
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

     * @return an array of paths that match the path pattern
     * @throws IOException
     */
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
      return globStatus(pathPattern, DEFAULT_FILTER);
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
    public FileStatus[] globStatus(final Path pathPattern, final PathFilter filter)
      throws IOException {
      
      String filename = pathPattern.toUri().getPath();
      
      List<String> filePatterns = GlobExpander.expand(filename);
      if (filePatterns.size() == 1) {
        Path p = fixRelativePart(pathPattern);
        FileSystem fs = getFSofPath(p);
        URI uri = fs.getUri();
        return globStatusInternal(uri, p, filter);
      } else {
        List<FileStatus> results = new ArrayList<FileStatus>();
        for (String filePattern : filePatterns) {
          Path p = new Path(filePattern);
          p = fixRelativePart(p);
          FileSystem fs = getFSofPath(p);
          URI uri = fs.getUri();
          FileStatus[] files = globStatusInternal(uri, p, filter);
          for (FileStatus file : files) {
            results.add(file);
          }
        }
        return results.toArray(new FileStatus[results.size()]);
      }
    }

    private FileStatus[] globStatusInternal(
        final URI uri, final Path inPathPattern, final PathFilter filter)
      throws IOException {
      Path[] parents = new Path[1];
      int level = 0;
      
      // comes in as full path, but just in case
      final Path pathPattern = fixRelativePart(inPathPattern);
      
      String filename = pathPattern.toUri().getPath();
      
      // path has only zero component
      if ("".equals(filename) || Path.SEPARATOR.equals(filename)) {
        Path p = pathPattern.makeQualified(uri, null);
        return getFileStatus(new Path[]{p});
      }

      // path has at least one component
      String[] components = filename.split(Path.SEPARATOR);
      // get the first component
      if (pathPattern.isAbsolute()) {
        parents[0] = new Path(Path.SEPARATOR);
        level = 1;
      } else {
        parents[0] = new Path(Path.CUR_DIR);
      }

      // glob the paths that match the parent path, ie. [0, components.length-1]
      boolean[] hasGlob = new boolean[]{false};
      Path[] relParentPaths = globPathsLevel(parents, components, level, hasGlob);
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
          results = listStatus(parentPaths, fp);
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
        int level, boolean[] hasGlob) throws IOException {
      if (level == filePattern.length - 1) {
        return parents;
      }
      if (parents == null || parents.length == 0) {
        return null;
      }
      GlobFilter fp = new GlobFilter(filePattern[level]);
      if (fp.hasPattern()) {
        parents = FileUtil.stat2Paths(listStatus(parents, fp));
        hasGlob[0] = true;
      } else {
        for (int i = 0; i < parents.length; i++) {
          parents[i] = new Path(parents[i], filePattern[level]);
        }
      }
      return globPathsLevel(parents, filePattern, level + 1, hasGlob);
    }

    /**
     * Copy file from src to dest.
     * @param src
     * @param dst
     * @return true if copy is successful
     * @throws IOException
     */
    public boolean copy(final Path src, final Path dst)  throws IOException {
      return copy(src, dst, false, false);
    }
    
    /**
     * Copy from src to dst, optionally deleting src and overwriting dst.
     * @param src
     * @param dst
     * @param deleteSource - delete src if true
     * @param overwrite  overwrite dst if true; throw IOException if dst exists
     *         and overwrite is false.
     * @return true if copy is successful
     * @throws IOException
     */
    public boolean copy(final Path src,  final Path dst,
        boolean deleteSource, boolean overwrite)
      throws IOException {
      checkNotSchemeWithRelative(src);
      checkNotSchemeWithRelative(dst);
      Path qSrc = makeQualified(src);
      Path qDst = makeQualified(dst);
      checkDest(qSrc.getName(), qDst, false);
      if (isDirectory(qSrc)) {
        checkDependencies(qSrc, qDst);
        mkdir(qDst, FsPermission.getDefault(), true);

        FileStatus[] contents = FileContext.this.listStatus(qSrc);
        for (FileStatus content : contents) {
          copy(content.getPath(), new Path(qDst, content.getPath()),
               deleteSource, overwrite);
        }
      } else {
        InputStream in=null;
        OutputStream out = null;
        try {
          in = open(qSrc);
          out = create(qDst, EnumSet.of(CreateFlag.OVERWRITE));
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

  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(final Path file) {
      return true;
    }
  };
  
  /* A class that could decide if a string matches the glob or not */
  private static class GlobFilter implements PathFilter {
    private PathFilter userFilter = DEFAULT_FILTER;
    private Pattern regex;
    private boolean hasPattern = false;
      
    /** Default pattern character: Escape any special meaning. */
    private static final char  PAT_ESCAPE = '\\';
    /** Default pattern character: Any single character. */
    private static final char  PAT_ANY = '.';
    /** Default pattern character: Character set close. */
    private static final char  PAT_SET_CLOSE = ']';
      
    GlobFilter() {
    }
      
    GlobFilter(final String filePattern) throws IOException {
      setRegex(filePattern);
    }
      
    GlobFilter(final String filePattern, final PathFilter filter)
      throws IOException {
      userFilter = filter;
      setRegex(filePattern);
    }
      
    private boolean isJavaRegexSpecialChar(char pChar) {
      return pChar == '.' || pChar == '$' || pChar == '(' || pChar == ')' ||
             pChar == '|' || pChar == '+';
    }
    
    void setRegex(String filePattern) throws IOException {
      int len;
      int setOpen;
      int curlyOpen;
      boolean setRange;

      StringBuilder fileRegex = new StringBuilder();

      // Validate the pattern
      len = filePattern.length();
      if (len == 0) {
        return;
      }

      setOpen = 0;
      setRange = false;
      curlyOpen = 0;

      for (int i = 0; i < len; i++) {
        char pCh;
          
        // Examine a single pattern character
        pCh = filePattern.charAt(i);
        if (pCh == PAT_ESCAPE) {
          fileRegex.append(pCh);
          i++;
          if (i >= len) {
            error("An escaped character does not present", filePattern, i);
          }
          pCh = filePattern.charAt(i);
        } else if (isJavaRegexSpecialChar(pCh)) {
          fileRegex.append(PAT_ESCAPE);
        } else if (pCh == '*') {
          fileRegex.append(PAT_ANY);
          hasPattern = true;
        } else if (pCh == '?') {
          pCh = PAT_ANY;
          hasPattern = true;
        } else if (pCh == '{') {
          fileRegex.append('(');
          pCh = '(';
          curlyOpen++;
          hasPattern = true;
        } else if (pCh == ',' && curlyOpen > 0) {
          fileRegex.append(")|");
          pCh = '(';
        } else if (pCh == '}' && curlyOpen > 0) {
          // End of a group
          curlyOpen--;
          fileRegex.append(")");
          pCh = ')';
        } else if (pCh == '[' && setOpen == 0) {
          setOpen++;
          hasPattern = true;
        } else if (pCh == '^' && setOpen > 0) {
        } else if (pCh == '-' && setOpen > 0) {
          // Character set range
          setRange = true;
        } else if (pCh == PAT_SET_CLOSE && setRange) {
          // Incomplete character set range
          error("Incomplete character set range", filePattern, i);
        } else if (pCh == PAT_SET_CLOSE && setOpen > 0) {
          // End of a character set
          if (setOpen < 2) {
            error("Unexpected end of set", filePattern, i);
          }
          setOpen = 0;
        } else if (setOpen > 0) {
          // Normal character, or the end of a character set range
          setOpen++;
          setRange = false;
        }
        fileRegex.append(pCh);
      }
        
      // Check for a well-formed pattern
      if (setOpen > 0 || setRange || curlyOpen > 0) {
        // Incomplete character set or character range
        error("Expecting set closure character or end of range, or }", 
            filePattern, len);
      }
      regex = Pattern.compile(fileRegex.toString());
    }
      
    boolean hasPattern() {
      return hasPattern;
    }
      
    public boolean accept(final Path path) {
      return regex.matcher(path.getName()).matches() && userFilter.accept(path);
    }
      
    private void error(final String s, final String pattern, final int pos)
      throws IOException {
      throw new IOException("Illegal file pattern: "
                            +s+ " for glob "+ pattern + " at " + pos);
    }
  }
  
  //
  // Check destionation. Throw IOException if destination already exists
  // and overwrite is not true
  //
  private void checkDest(String srcName, Path dst, boolean overwrite)
    throws IOException {
    if (exists(dst)) {
      if (isDirectory(dst)) {
      // TBD not very clear
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        checkDest(null, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
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
    return (srcUri.getAuthority().equals(dstUri.getAuthority()) && srcUri
        .getAuthority().equals(dstUri.getAuthority()));
  }

  /**
   * Deletes all the paths in deleteOnExit on JVM shutdown
   */
  static class FileContextFinalizer extends Thread {
    public synchronized void run() {
      processDeleteOnExit();
    }
  }
}
