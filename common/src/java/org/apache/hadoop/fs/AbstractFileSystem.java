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
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/**
 * This class provides an interface for implementors of a Hadoop file system
 * (analogous to the VFS of Unix). Applications do not access this class;
 * instead they access files across all file systems using {@link FileContext}.
 * 
 * Pathnames passed to AbstractFileSystem can be fully qualified URI that
 * matches the "this" file system (ie same scheme and authority) 
 * or a Slash-relative name that is assumed to be relative
 * to the root of the "this" file system .
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class AbstractFileSystem {
  static final Log LOG = LogFactory.getLog(AbstractFileSystem.class);

  /** Recording statistics per a file system class. */
  private static final Map<Class<? extends AbstractFileSystem>, Statistics> 
  STATISTICS_TABLE =
      new IdentityHashMap<Class<? extends AbstractFileSystem>, Statistics>();
  
  /** Cache of constructors for each file system class. */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = 
    new ConcurrentHashMap<Class<?>, Constructor<?>>();
  
  private static final Class<?>[] URI_CONFIG_ARGS = 
    new Class[]{URI.class, Configuration.class};
  
  /** The statistics for this file system. */
  protected Statistics statistics;
  
  private final URI myUri;
  
  protected Statistics getStatistics() {
    return statistics;
  }
  
  /**
   * Prohibits names which contain a ".", "..", ":" or "/" 
   */
  private static boolean isValidName(String src) {
    // Check for ".." "." ":" "/"
    StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") || 
          element.equals(".")  ||
          (element.indexOf(":") >= 0)) {
        return false;
      }
    }
    return true;
  }
  
  /** 
   * Create an object for the given class and initialize it from conf.
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  static <T> T newInstance(Class<T> theClass,
    URI uri, Configuration conf) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(URI_CONFIG_ARGS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(uri, conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }
  
  /**
   * Create a file system instance for the specified uri using the conf. The
   * conf is used to find the class name that implements the file system. The
   * conf is also passed to the file system for its configuration.
   *
   * @param uri URI of the file system
   * @param conf Configuration for the file system
   * 
   * @return Returns the file system for the given URI
   *
   * @throws UnsupportedFileSystemException file system for <code>uri</code> is
   *           not found
   */
  private static AbstractFileSystem createFileSystem(URI uri, Configuration conf)
      throws UnsupportedFileSystemException {
    Class<?> clazz = conf.getClass("fs.AbstractFileSystem." + 
                                uri.getScheme() + ".impl", null);
    if (clazz == null) {
      throw new UnsupportedFileSystemException(
          "No AbstractFileSystem for scheme: " + uri.getScheme());
    }
    return (AbstractFileSystem) newInstance(clazz, uri, conf);
  }
  
  
  /**
   * Get the statistics for a particular file system.
   * @param cls the class to lookup
   * @return a statistics object
   */
  protected static synchronized Statistics getStatistics(String scheme,
      Class<? extends AbstractFileSystem> cls) {
    Statistics result = STATISTICS_TABLE.get(cls);
    if (result == null) {
      result = new Statistics(scheme);
      STATISTICS_TABLE.put(cls, result);
    }
    return result;
  }
  
  protected static synchronized void clearStatistics() {
    for(Statistics stat: STATISTICS_TABLE.values()) {
      stat.reset();
    }
  }

  protected static synchronized void printStatistics() {
    for (Map.Entry<Class<? extends AbstractFileSystem>, Statistics> pair: 
            STATISTICS_TABLE.entrySet()) {
      System.out.println("  FileSystem " + pair.getKey().getName() + 
                         ": " + pair.getValue());
    }
  }

  /**
   * The main factory method for creating a file system. Get a file system for
   * the URI's scheme and authority. The scheme of the <code>uri</code>
   * determines a configuration property name,
   * <tt>fs.AbstractFileSystem.<i>scheme</i>.impl</tt> whose value names the
   * AbstractFileSystem class.
   * 
   * The entire URI and conf is passed to the AbstractFileSystem factory method.
   * 
   * @param uri for the file system to be created.
   * @param conf which is passed to the file system impl.
   * 
   * @return file system for the given URI.
   * 
   * @throws UnsupportedFileSystemException if the file system for
   *           <code>uri</code> is not supported.
   */
  static AbstractFileSystem get(final URI uri, final Configuration conf)
      throws UnsupportedFileSystemException {
    return createFileSystem(uri, conf);
  }

  /**
   * Constructor to be called by subclasses.
   * 
   * @param uri for this file system.
   * @param supportedScheme the scheme supported by the implementor
   * @param authorityNeeded if true then theURI must have authority, if false
   *          then the URI must have null authority.
   *
   * @throws URISyntaxException <code>uri</code> has syntax error
   */
  protected AbstractFileSystem(final URI uri, final String supportedScheme,
      final boolean authorityNeeded, final int defaultPort)
      throws URISyntaxException {
    myUri = getUri(uri, supportedScheme, authorityNeeded, defaultPort);
    statistics = getStatistics(supportedScheme, getClass()); 
  }
  
  protected void checkScheme(URI uri, String supportedScheme) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new HadoopIllegalArgumentException("Uri without scheme: " + uri);
    }
    if (!scheme.equals(supportedScheme)) {
      throw new HadoopIllegalArgumentException("Uri scheme " + uri
          + " does not match the scheme " + supportedScheme);
    }
  }

  /**
   * Get the URI for the file system based on the given URI. The path, query
   * part of the given URI is stripped out and default file system port is used
   * to form the URI.
   * 
   * @param uri FileSystem URI.
   * @param authorityNeeded if true authority cannot be null in the URI. If
   *          false authority must be null.
   * @param defaultPort default port to use if port is not specified in the URI.
   * 
   * @return URI of the file system
   * 
   * @throws URISyntaxException <code>uri</code> has syntax error
   */
  private URI getUri(URI uri, String supportedScheme,
      boolean authorityNeeded, int defaultPort) throws URISyntaxException {
    checkScheme(uri, supportedScheme);
    // A file system implementation that requires authority must always
    // specify default port
    if (defaultPort < 0 && authorityNeeded) {
      throw new HadoopIllegalArgumentException(
          "FileSystem implementation error -  default port " + defaultPort
              + " is not valid");
    }
    String authority = uri.getAuthority();
    if (!authorityNeeded) {
      if (authority != null) {
        throw new HadoopIllegalArgumentException("Scheme with non-null authority: "
            + uri);
      }
      return new URI(supportedScheme + ":///");
    }
    if (authority == null) {
      throw new HadoopIllegalArgumentException("Uri without authority: " + uri);
    }
    int port = uri.getPort();
    port = port == -1 ? defaultPort : port;
    return new URI(supportedScheme + "://" + uri.getHost() + ":" + port);
  }
  
  /**
   * The default port of this file system.
   * 
   * @return default port of this file system's Uri scheme
   *         A uri with a port of -1 => default port;
   */
  protected abstract int getUriDefaultPort();

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   * 
   * @return the uri of this file system.
   */
  protected URI getUri() {
    return myUri;
  }
  
  /**
   * Check that a Path belongs to this FileSystem.
   * 
   * If the path is fully qualified URI, then its scheme and authority
   * matches that of this file system. Otherwise the path must be 
   * slash-relative name.
   * 
   * @throws InvalidPathException if the path is invalid
   */
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String thatScheme = uri.getScheme();
    String thatAuthority = uri.getAuthority();
    if (thatScheme == null) {
      if (thatAuthority == null) {
        if (path.isUriPathAbsolute()) {
          return;
        }
        throw new InvalidPathException("relative paths not allowed:" + 
            path);
      } else {
        throw new InvalidPathException(
            "Path without scheme with non-null autorhrity:" + path);
      }
    }
    String thisScheme = this.getUri().getScheme();
    String thisAuthority = this.getUri().getAuthority();
    
    // Schemes and authorities must match.
    // Allow for null Authority for file:///
    if (!thisScheme.equalsIgnoreCase(thatScheme) ||
       (thisAuthority != null && 
            !thisAuthority.equalsIgnoreCase(thatAuthority)) ||
       (thisAuthority == null && thatAuthority != null)) {
      throw new InvalidPathException("Wrong FS: " + path + ", expected: "
          + this.getUri());
    }
    
    int thisPort = this.getUri().getPort();
    int thatPort = path.toUri().getPort();
    if (thatPort == -1) { // -1 => defaultPort of Uri scheme
      thatPort = this.getUriDefaultPort();
    }
    if (thisPort != thatPort) {
      throw new InvalidPathException("Wrong FS: " + path + ", expected: "
          + this.getUri());
    }
  }
  
  /**
   * Get the path-part of a pathname. Checks that URI matches this file system
   * and that the path-part is a valid name.
   * 
   * @param p path
   * 
   * @return path-part of the Path p
   */
  protected String getUriPath(final Path p) {
    checkPath(p);
    String s = p.toUri().getPath();
    if (!isValidName(s)) {
      throw new InvalidPathException("Path part " + s + " from URI" + p
          + " is not a valid filename.");
    }
    return s;
  }
  
  /**
   * Some file systems like LocalFileSystem have an initial workingDir
   * that is used as the starting workingDir. For other file systems
   * like HDFS there is no built in notion of an initial workingDir.
   * 
   * @return the initial workingDir if the file system has such a notion
   *         otherwise return a null.
   */
  protected Path getInitialWorkingDirectory() {
    return null;
  }
  
  /** 
   * Return the current user's home directory in this file system.
   * The default implementation returns "/user/$USER/".
   * 
   * @return current user's home directory.
   */
  protected Path getHomeDirectory() {
    return new Path("/user/"+System.getProperty("user.name")).makeQualified(
                                                                getUri(), null);
  }
  
  /**
   * Return a set of server default configuration values.
   * 
   * @return server default configuration values
   * 
   * @throws IOException an I/O error occurred
   */
  protected abstract FsServerDefaults getServerDefaults() throws IOException; 

  /**
   * The specification of this method matches that of
   * {@link FileContext#create(Path, EnumSet, Options.CreateOpts...)} except
   * that the Path f must be fully qualified and the permission is absolute
   * (i.e. umask has been applied).
   */
  protected final FSDataOutputStream create(final Path f,
      final EnumSet<CreateFlag> createFlag, Options.CreateOpts... opts)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, UnresolvedLinkException, IOException {
    checkPath(f);
    int bufferSize = -1;
    short replication = -1;
    long blockSize = -1;
    int bytesPerChecksum = -1;
    FsPermission permission = null;
    Progressable progress = null;
    Boolean createParent = null;
 
    for (CreateOpts iOpt : opts) {
      if (CreateOpts.BlockSize.class.isInstance(iOpt)) {
        if (blockSize != -1) {
          throw new HadoopIllegalArgumentException(
              "BlockSize option is set multiple times");
        }
        blockSize = ((CreateOpts.BlockSize) iOpt).getValue();
      } else if (CreateOpts.BufferSize.class.isInstance(iOpt)) {
        if (bufferSize != -1) {
          throw new HadoopIllegalArgumentException(
              "BufferSize option is set multiple times");
        }
        bufferSize = ((CreateOpts.BufferSize) iOpt).getValue();
      } else if (CreateOpts.ReplicationFactor.class.isInstance(iOpt)) {
        if (replication != -1) {
          throw new HadoopIllegalArgumentException(
              "ReplicationFactor option is set multiple times");
        }
        replication = ((CreateOpts.ReplicationFactor) iOpt).getValue();
      } else if (CreateOpts.BytesPerChecksum.class.isInstance(iOpt)) {
        if (bytesPerChecksum != -1) {
          throw new HadoopIllegalArgumentException(
              "BytesPerChecksum option is set multiple times");
        }
        bytesPerChecksum = ((CreateOpts.BytesPerChecksum) iOpt).getValue();
      } else if (CreateOpts.Perms.class.isInstance(iOpt)) {
        if (permission != null) {
          throw new HadoopIllegalArgumentException(
              "Perms option is set multiple times");
        }
        permission = ((CreateOpts.Perms) iOpt).getValue();
      } else if (CreateOpts.Progress.class.isInstance(iOpt)) {
        if (progress != null) {
          throw new HadoopIllegalArgumentException(
              "Progress option is set multiple times");
        }
        progress = ((CreateOpts.Progress) iOpt).getValue();
      } else if (CreateOpts.CreateParent.class.isInstance(iOpt)) {
        if (createParent != null) {
          throw new HadoopIllegalArgumentException(
              "CreateParent option is set multiple times");
        }
        createParent = ((CreateOpts.CreateParent) iOpt).getValue();
      } else {
        throw new HadoopIllegalArgumentException("Unkown CreateOpts of type " +
            iOpt.getClass().getName());
      }
    }
    if (permission == null) {
      throw new HadoopIllegalArgumentException("no permission supplied");
    }


    FsServerDefaults ssDef = getServerDefaults();
    if (ssDef.getBlockSize() % ssDef.getBytesPerChecksum() != 0) {
      throw new IOException("Internal error: default blockSize is" + 
          " not a multiple of default bytesPerChecksum ");
    }
    
    if (blockSize == -1) {
      blockSize = ssDef.getBlockSize();
    }
    if (bytesPerChecksum == -1) {
      bytesPerChecksum = ssDef.getBytesPerChecksum();
    }
    if (bufferSize == -1) {
      bufferSize = ssDef.getFileBufferSize();
    }
    if (replication == -1) {
      replication = ssDef.getReplication();
    }
    if (createParent == null) {
      createParent = false;
    }

    if (blockSize % bytesPerChecksum != 0) {
      throw new HadoopIllegalArgumentException(
             "blockSize should be a multiple of checksumsize");
    }

    return this.createInternal(f, createFlag, permission, bufferSize,
      replication, blockSize, progress, bytesPerChecksum, createParent);
  }

  /**
   * The specification of this method matches that of
   * {@link #create(Path, EnumSet, Options.CreateOpts...)} except that the opts
   * have been declared explicitly.
   */
  protected abstract FSDataOutputStream createInternal(Path f,
      EnumSet<CreateFlag> flag, FsPermission absolutePermission,
      int bufferSize, short replication, long blockSize, Progressable progress,
      int bytesPerChecksum, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#mkdir(Path, FsPermission, boolean)} except that the Path
   * f must be fully qualified and the permission is absolute (i.e. 
   * umask has been applied).
   */
  protected abstract void mkdir(final Path dir, final FsPermission permission,
      final boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#delete(Path, boolean)} except that Path f must be for
   * this file system.
   */
  protected abstract boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path)} except that Path f must be for this
   * file system.
   */
  protected FSDataInputStream open(final Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    return open(f, getServerDefaults().getFileBufferSize());
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path, int)} except that Path f must be for this
   * file system.
   */
  protected abstract FSDataInputStream open(final Path f, int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setReplication(Path, short)} except that Path f must be
   * for this file system.
   */
  protected abstract boolean setReplication(final Path f,
      final short replication) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this file system.
   */
  protected final void rename(final Path src, final Path dst,
      final Options.Rename... options) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    renameInternal(src, dst, overwrite);
  }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this file system and NO OVERWRITE is performed.
   * 
   * File systems that do not have a built in overwrite need implement only this
   * method and can take advantage of the default impl of the other
   * {@link #renameInternal(Path, Path, boolean)}
   */
  protected abstract void renameInternal(final Path src, final Path dst)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnresolvedLinkException, IOException;
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this file system.
   */
  protected void renameInternal(final Path src, final Path dst,
      boolean overwrite) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    // Default implementation deals with overwrite in a non-atomic way
    final FileStatus srcStatus = getFileLinkStatus(src);

    FileStatus dstStatus;
    try {
      dstStatus = getFileLinkStatus(dst);
    } catch (IOException e) {
      dstStatus = null;
    }
    if (dstStatus != null) {
      if (dst.equals(src)) {
        throw new FileAlreadyExistsException(
            "The source "+src+" and destination "+dst+" are the same");
      }
      if (srcStatus.isSymlink() && dst.equals(srcStatus.getSymlink())) {
        throw new FileAlreadyExistsException(
            "Cannot rename symlink "+src+" to its target "+dst);
      }
      // It's OK to rename a file to a symlink and vice versa
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new IOException("Source " + src + " and destination " + dst
            + " must both be directories");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException("Rename destination " + dst
            + " already exists.");
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDirectory()) {
        Iterator<FileStatus> list = listStatusIterator(dst);
        if (list != null && list.hasNext()) {
          throw new IOException(
              "Rename cannot overwrite non empty destination directory " + dst);
        }
      }
      delete(dst, false);
    } else {
      final Path parent = dst.getParent();
      final FileStatus parentStatus = getFileStatus(parent);
      if (parentStatus.isFile()) {
        throw new ParentNotDirectoryException("Rename destination parent "
            + parent + " is a file.");
      }
    }
    renameInternal(src, dst);
  }
  
  /**
   * Returns true if the file system supports symlinks, false otherwise.
   */
  protected boolean supportsSymlinks() {
    return false;
  }
  
  /**
   * The specification of this method matches that of  
   * {@link FileContext#createSymlink(Path, Path, boolean)};
   */
  protected void createSymlink(final Path target, final Path link,
      final boolean createParent) throws IOException, UnresolvedLinkException {
    throw new IOException("File system does not support symlinks");    
  }

  /**
   * The specification of this method matches that of  
   * {@link FileContext#getLinkTarget(Path)};
   */
  protected Path getLinkTarget(final Path f) throws IOException {
    /* We should never get here. Any file system that threw an
     * UnresolvedLinkException, causing this function to be called,
     * needs to override this method.
     */
    throw new AssertionError();
  }
    
  /**
   * The specification of this method matches that of
   * {@link FileContext#setPermission(Path, FsPermission)} except that Path f
   * must be for this file system.
   */
  protected abstract void setPermission(final Path f,
      final FsPermission permission) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setOwner(Path, String, String)} except that Path f must
   * be for this file system.
   */
  protected abstract void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setTimes(Path, long, long)} except that Path f must be
   * for this file system.
   */
  protected abstract void setTimes(final Path f, final long mtime,
    final long atime) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileChecksum(Path)} except that Path f must be for
   * this file system.
   */
  protected abstract FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileStatus(Path)} 
   * except that an UnresolvedLinkException may be thrown if a symlink is 
   * encountered in the path.
   */
  protected abstract FileStatus getFileStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileLinkStatus(Path)}
   * except that an UnresolvedLinkException may be thrown if a symlink is  
   * encountered in the path leading up to the final path component.
   * If the file system does not support symlinks then the behavior is
   * equivalent to {@link AbstractFileSystem#getFileStatus(Path)}.
   */
  protected FileStatus getFileLinkStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return getFileStatus(f);
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileBlockLocations(Path, long, long)} except that
   * Path f must be for this file system.
   */
  protected abstract BlockLocation[] getFileBlockLocations(final Path f,
      final long start, final long len) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFsStatus(Path)} except that Path f must be for this
   * file system.
   */
  protected FsStatus getFsStatus(final Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    // default impl gets FsStatus of root
    return getFsStatus();
  }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#getFsStatus(Path)}.
   */
  protected abstract FsStatus getFsStatus() throws AccessControlException,
      FileNotFoundException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#listStatus(Path)} except that Path f must be for this
   * file system.
   */
  protected Iterator<FileStatus> listStatusIterator(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return new Iterator<FileStatus>() {
      private int i = 0;
      private FileStatus[] statusList = listStatus(f);
      
      @Override
      public boolean hasNext() {
        return i < statusList.length;
      }
      
      @Override
      public FileStatus next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return statusList[i++];
      }
      
      @Override
      public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
      }
    };
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext.Util#listStatus(Path)} except that Path f must be 
   * for this file system.
   */
  protected abstract FileStatus[] listStatus(final Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setVerifyChecksum(boolean, Path)} except that Path f
   * must be for this file system.
   */
  protected abstract void setVerifyChecksum(final boolean verifyChecksum)
      throws AccessControlException, IOException;
}
