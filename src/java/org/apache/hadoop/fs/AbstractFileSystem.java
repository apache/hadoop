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
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * This class provides an interface for implementors of a Hadoop filesystem
 * (analogous to the VFS of Unix). Applications do not access this class;
 * instead they access files across all filesystems using {@link FileContext}.
 * 
 * Pathnames passed to AbstractFileSystem can be fully qualified URI that
 * matches the "this" filesystem (ie same scheme and authority) 
 * or a Slash-relative name that is assumed to be relative
 * to the root of the "this" filesystem .
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class AbstractFileSystem {
  static final Log LOG = LogFactory.getLog(AbstractFileSystem.class);

  /** Recording statistics per a filesystem class. */
  private static final Map<Class<? extends AbstractFileSystem>, Statistics> 
  STATISTICS_TABLE =
      new IdentityHashMap<Class<? extends AbstractFileSystem>, Statistics>();
  
  /** Cache of constructors for each filesystem class. */
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
   * Prohibits names which contain a ".", "..". ":" or "/" 
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
   * Create a file system instance for the specified uri using the conf.
   * The conf is used to find the class name that implements the filesystem.
   * The conf is also passed to the filesystem for its configuration.
   * @param uri
   * @param conf
   * @return
   * @throws IOException
   */
  private static AbstractFileSystem createFileSystem(URI uri,
    Configuration conf) throws IOException {
    Class<?> clazz = conf.getClass("fs.AbstractFileSystem." + 
                                uri.getScheme() + ".impl", null);
    if (clazz == null) {
      throw new IOException("No AbstractFileSystem for scheme: "
          + uri.getScheme());
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

  protected static synchronized void printStatistics() throws IOException {
    for (Map.Entry<Class<? extends AbstractFileSystem>, Statistics> pair: 
            STATISTICS_TABLE.entrySet()) {
      System.out.println("  FileSystem " + pair.getKey().getName() + 
                         ": " + pair.getValue());
    }
  }

  
  /**
   * The main factory method for creating a filesystem.
   * Get a filesystem for the URI's scheme and authority.
   * The scheme of the URI determines a configuration property name,
   * <tt>fs.AbstractFileSystem.<i>scheme</i>.impl</tt> whose value names
   * the AbstractFileSystem class. 
   * The entire URI and conf is passed to the AbstractFileSystem factory
   * method.
   * @param uri for the file system to be created.
   * @param conf which is passed to the filesystem impl.
   */
  static AbstractFileSystem get(final URI uri, final Configuration conf)
    throws IOException {
    return createFileSystem(uri, conf);
  }

  /**
   * Constructor to be called by subclasses.
   * 
   * @param uri for this file system.
   * @param supportedScheme the scheme supported by the implementor
   * @param authorityNeeded if true then theURI must have authority, if false
   *          then the URI must have null authority.
   * @throws URISyntaxException
   */
  protected AbstractFileSystem(final URI uri, final String supportedScheme,
      final boolean authorityNeeded, final int defaultPort) throws URISyntaxException {
    myUri = getUri(uri, supportedScheme, authorityNeeded, defaultPort);
    statistics = getStatistics(supportedScheme, getClass()); 
  }
  
  protected void checkScheme(URI uri, String supportedScheme) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("Uri without scheme: " + uri);
    }
    if (!scheme.equals(supportedScheme)) {
      throw new IllegalArgumentException("Uri scheme " + uri
          + " does not match the scheme " + supportedScheme);
    }
  }

  /**
   * Get the URI for the file system based on the given URI. The path, query
   * part of the given URI is stripped out and default filesystem port is used
   * to form the URI.
   * 
   * @param uri FileSystem URI.
   * @param authorityNeeded if true authority cannot be null in the URI. If
   *          false authority must be null.
   * @param defaultPort default port to use if port is not specified in the URI.
   * @return URI of the file system
   * @throws URISyntaxException 
   */
  private URI getUri(URI uri, String supportedScheme,
      boolean authorityNeeded, int defaultPort) throws URISyntaxException {
    checkScheme(uri, supportedScheme);
    // A filesystem implementation that requires authority must always
    // specify default port
    if (defaultPort < 0 && authorityNeeded) {
      throw new IllegalArgumentException(
          "FileSystem implementation error -  default port " + defaultPort
              + " is not valid");
    }
    String authority = uri.getAuthority();
    if (!authorityNeeded) {
      if (authority != null) {
        throw new IllegalArgumentException("Scheme with non-null authority: "
            + uri);
      }
      return new URI(supportedScheme + ":///");
    }
    if (authority == null) {
      throw new IllegalArgumentException("Uri without authority: " + uri);
    }
    int port = uri.getPort();
    port = port == -1 ? defaultPort : port;
    return new URI(supportedScheme + "://" + uri.getHost() + ":" + port);
  }
  
  /**
   * The default port of this filesystem.
   * @return default port of this filesystem's Uri scheme
   * A uri with a port of -1 => default port;
   */
  protected abstract int getUriDefaultPort();

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   * @return the uri of this filesystem.
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
        throw new IllegalArgumentException("relative paths not allowed:" + 
            path);
      } else {
        throw new IllegalArgumentException(
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
      throw new IllegalArgumentException("Wrong FS: " + path + 
                                    ", expected: "+this.getUri());
    }
    
    int thisPort = this.getUri().getPort();
    int thatPort = path.toUri().getPort();
    if (thatPort == -1) { // -1 => defaultPort of Uri scheme
      thatPort = this.getUriDefaultPort();
    }
    if (thisPort != thatPort) {
      throw new IllegalArgumentException("Wrong FS: "+path+
                                       ", expected: "+this.getUri());
    }
  }
  
  /**
   * Get the path-part of a pathname. Checks that URI matches this filesystem
   * and that the path-part is a valid name.
   * @param p
   * @return path-part of the Path p
   */
  protected String getUriPath(final Path p) {
    checkPath(p);
    String s = p.toUri().getPath();
    if (!isValidName(s)) {
      throw new IllegalArgumentException("Path part " + s + " from URI" +
          p + " is not a valid filename.");
    }
    return s;
  }
  
  /**
   * Some file systems like LocalFileSystem have an initial workingDir
   * that we use as the starting workingDir. For other file systems
   * like HDFS there is no built in notion of an initial workingDir.
   * 
   * @return the initial workingDir if the filesystem if it has such a notion
   * otherwise return a null.
   */
  protected Path getInitialWorkingDirectory() {
    return null;
  }
  
  /** 
   * Return the current user's home directory in this filesystem.
   * The default implementation returns "/user/$USER/".
   */
  protected Path getHomeDirectory() {
    return new Path("/user/"+System.getProperty("user.name")).makeQualified(
                                                                getUri(), null);
  }
  
  /**
   * Return a set of server default configuration values.
   * @return server default configuration values
   * @throws IOException
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
    throws IOException {
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
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        blockSize = ((CreateOpts.BlockSize) iOpt).getValue();
      } else if (CreateOpts.BufferSize.class.isInstance(iOpt)) {
        if (bufferSize != -1) {
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        bufferSize = ((CreateOpts.BufferSize) iOpt).getValue();
      } else if (CreateOpts.ReplicationFactor.class.isInstance(iOpt)) {
        if (replication != -1) {
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        replication = ((CreateOpts.ReplicationFactor) iOpt).getValue();
      } else if (CreateOpts.BytesPerChecksum.class.isInstance(iOpt)) {
        if (bytesPerChecksum != -1) {
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        bytesPerChecksum = ((CreateOpts.BytesPerChecksum) iOpt).getValue();
      } else if (CreateOpts.Perms.class.isInstance(iOpt)) {
        if (permission != null) {
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        permission = ((CreateOpts.Perms) iOpt).getValue();
      } else if (CreateOpts.Progress.class.isInstance(iOpt)) {
        if (progress != null) {
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        progress = ((CreateOpts.Progress) iOpt).getValue();
      } else if (CreateOpts.CreateParent.class.isInstance(iOpt)) {
        if (createParent != null) {
          throw new IllegalArgumentException("multiple varargs of same kind");
        }
        createParent = ((CreateOpts.CreateParent) iOpt).getValue();
      } else {
        throw new IllegalArgumentException("Unkown CreateOpts of type " +
            iOpt.getClass().getName());
      }
    }
    if (permission == null) {
      throw new IllegalArgumentException("no permission supplied");
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
      throw new IllegalArgumentException(
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
      EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize,
      short replication, long blockSize, Progressable progress,
      int bytesPerChecksum, boolean createParent) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#mkdir(Path, FsPermission, boolean)} except that the Path
   * f must be fully qualified and the permission is absolute (ie umask has been
   * applied).
   */
  protected abstract void mkdir(final Path dir,
      final FsPermission permission, final boolean createParent)
    throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#delete(Path, boolean)} except that Path f must be for
   * this filesystem.
   */
  protected abstract boolean delete(final Path f, final boolean recursive)
    throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path)} except that Path f must be for this
   * filesystem.
   */
  protected FSDataInputStream open(final Path f) throws IOException {
    return open(f, getServerDefaults().getFileBufferSize());
  }

  /**
   * The specification of this method matches that of
   * {@link FileContext#open(Path, int)} except that Path f must be for this
   * filesystem.
   */
  protected abstract FSDataInputStream open(final Path f, int bufferSize)
    throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setReplication(Path, short)} except that Path f must be
   * for this filesystem.
   */
  protected abstract boolean setReplication(final Path f,
    final short replication) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this filesystem.
   */
  protected final void rename(final Path src, final Path dst,
    final Options.Rename... options) throws IOException {
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
   * f must be for this filesystem and NO OVERWRITE is performed.
   * 
   * Filesystems that do not have a built in overwrite need implement only this
   * method and can take advantage of the default impl of the other
   * {@link #renameInternal(Path, Path, boolean)}
   */
  protected abstract void renameInternal(final Path src, final Path dst)
    throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#rename(Path, Path, Options.Rename...)} except that Path
   * f must be for this filesystem.
   */
  protected void renameInternal(final Path src, final Path dst,
    boolean overwrite) throws IOException {
    // Default implementation deals with overwrite in a non-atomic way
    final FileStatus srcStatus = getFileStatus(src);
    if (srcStatus == null) {
      throw new FileNotFoundException("rename source " + src + " not found.");
    }

    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dst);
    } catch (IOException e) {
      dstStatus = null;
    }
    if (dstStatus != null) {
      if (srcStatus.isDir() != dstStatus.isDir()) {
        throw new IOException("Source " + src + " Destination " + dst
            + " both should be either file or directory");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException("rename destination " + dst
            + " already exists.");
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDir()) {
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
      if (!parentStatus.isDir()) {
        throw new ParentNotDirectoryException("rename destination parent "
            + parent + " is a file.");
      }
    }
    renameInternal(src, dst);
  }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#setPermission(Path, FsPermission)} except that Path f
   * must be for this filesystem.
   */
  protected abstract void setPermission(final Path f,
      final FsPermission permission) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setOwner(Path, String, String)} except that Path f must
   * be for this filesystem.
   */
  protected abstract void setOwner(final Path f, final String username,
      final String groupname) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setTimes(Path, long, long)} except that Path f must be
   * for this filesystem.
   */
  protected abstract void setTimes(final Path f, final long mtime,
    final long atime) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileChecksum(Path)} except that Path f must be for
   * this filesystem.
   */
  protected abstract FileChecksum getFileChecksum(final Path f)
    throws IOException;
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#setVerifyChecksum(boolean, Path)} except that Path f
   * must be for this filesystem.
   */
  protected abstract FileStatus getFileStatus(final Path f) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFileBlockLocations(Path, long, long)} except that
   * Path f must be for this filesystem.
   */
  protected abstract BlockLocation[] getFileBlockLocations(final Path f,
    final long start, final long len) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#getFsStatus(Path)} except that Path f must be for this
   * filesystem.
   */
  protected FsStatus getFsStatus(final Path f) throws IOException {
    // default impl gets FsStatus of root
    return getFsStatus();
  }
  
  /**
   * The specification of this method matches that of
   * {@link FileContext#getFsStatus(Path)} except that Path f must be for this
   * filesystem.
   */
  protected abstract FsStatus getFsStatus() throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#listStatus(Path)} except that Path f must be for this
   * filesystem.
   */
  protected abstract FileStatus[] listStatus(final Path f) throws IOException;

  /**
   * The specification of this method matches that of
   * {@link FileContext#setVerifyChecksum(boolean, Path)} except that Path f
   * must be for this filesystem.
   */
  protected abstract void setVerifyChecksum(final boolean verifyChecksum)
    throws IOException;
}
