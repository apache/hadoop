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

package org.apache.hadoop.fs.s3native;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * Unlike {@link org.apache.hadoop.fs.s3.S3FileSystem} this implementation
 * stores files on S3 in their
 * native form so they can be read by other S3 tools.
 *
 * A note about directories. S3 of course has no "native" support for them.
 * The idiom we choose then is: for any directory created by this class,
 * we use an empty object "#{dirpath}_$folder$" as a marker.
 * Further, to interoperate with other S3 tools, we also accept the following:
 *  - an object "#{dirpath}/' denoting a directory marker
 *  - if there exists any objects with the prefix "#{dirpath}/", then the
 *    directory is said to exist
 *  - if both a file with the name of a directory and a marker for that
 *    directory exists, then the *file masks the directory*, and the directory
 *    is never returned.
 * </p>
 * @see org.apache.hadoop.fs.s3.S3FileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NativeS3FileSystem extends FileSystem {
  
  public static final Logger LOG =
      LoggerFactory.getLogger(NativeS3FileSystem.class);
  
  private static final String FOLDER_SUFFIX = "_$folder$";
  static final String PATH_DELIMITER = Path.SEPARATOR;
  private static final int S3_MAX_LISTING_LENGTH = 1000;
  
  static class NativeS3FsInputStream extends FSInputStream {
    
    private NativeFileSystemStore store;
    private Statistics statistics;
    private InputStream in;
    private final String key;
    private long pos = 0;
    
    public NativeS3FsInputStream(NativeFileSystemStore store, Statistics statistics, InputStream in, String key) {
      Preconditions.checkNotNull(in, "Null input stream");
      this.store = store;
      this.statistics = statistics;
      this.in = in;
      this.key = key;
    }
    
    @Override
    public synchronized int read() throws IOException {
      int result;
      try {
        result = in.read();
      } catch (IOException e) {
        LOG.info("Received IOException while reading '{}', attempting to reopen",
            key);
        LOG.debug("{}", e, e);
        try {
          seek(pos);
          result = in.read();
        } catch (EOFException eof) {
          LOG.debug("EOF on input stream read: {}", eof, eof);
          result = -1;
        }
      } 
      if (result != -1) {
        pos++;
      }
      if (statistics != null && result != -1) {
        statistics.incrementBytesRead(1);
      }
      return result;
    }
    @Override
    public synchronized int read(byte[] b, int off, int len)
      throws IOException {
      if (in == null) {
        throw new EOFException("Cannot read closed stream");
      }
      int result = -1;
      try {
        result = in.read(b, off, len);
      } catch (EOFException eof) {
        throw eof;
      } catch (IOException e) {
        LOG.info( "Received IOException while reading '{}'," +
                  " attempting to reopen.", key);
        seek(pos);
        result = in.read(b, off, len);
      }
      if (result > 0) {
        pos += result;
      }
      if (statistics != null && result > 0) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }

    @Override
    public synchronized void close() throws IOException {
      closeInnerStream();
    }

    /**
     * Close the inner stream if not null. Even if an exception
     * is raised during the close, the field is set to null
     * @throws IOException if raised by the close() operation.
     */
    private void closeInnerStream() throws IOException {
      if (in != null) {
        try {
          in.close();
        } finally {
          in = null;
        }
      }
    }

    /**
     * Update inner stream with a new stream and position
     * @param newStream new stream -must not be null
     * @param newpos new position
     * @throws IOException IO exception on a failure to close the existing
     * stream.
     */
    private synchronized void updateInnerStream(InputStream newStream, long newpos) throws IOException {
      Preconditions.checkNotNull(newStream, "Null newstream argument");
      closeInnerStream();
      in = newStream;
      this.pos = newpos;
    }

    @Override
    public synchronized void seek(long newpos) throws IOException {
      if (newpos < 0) {
        throw new EOFException(
            FSExceptionMessages.NEGATIVE_SEEK);
      }
      if (pos != newpos) {
        // the seek is attempting to move the current position
        LOG.debug("Opening key '{}' for reading at position '{}", key, newpos);
        InputStream newStream = store.retrieve(key, newpos);
        updateInnerStream(newStream, newpos);
      }
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }
  
  private class NativeS3FsOutputStream extends OutputStream {
    
    private Configuration conf;
    private String key;
    private File backupFile;
    private OutputStream backupStream;
    private MessageDigest digest;
    private boolean closed;
    private LocalDirAllocator lDirAlloc;
    
    public NativeS3FsOutputStream(Configuration conf,
        NativeFileSystemStore store, String key, Progressable progress,
        int bufferSize) throws IOException {
      this.conf = conf;
      this.key = key;
      this.backupFile = newBackupFile();
      LOG.info("OutputStream for key '" + key + "' writing to tempfile '" + this.backupFile + "'");
      try {
        this.digest = MessageDigest.getInstance("MD5");
        this.backupStream = new BufferedOutputStream(new DigestOutputStream(
            new FileOutputStream(backupFile), this.digest));
      } catch (NoSuchAlgorithmException e) {
        LOG.warn("Cannot load MD5 digest algorithm," +
            "skipping message integrity check.", e);
        this.backupStream = new BufferedOutputStream(
            new FileOutputStream(backupFile));
      }
    }

    private File newBackupFile() throws IOException {
      if (lDirAlloc == null) {
        lDirAlloc = new LocalDirAllocator("fs.s3.buffer.dir");
      }
      File result = lDirAlloc.createTmpFileForWrite("output-", LocalDirAllocator.SIZE_UNKNOWN, conf);
      result.deleteOnExit();
      return result;
    }
    
    @Override
    public void flush() throws IOException {
      backupStream.flush();
    }
    
    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }

      backupStream.close();
      LOG.info("OutputStream for key '{}' closed. Now beginning upload", key);
      
      try {
        byte[] md5Hash = digest == null ? null : digest.digest();
        store.storeFile(key, backupFile, md5Hash);
      } finally {
        if (!backupFile.delete()) {
          LOG.warn("Could not delete temporary s3n file: " + backupFile);
        }
        super.close();
        closed = true;
      } 
      LOG.info("OutputStream for key '{}' upload complete", key);
    }

    @Override
    public void write(int b) throws IOException {
      backupStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      backupStream.write(b, off, len);
    }
  }
  
  private URI uri;
  private NativeFileSystemStore store;
  private Path workingDir;
  
  public NativeS3FileSystem() {
    // set store in initialize()
  }
  
  public NativeS3FileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>s3n</code>
   */
  @Override
  public String getScheme() {
    return "s3n";
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir =
      new Path("/user", System.getProperty("user.name")).makeQualified(this.uri, this.getWorkingDirectory());
  }
  
  private static NativeFileSystemStore createDefaultStore(Configuration conf) {
    NativeFileSystemStore store = new Jets3tNativeFileSystemStore();
    
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        conf.getInt("fs.s3.maxRetries", 4),
        conf.getLong("fs.s3.sleepTimeSeconds", 10), TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(S3Exception.class, basePolicy);
    
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap =
      new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("storeFile", methodPolicy);
    methodNameToPolicyMap.put("rename", methodPolicy);
    
    return (NativeFileSystemStore)
      RetryProxy.create(NativeFileSystemStore.class, store,
          methodNameToPolicyMap);
  }
  
  private static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root,
      // like s3n://mybucket
      return "";
    }
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    String ret = path.toUri().getPath().substring(1); // remove initial slash
    if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
      ret = ret.substring(0, ret.length() -1);
  }
    return ret;
  }
  
  private static Path keyToPath(String key) {
    return new Path("/" + key);
  }
  
  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }
  
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    if (exists(f) && !overwrite) {
      throw new FileAlreadyExistsException("File already exists: " + f);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Creating new file '" + f + "' in S3");
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    return new FSDataOutputStream(new NativeS3FsOutputStream(getConf(), store,
        key, progress, bufferSize), statistics);
  }
  
  @Override
  public boolean delete(Path f, boolean recurse) throws IOException {
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Delete called for '" + f +
            "' but file does not exist, so returning false");
      }
      return false;
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (status.isDirectory()) {
      if (!recurse && listStatus(f).length > 0) {
        throw new IOException("Can not delete " + f + " as is a not empty directory and recurse option is false");
      }

      createParent(f);

      if(LOG.isDebugEnabled()) {
        LOG.debug("Deleting directory '" + f  + "'");
      }
      String priorLastKey = null;
      do {
        PartialListing listing = store.list(key, S3_MAX_LISTING_LENGTH, priorLastKey, true);
        for (FileMetadata file : listing.getFiles()) {
          store.delete(file.getKey());
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);

      try {
        store.delete(key + FOLDER_SUFFIX);
      } catch (FileNotFoundException e) {
        //this is fine, we don't require a marker
      }
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Deleting file '" + f + "'");
      }
      createParent(f);
      store.delete(key);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    
    if (key.length() == 0) { // root always exists
      return newDirectory(absolutePath);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus retrieving metadata for key '" + key + "'");
    }
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'file' for key '" + key + "'");
      }
      return newFile(meta, absolutePath);
    }
    if (store.retrieveMetadata(key + FOLDER_SUFFIX) != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'directory' for key '" + key +
            "' as '" + key + FOLDER_SUFFIX + "' exists");
      }
      return newDirectory(absolutePath);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus listing key '" + key + "'");
    }
    PartialListing listing = store.list(key, 1);
    if (listing.getFiles().length > 0 ||
        listing.getCommonPrefixes().length > 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'directory' for key '" + key +
            "' as it has contents");
      }
      return newDirectory(absolutePath);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus could not find key '" + key + "'");
    }
    throw new FileNotFoundException("No such file or directory '" + absolutePath + "'");
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * <p>
   * If <code>f</code> is a file, this method will make a single call to S3.
   * If <code>f</code> is a directory, this method will make a maximum of
   * (<i>n</i> / 1000) + 2 calls to S3, where <i>n</i> is the total number of
   * files and directories contained directly in <code>f</code>.
   * </p>
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    
    if (key.length() > 0) {
      FileMetadata meta = store.retrieveMetadata(key);
      if (meta != null) {
        return new FileStatus[] { newFile(meta, absolutePath) };
      }
    }
    
    URI pathUri = absolutePath.toUri();
    Set<FileStatus> status = new TreeSet<FileStatus>();
    String priorLastKey = null;
    do {
      PartialListing listing = store.list(key, S3_MAX_LISTING_LENGTH, priorLastKey, false);
      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();

        if (fileMetadata.getKey().equals(key + "/")) {
          // this is just the directory we have been asked to list
        }
        else if (relativePath.endsWith(FOLDER_SUFFIX)) {
          status.add(newDirectory(new Path(
              absolutePath,
              relativePath.substring(0, relativePath.indexOf(FOLDER_SUFFIX)))));
        }
        else {
          status.add(newFile(fileMetadata, subpath));
        }
      }
      for (String commonPrefix : listing.getCommonPrefixes()) {
        Path subpath = keyToPath(commonPrefix);
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();
        status.add(newDirectory(new Path(absolutePath, relativePath)));
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);
    
    if (status.isEmpty() &&
        key.length() > 0 &&
        store.retrieveMetadata(key + FOLDER_SUFFIX) == null) {
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
    
    return status.toArray(new FileStatus[status.size()]);
  }
  
  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus(meta.getLength(), false, 1, getDefaultBlockSize(),
        meta.getLastModified(), path.makeQualified(this.getUri(), this.getWorkingDirectory()));
  }
  
  private FileStatus newDirectory(Path path) {
    return new FileStatus(0, true, 1, 0, 0, path.makeQualified(this.getUri(), this.getWorkingDirectory()));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(f);
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);
    
    boolean result = true;
    for (Path path : paths) {
      result &= mkdir(path);
    }
    return result;
  }
  
  private boolean mkdir(Path f) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(f);
      if (fileStatus.isFile()) {
        throw new FileAlreadyExistsException(String.format(
            "Can't make directory for path '%s' since it is a file.", f));

      }
    } catch (FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Making dir '" + f + "' in S3");
      }
      String key = pathToKey(f) + FOLDER_SUFFIX;
      store.storeEmptyFile(key);    
    }
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    FileStatus fs = getFileStatus(f); // will throw if the file doesn't exist
    if (fs.isDirectory()) {
      throw new FileNotFoundException("'" + f + "' is a directory");
    }
    LOG.info("Opening '" + f + "' for reading");
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeS3FsInputStream(store, statistics, store.retrieve(key), key), bufferSize));
  }
  
  // rename() and delete() use this method to ensure that the parent directory
  // of the source does not vanish.
  private void createParent(Path path) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      String key = pathToKey(makeAbsolute(parent));
      if (key.length() > 0) {
          store.storeEmptyFile(key + FOLDER_SUFFIX);
      }
    }
  }
  
    
  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    String srcKey = pathToKey(makeAbsolute(src));

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";

    // Figure out the final destination
    String dstKey;
    try {
      boolean dstIsFile = getFileStatus(dst).isFile();
      if (dstIsFile) {
        if(LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble +
              "returning false as dst is an already existing file");
        }
        return false;
      } else {
        if(LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble + "using dst as output directory");
        }
        dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      }
    } catch (FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "using dst as output destination");
      }
      dstKey = pathToKey(makeAbsolute(dst));
      try {
        if (getFileStatus(dst.getParent()).isFile()) {
          if(LOG.isDebugEnabled()) {
            LOG.debug(debugPreamble +
                "returning false as dst parent exists and is a file");
          }
          return false;
        }
      } catch (FileNotFoundException ex) {
        if(LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble +
              "returning false as dst parent does not exist");
        }
        return false;
      }
    }

    boolean srcIsFile;
    try {
      srcIsFile = getFileStatus(src).isFile();
    } catch (FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "returning false as src does not exist");
      }
      return false;
    }
    if (srcIsFile) {
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
            "src is file, so doing copy then delete in S3");
      }
      store.copy(srcKey, dstKey);
      store.delete(srcKey);
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "src is directory, so copying contents");
      }
      store.storeEmptyFile(dstKey + FOLDER_SUFFIX);

      List<String> keysToDelete = new ArrayList<String>();
      String priorLastKey = null;
      do {
        PartialListing listing = store.list(srcKey, S3_MAX_LISTING_LENGTH, priorLastKey, true);
        for (FileMetadata file : listing.getFiles()) {
          keysToDelete.add(file.getKey());
          store.copy(file.getKey(), dstKey + file.getKey().substring(srcKey.length()));
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);

      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
            "all files in src copied, now removing src files");
      }
      for (String key: keysToDelete) {
        store.delete(key);
      }

      try {
        store.delete(srcKey + FOLDER_SUFFIX);
      } catch (FileNotFoundException e) {
        //this is fine, we don't require a marker
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "done");
      }
    }

    return true;
  }
  
  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong("fs.s3n.block.size", 64 * 1024 * 1024);
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }
}
