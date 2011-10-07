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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * Unlike {@link org.apache.hadoop.fs.s3.S3FileSystem} this implementation
 * stores files on S3 in their
 * native form so they can be read by other S3 tools.
 * </p>
 * @see org.apache.hadoop.fs.s3.S3FileSystem
 */
public class NativeS3FileSystem extends FileSystem {
  
  public static final Log LOG = 
    LogFactory.getLog(NativeS3FileSystem.class);
  
  private static final String FOLDER_SUFFIX = "_$folder$";
  private static final long MAX_S3_FILE_SIZE = 5 * 1024 * 1024 * 1024L;
  static final String PATH_DELIMITER = Path.SEPARATOR;
  private static final int S3_MAX_LISTING_LENGTH = 1000;
  
  private class NativeS3FsInputStream extends FSInputStream {
    
    private InputStream in;
    private final String key;
    private long pos = 0;
    
    public NativeS3FsInputStream(InputStream in, String key) {
      this.in = in;
      this.key = key;
    }
    
    public synchronized int read() throws IOException {
      int result = in.read();
      if (result != -1) {
        pos++;
      }
      return result;
    }
    public synchronized int read(byte[] b, int off, int len)
      throws IOException {
      
      int result = in.read(b, off, len);
      if (result > 0) {
        pos += result;
      }
      return result;
    }

    public void close() throws IOException {
      in.close();
    }

    public synchronized void seek(long pos) throws IOException {
      in.close();
      in = store.retrieve(key, pos);
      this.pos = pos;
    }
    public synchronized long getPos() throws IOException {
      return pos;
    }
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
    
    public NativeS3FsOutputStream(Configuration conf,
        NativeFileSystemStore store, String key, Progressable progress,
        int bufferSize) throws IOException {
      this.conf = conf;
      this.key = key;
      this.backupFile = newBackupFile();
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
      File dir = new File(conf.get("fs.s3.buffer.dir"));
      if (!dir.mkdirs() && !dir.exists()) {
        throw new IOException("Cannot create S3 buffer directory: " + dir);
      }
      File result = File.createTempFile("output-", ".tmp", dir);
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
      new Path("/user", System.getProperty("user.name")).makeQualified(this);
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
    
    return (NativeFileSystemStore)
      RetryProxy.create(NativeFileSystemStore.class, store,
          methodNameToPolicyMap);
  }
  
  private static String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return path.toUri().getPath().substring(1); // remove initial slash
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
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }
  
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    if (exists(f) && !overwrite) {
      throw new IOException("File already exists:"+f);
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    return new FSDataOutputStream(new NativeS3FsOutputStream(getConf(), store,
        key, progress, bufferSize), statistics);
  }
  
  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      return false;
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (status.isDir()) {
      FileStatus[] contents = listStatus(f);
      if (!recursive && contents.length > 0) {
        throw new IOException("Directory " + f.toString() + " is not empty.");
      }
      for (FileStatus p : contents) {
        if (!delete(p.getPath(), recursive)) {
          return false;
        }
      }
      store.delete(key + FOLDER_SUFFIX);
    } else {
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
    
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      return newFile(meta, absolutePath);
    }
    if (store.retrieveMetadata(key + FOLDER_SUFFIX) != null) {
      return newDirectory(absolutePath);
    }
    
    PartialListing listing = store.list(key, 1);
    if (listing.getFiles().length > 0 ||
        listing.getCommonPrefixes().length > 0) {
      return newDirectory(absolutePath);
    }
    
    throw new FileNotFoundException(absolutePath +
        ": No such file or directory.");
    
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
      PartialListing listing = store.list(key, S3_MAX_LISTING_LENGTH, 
          priorLastKey);
      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();
        if (relativePath.endsWith(FOLDER_SUFFIX)) {
          status.add(newDirectory(new Path(absolutePath,
              relativePath.substring(0,
                  relativePath.indexOf(FOLDER_SUFFIX)))));
        } else {
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
        store.retrieveMetadata(key + FOLDER_SUFFIX) == null) {
      return null;
    }
    
    return status.toArray(new FileStatus[0]);
  }
  
  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus(meta.getLength(), false, 1, MAX_S3_FILE_SIZE,
        meta.getLastModified(), path.makeQualified(this));
  }
  
  private FileStatus newDirectory(Path path) {
    return new FileStatus(0, true, 1, MAX_S3_FILE_SIZE, 0,
        path.makeQualified(this));
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
      if (!fileStatus.isDir()) {
        throw new IOException(String.format(
            "Can't make directory for path %s since it is a file.", f));

      }
    } catch (FileNotFoundException e) {
      String key = pathToKey(f) + FOLDER_SUFFIX;
      store.storeEmptyFile(key);    
    }
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeS3FsInputStream(store.retrieve(key), key), bufferSize));
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
  
  private boolean existsAndIsFile(Path f) throws IOException {
    
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    
    if (key.length() == 0) {
        return false;
    }
    
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
        // S3 object with given key exists, so this is a file
        return true;
    }
    
    if (store.retrieveMetadata(key + FOLDER_SUFFIX) != null) {
        // Signifies empty directory
        return false;
    }
    
    PartialListing listing = store.list(key, 1, null);
    if (listing.getFiles().length > 0 ||
        listing.getCommonPrefixes().length > 0) {
        // Non-empty directory
        return false;
    }
    
    throw new FileNotFoundException(absolutePath +
        ": No such file or directory");
}


  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    String srcKey = pathToKey(makeAbsolute(src));

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    // Figure out the final destination
    String dstKey;
    try {
      boolean dstIsFile = existsAndIsFile(dst);
      if (dstIsFile) {
        // Attempting to overwrite a file using rename()
        return false;
      } else {
        // Move to within the existent directory
        dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      }
    } catch (FileNotFoundException e) {
      // dst doesn't exist, so we can proceed
      dstKey = pathToKey(makeAbsolute(dst));
      try {
        if (!getFileStatus(dst.getParent()).isDir()) {
          return false; // parent dst is a file
        }
      } catch (FileNotFoundException ex) {
        return false; // parent dst does not exist
      }
    }

    try {
      boolean srcIsFile = existsAndIsFile(src);
      if (srcIsFile) {
        store.rename(srcKey, dstKey);
      } else {
        // Move the folder object
        store.delete(srcKey + FOLDER_SUFFIX);
        store.storeEmptyFile(dstKey + FOLDER_SUFFIX);

        // Move everything inside the folder
        String priorLastKey = null;
        do {
          PartialListing listing = store.listAll(srcKey, S3_MAX_LISTING_LENGTH,
              priorLastKey);
          for (FileMetadata file : listing.getFiles()) {
            store.rename(file.getKey(), dstKey
                + file.getKey().substring(srcKey.length()));
          }
          priorLastKey = listing.getPriorLastKey();
        } while (priorLastKey != null);
      }

      createParent(src);
      return true;

    } catch (FileNotFoundException e) {
      // Source file does not exist;
      return false;
    }
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

}
