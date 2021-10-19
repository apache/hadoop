/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;

/**
 * The core CosN Filesystem implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class CosNFileSystem extends FileSystem {
  static final Logger LOG = LoggerFactory.getLogger(CosNFileSystem.class);

  public static final String SCHEME = "cosn";
  public static final String PATH_DELIMITER = Path.SEPARATOR;

  private URI uri;
  private String bucket;
  private NativeFileSystemStore store;
  private Path workingDir;
  private String owner = "Unknown";
  private String group = "Unknown";

  private ExecutorService boundedIOThreadPool;
  private ExecutorService boundedCopyThreadPool;

  public CosNFileSystem() {
  }

  public CosNFileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>cosn</code>
   */
  @Override
  public String getScheme() {
    return CosNFileSystem.SCHEME;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.bucket = name.getHost();
    if (this.store == null) {
      this.store = createDefaultStore(conf);
    }
    this.store.initialize(name, conf);
    setConf(conf);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    this.workingDir = new Path("/user",
        System.getProperty("user.name")).makeQualified(
        this.uri,
        this.getWorkingDirectory());
    this.owner = getOwnerId();
    this.group = getGroupId();
    LOG.debug("owner:" + owner + ", group:" + group);

    BufferPool.getInstance().initialize(this.getConf());

    // initialize the thread pool
    int uploadThreadPoolSize = this.getConf().getInt(
        CosNConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
        CosNConfigKeys.DEFAULT_UPLOAD_THREAD_POOL_SIZE
    );
    int readAheadPoolSize = this.getConf().getInt(
        CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
        CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE
    );
    int ioThreadPoolSize = uploadThreadPoolSize + readAheadPoolSize / 3;
    long threadKeepAlive = this.getConf().getLong(
        CosNConfigKeys.THREAD_KEEP_ALIVE_TIME_KEY,
        CosNConfigKeys.DEFAULT_THREAD_KEEP_ALIVE_TIME
    );
    this.boundedIOThreadPool = BlockingThreadPoolExecutorService.newInstance(
        ioThreadPoolSize / 2, ioThreadPoolSize,
        threadKeepAlive, TimeUnit.SECONDS,
        "cos-transfer-thread-pool");
    int copyThreadPoolSize = this.getConf().getInt(
        CosNConfigKeys.COPY_THREAD_POOL_SIZE_KEY,
        CosNConfigKeys.DEFAULT_COPY_THREAD_POOL_SIZE
    );
    this.boundedCopyThreadPool = BlockingThreadPoolExecutorService.newInstance(
        CosNConfigKeys.DEFAULT_COPY_THREAD_POOL_SIZE, copyThreadPoolSize,
        60L, TimeUnit.SECONDS,
        "cos-copy-thread-pool");
  }

  private static NativeFileSystemStore createDefaultStore(Configuration conf) {
    NativeFileSystemStore store = new CosNativeFileSystemStore();
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        conf.getInt(CosNConfigKeys.COSN_MAX_RETRIES_KEY,
            CosNConfigKeys.DEFAULT_MAX_RETRIES),
        conf.getLong(CosNConfigKeys.COSN_RETRY_INTERVAL_KEY,
            CosNConfigKeys.DEFAULT_RETRY_INTERVAL),
        TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<>();

    exceptionToPolicyMap.put(IOException.class, basePolicy);
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL,
        exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<>();
    methodNameToPolicyMap.put("storeFile", methodPolicy);
    methodNameToPolicyMap.put("rename", methodPolicy);

    return (NativeFileSystemStore) RetryProxy.create(
        NativeFileSystemStore.class, store, methodNameToPolicyMap);
  }

  private String getOwnerId() {
    return System.getProperty("user.name");
  }

  private String getGroupId() {
    return System.getProperty("user.name");
  }

  private String getOwnerInfo(boolean getOwnerId) {
    String ownerInfoId = "";
    try {
      String userName = System.getProperty("user.name");
      String command = "id -u " + userName;
      if (!getOwnerId) {
        command = "id -g " + userName;
      }
      Process child = Runtime.getRuntime().exec(command);
      child.waitFor();

      // Get the input stream and read from it
      InputStream in = child.getInputStream();
      StringBuilder strBuffer = new StringBuilder();
      int c;
      while ((c = in.read()) != -1) {
        strBuffer.append((char) c);
      }
      in.close();
      ownerInfoId = strBuffer.toString();
    } catch (IOException | InterruptedException e) {
      LOG.error("Getting owner info occurs a exception", e);
    }
    return ownerInfoId;
  }

  private static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root,
      // like cosn://mybucket
      return "";
    }
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    String ret = path.toUri().getPath();
    if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
      ret = ret.substring(0, ret.length() - 1);
    }
    return ret;
  }

  private static Path keyToPath(String key) {
    if (!key.startsWith(PATH_DELIMITER)) {
      return new Path("/" + key);
    } else {
      return new Path(key);
    }
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * This optional operation is not yet supported.
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    FileStatus fileStatus;

    try {
      fileStatus = getFileStatus(f);
      if (fileStatus.isDirectory()) {
        throw new FileAlreadyExistsException(f + " is a directory");
      }
      if (!overwrite) {
        // path references a file and overwrite is disabled
        throw new FileAlreadyExistsException(f + " already exists");
      }

    } catch (FileNotFoundException e) {
      LOG.debug("Creating a new file: [{}] in COS.", f);
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    return new FSDataOutputStream(
        new CosNOutputStream(getConf(), store, key, blockSize,
            this.boundedIOThreadPool), statistics);
  }

  private boolean rejectRootDirectoryDelete(boolean isEmptyDir,
      boolean recursive) throws PathIOException {
    if (isEmptyDir) {
      return true;
    }
    if (recursive) {
      return false;
    } else {
      throw new PathIOException(this.bucket, "Can not delete root path");
    }
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    Path parent = f.getParent();
    if (null != parent) {
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }

    return create(f, permission, flags.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.debug("Ready to delete path: [{}]. recursive: [{}].", f, recursive);
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      LOG.debug("Ready to delete the file: [{}], but it does not exist.", f);
      return false;
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.compareToIgnoreCase("/") == 0) {
      FileStatus[] fileStatuses = listStatus(f);
      return this.rejectRootDirectoryDelete(
          fileStatuses.length == 0, recursive);
    }

    if (status.isDirectory()) {
      if (!key.endsWith(PATH_DELIMITER)) {
        key += PATH_DELIMITER;
      }
      if (!recursive && listStatus(f).length > 0) {
        String errMsg = String.format("Can not delete the directory: [%s], as"
            + " it is not empty and option recursive is false.", f);
        throw new IOException(errMsg);
      }

      createParent(f);

      String priorLastKey = null;
      do {
        PartialListing listing = store.list(
            key,
            Constants.COS_MAX_LISTING_LENGTH,
            priorLastKey,
            true);
        for (FileMetadata file : listing.getFiles()) {
          store.delete(file.getKey());
        }
        for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
          store.delete(commonPrefix.getKey());
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);
      try {
        store.delete(key);
      } catch (Exception e) {
        LOG.error("Deleting the COS key: [{}] occurs an exception.", key, e);
      }

    } else {
      LOG.debug("Delete the file: {}", f);
      createParent(f);
      store.delete(key);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    if (key.length() == 0) {
      // root always exists
      return newDirectory(absolutePath);
    }

    LOG.debug("Call the getFileStatus to obtain the metadata for "
        + "the file: [{}].", f);

    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      if (meta.isFile()) {
        LOG.debug("Path: [{}] is a file. COS key: [{}]", f, key);
        return newFile(meta, absolutePath);
      } else {
        LOG.debug("Path: [{}] is a dir. COS key: [{}]", f, key);
        return newDirectory(meta, absolutePath);
      }
    }

    if (!key.endsWith(PATH_DELIMITER)) {
      key += PATH_DELIMITER;
    }

    // Considering that the object store's directory is a common prefix in
    // the object key, it needs to check the existence of the path by listing
    // the COS key.
    LOG.debug("List COS key: [{}] to check the existence of the path.", key);
    PartialListing listing = store.list(key, 1);
    if (listing.getFiles().length > 0
        || listing.getCommonPrefixes().length > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Path: [{}] is a directory. COS key: [{}]", f, key);
      }
      return newDirectory(absolutePath);
    }

    throw new FileNotFoundException(
        "No such file or directory '" + absolutePath + "'");
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * <p>
   * If <code>f</code> is a file, this method will make a single call to COS.
   * If <code>f</code> is a directory,
   * this method will make a maximum of ( <i>n</i> / 199) + 2 calls to cos,
   * where <i>n</i> is the total number of files
   * and directories contained directly in <code>f</code>.
   * </p>
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    if (key.length() > 0) {
      FileStatus fileStatus = this.getFileStatus(f);
      if (fileStatus.isFile()) {
        return new FileStatus[]{fileStatus};
      }
    }

    if (!key.endsWith(PATH_DELIMITER)) {
      key += PATH_DELIMITER;
    }

    URI pathUri = absolutePath.toUri();
    Set<FileStatus> status = new TreeSet<>();
    String priorLastKey = null;
    do {
      PartialListing listing = store.list(
          key, Constants.COS_MAX_LISTING_LENGTH, priorLastKey, false);
      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subPath = keyToPath(fileMetadata.getKey());
        if (fileMetadata.getKey().equals(key)) {
          // this is just the directory we have been asked to list.
          LOG.debug("The file list contains the COS key [{}] to be listed.",
              key);
        } else {
          status.add(newFile(fileMetadata, subPath));
        }
      }

      for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
        Path subPath = keyToPath(commonPrefix.getKey());
        String relativePath = pathUri.relativize(subPath.toUri()).getPath();
        status.add(
            newDirectory(commonPrefix, new Path(absolutePath, relativePath)));
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);

    return status.toArray(new FileStatus[status.size()]);
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus(meta.getLength(), false, 1, getDefaultBlockSize(),
        meta.getLastModified(), 0, null, this.owner, this.group,
        path.makeQualified(this.getUri(), this.getWorkingDirectory()));
  }

  private FileStatus newDirectory(Path path) {
    return new FileStatus(0, true, 1, 0, 0, 0, null, this.owner, this.group,
        path.makeQualified(this.getUri(), this.getWorkingDirectory()));
  }

  private FileStatus newDirectory(FileMetadata meta, Path path) {
    if (meta == null) {
      return newDirectory(path);
    }
    return new FileStatus(0, true, 1, 0, meta.getLastModified(),
        0, null, this.owner, this.group,
        path.makeQualified(this.getUri(), this.getWorkingDirectory()));
  }

  /**
   * Validate the path from the bottom up.
   *
   * @param path The path to be validated
   * @throws FileAlreadyExistsException The specified path is an existing file
   * @throws IOException                Getting the file status of the
   *                                    specified path occurs
   *                                    an IOException.
   */
  private void validatePath(Path path) throws IOException {
    Path parent = path.getParent();
    do {
      try {
        FileStatus fileStatus = getFileStatus(parent);
        if (fileStatus.isDirectory()) {
          break;
        } else {
          throw new FileAlreadyExistsException(String.format(
              "Can't make directory for path '%s', it is a file.", parent));
        }
      } catch (FileNotFoundException e) {
        LOG.debug("The Path: [{}] does not exist.", path);
      }
      parent = parent.getParent();
    } while (parent != null);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(f);
      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      validatePath(f);
    }

    return mkDirRecursively(f, permission);
  }

  /**
   * Recursively create a directory.
   *
   * @param f          Absolute path to the directory.
   * @param permission Directory permissions. Permission does not work for
   *                   the CosN filesystem currently.
   * @return Return true if the creation was successful,  throw a IOException.
   * @throws IOException The specified path already exists or an error
   *                     creating the path.
   */
  public boolean mkDirRecursively(Path f, FsPermission permission)
      throws IOException {
    Path absolutePath = makeAbsolute(f);
    List<Path> paths = new ArrayList<>();
    do {
      paths.add(absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);

    for (Path path : paths) {
      if (path.equals(new Path(CosNFileSystem.PATH_DELIMITER))) {
        break;
      }
      try {
        FileStatus fileStatus = getFileStatus(path);
        if (fileStatus.isFile()) {
          throw new FileAlreadyExistsException(
              String.format("Can't make directory for path: %s, "
                  + "since it is a file.", f));
        }
        if (fileStatus.isDirectory()) {
          break;
        }
      } catch (FileNotFoundException e) {
        LOG.debug("Making dir: [{}] in COS", f);

        String folderPath = pathToKey(makeAbsolute(f));
        if (!folderPath.endsWith(PATH_DELIMITER)) {
          folderPath += PATH_DELIMITER;
        }
        store.storeEmptyFile(folderPath);
      }
    }
    return true;
  }

  private boolean mkdir(Path f) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(f);
      if (fileStatus.isFile()) {
        throw new FileAlreadyExistsException(
            String.format(
                "Can't make directory for path '%s' since it is a file.", f));
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Make directory: [{}] in COS.", f);
      }

      String folderPath = pathToKey(makeAbsolute(f));
      if (!folderPath.endsWith(PATH_DELIMITER)) {
        folderPath += PATH_DELIMITER;
      }
      store.storeEmptyFile(folderPath);
    }
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    FileStatus fs = getFileStatus(f); // will throw if the file doesn't
    // exist
    if (fs.isDirectory()) {
      throw new FileNotFoundException("'" + f + "' is a directory");
    }
    LOG.info("Open the file: [{}] for reading.", f);
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    long fileSize = store.getFileLength(key);
    return new FSDataInputStream(new BufferedFSInputStream(
        new CosNInputStream(this.getConf(), store, statistics, key, fileSize,
            this.boundedIOThreadPool), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("Rename source path: [{}] to dest path: [{}].", src, dst);

    // Renaming the root directory is not allowed
    if (src.isRoot()) {
      LOG.debug("Cannot rename the root directory of a filesystem.");
      return false;
    }

    // check the source path whether exists or not
    FileStatus srcFileStatus = this.getFileStatus(src);

    // Source path and destination path are not allowed to be the same
    if (src.equals(dst)) {
      LOG.debug("Source path and dest path refer to "
          + "the same file or directory: [{}].", dst);
      throw new IOException("Source path and dest path refer "
          + "the same file or directory");
    }

    // It is not allowed to rename a parent directory to its subdirectory
    Path dstParentPath;
    for (dstParentPath = dst.getParent();
         null != dstParentPath && !src.equals(dstParentPath);
         dstParentPath = dstParentPath.getParent()) {
      // Recursively find the common parent path of the source and
      // destination paths.
      LOG.debug("Recursively find the common parent directory of the source "
              + "and destination paths. The currently found parent path: {}",
          dstParentPath);
    }

    if (null != dstParentPath) {
      LOG.debug("It is not allowed to rename a parent directory:[{}] "
          + "to its subdirectory:[{}].", src, dst);
      throw new IOException(String.format(
          "It is not allowed to rename a parent directory: %s "
              + "to its subdirectory: %s", src, dst));
    }

    FileStatus dstFileStatus;
    try {
      dstFileStatus = this.getFileStatus(dst);

      // The destination path exists and is a file,
      // and the rename operation is not allowed.
      if (dstFileStatus.isFile()) {
        throw new FileAlreadyExistsException(String.format(
            "File: %s already exists", dstFileStatus.getPath()));
      } else {
        // The destination path is an existing directory,
        // and it is checked whether there is a file or directory
        // with the same name as the source path under the destination path
        dst = new Path(dst, src.getName());
        FileStatus[] statuses;
        try {
          statuses = this.listStatus(dst);
        } catch (FileNotFoundException e) {
          statuses = null;
        }
        if (null != statuses && statuses.length > 0) {
          LOG.debug("Cannot rename source file: [{}] to dest file: [{}], "
              + "because the file already exists.", src, dst);
          throw new FileAlreadyExistsException(
              String.format(
                  "File: %s already exists", dst
              )
          );
        }
      }
    } catch (FileNotFoundException e) {
      // destination path not exists
      Path tempDstParentPath = dst.getParent();
      FileStatus dstParentStatus = this.getFileStatus(tempDstParentPath);
      if (!dstParentStatus.isDirectory()) {
        throw new IOException(String.format(
            "Cannot rename %s to %s, %s is a file", src, dst, dst.getParent()
        ));
      }
      // The default root directory is definitely there.
    }

    boolean result;
    if (srcFileStatus.isDirectory()) {
      result = this.copyDirectory(src, dst);
    } else {
      result = this.copyFile(src, dst);
    }

    if (!result) {
      //Since rename is a non-atomic operation, after copy fails,
      // it is not allowed to delete the data of the original path.
      return false;
    } else {
      return this.delete(src, true);
    }
  }

  private boolean copyFile(Path srcPath, Path dstPath) throws IOException {
    String srcKey = pathToKey(srcPath);
    String dstKey = pathToKey(dstPath);
    this.store.copy(srcKey, dstKey);
    return true;
  }

  private boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
    String srcKey = pathToKey(srcPath);
    if (!srcKey.endsWith(PATH_DELIMITER)) {
      srcKey += PATH_DELIMITER;
    }
    String dstKey = pathToKey(dstPath);
    if (!dstKey.endsWith(PATH_DELIMITER)) {
      dstKey += PATH_DELIMITER;
    }

    if (dstKey.startsWith(srcKey)) {
      throw new IOException(
          "can not copy a directory to a subdirectory of self");
    }

    this.store.storeEmptyFile(dstKey);
    CosNCopyFileContext copyFileContext = new CosNCopyFileContext();

    int copiesToFinishes = 0;
    String priorLastKey = null;
    do {
      PartialListing objectList = this.store.list(
          srcKey, Constants.COS_MAX_LISTING_LENGTH, priorLastKey, true);
      for (FileMetadata file : objectList.getFiles()) {
        this.boundedCopyThreadPool.execute(new CosNCopyFileTask(
            this.store,
            file.getKey(),
            dstKey.concat(file.getKey().substring(srcKey.length())),
            copyFileContext));
        copiesToFinishes++;
        if (!copyFileContext.isCopySuccess()) {
          break;
        }
      }
      priorLastKey = objectList.getPriorLastKey();
    } while (null != priorLastKey);

    copyFileContext.lock();
    try {
      copyFileContext.awaitAllFinish(copiesToFinishes);
    } catch (InterruptedException e) {
      LOG.warn("interrupted when wait copies to finish");
    } finally {
      copyFileContext.lock();
    }

    return copyFileContext.isCopySuccess();
  }

  private void createParent(Path path) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      String parentKey = pathToKey(parent);
      LOG.debug("Create parent key: {}", parentKey);
      if (!parentKey.equals(PATH_DELIMITER)) {
        String key = pathToKey(makeAbsolute(parent));
        if (key.length() > 0) {
          try {
            store.storeEmptyFile(key + PATH_DELIMITER);
          } catch (IOException e) {
            LOG.debug("Store a empty file in COS failed.", e);
            throw e;
          }
        }
      }
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public long getDefaultBlockSize() {
    return getConf().getLong(
        CosNConfigKeys.COSN_BLOCK_SIZE_KEY,
        CosNConfigKeys.DEFAULT_BLOCK_SIZE);
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

  @Override
  public void close() throws IOException {
    try {
      this.store.close();
      this.boundedIOThreadPool.shutdown();
      this.boundedCopyThreadPool.shutdown();
    } finally {
      super.close();
    }
  }
}
