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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.tosfs.commit.MagicOutputStream;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ChecksumInfo;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.object.DirectoryStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectMultiRangeInputStream;
import org.apache.hadoop.fs.tosfs.object.ObjectOutputStream;
import org.apache.hadoop.fs.tosfs.object.ObjectRangeInputStream;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.exceptions.InvalidObjectKeyException;
import org.apache.hadoop.fs.tosfs.ops.DefaultFsOps;
import org.apache.hadoop.fs.tosfs.ops.DirectoryFsOps;
import org.apache.hadoop.fs.tosfs.ops.FsOps;
import org.apache.hadoop.fs.tosfs.util.FSUtils;
import org.apache.hadoop.fs.tosfs.util.FuseUtils;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.fs.tosfs.util.RemoteIterators;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.XAttrSetFlag.CREATE;
import static org.apache.hadoop.fs.XAttrSetFlag.REPLACE;

public class RawFileSystem extends FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(RawFileSystem.class);
  private static final String MULTIPART_THREAD_POOL_PREFIX = "rawfs-multipart-thread-pool";
  private static final String TASK_THREAD_POOL_PREFIX = "rawfs-task-thread-pool";
  // This is the same as HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, we do not
  // use that directly because we don't want to introduce the hdfs client library.
  private static final String DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  private static final long DFS_BLOCK_SIZE_DEFAULT = 128 << 20;

  private String scheme;
  private String username;
  private Path workingDir;
  private URI uri;
  private String bucket;
  private ObjectStorage storage;
  // Use for task parallel execution, such as parallel to copy multiple files.
  private ExecutorService taskThreadPool;
  // Use for file multipart upload only.
  private ExecutorService uploadThreadPool;
  private FsOps fsOps;

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @VisibleForTesting
  String bucket() {
    return bucket;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    LOG.debug("Opening '{}' for reading.", path);
    RawFileStatus status = innerFileStatus(path);
    if (status.isDirectory()) {
      throw new FileNotFoundException(
          String.format("Can't open %s because it is a directory", path));
    }

    // Parse the range size from the hadoop conf.
    long rangeSize = getConf().getLong(
        ConfKeys.FS_OBJECT_STREAM_RANGE_SIZE,
        ConfKeys.FS_OBJECT_STREAM_RANGE_SIZE_DEFAULT);
    Preconditions.checkArgument(rangeSize > 0, "Object storage range size must be positive.");

    FSInputStream fsIn = new ObjectMultiRangeInputStream(taskThreadPool, storage, path,
        status.getLen(), rangeSize, status.checksum());
    return new FSDataInputStream(fsIn);
  }

  public FSDataInputStream open(Path path, byte[] expectedChecksum, Range range) {
    return new FSDataInputStream(new ObjectRangeInputStream(storage, path, range, expectedChecksum));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    FileStatus fileStatus = getFileStatusOrNull(path);
    if (fileStatus != null) {
      if (fileStatus.isDirectory()) {
        throw new FileAlreadyExistsException(path + " is a directory");
      }

      if (!overwrite) {
        throw new FileAlreadyExistsException(path + " already exists");
      }
      LOG.debug("Overwriting file {}", path);
    }

    if (MagicOutputStream.isMagic(path)) {
      return new FSDataOutputStream(
          new MagicOutputStream(this, storage, uploadThreadPool, getConf(), makeQualified(path)),
          null);
    } else {
      ObjectOutputStream out =
          new ObjectOutputStream(storage, uploadThreadPool, getConf(), makeQualified(path), true);

      if (fileStatus == null && FuseUtils.fuseEnabled()) {
        // The fuse requires the file to be visible when accessing getFileStatus once we created
        // the file, so here we close and commit the file to be visible explicitly for fuse, and
        // then reopen the file output stream for further data bytes writing.
        out.close();
        out =
            new ObjectOutputStream(storage, uploadThreadPool, getConf(), makeQualified(path), true);
      }

      return new FSDataOutputStream(out, null);
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    Path path = makeQualified(f);
    LOG.debug("listFiles({}, {})", path, recursive);

    // assume the path is a dir at first, and list sub files
    RemoteIterator<LocatedFileStatus> subFiles = RemoteIterators.fromIterable(
        fsOps.listDir(path, recursive, key -> !ObjectInfo.isDir(key)), this::toLocatedFileStatus);
    if (!subFiles.hasNext()) {
      final RawFileStatus fileStatus = innerFileStatus(path);
      if (fileStatus.isFile()) {
        return RemoteIterators.fromSingleton(toLocatedFileStatus(fileStatus));
      }
    }
    return subFiles;
  }

  private RawLocatedFileStatus toLocatedFileStatus(RawFileStatus status) throws IOException {
    return new RawLocatedFileStatus(status,
        status.isFile() ? getFileBlockLocations(status, 0, status.getLen()) : null);
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flag,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    Path qualified = makeQualified(path);
    return create(qualified, permission, flag.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Rename src path to dest path, if dest path is an existed dir,
   * then FS will rename the src path under the dst dir.
   * E.g. rename('/a/b', '/a/c') and dest 'c' is an existed dir,
   * then the source path '/a/b' will be renamed with dest path '/a/b/c' internally.
   *
   * <ul>
   *   <li>Return false if src doesn't exist</li>
   *   <li>Return false if src is root</li>
   *   <li>Return false if dst path is under src path, e.g. rename('/a/b', '/a/b/c')</li>
   *   <li>Return false if dst path already exists</li>
   *   <li>Return true if rename('/a/b', '/a/b') and 'b' is an existed file</li>
   *   <li>Return true if rename('/a/b', '/a') and 'a' is an existed dir,
   *   fs will rename '/a/b' to '/a/b' internally</li>
   *   <li>Return false if rename('/a/b', '/a/b') and 'b' is an existed dir,
   *   because fs will try to rename '/a/b' to '/a/b/b', which is under '/a/b', this behavior
   *   is forbidden.</li>
   * </ul>
   *
   * @param src path to be renamed
   * @param dst path after rename
   * @return true if rename is successful
   * @throws IOException on failure
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("Rename source path {} to dest path {}", src, dst);

    // 1. Check source and destination path
    Future<FileStatus> srcStatusFuture = taskThreadPool.submit(() -> checkAndGetSrcStatus(src));
    Future<Path> destPathFuture = taskThreadPool.submit(() -> checkAndGetDstPath(src, dst));

    FileStatus srcStatus;
    Path dstPath;
    try {
      srcStatus = srcStatusFuture.get();
      dstPath = destPathFuture.get();

      if (src.equals(dstPath)) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed to rename path, src: {}, dst: {}", src, dst, e);
      return false;
    }

    // 2. Start copy source to destination
    if (srcStatus.isDirectory()) {
      fsOps.renameDir(srcStatus.getPath(), dstPath);
    } else {
      fsOps.renameFile(srcStatus.getPath(), dstPath, srcStatus.getLen());
    }

    return true;
  }

  private Path checkAndGetDstPath(Path src, Path dest) throws IOException {
    FileStatus destStatus = getFileStatusOrNull(dest);
    // 1. Rebuilding the destination path
    Path finalDstPath = dest;
    if (destStatus != null && destStatus.isDirectory()) {
      finalDstPath = new Path(dest, src.getName());
    }

    // 2. No need to check the dest path because renaming itself is allowed.
    if (src.equals(finalDstPath)) {
      return finalDstPath;
    }

    // 3. Ensure the source path cannot be the ancestor of destination path.
    if (RawFSUtils.inSubtree(src, finalDstPath)) {
      throw new IOException(String.format("Failed to rename since it is prohibited to " +
          "rename dest path %s under src path %s", finalDstPath, src));
    }

    // 4. Ensure the destination path doesn't exist.
    FileStatus finalDstStatus = destStatus;
    if (destStatus != null && destStatus.isDirectory()) {
      finalDstStatus = getFileStatusOrNull(finalDstPath);
    }
    if (finalDstStatus != null) {
      throw new FileAlreadyExistsException(
          String.format("Failed to rename since the dest path %s already exists.", finalDstPath));
    } else {
      return finalDstPath;
    }
  }

  private FileStatus checkAndGetSrcStatus(Path src) throws IOException {
    // throw FileNotFoundException if src not found.
    FileStatus srcStatus = innerFileStatus(src);

    if (src.isRoot()) {
      throw new IOException(String.format("Cannot rename the root directory %s to another name",
          src));
    }
    return srcStatus;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.debug("Delete path {} - recursive {}", f, recursive);
    try {
      FileStatus fileStatus = getFileStatus(f);
      Path path = fileStatus.getPath();

      if (path.isRoot()) {
        return deleteRoot(path, recursive);
      } else {
        if (fileStatus.isDirectory()) {
          fsOps.deleteDir(path, recursive);
        } else {
          fsOps.deleteFile(path);
        }
        return true;
      }
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist", f);
      return false;
    }
  }

  /**
   * Reject deleting root directory and implement the specific logic to compatible with
   * AbstractContractRootDirectoryTest rm test cases.
   *
   * @param root      the root path.
   * @param recursive indicate whether delete directory recursively
   * @return true if root directory is empty, false if trying to delete a non-empty dir recursively.
   * @throws IOException if trying to delete the non-empty root dir non-recursively.
   */
  private boolean deleteRoot(Path root, boolean recursive) throws IOException {
    LOG.info("Delete the {} root directory of {}", bucket, recursive);
    boolean isEmptyDir = fsOps.isEmptyDirectory(root);
    if (isEmptyDir) {
      return true;
    }
    if (recursive) {
      // AbstractContractRootDirectoryTest#testRmRootRecursive doesn't expect any exception if
      // trying to delete a non-empty root directory recursively, so we have to return false here
      // instead of throwing a IOException.
      return false;
    } else {
      // AbstractContractRootDirectoryTest#testRmNonEmptyRootDirNonRecursive expect a exception if
      // trying to delete a non-empty root directory non-recursively, so we have to throw a
      // IOException instead of returning false.
      throw new PathIOException(bucket, "Cannot delete root path");
    }
  }

  @Override
  public RawFileStatus[] listStatus(Path f) throws IOException {
    LOG.debug("List status for path: {}", f);
    return Iterators.toArray(listStatus(f, false), RawFileStatus.class);
  }

  public Iterator<RawFileStatus> listStatus(Path f, boolean recursive) throws IOException {
    Path path = makeQualified(f);
    // Assuming path is a dir at first.
    Iterator<RawFileStatus> iterator = fsOps.listDir(path, recursive, key -> true).iterator();
    if (iterator.hasNext()) {
      return iterator;
    } else {
      RawFileStatus fileStatus = innerFileStatus(path);
      if (fileStatus.isFile()) {
        return Collections.singletonList(fileStatus).iterator();
      } else {
        // The path is an empty dir.
        return Collections.emptyIterator();
      }
    }
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path p) throws IOException {
    // We expect throw FileNotFoundException if the path doesn't exist during creating the
    // RemoteIterator instead of throwing FileNotFoundException during call hasNext method.

    // The follow RemoteIterator is as same as {@link FileSystem#DirListingIterator} above
    // hadoop 3.2.2, but below 3.2.2, the DirListingIterator fetches the directory entries during
    // call hasNext method instead of create the DirListingIterator instance.
    return new RemoteIterator<FileStatus>() {
      private DirectoryEntries entries = listStatusBatch(p, null);
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < entries.getEntries().length || entries.hasMore();
      }

      private void fetchMore() throws IOException {
        byte[] token = entries.getToken();
        entries = listStatusBatch(p, token);
        index = 0;
      }

      @Override
      public FileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException("No more items in iterator");
        } else {
          if (index == entries.getEntries().length) {
            fetchMore();
            if (!hasNext()) {
              throw new NoSuchElementException("No more items in iterator");
            }
          }

          return entries.getEntries()[index++];
        }
      }
    };
  }

  public static long dateToLong(final Date date) {
    return date == null ? 0L : date.getTime();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    this.workingDir = new_dir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    try {
      FileStatus fileStatus = innerFileStatus(path);
      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + path);
      }
    } catch (FileNotFoundException e) {
      Path dir = makeQualified(path);
      validatePath(dir);
      fsOps.mkdirs(dir);
    }
    return true;
  }

  private void validatePath(Path path) throws IOException {
    Path parent = path.getParent();
    do {
      try {
        FileStatus fileStatus = innerFileStatus(parent);
        if (fileStatus.isDirectory()) {
          // If path exists and a directory, exit
          break;
        } else {
          throw new FileAlreadyExistsException(String.format("Can't make directory for path '%s',"
                  + " it is a file.", parent));
        }
      } catch (FileNotFoundException ignored) {
      }
      parent = parent.getParent();
    } while (parent != null);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    try {
      return innerFileStatus(path);
    } catch (ParentNotDirectoryException e) {
      // Treat ParentNotDirectoryException as FileNotFoundException for the case that check whether
      // path exist or not.
      throw new FileNotFoundException(e.getMessage());
    }
  }


  /**
   * Get the file status of given path.
   *
   * @param f the path
   * @return {@link RawFileStatus} describe file status info.
   * @throws FileNotFoundException       if the path doesn't exist.
   * @throws ParentNotDirectoryException if the path is locating under an existing file, which is
   *                                     not allowed in directory bucket case.
   */
  RawFileStatus innerFileStatus(Path f) throws ParentNotDirectoryException, FileNotFoundException {
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    RawFileStatus fileStatus = getFileStatusOrNull(qualifiedPath);
    if (fileStatus == null) {
      throw new FileNotFoundException(
          String.format("No such file or directory: %s", qualifiedPath));
    }
    return fileStatus;
  }

  /**
   * The different with {@link RawFileSystem#getFileStatus(Path)} is that:
   * 1. throw  {@link ParentNotDirectoryException} if the path is locating under an existing file in
   * directory bucket case, but {@link RawFileSystem#getFileStatus(Path)} will ignore whether the
   * invalid path and throw {@link FileNotFoundException}
   * 2. return null if the path doesn't exist instead of throwing {@link FileNotFoundException}.
   *
   * @param path the object path.
   * @return null if the path doesn't exist.
   * @throws ParentNotDirectoryException if the path is locating under an existing file, which is
   *                                     not allowed in directory bucket case.
   */
  public RawFileStatus getFileStatusOrNull(final Path path) throws ParentNotDirectoryException {
    Path qualifiedPath = path.makeQualified(uri, workingDir);
    String key = ObjectUtils.pathToKey(qualifiedPath);

    // Root directory always exists
    if (key.isEmpty()) {
      return new RawFileStatus(0, true, 0, 0, qualifiedPath, username, Constants.MAGIC_CHECKSUM);
    }

    try {
      ObjectInfo obj = storage.objectStatus(key);
      if (obj == null) {
        return null;
      } else {
        return objectToFileStatus(obj);
      }
    } catch (InvalidObjectKeyException e) {
      String msg =
          String.format("The object key %s is a invalid key, detail: %s", key, e.getMessage());
      throw new ParentNotDirectoryException(msg);
    }
  }

  private RawFileStatus objectToFileStatus(ObjectInfo obj) {
    Path keyPath = makeQualified(ObjectUtils.keyToPath(obj.key()));
    long blockSize = obj.isDir() ? 0 : getDefaultBlockSize(keyPath);
    long modificationTime = dateToLong(obj.mtime());
    return new RawFileStatus(obj.size(), obj.isDir(), blockSize, modificationTime, keyPath,
        username, obj.checksum());
  }

  @Override
  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) {
    Configuration config = getConf();
    // CRC32 is chosen as default as it is available in all
    // releases that support checksum.
    // The client trash configuration is ignored.
    return new FsServerDefaults(getDefaultBlockSize(),
        config.getInt("dfs.bytes-per-checksum", 512),
        64 * 1024,
        getDefaultReplication(),
        config.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
        false,
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT,
        DataChecksum.Type.CRC32,
        "");
  }

  private void stopAllServices() {
    ThreadPools.shutdown(uploadThreadPool, 30, TimeUnit.SECONDS);
    ThreadPools.shutdown(taskThreadPool, 30, TimeUnit.SECONDS);
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    this.scheme = FSUtils.scheme(conf, uri);

    // Username is the current user at the time the FS was instantiated.
    this.username = UserGroupInformation.getCurrentUser().getShortUserName();
    this.workingDir = new Path("/user", username).makeQualified(uri, null);
    this.uri = URI.create(scheme + "://" + uri.getAuthority());
    this.bucket = this.uri.getAuthority();
    this.storage = ObjectStorageFactory.create(scheme, bucket, getConf());
    if (storage.bucket() == null) {
      throw new FileNotFoundException(String.format("Bucket: %s not found.", uri.getAuthority()));
    }

    int taskThreadPoolSize =
        getConf().getInt(ConfKeys.FS_TASK_THREAD_POOL_SIZE.key(storage.scheme()),
            ConfKeys.FS_TASK_THREAD_POOL_SIZE_DEFAULT);
    this.taskThreadPool = ThreadPools.newWorkerPool(TASK_THREAD_POOL_PREFIX, taskThreadPoolSize);

    int uploadThreadPoolSize =
        getConf().getInt(ConfKeys.FS_MULTIPART_THREAD_POOL_SIZE.key(storage.scheme()),
            ConfKeys.FS_MULTIPART_THREAD_POOL_SIZE_DEFAULT);
    this.uploadThreadPool =
        ThreadPools.newWorkerPool(MULTIPART_THREAD_POOL_PREFIX, uploadThreadPoolSize);

    if (storage.bucket().isDirectory()) {
      fsOps = new DirectoryFsOps((DirectoryStorage) storage, this::objectToFileStatus);
    } else {
      fsOps = new DefaultFsOps(storage, getConf(), taskThreadPool, this::objectToFileStatus);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
      storage.close();
    } finally {
      stopAllServices();
    }
  }

  public ObjectStorage storage() {
    return storage;
  }

  public ExecutorService uploadThreadPool() {
    return uploadThreadPool;
  }

  /**
   * @return null if checksum is not supported.
   */
  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    Preconditions.checkArgument(length >= 0);

    RawFileStatus fileStatus = innerFileStatus(f);
    if (fileStatus.isDirectory()) {
      // Compatible with HDFS
      throw new FileNotFoundException(String.format("Path is not a file, %s", f));
    }
    if (!getConf().getBoolean(ConfKeys.FS_CHECKSUM_ENABLED.key(storage.scheme()),
        ConfKeys.FS_CHECKSUM_ENABLED_DEFAULT)) {
      return null;
    }

    ChecksumInfo csInfo = storage.checksumInfo();
    return new TosChecksum(csInfo.algorithm(), fileStatus.checksum());
  }

  @Override
  public String getCanonicalServiceName() {
    return null;
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    Preconditions.checkNotNull(name, "xAttr name must not be null.");
    Preconditions.checkArgument(!name.isEmpty(), "xAttr name must not be empty.");
    Preconditions.checkNotNull(value, "xAttr value must not be null.");

    if (getFileStatus(path).isFile()) {
      Path qualifiedPath = path.makeQualified(uri, workingDir);
      String key = ObjectUtils.pathToKey(qualifiedPath);

      Map<String, String> existedTags = storage.getTags(key);
      validateXAttrFlag(name, existedTags.containsKey(name), flag);

      String newValue = Bytes.toString(value);
      String previousValue = existedTags.put(name, newValue);
      if (!newValue.equals(previousValue)) {
        storage.putTags(key, existedTags);
      }
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    if (getFileStatus(path).isDirectory()) {
      return new HashMap<>();
    } else {
      Path qualifiedPath = path.makeQualified(uri, workingDir);
      String key = ObjectUtils.pathToKey(qualifiedPath);

      Map<String, String> tags = storage.getTags(key);
      return tags.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, t -> Bytes.toBytes(t.getValue())));
    }
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    Map<String, byte[]> xAttrs = getXAttrs(path);
    if (xAttrs.containsKey(name)) {
      return xAttrs.get(name);
    } else {
      throw new IOException("Attribute with name " + name + " is not found.");
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    Map<String, byte[]> xAttrs = getXAttrs(path);
    xAttrs.keySet().retainAll(names);
    if (xAttrs.size() == names.size()) {
      return xAttrs;
    } else {
      List<String> badNames = names.stream().filter(n -> !xAttrs.containsKey(n)).collect(
          Collectors.toList());
      throw new IOException("Attributes with name " + badNames + " are not found.");
    }
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return Lists.newArrayList(getXAttrs(path).keySet());
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    if (getFileStatus(path).isFile()) {
      Path qualifiedPath = path.makeQualified(uri, workingDir);
      String key = ObjectUtils.pathToKey(qualifiedPath);

      Map<String, String> existedTags = storage.getTags(key);
      if (existedTags.remove(name) != null) {
        storage.putTags(key, existedTags);
      }
    }
  }

  private void validateXAttrFlag(String xAttrName, boolean xAttrExists, EnumSet<XAttrSetFlag> flag)
      throws IOException {
    if (xAttrExists) {
      if (!flag.contains(REPLACE)) {
        throw new IOException("XAttr: " + xAttrName + " already exists. The REPLACE flag must be"
            + " specified.");
      }
    } else {
      if (!flag.contains(CREATE)) {
        throw new IOException("XAttr: " + xAttrName + " does not exist. The CREATE flag must be"
            + " specified.");
      }
    }
  }
}
