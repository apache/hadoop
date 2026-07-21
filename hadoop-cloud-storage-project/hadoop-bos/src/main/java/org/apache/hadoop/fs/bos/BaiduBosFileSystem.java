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

package org.apache.hadoop.fs.bos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop FileSystem implementation for Baidu Object Storage (BOS).
 * Supports both hierarchy (namespace) and non-hierarchy bucket modes.
 */
public class BaiduBosFileSystem extends FileSystem {

  /** Logger for this class. */
  public static final Logger LOG =
      LoggerFactory.getLogger(BaiduBosFileSystem.class);
  /** Path delimiter used in BOS keys. */
  public static final String PATH_DELIMITER = Path.SEPARATOR;
  private static final String FOLDER_SUFFIX = "/";

  private URI uri;
  private BosNativeFileSystemStore store;
  private Path workingDir;
  private long readAhead;
  private int readBufferSize;
  private boolean forceGetNonHierarchyMetadataInListStatus;

  /** Default constructor. */
  public BaiduBosFileSystem() {
  }

  /**
   * Convert a path to a BOS object key.
   *
   * @param path the path to convert
   * @return the BOS object key
   */
  protected String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException(
          "Path must be absolute: " + path);
    }
    checkPath(path);
    return path.toUri().getPath().substring(1);
  }

  private static Path keyToPath(String key) {
    return new Path("/" + key);
  }

  @Override
  public String getScheme() {
    return "bos";
  }

  @Override
  public void close() throws IOException {
    try {
      this.store.close();
    } finally {
      super.close();
    }
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(uri, conf);
    store = BosNativeFileSystemStore.createStore(uri, conf);
    setConf(conf);
    this.uri = URI.create(
        uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user",
        System.getProperty("user.name")).makeQualified(this);
    this.readAhead = conf.getLong(
        BaiduBosConstants.BOS_READ_AHEAD_SIZE,
        BaiduBosConstants.DEFAULT_BOS_READ_AHEAD_SIZE);
    this.readBufferSize = conf.getInt(
        BaiduBosConstants.BOS_READ_BUFFER_SIZE,
        BaiduBosConstants.DEFAULT_BOS_READ_BUFFER_SIZE);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * This optional operation is not yet supported.
   *
   * @param f the file to append to
   * @param bufferSize the buffer size
   * @param progress for reporting progress
   * @return never returns
   * @throws IOException always thrown
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException(
        "Append is not supported by BaiduBosFileSystem");
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(
      final Path f, final PathFilter filter) throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private final FileStatus[] stats = listStatus(f, filter);
      private int i = 0;

      public boolean hasNext() {
        return i < stats.length;
      }

      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException(
              "No more entry in " + f);
        }
        FileStatus result = stats[i++];
        BlockLocation[] locs = result.isFile()
            ? getFileBlockLocations(result, 0, result.getLen())
            : null;
        return new LocatedFileStatus(result, locs);
      }
    };
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    if (overwrite) {
      if (store.isDirectory(key)) {
        throw new FileAlreadyExistsException(
            f + " is a directory");
      }
    } else {
      try {
        getFileStatus(f);
        throw new FileAlreadyExistsException(
            f + " already exists");
      } catch (FileNotFoundException e) {
        // nothing, ok
      }
    }

    return new FSDataOutputStream(
        store.createFile(key, getConf()), statistics);
  }

  /**
   * Create a file non-recursively. The parent directory must
   * already exist.
   *
   * @param f the file name to create
   * @param permission the permission to set
   * @param overwrite if true, overwrite the existing file
   * @param bufferSize the buffer size
   * @param replication the replication factor
   * @param blockSize the block size
   * @param progress for reporting progress
   * @return an output stream to write to
   * @throws IOException if an I/O error occurs
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path f,
      FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    Path absolutePath = makeAbsolute(f);
    Path parent = absolutePath.getParent();

    if (parent != null && !parent.isRoot()) {
      try {
        FileStatus parentStatus = getFileStatus(parent);
        if (!parentStatus.isDirectory()) {
          throw new FileAlreadyExistsException(
              parent + " is a file");
        }
      } catch (FileNotFoundException e) {
        throw new FileNotFoundException(
            "Parent directory doesn't exist: " + parent);
      }
    }

    return create(f, permission, overwrite, bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Create a file non-recursively with CreateFlag set.
   *
   * @param f the file name to create
   * @param permission the permission to set
   * @param flags creation flags
   * @param bufferSize the buffer size
   * @param replication the replication factor
   * @param blockSize the block size
   * @param progress for reporting progress
   * @return an output stream to write to
   * @throws IOException if an I/O error occurs
   */
  @Deprecated
  @Override
  public FSDataOutputStream createNonRecursive(Path f,
      FsPermission permission, EnumSet<CreateFlag> flags,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return createNonRecursive(f, permission,
        flags.contains(CreateFlag.OVERWRITE),
        bufferSize, replication, blockSize, progress);
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive)
      throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    if (store.isHierarchy() && recursive) {
      try {
        getFileStatus(f);
      } catch (FileNotFoundException e) {
        return false;
      }
      store.deleteDirs(key, recursive);
    } else {
      FileStatus status;
      try {
        status = getFileStatus(f);
      } catch (FileNotFoundException e) {
        return false;
      }

      if (status.isDirectory()) {
        store.deleteDirs(key, recursive);
      } else {
        store.delete(key);
      }
    }

    if (!store.isHierarchy()) {
      createParent(f);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.isEmpty()) {
      return newDirectory(null, absolutePath);
    }

    try {
      FileMetadata meta = store.retrieveMetadata(key);
      if (meta != null) {
        if (meta.isFolder()) {
          return meta.getLastModified() == 0
              ? newDirectory(null, absolutePath)
              : newDirectory(meta, absolutePath);
        } else {
          return newFile(meta, absolutePath);
        }
      }
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException(
          absolutePath + ": No such file or directory.");
    } catch (IOException e) {
      LOG.error("bos-fs getFileStatus error: ", e);
      throw e;
    }
    throw new FileNotFoundException(
        absolutePath + ": No such file or directory.");
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length)
      throws IOException {
    LOG.debug("call the checksum for the path: {}",
        f.getName());
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    return this.store.getFileChecksum(key);
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    String key = pathToKey(makeAbsolute(f));
    try {
      return store.isDirectory(key);
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * List the statuses of the files/directories in the given path.
   *
   * <p>If {@code f} is a file, this method will make a single call
   * to BOS. If {@code f} is a directory, this method will make a
   * maximum of (<i>n</i> / 1000) + 2 calls to BOS, where <i>n</i>
   * is the total number of files and directories contained directly
   * in {@code f}.
   *
   * @param f the path
   * @return file status array
   * @throws IOException on IO failure
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    URI pathUri = absolutePath.toUri();
    Set<FileStatus> status = new TreeSet<>();
    String priorLastKey = null;

    do {
      PartialListing listing = store.list(key,
          BaiduBosConstants.BOS_MAX_LISTING_LENGTH,
          priorLastKey, PATH_DELIMITER);
      if (listing != null) {
        for (FileMetadata fileMetadata : listing.getFiles()) {
          if (fileMetadata.getKey().endsWith("/")) {
            continue;
          }
          Path subpath = keyToPath(fileMetadata.getKey());
          String relativePath =
              pathUri.relativize(subpath.toUri()).getPath();
          status.add(getBosFileStatus(fileMetadata,
              new Path(absolutePath, relativePath)));
        }
        for (FileMetadata dirMetadata
            : listing.getDirectories()) {
          Path subpath = keyToPath(dirMetadata.getKey());
          String relativePath =
              pathUri.relativize(subpath.toUri()).getPath();
          Path dirPath = new Path(absolutePath, relativePath);
          status.add(getBosDirStatus(dirMetadata, dirPath));
        }
        priorLastKey = listing.getPriorLastKey();
      }
    } while (priorLastKey != null && !priorLastKey.isEmpty());

    if (status.isEmpty()) {
      FileStatus stat = getFileStatus(f);
      if (stat.isFile()) {
        status.add(stat);
      }
    }
    return status.toArray(new FileStatus[0]);
  }

  /**
   * Get BOS file status. Only used in listStatus.
   */
  private FileStatus getBosFileStatus(FileMetadata fileMetadata,
      Path path) {
    boolean needExtHead = !this.store.isHierarchy();
    try {
      if (needExtHead
          && forceGetNonHierarchyMetadataInListStatus) {
        FileMetadata fMeta =
            store.retrieveMetadata(fileMetadata.getKey());
        if (fMeta != null) {
          return newFile(fMeta, path);
        }
      }
    } catch (IOException e) {
      // throw nothing
    }
    return newFile(fileMetadata, path);
  }

  private FileStatus getBosDirStatus(FileMetadata dirMetadata,
      Path path) {
    boolean needExtHead = !this.store.isHierarchy();
    try {
      if (needExtHead) {
        FileMetadata dirMeta =
            store.retrieveMetadata(
                prefixToDir(dirMetadata.getKey()));
        if (dirMeta != null) {
          return newDirectory(dirMeta, path);
        }
      }
    } catch (IOException e) {
      // throw nothing
    }
    return newDirectory(dirMetadata, path);
  }

  /**
   * Add a trailing slash to a prefix if not already present.
   *
   * @param prefix the prefix to convert
   * @return the prefix ending with "/"
   */
  public static String prefixToDir(String prefix) {
    if (prefix != null && !prefix.isEmpty()
        && !prefix.endsWith(PATH_DELIMITER)) {
      prefix += PATH_DELIMITER;
    }
    return prefix;
  }

  /**
   * Non-recursive getContentSummary.
   *
   * @param f path to use
   * @return the content summary
   * @throws IOException on IO failure
   */
  @Override
  public ContentSummary getContentSummary(Path f)
      throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    FileStatus status = this.getFileStatus(f);
    if (status.isFile()) {
      return new ContentSummary.Builder()
          .length(status.getLen()).fileCount(1L)
          .directoryCount(0L).build();
    }
    if (store.isHierarchy()) {
      return multiThreadGetContentSummary(f);
    }
    long[] summary = new long[]{0L, 0L, 0L};
    String priorLastKey = null;
    do {
      PartialListing listing = store.list(
          store.prefixToDir(key),
          BaiduBosConstants.BOS_MAX_LISTING_LENGTH,
          priorLastKey, null);
      if (listing != null) {
        for (FileMetadata fileMetadata : listing.getFiles()) {
          if (fileMetadata.getKey() != null
              && !fileMetadata.getKey().isEmpty()) {
            summary[0] += fileMetadata.getLength();
            if (!fileMetadata.getKey().endsWith("/")) {
              summary[1] += 1;
            } else {
              summary[2] += 1;
            }
          }
        }
        priorLastKey = listing.getPriorLastKey();
      }
    } while (priorLastKey != null && !priorLastKey.isEmpty());
    return new ContentSummary.Builder()
        .length(summary[0]).fileCount(summary[1])
        .directoryCount(summary[2]).build();
  }

  /**
   * Multi-threaded content summary for hierarchy buckets.
   *
   * @param root the root path
   * @return the content summary
   * @throws IOException on IO failure
   */
  public ContentSummary multiThreadGetContentSummary(Path root)
      throws IOException {
    ExecutorService es = store.getBoundedThreadPool();
    Queue<Future<ContentSummary>> futures =
        new LinkedBlockingQueue<>();
    AtomicBoolean exceptionThrow = new AtomicBoolean(false);
    long[] summary = new long[]{0L, 0L, 1L};
    FileStatus[] statuses = listStatus(root);
    for (FileStatus status : statuses) {
      if (status.isFile()) {
        summary[0] += status.getLen();
        summary[1]++;
      } else {
        summary[2]++;
        processDirectory(status.getPath(), futures, es,
            exceptionThrow);
      }
    }

    while (!exceptionThrow.get() && !futures.isEmpty()) {
      Future<ContentSummary> future = futures.poll();
      try {
        ContentSummary subSummary = future.get();
        summary[0] += subSummary.getLength();
        summary[1] += subSummary.getFileCount();
        summary[2] += subSummary.getDirectoryCount();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(e);
      }
    }
    if (exceptionThrow.get()) {
      throw new IOException(
          "Exception while get content summary");
    }
    return new ContentSummary.Builder()
        .length(summary[0]).fileCount(summary[1])
        .directoryCount(summary[2]).build();
  }

  private void processDirectory(Path p,
      Queue<Future<ContentSummary>> futures,
      ExecutorService es, AtomicBoolean exceptionThrow) {
    futures.add(es.submit(() -> {
      long[] summary = new long[]{0L, 0L, 0L};
      try {
        if (!exceptionThrow.get()) {
          FileStatus[] statuses = listStatus(p);
          for (FileStatus status : statuses) {
            if (status.isFile()) {
              summary[0] += status.getLen();
              summary[1]++;
            } else {
              summary[2]++;
              processDirectory(status.getPath(), futures,
                  es, exceptionThrow);
            }
          }
        }
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
        exceptionThrow.set(true);
      }
      return new ContentSummary.Builder()
          .length(summary[0]).fileCount(summary[1])
          .directoryCount(summary[2]).build();
    }));
  }

  // SpotBugs fix: removed dead-store variables owner/group.
  // SpotBugs fix: meta is never null here (length is set),
  //   so the null-check was misleading.
  private FileStatus newFile(FileMetadata meta, Path path) {
    long blockSize = getConf().getLong(
        BaiduBosConstants.BOS_BLOCK_SIZE,
        BaiduBosConstants.DEFAULT_BOS_BLOCK_SIZE);
    String owner = meta.getUserName() != null
        ? meta.getUserName() : store.getEnvUserName();
    String group = meta.getGroupName() != null
        ? meta.getGroupName() : store.getEnvGroupName();
    return new FileStatus(
        meta.getLength(),
        false,
        1,
        blockSize,
        meta.getLastModified(),
        0L,
        null,
        owner,
        group,
        null,
        path.makeQualified(this),
        FileStatus.NONE);
  }

  private FileStatus newDirectory(FileMetadata meta,
      Path path) {
    long blockSize = getConf().getLong(
        BaiduBosConstants.BOS_BLOCK_SIZE,
        BaiduBosConstants.DEFAULT_BOS_BLOCK_SIZE);
    String owner = (meta != null && meta.getUserName() != null)
        ? meta.getUserName() : store.getEnvUserName();
    String group = (meta != null && meta.getGroupName() != null)
        ? meta.getGroupName() : store.getEnvGroupName();
    return new FileStatus(
        0,
        true,
        1,
        blockSize,
        (meta == null ? 0 : meta.getLastModified()),
        0L,
        null,
        owner,
        group,
        null,
        path.makeQualified(this),
        FileStatus.NONE);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission)
      throws IOException {
    Path absolutePath = makeAbsolute(f);

    List<Path> paths = new ArrayList<>();
    do {
      try {
        FileStatus fileStatus = getFileStatus(absolutePath);
        if (fileStatus.isFile()) {
          throw new FileAlreadyExistsException(
              absolutePath + " is a file");
        } else {
          break;
        }
      } catch (FileNotFoundException e) {
        paths.add(0, absolutePath);
        absolutePath = absolutePath.getParent();
      }
    } while (!pathToKey(absolutePath).isEmpty());

    boolean result = true;
    for (Path path : paths) {
      result &= mkdir(path);
    }
    return result;
  }

  private boolean mkdir(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.endsWith(FOLDER_SUFFIX)) {
      key = pathToKey(f) + FOLDER_SUFFIX;
    }

    store.storeEmptyFile(key, this.store.getEnvUserName(),
        this.store.getEnvGroupName());
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    FileMetadata fileMetaData = null;
    try {
      fileMetaData = store.retrieveMetadata(key);
    } catch (FileNotFoundException ignore) {
      throw new FileNotFoundException(f.toString());
    }

    if (fileMetaData.isFolder()) {
      throw new FileNotFoundException("Can not open a folder");
    }

    BosInputStream bosFsInputStream = new BosInputStream(
        key, fileMetaData, this.store, this.statistics);

    bosFsInputStream.setReadahead(this.readAhead);
    return new FSDataInputStream(
        new BufferedFSInputStream(
            bosFsInputStream, this.readBufferSize));
  }

  private void createParent(Path path) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      String key = pathToKey(makeAbsolute(parent));
      if (key.length() > 0) {
        if (!store.isDirectory(key)) {
          LOG.warn("create parent when rename or delete, "
              + "common sense the code won't reach here");
          store.storeEmptyFile(store.prefixToDir(key));
        }
      }
    }
  }

  @Override
  public boolean rename(Path srcPath, Path dstPath)
      throws IOException {
    srcPath = makeAbsolute(srcPath);
    dstPath = makeAbsolute(dstPath);

    if (srcPath.equals(dstPath)) {
      return true;
    }

    FileStatus srcStatus;
    try {
      srcStatus = getFileStatus(srcPath);
    } catch (FileNotFoundException e) {
      return false;
    }

    if (store.isHierarchy()) {
      store.rename(pathToKey(srcPath),
          pathToKey(dstPath), true);
      return true;
    }

    Path parent = dstPath.getParent();
    while (parent != null && !srcPath.equals(parent)) {
      parent = parent.getParent();
    }
    if (parent != null) {
      return false;
    }

    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dstPath);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }

    if (dstStatus == null) {
      Path dstParent = dstPath.getParent();
      if (dstParent != null) {
        Path currentPath = dstParent;
        while (currentPath != null
            && !currentPath.isRoot()) {
          FileStatus ancestorStatus;
          try {
            ancestorStatus = getFileStatus(currentPath);
            if (!ancestorStatus.isDirectory()) {
              throw new IOException(String.format(
                  "Failed to rename %s to %s, %s is a file",
                  srcPath, dstPath, currentPath));
            }
            break;
          } catch (FileNotFoundException fnde) {
            currentPath = currentPath.getParent();
          }
        }
      }
      LOG.debug("Parent directory {} does not exist "
          + "or will be implicitly created during rename",
          dstPath.getParent());
    } else {
      if (dstStatus.isDirectory()) {
        dstPath = new Path(dstPath, srcPath.getName());
        FileStatus status;
        try {
          status = getFileStatus(dstPath);
        } catch (FileNotFoundException fnde) {
          status = null;
        }
        if (status != null) {
          if (status.isFile()) {
            LOG.debug("Deleting existing destination "
                + "file {} before rename", dstPath);
            delete(dstPath, false);
          } else {
            throw new FileAlreadyExistsException(
                String.format(
                    "Failed to rename %s to %s, "
                        + "file already exists or not empty!",
                    srcPath, dstPath));
          }
        }
      } else {
        LOG.debug("Deleting existing destination "
            + "file {} before rename", dstPath);
        delete(dstPath, false);
      }
    }

    store.rename(pathToKey(srcPath), pathToKey(dstPath),
        srcStatus.isFile());
    return true;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean truncate(Path f, long newLength)
      throws IOException {
    throw new UnsupportedOperationException(
        "Truncate is not supported by BaiduBosFileSystem");
  }

  /**
   * Set the working directory to the given directory.
   *
   * @param newDir the new working directory
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public String getCanonicalServiceName() {
    return null;
  }
}
