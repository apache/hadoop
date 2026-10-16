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

package org.apache.hadoop.fs.gs;

import static org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.gs.Constants.GCS_CONFIG_PREFIX;
import static org.apache.hadoop.fs.gs.GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.thirdparty.com.google.common.base.Ascii;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GoogleHadoopFileSystem is rooted in a single bucket at initialization time; in this case, Hadoop
 * paths no longer correspond directly to general GCS paths, and all Hadoop operations going through
 * this FileSystem will never touch any GCS bucket other than the bucket on which this FileSystem is
 * rooted.
 *
 * <p>This implementation sacrifices a small amount of cross-bucket interoperability in favor of
 * more straightforward FileSystem semantics and compatibility with existing Hadoop applications. In
 * particular, it is not subject to bucket-naming constraints, and files are allowed to be placed in
 * root.
 */
public class GoogleHadoopFileSystem extends FileSystem implements IOStatisticsSource {

  public static final Logger LOG = LoggerFactory.getLogger(GoogleHadoopFileSystem.class);

  /**
   * URI scheme for GoogleHadoopFileSystem.
   */
  private static final String SCHEME = Constants.SCHEME;

  /**
   * Default value of replication factor.
   */
  static final short REPLICATION_FACTOR_DEFAULT = 3;

  // TODO: Take this from config
  private static final int PERMISSIONS_TO_REPORT = 700;

  /**
   * The URI the File System is passed in initialize.
   */
  private URI initUri;

  /**
   * Default block size. Note that this is the size that is reported to Hadoop FS clients. It does
   * not modify the actual block size of an underlying GCS object, because GCS JSON API does not
   * allow modifying or querying the value. Modifying this value allows one to control how many
   * mappers are used to process a given file.
   */
  private final long defaultBlockSize = GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getDefault();

  // The bucket the file system is rooted in used for default values of:
  // -- working directory
  // -- user home directories (only for Hadoop purposes).
  private Path fsRoot;

  /**
   * Current working directory; overridden in initialize() if {@link
   * GoogleHadoopFileSystemConfiguration#GCS_WORKING_DIRECTORY} is set.
   */
  private Path workingDirectory;
  private GoogleCloudStorageFileSystem gcsFs;
  private boolean isClosed;
  private FsPermission reportedPermissions;

  /**
   * Setting this to static inorder to have a singleton instance. This will help us get the JVM
   * level metrics. Note that we use this to generate Global Storage Statistics. If we make this
   * an instance field, only the first filesystem instance metrics will be updated since while
   * initializing GlobalStorageStatistics (refer initialize()) only the first instance will be
   * registered.
   *
   * For filesystem instance level instrumentation, one more per instance object can be created
   * and both be updated.
   */
  private static GcsInstrumentation instrumentation = new GcsInstrumentation();
  private GcsStorageStatistics storageStatistics;


  GoogleHadoopFileSystemConfiguration getFileSystemConfiguration() {
    return fileSystemConfiguration;
  }

  private GoogleHadoopFileSystemConfiguration fileSystemConfiguration;

  @Override
  public void initialize(final URI path, Configuration config) throws IOException {
    LOG.trace("initialize(path: {}, config: {})", path, config);

    checkArgument(path != null, "path must not be null");
    checkArgument(config != null, "config must not be null");
    checkArgument(path.getScheme() != null, "scheme of path must not be null");
    checkArgument(path.getScheme().equals(getScheme()), "URI scheme not supported: {}", path);

    config =
        ProviderUtils.excludeIncompatibleCredentialProviders(config, GoogleHadoopFileSystem.class);
    super.initialize(path, config);

    initUri = path;

    // Set this configuration as the default config for this instance; configure()
    // will perform some file-system-specific adjustments, but the original should
    // be sufficient (and is required) for the delegation token binding initialization.
    setConf(config);

    storageStatistics = createStorageStatistics(
            requireNonNull(getIOStatistics()));

    this.reportedPermissions = new FsPermission(PERMISSIONS_TO_REPORT);

    initializeFsRoot();

    this.fileSystemConfiguration = new GoogleHadoopFileSystemConfiguration(config);
    initializeWorkingDirectory(fileSystemConfiguration);
    initializeGcsFs(fileSystemConfiguration);
  }

  private static GcsStorageStatistics createStorageStatistics(
          final IOStatistics ioStatistics) {
    return (GcsStorageStatistics) GlobalStorageStatistics.INSTANCE
            .put(GcsStorageStatistics.NAME, () -> new GcsStorageStatistics(ioStatistics));
  }

  private void initializeFsRoot() {
    String rootBucket = initUri.getAuthority();
    checkArgument(rootBucket != null, "No bucket specified in GCS URI: {}", initUri);
    // Validate root bucket name
    URI rootUri = UriPaths.fromStringPathComponents(rootBucket, /* objectName= */
        null, /* allowEmptyObjectName= */ true);
    fsRoot = new Path(rootUri);
    LOG.trace("Configured FS root: '{}'", fsRoot);
  }

  private void initializeWorkingDirectory(final GoogleHadoopFileSystemConfiguration config) {
    String configWorkingDirectory = config.getWorkingDirectory();
    if (isNullOrEmpty(configWorkingDirectory)) {
      LOG.warn("No working directory configured, using default: '{}'", workingDirectory);
    }
    // Use the public method to ensure proper behavior of normalizing and resolving the new
    // working directory relative to the initial filesystem-root directory.
    setWorkingDirectory(
        isNullOrEmpty(configWorkingDirectory) ? fsRoot : new Path(configWorkingDirectory));
    LOG.trace("Configured working directory: {} = {}", GCS_WORKING_DIRECTORY.getKey(),
        getWorkingDirectory());
  }

  private synchronized void initializeGcsFs(final GoogleHadoopFileSystemConfiguration config)
      throws IOException {
    this.gcsFs = createGcsFs(config);
  }

  private GoogleCloudStorageFileSystem createGcsFs(final GoogleHadoopFileSystemConfiguration config)
      throws IOException {
    GoogleCredentials credentials = getCredentials(config);
    return new GoogleCloudStorageFileSystem(config, credentials);
  }

  private GoogleCredentials getCredentials(GoogleHadoopFileSystemConfiguration config)
      throws IOException {
    return getCredentials(config, GCS_CONFIG_PREFIX);
  }

  static GoogleCredentials getCredentials(GoogleHadoopFileSystemConfiguration config,
      String... keyPrefixesVararg) throws IOException {
    return HadoopCredentialsConfiguration.getCredentials(config.getConfig(), keyPrefixesVararg);
  }

  @Override
  protected void checkPath(final Path path) {
    LOG.trace("checkPath(path: {})", path);
    // Validate scheme
    URI uri = path.toUri();

    String scheme = uri.getScheme();
    if (scheme != null && !scheme.equalsIgnoreCase(getScheme())) {
      throw new IllegalArgumentException(
          String.format("Wrong scheme: %s, in path: %s, expected scheme: %s", scheme, path,
              getScheme()));
    }

    String bucket = uri.getAuthority();
    String rootBucket = fsRoot.toUri().getAuthority();

    // Bucket-less URIs will be qualified later
    if (bucket == null || bucket.equals(rootBucket)) {
      return;
    }

    throw new IllegalArgumentException(
        String.format("Wrong bucket: %s, in path: %s, expected bucket: %s", bucket, path,
            rootBucket));
  }

  /**
   * Validates that GCS path belongs to this file system. The bucket must match the root bucket
   * provided at initialization time.
   */
  Path getHadoopPath(final URI gcsPath) {
    LOG.trace("getHadoopPath(gcsPath: {})", gcsPath);

    // Handle root. Delegate to getGcsPath on "gs:/" to resolve the appropriate gs://<bucket> URI.
    if (gcsPath.equals(getGcsPath(fsRoot))) {
      return fsRoot;
    }

    StorageResourceId resourceId = StorageResourceId.fromUriPath(gcsPath, true);

    checkArgument(!resourceId.isRoot(), "Missing authority in gcsPath '{}'", gcsPath);
    String rootBucket = fsRoot.toUri().getAuthority();
    checkArgument(resourceId.getBucketName().equals(rootBucket),
        "Authority of URI '{}' doesn't match root bucket '{}'", resourceId.getBucketName(),
        rootBucket);

    Path hadoopPath = new Path(fsRoot,
        new Path(/* schema= */ null, /* authority= */ null, resourceId.getObjectName()));
    LOG.trace("getHadoopPath(gcsPath: {}): {}", gcsPath, hadoopPath);
    return hadoopPath;
  }

  /**
   * Translates a "gs:/" style hadoopPath (or relative path which is not fully-qualified) into the
   * appropriate GCS path which is compatible with the underlying GcsFs.
   */
  URI getGcsPath(final Path hadoopPath) {
    LOG.trace("getGcsPath(hadoopPath: {})", hadoopPath);

    // Convert to fully qualified absolute path; the Path object will call back to get our current
    // workingDirectory as part of fully resolving the path.
    Path resolvedPath = makeQualified(hadoopPath);

    String objectName = resolvedPath.toUri().getPath();
    if (objectName != null && resolvedPath.isAbsolute()) {
      // Strip off leading '/' because GoogleCloudStorageFileSystem.getPath appends it explicitly
      // between bucket and objectName.
      objectName = objectName.substring(1);
    }

    // Construct GCS path URI
    String rootBucket = fsRoot.toUri().getAuthority();
    URI gcsPath =
        UriPaths.fromStringPathComponents(rootBucket, objectName, /* allowEmptyObjectName= */ true);
    LOG.trace("getGcsPath(hadoopPath: {}): {}", hadoopPath, gcsPath);
    return gcsPath;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(final Path hadoopPath, final int bufferSize) throws IOException {
    return runOperation(
      GcsStatistics.INVOCATION_OPEN,
      () -> {
        LOG.trace("open({})", hadoopPath);
        URI gcsPath = getGcsPath(hadoopPath);
        return new FSDataInputStream(GoogleHadoopFSInputStream.create(this, gcsPath, statistics));
      },
      String.format("open(%s)", hadoopPath));
  }

  @Override
  public FSDataOutputStream create(
          Path hadoopPath, FsPermission permission, boolean overwrite, int bufferSize,
          short replication, long blockSize, Progressable progress) throws IOException {
    return runOperation(
      GcsStatistics.INVOCATION_CREATE,
      () -> {
        checkArgument(hadoopPath != null, "hadoopPath must not be null");
        checkArgument(replication > 0, "replication must be a positive integer: %s", replication);
        checkArgument(blockSize > 0, "blockSize must be a positive integer: %s", blockSize);

        checkOpen();

        LOG.trace("create(hadoopPath: {}, overwrite: {}, bufferSize: {} [ignored])", hadoopPath,
                overwrite, bufferSize);

        CreateFileOptions.WriteMode writeMode = overwrite ?
                CreateFileOptions.WriteMode.OVERWRITE : CreateFileOptions.WriteMode.CREATE_NEW;

        CreateFileOptions fileOptions = CreateFileOptions.builder().setWriteMode(writeMode).build();
        return new FSDataOutputStream(new GoogleHadoopOutputStream(
                this, getGcsPath(hadoopPath), fileOptions, statistics), statistics);
      },
      String.format("create(%s, %s)", hadoopPath, overwrite));
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path hadoopPath,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    URI gcsPath = getGcsPath(checkNotNull(hadoopPath, "hadoopPath must not be null"));
    URI parentGcsPath = UriPaths.getParentPath(gcsPath);
    if (!getGcsFs().getFileInfo(parentGcsPath).exists()) {
      throw new FileNotFoundException(
          String.format(
              "Can not create '%s' file, because parent folder does not exist: %s",
              gcsPath, parentGcsPath));
    }

    return create(
        hadoopPath,
        permission,
        flags.contains(CreateFlag.OVERWRITE),
        bufferSize,
        replication,
        blockSize,
        progress);
  }

  /**
   * Appends to an existing file (optional operation). Not supported.
   *
   * @param hadoopPath The existing file to be appended.
   * @param bufferSize The size of the buffer to be used.
   * @param progress For reporting progress if it is not null.
   * @return A writable stream.
   * @throws IOException if an error occurs.
   */
  @Override
  public FSDataOutputStream append(Path hadoopPath, int bufferSize, Progressable progress)
      throws IOException {
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");
    LOG.trace("append(hadoopPath: {}, bufferSize: {} [ignored])", hadoopPath, bufferSize);
    URI filePath = getGcsPath(hadoopPath);
    return new FSDataOutputStream(
        new GoogleHadoopOutputStream(
            this,
            filePath,
            CreateFileOptions.builder()
                .setWriteMode(CreateFileOptions.WriteMode.APPEND)
                .build(),
            statistics),
        statistics);
  }

  /**
   * Concat existing files into one file.
   *
   * @param tgt the path to the target destination.
   * @param srcs the paths to the sources to use for the concatenation.
   * @throws IOException IO failure
   */
  @Override
  public void concat(Path tgt, Path[] srcs) throws IOException {
    LOG.trace("concat(tgt: {}, srcs.length: {})", tgt, srcs.length);

    Preconditions.checkArgument(srcs.length > 0, "srcs must have at least one source");

    URI tgtPath = getGcsPath(tgt);
    List<URI> srcPaths = Arrays.stream(srcs).map(this::getGcsPath).collect(toImmutableList());

    Preconditions.checkArgument(
        !srcPaths.contains(tgtPath),
        "target must not be contained in sources");

    List<List<URI>> partitions =
        Lists.partition(srcPaths, Constants.MAX_COMPOSE_OBJECTS - 1);
    LOG.trace("concat(tgt: {}, {} partitions: {})", tgt, partitions.size(), partitions);
    for (List<URI> partition : partitions) {
      // We need to include the target in the list of sources to compose since
      // the GCS FS compose operation will overwrite the target, whereas the Hadoop
      // concat operation appends to the target.
      List<URI> sources = Lists.newArrayList(tgtPath);
      sources.addAll(partition);
      getGcsFs().compose(sources, tgtPath, CreateFileOptions.DEFAULT.getContentType());
    }
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    return runOperation(GcsStatistics.INVOCATION_RENAME,
      () -> {
        LOG.trace("rename({}, {})", src, dst);

        checkArgument(src != null, "src must not be null");
        checkArgument(dst != null, "dst must not be null");

        // Even though the underlying GCSFS will also throw an IAE if src is root, since our
        // filesystem root happens to equal the global root, we want to explicitly check it
        // here since derived classes may not have filesystem roots equal to the global root.
        if (this.makeQualified(src).equals(fsRoot)) {
          LOG.trace("rename(src: {}, dst: {}): false [src is a root]", src, dst);
          return false;
        }

        try {
          checkOpen();

          URI srcPath = getGcsPath(src);
          URI dstPath = getGcsPath(dst);
          getGcsFs().rename(srcPath, dstPath);

          LOG.trace("rename(src: {}, dst: {}): true", src, dst);
        } catch (IOException e) {
          if (ApiErrorExtractor.INSTANCE.requestFailure(e)) {
            throw e;
          }
          LOG.trace("rename(src: {}, dst: {}): false [failed]", src, dst, e);
          return false;
        }

        return true;
      },
      String.format("rename(%s, %s)", src, dst));
  }

  @Override
  public boolean delete(final Path hadoopPath, final boolean recursive) throws IOException {
    return runOperation(GcsStatistics.INVOCATION_DELETE,
      () -> {
        LOG.trace("delete({}, {})", hadoopPath, recursive);
        checkArgument(hadoopPath != null, "hadoopPath must not be null");

        checkOpen();

        URI gcsPath = getGcsPath(hadoopPath);
        try {
          getGcsFs().delete(gcsPath, recursive);
        } catch (DirectoryNotEmptyException e) {
          throw e;
        } catch (IOException e) {
          if (ApiErrorExtractor.INSTANCE.requestFailure(e)) {
            throw e;
          }

          LOG.trace("delete(hadoopPath: {}, recursive: {}): false [failed]",
                  hadoopPath, recursive, e);
          return false;
        }

        LOG.trace("delete(hadoopPath: {}, recursive: {}): true",
                hadoopPath, recursive);
        return true;
      },
      String.format("delete(%s,%s", hadoopPath, recursive));
  }

  private <B> B runOperation(GcsStatistics stat, CallableRaisingIOE<B> operation, String context)
          throws IOException {
    LOG.trace("{}({})", stat, context);
    return trackDuration(instrumentation.getIOStatistics(), stat.getSymbol(), operation);
  }

  @Override
  public FileStatus[] listStatus(final Path hadoopPath) throws IOException {
    return runOperation(
      GcsStatistics.INVOCATION_LIST_STATUS,
      () -> {
        checkArgument(hadoopPath != null, "hadoopPath must not be null");

        checkOpen();

        LOG.trace("listStatus(hadoopPath: {})", hadoopPath);

        URI gcsPath = getGcsPath(hadoopPath);
        List<FileStatus> status;

        try {
          List<FileInfo> fileInfos = getGcsFs().listDirectory(gcsPath);
          status = new ArrayList<>(fileInfos.size());
          String userName = getUgiUserName();
          for (FileInfo fileInfo : fileInfos) {
            status.add(getFileStatus(fileInfo, userName));
          }
        } catch (FileNotFoundException fnfe) {
          throw (FileNotFoundException)
                  new FileNotFoundException(
                          String.format(
                                  "listStatus(hadoopPath: %s): '%s' does not exist.",
                                  hadoopPath, gcsPath))
                          .initCause(fnfe);
        }

        return status.toArray(new FileStatus[0]);
      },
      String.format("listStatus(%s", hadoopPath));
  }

  /**
   * Overridden to make root its own parent. This is POSIX compliant, but more importantly guards
   * against poor directory accounting in the PathData class of Hadoop 2's FsShell.
   */
  @Override
  public Path makeQualified(final Path path) {
    Path qualifiedPath = super.makeQualified(path);

    URI uri = qualifiedPath.toUri();

    checkState("".equals(uri.getPath()) || qualifiedPath.isAbsolute(),
        "Path '{}' must be fully qualified.", qualifiedPath);

    Path result;
    String upath = uri.getPath();

    // Strip initial '..'s to make root is its own parent.
    int i = 0;
    while (upath.startsWith("/../", i)) {
      // Leave a preceding slash, so path is still absolute.
      i += 3;
    }
    if (i == upath.length() || upath.substring(i).equals("/..")) {
      // Allow a Path of gs://someBucket to map to gs://someBucket/
      result = new Path(uri.getScheme(), uri.getAuthority(), "/");
    } else if (i == 0) {
      result = qualifiedPath;
    } else {
      result = new Path(uri.getScheme(), uri.getAuthority(), upath.substring(i));
    }

    LOG.trace("makeQualified(path: {}): {}", path, result);
    return result;
  }

  /**
   * Returns a URI of the root of this FileSystem.
   */
  @Override
  public URI getUri() {
    return fsRoot.toUri();
  }

  /**
   * The default port is listed as -1 as an indication that ports are not used.
   */
  @Override
  protected int getDefaultPort() {
    int result = -1;
    LOG.trace("getDefaultPort(): {}", result);
    return result;
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability) {
    checkNotNull(path, "path must not be null");
    checkArgument(!isNullOrEmpty(capability), "capability must not be null or empty string for {}",
        path);
    switch (Ascii.toLowerCase(capability)) {
    case CommonPathCapabilities.FS_APPEND:
    case CommonPathCapabilities.FS_CONCAT:
      return true;
    default:
      return false;
    }
  }

  /**
   * Gets the current working directory.
   *
   * @return The current working directory.
   */
  @Override
  public Path getWorkingDirectory() {
    LOG.trace("getWorkingDirectory(): {}", workingDirectory);
    return workingDirectory;
  }

  @Override
  public boolean mkdirs(final Path hadoopPath, final FsPermission permission) throws IOException {
    return runOperation(
      GcsStatistics.INVOCATION_MKDIRS,
      () -> {
        checkArgument(hadoopPath != null, "hadoopPath must not be null");

        LOG.trace("mkdirs(hadoopPath: {}, permission: {}): true", hadoopPath, permission);

        checkOpen();

        URI gcsPath = getGcsPath(hadoopPath);
        try {
          getGcsFs().mkdirs(gcsPath);
        } catch (java.nio.file.FileAlreadyExistsException faee) {
          // Need to convert to the Hadoop flavor of FileAlreadyExistsException.
          throw (FileAlreadyExistsException)
                  new FileAlreadyExistsException(
                          String.format(
                                  "mkdirs(hadoopPath: %s, permission: %s): failed",
                                  hadoopPath, permission))
                          .initCause(faee);
        }

        return true;
      },
      String.format("mkdirs(%s)", hadoopPath));
  }

  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    return runOperation(
      GcsStatistics.INVOCATION_GET_FILE_STATUS,
      () -> {
        checkArgument(path != null, "path must not be null");

        checkOpen();

        URI gcsPath = getGcsPath(path);
        FileInfo fileInfo = getGcsFs().getFileInfo(gcsPath);
        if (!fileInfo.exists()) {
          throw new FileNotFoundException(
                  String.format(
                          "%s not found: %s", fileInfo.isDirectory() ? "Directory" : "File", path));
        }
        String userName = getUgiUserName();
        return getFileStatus(fileInfo, userName);
      },
      String.format("getFileStatus(%s)", path));
  }

  /**
   * Returns home directory of the current user.
   *
   * <p>Note: This directory is only used for Hadoop purposes. It is not the same as a user's OS
   * home directory.
   */
  @Override
  public Path getHomeDirectory() {
    Path result = new Path(fsRoot, "user/" + System.getProperty("user.name"));
    LOG.trace("getHomeDirectory(): {}", result);
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns the service if delegation tokens are configured, otherwise, null.
   */
  @Override
  public String getCanonicalServiceName() {
    // TODO: Add delegation token support
    return null;
  }

  /**
   * Gets GCS FS instance.
   */
  GoogleCloudStorageFileSystem getGcsFs() {
    return gcsFs;
  }

  /**
   * Assert that the FileSystem has been initialized and not close()d.
   */
  private void checkOpen() throws IOException {
    if (isClosed) {
      throw new IOException("GoogleHadoopFileSystem has been closed or not initialized.");
    }
  }

  @Override
  public void close() throws IOException {
    LOG.trace("close()");
    if (isClosed) {
      return;
    }

    super.close();

    getGcsFs().close();

    this.isClosed = true;
  }

  @Override
  public long getUsed() throws IOException {
    long result = super.getUsed();
    LOG.trace("getUsed(): {}", result);
    return result;
  }

  @Override
  public void setWorkingDirectory(final Path hadoopPath) {
    checkArgument(hadoopPath != null, "hadoopPath must not be null");
    URI gcsPath = UriPaths.toDirectory(getGcsPath(hadoopPath));
    workingDirectory = getHadoopPath(gcsPath);
    LOG.trace("setWorkingDirectory(hadoopPath: {}): {}", hadoopPath, workingDirectory);
  }

  /**
   * Get the instrumentation's IOStatistics.
   * @return statistics
   */
  @Override
  public IOStatistics getIOStatistics() {
    return instrumentation != null
            ? instrumentation.getIOStatistics()
            : null;
  }

  /**
   * Get the storage statistics of this filesystem.
   * @return the storage statistics
   */
  @Override
  public GcsStorageStatistics getStorageStatistics() {
    return this.storageStatistics;
  }

  private static String getUgiUserName() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return ugi.getShortUserName();
  }

  private FileStatus getFileStatus(FileInfo fileInfo, String userName) {
    checkNotNull(fileInfo, "fileInfo should not be null");
    // GCS does not provide modification time. It only provides creation time.
    // It works for objects because they are immutable once created.
    FileStatus status = new FileStatus(
        fileInfo.getSize(),
        fileInfo.isDirectory(),
        REPLICATION_FACTOR_DEFAULT,
        defaultBlockSize,
        fileInfo.getModificationTime(),
        fileInfo.getModificationTime(),
        reportedPermissions,
        userName,
        userName,
        getHadoopPath(fileInfo.getPath()));
    LOG.trace("FileStatus(path: {}, userName: {}): {}", fileInfo.getPath(), userName, status);
    return status;
  }
}
