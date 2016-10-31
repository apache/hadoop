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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Listing.ACCEPT_ALL;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core S3A Filesystem implementation.
 *
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(Configuration)} and variants to
 * create one.
 *
 * If cast to {@code S3AFileSystem}, extra methods and features may be accessed.
 * Consider those private and unstable.
 *
 * Because it prints some of the state of the instrumentation,
 * the output of {@link #toString()} must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AFileSystem extends FileSystem {
  /**
   * Default blocksize as used in blocksize and FS status queries.
   */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;
  private URI uri;
  private Path workingDir;
  private String username;
  private AmazonS3 s3;
  private String bucket;
  private int maxKeys;
  private Listing listing;
  private long partSize;
  private boolean enableMultiObjectsDelete;
  private TransferManager transfers;
  private ListeningExecutorService threadPoolExecutor;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem.class);
  private static final Logger PROGRESS =
      LoggerFactory.getLogger("org.apache.hadoop.fs.s3a.S3AFileSystem.Progress");
  private LocalDirAllocator directoryAllocator;
  private CannedAccessControlList cannedACL;
  private String serverSideEncryptionAlgorithm;
  private S3AInstrumentation instrumentation;
  private S3AStorageStatistics storageStatistics;
  private long readAhead;
  private S3AInputPolicy inputPolicy;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  // The maximum number of entries that can be deleted in any call to s3
  private static final int MAX_ENTRIES_TO_DELETE = 1000;
  private boolean blockUploadEnabled;
  private String blockOutputBuffer;
  private S3ADataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    try {
      instrumentation = new S3AInstrumentation(name);

      uri = S3xLoginHelper.buildFSURI(name);
      // Username is the current user at the time the FS was instantiated.
      username = UserGroupInformation.getCurrentUser().getShortUserName();
      workingDir = new Path("/user", username)
          .makeQualified(this.uri, this.getWorkingDirectory());

      bucket = name.getHost();

      Class<? extends S3ClientFactory> s3ClientFactoryClass = conf.getClass(
          S3_CLIENT_FACTORY_IMPL, DEFAULT_S3_CLIENT_FACTORY_IMPL,
          S3ClientFactory.class);
      s3 = ReflectionUtils.newInstance(s3ClientFactoryClass, conf)
          .createS3Client(name, uri);

      maxKeys = intOption(conf, MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS, 1);
      listing = new Listing(this);
      partSize = getMultipartSizeProperty(conf,
          MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
      multiPartThreshold = getMultipartSizeProperty(conf,
          MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

      //check but do not store the block size
      longBytesOption(conf, FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE, 1);
      enableMultiObjectsDelete = conf.getBoolean(ENABLE_MULTI_DELETE, true);

      readAhead = longBytesOption(conf, READAHEAD_RANGE,
          DEFAULT_READAHEAD_RANGE, 0);
      storageStatistics = (S3AStorageStatistics)
          GlobalStorageStatistics.INSTANCE
              .put(S3AStorageStatistics.NAME,
                  new GlobalStorageStatistics.StorageStatisticsProvider() {
                    @Override
                    public StorageStatistics provide() {
                      return new S3AStorageStatistics();
                    }
                  });

      int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
      if (maxThreads < 2) {
        LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
        maxThreads = 2;
      }
      int totalTasks = intOption(conf,
          MAX_TOTAL_TASKS, DEFAULT_MAX_TOTAL_TASKS, 1);
      long keepAliveTime = longOption(conf, KEEPALIVE_TIME,
          DEFAULT_KEEPALIVE_TIME, 0);
      threadPoolExecutor = BlockingThreadPoolExecutorService.newInstance(
          maxThreads,
          maxThreads + totalTasks,
          keepAliveTime, TimeUnit.SECONDS,
          "s3a-transfer-shared");

      initTransferManager();

      initCannedAcls(conf);

      verifyBucketExists();

      initMultipartUploads(conf);

      serverSideEncryptionAlgorithm =
          conf.getTrimmed(SERVER_SIDE_ENCRYPTION_ALGORITHM);
      LOG.debug("Using encryption {}", serverSideEncryptionAlgorithm);
      inputPolicy = S3AInputPolicy.getPolicy(
          conf.getTrimmed(INPUT_FADVISE, INPUT_FADV_NORMAL));

      blockUploadEnabled = conf.getBoolean(FAST_UPLOAD, DEFAULT_FAST_UPLOAD);

      if (blockUploadEnabled) {
        blockOutputBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER,
            DEFAULT_FAST_UPLOAD_BUFFER);
        partSize = ensureOutputParameterInRange(MULTIPART_SIZE, partSize);
        blockFactory = S3ADataBlocks.createFactory(this, blockOutputBuffer);
        blockOutputActiveBlocks = intOption(conf,
            FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
        LOG.debug("Using S3ABlockOutputStream with buffer = {}; block={};" +
                " queue limit={}",
            blockOutputBuffer, partSize, blockOutputActiveBlocks);
      } else {
        LOG.debug("Using S3AOutputStream");
      }
    } catch (AmazonClientException e) {
      throw translateException("initializing ", new Path(name), e);
    }

  }

  /**
   * Verify that the bucket exists. This does not check permissions,
   * not even read access.
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException any other problem talking to S3
   */
  protected void verifyBucketExists()
      throws FileNotFoundException, IOException {
    try {
      if (!s3.doesBucketExist(bucket)) {
        throw new FileNotFoundException("Bucket " + bucket + " does not exist");
      }
    } catch (AmazonS3Exception e) {
      // this is a sign of a serious startup problem so do dump everything
      LOG.warn(stringify(e), e);
      throw translateException("doesBucketExist", bucket, e);
    } catch (AmazonServiceException e) {
      // this is a sign of a serious startup problem so do dump everything
      LOG.warn(stringify(e), e);
      throw translateException("doesBucketExist", bucket, e);
    } catch (AmazonClientException e) {
      throw translateException("doesBucketExist", bucket, e);
    }
  }

  /**
   * Get S3A Instrumentation. For test purposes.
   * @return this instance's instrumentation.
   */
  public S3AInstrumentation getInstrumentation() {
    return instrumentation;
  }

  private void initTransferManager() {
    TransferManagerConfiguration transferConfiguration =
        new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    transfers = new TransferManager(s3, threadPoolExecutor);
    transfers.setConfiguration(transferConfiguration);
  }

  private void initCannedAcls(Configuration conf) {
    String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
    if (!cannedACLName.isEmpty()) {
      cannedACL = CannedAccessControlList.valueOf(cannedACLName);
    } else {
      cannedACL = null;
    }
  }

  private void initMultipartUploads(Configuration conf) throws IOException {
    boolean purgeExistingMultipart = conf.getBoolean(PURGE_EXISTING_MULTIPART,
        DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge = longOption(conf,
        PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

    if (purgeExistingMultipart) {
      Date purgeBefore =
          new Date(new Date().getTime() - purgeExistingMultipartAge * 1000);

      try {
        transfers.abortMultipartUploads(bucket, purgeBefore);
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() == 403) {
          instrumentation.errorIgnored();
          LOG.debug("Failed to purging multipart uploads against {}," +
              " FS may be read only", bucket, e);
        } else {
          throw translateException("purging multipart uploads", bucket, e);
        }
      }
    }
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return "s3a"
   */
  @Override
  public String getScheme() {
    return "s3a";
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public int getDefaultPort() {
    return Constants.S3A_DEFAULT_PORT;
  }

  /**
   * Returns the S3 client used by this filesystem.
   * @return AmazonS3Client
   */
  @VisibleForTesting
  AmazonS3 getAmazonS3Client() {
    return s3;
  }

  /**
   * Returns the read ahead range value used by this filesystem
   * @return
   */

  @VisibleForTesting
  long getReadAheadRange() {
    return readAhead;
  }

  /**
   * Get the input policy for this FS instance.
   * @return the input policy
   */
  @InterfaceStability.Unstable
  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  /**
   * Demand create the directory allocator, then create a temporary file.
   * {@link LocalDirAllocator#createTmpFileForWrite(String, long, Configuration)}.
   *  @param pathStr prefix for the temporary file
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return a unique temporary file
   *  @throws IOException IO problems
   */
  synchronized File createTmpFileForWrite(String pathStr, long size,
      Configuration conf) throws IOException {
    if (directoryAllocator == null) {
      String bufferDir = conf.get(BUFFER_DIR) != null
          ? BUFFER_DIR : "hadoop.tmp.dir";
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }
    return directoryAllocator.createTmpFileForWrite(pathStr, size, conf);
  }

  /**
   * Get the bucket of this filesystem.
   * @return the bucket
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * Change the input policy for this FS.
   * @param inputPolicy new policy
   */
  @InterfaceStability.Unstable
  public void setInputPolicy(S3AInputPolicy inputPolicy) {
    Objects.requireNonNull(inputPolicy, "Null inputStrategy");
    LOG.debug("Setting input strategy: {}", inputPolicy);
    this.inputPolicy = inputPolicy;
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key.
   *
   * @param path input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key, adding a trailing
   * "/" if the path is not the root <i>and</i> does not already have a "/"
   * at the end.
   *
   * @param key s3 key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  private String maybeAddTrailingSlash(String key) {
    if (!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Convert a path back to a key.
   * @param key input key
   * @return the path from this key
   */
  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  /**
   * Convert a key to a fully qualified path.
   * @param key input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  Path keyToQualifiedPath(String key) {
    return qualify(keyToPath(key));
  }

  /**
   * Qualify a path.
   * @param path path to qualify
   * @return a qualified path.
   */
  Path qualify(Path path) {
    return path.makeQualified(uri, workingDir);
  }

  /**
   * Check that a Path belongs to this FileSystem.
   * Unlike the superclass, this version does not look at authority,
   * only hostnames.
   * @param path to check
   * @throws IllegalArgumentException if there is an FS mismatch
   */
  @Override
  public void checkPath(Path path) {
    S3xLoginHelper.checkPath(getConf(), getUri(), path, getDefaultPort());
  }

  @Override
  protected URI canonicalizeUri(URI rawUri) {
    return S3xLoginHelper.canonicalizeUri(rawUri, getDefaultPort());
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {

    LOG.debug("Opening '{}' for reading.", f);
    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + f
          + " because it is a directory");
    }

    return new FSDataInputStream(new S3AInputStream(bucket, pathToKey(f),
      fileStatus.getLen(), s3, statistics, instrumentation, readAhead,
        inputPolicy));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission the permission to set.
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the requested block size.
   * @param progress the progress reporter.
   * @throws IOException in the event of IO related errors.
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    String key = pathToKey(f);
    S3AFileStatus status = null;
    try {
      // get the status or throw an FNFE
      status = getFileStatus(f);

      // if the thread reaches here, there is something at the path
      if (status.isDirectory()) {
        // path references a directory: automatic error
        throw new FileAlreadyExistsException(f + " is a directory");
      }
      if (!overwrite) {
        // path references a file and overwrite is disabled
        throw new FileAlreadyExistsException(f + " already exists");
      }
      LOG.debug("Overwriting file {}", f);
    } catch (FileNotFoundException e) {
      // this means the file is not found

    }
    instrumentation.fileCreated();
    FSDataOutputStream output;
    if (blockUploadEnabled) {
      output = new FSDataOutputStream(
          new S3ABlockOutputStream(this,
              key,
              new SemaphoredDelegatingExecutor(threadPoolExecutor,
                  blockOutputActiveBlocks, true),
              progress,
              partSize,
              blockFactory,
              instrumentation.newOutputStreamStatistics(statistics),
              new WriteOperationHelper(key)
          ),
          null);
    } else {

      // We pass null to FSDataOutputStream so it won't count writes that
      // are being buffered to a file
      output = new FSDataOutputStream(
          new S3AOutputStream(getConf(),
              this,
              key,
              progress
          ),
          null);
    }
    return output;
  }

  /**
   * {@inheritDoc}
   * @throws FileNotFoundException if the parent directory is not present -or
   * is not a directory.
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      // expect this to raise an exception if there is no parent
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return create(path, permission,
        flags.contains(CreateFlag.OVERWRITE), bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException indicating that append is not supported.
   */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Append is not supported "
        + "by S3AFileSystem");
  }


  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * Warning: S3 does not support renames. This method does a copy which can
   * take S3 some time to execute with large files and directories. Since
   * there is no Progressable passed in, this can time out jobs.
   *
   * Note: This implementation differs with other S3 drivers. Specifically:
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on IO failure
   * @return true if rename is successful
   */
  public boolean rename(Path src, Path dst) throws IOException {
    try {
      return innerRename(src, dst);
    } catch (AmazonClientException e) {
      throw translateException("rename(" + src +", " + dst + ")", src, e);
    }
  }

  /**
   * The inner rename operation. See {@link #rename(Path, Path)} for
   * the description of the operation.
   * @param src path to be renamed
   * @param dst new path after rename
   * @return true if rename is successful
   * @throws IOException on IO failure.
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  private boolean innerRename(Path src, Path dst) throws IOException,
      AmazonClientException {
    LOG.debug("Rename path {} to {}", src, dst);
    incrementStatistic(INVOCATION_RENAME);

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.isEmpty() || dstKey.isEmpty()) {
      LOG.debug("rename: source {} or dest {}, is empty", srcKey, dstKey);
      return false;
    }

    S3AFileStatus srcStatus;
    try {
      srcStatus = getFileStatus(src);
    } catch (FileNotFoundException e) {
      LOG.error("rename: src not found {}", src);
      return false;
    }

    if (srcKey.equals(dstKey)) {
      LOG.debug("rename: src and dst refer to the same file or directory: {}",
          dst);
      return srcStatus.isFile();
    }

    S3AFileStatus dstStatus = null;
    try {
      dstStatus = getFileStatus(dst);

      if (srcStatus.isDirectory() && dstStatus.isFile()) {
        LOG.debug("rename: src {} is a directory and dst {} is a file",
            src, dst);
        return false;
      }

      if (dstStatus.isDirectory() && !dstStatus.isEmptyDirectory()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      LOG.debug("rename: destination path {} not found", dst);
      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()) {
        try {
          S3AFileStatus dstParentStatus = getFileStatus(dst.getParent());
          if (!dstParentStatus.isDirectory()) {
            return false;
          }
        } catch (FileNotFoundException e2) {
          LOG.debug("rename: destination path {} has no parent {}",
              dst, parent);
          return false;
        }
      }
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      LOG.debug("rename: renaming file {} to {}", src, dst);
      if (dstStatus != null && dstStatus.isDirectory()) {
        String newDstKey = dstKey;
        if (!newDstKey.endsWith("/")) {
          newDstKey = newDstKey + "/";
        }
        String filename =
            srcKey.substring(pathToKey(src.getParent()).length()+1);
        newDstKey = newDstKey + filename;
        copyFile(srcKey, newDstKey, srcStatus.getLen());
      } else {
        copyFile(srcKey, dstKey, srcStatus.getLen());
      }
      innerDelete(srcStatus, false);
    } else {
      LOG.debug("rename: renaming directory {} to {}", src, dst);

      // This is a directory to directory copy
      if (!dstKey.endsWith("/")) {
        dstKey = dstKey + "/";
      }

      if (!srcKey.endsWith("/")) {
        srcKey = srcKey + "/";
      }

      //Verify dest is not a child of the source directory
      if (dstKey.startsWith(srcKey)) {
        LOG.debug("cannot rename a directory {}" +
              " to a subdirectory of self: {}", srcKey, dstKey);
        return false;
      }

      List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();
      if (dstStatus != null && dstStatus.isEmptyDirectory()) {
        // delete unnecessary fake directory.
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(dstKey));
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(srcKey);
      request.setMaxKeys(maxKeys);

      ObjectListing objects = listObjects(request);

      while (true) {
        for (S3ObjectSummary summary : objects.getObjectSummaries()) {
          keysToDelete.add(
              new DeleteObjectsRequest.KeyVersion(summary.getKey()));
          String newDstKey =
              dstKey + summary.getKey().substring(srcKey.length());
          copyFile(summary.getKey(), newDstKey, summary.getSize());

          if (keysToDelete.size() == MAX_ENTRIES_TO_DELETE) {
            removeKeys(keysToDelete, true, false);
          }
        }

        if (objects.isTruncated()) {
          objects = continueListObjects(objects);
        } else {
          if (!keysToDelete.isEmpty()) {
            removeKeys(keysToDelete, false, false);
          }
          break;
        }
      }
    }

    if (src.getParent() != dst.getParent()) {
      deleteUnnecessaryFakeDirectories(dst.getParent());
      createFakeDirectoryIfNecessary(src.getParent());
    }
    return true;
  }

  /**
   * Low-level call to get at the object metadata.
   * @param path path to the object
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @VisibleForTesting
  public ObjectMetadata getObjectMetadata(Path path) throws IOException {
    return getObjectMetadata(pathToKey(path));
  }

  /**
   * Increment a statistic by 1.
   * @param statistic The operation to increment
   */
  protected void incrementStatistic(Statistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementStatistic(Statistic statistic, long count) {
    instrumentation.incrementCounter(statistic, count);
    storageStatistics.incrementCounter(statistic, count);
  }

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  protected void decrementGauge(Statistic statistic, long count) {
    instrumentation.decrementGauge(statistic, count);
  }

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementGauge(Statistic statistic, long count) {
    instrumentation.incrementGauge(statistic, count);
  }

  /**
   * Get the storage statistics of this filesystem.
   * @return the storage statistics
   */
  @Override
  public S3AStorageStatistics getStorageStatistics() {
    return storageStatistics;
  }

  /**
   * Request object metadata; increments counters in the process.
   * @param key key
   * @return the metadata
   */
  protected ObjectMetadata getObjectMetadata(String key) {
    incrementStatistic(OBJECT_METADATA_REQUESTS);
    ObjectMetadata meta = s3.getObjectMetadata(bucket, key);
    incrementReadOperations();
    return meta;
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics
   * in the process.
   * @param request request to initiate
   * @return the results
   */
  protected ObjectListing listObjects(ListObjectsRequest request) {
    incrementStatistic(OBJECT_LIST_REQUESTS);
    incrementReadOperations();
    return s3.listObjects(request);
  }

  /**
   * List the next set of objects.
   * @param objects paged result
   * @return the next result object
   */
  protected ObjectListing continueListObjects(ObjectListing objects) {
    incrementStatistic(OBJECT_CONTINUE_LIST_REQUESTS);
    incrementReadOperations();
    return s3.listNextBatchOfObjects(objects);
  }

  /**
   * Increment read operations.
   */
  public void incrementReadOperations() {
    statistics.incrementReadOps(1);
  }

  /**
   * Increment the write operation counter.
   * This is somewhat inaccurate, as it appears to be invoked more
   * often than needed in progress callbacks.
   */
  public void incrementWriteOperations() {
    statistics.incrementWriteOps(1);
  }

  /**
   * Delete an object.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * @param key key to blob to delete.
   */
  private void deleteObject(String key) throws InvalidRequestException {
    blockRootDelete(key);
    incrementWriteOperations();
    incrementStatistic(OBJECT_DELETE_REQUESTS);
    s3.deleteObject(bucket, key);
  }

  /**
   * Reject any request to delete an object where the key is root.
   * @param key key to validate
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void blockRootDelete(String key) throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException("Bucket "+ bucket
          +" cannot be deleted");
    }
  }

  /**
   * Perform a bulk object delete operation.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * @param deleteRequest keys to delete on the s3-backend
   */
  private void deleteObjects(DeleteObjectsRequest deleteRequest) {
    incrementWriteOperations();
    incrementStatistic(OBJECT_DELETE_REQUESTS, 1);
    s3.deleteObjects(deleteRequest);
  }

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  public PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, File srcfile) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        srcfile);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(metadata);
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data.
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, InputStream inputStream) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        inputStream, metadata);
    putObjectRequest.setCannedAcl(cannedACL);
    return putObjectRequest;
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata() {
    final ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    return om;
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   *
   * @param length length of data to set in header.
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(long length) {
    final ObjectMetadata om = newObjectMetadata();
    if (length >= 0) {
      om.setContentLength(length);
    }
    return om;
  }

  /**
   * PUT an object, incrementing the put requests and put bytes
   * counters.
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * @param putObjectRequest the request
   * @return the upload initiated
   */
  public Upload putObject(PutObjectRequest putObjectRequest) {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    incrementPutStartStatistics(len);
    try {
      Upload upload = transfers.upload(putObjectRequest);
      incrementPutCompletedStatistics(true, len);
      return upload;
    } catch (AmazonClientException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws AmazonClientException on problems
   */
  public PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest)
      throws AmazonClientException {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    incrementPutStartStatistics(len);
    try {
      PutObjectResult result = s3.putObject(putObjectRequest);
      incrementPutCompletedStatistics(true, len);
      return result;
    } catch (AmazonClientException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters
   * @param request request
   * @return the result of the operation.
   * @throws AmazonClientException on problems
   */
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws AmazonClientException {
    long len = request.getPartSize();
    incrementPutStartStatistics(len);
    try {
      UploadPartResult uploadPartResult = s3.uploadPart(request);
      incrementPutCompletedStatistics(true, len);
      return uploadPartResult;
    } catch (AmazonClientException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * At the start of a put/multipart upload operation, update the
   * relevant counters.
   *
   * @param bytes bytes in the request.
   */
  public void incrementPutStartStatistics(long bytes) {
    LOG.debug("PUT start {} bytes", bytes);
    incrementWriteOperations();
    incrementStatistic(OBJECT_PUT_REQUESTS);
    incrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
    if (bytes > 0) {
      incrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
  }

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  public void incrementPutCompletedStatistics(boolean success, long bytes) {
    LOG.debug("PUT completed success={}; {} bytes", success, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      incrementStatistic(OBJECT_PUT_BYTES, bytes);
      decrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
    incrementStatistic(OBJECT_PUT_REQUESTS_COMPLETED);
    decrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  public void incrementPutProgressStatistics(String key, long bytes) {
    PROGRESS.debug("PUT {}: {} bytes", key, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      statistics.incrementBytesWritten(bytes);
    }
  }

  /**
   * A helper method to delete a list of keys on a s3-backend.
   *
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param clearKeys clears the keysToDelete-list after processing the list
   *            when set to true
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void removeKeys(List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      boolean clearKeys, boolean deleteFakeDir)
      throws AmazonClientException, InvalidRequestException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      return;
    }
    for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
      blockRootDelete(keyVersion.getKey());
    }
    if (enableMultiObjectsDelete) {
      deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keysToDelete));
    } else {
      for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
        deleteObject(keyVersion.getKey());
      }
    }
    if (!deleteFakeDir) {
      instrumentation.fileDeleted(keysToDelete.size());
    } else {
      instrumentation.fakeDirsDeleted(keysToDelete.size());
    }
    if (clearKeys) {
      keysToDelete.clear();
    }
  }

  /**
   * Delete a Path. This operation is at least {@code O(files)}, with
   * added overheads to enumerate the path. It is also not atomic.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    try {
      return innerDelete(getFileStatus(f), recursive);
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist", f);
      instrumentation.errorIgnored();
      return false;
    } catch (AmazonClientException e) {
      throw translateException("delete", f, e);
    }
  }

  /**
   * Delete an object. See {@link #delete(Path, boolean)}.
   *
   * @param status fileStatus object
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  private boolean innerDelete(S3AFileStatus status, boolean recursive)
      throws IOException, AmazonClientException {
    Path f = status.getPath();
    LOG.debug("Delete path {} - recursive {}", f , recursive);

    String key = pathToKey(f);

    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {}", f);

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (key.equals("/")) {
        return rejectRootDirectoryDelete(status, recursive);
      }

      if (!recursive && !status.isEmptyDirectory()) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }

      if (status.isEmptyDirectory()) {
        LOG.debug("Deleting fake empty directory {}", key);
        deleteObject(key);
        instrumentation.directoryDeleted();
      } else {
        LOG.debug("Getting objects for directory prefix {} to delete", key);

        ListObjectsRequest request = createListObjectsRequest(key, null);

        ObjectListing objects = listObjects(request);
        List<DeleteObjectsRequest.KeyVersion> keys =
            new ArrayList<>(objects.getObjectSummaries().size());
        while (true) {
          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            LOG.debug("Got object to delete {}", summary.getKey());

            if (keys.size() == MAX_ENTRIES_TO_DELETE) {
              removeKeys(keys, true, false);
            }
          }

          if (objects.isTruncated()) {
            objects = continueListObjects(objects);
          } else {
            if (!keys.isEmpty()) {
              removeKeys(keys, false, false);
            }
            break;
          }
        }
      }
    } else {
      LOG.debug("delete: Path is a file");
      instrumentation.fileDeleted(1);
      deleteObject(key);
    }

    Path parent = f.getParent();
    if (parent != null) {
      createFakeDirectoryIfNecessary(parent);
    }
    return true;
  }

  /**
   * Implements the specific logic to reject root directory deletion.
   * The caller must return the result of this call, rather than
   * attempt to continue with the delete operation: deleting root
   * directories is never allowed. This method simply implements
   * the policy of when to return an exit code versus raise an exception.
   * @param status filesystem status
   * @param recursive recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was explicitly rejected.
   */
  private boolean rejectRootDirectoryDelete(S3AFileStatus status,
      boolean recursive) throws IOException {
    LOG.info("s3a delete the {} root directory of {}", bucket, recursive);
    boolean emptyRoot = status.isEmptyDirectory();
    if (emptyRoot) {
      return true;
    }
    if (recursive) {
      return false;
    } else {
      // reject
      throw new PathIOException(bucket, "Cannot delete root path");
    }
  }

  private void createFakeDirectoryIfNecessary(Path f)
      throws IOException, AmazonClientException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      createFakeDirectory(key);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    try {
      return innerListStatus(f);
    } catch (AmazonClientException e) {
      throw translateException("listStatus", f, e);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException due to an IO problem.
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  public FileStatus[] innerListStatus(Path f) throws FileNotFoundException,
      IOException, AmazonClientException {
    Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("List status for path: {}", path);
    incrementStatistic(INVOCATION_LIST_STATUS);

    List<FileStatus> result;
    final FileStatus fileStatus =  getFileStatus(path);

    if (fileStatus.isDirectory()) {
      if (!key.isEmpty()) {
        key = key + '/';
      }

      ListObjectsRequest request = createListObjectsRequest(key, "/");
      LOG.debug("listStatus: doing listObjects for directory {}", key);

      Listing.FileStatusListingIterator files =
          listing.createFileStatusListingIterator(path,
              request,
              ACCEPT_ALL,
              new Listing.AcceptAllButSelfAndS3nDirs(path));
      result = new ArrayList<>(files.getBatchSize());
      while (files.hasNext()) {
        result.add(files.next());
      }
      return result.toArray(new FileStatus[result.size()]);
    } else {
      LOG.debug("Adding: rd (not a dir): {}", path);
      FileStatus[] stats = new FileStatus[1];
      stats[0]= fileStatus;
      return stats;
    }
  }

  /**
   * Create a {@code ListObjectsRequest} request against this bucket,
   * with the maximum keys returned in a query set by {@link #maxKeys}.
   * @param key key for request
   * @param delimiter any delimiter
   * @return the request
   */
  private ListObjectsRequest createListObjectsRequest(String key,
      String delimiter) {
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(bucket);
    request.setMaxKeys(maxKeys);
    request.setPrefix(key);
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return request;
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param newDir the current working directory.
   */
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  /**
   * Get the current working directory for the given file system.
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Get the username of the FS.
   * @return the short name of the user who instantiated the FS
   */
  public String getUsername() {
    return username;
  }

  /**
   *
   * Make the given path and all non-existent parents into
   * directories. Has the semantics of Unix {@code 'mkdir -p'}.
   * Existence of the directory hierarchy is not an error.
   * @param path path to create
   * @param permission to apply to f
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  public boolean mkdirs(Path path, FsPermission permission) throws IOException,
      FileAlreadyExistsException {
    try {
      return innerMkdirs(path, permission);
    } catch (AmazonClientException e) {
      throw translateException("innerMkdirs", path, e);
    }
  }
  /**
   *
   * Make the given path and all non-existent parents into
   * directories.
   * See {@link #mkdirs(Path, FsPermission)}
   * @param f path to create
   * @param permission to apply to f
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  private boolean innerMkdirs(Path f, FsPermission permission)
      throws IOException, FileAlreadyExistsException, AmazonClientException {
    LOG.debug("Making directory: {}", f);
    incrementStatistic(INVOCATION_MKDIRS);
    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(f);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      Path fPart = f.getParent();
      do {
        try {
          fileStatus = getFileStatus(fPart);
          if (fileStatus.isDirectory()) {
            break;
          }
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(String.format(
                "Can't make directory for path '%s' since it is a file.",
                fPart));
          }
        } catch (FileNotFoundException fnfe) {
          instrumentation.errorIgnored();
        }
        fPart = fPart.getParent();
      } while (fPart != null);

      String key = pathToKey(f);
      createFakeDirectory(key);
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws java.io.FileNotFoundException when the path does not exist;
   * @throws IOException on other problems.
   */
  public S3AFileStatus getFileStatus(final Path f) throws IOException {
    incrementStatistic(INVOCATION_GET_FILE_STATUS);
    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({})", path , key);
    if (!key.isEmpty()) {
      try {
        ObjectMetadata meta = getObjectMetadata(key);

        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          LOG.debug("Found exact file: fake directory");
          return new S3AFileStatus(true, path, username);
        } else {
          LOG.debug("Found exact file: normal file");
          return new S3AFileStatus(meta.getContentLength(),
              dateToLong(meta.getLastModified()),
              path,
              getDefaultBlockSize(path),
              username);
        }
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() != 404) {
          throw translateException("getFileStatus", path, e);
        }
      } catch (AmazonClientException e) {
        throw translateException("getFileStatus", path, e);
      }

      // Necessary?
      if (!key.endsWith("/")) {
        String newKey = key + "/";
        try {
          ObjectMetadata meta = getObjectMetadata(newKey);

          if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
            LOG.debug("Found file (with /): fake directory");
            return new S3AFileStatus(true, path, username);
          } else {
            LOG.warn("Found file (with /): real file? should not happen: {}",
                key);

            return new S3AFileStatus(meta.getContentLength(),
                dateToLong(meta.getLastModified()),
                path,
                getDefaultBlockSize(path),
                username);
          }
        } catch (AmazonServiceException e) {
          if (e.getStatusCode() != 404) {
            throw translateException("getFileStatus", newKey, e);
          }
        } catch (AmazonClientException e) {
          throw translateException("getFileStatus", newKey, e);
        }
      }
    }

    try {
      key = maybeAddTrailingSlash(key);
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(key);
      request.setDelimiter("/");
      request.setMaxKeys(1);

      ObjectListing objects = listObjects(request);

      if (!objects.getCommonPrefixes().isEmpty()
          || !objects.getObjectSummaries().isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as directory (with /): {}/{}",
              objects.getCommonPrefixes().size() ,
              objects.getObjectSummaries().size());

          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            LOG.debug("Summary: {} {}", summary.getKey(), summary.getSize());
          }
          for (String prefix : objects.getCommonPrefixes()) {
            LOG.debug("Prefix: {}", prefix);
          }
        }

        return new S3AFileStatus(false, path, username);
      } else if (key.isEmpty()) {
        LOG.debug("Found root directory");
        return new S3AFileStatus(true, path, username);
      }
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() != 404) {
        throw translateException("getFileStatus", key, e);
      }
    } catch (AmazonClientException e) {
      throw translateException("getFileStatus", key, e);
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false
   * @throws AmazonClientException failure in the AWS SDK
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
      Path dst) throws IOException {
    try {
      innerCopyFromLocalFile(delSrc, overwrite, src, dst);
    } catch (AmazonClientException e) {
      throw translateException("copyFromLocalFile(" + src + ", " + dst + ")",
          src, e);
    }
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false
   * @throws AmazonClientException failure in the AWS SDK
   */
  private void innerCopyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException, FileAlreadyExistsException, AmazonClientException {
    incrementStatistic(INVOCATION_COPY_FROM_LOCAL_FILE);
    final String key = pathToKey(dst);

    if (!overwrite && exists(dst)) {
      throw new FileAlreadyExistsException(dst + " already exists");
    }
    LOG.debug("Copying local file from {} to {}", src, dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);

    final ObjectMetadata om = newObjectMetadata(srcfile.length());
    PutObjectRequest putObjectRequest = newPutObjectRequest(key, om, srcfile);
    Upload up = putObject(putObjectRequest);
    ProgressableProgressListener listener = new ProgressableProgressListener(
        this, key, up, null);
    up.addProgressListener(listener);
    try {
      up.waitForUploadResult();
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted copying " + src
          + " to "  + dst + ", cancelling");
    }
    listener.uploadCompleted();

    // This will delete unnecessary fake parent directories
    finishedWrite(key);

    if (delSrc) {
      local.delete(src, false);
    }
  }

  /**
   * Close the filesystem. This shuts down all transfers.
   * @throws IOException IO problem
   */
  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      // already closed
      return;
    }
    try {
      super.close();
    } finally {
      if (transfers != null) {
        transfers.shutdownNow(true);
        transfers = null;
      }
    }
  }

  /**
   * Override getCanonicalServiceName because we don't support token in S3A.
   */
  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  /**
   * Copy a single object in the bucket via a COPY operation.
   * @param srcKey source object path
   * @param dstKey destination object path
   * @param size object size
   * @throws AmazonClientException on failures inside the AWS SDK
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException Other IO problems
   */
  private void copyFile(String srcKey, String dstKey, long size)
      throws IOException, InterruptedIOException, AmazonClientException {
    LOG.debug("copyFile {} -> {} ", srcKey, dstKey);

    try {
      ObjectMetadata srcom = getObjectMetadata(srcKey);
      ObjectMetadata dstom = cloneObjectMetadata(srcom);
      if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
        dstom.setSSEAlgorithm(serverSideEncryptionAlgorithm);
      }
      CopyObjectRequest copyObjectRequest =
          new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
      copyObjectRequest.setCannedAccessControlList(cannedACL);
      copyObjectRequest.setNewObjectMetadata(dstom);

      ProgressListener progressListener = new ProgressListener() {
        public void progressChanged(ProgressEvent progressEvent) {
          switch (progressEvent.getEventType()) {
            case TRANSFER_PART_COMPLETED_EVENT:
              incrementWriteOperations();
              break;
            default:
              break;
          }
        }
      };

      Copy copy = transfers.copy(copyObjectRequest);
      copy.addProgressListener(progressListener);
      try {
        copy.waitForCopyResult();
        incrementWriteOperations();
        instrumentation.filesCopied(1, size);
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted copying " + srcKey
            + " to " + dstKey + ", cancelling");
      }
    } catch (AmazonClientException e) {
      throw translateException("copyFile("+ srcKey+ ", " + dstKey + ")",
          srcKey, e);
    }
  }

  /**
   * Perform post-write actions.
   * @param key key written to
   */
  public void finishedWrite(String key) {
    LOG.debug("Finished write to {}", key);
    deleteUnnecessaryFakeDirectories(keyToPath(key).getParent());
  }

  /**
   * Delete mock parent directories which are no longer needed.
   * This code swallows IO exceptions encountered
   * @param path path
   */
  private void deleteUnnecessaryFakeDirectories(Path path) {
    List<DeleteObjectsRequest.KeyVersion> keysToRemove = new ArrayList<>();
    while (!path.isRoot()) {
      String key = pathToKey(path);
      key = (key.endsWith("/")) ? key : (key + "/");
      keysToRemove.add(new DeleteObjectsRequest.KeyVersion(key));
      path = path.getParent();
    }
    try {
      removeKeys(keysToRemove, false, true);
    } catch(AmazonClientException | InvalidRequestException e) {
      instrumentation.errorIgnored();
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        for(DeleteObjectsRequest.KeyVersion kv : keysToRemove) {
          sb.append(kv.getKey()).append(",");
        }
        LOG.debug("While deleting keys {} ", sb.toString(), e);
      }
    }
  }

  private void createFakeDirectory(final String objectName)
      throws AmazonClientException, AmazonServiceException,
      InterruptedIOException {
    if (!objectName.endsWith("/")) {
      createEmptyObject(objectName + "/");
    } else {
      createEmptyObject(objectName);
    }
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String objectName)
      throws AmazonClientException, AmazonServiceException,
      InterruptedIOException {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    PutObjectRequest putObjectRequest = newPutObjectRequest(objectName,
        newObjectMetadata(0L),
        im);
    Upload upload = putObject(putObjectRequest);
    try {
      upload.waitForUploadResult();
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted creating " + objectName);
    }
    incrementPutProgressStatistics(objectName, 0);
    instrumentation.directoryCreated();
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}.
   * Does so without using the {@link ObjectMetadata#clone()} method,
   * to avoid copying unnecessary headers.
   * @param source the {@link ObjectMetadata} to copy
   * @return a copy of {@link ObjectMetadata} with only relevant attributes
   */
  private ObjectMetadata cloneObjectMetadata(ObjectMetadata source) {
    // This approach may be too brittle, especially if
    // in future there are new attributes added to ObjectMetadata
    // that we do not explicitly call to set here
    ObjectMetadata ret = newObjectMetadata(source.getContentLength());

    // Possibly null attributes
    // Allowing nulls to pass breaks it during later use
    if (source.getCacheControl() != null) {
      ret.setCacheControl(source.getCacheControl());
    }
    if (source.getContentDisposition() != null) {
      ret.setContentDisposition(source.getContentDisposition());
    }
    if (source.getContentEncoding() != null) {
      ret.setContentEncoding(source.getContentEncoding());
    }
    if (source.getContentMD5() != null) {
      ret.setContentMD5(source.getContentMD5());
    }
    if (source.getContentType() != null) {
      ret.setContentType(source.getContentType());
    }
    if (source.getExpirationTime() != null) {
      ret.setExpirationTime(source.getExpirationTime());
    }
    if (source.getExpirationTimeRuleId() != null) {
      ret.setExpirationTimeRuleId(source.getExpirationTimeRuleId());
    }
    if (source.getHttpExpiresDate() != null) {
      ret.setHttpExpiresDate(source.getHttpExpiresDate());
    }
    if (source.getLastModified() != null) {
      ret.setLastModified(source.getLastModified());
    }
    if (source.getOngoingRestore() != null) {
      ret.setOngoingRestore(source.getOngoingRestore());
    }
    if (source.getRestoreExpirationTime() != null) {
      ret.setRestoreExpirationTime(source.getRestoreExpirationTime());
    }
    if (source.getSSEAlgorithm() != null) {
      ret.setSSEAlgorithm(source.getSSEAlgorithm());
    }
    if (source.getSSECustomerAlgorithm() != null) {
      ret.setSSECustomerAlgorithm(source.getSSECustomerAlgorithm());
    }
    if (source.getSSECustomerKeyMd5() != null) {
      ret.setSSECustomerKeyMd5(source.getSSECustomerKeyMd5());
    }

    for (Map.Entry<String, String> e : source.getUserMetadata().entrySet()) {
      ret.addUserMetadata(e.getKey(), e.getValue());
    }
    return ret;
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", workingDir=").append(workingDir);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", partSize=").append(partSize);
    sb.append(", enableMultiObjectsDelete=").append(enableMultiObjectsDelete);
    sb.append(", maxKeys=").append(maxKeys);
    if (cannedACL != null) {
      sb.append(", cannedACL=").append(cannedACL.toString());
    }
    sb.append(", readAhead=").append(readAhead);
    sb.append(", blockSize=").append(getDefaultBlockSize());
    sb.append(", multiPartThreshold=").append(multiPartThreshold);
    if (serverSideEncryptionAlgorithm != null) {
      sb.append(", serverSideEncryptionAlgorithm='")
          .append(serverSideEncryptionAlgorithm)
          .append('\'');
    }
    if (blockFactory != null) {
      sb.append(", blockFactory=").append(blockFactory);
    }
    sb.append(", executor=").append(threadPoolExecutor);
    sb.append(", statistics {")
        .append(statistics)
        .append("}");
    sb.append(", metrics {")
        .append(instrumentation.dump("{", "=", "} ", true))
        .append("}");
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the partition size for multipart operations.
   * @return the value as set during initialization
   */
  public long getPartitionSize() {
    return partSize;
  }

  /**
   * Get the threshold for multipart files.
   * @return the value as set during initialization
   */
  public long getMultiPartThreshold() {
    return multiPartThreshold;
  }

  /**
   * Get the maximum key count.
   * @return a value, valid after initialization
   */
  int getMaxKeys() {
    return maxKeys;
  }

  /**
   * Increments the statistic {@link Statistic#INVOCATION_GLOB_STATUS}.
   * {@inheritDoc}
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    incrementStatistic(INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    incrementStatistic(INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern, filter);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean exists(Path f) throws IOException {
    incrementStatistic(INVOCATION_EXISTS);
    return super.exists(f);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean isDirectory(Path f) throws IOException {
    incrementStatistic(INVOCATION_IS_DIRECTORY);
    return super.isDirectory(f);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean isFile(Path f) throws IOException {
    incrementStatistic(INVOCATION_IS_FILE);
    return super.isFile(f);
  }

  /**
   * {@inheritDoc}.
   *
   * This implementation is optimized for S3, which can do a bulk listing
   * off all entries under a path in one single operation. Thus there is
   * no need to recursively walk the directory tree.
   *
   * Instead a {@link ListObjectsRequest} is created requesting a (windowed)
   * listing of all entries under the given path. This is used to construct
   * an {@code ObjectListingIterator} instance, iteratively returning the
   * sequence of lists of elements under the path. This is then iterated
   * over in a {@code FileStatusListingIterator}, which generates
   * {@link S3AFileStatus} instances, one per listing entry.
   * These are then translated into {@link LocatedFileStatus} instances.
   *
   * This is essentially a nested and wrapped set of iterators, with some
   * generator classes; an architecture which may become less convoluted
   * using lambda-expressions.
   * @param f a path
   * @param recursive if the subdirectories need to be traversed recursively
   *
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f,
      boolean recursive) throws FileNotFoundException, IOException {
    incrementStatistic(INVOCATION_LIST_FILES);
    Path path = qualify(f);
    LOG.debug("listFiles({}, {})", path, recursive);
    try {
      // lookup dir triggers existence check
      final FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        // simple case: File
        LOG.debug("Path is a file");
        return new Listing.SingleStatusRemoteIterator(
            toLocatedFileStatus(fileStatus));
      } else {
        // directory: do a bulk operation
        String key = maybeAddTrailingSlash(pathToKey(path));
        String delimiter = recursive ? null : "/";
        LOG.debug("Requesting all entries under {} with delimiter '{}'",
            key, delimiter);
        return listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(path,
                createListObjectsRequest(key, delimiter),
                ACCEPT_ALL,
                new Listing.AcceptFilesOnly(path)));
      }
    } catch (AmazonClientException e) {
      throw translateException("listFiles", path, e);
    }
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws FileNotFoundException, IOException {
    return listLocatedStatus(f, ACCEPT_ALL);
  }

  /**
   * {@inheritDoc}.
   *
   * S3 Optimized directory listing. The initial operation performs the
   * first bulk listing; extra listings will take place
   * when all the current set of results are used up.
   * @param f a path
   * @param filter a path filter
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
      throws FileNotFoundException, IOException {
    incrementStatistic(INVOCATION_LIST_LOCATED_STATUS);
    Path path = qualify(f);
    LOG.debug("listLocatedStatus({}, {}", path, filter);
    try {
      // lookup dir triggers existence check
      final FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        // simple case: File
        LOG.debug("Path is a file");
        return new Listing.SingleStatusRemoteIterator(
            filter.accept(path) ? toLocatedFileStatus(fileStatus) : null);
      } else {
        // directory: trigger a lookup
        String key = maybeAddTrailingSlash(pathToKey(path));
        return listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(path,
                createListObjectsRequest(key, "/"),
                filter,
                new Listing.AcceptAllButSelfAndS3nDirs(path)));
      }
    } catch (AmazonClientException e) {
      throw translateException("listLocatedStatus", path, e);
    }
  }

  /**
   * Build a {@link LocatedFileStatus} from a {@link FileStatus} instance.
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  LocatedFileStatus toLocatedFileStatus(FileStatus status)
      throws IOException {
    return new LocatedFileStatus(status,
        status.isFile() ?
          getFileBlockLocations(status, 0, status.getLen())
          : null);
  }

  /**
   * Helper for an ongoing write operation.
   * <p>
   * It hides direct access to the S3 API from the output stream,
   * and is a location where the object upload process can be evolved/enhanced.
   * <p>
   * Features
   * <ul>
   *   <li>Methods to create and submit requests to S3, so avoiding
   *   all direct interaction with the AWS APIs.</li>
   *   <li>Some extra preflight checks of arguments, so failing fast on
   *   errors.</li>
   *   <li>Callbacks to let the FS know of events in the output stream
   *   upload process.</li>
   * </ul>
   *
   * Each instance of this state is unique to a single output stream.
   */
  final class WriteOperationHelper {
    private final String key;

    private WriteOperationHelper(String key) {
      this.key = key;
    }

    /**
     * Create a {@link PutObjectRequest} request.
     * The metadata is assumed to have been configured with the size of the
     * operation.
     * @param inputStream source data.
     * @param length size, if known. Use -1 for not known
     * @return the request
     */
    PutObjectRequest newPutRequest(InputStream inputStream, long length) {
      return newPutObjectRequest(key, newObjectMetadata(length), inputStream);
    }

    /**
     * Callback on a successful write.
     */
    void writeSuccessful() {
      finishedWrite(key);
    }

    /**
     * Callback on a write failure.
     * @param e Any exception raised which triggered the failure.
     */
    void writeFailed(Exception e) {
      LOG.debug("Write to {} failed", this, e);
    }

    /**
     * Create a new object metadata instance.
     * Any standard metadata headers are added here, for example:
     * encryption.
     * @param length size, if known. Use -1 for not known
     * @return a new metadata instance
     */
    public ObjectMetadata newObjectMetadata(long length) {
      return S3AFileSystem.this.newObjectMetadata(length);
    }

    /**
     * Start the multipart upload process.
     * @return the upload result containing the ID
     * @throws IOException IO problem
     */
    String initiateMultiPartUpload() throws IOException {
      LOG.debug("Initiating Multipart upload");
      final InitiateMultipartUploadRequest initiateMPURequest =
          new InitiateMultipartUploadRequest(bucket,
              key,
              newObjectMetadata(-1));
      initiateMPURequest.setCannedACL(cannedACL);
      try {
        return s3.initiateMultipartUpload(initiateMPURequest)
            .getUploadId();
      } catch (AmazonClientException ace) {
        throw translateException("initiate MultiPartUpload", key, ace);
      }
    }

    /**
     * Complete a multipart upload operation.
     * @param uploadId multipart operation Id
     * @param partETags list of partial uploads
     * @return the result
     * @throws AmazonClientException on problems.
     */
    CompleteMultipartUploadResult completeMultipartUpload(String uploadId,
        List<PartETag> partETags) throws AmazonClientException {
      Preconditions.checkNotNull(uploadId);
      Preconditions.checkNotNull(partETags);
      Preconditions.checkArgument(!partETags.isEmpty(),
          "No partitions have been uploaded");
      return s3.completeMultipartUpload(
          new CompleteMultipartUploadRequest(bucket,
              key,
              uploadId,
              partETags));
    }

    /**
     * Abort a multipart upload operation.
     * @param uploadId multipart operation Id
     * @return the result
     * @throws AmazonClientException on problems.
     */
    void abortMultipartUpload(String uploadId) throws AmazonClientException {
      s3.abortMultipartUpload(
          new AbortMultipartUploadRequest(bucket, key, uploadId));
    }

    /**
     * Create and initialize a part request of a multipart upload.
     * @param uploadId ID of ongoing upload
     * @param uploadStream source of data to upload
     * @param partNumber current part number of the upload
     * @param size amount of data
     * @return the request.
     */
    UploadPartRequest newUploadPartRequest(String uploadId,
        InputStream uploadStream,
        int partNumber,
        int size) {
      Preconditions.checkNotNull(uploadId);
      Preconditions.checkNotNull(uploadStream);
      Preconditions.checkArgument(size > 0, "Invalid partition size %s", size);
      Preconditions.checkArgument(partNumber> 0 && partNumber <=10000,
          "partNumber must be between 1 and 10000 inclusive, but is %s",
          partNumber);

      LOG.debug("Creating part upload request for {} #{} size {}",
          uploadId, partNumber, size);
      return new UploadPartRequest()
          .withBucketName(bucket)
          .withKey(key)
          .withUploadId(uploadId)
          .withInputStream(uploadStream)
          .withPartNumber(partNumber)
          .withPartSize(size);
    }

    /**
     * The toString method is intended to be used in logging/toString calls.
     * @return a string description.
     */
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "{bucket=").append(bucket);
      sb.append(", key='").append(key).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

}
