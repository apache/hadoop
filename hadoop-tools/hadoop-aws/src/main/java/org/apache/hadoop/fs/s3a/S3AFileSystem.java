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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
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
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStoreListFilesIterator;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
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
  private ListeningExecutorService boundedThreadPool;
  private ExecutorService unboundedThreadPool;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem.class);
  private static final Logger PROGRESS =
      LoggerFactory.getLogger("org.apache.hadoop.fs.s3a.S3AFileSystem.Progress");
  private LocalDirAllocator directoryAllocator;
  private CannedAccessControlList cannedACL;
  private S3AEncryptionMethods serverSideEncryptionAlgorithm;
  private S3AInstrumentation instrumentation;
  private S3AStorageStatistics storageStatistics;
  private long readAhead;
  private S3AInputPolicy inputPolicy;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private MetadataStore metadataStore;
  private boolean allowAuthoritative;

  // The maximum number of entries that can be deleted in any call to s3
  private static final int MAX_ENTRIES_TO_DELETE = 1000;
  private String blockOutputBuffer;
  private S3ADataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;
  private boolean useListV1;

  /** Add any deprecated keys. */
  @SuppressWarnings("deprecation")
  private static void addDeprecatedKeys() {
    Configuration.addDeprecations(
        new Configuration.DeprecationDelta[]{
            // never shipped in an ASF release, but did get into the wild.
            new Configuration.DeprecationDelta(
                OLD_S3A_SERVER_SIDE_ENCRYPTION_KEY,
                SERVER_SIDE_ENCRYPTION_KEY)
        });
    Configuration.reloadExistingConfigurations();
  }

  static {
    addDeprecatedKeys();
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param originalConf the configuration to use for the FS. The
   * bucket-specific options are patched over the base ones before any use is
   * made of the config.
   */
  public void initialize(URI name, Configuration originalConf)
      throws IOException {
    uri = S3xLoginHelper.buildFSURI(name);
    // get the host; this is guaranteed to be non-null, non-empty
    bucket = name.getHost();
    // clone the configuration into one with propagated bucket options
    Configuration conf = propagateBucketOptions(originalConf, bucket);
    patchSecurityCredentialProviders(conf);
    super.initialize(name, conf);
    setConf(conf);
    try {
      instrumentation = new S3AInstrumentation(name);

      // Username is the current user at the time the FS was instantiated.
      username = UserGroupInformation.getCurrentUser().getShortUserName();
      workingDir = new Path("/user", username)
          .makeQualified(this.uri, this.getWorkingDirectory());


      Class<? extends S3ClientFactory> s3ClientFactoryClass = conf.getClass(
          S3_CLIENT_FACTORY_IMPL, DEFAULT_S3_CLIENT_FACTORY_IMPL,
          S3ClientFactory.class);
      s3 = ReflectionUtils.newInstance(s3ClientFactoryClass, conf)
          .createS3Client(name);

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
      boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
          maxThreads,
          maxThreads + totalTasks,
          keepAliveTime, TimeUnit.SECONDS,
          "s3a-transfer-shared");
      unboundedThreadPool = new ThreadPoolExecutor(
          maxThreads, Integer.MAX_VALUE,
          keepAliveTime, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>(),
          BlockingThreadPoolExecutorService.newDaemonThreadFactory(
              "s3a-transfer-unbounded"));

      int listVersion = conf.getInt(LIST_VERSION, DEFAULT_LIST_VERSION);
      if (listVersion < 1 || listVersion > 2) {
        LOG.warn("Configured fs.s3a.list.version {} is invalid, forcing " +
            "version 2", listVersion);
      }
      useListV1 = (listVersion == 1);

      initTransferManager();

      initCannedAcls(conf);

      verifyBucketExists();

      initMultipartUploads(conf);

      serverSideEncryptionAlgorithm = getEncryptionAlgorithm(conf);
      inputPolicy = S3AInputPolicy.getPolicy(
          conf.getTrimmed(INPUT_FADVISE, INPUT_FADV_NORMAL));

      boolean blockUploadEnabled = conf.getBoolean(FAST_UPLOAD, true);

      if (!blockUploadEnabled) {
        LOG.warn("The \"slow\" output stream is no longer supported");
      }
      blockOutputBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER,
          DEFAULT_FAST_UPLOAD_BUFFER);
      partSize = ensureOutputParameterInRange(MULTIPART_SIZE, partSize);
      blockFactory = S3ADataBlocks.createFactory(this, blockOutputBuffer);
      blockOutputActiveBlocks = intOption(conf,
          FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
      LOG.debug("Using S3ABlockOutputStream with buffer = {}; block={};" +
              " queue limit={}",
          blockOutputBuffer, partSize, blockOutputActiveBlocks);

      metadataStore = S3Guard.getMetadataStore(this);
      allowAuthoritative = conf.getBoolean(METADATASTORE_AUTHORITATIVE,
          DEFAULT_METADATASTORE_AUTHORITATIVE);
      if (hasMetadataStore()) {
        LOG.debug("Using metadata store {}, authoritative={}",
            getMetadataStore(), allowAuthoritative);
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

    transfers = new TransferManager(s3, unboundedThreadPool);
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
  AmazonS3 getAmazonS3Client() {
    return s3;
  }

  /**
   * Get the region of a bucket.
   * @return the region in which a bucket is located
   * @throws IOException on any failure.
   */
  public String getBucketLocation() throws IOException {
    return getBucketLocation(bucket);
  }

  /**
   * Get the region of a bucket.
   * @param bucketName the name of the bucket
   * @return the region in which a bucket is located
   * @throws IOException on any failure.
   */
  public String getBucketLocation(String bucketName) throws IOException {
    try {
      return s3.getBucketLocation(bucketName);
    } catch (AmazonClientException e) {
      throw translateException("getBucketLocation()",
          bucketName, e);
    }
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
  @VisibleForTesting
  public String pathToKey(Path path) {
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
  public Path qualify(Path path) {
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

    return new FSDataInputStream(
        new S3AInputStream(new S3ObjectAttributes(
          bucket,
          pathToKey(f),
          serverSideEncryptionAlgorithm,
          getServerSideEncryptionKey(getConf())),
            fileStatus.getLen(),
            s3,
            statistics,
            instrumentation,
            readAhead,
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
    FileStatus status = null;
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
    return new FSDataOutputStream(
        new S3ABlockOutputStream(this,
            key,
            new SemaphoredDelegatingExecutor(boundedThreadPool,
                blockOutputActiveBlocks, true),
            progress,
            partSize,
            blockFactory,
            instrumentation.newOutputStreamStatistics(statistics),
            new WriteOperationHelper(key)
        ),
        null);
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
   * <pre>
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   * </pre>
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
    } catch (RenameFailedException e) {
      LOG.debug(e.getMessage());
      return e.getExitCode();
    } catch (FileNotFoundException e) {
      LOG.debug(e.toString());
      return false;
    }
  }

  /**
   * The inner rename operation. See {@link #rename(Path, Path)} for
   * the description of the operation.
   * This operation throws an exception on any failure which needs to be
   * reported and downgraded to a failure. That is: if a rename
   * @param source path to be renamed
   * @param dest new path after rename
   * @throws RenameFailedException if some criteria for a state changing
   * rename was not met. This means work didn't happen; it's not something
   * which is reported upstream to the FileSystem APIs, for which the semantics
   * of "false" are pretty vague.
   * @throws FileNotFoundException there's no source file.
   * @throws IOException on IO failure.
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  private boolean innerRename(Path source, Path dest)
      throws RenameFailedException, FileNotFoundException, IOException,
        AmazonClientException {
    Path src = qualify(source);
    Path dst = qualify(dest);

    LOG.debug("Rename path {} to {}", src, dst);
    incrementStatistic(INVOCATION_RENAME);

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "source is root directory");
    }
    if (dstKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "dest is root directory");
    }

    // get the source file status; this raises a FNFE if there is no source
    // file.
    S3AFileStatus srcStatus = innerGetFileStatus(src, true);

    if (srcKey.equals(dstKey)) {
      LOG.debug("rename: src and dest refer to the same file or directory: {}",
          dst);
      throw new RenameFailedException(src, dst,
          "source and dest refer to the same file or directory")
          .withExitCode(srcStatus.isFile());
    }

    S3AFileStatus dstStatus = null;
    try {
      dstStatus = innerGetFileStatus(dst, true);
      // if there is no destination entry, an exception is raised.
      // hence this code sequence can assume that there is something
      // at the end of the path; the only detail being what it is and
      // whether or not it can be the destination of the rename.
      if (srcStatus.isDirectory()) {
        if (dstStatus.isFile()) {
          throw new RenameFailedException(src, dst,
              "source is a directory and dest is a file")
              .withExitCode(srcStatus.isFile());
        } else if (dstStatus.isEmptyDirectory() != Tristate.TRUE) {
          throw new RenameFailedException(src, dst,
              "Destination is a non-empty directory")
              .withExitCode(false);
        }
        // at this point the destination is an empty directory
      } else {
        // source is a file. The destination must be a directory,
        // empty or not
        if (dstStatus.isFile()) {
          throw new RenameFailedException(src, dst,
              "Cannot rename onto an existing file")
              .withExitCode(false);
        }
      }

    } catch (FileNotFoundException e) {
      LOG.debug("rename: destination path {} not found", dst);
      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()) {
        try {
          S3AFileStatus dstParentStatus = innerGetFileStatus(dst.getParent(),
              false);
          if (!dstParentStatus.isDirectory()) {
            throw new RenameFailedException(src, dst,
                "destination parent is not a directory");
          }
        } catch (FileNotFoundException e2) {
          throw new RenameFailedException(src, dst,
              "destination has no parent ");
        }
      }
    }

    // If we have a MetadataStore, track deletions/creations.
    Collection<Path> srcPaths = null;
    List<PathMetadata> dstMetas = null;
    if (hasMetadataStore()) {
      srcPaths = new HashSet<>(); // srcPaths need fast look up before put
      dstMetas = new ArrayList<>();
    }
    // TODO S3Guard HADOOP-13761: retries when source paths are not visible yet
    // TODO S3Guard: performance: mark destination dirs as authoritative

    // Ok! Time to start
    if (srcStatus.isFile()) {
      LOG.debug("rename: renaming file {} to {}", src, dst);
      long length = srcStatus.getLen();
      if (dstStatus != null && dstStatus.isDirectory()) {
        String newDstKey = dstKey;
        if (!newDstKey.endsWith("/")) {
          newDstKey = newDstKey + "/";
        }
        String filename =
            srcKey.substring(pathToKey(src.getParent()).length()+1);
        newDstKey = newDstKey + filename;
        copyFile(srcKey, newDstKey, length);
        S3Guard.addMoveFile(metadataStore, srcPaths, dstMetas, src,
            keyToQualifiedPath(newDstKey), length, getDefaultBlockSize(dst),
            username);
      } else {
        copyFile(srcKey, dstKey, srcStatus.getLen());
        S3Guard.addMoveFile(metadataStore, srcPaths, dstMetas, src, dst,
            length, getDefaultBlockSize(dst), username);
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
        throw new RenameFailedException(srcKey, dstKey,
            "cannot rename a directory to a subdirectory of itself ");
      }

      List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();
      if (dstStatus != null && dstStatus.isEmptyDirectory() == Tristate.TRUE) {
        // delete unnecessary fake directory.
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(dstKey));
      }

      Path parentPath = keyToPath(srcKey);
      RemoteIterator<LocatedFileStatus> iterator = listFilesAndEmptyDirectories(
          parentPath, true);
      while (iterator.hasNext()) {
        LocatedFileStatus status = iterator.next();
        long length = status.getLen();
        String key = pathToKey(status.getPath());
        if (status.isDirectory() && !key.endsWith("/")) {
          key += "/";
        }
        keysToDelete
            .add(new DeleteObjectsRequest.KeyVersion(key));
        String newDstKey =
            dstKey + key.substring(srcKey.length());
        copyFile(key, newDstKey, length);

        if (hasMetadataStore()) {
          // with a metadata store, the object entries need to be updated,
          // including, potentially, the ancestors
          Path childSrc = keyToQualifiedPath(key);
          Path childDst = keyToQualifiedPath(newDstKey);
          if (objectRepresentsDirectory(key, length)) {
            S3Guard.addMoveDir(metadataStore, srcPaths, dstMetas, childSrc,
                childDst, username);
          } else {
            S3Guard.addMoveFile(metadataStore, srcPaths, dstMetas, childSrc,
                childDst, length, getDefaultBlockSize(childDst), username);
          }
          // Ancestor directories may not be listed, so we explicitly add them
          S3Guard.addMoveAncestors(metadataStore, srcPaths, dstMetas,
              keyToQualifiedPath(srcKey), childSrc, childDst, username);
        }

        if (keysToDelete.size() == MAX_ENTRIES_TO_DELETE) {
          removeKeys(keysToDelete, true, false);
        }
      }
      if (!keysToDelete.isEmpty()) {
        removeKeys(keysToDelete, false, false);
      }

      // We moved all the children, now move the top-level dir
      // Empty directory should have been added as the object summary
      if (hasMetadataStore()
          && srcPaths != null
          && !srcPaths.contains(src)) {
        LOG.debug("To move the non-empty top-level dir src={} and dst={}",
            src, dst);
        S3Guard.addMoveDir(metadataStore, srcPaths, dstMetas, src, dst,
            username);
      }
    }

    metadataStore.move(srcPaths, dstMetas);

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
   * Does this Filesystem have a metadata store?
   * @return true iff the FS has been instantiated with a metadata store
   */
  public boolean hasMetadataStore() {
    return !S3Guard.isNullMetadataStore(metadataStore);
  }

  /**
   * Get the metadata store.
   * This will always be non-null, but may be bound to the
   * {@code NullMetadataStore}.
   * @return the metadata store of this FS instance
   */
  @VisibleForTesting
  public MetadataStore getMetadataStore() {
    return metadataStore;
  }

  /** For testing only.  See ITestS3GuardEmptyDirs. */
  @VisibleForTesting
  void setMetadataStore(MetadataStore ms) {
    metadataStore = ms;
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
    GetObjectMetadataRequest request =
        new GetObjectMetadataRequest(bucket, key);
    //SSE-C requires to be filled in if enabled for object metadata
    if(S3AEncryptionMethods.SSE_C.equals(serverSideEncryptionAlgorithm) &&
        StringUtils.isNotBlank(getServerSideEncryptionKey(getConf()))){
      request.setSSECustomerKey(generateSSECustomerKey());
    }
    ObjectMetadata meta = s3.getObjectMetadata(request);
    incrementReadOperations();
    return meta;
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics
   * in the process.
   * @param request request to initiate
   * @return the results
   */
  protected S3ListResult listObjects(S3ListRequest request) {
    incrementStatistic(OBJECT_LIST_REQUESTS);
    incrementReadOperations();
    if (useListV1) {
      Preconditions.checkArgument(request.isV1());
      return S3ListResult.v1(s3.listObjects(request.getV1()));
    } else {
      Preconditions.checkArgument(!request.isV1());
      return S3ListResult.v2(s3.listObjectsV2(request.getV2()));
    }
  }

  /**
   * List the next set of objects.
   * @param request last list objects request to continue
   * @param prevResult last paged result to continue from
   * @return the next result object
   */
  protected S3ListResult continueListObjects(S3ListRequest request,
      S3ListResult prevResult) {
    incrementStatistic(OBJECT_CONTINUE_LIST_REQUESTS);
    incrementReadOperations();
    if (useListV1) {
      Preconditions.checkArgument(request.isV1());
      return S3ListResult.v1(s3.listNextBatchOfObjects(prevResult.getV1()));
    } else {
      Preconditions.checkArgument(!request.isV1());
      request.getV2().setContinuationToken(prevResult.getV2()
          .getNextContinuationToken());
      return S3ListResult.v2(s3.listObjectsV2(request.getV2()));
    }
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
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted.
   * @throws AmazonClientException amazon-layer failure.
   */
  private void deleteObjects(DeleteObjectsRequest deleteRequest)
      throws MultiObjectDeleteException, AmazonClientException {
    incrementWriteOperations();
    incrementStatistic(OBJECT_DELETE_REQUESTS, 1);
    try {
      s3.deleteObjects(deleteRequest);
    } catch (MultiObjectDeleteException e) {
      // one or more of the operations failed.
      List<MultiObjectDeleteException.DeleteError> errors = e.getErrors();
      LOG.error("Partial failure of delete, {} errors", errors.size(), e);
      for (MultiObjectDeleteException.DeleteError error : errors) {
        LOG.error("{}: \"{}\" - {}",
            error.getKey(), error.getCode(), error.getMessage());
      }
      throw e;
    }
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
    Preconditions.checkNotNull(srcfile);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        srcfile);
    setOptionalPutRequestParameters(putObjectRequest);
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
      ObjectMetadata metadata,
      InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        inputStream, metadata);
    setOptionalPutRequestParameters(putObjectRequest);
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
    setOptionalObjectMetadata(om);
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
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * @param putObjectRequest the request
   * @return the upload initiated
   */
  public UploadInfo putObject(PutObjectRequest putObjectRequest) {
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
      return new UploadInfo(upload, len);
    } catch (AmazonClientException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * <i>Important: this call will close any input stream in the request.</i>
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws AmazonClientException on problems
   */
  PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest)
      throws AmazonClientException {
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {}", len, putObjectRequest.getKey());
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
   * Get the length of the PUT, verifying that the length is known.
   * @param putObjectRequest a request bound to a file or a stream.
   * @return the request length
   * @throws IllegalArgumentException if the length is negative
   */
  private long getPutRequestLength(PutObjectRequest putObjectRequest) {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    Preconditions.checkState(len >= 0, "Cannot PUT object of unknown length");
    return len;
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the request.</i>
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
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AmazonClientException amazon-layer failure.
   */
  @VisibleForTesting
  void removeKeys(List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      boolean clearKeys, boolean deleteFakeDir)
      throws MultiObjectDeleteException, AmazonClientException,
      InvalidRequestException {
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
      return innerDelete(innerGetFileStatus(f, true), recursive);
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
      Preconditions.checkArgument(
          status.isEmptyDirectory() != Tristate.UNKNOWN,
          "File status must have directory emptiness computed");

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (key.equals("/")) {
        return rejectRootDirectoryDelete(status, recursive);
      }

      if (!recursive && status.isEmptyDirectory() == Tristate.FALSE) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }

      if (status.isEmptyDirectory() == Tristate.TRUE) {
        LOG.debug("Deleting fake empty directory {}", key);
        // HADOOP-13761 S3Guard: retries here
        deleteObject(key);
        metadataStore.delete(f);
        instrumentation.directoryDeleted();
      } else {
        LOG.debug("Getting objects for directory prefix {} to delete", key);

        S3ListRequest request = createListObjectsRequest(key, null);

        S3ListResult objects = listObjects(request);
        List<DeleteObjectsRequest.KeyVersion> keys =
            new ArrayList<>(objects.getObjectSummaries().size());
        while (true) {
          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            LOG.debug("Got object to delete {}", summary.getKey());

            if (keys.size() == MAX_ENTRIES_TO_DELETE) {
              // TODO: HADOOP-13761 S3Guard: retries
              removeKeys(keys, true, false);
            }
          }

          if (objects.isTruncated()) {
            objects = continueListObjects(request, objects);
          } else {
            if (!keys.isEmpty()) {
              // TODO: HADOOP-13761 S3Guard: retries
              removeKeys(keys, false, false);
            }
            break;
          }
        }
      }
      metadataStore.deleteSubtree(f);
    } else {
      LOG.debug("delete: Path is a file");
      instrumentation.fileDeleted(1);
      deleteObject(key);
      metadataStore.delete(f);
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
    boolean emptyRoot = status.isEmptyDirectory() == Tristate.TRUE;
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
    if (!key.isEmpty() && !s3Exists(f)) {
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

      DirListingMetadata dirMeta = metadataStore.listChildren(path);
      if (allowAuthoritative && dirMeta != null && dirMeta.isAuthoritative()) {
        return S3Guard.dirMetaToStatuses(dirMeta);
      }

      S3ListRequest request = createListObjectsRequest(key, "/");
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
      return S3Guard.dirListingUnion(metadataStore, path, result, dirMeta,
          allowAuthoritative);
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
  @VisibleForTesting
  S3ListRequest createListObjectsRequest(String key,
      String delimiter) {
    return createListObjectsRequest(key, delimiter, null);
  }

  private S3ListRequest createListObjectsRequest(String key,
      String delimiter, Integer overrideMaxKeys) {
    if (!useListV1) {
      ListObjectsV2Request request =
          new ListObjectsV2Request().withBucketName(bucket)
              .withMaxKeys(maxKeys)
              .withPrefix(key);
      if (delimiter != null) {
        request.setDelimiter(delimiter);
      }
      if (overrideMaxKeys != null) {
        request.setMaxKeys(overrideMaxKeys);
      }
      return S3ListRequest.v2(request);
    } else {
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setMaxKeys(maxKeys);
      request.setPrefix(key);
      if (delimiter != null) {
        request.setDelimiter(delimiter);
      }
      if (overrideMaxKeys != null) {
        request.setMaxKeys(overrideMaxKeys);
      }
      return S3ListRequest.v1(request);
    }
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
   * @param p path to create
   * @param permission to apply to f
   * @return true if a directory was created or already existed
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  private boolean innerMkdirs(Path p, FsPermission permission)
      throws IOException, FileAlreadyExistsException, AmazonClientException {
    Path f = qualify(p);
    LOG.debug("Making directory: {}", f);
    incrementStatistic(INVOCATION_MKDIRS);
    FileStatus fileStatus;
    List<Path> metadataStoreDirs = null;
    if (hasMetadataStore()) {
      metadataStoreDirs = new ArrayList<>();
    }

    try {
      fileStatus = getFileStatus(f);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      // Walk path to root, ensuring closest ancestor is a directory, not file
      Path fPart = f.getParent();
      if (metadataStoreDirs != null) {
        metadataStoreDirs.add(f);
      }
      while (fPart != null) {
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
          // We create all missing directories in MetadataStore; it does not
          // infer directories exist by prefix like S3.
          if (metadataStoreDirs != null) {
            metadataStoreDirs.add(fPart);
          }
        }
        fPart = fPart.getParent();
      }
      String key = pathToKey(f);
      createFakeDirectory(key);
      S3Guard.makeDirsOrdered(metadataStore, metadataStoreDirs, username, true);
      // this is complicated because getParent(a/b/c/) returns a/b/c, but
      // we want a/b. See HADOOP-14428 for more details.
      deleteUnnecessaryFakeDirectories(new Path(f.toString()).getParent());
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException on other problems.
   */
  public FileStatus getFileStatus(final Path f) throws IOException {
    return innerGetFileStatus(f, false);
  }

  /**
   * Internal version of {@link #getFileStatus(Path)}.
   * @param f The path we want information from
   * @param needEmptyDirectoryFlag if true, implementation will calculate
   *        a TRUE or FALSE value for {@link S3AFileStatus#isEmptyDirectory()}
   * @return a S3AFileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException on other problems.
   */
  @VisibleForTesting
  S3AFileStatus innerGetFileStatus(final Path f,
      boolean needEmptyDirectoryFlag) throws IOException {
    incrementStatistic(INVOCATION_GET_FILE_STATUS);
    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({})", path, key);

    // Check MetadataStore, if any.
    PathMetadata pm = metadataStore.get(path, needEmptyDirectoryFlag);
    Set<Path> tombstones = Collections.EMPTY_SET;
    if (pm != null) {
      if (pm.isDeleted()) {
        throw new FileNotFoundException("Path " + f + " is recorded as " +
            "deleted by S3Guard");
      }

      FileStatus msStatus = pm.getFileStatus();
      if (needEmptyDirectoryFlag && msStatus.isDirectory()) {
        if (pm.isEmptyDirectory() != Tristate.UNKNOWN) {
          // We have a definitive true / false from MetadataStore, we are done.
          return S3AFileStatus.fromFileStatus(msStatus, pm.isEmptyDirectory());
        } else {
          DirListingMetadata children = metadataStore.listChildren(path);
          if (children != null) {
            tombstones = children.listTombstones();
          }
          LOG.debug("MetadataStore doesn't know if dir is empty, using S3.");
        }
      } else {
        // Either this is not a directory, or we don't care if it is empty
        return S3AFileStatus.fromFileStatus(msStatus, pm.isEmptyDirectory());
      }

      // If the metadata store has no children for it and it's not listed in
      // S3 yet, we'll assume the empty directory is true;
      S3AFileStatus s3FileStatus;
      try {
        s3FileStatus = s3GetFileStatus(path, key, tombstones);
      } catch (FileNotFoundException e) {
        return S3AFileStatus.fromFileStatus(msStatus, Tristate.TRUE);
      }
      // entry was found, save in S3Guard
      return S3Guard.putAndReturn(metadataStore, s3FileStatus, instrumentation);
    } else {
      // there was no entry in S3Guard
      // retrieve the data and update the metadata store in the process.
      return S3Guard.putAndReturn(metadataStore,
          s3GetFileStatus(path, key, tombstones), instrumentation);
    }
  }

  /**
   * Raw {@code getFileStatus} that talks direct to S3.
   * Used to implement {@link #innerGetFileStatus(Path, boolean)},
   * and for direct management of empty directory blobs.
   * @param path Qualified path
   * @param key  Key string for the path
   * @return Status
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException on other problems.
   */
  private S3AFileStatus s3GetFileStatus(final Path path, String key,
      Set<Path> tombstones) throws IOException {
    if (!key.isEmpty()) {
      try {
        ObjectMetadata meta = getObjectMetadata(key);

        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          LOG.debug("Found exact file: fake directory");
          return new S3AFileStatus(Tristate.TRUE, path, username);
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
            return new S3AFileStatus(Tristate.TRUE, path, username);
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
      S3ListRequest request = createListObjectsRequest(key, "/", 1);

      S3ListResult objects = listObjects(request);

      Collection<String> prefixes = objects.getCommonPrefixes();
      Collection<S3ObjectSummary> summaries = objects.getObjectSummaries();
      if (!isEmptyOfKeys(prefixes, tombstones) ||
          !isEmptyOfObjects(summaries, tombstones)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as directory (with /): {}/{}",
              prefixes.size(), summaries.size());

          for (S3ObjectSummary summary : summaries) {
            LOG.debug("Summary: {} {}", summary.getKey(), summary.getSize());
          }
          for (String prefix : prefixes) {
            LOG.debug("Prefix: {}", prefix);
          }
        }

        return new S3AFileStatus(Tristate.FALSE, path, username);
      } else if (key.isEmpty()) {
        LOG.debug("Found root directory");
        return new S3AFileStatus(Tristate.TRUE, path, username);
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
   * Helper function to determine if a collection of paths is empty
   * after accounting for tombstone markers (if provided).
   * @param keys Collection of path (prefixes / directories or keys).
   * @param tombstones Set of tombstone markers, or null if not applicable.
   * @return false if summaries contains objects not accounted for by
   * tombstones.
   */
  private boolean isEmptyOfKeys(Collection<String> keys, Set<Path>
      tombstones) {
    if (tombstones == null) {
      return keys.isEmpty();
    }
    for (String key : keys) {
      Path qualified = keyToQualifiedPath(key);
      if (!tombstones.contains(qualified)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Helper function to determine if a collection of object summaries is empty
   * after accounting for tombstone markers (if provided).
   * @param summaries Collection of objects as returned by listObjects.
   * @param tombstones Set of tombstone markers, or null if not applicable.
   * @return false if summaries contains objects not accounted for by
   * tombstones.
   */
  private boolean isEmptyOfObjects(Collection<S3ObjectSummary> summaries,
      Set<Path> tombstones) {
    if (tombstones == null) {
      return summaries.isEmpty();
    }
    Collection<String> stringCollection = new ArrayList<>(summaries.size());
    for (S3ObjectSummary summary : summaries) {
      stringCollection.add(summary.getKey());
    }
    return isEmptyOfKeys(stringCollection, tombstones);
  }

  /**
   * Raw version of {@link FileSystem#exists(Path)} which uses S3 only:
   * S3Guard MetadataStore, if any, will be skipped.
   * @return true if path exists in S3
   */
  private boolean s3Exists(final Path f) throws IOException {
    Path path = qualify(f);
    String key = pathToKey(path);
    try {
      s3GetFileStatus(path, key, null);
      return true;
    } catch (FileNotFoundException e) {
      return false;
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
   * @param src Source path: must be on local filesystem
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false, or if the destination is a directory.
   * @throws FileNotFoundException if the source file does not exit
   * @throws AmazonClientException failure in the AWS SDK
   * @throws IllegalArgumentException if the source path is not on the local FS
   */
  private void innerCopyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException, FileAlreadyExistsException, AmazonClientException {
    incrementStatistic(INVOCATION_COPY_FROM_LOCAL_FILE);
    LOG.debug("Copying local file from {} to {}", src, dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);
    if (!srcfile.exists()) {
      throw new FileNotFoundException("No file: " + src);
    }
    if (!srcfile.isFile()) {
      throw new FileNotFoundException("Not a file: " + src);
    }

    try {
      FileStatus status = getFileStatus(dst);
      if (!status.isFile()) {
        throw new FileAlreadyExistsException(dst + " exists and is not a file");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException(dst + " already exists");
      }
    } catch (FileNotFoundException e) {
      // no destination, all is well
    }
    final String key = pathToKey(dst);
    final ObjectMetadata om = newObjectMetadata(srcfile.length());
    PutObjectRequest putObjectRequest = newPutObjectRequest(key, om, srcfile);
    UploadInfo info = putObject(putObjectRequest);
    Upload upload = info.getUpload();
    ProgressableProgressListener listener = new ProgressableProgressListener(
        this, key, upload, null);
    upload.addProgressListener(listener);
    try {
      upload.waitForUploadResult();
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted copying " + src
          + " to "  + dst + ", cancelling");
    }
    listener.uploadCompleted();

    // This will delete unnecessary fake parent directories
    finishedWrite(key, info.getLength());

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
      if (metadataStore != null) {
        metadataStore.close();
        metadataStore = null;
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
      setOptionalObjectMetadata(dstom);
      CopyObjectRequest copyObjectRequest =
          new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
      setOptionalCopyObjectRequestParameters(copyObjectRequest);
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

  protected void setOptionalMultipartUploadRequestParameters(
      InitiateMultipartUploadRequest req) {
    switch (serverSideEncryptionAlgorithm) {
    case SSE_KMS:
      req.setSSEAwsKeyManagementParams(generateSSEAwsKeyParams());
      break;
    case SSE_C:
      if (StringUtils.isNotBlank(getServerSideEncryptionKey(getConf()))) {
        //at the moment, only supports copy using the same key
        req.setSSECustomerKey(generateSSECustomerKey());
      }
      break;
    default:
    }
  }


  protected void setOptionalCopyObjectRequestParameters(
      CopyObjectRequest copyObjectRequest) throws IOException {
    switch (serverSideEncryptionAlgorithm) {
    case SSE_KMS:
      copyObjectRequest.setSSEAwsKeyManagementParams(
          generateSSEAwsKeyParams()
      );
      break;
    case SSE_C:
      if (StringUtils.isNotBlank(getServerSideEncryptionKey(getConf()))) {
        //at the moment, only supports copy using the same key
        SSECustomerKey customerKey = generateSSECustomerKey();
        copyObjectRequest.setSourceSSECustomerKey(customerKey);
        copyObjectRequest.setDestinationSSECustomerKey(customerKey);
      }
      break;
    default:
    }
  }

  private void setOptionalPutRequestParameters(PutObjectRequest request) {
    switch (serverSideEncryptionAlgorithm) {
    case SSE_KMS:
      request.setSSEAwsKeyManagementParams(generateSSEAwsKeyParams());
      break;
    case SSE_C:
      if (StringUtils.isNotBlank(getServerSideEncryptionKey(getConf()))) {
        request.setSSECustomerKey(generateSSECustomerKey());
      }
      break;
    default:
    }
  }

  private void setOptionalObjectMetadata(ObjectMetadata metadata) {
    if (S3AEncryptionMethods.SSE_S3.equals(serverSideEncryptionAlgorithm)) {
      metadata.setSSEAlgorithm(serverSideEncryptionAlgorithm.getMethod());
    }
  }

  private SSEAwsKeyManagementParams generateSSEAwsKeyParams() {
    //Use specified key, otherwise default to default master aws/s3 key by AWS
    SSEAwsKeyManagementParams sseAwsKeyManagementParams =
        new SSEAwsKeyManagementParams();
    if (StringUtils.isNotBlank(getServerSideEncryptionKey(getConf()))) {
      sseAwsKeyManagementParams =
        new SSEAwsKeyManagementParams(
          getServerSideEncryptionKey(getConf())
        );
    }
    return sseAwsKeyManagementParams;
  }

  private SSECustomerKey generateSSECustomerKey() {
    SSECustomerKey customerKey = new SSECustomerKey(
        getServerSideEncryptionKey(getConf())
    );
    return customerKey;
  }

  /**
   * Perform post-write actions.
   * This operation MUST be called after any PUT/multipart PUT completes
   * successfully.
   * This includes
   * <ol>
   *   <li>Calling {@link #deleteUnnecessaryFakeDirectories(Path)}</li>
   *   <li>Updating any metadata store with details on the newly created
   *   object.</li>
   * </ol>
   * @param key key written to
   * @param length  total length of file written
   */
  @InterfaceAudience.Private
  void finishedWrite(String key, long length) {
    LOG.debug("Finished write to {}, len {}", key, length);
    Path p = keyToQualifiedPath(key);
    deleteUnnecessaryFakeDirectories(p.getParent());
    Preconditions.checkArgument(length >= 0, "content length is negative");

    // See note about failure semantics in S3Guard documentation
    try {
      if (hasMetadataStore()) {
        S3Guard.addAncestors(metadataStore, p, username);
        S3AFileStatus status = createUploadFileStatus(p,
            S3AUtils.objectRepresentsDirectory(key, length), length,
            getDefaultBlockSize(p), username);
        S3Guard.putAndReturn(metadataStore, status, instrumentation);
      }
    } catch (IOException e) {
      LOG.error("S3Guard: Error updating MetadataStore for write to {}:",
          key, e);
      instrumentation.errorIgnored();
    }
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
      LOG.trace("To delete unnecessary fake directory {} for {}", key, path);
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
    UploadInfo info = putObject(putObjectRequest);
    try {
      info.getUpload().waitForUploadResult();
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
    if (getConf() != null) {
      sb.append(", blockSize=").append(getDefaultBlockSize());
    }
    sb.append(", multiPartThreshold=").append(multiPartThreshold);
    if (serverSideEncryptionAlgorithm != null) {
      sb.append(", serverSideEncryptionAlgorithm='")
          .append(serverSideEncryptionAlgorithm)
          .append('\'');
    }
    if (blockFactory != null) {
      sb.append(", blockFactory=").append(blockFactory);
    }
    sb.append(", metastore=").append(metadataStore);
    sb.append(", authoritative=").append(allowAuthoritative);
    sb.append(", useListV1=").append(useListV1);
    sb.append(", boundedExecutor=").append(boundedThreadPool);
    sb.append(", unboundedExecutor=").append(unboundedThreadPool);
    sb.append(", statistics {")
        .append(statistics)
        .append("}");
    if (instrumentation != null) {
      sb.append(", metrics {")
          .append(instrumentation.dump("{", "=", "} ", true))
          .append("}");
    }
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
  @SuppressWarnings("deprecation")
  public boolean isDirectory(Path f) throws IOException {
    incrementStatistic(INVOCATION_IS_DIRECTORY);
    return super.isDirectory(f);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("deprecation")
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
    return innerListFiles(f, recursive,
        new Listing.AcceptFilesOnly(qualify(f)));
  }

  public RemoteIterator<LocatedFileStatus> listFilesAndEmptyDirectories(Path f,
      boolean recursive) throws IOException {
    return innerListFiles(f, recursive,
        new Listing.AcceptAllButS3nDirs());
  }

  private RemoteIterator<LocatedFileStatus> innerListFiles(Path f, boolean
      recursive, Listing.FileStatusAcceptor acceptor) throws IOException {
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
        final RemoteIterator<FileStatus> cachedFilesIterator;
        final Set<Path> tombstones;
        if (recursive) {
          final PathMetadata pm = metadataStore.get(path, true);
          // shouldn't need to check pm.isDeleted() because that will have
          // been caught by getFileStatus above.
          MetadataStoreListFilesIterator metadataStoreListFilesIterator =
              new MetadataStoreListFilesIterator(metadataStore, pm,
                  allowAuthoritative);
          tombstones = metadataStoreListFilesIterator.listTombstones();
          cachedFilesIterator = metadataStoreListFilesIterator;
        } else {
          DirListingMetadata meta = metadataStore.listChildren(path);
          if (meta != null) {
            tombstones = meta.listTombstones();
          } else {
            tombstones = null;
          }
          cachedFilesIterator = listing.createProvidedFileStatusIterator(
              S3Guard.dirMetaToStatuses(meta), ACCEPT_ALL, acceptor);
          if (allowAuthoritative && meta != null && meta.isAuthoritative()) {
            // metadata listing is authoritative, so return it directly
            return listing.createLocatedFileStatusIterator(cachedFilesIterator);
          }
        }
        return listing.createTombstoneReconcilingIterator(
            listing.createLocatedFileStatusIterator(
                listing.createFileStatusListingIterator(path,
                    createListObjectsRequest(key, delimiter),
                    ACCEPT_ALL,
                    acceptor,
                    cachedFilesIterator)),
            tombstones);
      }
    } catch (AmazonClientException e) {
      // TODO S3Guard: retry on file not found exception
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
        final String key = maybeAddTrailingSlash(pathToKey(path));
        final Listing.FileStatusAcceptor acceptor =
            new Listing.AcceptAllButSelfAndS3nDirs(path);
        DirListingMetadata meta = metadataStore.listChildren(path);
        final RemoteIterator<FileStatus> cachedFileStatusIterator =
            listing.createProvidedFileStatusIterator(
                S3Guard.dirMetaToStatuses(meta), filter, acceptor);
        return (allowAuthoritative && meta != null && meta.isAuthoritative())
            ? listing.createLocatedFileStatusIterator(cachedFileStatusIterator)
            : listing.createLocatedFileStatusIterator(
                listing.createFileStatusListingIterator(path,
                    createListObjectsRequest(key, "/"),
                    filter,
                    acceptor,
                    cachedFileStatusIterator));
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
     * If {@code length} is set, the metadata is configured with the size of
     * the upload.
     * @param inputStream source data.
     * @param length size, if known. Use -1 for not known
     * @return the request
     */
    PutObjectRequest newPutRequest(InputStream inputStream, long length) {
      PutObjectRequest request = newPutObjectRequest(key,
          newObjectMetadata(length), inputStream);
      return request;
    }

    /**
     * Create a {@link PutObjectRequest} request to upload a file.
     * @param sourceFile source file
     * @return the request
     */
    PutObjectRequest newPutRequest(File sourceFile) {
      int length = (int) sourceFile.length();
      PutObjectRequest request = newPutObjectRequest(key,
          newObjectMetadata(length), sourceFile);
      return request;
    }

    /**
     * Callback on a successful write.
     */
    void writeSuccessful(long length) {
      finishedWrite(key, length);
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
      setOptionalMultipartUploadRequestParameters(initiateMPURequest);
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
      LOG.debug("Completing multipart upload {} with {} parts",
          uploadId, partETags.size());
      // a copy of the list is required, so that the AWS SDK doesn't
      // attempt to sort an unmodifiable list.
      return s3.completeMultipartUpload(
          new CompleteMultipartUploadRequest(bucket,
              key,
              uploadId,
              new ArrayList<>(partETags)));
    }

    /**
     * Abort a multipart upload operation.
     * @param uploadId multipart operation Id
     * @throws AmazonClientException on problems.
     */
    void abortMultipartUpload(String uploadId) throws AmazonClientException {
      LOG.debug("Aborting multipart upload {}", uploadId);
      s3.abortMultipartUpload(
          new AbortMultipartUploadRequest(bucket, key, uploadId));
    }

    /**
     * Create and initialize a part request of a multipart upload.
     * Exactly one of: {@code uploadStream} or {@code sourceFile}
     * must be specified.
     * @param uploadId ID of ongoing upload
     * @param partNumber current part number of the upload
     * @param size amount of data
     * @param uploadStream source of data to upload
     * @param sourceFile optional source file.
     * @return the request.
     */
    UploadPartRequest newUploadPartRequest(String uploadId,
        int partNumber, int size, InputStream uploadStream, File sourceFile) {
      Preconditions.checkNotNull(uploadId);
      // exactly one source must be set; xor verifies this
      Preconditions.checkArgument((uploadStream != null) ^ (sourceFile != null),
          "Data source");
      Preconditions.checkArgument(size > 0, "Invalid partition size %s", size);
      Preconditions.checkArgument(partNumber > 0 && partNumber <= 10000,
          "partNumber must be between 1 and 10000 inclusive, but is %s",
          partNumber);

      LOG.debug("Creating part upload request for {} #{} size {}",
          uploadId, partNumber, size);
      UploadPartRequest request = new UploadPartRequest()
          .withBucketName(bucket)
          .withKey(key)
          .withUploadId(uploadId)
          .withPartNumber(partNumber)
          .withPartSize(size);
      if (uploadStream != null) {
        // there's an upload stream. Bind to it.
        request.setInputStream(uploadStream);
      } else {
        request.setFile(sourceFile);
      }
      return request;
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

    /**
     * PUT an object directly (i.e. not via the transfer manager).
     * @param putObjectRequest the request
     * @return the upload initiated
     * @throws IOException on problems
     */
    PutObjectResult putObject(PutObjectRequest putObjectRequest)
        throws IOException {
      try {
        return putObjectDirect(putObjectRequest);
      } catch (AmazonClientException e) {
        throw translateException("put", putObjectRequest.getKey(), e);
      }
    }
  }

}
