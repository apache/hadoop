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

package org.apache.hadoop.fs.azurebfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept;
import org.apache.hadoop.fs.azurebfs.services.AbfsListStatusRemoteIterator;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.*;
import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;

/**
 * A {@link org.apache.hadoop.fs.FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>
 */
@InterfaceStability.Evolving
public class AzureBlobFileSystem extends FileSystem
    implements IOStatisticsSource {
  public static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystem.class);
  private URI uri;
  private Path workingDir;
  private AzureBlobFileSystemStore abfsStore;
  private boolean isClosed;

  private boolean delegationTokenEnabled = false;
  private AbfsDelegationTokenManager delegationTokenManager;
  private AbfsCounters abfsCounters;

  @Override
  public void initialize(URI uri, Configuration configuration)
      throws IOException {
    uri = ensureAuthority(uri, configuration);
    super.initialize(uri, configuration);
    setConf(configuration);

    LOG.debug("Initializing AzureBlobFileSystem for {}", uri);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    abfsCounters = new AbfsCountersImpl(uri);
    this.abfsStore = new AzureBlobFileSystemStore(uri, this.isSecureScheme(),
        configuration, abfsCounters);
    LOG.trace("AzureBlobFileSystemStore init complete");

    final AbfsConfiguration abfsConfiguration = abfsStore.getAbfsConfiguration();
    this.setWorkingDirectory(this.getHomeDirectory());

    if (abfsConfiguration.getCreateRemoteFileSystemDuringInitialization()) {
      if (this.tryGetFileStatus(new Path(AbfsHttpConstants.ROOT_PATH)) == null) {
        try {
          this.createFileSystem();
        } catch (AzureBlobFileSystemException ex) {
          checkException(null, ex, AzureServiceErrorCode.FILE_SYSTEM_ALREADY_EXISTS);
        }
      }
    }

    LOG.trace("Initiate check for delegation token manager");
    if (UserGroupInformation.isSecurityEnabled()) {
      this.delegationTokenEnabled = abfsConfiguration.isDelegationTokenManagerEnabled();

      if (this.delegationTokenEnabled) {
        LOG.debug("Initializing DelegationTokenManager for {}", uri);
        this.delegationTokenManager = abfsConfiguration.getDelegationTokenManager();
        delegationTokenManager.bind(getUri(), configuration);
        LOG.debug("Created DelegationTokenManager {}", delegationTokenManager);
      }
    }

    AbfsClientThrottlingIntercept.initializeSingleton(abfsConfiguration.isAutoThrottlingEnabled());

    LOG.debug("Initializing AzureBlobFileSystem for {} complete", uri);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AzureBlobFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", user='").append(abfsStore.getUser()).append('\'');
    sb.append(", primaryUserGroup='").append(abfsStore.getPrimaryGroup()).append('\'');
    sb.append(", \nIOStatistics: {").append(ioStatisticsToString(getIOStatistics()));
    sb.append("}");
    sb.append('}');
    return sb.toString();
  }

  public boolean isSecureScheme() {
    return false;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    LOG.debug("AzureBlobFileSystem.open path: {} bufferSize: {}", path, bufferSize);
    // bufferSize is unused.
    return open(path, Optional.empty());
  }

  private FSDataInputStream open(final Path path,
      final Optional<Configuration> options) throws IOException {
    statIncrement(CALL_OPEN);
    Path qualifiedPath = makeQualified(path);

    try {
      InputStream inputStream = abfsStore.openFileForRead(qualifiedPath,
          options, statistics);
      return new FSDataInputStream(inputStream);
    } catch(AzureBlobFileSystemException ex) {
      checkException(path, ex);
      return null;
    }
  }

  @Override
  protected CompletableFuture<FSDataInputStream> openFileWithOptions(
      final Path path, final OpenFileParameters parameters) throws IOException {
    LOG.debug("AzureBlobFileSystem.openFileWithOptions path: {}", path);
    AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
        parameters.getMandatoryKeys(),
        Collections.emptySet(),
        "for " + path);
    return LambdaUtils.eval(
        new CompletableFuture<>(), () ->
            open(path, Optional.of(parameters.getOptions())));
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize,
      final short replication, final long blockSize, final Progressable progress) throws IOException {
    LOG.debug("AzureBlobFileSystem.create path: {} permission: {} overwrite: {} bufferSize: {}",
        f,
        permission,
        overwrite,
        blockSize);

    statIncrement(CALL_CREATE);
    trailingPeriodCheck(f);

    Path qualifiedPath = makeQualified(f);

    try {
      OutputStream outputStream = abfsStore.createFile(qualifiedPath, statistics, overwrite,
          permission == null ? FsPermission.getFileDefault() : permission, FsPermission.getUMask(getConf()));
      statIncrement(FILES_CREATED);
      return new FSDataOutputStream(outputStream, statistics);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {

    statIncrement(CALL_CREATE_NON_RECURSIVE);
    final Path parent = f.getParent();
    final FileStatus parentFileStatus = tryGetFileStatus(parent);

    if (parentFileStatus == null) {
      throw new FileNotFoundException("Cannot create file "
          + f.getName() + " because parent folder does not exist.");
    }

    return create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f, final FsPermission permission,
      final EnumSet<CreateFlag> flags, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {

    // Check if file should be appended or overwritten. Assume that the file
    // is overwritten on if the CREATE and OVERWRITE create flags are set.
    final EnumSet<CreateFlag> createflags =
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    final boolean overwrite = flags.containsAll(createflags);

    // Delegate the create non-recursive call.
    return this.createNonRecursive(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f,
      final boolean overwrite, final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.append path: {} bufferSize: {}",
        f.toString(),
        bufferSize);
    statIncrement(CALL_APPEND);
    Path qualifiedPath = makeQualified(f);

    try {
      OutputStream outputStream = abfsStore.openFileForWrite(qualifiedPath, statistics, false);
      return new FSDataOutputStream(outputStream, statistics);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  public boolean rename(final Path src, final Path dst) throws IOException {
    LOG.debug("AzureBlobFileSystem.rename src: {} dst: {}", src, dst);
    statIncrement(CALL_RENAME);

    trailingPeriodCheck(dst);

    Path parentFolder = src.getParent();
    if (parentFolder == null) {
      return false;
    }
    Path qualifiedSrcPath = makeQualified(src);
    Path qualifiedDstPath = makeQualified(dst);

    // rename under same folder;
    if(makeQualified(parentFolder).equals(qualifiedDstPath)) {
      return tryGetFileStatus(qualifiedSrcPath) != null;
    }

    FileStatus dstFileStatus = null;
    if (qualifiedSrcPath.equals(qualifiedDstPath)) {
      // rename to itself
      // - if it doesn't exist, return false
      // - if it is file, return true
      // - if it is dir, return false.
      dstFileStatus = tryGetFileStatus(qualifiedDstPath);
      if (dstFileStatus == null) {
        return false;
      }
      return dstFileStatus.isDirectory() ? false : true;
    }

    // Non-HNS account need to check dst status on driver side.
    if (!abfsStore.getIsNamespaceEnabled() && dstFileStatus == null) {
      dstFileStatus = tryGetFileStatus(qualifiedDstPath);
    }

    try {
      String sourceFileName = src.getName();
      Path adjustedDst = dst;

      if (dstFileStatus != null) {
        if (!dstFileStatus.isDirectory()) {
          return qualifiedSrcPath.equals(qualifiedDstPath);
        }
        adjustedDst = new Path(dst, sourceFileName);
      }

      qualifiedDstPath = makeQualified(adjustedDst);

      abfsStore.rename(qualifiedSrcPath, qualifiedDstPath);
      return true;
    } catch(AzureBlobFileSystemException ex) {
      LOG.debug("Rename operation failed. ", ex);
      checkException(
              src,
              ex,
              AzureServiceErrorCode.PATH_ALREADY_EXISTS,
              AzureServiceErrorCode.INVALID_RENAME_SOURCE_PATH,
              AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND,
              AzureServiceErrorCode.INVALID_SOURCE_OR_DESTINATION_RESOURCE_TYPE,
              AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND,
              AzureServiceErrorCode.INTERNAL_OPERATION_ABORT);
      return false;
    }

  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.delete path: {} recursive: {}", f.toString(), recursive);
    statIncrement(CALL_DELETE);
    Path qualifiedPath = makeQualified(f);

    if (f.isRoot()) {
      if (!recursive) {
        return false;
      }

      return deleteRoot();
    }

    try {
      abfsStore.delete(qualifiedPath, recursive);
      return true;
    } catch (AzureBlobFileSystemException ex) {
      checkException(f, ex, AzureServiceErrorCode.PATH_NOT_FOUND);
      return false;
    }

  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.listStatus path: {}", f.toString());
    statIncrement(CALL_LIST_STATUS);
    Path qualifiedPath = makeQualified(f);

    try {
      FileStatus[] result = abfsStore.listStatus(qualifiedPath);
      return result;
    } catch (AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  /**
   * Increment of an Abfs statistic.
   *
   * @param statistic AbfsStatistic that needs increment.
   */
  private void statIncrement(AbfsStatistic statistic) {
    incrementStatistic(statistic);
  }

  /**
   * Method for incrementing AbfsStatistic by a long value.
   *
   * @param statistic the Statistic to be incremented.
   */
  private void incrementStatistic(AbfsStatistic statistic) {
    if (abfsCounters != null) {
      abfsCounters.incrementCounter(statistic, 1);
    }
  }

  /**
   * Performs a check for (.) until root in the path to throw an exception.
   * The purpose is to differentiate between dir/dir1 and dir/dir1.
   * Without the exception the behavior seen is dir1. will appear
   * to be present without it's actual creation as dir/dir1 and dir/dir1. are
   * treated as identical.
   * @param path the path to be checked for trailing period (.)
   * @throws IllegalArgumentException if the path has a trailing period (.)
   */
  private void trailingPeriodCheck(Path path) throws IllegalArgumentException {
    while (!path.isRoot()){
      String pathToString = path.toString();
      if (pathToString.length() != 0) {
        if (pathToString.charAt(pathToString.length() - 1) == '.') {
          throw new IllegalArgumentException(
              "ABFS does not allow files or directories to end with a dot.");
        }
        path = path.getParent();
      }
      else {
        break;
      }
    }
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.mkdirs path: {} permissions: {}", f, permission);
    statIncrement(CALL_MKDIRS);
    trailingPeriodCheck(f);

    final Path parentFolder = f.getParent();
    if (parentFolder == null) {
      // Cannot create root
      return true;
    }

    Path qualifiedPath = makeQualified(f);

    try {
      abfsStore.createDirectory(qualifiedPath, permission == null ? FsPermission.getDirDefault() : permission,
          FsPermission.getUMask(getConf()));
      statIncrement(DIRECTORIES_CREATED);
      return true;
    } catch (AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return true;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (isClosed) {
      return;
    }
    // does all the delete-on-exit calls, and may be slow.
    super.close();
    LOG.debug("AzureBlobFileSystem.close");
    IOUtils.cleanupWithLogger(LOG, abfsStore, delegationTokenManager);
    this.isClosed = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing Abfs: {}", toString());
    }
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    LOG.debug("AzureBlobFileSystem.getFileStatus path: {}", f);
    statIncrement(CALL_GET_FILE_STATUS);
    Path qualifiedPath = makeQualified(f);

    try {
      return abfsStore.getFileStatus(qualifiedPath);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
      return null;
    }
  }

  /**
   * Break the current lease on an ABFS file if it exists. A lease that is broken cannot be
   * renewed. A new lease may be obtained on the file immediately.
   *
   * @param f file name
   * @throws IOException on any exception while breaking the lease
   */
  public void breakLease(final Path f) throws IOException {
    LOG.debug("AzureBlobFileSystem.breakLease path: {}", f);

    Path qualifiedPath = makeQualified(f);

    try (DurationInfo ignored = new DurationInfo(LOG, false, "Break lease for %s",
        qualifiedPath)) {
      abfsStore.breakLease(qualifiedPath);
    } catch(AzureBlobFileSystemException ex) {
      checkException(f, ex);
    }
  }

  /**
   * Qualify a path to one which uses this FileSystem and, if relative,
   * made absolute.
   * @param path to qualify.
   * @return this path if it contains a scheme and authority and is absolute, or
   * a new path that includes a path and authority and is fully qualified
   * @see Path#makeQualified(URI, Path)
   * @throws IllegalArgumentException if the path has a schema/URI different
   * from this FileSystem.
   */
  @Override
  public Path makeQualified(Path path) {
    // To support format: abfs://{dfs.nameservices}/file/path,
    // path need to be first converted to URI, then get the raw path string,
    // during which {dfs.nameservices} will be omitted.
    if (path != null) {
      String uriPath = path.toUri().getPath();
      path = uriPath.isEmpty() ? path : new Path(uriPath);
    }
    return super.makeQualified(path);
  }


  @Override
  public Path getWorkingDirectory() {
    return this.workingDir;
  }

  @Override
  public void setWorkingDirectory(final Path newDir) {
    if (newDir.isAbsolute()) {
      this.workingDir = newDir;
    } else {
      this.workingDir = new Path(workingDir, newDir);
    }
  }

  @Override
  public String getScheme() {
    return FileSystemUriSchemes.ABFS_SCHEME;
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(
            FileSystemConfigurations.USER_HOME_DIRECTORY_PREFIX
                + "/" + abfsStore.getUser()));
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file. For ABFS we'll just lie and give
   * fake hosts to make sure we get many splits in MR jobs.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) {
    if (file == null) {
      return null;
    }

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];
    }
    final String blobLocationHost = abfsStore.getAbfsConfiguration().getAzureBlockLocationHost();

    final String[] name = { blobLocationHost };
    final String[] host = { blobLocationHost };
    long blockSize = file.getBlockSize();
    if (blockSize <= 0) {
      throw new IllegalArgumentException(
          "The block size for the given file is not a positive number: "
              + blockSize);
    }
    int numberOfLocations = (int) (len / blockSize)
        + ((len % blockSize == 0) ? 0 : 1);
    BlockLocation[] locations = new BlockLocation[numberOfLocations];
    for (int i = 0; i < locations.length; i++) {
      long currentOffset = start + (i * blockSize);
      long currentLength = Math.min(blockSize, start + len - currentOffset);
      locations[i] = new BlockLocation(name, host, currentOffset, currentLength);
    }

    return locations;
  }

  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called.");
    close();
    super.finalize();
  }

  /**
   * Get the username of the FS.
   * @return the short name of the user who instantiated the FS
   */
  public String getOwnerUser() {
    return abfsStore.getUser();
  }

  /**
   * Get the group name of the owner of the FS.
   * @return primary group name
   */
  public String getOwnerUserPrimaryGroup() {
    return abfsStore.getPrimaryGroup();
  }

  private boolean deleteRoot() throws IOException {
    LOG.debug("Deleting root content");

    final ExecutorService executorService = Executors.newFixedThreadPool(10);

    try {
      final FileStatus[] ls = listStatus(makeQualified(new Path(File.separator)));
      final ArrayList<Future> deleteTasks = new ArrayList<>();
      for (final FileStatus fs : ls) {
        final Future deleteTask = executorService.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            delete(fs.getPath(), fs.isDirectory());
            if (fs.isDirectory()) {
              statIncrement(DIRECTORIES_DELETED);
            } else {
              statIncrement(FILES_DELETED);
            }
            return null;
          }
        });
        deleteTasks.add(deleteTask);
      }

      for (final Future deleteTask : deleteTasks) {
        execute("deleteRoot", new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            deleteTask.get();
            return null;
          }
        });
      }
    }
    finally {
      executorService.shutdownNow();
    }

    return true;
  }

   /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters owner and group cannot both be null.
   *
   * @param path  The path
   * @param owner If it is null, the original username remains unchanged.
   * @param group If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(final Path path, final String owner, final String group)
      throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.setOwner path: {}", path);
    if (!getIsNamespaceEnabled()) {
      super.setOwner(path, owner, group);
      return;
    }

    if ((owner == null || owner.isEmpty()) && (group == null || group.isEmpty())) {
      throw new IllegalArgumentException("A valid owner or group must be specified.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.setOwner(qualifiedPath,
              owner,
              group);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Set the value of an attribute for a path.
   *
   * @param path The path on which to set the attribute
   * @param name The attribute to set
   * @param value The byte value of the attribute to set (encoded in latin-1)
   * @param flag The mode in which to set the attribute
   * @throws IOException If there was an issue setting the attribute on Azure
   * @throws IllegalArgumentException If name is null or empty or if value is null
   */
  @Override
  public void setXAttr(final Path path, final String name, final byte[] value, final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.setXAttr path: {}", path);

    if (name == null || name.isEmpty() || value == null) {
      throw new IllegalArgumentException("A valid name and value must be specified.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      Hashtable<String, String> properties = abfsStore.getPathStatus(qualifiedPath);
      String xAttrName = ensureValidAttributeName(name);
      boolean xAttrExists = properties.containsKey(xAttrName);
      XAttrSetFlag.validate(name, xAttrExists, flag);

      String xAttrValue = abfsStore.decodeAttribute(value);
      properties.put(xAttrName, xAttrValue);
      abfsStore.setPathProperties(qualifiedPath, properties);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Get the value of an attribute for a path.
   *
   * @param path The path on which to get the attribute
   * @param name The attribute to get
   * @return The bytes of the attribute's value (encoded in latin-1)
   *         or null if the attribute does not exist
   * @throws IOException If there was an issue getting the attribute from Azure
   * @throws IllegalArgumentException If name is null or empty
   */
  @Override
  public byte[] getXAttr(final Path path, final String name)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.getXAttr path: {}", path);

    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("A valid name must be specified.");
    }

    Path qualifiedPath = makeQualified(path);

    byte[] value = null;
    try {
      Hashtable<String, String> properties = abfsStore.getPathStatus(qualifiedPath);
      String xAttrName = ensureValidAttributeName(name);
      if (properties.containsKey(xAttrName)) {
        String xAttrValue = properties.get(xAttrName);
        value = abfsStore.encodeAttribute(xAttrValue);
      }
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
    return value;
  }

  private static String ensureValidAttributeName(String attribute) {
    // to avoid HTTP 400 Bad Request, InvalidPropertyName
    return attribute.replace('.', '_');
  }

  /**
   * Set permission of a path.
   *
   * @param path       The path
   * @param permission Access permission
   */
  @Override
  public void setPermission(final Path path, final FsPermission permission)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.setPermission path: {}", path);
    if (!getIsNamespaceEnabled()) {
      super.setPermission(path, permission);
      return;
    }

    if (permission == null) {
      throw new IllegalArgumentException("The permission can't be null");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.setPermission(qualifiedPath,
              permission);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Modifies ACL entries of files and directories.  This method can add new ACL
   * entries or modify the permissions on existing ACL entries.  All existing
   * ACL entries that are not specified in this call are retained without
   * changes.  (Modifications are merged into the current ACL.)
   *
   * @param path    Path to modify
   * @param aclSpec List of AbfsAclEntry describing modifications
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.modifyAclEntries path: {}", path);

    if (!getIsNamespaceEnabled()) {
      throw new UnsupportedOperationException(
          "modifyAclEntries is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    if (aclSpec == null || aclSpec.isEmpty()) {
      throw new IllegalArgumentException("The value of the aclSpec parameter is invalid.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.modifyAclEntries(qualifiedPath,
              aclSpec);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Removes ACL entries from files and directories.  Other ACL entries are
   * retained.
   *
   * @param path    Path to modify
   * @param aclSpec List of AclEntry describing entries to remove
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.removeAclEntries path: {}", path);

    if (!getIsNamespaceEnabled()) {
      throw new UnsupportedOperationException(
          "removeAclEntries is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    if (aclSpec == null || aclSpec.isEmpty()) {
      throw new IllegalArgumentException("The aclSpec argument is invalid.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.removeAclEntries(qualifiedPath, aclSpec);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Removes all default ACL entries from files and directories.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void removeDefaultAcl(final Path path) throws IOException {
    LOG.debug("AzureBlobFileSystem.removeDefaultAcl path: {}", path);

    if (!getIsNamespaceEnabled()) {
      throw new UnsupportedOperationException(
          "removeDefaultAcl is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.removeDefaultAcl(qualifiedPath);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Removes all but the base ACL entries of files and directories.  The entries
   * for user, group, and others are retained for compatibility with permission
   * bits.
   *
   * @param path Path to modify
   * @throws IOException if an ACL could not be removed
   */
  @Override
  public void removeAcl(final Path path) throws IOException {
    LOG.debug("AzureBlobFileSystem.removeAcl path: {}", path);

    if (!getIsNamespaceEnabled()) {
      throw new UnsupportedOperationException(
          "removeAcl is only supported by storage accounts with the "
          + "hierarchical namespace enabled.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.removeAcl(qualifiedPath);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Fully replaces ACL of files and directories, discarding all existing
   * entries.
   *
   * @param path    Path to modify
   * @param aclSpec List of AclEntry describing modifications, must include
   *                entries for user, group, and others for compatibility with
   *                permission bits.
   * @throws IOException if an ACL could not be modified
   */
  @Override
  public void setAcl(final Path path, final List<AclEntry> aclSpec)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.setAcl path: {}", path);

    if (!getIsNamespaceEnabled()) {
      throw new UnsupportedOperationException(
          "setAcl is only supported by storage accounts with the hierarchical "
          + "namespace enabled.");
    }

    if (aclSpec == null || aclSpec.size() == 0) {
      throw new IllegalArgumentException("The aclSpec argument is invalid.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      abfsStore.setAcl(qualifiedPath, aclSpec);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
    }
  }

  /**
   * Gets the ACL of a file or directory.
   *
   * @param path Path to get
   * @return AbfsAclStatus describing the ACL of the file or directory
   * @throws IOException if an ACL could not be read
   */
  @Override
  public AclStatus getAclStatus(final Path path) throws IOException {
    LOG.debug("AzureBlobFileSystem.getAclStatus path: {}", path);

    if (!getIsNamespaceEnabled()) {
      throw new UnsupportedOperationException(
          "getAclStatus is only supported by storage account with the "
          + "hierarchical namespace enabled.");
    }

    Path qualifiedPath = makeQualified(path);

    try {
      return abfsStore.getAclStatus(qualifiedPath);
    } catch (AzureBlobFileSystemException ex) {
      checkException(path, ex);
      return null;
    }
  }

  /**
   * Checks if the user can access a path.  The mode specifies which access
   * checks to perform.  If the requested permissions are granted, then the
   * method returns normally.  If access is denied, then the method throws an
   * {@link AccessControlException}.
   *
   * @param path Path to check
   * @param mode type of access to check
   * @throws AccessControlException        if access is denied
   * @throws java.io.FileNotFoundException if the path does not exist
   * @throws IOException                   see specific implementation
   */
  @Override
  public void access(final Path path, final FsAction mode) throws IOException {
    LOG.debug("AzureBlobFileSystem.access path : {}, mode : {}", path, mode);
    Path qualifiedPath = makeQualified(path);
    try {
      this.abfsStore.access(qualifiedPath, mode);
    } catch (AzureBlobFileSystemException ex) {
      checkCheckAccessException(path, ex);
    }
  }

  /**
   * Incrementing exists() calls from superclass for statistic collection.
   *
   * @param f source path.
   * @return true if the path exists.
   * @throws IOException
   */
  @Override
  public boolean exists(Path f) throws IOException {
    statIncrement(CALL_EXIST);
    return super.exists(f);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException {
    LOG.debug("AzureBlobFileSystem.listStatusIterator path : {}", path);
    if (abfsStore.getAbfsConfiguration().enableAbfsListIterator()) {
      AbfsListStatusRemoteIterator abfsLsItr =
          new AbfsListStatusRemoteIterator(getFileStatus(path), abfsStore);
      return RemoteIterators.typeCastingRemoteIterator(abfsLsItr);
    } else {
      return super.listStatusIterator(path);
    }
  }

  private FileStatus tryGetFileStatus(final Path f) {
    try {
      return getFileStatus(f);
    } catch (IOException ex) {
      LOG.debug("File not found {}", f);
      statIncrement(ERROR_IGNORED);
      return null;
    }
  }

  private boolean fileSystemExists() throws IOException {
    LOG.debug(
            "AzureBlobFileSystem.fileSystemExists uri: {}", uri);
    try {
      abfsStore.getFilesystemProperties();
    } catch (AzureBlobFileSystemException ex) {
      try {
        checkException(null, ex);
        // Because HEAD request won't contain message body,
        // there is not way to get the storage error code
        // workaround here is to check its status code.
      } catch (FileNotFoundException e) {
        statIncrement(ERROR_IGNORED);
        return false;
      }
    }
    return true;
  }

  private void createFileSystem() throws IOException {
    LOG.debug(
        "AzureBlobFileSystem.createFileSystem uri: {}", uri);
    try {
      abfsStore.createFilesystem();
    } catch (AzureBlobFileSystemException ex) {
      checkException(null, ex);
    }
  }

  private URI ensureAuthority(URI uri, final Configuration conf) {

    Preconditions.checkNotNull(uri, "uri");

    if (uri.getAuthority() == null) {
      final URI defaultUri = FileSystem.getDefaultUri(conf);

      if (defaultUri != null && isAbfsScheme(defaultUri.getScheme())) {
        try {
          // Reconstruct the URI with the authority from the default URI.
          uri = new URI(
              uri.getScheme(),
              defaultUri.getAuthority(),
              uri.getPath(),
              uri.getQuery(),
              uri.getFragment());
        } catch (URISyntaxException e) {
          // This should never happen.
          throw new IllegalArgumentException(new InvalidUriException(uri.toString()));
        }
      }
    }

    if (uri.getAuthority() == null) {
      throw new IllegalArgumentException(new InvalidUriAuthorityException(uri.toString()));
    }

    return uri;
  }

  private boolean isAbfsScheme(final String scheme) {
    if (scheme == null) {
      return false;
    }

    if (scheme.equals(FileSystemUriSchemes.ABFS_SCHEME)
        || scheme.equals(FileSystemUriSchemes.ABFS_SECURE_SCHEME)) {
      return true;
    }

    return false;
  }

  @VisibleForTesting
  <T> FileSystemOperation<T> execute(
      final String scopeDescription,
      final Callable<T> callableFileOperation) throws IOException {
    return execute(scopeDescription, callableFileOperation, null);
  }

  @VisibleForTesting
  <T> FileSystemOperation<T> execute(
      final String scopeDescription,
      final Callable<T> callableFileOperation,
      T defaultResultValue) throws IOException {

    try {
      final T executionResult = callableFileOperation.call();
      return new FileSystemOperation<>(executionResult, null);
    } catch (AbfsRestOperationException abfsRestOperationException) {
      return new FileSystemOperation<>(defaultResultValue, abfsRestOperationException);
    } catch (AzureBlobFileSystemException azureBlobFileSystemException) {
      throw new IOException(azureBlobFileSystemException);
    } catch (Exception exception) {
      if (exception instanceof ExecutionException) {
        exception = (Exception) getRootCause(exception);
      }
      final FileSystemOperationUnhandledException fileSystemOperationUnhandledException
          = new FileSystemOperationUnhandledException(exception);
      throw new IOException(fileSystemOperationUnhandledException);
    }
  }

  private void checkCheckAccessException(final Path path,
      final AzureBlobFileSystemException exception) throws IOException {
    if (exception instanceof AbfsRestOperationException) {
      AbfsRestOperationException ere = (AbfsRestOperationException) exception;
      if (ere.getStatusCode() == HttpURLConnection.HTTP_FORBIDDEN) {
        throw (IOException) new AccessControlException(ere.getMessage())
            .initCause(exception);
      }
    }
    checkException(path, exception);
  }

  /**
   * Given a path and exception, choose which IOException subclass
   * to create.
   * Will return if and only iff the error code is in the list of allowed
   * error codes.
   * @param path path of operation triggering exception; may be null
   * @param exception the exception caught
   * @param allowedErrorCodesList varargs list of error codes.
   * @throws IOException if the exception error code is not on the allowed list.
   */
  @VisibleForTesting
  static void checkException(final Path path,
                              final AzureBlobFileSystemException exception,
                              final AzureServiceErrorCode... allowedErrorCodesList) throws IOException {
    if (exception instanceof AbfsRestOperationException) {
      AbfsRestOperationException ere = (AbfsRestOperationException) exception;

      if (ArrayUtils.contains(allowedErrorCodesList, ere.getErrorCode())) {
        return;
      }
      //AbfsRestOperationException.getMessage() contains full error info including path/uri.
      String message = ere.getMessage();

      switch (ere.getStatusCode()) {
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw (IOException) new FileNotFoundException(message)
            .initCause(exception);
      case HttpURLConnection.HTTP_CONFLICT:
        throw (IOException) new FileAlreadyExistsException(message)
            .initCause(exception);
      case HttpURLConnection.HTTP_FORBIDDEN:
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        throw (IOException) new AccessDeniedException(message)
            .initCause(exception);
      default:
        throw ere;
      }
    } else if (exception instanceof SASTokenProviderException) {
      throw exception;
    } else {
      if (path == null) {
        throw exception;
      }
      // record info of path
      throw new PathIOException(path.toString(), exception);
    }
  }

  /**
   * Gets the root cause of a provided {@link Throwable}.  If there is no cause for the
   * {@link Throwable} provided into this function, the original {@link Throwable} is returned.
   *
   * @param throwable starting {@link Throwable}
   * @return root cause {@link Throwable}
   */
  private Throwable getRootCause(Throwable throwable) {
    if (throwable == null) {
      throw new IllegalArgumentException("throwable can not be null");
    }

    Throwable result = throwable;
    while (result.getCause() != null) {
      result = result.getCause();
    }

    return result;
  }

  /**
   * Get a delegation token from remote service endpoint if
   * 'fs.azure.enable.kerberos.support' is set to 'true', and
   * 'fs.azure.enable.delegation.token' is set to 'true'.
   * @param renewer the account name that is allowed to renew the token.
   * @return delegation token
   * @throws IOException thrown when getting the current user.
   */
  @Override
  public synchronized Token<?> getDelegationToken(final String renewer) throws IOException {
    statIncrement(CALL_GET_DELEGATION_TOKEN);
    return this.delegationTokenEnabled ? this.delegationTokenManager.getDelegationToken(renewer)
        : super.getDelegationToken(renewer);
  }

  /**
   * If Delegation tokens are enabled, the canonical service name of
   * this filesystem is the filesystem URI.
   * @return either the filesystem URI as a string, or null.
   */
  @Override
  public String getCanonicalServiceName() {
    String name = null;
    if (delegationTokenManager != null) {
      name = delegationTokenManager.getCanonicalServiceName();
    }
    return name != null ? name : super.getCanonicalServiceName();
  }

  @VisibleForTesting
  FileSystem.Statistics getFsStatistics() {
    return this.statistics;
  }

  @VisibleForTesting
  static class FileSystemOperation<T> {
    private final T result;
    private final AbfsRestOperationException exception;

    FileSystemOperation(final T result, final AbfsRestOperationException exception) {
      this.result = result;
      this.exception = exception;
    }

    public boolean failed() {
      return this.exception != null;
    }
  }

  @VisibleForTesting
  AzureBlobFileSystemStore getAbfsStore() {
    return abfsStore;
  }

  @VisibleForTesting
  AbfsClient getAbfsClient() {
    return abfsStore.getClient();
  }

  /**
   * Get any Delegation Token manager created by the filesystem.
   * @return the DT manager or null.
   */
  @VisibleForTesting
  AbfsDelegationTokenManager getDelegationTokenManager() {
    return delegationTokenManager;
  }

  @VisibleForTesting
  boolean getIsNamespaceEnabled() throws AzureBlobFileSystemException {
    return abfsStore.getIsNamespaceEnabled();
  }

  /**
   * Returns the counter() map in IOStatistics containing all the counters
   * and their values.
   *
   * @return Map of IOStatistics counters.
   */
  @VisibleForTesting
  Map<String, Long> getInstrumentationMap() {
    return abfsCounters.toMap();
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    // qualify the path to make sure that it refers to the current FS.
    final Path p = makeQualified(path);
    switch (validatePathCapabilityArgs(p, capability)) {
    case CommonPathCapabilities.FS_PERMISSIONS:
    case CommonPathCapabilities.FS_APPEND:
      return true;
    case CommonPathCapabilities.FS_ACLS:
      return getIsNamespaceEnabled();
    default:
      return super.hasPathCapability(p, capability);
    }
  }

  /**
   * Getter for IOStatistic instance in AzureBlobFilesystem.
   *
   * @return the IOStatistic instance from abfsCounters.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return abfsCounters != null ? abfsCounters.getIOStatistics() : null;
  }
}
