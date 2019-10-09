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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.azurebfs.services.AbfsAclHelper;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsPermission;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.LatencyTracker;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.CRC64;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_HYPHEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_PLUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_UNDERSCORE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TOKEN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ABFS_ENDPOINT;

/**
 * Provides the bridging logic between Hadoop's abstract filesystem and Azure Storage.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureBlobFileSystemStore implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystemStore.class);

  private AbfsClient client;
  private URI uri;
  private String userName;
  private String primaryUserGroup;
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss z";
  private static final String TOKEN_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int LIST_MAX_RESULTS = 500;

  private final AbfsConfiguration abfsConfiguration;
  private final Set<String> azureAtomicRenameDirSet;
  private boolean isNamespaceEnabledSet;
  private boolean isNamespaceEnabled;
  private final AuthType authType;
  private final UserGroupInformation userGroupInformation;
  private final IdentityTransformer identityTransformer;
  private final LatencyTracker latencyTracker;

  public AzureBlobFileSystemStore(URI uri, boolean isSecureScheme, Configuration configuration)
          throws IOException {
    this.uri = uri;
    String[] authorityParts = authorityParts(uri);
    final String fileSystemName = authorityParts[0];
    final String accountName = authorityParts[1];

    try {
      this.abfsConfiguration = new AbfsConfiguration(configuration, accountName);
    } catch (IllegalAccessException exception) {
      throw new FileSystemOperationUnhandledException(exception);
    }
    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.userName = userGroupInformation.getShortUserName();
    if (!abfsConfiguration.getSkipUserGroupMetadataDuringInitialization()) {
      try {
        this.primaryUserGroup = userGroupInformation.getPrimaryGroupName();
      } catch (IOException ex) {
        LOG.error("Failed to get primary group for {}, using user name as primary group name", userName);
        this.primaryUserGroup = userName;
      }
    } else {
      //Provide a default group name
      this.primaryUserGroup = userName;
    }

    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureAtomicRenameDirs().split(AbfsHttpConstants.COMMA)));
    this.authType = abfsConfiguration.getAuthType(accountName);
    boolean usingOauth = (authType == AuthType.OAuth);
    boolean useHttps = (usingOauth || abfsConfiguration.isHttpsAlwaysUsed()) ? true : isSecureScheme;
    this.latencyTracker = new LatencyTracker(fileSystemName, accountName, this.abfsConfiguration);
    initializeClient(uri, fileSystemName, accountName, useHttps);
    this.identityTransformer = new IdentityTransformer(abfsConfiguration.getRawConfiguration());
  }

  /**
   * @return local user name.
   * */
  public String getUser() {
    return this.userName;
  }

  /**
  * @return primary group that user belongs to.
  * */
  public String getPrimaryGroup() {
    return this.primaryUserGroup;
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG, client);
  }

  private String[] authorityParts(URI uri) throws InvalidUriAuthorityException, InvalidUriException {
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || authorityParts[0] != null
        && authorityParts[0].isEmpty()) {
      final String errMsg = String
              .format("'%s' has a malformed authority, expected container name. "
                      + "Authority takes the form "
                      + FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
                      uri.toString());
      throw new InvalidUriException(errMsg);
    }
    return authorityParts;
  }

  public boolean getIsNamespaceEnabled() throws AzureBlobFileSystemException {
    if (!isNamespaceEnabledSet) {
      final Instant start = latencyTracker.getLatencyInstant();
      boolean success = false;
      AbfsHttpOperation res = null;

      LOG.debug("Get root ACL status");
      try {
        AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + AbfsHttpConstants.ROOT_PATH);
        res = op.getResult();
        isNamespaceEnabled = true;
        success = true;
      } catch (AbfsRestOperationException ex) {
        // Get ACL status is a HEAD request, its response doesn't contain errorCode
        // So can only rely on its status code to determine its account type.
        if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
          throw ex;
        }
        isNamespaceEnabled = false;
      } finally {
        latencyTracker.recordClientLatency(start, "getIsNamespaceEnabled", "getAclStatus", success, res);
      }
      isNamespaceEnabledSet = true;
    }

    return isNamespaceEnabled;
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, boolean isSecure) {
    String scheme = isSecure ? FileSystemUriSchemes.HTTPS_SCHEME : FileSystemUriSchemes.HTTP_SCHEME;

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);

    // For testing purposes, an IP address and port may be provided to override
    // the host specified in the FileSystem URI.  Also note that the format of
    // the Azure Storage Service URI changes from
    // http[s]://[account][domain-suffix]/[filesystem] to
    // http[s]://[ip]:[port]/[account]/[filesystem].
    String endPoint = abfsConfiguration.get(AZURE_ABFS_ENDPOINT);
    if (endPoint == null || !endPoint.contains(AbfsHttpConstants.COLON)) {
      uriBuilder.setHost(hostName);
      return uriBuilder;
    }

    // Split ip and port
    String[] data = endPoint.split(AbfsHttpConstants.COLON);
    if (data.length != 2) {
      throw new RuntimeException(String.format("ABFS endpoint is not set correctly : %s, "
              + "Do not specify scheme when using {IP}:{PORT}", endPoint));
    }
    uriBuilder.setHost(data[0].trim());
    uriBuilder.setPort(Integer.parseInt(data[1].trim()));
    uriBuilder.setPath("/" + UriUtils.extractAccountNameFromHostName(hostName));

    return uriBuilder;
  }

  public AbfsConfiguration getAbfsConfiguration() {
    return this.abfsConfiguration;
  }

  public Hashtable<String, String> getFilesystemProperties() throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("getFilesystemProperties for filesystem: {}",
              client.getFileSystem());

      final Hashtable<String, String> parsedXmsProperties;

      final AbfsRestOperation op = client.getFilesystemProperties();
      res = op.getResult();

      final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

      parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);
      success = true;

      return parsedXmsProperties;
    } finally {
      latencyTracker.recordClientLatency(start, "getFilesystemProperties", "getFilesystemProperties", success, res);
    }
  }

  public void setFilesystemProperties(final Hashtable<String, String> properties)
      throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      if (properties == null || properties.isEmpty()) {
        return;
      }

      LOG.debug("setFilesystemProperties for filesystem: {} with properties: {}",
              client.getFileSystem(),
              properties);

      final String commaSeparatedProperties;
      try {
        commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
      } catch (CharacterCodingException ex) {
        throw new InvalidAbfsRestOperationException(ex);
      }

      final AbfsRestOperation op = client.setFilesystemProperties(commaSeparatedProperties);
      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "setFilesystemProperties", "setFilesystemProperties", success, res);
    }
  }

  public Hashtable<String, String> getPathStatus(final Path path) throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("getPathStatus for filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      final Hashtable<String, String> parsedXmsProperties;
      final AbfsRestOperation op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
      res = op.getResult();

      final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

      parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);

      success = true;

      return parsedXmsProperties;
    } finally {
      latencyTracker.recordClientLatency(start, "getPathStatus", "getPathStatus", success, res);
    }
  }

  public void setPathProperties(final Path path, final Hashtable<String, String> properties) throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("setFilesystemProperties for filesystem: {} path: {} with properties: {}",
              client.getFileSystem(),
              path,
              properties);

      final String commaSeparatedProperties;
      try {
        commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
      } catch (CharacterCodingException ex) {
        throw new InvalidAbfsRestOperationException(ex);
      }
      final AbfsRestOperation op = client.setPathProperties(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), commaSeparatedProperties);
      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "setPathProperties", "setPathProperties", success, res);
    }
  }

  public void createFilesystem() throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("createFilesystem for filesystem: {}",
              client.getFileSystem());

      final AbfsRestOperation op = client.createFilesystem();
      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "createFilesystem", "createFilesystem", success, res);
    }
  }

  public void deleteFilesystem() throws AzureBlobFileSystemException {
    Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("deleteFilesystem for filesystem: {}",
              client.getFileSystem());

      final AbfsRestOperation op = client.deleteFilesystem();
      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "deleteFilesystem", "deleteFilesystem", success, res);
    }
  }

  public OutputStream createFile(final Path path, final boolean overwrite, final FsPermission permission,
                                 final FsPermission umask) throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      boolean isNamespaceEnabled = getIsNamespaceEnabled();
      LOG.debug("createFile filesystem: {} path: {} overwrite: {} permission: {} umask: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              overwrite,
              permission.toString(),
              umask.toString(),
              isNamespaceEnabled);

      final AbfsRestOperation op = client.createPath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), true, overwrite,
              isNamespaceEnabled ? getOctalNotation(permission) : null,
              isNamespaceEnabled ? getOctalNotation(umask) : null);
      res = op.getResult();

      success = true;

      return new AbfsOutputStream(
              client,
              AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path),
              0,
              abfsConfiguration.getWriteBufferSize(),
              abfsConfiguration.isFlushEnabled(),
              abfsConfiguration.isOutputStreamFlushDisabled());
    } finally {
      latencyTracker.recordClientLatency(start, "createFile", "createPath", success, res);
    }
  }

  public void createDirectory(final Path path, final FsPermission permission, final FsPermission umask)
      throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      boolean isNamespaceEnabled = getIsNamespaceEnabled();
      LOG.debug("createDirectory filesystem: {} path: {} permission: {} umask: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              permission,
              umask,
              isNamespaceEnabled);

      final AbfsRestOperation op = client.createPath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), false, true,
              isNamespaceEnabled ? getOctalNotation(permission) : null,
              isNamespaceEnabled ? getOctalNotation(umask) : null);
      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "createDirectory", "createPath", success, res);
    }
  }

  public AbfsInputStream openFileForRead(final Path path, final FileSystem.Statistics statistics)
      throws AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("openFileForRead filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      final AbfsRestOperation op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
      res = op.getResult();

      final String resourceType = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
      final long contentLength = Long.parseLong(op.getResult().getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      if (parseIsDirectory(resourceType)) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "openFileForRead must be used with files and not directories",
                null);
      }

      success = true;

      // Add statistics for InputStream
      return new AbfsInputStream(client, statistics,
              AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), contentLength,
              abfsConfiguration.getReadBufferSize(), abfsConfiguration.getReadAheadQueueDepth(),
              abfsConfiguration.getTolerateOobAppends(), eTag);
    } finally {
      latencyTracker.recordClientLatency(start, "openFileForRead", "getPathStatus", success, res);
    }
  }

  public OutputStream openFileForWrite(final Path path, final boolean overwrite) throws
          AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      LOG.debug("openFileForWrite filesystem: {} path: {} overwrite: {}",
              client.getFileSystem(),
              path,
              overwrite);

      final AbfsRestOperation op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
      res = op.getResult();

      final String resourceType = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
      final Long contentLength = Long.valueOf(op.getResult().getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));

      if (parseIsDirectory(resourceType)) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "openFileForRead must be used with files and not directories",
                null);
      }

      final long offset = overwrite ? 0 : contentLength;

      success = true;

      return new AbfsOutputStream(
              client,
              AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path),
              offset,
              abfsConfiguration.getWriteBufferSize(),
              abfsConfiguration.isFlushEnabled(),
              abfsConfiguration.isOutputStreamFlushDisabled());
    } finally {
      latencyTracker.recordClientLatency(start, "openFileForWrite", "getPathStatus", success, res);
    }
  }

  public void rename(final Path source, final Path destination) throws
          AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    if (isAtomicRenameKey(source.getName())) {
      LOG.warn("The atomic rename feature is not supported by the ABFS scheme; however rename,"
              +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    LOG.debug("renameAsync filesystem: {} source: {} destination: {}",
            client.getFileSystem(),
            source,
            destination);

    String continuation = null;

    do {
      Instant start = latencyTracker.getLatencyInstant();
      boolean success = false;
      AbfsHttpOperation res = null;

      try {
        AbfsRestOperation op = client.renamePath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(source),
                AbfsHttpConstants.FORWARD_SLASH + getRelativePath(destination), continuation);
        res = op.getResult();
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        success = true;
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();
      } finally {
        if (shouldContinue) {
          latencyTracker.recordClientLatency(start, "rename", "renamePath", success, res);
        } else {
          latencyTracker.recordClientLatency(start, "rename", "renamePath", success, startAggregate, countAggregate, res);
        }
      }
    } while (shouldContinue);
  }

  public void delete(final Path path, final boolean recursive)
      throws AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("delete filesystem: {} path: {} recursive: {}",
            client.getFileSystem(),
            path,
            String.valueOf(recursive));

    String continuation = null;

    do {
      Instant start = latencyTracker.getLatencyInstant();
      boolean success = false;
      AbfsHttpOperation res = null;

      try {
        AbfsRestOperation op = client.deletePath(
                AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), recursive, continuation);
        res = op.getResult();
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        success = true;
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();
      } finally {
        if (shouldContinue) {
          latencyTracker.recordClientLatency(start, "delete", "deletePath", success, res);
        } else {
          latencyTracker.recordClientLatency(start, "delete", "deletePath", success, startAggregate, countAggregate, res);
        }
      }
    } while (shouldContinue);
  }

  public FileStatus getFileStatus(final Path path) throws IOException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;
    String calleeName = null;

    try {
      boolean isNamespaceEnabled = getIsNamespaceEnabled();
      LOG.debug("getFileStatus filesystem: {} path: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              isNamespaceEnabled);

      final AbfsRestOperation op;
      if (path.isRoot()) {
        if (isNamespaceEnabled) {
          calleeName = "getAclStatus";
          op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + AbfsHttpConstants.ROOT_PATH);
        } else {
          calleeName = "getFilesystemProperties";
          op = client.getFilesystemProperties();
        }
      } else {
        calleeName = "getPathStatus";
        op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
      }

      res = op.getResult();
      final long blockSize = abfsConfiguration.getAzureBlockSize();
      final AbfsHttpOperation result = op.getResult();

      final String eTag = result.getResponseHeader(HttpHeaderConfigurations.ETAG);
      final String lastModified = result.getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
      final String permissions = result.getResponseHeader((HttpHeaderConfigurations.X_MS_PERMISSIONS));
      final boolean hasAcl = AbfsPermission.isExtendedAcl(permissions);
      final long contentLength;
      final boolean resourceIsDir;

      if (path.isRoot()) {
        contentLength = 0;
        resourceIsDir = true;
      } else {
        contentLength = parseContentLength(result.getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));
        resourceIsDir = parseIsDirectory(result.getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE));
      }

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);

      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      success = true;

      return new VersionedFileStatus(
              transformedOwner,
              transformedGroup,
              permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                      : AbfsPermission.valueOf(permissions),
              hasAcl,
              contentLength,
              resourceIsDir,
              1,
              blockSize,
              parseLastModifiedTime(lastModified),
              path,
              eTag);
    } finally {
      latencyTracker.recordClientLatency(start, "getFileStatus", calleeName, success, res);
    }
  }

  /**
   * @param path The list path.
   * @return the entries in the path.
   * */
  public FileStatus[] listStatus(final Path path) throws IOException {
    return listStatus(path, null);
  }

  /**
   * @param path Path the list path.
   * @param startFrom the entry name that list results should start with.
   *                  For example, if folder "/folder" contains four files: "afile", "bfile", "hfile", "ifile".
   *                  Then listStatus(Path("/folder"), "hfile") will return "/folder/hfile" and "folder/ifile"
   *                  Notice that if startFrom is a non-existent entry name, then the list response contains
   *                  all entries after this non-existent entry in lexical order:
   *                  listStatus(Path("/folder"), "cfile") will return "/folder/hfile" and "/folder/ifile".
   *
   * @return the entries in the path start from  "startFrom" in lexical order.
   * */
  @InterfaceStability.Unstable
  public FileStatus[] listStatus(final Path path, final String startFrom) throws IOException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("listStatus filesystem: {} path: {}, startFrom: {}",
            client.getFileSystem(),
            path,
            startFrom);

    final String relativePath = path.isRoot() ? AbfsHttpConstants.EMPTY_STRING : getRelativePath(path);
    String continuation = null;

    // generate continuation token if a valid startFrom is provided.
    if (startFrom != null && !startFrom.isEmpty()) {
      continuation = getIsNamespaceEnabled()
              ? generateContinuationTokenForXns(startFrom)
              : generateContinuationTokenForNonXns(path.isRoot() ? ROOT_PATH : relativePath, startFrom);
    }

    ArrayList<FileStatus> fileStatuses = new ArrayList<>();
    do {
      Instant start = latencyTracker.getLatencyInstant();
      boolean success = false;
      AbfsHttpOperation res = null;

      try {
        AbfsRestOperation op = client.listPath(relativePath, false, LIST_MAX_RESULTS, continuation);
        res = op.getResult();
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
        if (retrievedSchema == null) {
          throw new AbfsRestOperationException(
                  AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                  AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                  "listStatusAsync path not found",
                  null, op.getResult());
        }

        long blockSize = abfsConfiguration.getAzureBlockSize();

        for (ListResultEntrySchema entry : retrievedSchema.paths()) {
          final String owner = identityTransformer.transformIdentityForGetRequest(entry.owner(), true, userName);
          final String group = identityTransformer.transformIdentityForGetRequest(entry.group(), false, primaryUserGroup);
          final FsPermission fsPermission = entry.permissions() == null
                  ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                  : AbfsPermission.valueOf(entry.permissions());
          final boolean hasAcl = AbfsPermission.isExtendedAcl(entry.permissions());

          long lastModifiedMillis = 0;
          long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
          boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
          if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
            lastModifiedMillis = parseLastModifiedTime(entry.lastModified());
          }

          Path entryPath = new Path(File.separator + entry.name());
          entryPath = entryPath.makeQualified(this.uri, entryPath);

          fileStatuses.add(
                  new VersionedFileStatus(
                          owner,
                          group,
                          fsPermission,
                          hasAcl,
                          contentLength,
                          isDirectory,
                          1,
                          blockSize,
                          lastModifiedMillis,
                          entryPath,
                          entry.eTag()));
        }

        success = true;
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();
      } finally {
        if (shouldContinue) {
          latencyTracker.recordClientLatency(start, "listStatus", "listPath", success, res);
        } else {
          latencyTracker.recordClientLatency(start, "listStatus", "listPath", success, startAggregate, countAggregate, res);
        }
      }
    } while (shouldContinue);

    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  // generate continuation token for xns account
  private String generateContinuationTokenForXns(final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    StringBuilder sb = new StringBuilder();
    sb.append(firstEntryName).append("#$").append("0");

    CRC64 crc64 = new CRC64();
    StringBuilder token = new StringBuilder();
    token.append(crc64.compute(sb.toString().getBytes(StandardCharsets.UTF_8)))
            .append(SINGLE_WHITE_SPACE)
            .append("0")
            .append(SINGLE_WHITE_SPACE)
            .append(firstEntryName);

    return Base64.encode(token.toString().getBytes(StandardCharsets.UTF_8));
  }

  // generate continuation token for non-xns account
  private String generateContinuationTokenForNonXns(final String path, final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    // Notice: non-xns continuation token requires full path (first "/" is not included) for startFrom
    final String startFrom = (path.isEmpty() || path.equals(ROOT_PATH))
            ? firstEntryName
            : path + ROOT_PATH + firstEntryName;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TOKEN_DATE_PATTERN, Locale.US);
    String date = simpleDateFormat.format(new Date());
    String token = String.format("%06d!%s!%06d!%s!%06d!%s!",
            path.length(), path, startFrom.length(), startFrom, date.length(), date);
    String base64EncodedToken = Base64.encode(token.getBytes(StandardCharsets.UTF_8));

    StringBuilder encodedTokenBuilder = new StringBuilder(base64EncodedToken.length() + 5);
    encodedTokenBuilder.append(String.format("%s!%d!", TOKEN_VERSION, base64EncodedToken.length()));

    for (int i = 0; i < base64EncodedToken.length(); i++) {
      char current = base64EncodedToken.charAt(i);
      if (CHAR_FORWARD_SLASH == current) {
        current = CHAR_UNDERSCORE;
      } else if (CHAR_PLUS == current) {
        current = CHAR_STAR;
      } else if (CHAR_EQUALS == current) {
        current = CHAR_HYPHEN;
      }
      encodedTokenBuilder.append(current);
    }

    return encodedTokenBuilder.toString();
  }

  public void setOwner(final Path path, final String owner, final String group) throws
          AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "setOwner filesystem: {} path: {} owner: {} group: {}",
              client.getFileSystem(),
              path.toString(),
              owner,
              group);

      final String transformedOwner = identityTransformer.transformUserOrGroupForSetRequest(owner);
      final String transformedGroup = identityTransformer.transformUserOrGroupForSetRequest(group);

      final AbfsRestOperation op = client.setOwner(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), transformedOwner, transformedGroup);

      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "setOwner", "setOwner", success, res);
    }
  }

  public void setPermission(final Path path, final FsPermission permission) throws
          AzureBlobFileSystemException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation res = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "setPermission filesystem: {} path: {} permission: {}",
              client.getFileSystem(),
              path.toString(),
              permission.toString());
      final AbfsRestOperation op = client.setPermission(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
              String.format(AbfsHttpConstants.PERMISSION_FORMAT, permission.toOctal()));

      res = op.getResult();

      success = true;
    } finally {
      latencyTracker.recordClientLatency(start, "setPermission", "setPermission", success, res);
    }
  }

  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec) throws
          AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    Instant startSet = null;

    boolean successGet = false;
    boolean successSet = false;
    AbfsHttpOperation resultGet = null;
    AbfsHttpOperation resultSet = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "modifyAclEntries filesystem: {} path: {} aclSpec: {}",
              client.getFileSystem(),
              path.toString(),
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> modifyAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean useUpn = AbfsAclHelper.isUpnFormatAclEntries(modifyAclEntries);

      final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), useUpn);
      resultGet = op.getResult();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.modifyAclEntriesInternal(aclEntries, modifyAclEntries);

      successGet = true;
      countAggregate++;
      startSet = latencyTracker.getLatencyInstant();

      final AbfsRestOperation setAclOp = client.setAcl(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
              AbfsAclHelper.serializeAclSpec(aclEntries), eTag);
      resultSet = setAclOp.getResult();

      successSet = true;
      countAggregate++;
    } finally {
      latencyTracker.recordClientLatency(startAggregate, startSet, "modifyAclEntries", "getAclStatus", successGet, resultGet);
      latencyTracker.recordClientLatency(startSet, "modifyAclEntries", "setAcl", successSet, startAggregate, countAggregate, resultSet);
    }
  }

  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec) throws AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    Instant startSet = null;

    boolean successGet = false;
    boolean successSet = false;
    AbfsHttpOperation resultGet = null;
    AbfsHttpOperation resultSet = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "removeAclEntries filesystem: {} path: {} aclSpec: {}",
              client.getFileSystem(),
              path.toString(),
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> removeAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(removeAclEntries);

      final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), isUpnFormat);
      resultGet = op.getResult();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.removeAclEntriesInternal(aclEntries, removeAclEntries);

      successGet = true;
      countAggregate++;
      startSet = latencyTracker.getLatencyInstant();

      final AbfsRestOperation setAclOp = client.setAcl(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
              AbfsAclHelper.serializeAclSpec(aclEntries), eTag);
      resultSet = setAclOp.getResult();

      successSet = true;
      countAggregate++;
    } finally {
      latencyTracker.recordClientLatency(startAggregate, startSet, "removeAclEntries", "getAclStatus", successGet, resultGet);
      latencyTracker.recordClientLatency(startSet, "removeAclEntries", "setAcl", successSet, startAggregate, countAggregate, resultSet);
    }
  }

  public void removeDefaultAcl(final Path path) throws AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    Instant startSet = null;

    boolean successGet = false;
    boolean successSet = false;
    AbfsHttpOperation resultGet = null;
    AbfsHttpOperation resultSet = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "removeDefaultAcl filesystem: {} path: {}",
              client.getFileSystem(),
              path.toString());

      final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true));
      resultGet = op.getResult();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> defaultAclEntries = new HashMap<>();

      for (Map.Entry<String, String> aclEntry : aclEntries.entrySet()) {
        if (aclEntry.getKey().startsWith("default:")) {
          defaultAclEntries.put(aclEntry.getKey(), aclEntry.getValue());
        }
      }

      aclEntries.keySet().removeAll(defaultAclEntries.keySet());

      successGet = true;
      countAggregate++;
      startSet = latencyTracker.getLatencyInstant();

      final AbfsRestOperation setAclOp = client.setAcl(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
      AbfsAclHelper.serializeAclSpec(aclEntries), eTag);
      resultSet = setAclOp.getResult();

      successSet = true;
      countAggregate++;
    } finally {
      latencyTracker.recordClientLatency(startAggregate, startSet, "removeDefaultAcl", "getAclStatus", successGet, resultGet);
      latencyTracker.recordClientLatency(startSet, "removeDefaultAcl", "setAcl", successSet, startAggregate, countAggregate, resultSet);
    }
  }

  public void removeAcl(final Path path) throws AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    Instant startSet = null;

    boolean successGet = false;
    boolean successSet = false;
    AbfsHttpOperation resultGet = null;
    AbfsHttpOperation resultSet = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "removeAcl filesystem: {} path: {}",
              client.getFileSystem(),
              path.toString());
      final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true));
      resultGet = op.getResult();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> newAclEntries = new HashMap<>();

      newAclEntries.put(AbfsHttpConstants.ACCESS_USER, aclEntries.get(AbfsHttpConstants.ACCESS_USER));
      newAclEntries.put(AbfsHttpConstants.ACCESS_GROUP, aclEntries.get(AbfsHttpConstants.ACCESS_GROUP));
      newAclEntries.put(AbfsHttpConstants.ACCESS_OTHER, aclEntries.get(AbfsHttpConstants.ACCESS_OTHER));

      successGet = true;
      countAggregate++;
      startSet = latencyTracker.getLatencyInstant();

      final AbfsRestOperation setAclOp = client.setAcl(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
      AbfsAclHelper.serializeAclSpec(newAclEntries), eTag);

      resultSet = setAclOp.getResult();

      successSet = true;
      countAggregate++;
    } finally {
      latencyTracker.recordClientLatency(startAggregate, startSet, "removeAcl", "getAclStatus", successGet, resultGet);
      latencyTracker.recordClientLatency(startSet, "removeAcl", "setAcl", successSet, startAggregate, countAggregate, resultSet);
    }
  }

  public void setAcl(final Path path, final List<AclEntry> aclSpec) throws AzureBlobFileSystemException {
    final Instant startAggregate = latencyTracker.getLatencyInstant();
    long countAggregate = 0;
    Instant startSet = null;

    boolean successGet = false;
    boolean successSet = false;
    AbfsHttpOperation resultGet = null;
    AbfsHttpOperation resultSet = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "setAcl filesystem: {} path: {} aclspec: {}",
              client.getFileSystem(),
              path.toString(),
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      final boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(aclEntries);

      final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), isUpnFormat);
      resultGet = op.getResult();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> getAclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.setAclEntriesInternal(aclEntries, getAclEntries);

      startSet = latencyTracker.getLatencyInstant();
      successGet = true;
      countAggregate++;

      final AbfsRestOperation setAclOp = client.setAcl(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
      AbfsAclHelper.serializeAclSpec(aclEntries), eTag);
      resultSet = setAclOp.getResult();

      successSet = true;
      countAggregate++;
    } finally {
      latencyTracker.recordClientLatency(startAggregate, startSet, "setAcl", "getAclStatus", successGet, resultGet);
      latencyTracker.recordClientLatency(startSet, "setAcl", "setAcl", successSet, startAggregate, countAggregate, resultSet);
    }
  }

  public AclStatus getAclStatus(final Path path) throws IOException {
    final Instant start = latencyTracker.getLatencyInstant();
    boolean success = false;
    AbfsHttpOperation result = null;

    try {
      if (!getIsNamespaceEnabled()) {
        throw new UnsupportedOperationException(
                "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
      }

      LOG.debug(
              "getAclStatus filesystem: {} path: {}",
              client.getFileSystem(),
              path.toString());
      AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true));
      result = op.getResult();

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);
      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      final String permissions = result.getResponseHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS);
      final String aclSpecString = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL);

      final List<AclEntry> aclEntries = AclEntry.parseAclSpec(AbfsAclHelper.processAclString(aclSpecString), true);
      identityTransformer.transformAclEntriesForGetRequest(aclEntries, userName, primaryUserGroup);
      final FsPermission fsPermission = permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
              : AbfsPermission.valueOf(permissions);

      final AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
      aclStatusBuilder.owner(transformedOwner);
      aclStatusBuilder.group(transformedGroup);

      aclStatusBuilder.setPermission(fsPermission);
      aclStatusBuilder.stickyBit(fsPermission.getStickyBit());
      aclStatusBuilder.addEntries(aclEntries);
      success = true;
      return aclStatusBuilder.build();
    } finally {
      latencyTracker.recordClientLatency(start, "getAclStatus", "getAclStatus", success, result);
    }
  }

  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
  }

  private void initializeClient(URI uri, String fileSystemName, String accountName, boolean isSecure)
      throws IOException {
    if (this.client != null) {
      return;
    }

    final URIBuilder uriBuilder = getURIBuilder(accountName, isSecure);

    final String url = uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH + fileSystemName;

    URL baseUrl;
    try {
      baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(uri.toString());
    }

    SharedKeyCredentials creds = null;
    AccessTokenProvider tokenProvider = null;

    if (abfsConfiguration.getAuthType(accountName) == AuthType.SharedKey) {
      int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
      if (dotIndex <= 0) {
        throw new InvalidUriException(
                uri.toString() + " - account name is not fully qualified.");
      }
      creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
            abfsConfiguration.getStorageAccountKey());
    } else {
      tokenProvider = abfsConfiguration.getTokenProvider();
      ExtensionHelper.bind(tokenProvider, uri,
            abfsConfiguration.getRawConfiguration());
    }

    this.client =  new AbfsClient(baseUrl, creds, abfsConfiguration, new ExponentialRetryPolicy(), tokenProvider, latencyTracker);
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    return getRelativePath(path, false);
  }

  private String getRelativePath(final Path path, final boolean allowRootPath) {
    Preconditions.checkNotNull(path, "path");
    final String relativePath = path.toUri().getPath();

    if (relativePath.length() == 0 || (relativePath.length() == 1 && relativePath.charAt(0) == Path.SEPARATOR_CHAR)) {
      return allowRootPath ? AbfsHttpConstants.ROOT_PATH : AbfsHttpConstants.EMPTY_STRING;
    }

    if (relativePath.charAt(0) == Path.SEPARATOR_CHAR) {
      return relativePath.substring(1);
    }

    return relativePath;
  }

  private long parseContentLength(final String contentLength) {
    if (contentLength == null) {
      return -1;
    }

    return Long.parseLong(contentLength);
  }

  private boolean parseIsDirectory(final String resourceType) {
    return resourceType != null
        && resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
  }

  private long parseLastModifiedTime(final String lastModifiedTime) {
    long parsedTime = 0;
    try {
      Date utcDate = new SimpleDateFormat(DATE_TIME_PATTERN, Locale.US).parse(lastModifiedTime);
      parsedTime = utcDate.getTime();
    } catch (ParseException e) {
      LOG.error("Failed to parse the date {}", lastModifiedTime);
    } finally {
      return parsedTime;
    }
  }

  private String convertXmsPropertiesToCommaSeparatedString(final Hashtable<String, String> properties) throws
          CharacterCodingException {
    StringBuilder commaSeparatedProperties = new StringBuilder();

    final CharsetEncoder encoder = Charset.forName(XMS_PROPERTIES_ENCODING).newEncoder();

    for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
      String key = propertyEntry.getKey();
      String value = propertyEntry.getValue();

      Boolean canEncodeValue = encoder.canEncode(value);
      if (!canEncodeValue) {
        throw new CharacterCodingException();
      }

      String encodedPropertyValue = Base64.encode(encoder.encode(CharBuffer.wrap(value)).array());
      commaSeparatedProperties.append(key)
              .append(AbfsHttpConstants.EQUAL)
              .append(encodedPropertyValue);

      commaSeparatedProperties.append(AbfsHttpConstants.COMMA);
    }

    if (commaSeparatedProperties.length() != 0) {
      commaSeparatedProperties.deleteCharAt(commaSeparatedProperties.length() - 1);
    }

    return commaSeparatedProperties.toString();
  }

  private Hashtable<String, String> parseCommaSeparatedXmsProperties(String xMsProperties) throws
          InvalidFileSystemPropertyException, InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();

    final CharsetDecoder decoder = Charset.forName(XMS_PROPERTIES_ENCODING).newDecoder();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(AbfsHttpConstants.COMMA);

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split(AbfsHttpConstants.EQUAL, 2);
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        byte[] decodedValue = Base64.decode(nameValue[1]);

        final String value;
        try {
          value = decoder.decode(ByteBuffer.wrap(decodedValue)).toString();
        } catch (CharacterCodingException ex) {
          throw new InvalidAbfsRestOperationException(ex);
        }
        properties.put(nameValue[0], value);
      }
    }

    return properties;
  }

  private boolean isKeyForDirectorySet(String key, Set<String> dirSet) {
    for (String dir : dirSet) {
      if (dir.isEmpty() || key.startsWith(dir + AbfsHttpConstants.FORWARD_SLASH)) {
        return true;
      }

      try {
        URI uri = new URI(dir);
        if (null == uri.getAuthority()) {
          if (key.startsWith(dir + "/")){
            return true;
          }
        }
      } catch (URISyntaxException e) {
        LOG.info("URI syntax error creating URI for {}", dir);
      }
    }

    return false;
  }

  private static class VersionedFileStatus extends FileStatus {
    private final String version;

    VersionedFileStatus(
            final String owner, final String group, final FsPermission fsPermission, final boolean hasAcl,
            final long length, final boolean isdir, final int blockReplication,
            final long blocksize, final long modificationTime, final Path path,
            String version) {
      super(length, isdir, blockReplication, blocksize, modificationTime, 0,
              fsPermission,
              owner,
              group,
              null,
              path,
              hasAcl, false, false);

      this.version = version;
    }

    /** Compare if this object is equal to another object.
     * @param   obj the object to be compared.
     * @return  true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof FileStatus)) {
        return false;
      }

      FileStatus other = (FileStatus) obj;

      if (!this.getPath().equals(other.getPath())) {// compare the path
        return false;
      }

      if (other instanceof VersionedFileStatus) {
        return this.version.equals(((VersionedFileStatus) other).version);
      }

      return true;
    }

    /**
     * Returns a hash code value for the object, which is defined as
     * the hash code of the path name.
     *
     * @return  a hash code value for the path name and version
     */
    @Override
    public int hashCode() {
      int hash = getPath().hashCode();
      hash = 89 * hash + (this.version != null ? this.version.hashCode() : 0);
      return hash;
    }

    /**
     * Returns the version of this FileStatus
     *
     * @return  a string value for the FileStatus version
     */
    public String getVersion() {
      return this.version;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "VersionedFileStatus{");
      sb.append(super.toString());
      sb.append("; version='").append(version).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  @VisibleForTesting
  AbfsClient getClient() {
    return this.client;
  }
}
