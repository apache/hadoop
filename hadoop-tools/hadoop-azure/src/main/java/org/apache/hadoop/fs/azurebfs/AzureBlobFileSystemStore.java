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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.utils.URIBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.util.Time.now;

/**
 * Provides the bridging logic between Hadoop's abstract filesystem and Azure Storage
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureBlobFileSystemStore {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystemStore.class);

  private AbfsClient client;
  private URI uri;
  private final UserGroupInformation userGroupInformation;
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss 'GMT'";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int LIST_MAX_RESULTS = 5000;
  private static final int DELETE_DIRECTORY_TIMEOUT_MILISECONDS = 180000;
  private static final int RENAME_TIMEOUT_MILISECONDS = 180000;

  private final AbfsConfiguration abfsConfiguration;
  private final Set<String> azureAtomicRenameDirSet;


  public AzureBlobFileSystemStore(URI uri, boolean isSeure, Configuration configuration, UserGroupInformation userGroupInformation)
          throws AzureBlobFileSystemException {
    this.uri = uri;
    try {
      this.abfsConfiguration = new AbfsConfiguration(configuration);
    } catch (IllegalAccessException exception) {
      throw new FileSystemOperationUnhandledException(exception);
    }

    this.userGroupInformation = userGroupInformation;
    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(abfsConfiguration.getAzureAtomicRenameDirs().split(AbfsHttpConstants.COMMA)));

    initializeClient(uri, isSeure);
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, boolean isSecure) {
    String scheme = isSecure ? FileSystemUriSchemes.HTTPS_SCHEME : FileSystemUriSchemes.HTTP_SCHEME;

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);
    uriBuilder.setHost(hostName);

    return uriBuilder;
  }

  public AbfsConfiguration getAbfsConfiguration() {
    return this.abfsConfiguration;
  }

  public Hashtable<String, String> getFilesystemProperties() throws AzureBlobFileSystemException {
    this.LOG.debug(
            "getFilesystemProperties for filesystem: {}",
            client.getFileSystem());

    final Hashtable<String, String> parsedXmsProperties;

    final AbfsRestOperation op = client.getFilesystemProperties();
    final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

    parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);

    return parsedXmsProperties;
  }

  public void setFilesystemProperties(final Hashtable<String, String> properties) throws AzureBlobFileSystemException {
    if (properties == null || properties.size() == 0) {
      return;
    }

    this.LOG.debug(
            "setFilesystemProperties for filesystem: {} with properties: {}",
            client.getFileSystem(),
            properties);

    final String commaSeparatedProperties;
    try {
      commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
    } catch (CharacterCodingException ex) {
      throw new InvalidAbfsRestOperationException(ex);
    }

    client.setFilesystemProperties(commaSeparatedProperties);
  }

  public Hashtable<String, String> getPathProperties(final Path path) throws AzureBlobFileSystemException {
    this.LOG.debug(
            "getPathProperties for filesystem: {} path: {}",
            client.getFileSystem(),
            path.toString());

    final Hashtable<String, String> parsedXmsProperties;
    final AbfsRestOperation op = client.getPathProperties(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));

    final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

    parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);

    return parsedXmsProperties;
  }

  public void setPathProperties(final Path path, final Hashtable<String, String> properties) throws AzureBlobFileSystemException {
    this.LOG.debug(
            "setFilesystemProperties for filesystem: {} path: {} with properties: {}",
            client.getFileSystem(),
            path.toString(),
            properties);

    final String commaSeparatedProperties;
    try {
      commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
    } catch (CharacterCodingException ex) {
      throw new InvalidAbfsRestOperationException(ex);
    }
    client.setPathProperties("/" + getRelativePath(path), commaSeparatedProperties);
  }

  public void createFilesystem() throws AzureBlobFileSystemException {
    this.LOG.debug(
            "createFilesystem for filesystem: {}",
            client.getFileSystem());

    client.createFilesystem();
  }

  public void deleteFilesystem() throws AzureBlobFileSystemException {
    this.LOG.debug(
            "deleteFilesystem for filesystem: {}",
            client.getFileSystem());

    client.deleteFilesystem();
  }

  public OutputStream createFile(final Path path, final boolean overwrite) throws AzureBlobFileSystemException {
    this.LOG.debug(
            "createFile filesystem: {} path: {} overwrite: {}",
            client.getFileSystem(),
            path.toString(),
            overwrite);

    client.createPath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), true, overwrite);

    final OutputStream outputStream;
    outputStream = new FSDataOutputStream(
            new AbfsOutputStream(client, AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), 0,
                    abfsConfiguration.getWriteBufferSize()), null);
    return outputStream;
  }

  public Void createDirectory(final Path path) throws AzureBlobFileSystemException {
    this.LOG.debug(
            "createDirectory filesystem: {} path: {} overwrite: {}",
            client.getFileSystem(),
            path.toString());

    client.createPath("/" + getRelativePath(path), false, true);

    return null;
  }

  public InputStream openFileForRead(final Path path, final FileSystem.Statistics statistics) throws AzureBlobFileSystemException {

    this.LOG.debug(
            "openFileForRead filesystem: {} path: {}",
            client.getFileSystem(),
            path.toString());

    final AbfsRestOperation op = client.getPathProperties(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));

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

    // Add statistics for InputStream
    return new FSDataInputStream(
            new AbfsInputStream(client, statistics, AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), contentLength,
                    abfsConfiguration.getReadBufferSize(), abfsConfiguration.getReadAheadQueueDepth(), eTag));
  }

  public OutputStream openFileForWrite(final Path path, final boolean overwrite) throws
          AzureBlobFileSystemException {
    this.LOG.debug(
            "openFileForWrite filesystem: {} path: {} overwrite: {}",
            client.getFileSystem(),
            path.toString(),
            overwrite);

    final AbfsRestOperation op = client.getPathProperties(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));

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

    final OutputStream outputStream;
    outputStream = new FSDataOutputStream(
            new AbfsOutputStream(client, AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path),
                    offset, abfsConfiguration.getWriteBufferSize()), null);
    return outputStream;
  }

  public void rename(final Path source, final Path destination) throws
          AzureBlobFileSystemException {

    if (isAtomicRenameKey(source.getName())) {
      this.LOG.warn("The atomic rename feature is not supported by the ABFS scheme; however rename,"
              +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    this.LOG.debug(
            "renameAsync filesystem: {} source: {} destination: {}",
            client.getFileSystem(),
            source.toString(),
            destination.toString());

    String continuation = null;
    long deadline = now() + RENAME_TIMEOUT_MILISECONDS;

    do {
      if (now() > deadline) {
        LOG.debug(
                "Rename {} to {} timed out.",
                source,
                destination);

        throw new TimeoutException("Rename timed out.");
      }

      AbfsRestOperation op = client.renamePath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(source),
              AbfsHttpConstants.FORWARD_SLASH + getRelativePath(destination), continuation);
      continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);

    } while (continuation != null && !continuation.isEmpty());
  }

  public void delete(final Path path, final boolean recursive) throws
          AzureBlobFileSystemException {

    this.LOG.debug(
            "delete filesystem: {} path: {} recursive: {}",
            client.getFileSystem(),
            path.toString(),
            String.valueOf(recursive));

    String continuation = null;
    long deadline = now() + DELETE_DIRECTORY_TIMEOUT_MILISECONDS;

    do {
      if (now() > deadline) {
        this.LOG.debug(
                "Delete directory {} timed out.", path);

        throw new TimeoutException("Delete directory timed out.");
      }

      AbfsRestOperation op = client.deletePath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), recursive, continuation);
      continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);

    } while (continuation != null && !continuation.isEmpty());
  }

  public FileStatus getFileStatus(final Path path) throws IOException {

    this.LOG.debug(
            "getFileStatus filesystem: {} path: {}",
            client.getFileSystem(),
            path.toString());

    if (path.isRoot()) {
      AbfsRestOperation op = client.getFilesystemProperties();
      final long blockSize = abfsConfiguration.getAzureBlockSize();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
      final String lastModified = op.getResult().getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
      return new VersionedFileStatus(
              userGroupInformation.getUserName(),
              userGroupInformation.getPrimaryGroupName(),
              0,
              true,
              1,
              blockSize,
              parseLastModifiedTime(lastModified).getMillis(),
              path,
              eTag);
    } else {
      AbfsRestOperation op = client.getPathProperties(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));

      final long blockSize = abfsConfiguration.getAzureBlockSize();
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
      final String lastModified = op.getResult().getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
      final String contentLength = op.getResult().getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH);
      final String resourceType = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);

      return new VersionedFileStatus(
              userGroupInformation.getUserName(),
              userGroupInformation.getPrimaryGroupName(),
              parseContentLength(contentLength),
              parseIsDirectory(resourceType),
              1,
              blockSize,
              parseLastModifiedTime(lastModified).getMillis(),
              path,
              eTag);
    }
  }

  public FileStatus[] listStatus(final Path path) throws IOException {
    this.LOG.debug(
            "listStatus filesystem: {} path: {}",
            client.getFileSystem(),
            path.toString());

    String relativePath = path.isRoot() ? AbfsHttpConstants.EMPTY_STRING : getRelativePath(path);
    String continuation = null;
    ArrayList<FileStatus> fileStatuses = new ArrayList<>();

    do {
      AbfsRestOperation op = client.listPath(relativePath, false, LIST_MAX_RESULTS, continuation);
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
        long lastModifiedMillis = 0;
        long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
        boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
        if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
          final DateTime dateTime = DateTime.parse(
                  entry.lastModified(),
                  DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());
          lastModifiedMillis = dateTime.getMillis();
        }

        Path entryPath = new Path(File.separator + entry.name());
        entryPath = entryPath.makeQualified(this.uri, entryPath);

        fileStatuses.add(
                new VersionedFileStatus(
                        userGroupInformation.getUserName(),
                        userGroupInformation.getPrimaryGroupName(),
                        contentLength,
                        isDirectory,
                        1,
                        blockSize,
                        lastModifiedMillis,
                        entryPath,
                        entry.eTag()));
      }

    } while (continuation != null && !continuation.isEmpty());

    return fileStatuses.toArray(new FileStatus[0]);
  }

  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
  }

  private void initializeClient(URI uri, boolean isSeure) throws AzureBlobFileSystemException {
    if (this.client != null) {
      return;
    }

    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      final String errMsg = String
              .format("URI '%s' has a malformed authority, expected container name. "
                              + "Authority takes the form "+ FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
                      uri.toString());
      throw new InvalidUriException(errMsg);
    }

    final String fileSystemName = authorityParts[0];
    final String accountName = authorityParts[1];

    final URIBuilder uriBuilder = getURIBuilder(accountName, isSeure);

    final String url = uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH + fileSystemName;

    URL baseUrl;
    try {
      baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(String.format("URI '%s' is malformed", uri.toString()));
    }

    SharedKeyCredentials creds =
            new SharedKeyCredentials(accountName.substring(0, accountName.indexOf(AbfsHttpConstants.DOT)),
                    this.abfsConfiguration.getStorageAccountKey(accountName));

    this.client =  new AbfsClient(baseUrl, creds, abfsConfiguration, new ExponentialRetryPolicy());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    final String relativePath = path.toUri().getPath();

    if (relativePath.length() == 0) {
      return relativePath;
    }

    if (relativePath.charAt(0) == Path.SEPARATOR_CHAR) {
      if (relativePath.length() == 1) {
        return AbfsHttpConstants.EMPTY_STRING;
      }

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
    return resourceType == null ? false : resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
  }

  private DateTime parseLastModifiedTime(final String lastModifiedTime) {
    return DateTime.parse(
            lastModifiedTime,
            DateTimeFormat.forPattern(DATE_TIME_PATTERN).withZoneUTC());
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

      String encodedPropertyValue = DatatypeConverter.printBase64Binary(encoder.encode(CharBuffer.wrap(value)).array());
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

        byte[] decodedValue = DatatypeConverter.parseBase64Binary(nameValue[1]);

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
        this.LOG.info("URI syntax error creating URI for {}", dir);
      }
    }

    return false;
  }

  private static class VersionedFileStatus extends FileStatus {
    private final String version;

    VersionedFileStatus(
            final String owner, final String group,
            final long length, final boolean isdir, final int blockReplication,
            final long blocksize, final long modificationTime, final Path path,
            String version) {
      super(length, isdir, blockReplication, blocksize, modificationTime, 0,
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL),
              owner,
              group,
              path);

      this.version = version;
    }

    /** Compare if this object is equal to another object.
     * @param   obj the object to be compared.
     * @return  true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (obj == null) {
        return false;
      }

      if (this.getClass() == obj.getClass()) {
        VersionedFileStatus other = (VersionedFileStatus) obj;
        return this.getPath().equals(other.getPath()) && this.version.equals(other.version);
      }

      return false;
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
  }


}
