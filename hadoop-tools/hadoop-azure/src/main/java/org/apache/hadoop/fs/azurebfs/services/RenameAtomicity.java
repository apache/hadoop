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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobAppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_ID_LENGTH;
import static org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient.generateBlockListXml;

/**
 * For a directory enabled for atomic-rename, before rename starts, a file with
 * -RenamePending.json suffix is created. In this file, the states required for the
 * rename operation are given. This file is created by {@link #preRename()} method.
 * This is important in case the JVM process crashes during rename, the atomicity
 * will be maintained, when the job calls {@link AzureBlobFileSystem#listStatus(Path)}
 * or {@link AzureBlobFileSystem#getFileStatus(Path)}. On these API calls to filesystem,
 * it will be checked if there is any RenamePending JSON file. If yes, the crashed rename
 * operation would be resumed as per the file.
 */
public class RenameAtomicity {

  private final TracingContext tracingContext;

  private Path src, dst;

  private String srcEtag;

  private final AbfsBlobClient abfsClient;

  private final Path renameJsonPath;

  public static final String SUFFIX = "-RenamePending.json";

  private int preRenameRetryCount = 0;

  private int renamePendingJsonLen;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final Random RANDOM = new Random();

  /**
   * Performs pre-rename operations. Creates a file with -RenamePending.json
   * suffix in the source parent directory. This file contains the states
   * required for the rename operation.
   *
   * @param src Source path
   * @param dst Destination path
   * @param renameJsonPath Path of the JSON file to be created
   * @param tracingContext Tracing context
   * @param srcEtag ETag of the source directory
   * @param abfsClient AbfsClient instance
   */
  public RenameAtomicity(final Path src, final Path dst,
      final Path renameJsonPath,
      TracingContext tracingContext,
      final String srcEtag,
      final AbfsClient abfsClient) {
    this.src = src;
    this.dst = dst;
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
  }

  /**
   * Resumes the rename operation from the JSON file.
   *
   * @param renameJsonPath Path of the JSON file
   * @param renamePendingJsonFileLen Length of the JSON file
   * @param tracingContext Tracing context
   * @param srcEtag ETag of the source directory
   * @param abfsClient AbfsClient instance
   */
  public RenameAtomicity(final Path renameJsonPath,
      final int renamePendingJsonFileLen,
      TracingContext tracingContext,
      final String srcEtag,
      final AbfsClient abfsClient) {
    this.abfsClient = (AbfsBlobClient) abfsClient;
    this.renameJsonPath = renameJsonPath;
    this.tracingContext = tracingContext;
    this.srcEtag = srcEtag;
    this.renamePendingJsonLen = renamePendingJsonFileLen;
  }

  /**
   * Redo the rename operation from the JSON file.
   *
   * @throws AzureBlobFileSystemException If the redo operation fails.
   */
  public void redo() throws AzureBlobFileSystemException {
    byte[] buffer = readRenamePendingJson(renameJsonPath, renamePendingJsonLen);
    String contents = new String(buffer, Charset.defaultCharset());
    try {
      final RenamePendingJsonFormat renamePendingJsonFormatObj;
      try {
        renamePendingJsonFormatObj = objectMapper.readValue(contents,
            RenamePendingJsonFormat.class);
      } catch (JsonProcessingException e) {
        return;
      }
      if (renamePendingJsonFormatObj != null && StringUtils.isNotEmpty(
          renamePendingJsonFormatObj.getOldFolderName())
          && StringUtils.isNotEmpty(
          renamePendingJsonFormatObj.getNewFolderName())
          && StringUtils.isNotEmpty(renamePendingJsonFormatObj.getETag())) {
        this.src = new Path(renamePendingJsonFormatObj.getOldFolderName());
        this.dst = new Path(renamePendingJsonFormatObj.getNewFolderName());
        this.srcEtag = renamePendingJsonFormatObj.getETag();

        BlobRenameHandler blobRenameHandler = new BlobRenameHandler(
            this.src.toUri().getPath(), dst.toUri().getPath(),
            abfsClient, srcEtag, true,
            true, tracingContext);

        blobRenameHandler.execute();
      }
    } finally {
      deleteRenamePendingJson();
    }
  }

  /** Read the JSON file.
   *
   * @param path Path of the JSON file
   * @param len Length of the JSON file
   * @return Contents of the JSON file
   * @throws AzureBlobFileSystemException If the read operation fails.
   */
  @VisibleForTesting
  byte[] readRenamePendingJson(Path path, int len)
      throws AzureBlobFileSystemException {
    byte[] bytes = new byte[len];
    abfsClient.read(path.toUri().getPath(), 0, bytes, 0,
        len, null, null, null,
        tracingContext);
    return bytes;
  }

  /** Generate a random block ID.
   *
   * @return Random block ID
   */
  public static String generateBlockId() {
    // PutBlock on the path.
    byte[] blockIdByteArray = new byte[BLOCK_ID_LENGTH];
    RANDOM.nextBytes(blockIdByteArray);
    return new String(Base64.encodeBase64(blockIdByteArray),
        StandardCharsets.UTF_8);
  }

  /** Create the JSON file with the contents.
   *
   * @param path Path of the JSON file
   * @param bytes Contents of the JSON file
   * @throws AzureBlobFileSystemException If the create operation fails.
   */
  @VisibleForTesting
  void createRenamePendingJson(Path path, byte[] bytes)
      throws AzureBlobFileSystemException {
    // PutBlob on the path.
    AbfsRestOperation putBlobOp = abfsClient.createPath(path.toUri().getPath(),
        true,
        true,
        null,
        false,
        null,
        null,
        tracingContext);
    String eTag = extractEtagHeader(putBlobOp.getResult());

    String blockId = generateBlockId();
    AppendRequestParameters appendRequestParameters
        = new AppendRequestParameters(0, 0,
        bytes.length, AppendRequestParameters.Mode.APPEND_MODE, false, null,
        abfsClient.getAbfsConfiguration().isExpectHeaderEnabled(),
        new BlobAppendRequestParameters(blockId, eTag));

    abfsClient.append(path.toUri().getPath(), bytes,
        appendRequestParameters, null, null, tracingContext);

    String blockList = generateBlockListXml(blockId);
    // PutBlockList on the path.
    abfsClient.flush(blockList.getBytes(StandardCharsets.UTF_8),
        path.toUri().getPath(), true, null, null, eTag, null, tracingContext);
  }

  /**
   * Before starting the atomic rename, create a file with -RenamePending.json
   * suffix in the source parent directory. This file contains the states
   * required source, destination, and source-eTag for the rename operation.
   * <p>
   * If the path that is getting renamed is a /sourcePath, then the JSON file
   * will be /sourcePath-RenamePending.json.
   *
   * @return Length of the JSON file.
   * @throws AzureBlobFileSystemException If the pre-rename operation fails.
   */
  @VisibleForTesting
  public int preRename() throws AzureBlobFileSystemException {
    String makeRenamePendingFileContents = makeRenamePendingFileContents(
        srcEtag);

    try {
      createRenamePendingJson(renameJsonPath,
          makeRenamePendingFileContents.getBytes(StandardCharsets.UTF_8));
      return makeRenamePendingFileContents.length();
    } catch (AzureBlobFileSystemException e) {
      /*
       * Scenario: file has been deleted by parallel thread before the RenameJSON
       * could be written and flushed. In such case, there has to be one retry of
       * preRename.
       * ref: https://issues.apache.org/jira/browse/HADOOP-12678
       * On DFS endpoint, flush API is called. If file is not there, server returns
       * 404.
       * On blob endpoint, flush API is not there. PutBlockList is called with
       * if-match header. If file is not there, the conditional header will fail,
       * the server will return 412.
       */
      if (isPreRenameRetriableException(e)) {
        preRenameRetryCount++;
        if (preRenameRetryCount == 1) {
          return preRename();
        }
      }
      throw e;
    }
  }

  /** Check if the exception is retryable for pre-rename operation.
   *
   * @param e Exception to be checked
   * @return true if the exception is retryable, false otherwise
   */
  private boolean isPreRenameRetriableException(IOException e) {
    AbfsRestOperationException ex;
    while (e != null) {
      if (e instanceof AbfsRestOperationException) {
        ex = (AbfsRestOperationException) e;
        return ex.getStatusCode() == HTTP_NOT_FOUND
            || ex.getStatusCode() == HTTP_PRECON_FAILED;
      }
      e = (IOException) e.getCause();
    }
    return false;
  }

  /** Delete the JSON file after rename is done.
   * @throws AzureBlobFileSystemException If the delete operation fails.
   */
  public void postRename() throws AzureBlobFileSystemException {
    deleteRenamePendingJson();
  }

  /** Delete the JSON file.
   *
   * @throws AzureBlobFileSystemException If the delete operation fails.
   */
  private void deleteRenamePendingJson() throws AzureBlobFileSystemException {
    try {
      abfsClient.deleteBlobPath(renameJsonPath, null,
          tracingContext);
    } catch (AzureBlobFileSystemException e) {
      if (e instanceof AbfsRestOperationException
          && ((AbfsRestOperationException) e).getStatusCode()
          == HTTP_NOT_FOUND) {
        return;
      }
      throw e;
    }
  }

  /**
   * Return the contents of the JSON file to represent the operations
   * to be performed for a folder rename.
   *
   * @return JSON string which represents the operation.
   */
  private String makeRenamePendingFileContents(String eTag) throws
      AzureBlobFileSystemException {

    final RenamePendingJsonFormat renamePendingJsonFormat
        = new RenamePendingJsonFormat();
    renamePendingJsonFormat.setOldFolderName(src.toUri().getPath());
    renamePendingJsonFormat.setNewFolderName(dst.toUri().getPath());
    renamePendingJsonFormat.setETag(eTag);
    try {
      return objectMapper.writeValueAsString(renamePendingJsonFormat);
    } catch (JsonProcessingException e) {
      throw new AbfsDriverException(e);
    }
  }
}
