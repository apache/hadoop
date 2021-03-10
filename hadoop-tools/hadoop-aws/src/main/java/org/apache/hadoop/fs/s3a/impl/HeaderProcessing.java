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

package org.apache.hadoop.fs.s3a.impl;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;

import static org.apache.hadoop.fs.s3a.Constants.XA_HEADER_PREFIX;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_OP_XATTR_LIST;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_XATTR_GET_MAP;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_XATTR_GET_NAMED;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_XATTR_GET_NAMED_MAP;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.X_HEADER_MAGIC_MARKER;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

/**
 * Part of the S3A FS where object headers are
 * processed.
 * Implements all the various XAttr read operations.
 * Those APIs all expect byte arrays back.
 * Metadata cloning is also implemented here, so as
 * to stay in sync with custom header logic.
 *
 * The standard header names are extracted from the AWS SDK.
 * The S3A connector does not (currently) support setting them,
 * though it would be possible to do so through the createFile()
 * builder API.
 */
public class HeaderProcessing extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      HeaderProcessing.class);

  /**
   * An empty buffer.
   */
  private static final byte[] EMPTY = new byte[0];


  /**
   * Standard HTTP header found on some S3 objects: {@value}.
   */
  public static final String XA_CACHE_CONTROL =
      XA_HEADER_PREFIX + Headers.CACHE_CONTROL;
  /**
   * Standard HTTP header found on some S3 objects: {@value}.
   */
  public static final String XA_CONTENT_DISPOSITION =
      XA_HEADER_PREFIX + Headers.CONTENT_DISPOSITION;

  /**
   * Standard HTTP header found on some S3 objects: {@value}.
   */
  public static final String XA_CONTENT_ENCODING =
      XA_HEADER_PREFIX + Headers.CONTENT_ENCODING;

  /**
   * Standard HTTP header found on some S3 objects: {@value}.
   */
  public static final String XA_CONTENT_LANGUAGE =
      XA_HEADER_PREFIX + Headers.CONTENT_LANGUAGE;

  /**
   * Length XAttr: {@value}.
   */
  public static final String XA_CONTENT_LENGTH =
      XA_HEADER_PREFIX + Headers.CONTENT_LENGTH;

  /**
   * Standard HTTP header found on some S3 objects: {@value}.
   */
  public static final String XA_CONTENT_MD5 =
      XA_HEADER_PREFIX + Headers.CONTENT_MD5;

  /**
   * Content range: {@value}.
   * This is returned on GET requests with ranges.
   */
  public static final String XA_CONTENT_RANGE =
      XA_HEADER_PREFIX + Headers.CONTENT_RANGE;

  /**
   * Content type: may be set when uploading.
   * {@value}.
   */
  public static final String XA_CONTENT_TYPE =
      XA_HEADER_PREFIX + Headers.CONTENT_TYPE;

  /**
   * Etag Header {@value}.
   * Also accessible via {@code ObjectMetadata.getEtag()}, where
   * it can be retrieved via {@code getFileChecksum(path)} if
   * the S3A connector is enabled.
   */
  public static final String XA_ETAG = XA_HEADER_PREFIX + Headers.ETAG;


  /**
   * last modified XAttr: {@value}.
   */
  public static final String XA_LAST_MODIFIED =
      XA_HEADER_PREFIX + Headers.LAST_MODIFIED;

  /* AWS Specific Headers. May not be found on other S3 endpoints. */

  /**
   * object archive status; empty if not on S3 Glacier
   * (i.e all normal files should be non-archived as
   * S3A and applications don't handle archived data)
   * Value {@value}.
   */
  public static final String XA_ARCHIVE_STATUS =
      XA_HEADER_PREFIX + Headers.ARCHIVE_STATUS;

  /**
   *  Object legal hold status. {@value}.
   */
  public static final String XA_OBJECT_LOCK_LEGAL_HOLD_STATUS =
      XA_HEADER_PREFIX + Headers.OBJECT_LOCK_LEGAL_HOLD_STATUS;

  /**
   *  Object lock mode. {@value}.
   */
  public static final String XA_OBJECT_LOCK_MODE =
      XA_HEADER_PREFIX + Headers.OBJECT_LOCK_MODE;

  /**
   *  ISO8601 expiry date of object lock hold. {@value}.
   */
  public static final String XA_OBJECT_LOCK_RETAIN_UNTIL_DATE =
      XA_HEADER_PREFIX + Headers.OBJECT_LOCK_RETAIN_UNTIL_DATE;

  /**
   *  Replication status for cross-region replicated objects. {@value}.
   */
  public static final String XA_OBJECT_REPLICATION_STATUS =
      XA_HEADER_PREFIX + Headers.OBJECT_REPLICATION_STATUS;

  /**
   *  Version ID; empty for non-versioned buckets/data. {@value}.
   */
  public static final String XA_S3_VERSION_ID =
      XA_HEADER_PREFIX + Headers.S3_VERSION_ID;

  /**
   * The server-side encryption algorithm to use
   * with AWS-managed keys: {@value}.
   */
  public static final String XA_SERVER_SIDE_ENCRYPTION =
      XA_HEADER_PREFIX + Headers.SERVER_SIDE_ENCRYPTION;

  /**
   * Storage Class XAttr: {@value}.
   */
  public static final String XA_STORAGE_CLASS =
      XA_HEADER_PREFIX + Headers.STORAGE_CLASS;

  /**
   * Standard headers which are retrieved from HEAD Requests
   * and set as XAttrs if the response included the relevant header.
   */
  public static final String[] XA_STANDARD_HEADERS = {
      /* HTTP standard headers */
      XA_CACHE_CONTROL,
      XA_CONTENT_DISPOSITION,
      XA_CONTENT_ENCODING,
      XA_CONTENT_LANGUAGE,
      XA_CONTENT_LENGTH,
      XA_CONTENT_MD5,
      XA_CONTENT_RANGE,
      XA_CONTENT_TYPE,
      XA_ETAG,
      XA_LAST_MODIFIED,
      /* aws headers */
      XA_ARCHIVE_STATUS,
      XA_OBJECT_LOCK_LEGAL_HOLD_STATUS,
      XA_OBJECT_LOCK_MODE,
      XA_OBJECT_LOCK_RETAIN_UNTIL_DATE,
      XA_OBJECT_REPLICATION_STATUS,
      XA_S3_VERSION_ID,
      XA_SERVER_SIDE_ENCRYPTION,
      XA_STORAGE_CLASS,
  };

  /**
   * Content type of generic binary objects.
   * This is the default for uploaded objects.
   */
  public static final String CONTENT_TYPE_OCTET_STREAM =
      "application/octet-stream";

  /**
   * XML content type : {@value}.
   * This is application/xml, not text/xml, and is
   * what a HEAD of / returns as the type of a root path.
   */
  public static final String CONTENT_TYPE_APPLICATION_XML =
      "application/xml";

  /**
   * Construct.
   * @param storeContext store context.
   */
  public HeaderProcessing(final StoreContext storeContext) {
    super(storeContext);
  }

  /**
   * Query the store, get all the headers into a map. Each Header
   * has the "header." prefix.
   * Caller must have read access.
   * The value of each header is the string value of the object
   * UTF-8 encoded.
   * @param path path of object.
   * @param statistic statistic to use for duration tracking.
   * @return the headers
   * @throws IOException failure, including file not found.
   */
  private Map<String, byte[]> retrieveHeaders(
      final Path path,
      final Statistic statistic) throws IOException {
    StoreContext context = getStoreContext();
    ContextAccessors accessors = context.getContextAccessors();
    String objectKey = accessors.pathToKey(path);
    ObjectMetadata md;
    String symbol = statistic.getSymbol();
    S3AStatisticsContext instrumentation = context.getInstrumentation();
    try {
      md = trackDuration(instrumentation, symbol, () ->
              accessors.getObjectMetadata(objectKey));
    } catch (FileNotFoundException e) {
      // no entry. It could be a directory, so try again.
      md = trackDuration(instrumentation, symbol, () ->
          accessors.getObjectMetadata(objectKey + "/"));
    }
    // all user metadata
    Map<String, String> rawHeaders = md.getUserMetadata();
    Map<String, byte[]> headers = new TreeMap<>();
    rawHeaders.forEach((key, value) ->
        headers.put(XA_HEADER_PREFIX + key, encodeBytes(value)));

    // and add the usual content length &c, if set
    maybeSetHeader(headers, XA_CACHE_CONTROL,
        md.getCacheControl());
    maybeSetHeader(headers, XA_CONTENT_DISPOSITION,
        md.getContentDisposition());
    maybeSetHeader(headers, XA_CONTENT_ENCODING,
        md.getContentEncoding());
    maybeSetHeader(headers, XA_CONTENT_LANGUAGE,
        md.getContentLanguage());
    maybeSetHeader(headers, XA_CONTENT_LENGTH,
        md.getContentLength());
    maybeSetHeader(headers, XA_CONTENT_MD5,
        md.getContentMD5());
    maybeSetHeader(headers, XA_CONTENT_RANGE,
        md.getContentRange());
    maybeSetHeader(headers, XA_CONTENT_TYPE,
        md.getContentType());
    maybeSetHeader(headers, XA_ETAG,
        md.getETag());
    maybeSetHeader(headers, XA_LAST_MODIFIED,
        md.getLastModified());

    // AWS custom headers
    maybeSetHeader(headers, XA_ARCHIVE_STATUS,
        md.getArchiveStatus());
    maybeSetHeader(headers, XA_OBJECT_LOCK_LEGAL_HOLD_STATUS,
        md.getObjectLockLegalHoldStatus());
    maybeSetHeader(headers, XA_OBJECT_LOCK_MODE,
        md.getObjectLockMode());
    maybeSetHeader(headers, XA_OBJECT_LOCK_RETAIN_UNTIL_DATE,
        md.getObjectLockRetainUntilDate());
    maybeSetHeader(headers, XA_OBJECT_REPLICATION_STATUS,
        md.getReplicationStatus());
    maybeSetHeader(headers, XA_S3_VERSION_ID,
        md.getVersionId());
    maybeSetHeader(headers, XA_SERVER_SIDE_ENCRYPTION,
        md.getSSEAlgorithm());
    maybeSetHeader(headers, XA_STORAGE_CLASS,
        md.getStorageClass());
    maybeSetHeader(headers, XA_STORAGE_CLASS,
        md.getReplicationStatus());
    return headers;
  }

  /**
   * Set a header if the value is non null.
   *
   * @param headers header map
   * @param name header name
   * @param value value to encode.
   */
  private void maybeSetHeader(
      final Map<String, byte[]> headers,
      final String name,
      final Object value) {
    if (value != null) {
      headers.put(name, encodeBytes(value));
    }
  }

  /**
   * Stringify an object and return its bytes in UTF-8 encoding.
   * @param s source
   * @return encoded object or an empty buffer
   */
  public static byte[] encodeBytes(@Nullable Object s) {
    return s == null
        ? EMPTY
        : s.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Get the string value from the bytes.
   * if null : return null, otherwise the UTF-8 decoded
   * bytes.
   * @param bytes source bytes
   * @return decoded value
   */
  public static String decodeBytes(byte[] bytes) {
    return bytes == null
        ? null
        : new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Get an XAttr name and value for a file or directory.
   * @param path Path to get extended attribute
   * @param name XAttr name.
   * @return byte[] XAttr value or null
   * @throws IOException IO failure
   */
  public byte[] getXAttr(Path path, String name) throws IOException {
    return retrieveHeaders(path, INVOCATION_XATTR_GET_NAMED).get(name);
  }

  /**
   * See {@code FileSystem.getXAttrs(path}.
   *
   * @param path Path to get extended attributes
   * @return Map describing the XAttrs of the file or directory
   * @throws IOException IO failure
   */
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return retrieveHeaders(path, INVOCATION_XATTR_GET_MAP);
  }

  /**
   * See {@code FileSystem.listXAttrs(path)}.
   * @param path Path to get extended attributes
   * @return List of supported XAttrs
   * @throws IOException IO failure
   */
  public List<String> listXAttrs(final Path path) throws IOException {
    return new ArrayList<>(retrieveHeaders(path, INVOCATION_OP_XATTR_LIST)
        .keySet());
  }

  /**
   * See {@code FileSystem.getXAttrs(path, names}.
   * @param path Path to get extended attributes
   * @param names XAttr names.
   * @return Map describing the XAttrs of the file or directory
   * @throws IOException IO failure
   */
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    Map<String, byte[]> headers = retrieveHeaders(path,
        INVOCATION_XATTR_GET_NAMED_MAP);
    Map<String, byte[]> result = new TreeMap<>();
    headers.entrySet().stream()
        .filter(entry -> names.contains(entry.getKey()))
        .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
    return result;
  }

  /**
   * Convert an XAttr byte array to a long.
   * testability.
   * @param data data to parse
   * @return either a length or none
   */
  public static Optional<Long> extractXAttrLongValue(byte[] data) {
    String xAttr;
    xAttr = HeaderProcessing.decodeBytes(data);
    if (StringUtils.isNotEmpty(xAttr)) {
      try {
        long l = Long.parseLong(xAttr);
        if (l >= 0) {
          return Optional.of(l);
        }
      } catch (NumberFormatException ex) {
        LOG.warn("Not a number: {}", xAttr, ex);
      }
    }
    // missing/empty header or parse failure.
    return Optional.empty();
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}.
   * Does so without using the {@link ObjectMetadata#clone()} method,
   * to avoid copying unnecessary headers.
   * This operation does not copy the {@code X_HEADER_MAGIC_MARKER}
   * header to avoid confusion. If a marker file is renamed,
   * it loses information about any remapped file.
   * If new fields are added to ObjectMetadata which are not
   * present in the user metadata headers, they will not be picked
   * up or cloned unless this operation is updated.
   * @param source the {@link ObjectMetadata} to copy
   * @param dest the metadata to update; this is the return value.
   */
  public void cloneObjectMetadata(ObjectMetadata source,
      ObjectMetadata dest) {

    // Possibly null attributes
    // Allowing nulls to pass breaks it during later use
    if (source.getCacheControl() != null) {
      dest.setCacheControl(source.getCacheControl());
    }
    if (source.getContentDisposition() != null) {
      dest.setContentDisposition(source.getContentDisposition());
    }
    if (source.getContentEncoding() != null) {
      dest.setContentEncoding(source.getContentEncoding());
    }
    if (source.getContentMD5() != null) {
      dest.setContentMD5(source.getContentMD5());
    }
    if (source.getContentType() != null) {
      dest.setContentType(source.getContentType());
    }
    if (source.getExpirationTime() != null) {
      dest.setExpirationTime(source.getExpirationTime());
    }
    if (source.getExpirationTimeRuleId() != null) {
      dest.setExpirationTimeRuleId(source.getExpirationTimeRuleId());
    }
    if (source.getHttpExpiresDate() != null) {
      dest.setHttpExpiresDate(source.getHttpExpiresDate());
    }
    if (source.getLastModified() != null) {
      dest.setLastModified(source.getLastModified());
    }
    if (source.getOngoingRestore() != null) {
      dest.setOngoingRestore(source.getOngoingRestore());
    }
    if (source.getRestoreExpirationTime() != null) {
      dest.setRestoreExpirationTime(source.getRestoreExpirationTime());
    }
    if (source.getSSEAlgorithm() != null) {
      dest.setSSEAlgorithm(source.getSSEAlgorithm());
    }
    if (source.getSSECustomerAlgorithm() != null) {
      dest.setSSECustomerAlgorithm(source.getSSECustomerAlgorithm());
    }
    if (source.getSSECustomerKeyMd5() != null) {
      dest.setSSECustomerKeyMd5(source.getSSECustomerKeyMd5());
    }

    // copy user metadata except the magic marker header.
    source.getUserMetadata().entrySet().stream()
        .filter(e -> !e.getKey().equals(X_HEADER_MAGIC_MARKER))
        .forEach(e -> dest.addUserMetadata(e.getKey(), e.getValue()));
  }

}
