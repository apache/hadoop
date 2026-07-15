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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_ACL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_COMPLETION_TIME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_ID;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_PROGRESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_STATUS_DESCRIPTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_CREATION_TIME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_ETAG;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_GROUP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_IS_DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_LAST_MODIFIED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_METADATA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_OWNER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_PERMISSIONS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_METADATA_NEXT_MARKER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_RESOURCE_TYPE_BLOB_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ARROW_LIST_PARSING;

/**
 * Photon response parser implementation for Apache Arrow based ListBlobs
 * responses. The parser reads the Arrow IPC stream returned by the service,
 * iterates through the record batches and converts each row into a
 * {@link BlobListResultEntrySchema}, producing a {@link BlobListResultSchema}
 * that keeps the downstream FileStatus conversion path unchanged.
 * <p>
 * Arrow specific objects are confined to this class; the output contract is
 * {@link BlobListResultSchema} so existing listing behavior is preserved.
 * <p>
 * Column matching is case-insensitive. Unknown columns are ignored and missing
 * columns are treated as absent values so that additive schema changes do not
 * break parsing. The continuation token is read from the Arrow schema custom
 * metadata (key {@code NextMarker}).
 */
public class ArrowListBlobParser implements ListBlobResponseParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(ArrowListBlobParser.class);

  /**
   * Upper bound, in bytes, on the heap staging buffer allocated per read when
   * copying into a direct (non-array-backed) {@link ByteBuffer}. Caps transient
   * allocations for large reader requests; the reader simply issues additional
   * reads for anything beyond this chunk.
   */
  private static final int NON_INTERRUPTIBLE_READ_CHUNK = 8192;

  /**
   * Base URL for which the ListBlobs API is called, used to build the absolute
   * URL for each entry (mirrors the XML parser behavior).
   */
  private final String url;

  /**
   * Maximum off-heap (direct) memory in bytes the Arrow allocator may use while
   * parsing a single response. Bounds the memory an oversized or malformed
   * response can consume; exceeding it fails the parse with an
   * {@link org.apache.arrow.memory.OutOfMemoryException} that is surfaced as an
   * {@link IOException}.
   */
  private final long memoryLimitBytes;

  /**
   * @param url base URL for which the ListBlobs API is called.
   * @param memoryLimitBytes maximum off-heap memory in bytes the Arrow allocator
   *                         may use while parsing a single response.
   */
  public ArrowListBlobParser(final String url, final long memoryLimitBytes) {
    this.url = url;
    this.memoryLimitBytes = memoryLimitBytes;
  }

  @Override
  public BlobListResultSchema parse(final InputStream responseStream)
      throws IOException {
    BlobListResultSchema listResultSchema = new BlobListResultSchema();
    try (BufferAllocator allocator = new RootAllocator(memoryLimitBytes);
        ArrowStreamReader reader =
            new ArrowStreamReader(nonInterruptibleChannel(responseStream),
                allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      Schema schema = root.getSchema();

      // Continuation token is carried in the Arrow schema custom metadata.
      // The service emits an empty string when there is no continuation token,
      // whereas the XML path leaves it null; normalize an absent or empty
      // marker to null so both formats produce identical values.
      Map<String, String> customMetadata = schema.getCustomMetadata();
      if (customMetadata != null) {
        String nextMarker = customMetadata.get(ARROW_METADATA_NEXT_MARKER);
        listResultSchema.setNextMarker(
            (nextMarker == null || nextMarker.isEmpty()) ? null : nextMarker);
      }

      while (reader.loadNextBatch()) {
        BatchColumns columns = BatchColumns.resolve(root);
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
          BlobListResultEntrySchema entry = buildEntry(columns, row);
          if (entry != null) {
            listResultSchema.addBlobListEntry(entry);
          }
        }
      }
      LOG.debug("Photon ListBlobs parsed {} blobs with {} as continuation token",
          listResultSchema.paths().size(), listResultSchema.getNextMarker());
      return listResultSchema;
    } catch (IOException ex) {
      throw ex;
    } catch (RuntimeException ex) {
      // Wrap Arrow runtime failures (e.g. corrupted stream) as IOException so
      // callers can surface them through the existing error-handling model.
      throw new IOException(ex);
    }
  }

  @Override
  public String getParsingErrorMessage() {
    return ERR_ARROW_LIST_PARSING;
  }

  /**
   * Wrap an {@link InputStream} in a {@link ReadableByteChannel} that is
   * <em>not</em> interruptible.
   *
   * <p>{@link ArrowStreamReader}, when handed an {@code InputStream}, internally
   * adapts it with {@link java.nio.channels.Channels#newChannel(InputStream)},
   * which returns a channel extending
   * {@link java.nio.channels.spi.AbstractInterruptibleChannel}. If the parsing
   * thread's interrupt flag is set (e.g. Hadoop task cancellation, speculative
   * execution kills, or an executor {@code shutdownNow()}), that channel aborts
   * the read with a {@link java.nio.channels.ClosedByInterruptException},
   * turning a benign interrupt into a hard listing failure. The XML SAX parser
   * reads from a plain {@code InputStream} and is immune to this, so the Arrow
   * path must not be more fragile.</p>
   *
   * <p>The list response body is already fully buffered in memory before it
   * reaches the parser, so an interruptible channel offers no benefit here; this
   * plain adapter simply reads from the buffer without consulting the thread's
   * interrupt status.</p>
   *
   * @param in the buffered response stream.
   * @return a non-interruptible channel over {@code in}.
   */
  private static ReadableByteChannel nonInterruptibleChannel(
      final InputStream in) {
    return new ReadableByteChannel() {
      private volatile boolean open = true;

      @Override
      public int read(final ByteBuffer dst) throws IOException {
        final int toRead = dst.remaining();
        if (toRead == 0) {
          return 0;
        }
        if (dst.hasArray()) {
          final int n = in.read(dst.array(),
              dst.arrayOffset() + dst.position(), toRead);
          if (n > 0) {
            dst.position(dst.position() + n);
          }
          return n;
        }
        // For a direct (non-array-backed) ByteBuffer we must stage bytes in a
        // heap buffer first. ArrowStreamReader can request very large reads, so
        // cap the staging buffer to a fixed size and let the reader issue
        // further reads for the remainder, avoiding large transient allocations.
        final int chunk = Math.min(toRead, NON_INTERRUPTIBLE_READ_CHUNK);
        final byte[] tmp = new byte[chunk];
        final int n = in.read(tmp, 0, chunk);
        if (n > 0) {
          dst.put(tmp, 0, n);
        }
        return n;
      }

      @Override
      public boolean isOpen() {
        return open;
      }

      @Override
      public void close() throws IOException {
        open = false;
        in.close();
      }
    };
  }

  /**
   * Convert a single Arrow row into a {@link BlobListResultEntrySchema}. Rows
   * without a usable name are skipped.
   */
  private BlobListResultEntrySchema buildEntry(
      final BatchColumns columns, final int row) {
    String name = readString(columns.name, row);
    if (name == null || name.isEmpty()) {
      return null;
    }
    // Directory names may carry a trailing slash; strip it to match XML parser.
    if (name.endsWith(FORWARD_SLASH)) {
      name = name.substring(0, name.length() - 1);
    }

    BlobListResultEntrySchema entry = new BlobListResultEntrySchema();
    entry.setName(name);
    entry.setPath(new Path(ROOT_PATH + name));
    entry.setUrl(url + ROOT_PATH + name);

    entry.setETag(readString(columns.etag, row));
    // The Arrow (Photon) response serializes timestamps as native Arrow
    // timestamp vectors (e.g. Creation-Time / Last-Modified as TimeStampSec),
    // whose object form is an ISO-8601 local date-time (e.g. 2026-07-06T10:31:19)
    // in UTC. Normalize to RFC 1123 GMT so both paths produce identical values
    // and DateTimeUtils.parseLastModifiedTime yields the correct time.
    entry.setLastModifiedTime(readTimestampAsRfc1123(columns.lastModified, row));
    entry.setCreationTime(readTimestampAsRfc1123(columns.creationTime, row));
    entry.setOwner(readString(columns.owner, row));
    entry.setGroup(readString(columns.group, row));
    entry.setPermission(readString(columns.permission, row));
    entry.setAcl(readString(columns.acl, row));

    // Blob user metadata (including the hdi_isfolder directory marker) is
    // carried in a single Arrow map column; mirror the XML parser which
    // populates the metadata map from the <Metadata> element.
    Map<String, String> metadata = readMetadata(columns.metadata, row);
    if (!metadata.isEmpty()) {
      entry.setMetadata(metadata);
    }

    setContentLength(entry, columns.contentLength, row);
    setCopyProperties(entry, columns, row);
    entry.setIsDirectory(resolveIsDirectory(columns, metadata, row));
    return entry;
  }

  /**
   * Populate the copy-related properties, mirroring the XML parser's handling
   * of the {@code Properties} copy elements. Absent columns leave the
   * corresponding field unset, exactly as the XML path does when the elements
   * are missing.
   */
  private void setCopyProperties(final BlobListResultEntrySchema entry,
      final BatchColumns columns, final int row) {
    entry.setCopyId(readString(columns.copyId, row));
    entry.setCopyStatus(readString(columns.copyStatus, row));
    entry.setCopySourceUrl(readString(columns.copySource, row));
    entry.setCopyProgress(readString(columns.copyProgress, row));
    entry.setCopyStatusDescription(
        readString(columns.copyStatusDescription, row));
    // XML stores the completion time as epoch millis parsed from an RFC 1123
    // string; normalize the Arrow timestamp to the same RFC 1123 form first so
    // both paths yield identical epoch values.
    String copyCompletion =
        readTimestampAsRfc1123(columns.copyCompletionTime, row);
    if (copyCompletion != null) {
      entry.setCopyCompletionTime(
          DateTimeUtils.parseLastModifiedTime(copyCompletion));
    }
  }

  /**
   * Populate the content length from the content-length column. The Blob
   * endpoint surfaces it as a native unsigned 64-bit integer
   * ({@code UInt8Vector}); other numeric vector types and a numeric string
   * column are also tolerated so additive schema changes do not break parsing.
   */
  private void setContentLength(final BlobListResultEntrySchema entry,
      final FieldVector contentLength, final int row) {
    Long value = readLong(contentLength, row);
    if (value != null) {
      entry.setContentLength(value);
    }
  }

  /**
   * Determine whether the row represents a directory. Mirrors the XML parser
   * ({@code BlobListXmlParser}), which flags an entry as a directory when any of
   * the following hold:
   * <ul>
   *   <li>the entry is a {@code BlobPrefix} - surfaced in the Arrow response as
   *   a {@code ResourceType} of {@code blobprefix} - i.e. an implicit
   *   directory;</li>
   *   <li>a {@code ResourceType} of {@code directory};</li>
   *   <li>an explicit boolean {@code IsDirectory} indicator;</li>
   *   <li>the {@code hdi_isfolder} user metadata being {@code true}.</li>
   * </ul>
   * The implicit-directory ({@code blobprefix}) and {@code hdi_isfolder} cases
   * are essential: an implicit directory has no marker blob at all, while an
   * empty directory created by {@code mkdir} is a zero-byte marker blob whose
   * only directory indicator is the {@code hdi_isfolder=true} metadata entry.
   * The metadata key is matched case-insensitively, matching the XML parser
   * ({@code equalsIgnoreCase}). Without honoring these, such directories are
   * misclassified as files, diverging from the XML listing path.
   */
  private boolean resolveIsDirectory(final BatchColumns columns,
      final Map<String, String> metadata, final int row) {
    String resourceType = readString(columns.resourceType, row);
    if (DIRECTORY.equalsIgnoreCase(resourceType)
        || ARROW_RESOURCE_TYPE_BLOB_PREFIX.equalsIgnoreCase(resourceType)) {
      return true;
    }
    if (readBoolean(columns.isDirectory, row)) {
      return true;
    }
    String hdiIsFolder = metadataValueIgnoreCase(metadata, XML_TAG_HDI_ISFOLDER);
    if (hdiIsFolder != null && Boolean.parseBoolean(hdiIsFolder.trim())) {
      return true;
    }
    // Fallback for a schema that flattens the marker metadata into a dedicated
    // column instead of the metadata map.
    return readBoolean(columns.hdiIsFolder, row);
  }

  /**
   * Case-insensitive lookup of a metadata value by key, mirroring the XML
   * parser's {@code equalsIgnoreCase} matching of the {@code hdi_isfolder}
   * marker (the service preserves the casing used when the metadata was set).
   */
  private static String metadataValueIgnoreCase(
      final Map<String, String> metadata, final String key) {
    String value = metadata.get(key);
    if (value != null || metadata.containsKey(key)) {
      return value;
    }
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      if (key.equalsIgnoreCase(entry.getKey())) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Read the user metadata key/value pairs from the Arrow map column for the
   * given row. Keys are stored verbatim (e.g. {@code hdi_isfolder}) to match the
   * XML parser. Returns an empty (mutable) map when the column is absent or the
   * value is null.
   */
  private Map<String, String> readMetadata(final MapVector metadata,
      final int row) {
    Map<String, String> result = new HashMap<>();
    if (metadata == null || row >= metadata.getValueCount()
        || metadata.isNull(row)) {
      return result;
    }
    Object entries = metadata.getObject(row);
    if (!(entries instanceof List)) {
      return result;
    }
    for (Object element : (List<?>) entries) {
      if (element instanceof Map) {
        Map<?, ?> keyValue = (Map<?, ?>) element;
        Object key = keyValue.get(MapVector.KEY_NAME);
        Object value = keyValue.get(MapVector.VALUE_NAME);
        if (key != null) {
          result.put(key.toString(), value == null ? null : value.toString());
        }
      }
    }
    return result;
  }

  /**
   * Read a timestamp column for the given row and convert it to the RFC 1123
   * GMT representation used by the XML path. Native Arrow timestamp vectors
   * expose an ISO-8601 local date-time via {@link FieldVector#getObject(int)};
   * a textual (VarChar) timestamp column is also tolerated. Returns
   * {@code null} when the column is absent or the value is null.
   */
  private String readTimestampAsRfc1123(final FieldVector vector,
      final int row) {
    if (vector == null || row >= vector.getValueCount()
        || vector.isNull(row)) {
      return null;
    }
    Object value = vector.getObject(row);
    if (value == null) {
      return null;
    }
    return DateTimeUtils.formatArrowDateTimeToRfc1123(value.toString());
  }

  /**
   * Read a numeric column for the given row as a {@code long}. Supports native
   * Arrow integer vectors (whose object form is a {@link Number}) and a numeric
   * {@link VarCharVector}. Returns {@code null} when the column is absent, the
   * value is null, or a textual value is not parseable.
   */
  private Long readLong(final FieldVector vector, final int row) {
    if (vector == null || row >= vector.getValueCount()
        || vector.isNull(row)) {
      return null;
    }
    Object value = vector.getObject(row);
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value != null) {
      String text = value.toString().trim();
      if (!text.isEmpty()) {
        try {
          return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
          // Leave unset if the value is not numeric.
        }
      }
    }
    return null;
  }

  /**
   * Read a boolean-valued column for the given row. Supports both a native
   * Arrow {@link BitVector} and a {@link VarCharVector} carrying a textual
   * {@code true}/{@code 1} value (matching how the XML parser interprets the
   * {@code hdi_isfolder} metadata via {@link Boolean#valueOf(String)}). Returns
   * {@code false} when the column is absent or the value is null/empty.
   */
  private boolean readBoolean(final FieldVector vector, final int row) {
    if (vector == null || row >= vector.getValueCount()
        || vector.isNull(row)) {
      return false;
    }
    if (vector instanceof BitVector) {
      return ((BitVector) vector).get(row) != 0;
    }
    if (vector instanceof VarCharVector) {
      String value = readString((VarCharVector) vector, row);
      if (value != null && !value.isEmpty()) {
        String trimmed = value.trim();
        return Boolean.parseBoolean(trimmed) || "1".equals(trimmed);
      }
    }
    return false;
  }

  /**
   * Read the UTF-8 value of a variable-width column for the given row, or
   * {@code null} if the column is absent or the value is null. Reads the value
   * bytes directly to avoid the boxing performed by
   * {@link FieldVector#getObject(int)}.
   */
  private static String readString(final VarCharVector vector, final int row) {
    if (vector == null || row >= vector.getValueCount()
        || vector.isNull(row)) {
      return null;
    }
    return new String(vector.get(row), StandardCharsets.UTF_8);
  }

  /**
   * Column references for a single loaded record batch, resolved once per batch
   * so that per-row access avoids repeated case-insensitive name lookups and
   * boxing. Columns absent from the schema are held as {@code null}.
   */
  private static final class BatchColumns {
    private final VarCharVector name;
    private final VarCharVector etag;
    private final FieldVector lastModified;
    private final FieldVector creationTime;
    private final VarCharVector owner;
    private final VarCharVector group;
    private final VarCharVector permission;
    private final VarCharVector acl;
    private final VarCharVector resourceType;
    private final FieldVector contentLength;
    private final FieldVector isDirectory;
    private final FieldVector hdiIsFolder;
    private final MapVector metadata;
    private final VarCharVector copyId;
    private final VarCharVector copyStatus;
    private final VarCharVector copySource;
    private final VarCharVector copyProgress;
    private final FieldVector copyCompletionTime;
    private final VarCharVector copyStatusDescription;

    private BatchColumns(final Map<String, FieldVector> byLowerName) {
      this.name = varChar(byLowerName, ARROW_COL_NAME);
      this.etag = varChar(byLowerName, ARROW_COL_ETAG);
      this.lastModified = byLowerName.get(lower(ARROW_COL_LAST_MODIFIED));
      this.creationTime = byLowerName.get(lower(ARROW_COL_CREATION_TIME));
      this.owner = varChar(byLowerName, ARROW_COL_OWNER);
      this.group = varChar(byLowerName, ARROW_COL_GROUP);
      this.permission = varChar(byLowerName, ARROW_COL_PERMISSIONS);
      this.acl = varChar(byLowerName, ARROW_COL_ACL);
      this.resourceType = varChar(byLowerName, ARROW_COL_RESOURCE_TYPE);
      this.contentLength = byLowerName.get(lower(ARROW_COL_CONTENT_LENGTH));
      this.isDirectory = byLowerName.get(lower(ARROW_COL_IS_DIRECTORY));
      this.copyId = varChar(byLowerName, ARROW_COL_COPY_ID);
      this.copyStatus = varChar(byLowerName, ARROW_COL_COPY_STATUS);
      this.copySource = varChar(byLowerName, ARROW_COL_COPY_SOURCE);
      this.copyProgress = varChar(byLowerName, ARROW_COL_COPY_PROGRESS);
      this.copyCompletionTime =
          byLowerName.get(lower(ARROW_COL_COPY_COMPLETION_TIME));
      this.copyStatusDescription =
          varChar(byLowerName, ARROW_COL_COPY_STATUS_DESCRIPTION);
      FieldVector metadataColumn = byLowerName.get(lower(ARROW_COL_METADATA));
      this.metadata = metadataColumn instanceof MapVector
          ? (MapVector) metadataColumn : null;
      // A directory marker's folder-ness is normally carried inside the
      // metadata map (key hdi_isfolder). As a fallback, also accept a schema
      // that flattens it into a dedicated column, in either the bare metadata
      // key (hdi_isfolder) or HTTP header form (x-ms-meta-hdi_isfolder).
      FieldVector hdiIsFolderColumn = byLowerName.get(lower(XML_TAG_HDI_ISFOLDER));
      if (hdiIsFolderColumn == null) {
        hdiIsFolderColumn = byLowerName.get(lower(X_MS_META_HDI_ISFOLDER));
      }
      this.hdiIsFolder = hdiIsFolderColumn;
    }

    /**
     * Build a case-insensitive column index for the current batch and resolve
     * the strongly-typed column references from it.
     */
    static BatchColumns resolve(final VectorSchemaRoot root) {
      Map<String, FieldVector> byLowerName = new HashMap<>();
      for (FieldVector fieldVector : root.getFieldVectors()) {
        byLowerName.put(lower(fieldVector.getName()), fieldVector);
      }
      return new BatchColumns(byLowerName);
    }

    private static VarCharVector varChar(
        final Map<String, FieldVector> byLowerName, final String columnName) {
      FieldVector vector = byLowerName.get(lower(columnName));
      return vector instanceof VarCharVector ? (VarCharVector) vector : null;
    }

    private static String lower(final String value) {
      return value.toLowerCase(Locale.ROOT);
    }
  }
}
