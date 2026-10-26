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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.ArrowListBlobParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_COMPLETION_TIME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_ID;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_PROGRESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_COPY_STATUS_DESCRIPTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_CREATION_TIME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_ETAG;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_IS_DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_LAST_MODIFIED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_METADATA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_METADATA_NEXT_MARKER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ArrowListBlobParser}, the Photon (Apache Arrow based)
 * ListBlobs response parser.
 */
public class TestArrowListBlobParser {

  private static final String URL = "https://account.blob.core.windows.net/container";

  /** Generous allocator limit used by the functional parsing tests. */
  private static final long MEMORY_LIMIT = 256L * 1024 * 1024;

  /** Deliberately tiny allocator limit used to force an over-limit failure. */
  private static final long TINY_MEMORY_LIMIT = 1024L;

  /** Arrow integer bit width used for the content-length column. */
  private static final int ARROW_INT_BIT_WIDTH = 64;

  // Sample content-length values shared between builder rows and assertions.
  private static final long CONTENT_LENGTH_A = 20L;
  private static final long CONTENT_LENGTH_B = 30L;
  private static final long CONTENT_LENGTH_C = 40L;
  private static final long CONTENT_LENGTH_D = 42L;
  private static final long CONTENT_LENGTH_SAMPLE = 1234L;

  /**
   * Mirrors {@code ArrowListBlobParser.NON_INTERRUPTIBLE_READ_CHUNK}: the 8 KB
   * heap staging chunk used when copying into a direct byte buffer. A payload
   * larger than this drives the partial-read staging loop.
   */
  private static final int STAGING_CHUNK_BYTES = 8192;

  /**
   * Verify an Arrow response with a single blob is parsed correctly and all
   * common blob properties are populated.
   */
  @Test
  public void testSingleBlob() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .addRow("file1.txt", "0x8DB6668EAB50E67", CONTENT_LENGTH_SAMPLE,
            "Tue, 06 Jun 2023 08:35:00 GMT",
            "Tue, 06 Jun 2023 08:34:28 GMT", false)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    BlobListResultEntrySchema entry = result.paths().get(0);
    assertThat(entry.name()).isEqualTo("file1.txt");
    assertThat(entry.path().toUri().getPath()).isEqualTo("/file1.txt");
    assertThat(entry.url()).isEqualTo(URL + "/file1.txt");
    assertThat(entry.eTag()).isEqualTo("0x8DB6668EAB50E67");
    assertThat(entry.contentLength()).isEqualTo(CONTENT_LENGTH_SAMPLE);
    assertThat(entry.lastModified()).isEqualTo("Tue, 06 Jun 2023 08:35:00 GMT");
    assertThat(entry.creation()).isEqualTo("Tue, 06 Jun 2023 08:34:28 GMT");
    assertThat(entry.isDirectory()).isFalse();
  }

  /**
   * Verify that an ISO-8601 date-time from the Arrow response (as Photon
   * serializes timestamps, without an explicit zone) is normalized to the same
   * RFC 1123 GMT representation used by the XML ListBlobs path, and that it
   * yields the expected epoch when parsed downstream.
   */
  @Test
  public void testArrowIsoDateTimeNormalizedToRfc1123() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .addRow("file1.txt", "etag", 1L,
            "2026-07-06T10:31:19", "2026-07-06T10:30:00", false)
        .build();

    BlobListResultSchema result = parse(stream);

    BlobListResultEntrySchema entry = result.paths().get(0);
    assertThat(entry.lastModified())
        .isEqualTo("Mon, 06 Jul 2026 10:31:19 GMT");
    assertThat(entry.creation())
        .isEqualTo("Mon, 06 Jul 2026 10:30:00 GMT");

    long arrowEpoch = DateTimeUtils.parseLastModifiedTime(
        entry.lastModified());
    long xmlEpoch = DateTimeUtils.parseLastModifiedTime(
        "Mon, 06 Jul 2026 10:31:19 GMT");
    assertThat(arrowEpoch).isEqualTo(xmlEpoch);
  }

  /**
   * A directory entry (or any entry) may carry no timestamp. Verify that a
   * missing timestamp is exposed as {@code null} - not the string {@code "null"}
   * - through the {@code lastModified()}/{@code creation()} accessors used by
   * the FileStatus conversion. Previously these accessors wrapped the field in
   * {@code String.valueOf}, turning an absent value into {@code "null"} that
   * then failed {@link DateTimeUtils#parseLastModifiedTime(String)} and logged a
   * spurious error.
   */
  @Test
  public void testMissingTimestampExposedAsNull() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .addRow("dir1", "etag", 0L, null, null, true)
        .build();

    BlobListResultSchema result = parse(stream);

    BlobListResultEntrySchema entry = result.paths().get(0);
    assertThat(entry.name()).isEqualTo("dir1");
    assertThat(entry.lastModified()).isNull();
    assertThat(entry.creation()).isNull();
  }

  /**
   * Verify an Arrow response with multiple blobs is parsed correctly.
   */
  @Test
  public void testMultipleBlobs() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .addRow("a.txt", "etagA", 10L, "lmA", "ctA", false)
        .addRow("b.txt", "etagB", CONTENT_LENGTH_A, "lmB", "ctB", false)
        .addRow("dir1", "etagC", 0L, "lmC", "ctC", true)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(3);
    assertThat(result.paths().get(0).name()).isEqualTo("a.txt");
    assertThat(result.paths().get(1).contentLength()).isEqualTo(CONTENT_LENGTH_A);
    assertThat(result.paths().get(2).isDirectory()).isTrue();
  }

  /**
   * Verify an empty Arrow response returns an empty listing.
   */
  @Test
  public void testEmptyResponse() throws Exception {
    byte[] stream = new ArrowStreamBuilder().build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).isEmpty();
    assertThat(result.getNextMarker()).isNull();
  }

  /**
   * Verify directory entries are populated correctly from an Arrow response.
   */
  @Test
  public void testDirectoryEntry() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .addRow("mydir", "etag", 0L, "lm", "ct", true)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).isDirectory()).isTrue();
    assertThat(result.paths().get(0).name()).isEqualTo("mydir");
  }

  /**
   * Verify a directory name with a trailing slash is normalized (slash removed)
   * to match the XML parser behavior.
   */
  @Test
  public void testDirectoryNameTrailingSlashStripped() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .addRow("mydir/", "etag", 0L, "lm", "ct", true)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).name()).isEqualTo("mydir");
    assertThat(result.paths().get(0).path().toUri().getPath()).isEqualTo("/mydir");
  }

  /**
   * Verify an empty-directory marker blob is classified as a directory when its
   * only directory indicator is the {@code hdi_isfolder=true} user metadata
   * (surfaced as a column), even though no {@code IsDirectory}/{@code
   * ResourceType} column marks it as such. This mirrors the XML parser and is
   * the scenario that otherwise causes recursive {@code listFiles} on an empty
   * directory to wrongly report the marker as a file.
   */
  @Test
  public void testMarkerDirectoryViaHdiIsFolderMetadata() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "emptydir");
    row.put(ARROW_COL_ETAG, "etag");
    row.put(ARROW_COL_CONTENT_LENGTH, "0");
    row.put(XML_TAG_HDI_ISFOLDER, "true");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).name()).isEqualTo("emptydir");
    assertThat(result.paths().get(0).isDirectory())
        .as("marker blob with hdi_isfolder=true must be a directory")
        .isTrue();
  }

  /**
   * Verify a {@code hdi_isfolder} value of {@code false} does not flip a plain
   * blob into a directory.
   */
  @Test
  public void testHdiIsFolderFalseIsNotDirectory() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "file.txt");
    row.put(XML_TAG_HDI_ISFOLDER, "false");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).isDirectory()).isFalse();
  }

  /**
   * Verify the marker is also recognized when the metadata surfaces under the
   * HTTP header form column name {@code x-ms-meta-hdi_isfolder}.
   */
  @Test
  public void testMarkerDirectoryViaHttpHeaderMetadataColumn() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "emptydir");
    row.put(X_MS_META_HDI_ISFOLDER, "true");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).isDirectory()).isTrue();
  }

  /**
   * Verify an entry with a {@code ResourceType} of {@code directory} is
   * classified as a directory, matching the XML parser's Properties handling.
   */
  @Test
  public void testResourceTypeDirectory() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "dir1");
    // Literal mixed-case "Directory" so a casing regression is caught (the
    // service returns mixed case and resolveIsDirectory uses equalsIgnoreCase).
    row.put(ARROW_COL_RESOURCE_TYPE, "Directory");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).isDirectory()).isTrue();
  }

  /**
   * Verify parsing against the real Blob-endpoint Arrow schema, where the
   * empty-directory marker's {@code hdi_isfolder} flag is carried inside a
   * {@code Metadata} map column, {@code Content-Length} is an unsigned 64-bit
   * integer ({@code UInt8}) and the timestamps are native Arrow
   * {@code TimeStampSec} vectors. This is the exact shape returned by the
   * service (see {@code ITestAbfsFileSystemContractGetFileStatus
   * #testListFilesEmptyDirectoryRecursive}); the earlier flattened-column
   * assumption never matched a live response.
   */
  @Test
  public void testMarkerDirectoryViaMetadataMapColumn() throws Exception {
    Map<String, String> markerMetadata = new LinkedHashMap<>();
    markerMetadata.put(XML_TAG_HDI_ISFOLDER, "true");
    byte[] stream = new BlobEndpointStreamBuilder()
        .addRow("emptydir", "0x8DEE", 0L, "2026-07-13T07:06:45",
            "2026-07-13T07:06:45", "blob", markerMetadata)
        .addRow("file.txt", "0x8DEF", CONTENT_LENGTH_SAMPLE, "2026-07-13T07:06:46",
            "2026-07-13T07:06:46", "blob", Collections.emptyMap())
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(2);
    BlobListResultEntrySchema marker = result.paths().get(0);
    assertThat(marker.name()).isEqualTo("emptydir");
    assertThat(marker.isDirectory())
        .as("marker blob with Metadata hdi_isfolder=true must be a directory")
        .isTrue();
    assertThat(marker.contentLength()).isEqualTo(0L);
    assertThat(marker.eTag()).isEqualTo("0x8DEE");
    assertThat(marker.metadata()).containsEntry(XML_TAG_HDI_ISFOLDER, "true");
    assertThat(marker.lastModified())
        .as("TimeStampSec value must be normalized to RFC 1123 GMT")
        .isEqualTo(DateTimeUtils.formatArrowDateTimeToRfc1123(
            "2026-07-13T07:06:45"));

    BlobListResultEntrySchema file = result.paths().get(1);
    assertThat(file.name()).isEqualTo("file.txt");
    assertThat(file.isDirectory())
        .as("plain blob without hdi_isfolder must be a file")
        .isFalse();
    assertThat(file.contentLength())
        .as("UInt8 content length must be read as a long")
        .isEqualTo(CONTENT_LENGTH_SAMPLE);
  }

  /**
   * Verify an implicit directory - surfaced by the Blob endpoint as a
   * {@code BlobPrefix} row whose {@code ResourceType} is {@code blobprefix} and
   * whose {@code Name} carries a trailing slash - is classified as a directory,
   * matching the XML parser which flags every {@code <BlobPrefix>} entry as a
   * directory. Without this, implicit directories are misclassified as files.
   */
  @Test
  public void testImplicitDirectoryViaBlobPrefixResourceType()
      throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "implicitDir/azcopy/");
    // Use a literal mixed-case "BlobPrefix" rather than the lower-case constant
    // so a casing regression in resolveIsDirectory (which relies on
    // equalsIgnoreCase) would be caught; the service returns mixed case.
    row.put(ARROW_COL_RESOURCE_TYPE, "BlobPrefix");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).isDirectory())
        .as("blobprefix (implicit directory) must be a directory")
        .isTrue();
  }

  /**
   * Verify the {@code hdi_isfolder} marker is matched case-insensitively, as the
   * Blob service preserves whatever casing the metadata key was set with (e.g.
   * {@code HDI_ISFOLDER}). The XML parser compares with {@code equalsIgnoreCase},
   * so Arrow must too, otherwise directories created with a differently-cased
   * key are misclassified as files (see
   * {@code ITestAzureBlobFileSystemListStatus#testIsDirectoryWithDifferentCases}).
   */
  @Test
  public void testMarkerDirectoryMetadataKeyCaseInsensitive() throws Exception {
    Map<String, String> markerMetadata = new LinkedHashMap<>();
    markerMetadata.put("HDI_ISFOLDER", "true");
    byte[] stream = new BlobEndpointStreamBuilder()
        .addRow("emptydir", "0x8DEE", 0L, "2026-07-13T07:06:45",
            "2026-07-13T07:06:45", "blob", markerMetadata)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).isDirectory())
        .as("hdi_isfolder marker must be matched case-insensitively")
        .isTrue();
  }

  /**
   * Verify the copy-status Properties are propagated from the Arrow columns to
   * the result entry, matching the XML parser which surfaces {@code CopyId},
   * {@code CopyStatus}, {@code CopySource}, {@code CopyProgress},
   * {@code CopyCompletionTime} and {@code CopyStatusDescription}.
   */
  @Test
  public void testCopyPropertiesPopulated() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "copied.txt");
    row.put(ARROW_COL_COPY_ID, "copy-id-1");
    row.put(ARROW_COL_COPY_STATUS, "success");
    row.put(ARROW_COL_COPY_SOURCE, "https://src/blob");
    row.put(ARROW_COL_COPY_PROGRESS, "1234/1234");
    row.put(ARROW_COL_COPY_STATUS_DESCRIPTION, "done");
    row.put(ARROW_COL_COPY_COMPLETION_TIME, "Mon, 13 Jul 2026 07:06:45 GMT");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    BlobListResultEntrySchema entry = result.paths().get(0);
    assertThat(entry.copyId()).isEqualTo("copy-id-1");
    assertThat(entry.copyStatus()).isEqualTo("success");
    assertThat(entry.copySourceUrl()).isEqualTo("https://src/blob");
    assertThat(entry.copyProgress()).isEqualTo("1234/1234");
    assertThat(entry.copyStatusDescription()).isEqualTo("done");
    assertThat(entry.copyCompletionTime())
        .as("CopyCompletionTime must be parsed to epoch millis")
        .isEqualTo(DateTimeUtils.parseLastModifiedTime(
            "Mon, 13 Jul 2026 07:06:45 GMT"));
  }

  /**
   * Verify the continuation token is extracted from the Arrow schema custom
   * metadata.
   */
  @Test
  public void testContinuationToken() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .withNextMarker("marker-123")
        .addRow("a.txt", "etag", 10L, "lm", "ct", false)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.getNextMarker()).isEqualTo("marker-123");
    assertThat(result.paths()).hasSize(1);
  }

  /**
   * Verify that an empty continuation token in the Arrow schema custom metadata
   * (as emitted by the service on the terminal page) is normalized to
   * {@code null}, matching the XML parser which leaves an empty NextMarker null.
   */
  @Test
  public void testEmptyContinuationTokenNormalizedToNull() throws Exception {
    byte[] stream = new ArrowStreamBuilder()
        .withNextMarker("")
        .addRow("a.txt", "etag", 10L, "lm", "ct", false)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.getNextMarker()).isNull();
    assertThat(result.paths()).hasSize(1);
  }

  /**
   * Verify a multi-page listing is handled correctly at the parser level: a
   * non-terminal page carries a next marker signalling continuation while the
   * terminal page carries none, and the rows from every page are parsed. This
   * exercises the per-page building blocks the {@code listPath} pagination loop
   * relies on to stitch multiple Photon responses into one listing.
   */
  @Test
  public void testMultiPageListingHandledCorrectly() throws Exception {
    byte[] page1 = new ArrowStreamBuilder()
        .withNextMarker("page-2-marker")
        .addRow("a.txt", "etagA", 10L, "lm", "ct", false)
        .addRow("b.txt", "etagB", CONTENT_LENGTH_A, "lm", "ct", false)
        .build();
    byte[] page2 = new ArrowStreamBuilder()
        .addRow("c.txt", "etagC", CONTENT_LENGTH_B, "lm", "ct", false)
        .addRow("d.txt", "etagD", CONTENT_LENGTH_C, "lm", "ct", false)
        .build();

    BlobListResultSchema first = parse(page1);
    BlobListResultSchema second = parse(page2);

    // Non-terminal page advertises the marker used to fetch the next page.
    assertThat(first.getNextMarker()).isEqualTo("page-2-marker");
    assertThat(first.paths()).hasSize(2);
    // Terminal page carries no marker, ending the pagination loop.
    assertThat(second.getNextMarker()).isNull();
    assertThat(second.paths()).hasSize(2);

    List<String> allNames = new ArrayList<>();
    first.paths().forEach(p -> allNames.add(p.name()));
    second.paths().forEach(p -> allNames.add(p.name()));
    assertThat(allNames)
        .containsExactly("a.txt", "b.txt", "c.txt", "d.txt");
  }

  /**
   * Verify special-character blob names (spaces, reserved URL characters,
   * percent and plus signs, nested path segments) are parsed without loss or
   * corruption, complementing {@link #testUnicodeBlobName()}. The parser must
   * expose the blob name exactly as returned in the Arrow payload.
   */
  @Test
  public void testSpecialCharacterBlobNames() throws Exception {
    String[] names = {
        "with space.txt",
        "with+plus.txt",
        "with%percent.txt",
        "a&b=c;d,e.txt",
        "nested/dir/child.txt",
        "trailing.dots...",
        "emoji-\uD83D\uDE00.txt",
    };
    ArrowStreamBuilder builder = new ArrowStreamBuilder();
    for (String name : names) {
      builder.addRow(name, "etag", 1L, "lm", "ct", false);
    }

    BlobListResultSchema result = parse(builder.build());

    assertThat(result.paths()).hasSize(names.length);
    for (int i = 0; i < names.length; i++) {
      assertThat(result.paths().get(i).name())
          .as("name preserved verbatim for %s", names[i])
          .isEqualTo(names[i]);
    }
    // For names free of URI-reserved delimiters, the derived Path must round
    // trip to the same relative path used by the XML listing route.
    assertThat(result.paths().get(0).path().toUri().getPath())
        .isEqualTo("/with space.txt");
    assertThat(result.paths().get(4).path().toUri().getPath())
        .isEqualTo("/nested/dir/child.txt");
  }

  /**
   * Verify special characters and Unicode blob names are parsed correctly.
   */
  @Test
  public void testUnicodeBlobName() throws Exception {
    String name = "文件-Ünïcode &+ space.txt";
    byte[] stream = new ArrowStreamBuilder()
        .addRow(name, "etag", 5L, "lm", "ct", false)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).name()).isEqualTo(name);
  }

  /**
   * Verify additional unknown columns in the Arrow response are ignored safely
   * and the known columns are still parsed.
   */
  @Test
  public void testUnknownColumnsIgnored() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "a.txt");
    row.put(ARROW_COL_ETAG, "etag");
    row.put(ARROW_COL_CONTENT_LENGTH, "42");
    row.put("SomeUnknownColumn", "ignore-me");
    row.put("AnotherFutureColumn", "also-ignored");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).name()).isEqualTo("a.txt");
    assertThat(result.paths().get(0).contentLength()).isEqualTo(CONTENT_LENGTH_D);
  }

  /**
   * Verify that when the Name column is entirely absent from the Arrow schema
   * the parser fails loudly rather than silently returning an empty listing.
   * Name is the one column {@code buildEntry()} cannot proceed without, so a
   * schema lacking it would otherwise be indistinguishable from a genuinely
   * empty directory and surface later as missing data in Spark/Hive.
   */
  @Test
  public void testMissingMandatoryNameColumn() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_ETAG, "etag");
    row.put(ARROW_COL_CONTENT_LENGTH, "42");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    assertThatThrownBy(() -> parse(stream))
        .isInstanceOf(IOException.class)
        .hasMessageContaining(ARROW_COL_NAME);
  }

  /**
   * Verify a row whose Name column is present but null is skipped (rather than
   * failing the whole listing), matching the XML parser which ignores an entry
   * without a name.
   */
  @Test
  public void testNullNameValueRowSkipped() throws Exception {
    Map<String, String> withName = new LinkedHashMap<>();
    withName.put(ARROW_COL_NAME, "a.txt");
    withName.put(ARROW_COL_CONTENT_LENGTH, "42");
    Map<String, String> withoutName = new LinkedHashMap<>();
    withoutName.put(ARROW_COL_NAME, null);
    withoutName.put(ARROW_COL_CONTENT_LENGTH, "10");
    byte[] stream = buildStringColumnStream(
        new ArrayList<Map<String, String>>() {{
          add(withName);
          add(withoutName);
        }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).name()).isEqualTo("a.txt");
  }

  /**
   * Verify a corrupted Arrow stream results in an appropriate failure
   * (IOException) rather than a silent empty result.
   */
  @Test
  public void testCorruptedStreamFails() {
    byte[] garbage = "this is definitely not an arrow stream".getBytes(
        StandardCharsets.UTF_8);
    assertThatThrownBy(() -> parse(garbage)).isInstanceOf(IOException.class);
  }

  /**
   * Verify a truncated Arrow stream that carries a complete schema but a
   * dropped/incomplete record batch fails rather than silently returning an
   * empty listing. Cutting at an arbitrary midpoint is unreliable - if the whole
   * schema and no batch survive, the reader can legitimately return zero batches
   * and the parser would hand back an empty listing (silent data loss). We
   * therefore keep the entire schema message and truncate inside the record
   * batch, and assert the parse throws.
   */
  @Test
  public void testTruncatedStreamFails() throws Exception {
    // A schema-only stream (no rows) gives the exact byte length of the schema
    // message (plus the end-of-stream marker). Truncating the full stream at
    // that length keeps the whole schema intact while cutting off the record
    // batch part-way through.
    byte[] schemaOnly = new ArrowStreamBuilder().build();
    byte[] full = new ArrowStreamBuilder()
        .addRow("a.txt", "etag", 10L, "lm", "ct", false)
        .build();
    byte[] truncated = Arrays.copyOf(full, schemaOnly.length);

    assertThatThrownBy(() -> parse(truncated)).isInstanceOf(IOException.class);
  }

  /**
   * Verify that an Arrow response whose parsing would exceed the configured
   * allocator memory limit fails with an {@link IOException} rather than
   * exhausting off-heap memory.
   */
  @Test
  public void testOverAllocatorLimitFails() throws Exception {
    byte[] stream = ArrowListBlobTestStreams.overAllocatorLimitNameStream();

    assertThatThrownBy(() -> {
      try (InputStream in = new ByteArrayInputStream(stream)) {
        new ArrowListBlobParser(URL, TINY_MEMORY_LIMIT).parse(in);
      }
    }).isInstanceOf(IOException.class)
        // The failure must be the allocator running out of memory, not some
        // unrelated IOException; otherwise the test could silently stop
        // exercising the memory-limit guard. parse() wraps the Arrow runtime
        // failure as a message-less IOException, so assert on the cause.
        .hasCauseInstanceOf(OutOfMemoryException.class);
  }

  /**
   * Verify the Arrow parser is immune to a set thread-interrupt flag. Because
   * the parser must not adapt the buffered body with an interruptible NIO
   * channel, parsing a valid stream on a thread whose interrupt status is set
   * must still succeed (rather than fail with a
   * {@link java.nio.channels.ClosedByInterruptException}), matching the XML SAX
   * parser which reads a plain stream and ignores interrupts. The interrupt
   * status is preserved and restored so callers can still observe it.
   */
  @Test
  public void testParseSurvivesInterruptedThread() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "file.txt");
    row.put(ARROW_COL_CONTENT_LENGTH, "10");
    byte[] stream = buildStringColumnStream(new ArrayList<Map<String, String>>() {{
        add(row);
      }}, null);

    Thread.currentThread().interrupt();
    try {
      BlobListResultSchema result = parse(stream);

      assertThat(result.paths()).hasSize(1);
      assertThat(result.paths().get(0).name()).isEqualTo("file.txt");
      assertThat(Thread.currentThread().isInterrupted())
          .as("parser must not clear the caller's interrupt status")
          .isTrue();
    } finally {
      // Clear the interrupt flag so it cannot leak into other tests.
      Thread.interrupted();
    }
  }

  /**
   * A timezone-aware Arrow timestamp vector exposes an epoch number (not a
   * date-time object) via {@code getObject()}. Verify such a column is still
   * normalized to the RFC 1123 GMT string the XML path produces, rather than
   * passing the raw epoch through as a bogus modification time. The service
   * could switch to a TZ column without that being a breaking change.
   */
  @Test
  public void testTimeZoneTimestampNormalizedToRfc1123() throws Exception {
    long epochMillis = LocalDateTime.parse("2026-07-06T10:31:19")
        .toEpochSecond(ZoneOffset.UTC) * 1000L;
    byte[] stream = buildTimeZoneTimestampStream("file.txt", epochMillis);

    BlobListResultSchema result = parse(stream);

    BlobListResultEntrySchema entry = result.paths().get(0);
    assertThat(entry.lastModified()).isEqualTo("Mon, 06 Jul 2026 10:31:19 GMT");
    assertThat(DateTimeUtils.parseLastModifiedTime(entry.lastModified()))
        .isEqualTo(DateTimeUtils.parseLastModifiedTime(
            "Mon, 06 Jul 2026 10:31:19 GMT"));
  }

  /**
   * Verify a single Arrow stream carrying two record batches is fully drained:
   * the {@code while (loadNextBatch())} loop must iterate for every batch and
   * the column references resolved once before the loop must stay valid across
   * batches.
   */
  @Test
  public void testTwoBatchesInOneStream() throws Exception {
    byte[] stream = buildTwoBatchNameStream();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(3);
    assertThat(result.paths().get(0).name()).isEqualTo("batch1-a");
    assertThat(result.paths().get(1).name()).isEqualTo("batch1-b");
    assertThat(result.paths().get(2).name()).isEqualTo("batch2-a");
  }

  /**
   * An unsigned 64-bit ({@code UInt8}) content length above {@code Long.MAX_VALUE}
   * comes back negative from {@code getObject()}. Verify such a malformed length
   * is rejected so a FileStatus can never surface with a negative length; the
   * entry falls back to the default zero length instead.
   */
  @Test
  public void testNegativeContentLengthRejected() throws Exception {
    byte[] stream = new BlobEndpointStreamBuilder()
        .addRow("blob.txt", "etag", -1L, "2026-07-06T10:31:19",
            "2026-07-06T10:30:00", "blob", new LinkedHashMap<String, String>())
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).contentLength()).isEqualTo(0L);
  }

  /**
   * Verify a non-numeric textual content length is ignored (leaving the default
   * zero length) rather than failing parsing, matching the XML path's handling
   * of an unparseable {@code Content-Length}.
   */
  @Test
  public void testNonNumericContentLengthIgnored() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "blob.txt");
    row.put(ARROW_COL_CONTENT_LENGTH, "not-a-number");
    byte[] stream = buildStringColumnStream(
        new ArrayList<Map<String, String>>() {{
          add(row);
        }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).contentLength()).isEqualTo(0L);
  }

  /**
   * Verify a directory marker carried as a flattened {@code hdi_isfolder}
   * column with the numeric-true value {@code "1"} is recognized as a directory.
   */
  @Test
  public void testHdiIsFolderNumericTrueColumn() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "dir1");
    row.put(XML_TAG_HDI_ISFOLDER, "1");
    byte[] stream = buildStringColumnStream(
        new ArrayList<Map<String, String>>() {{
          add(row);
        }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).isDirectory()).isTrue();
  }

  /**
   * Verify a blob name consisting solely of {@code "/"} is handled without
   * error. The trailing slash is stripped (as for any directory name), leaving
   * an entry rooted at the container root.
   */
  @Test
  public void testNameThatIsOnlySlash() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "/");
    byte[] stream = buildStringColumnStream(
        new ArrayList<Map<String, String>>() {{
          add(row);
        }}, null);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).name()).isEmpty();
  }

  /**
   * Verify a payload larger than the 8 KB heap staging chunk used when copying
   * into a direct {@link java.nio.ByteBuffer} parses correctly. Small payloads
   * never exercise the partial-read staging branch of the non-interruptible
   * channel; a record batch well above 8 KB drives that loop across multiple
   * reads.
   */
  @Test
  public void testLargePayloadExceedingStagingChunkParses() throws Exception {
    final int rowCount = 500;
    ArrowStreamBuilder builder = new ArrowStreamBuilder();
    for (int i = 0; i < rowCount; i++) {
      builder.addRow("some-reasonably-long-blob-name-for-staging-" + i,
          "etag-" + i, i, "2026-07-06T10:31:19", "2026-07-06T10:30:00", false);
    }
    byte[] stream = builder.build();
    assertThat(stream.length)
        .as("payload must exceed the 8 KB staging chunk to exercise it")
        .isGreaterThan(STAGING_CHUNK_BYTES);

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(rowCount);
    assertThat(result.paths().get(0).name())
        .isEqualTo("some-reasonably-long-blob-name-for-staging-0");
    assertThat(result.paths().get(rowCount - 1).name())
        .isEqualTo("some-reasonably-long-blob-name-for-staging-" + (rowCount - 1));
    assertThat(result.paths().get(rowCount - 1).contentLength())
        .isEqualTo(rowCount - 1L);
  }

  /**
   * Verify the ISO-8601 variants Photon can serialize are all normalized to the
   * same RFC 1123 GMT value: fractional seconds and a trailing {@code Z} are
   * treated as UTC by the fast path, while an explicit non-UTC offset falls back
   * to the {@code java.time} path and is converted to UTC (not dropped).
   */
  @Test
  public void testArrowIsoDateTimeVariantsNormalized() throws Exception {
    assertThat(lastModifiedFor("2026-07-06T10:31:19.1234567"))
        .as("fractional seconds are truncated and treated as UTC")
        .isEqualTo("Mon, 06 Jul 2026 10:31:19 GMT");
    assertThat(lastModifiedFor("2026-07-06T10:31:19Z"))
        .as("a trailing Z denotes UTC")
        .isEqualTo("Mon, 06 Jul 2026 10:31:19 GMT");
    assertThat(lastModifiedFor("2026-07-06T10:31:19+05:30"))
        .as("an explicit offset must be honoured and converted to UTC")
        .isEqualTo("Mon, 06 Jul 2026 05:01:19 GMT");
  }

  /**
   * Verify the hand-rolled Sakamoto weekday math matches the JDK for a leap day
   * (2028-02-29) and for Jan/Feb dates (which take the {@code month < 3 ? year -
   * 1} branch). A wrong weekday silently corrupts every FileStatus mtime.
   */
  @Test
  public void testArrowDateWeekdayLeapDayAndJanFeb() throws Exception {
    assertThat(lastModifiedFor("2028-02-29T00:00:00"))
        .isEqualTo("Tue, 29 Feb 2028 00:00:00 GMT")
        .isEqualTo(rfc1123Utc("2028-02-29T00:00:00"));
    assertThat(lastModifiedFor("2026-01-15T08:00:00"))
        .isEqualTo(rfc1123Utc("2026-01-15T08:00:00"));
    assertThat(lastModifiedFor("2026-02-15T08:00:00"))
        .isEqualTo(rfc1123Utc("2026-02-15T08:00:00"));
  }

  /**
   * Verify an impossible calendar date (2026-02-31) is not rendered as a
   * confident-but-bogus RFC 1123 string by the fast path (which would otherwise
   * emit e.g. {@code Tue, 31 Feb 2026}). The fast path rejects it and defers to
   * the precise {@code java.time} path, which resolves the impossible date
   * rather than inventing a wrong weekday. Pin that behaviour.
   */
  @Test
  public void testArrowInvalidCalendarDatePassesThrough() throws Exception {
    assertThat(lastModifiedFor("2026-02-31T00:00:00"))
        .as("an invalid calendar date must not yield a bogus 31-Feb weekday")
        .isEqualTo("Sat, 28 Feb 2026 00:00:00 GMT");
  }

  /**
   * Verify a {@code hdi_isfolder} marker carried inside the {@code Metadata} map
   * column with the numeric-true value {@code "1"} is recognized as a directory,
   * exactly as the flattened {@code hdi_isfolder="1"} column is
   * ({@link #testHdiIsFolderNumericTrueColumn()}). Both directory-marker paths
   * must share one predicate so a map-carried {@code "1"} is never a file while a
   * flattened {@code "1"} is a directory.
   */
  @Test
  public void testHdiIsFolderNumericTrueMetadataMapColumn() throws Exception {
    Map<String, String> markerMetadata = new LinkedHashMap<>();
    markerMetadata.put(XML_TAG_HDI_ISFOLDER, "1");
    byte[] stream = new BlobEndpointStreamBuilder()
        .addRow("emptydir", "0x8DEE", 0L, "2026-07-13T07:06:45",
            "2026-07-13T07:06:45", "blob", markerMetadata)
        .build();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.paths().get(0).isDirectory())
        .as("map-carried hdi_isfolder=\"1\" must be a directory, like the "
            + "flattened column")
        .isTrue();
  }

  /**
   * Verify a metadata map with multiple pairs, one of them null-valued, is read
   * without misclassification or NPE. A null-valued {@code hdi_isfolder} must
   * not flag the entry as a directory (exact-key hit with a null value), and the
   * whole map - including the null value - must be surfaced verbatim.
   */
  @Test
  public void testMetadataMapWithNullValueAndMultiplePairs() throws Exception {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(XML_TAG_HDI_ISFOLDER, null);
    metadata.put("custom", "value");
    byte[] stream = new BlobEndpointStreamBuilder()
        .addRow("file.txt", "0x8DEE", CONTENT_LENGTH_SAMPLE,
            "2026-07-13T07:06:45", "2026-07-13T07:06:45", "blob", metadata)
        .build();

    BlobListResultSchema result = parse(stream);

    BlobListResultEntrySchema entry = result.paths().get(0);
    assertThat(entry.isDirectory())
        .as("a null-valued hdi_isfolder must not classify the entry as a "
            + "directory")
        .isFalse();
    Map<String, String> expected = new HashMap<>();
    expected.put(XML_TAG_HDI_ISFOLDER, null);
    expected.put("custom", "value");
    assertThat(entry.metadata()).isEqualTo(expected);
  }

  /**
   * Verify a stream with two record batches carrying a {@code Metadata} map
   * column and a native {@code TimeStampSec} column is fully drained and the
   * column references resolved once before the loop stay valid across batches -
   * values from both batches (including metadata and normalized timestamps) must
   * be read correctly.
   */
  @Test
  public void testTwoBatchesWithMetadataAndTimestampColumns() throws Exception {
    byte[] stream = buildTwoBatchMetadataTimestampStream();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(3);
    // Batch 1.
    assertThat(result.paths().get(0).name()).isEqualTo("dirA");
    assertThat(result.paths().get(0).isDirectory())
        .as("batch-1 marker must be a directory via its Metadata map")
        .isTrue();
    assertThat(result.paths().get(0).lastModified())
        .isEqualTo(rfc1123Utc("2026-07-13T07:06:45"));
    assertThat(result.paths().get(1).name()).isEqualTo("fileB.txt");
    assertThat(result.paths().get(1).isDirectory()).isFalse();
    // Batch 2 - proves the resolve-once vectors stayed valid across batches.
    assertThat(result.paths().get(2).name()).isEqualTo("dirC");
    assertThat(result.paths().get(2).isDirectory())
        .as("batch-2 marker must still be read via the same Metadata vector")
        .isTrue();
    assertThat(result.paths().get(2).lastModified())
        .isEqualTo(rfc1123Utc("2026-07-14T08:09:10"));
  }

  /**
   * Verify {@code CopyCompletionTime} delivered as a native Arrow timestamp (as
   * the real Blob endpoint sends it, like Last-Modified) is read through
   * {@code readTimestampAsRfc1123()} and parsed to the same epoch the XML path
   * yields from the equivalent RFC 1123 string.
   */
  @Test
  public void testCopyCompletionTimeNativeTimestamp() throws Exception {
    byte[] stream = buildCopyCompletionTimestampStream("copied.txt",
        "2026-07-13T07:06:45");

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths().get(0).copyCompletionTime())
        .as("native Arrow CopyCompletionTime must parse to the XML epoch")
        .isEqualTo(DateTimeUtils.parseLastModifiedTime(
            rfc1123Utc("2026-07-13T07:06:45")));
  }

  /**
   * Verify a record batch of zero rows - what the service sends for an empty
   * container - returns an empty listing (as opposed to the schema-only,
   * no-batch stream covered by {@link #testEmptyResponse()}).
   */
  @Test
  public void testEmptyBatchZeroRows() throws Exception {
    byte[] stream = buildEmptyBatchStream();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).isEmpty();
  }

  /**
   * Verify an empty listing whose zero-row batch carries no {@code Name} column
   * returns an empty result instead of tripping the missing-Name-column guard.
   * The service can omit the data columns when there are no rows; treating that
   * as a parse error would wrongly fail a legitimate empty-directory listing,
   * whereas the XML path returns an empty {@code EnumerationResults} without
   * error. The guard must fire only when a batch actually carries rows (see
   * {@link #testMissingMandatoryNameColumn()}).
   */
  @Test
  public void testEmptyResponseWithoutNameColumnReturnsEmpty() throws Exception {
    byte[] stream = buildEmptyStreamWithoutNameColumn();

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).isEmpty();
    assertThat(result.getNextMarker()).isNull();
  }

  /**
   * Verify a whitespace-only continuation token is normalized to {@code null}
   * rather than echoed back as a continuation cursor (which would drive a
   * spurious extra page request), matching the empty-token normalization.
   */
  @Test
  public void testWhitespaceContinuationTokenNormalizedToNull() throws Exception {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "file.txt");
    byte[] stream = buildStringColumnStream(
        new ArrayList<Map<String, String>>() {{
          add(row);
        }}, "   ");

    BlobListResultSchema result = parse(stream);

    assertThat(result.paths()).hasSize(1);
    assertThat(result.getNextMarker())
        .as("a whitespace-only marker must be normalized to null")
        .isNull();
  }

  /**
   * Parse a single-row string-column stream whose {@code Last-Modified} column
   * carries the given ISO date-time value and return the normalized
   * {@code lastModified()} of the resulting entry.
   */
  private String lastModifiedFor(String isoDateTime) throws IOException {
    Map<String, String> row = new LinkedHashMap<>();
    row.put(ARROW_COL_NAME, "file.txt");
    row.put(ARROW_COL_LAST_MODIFIED, isoDateTime);
    byte[] stream = buildStringColumnStream(
        new ArrayList<Map<String, String>>() {{
          add(row);
        }}, null);
    return parse(stream).paths().get(0).lastModified();
  }

  /**
   * Format an ISO local date-time (interpreted as UTC) into the RFC 1123 GMT
   * string using the JDK, providing an independent oracle for the parser's
   * hand-rolled formatting.
   */
  private static String rfc1123Utc(String isoLocalDateTime) {
    return DateTimeFormatter
        .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
        .withZone(ZoneOffset.UTC)
        .format(LocalDateTime.parse(isoLocalDateTime).toInstant(ZoneOffset.UTC));
  }

  private BlobListResultSchema parse(byte[] stream) throws IOException {
    try (InputStream in = new ByteArrayInputStream(stream)) {
      return new ArrowListBlobParser(URL, MEMORY_LIMIT).parse(in);
    }
  }

  /**
   * Builder that produces an Arrow IPC stream with the strongly-typed columns
   * (Name, Etag, Content-Length, Last-Modified, Creation-Time, IsDirectory)
   * used by ListBlobs, optionally carrying a NextMarker in the schema metadata.
   */
  private static final class ArrowStreamBuilder {
    private final List<Object[]> rows = new ArrayList<>();
    private String nextMarker;

    ArrowStreamBuilder addRow(String name, String etag, long contentLength,
        String lastModified, String creationTime, boolean isDirectory) {
      rows.add(new Object[]{name, etag, contentLength, lastModified,
          creationTime, isDirectory});
      return this;
    }

    ArrowStreamBuilder withNextMarker(String marker) {
      this.nextMarker = marker;
      return this;
    }

    byte[] build() throws IOException {
      List<Field> fields = new ArrayList<>();
      fields.add(new Field(ARROW_COL_NAME,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(new Field(ARROW_COL_ETAG,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(new Field(ARROW_COL_CONTENT_LENGTH,
          FieldType.nullable(new ArrowType.Int(ARROW_INT_BIT_WIDTH, true)), null));
      fields.add(new Field(ARROW_COL_LAST_MODIFIED,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(new Field(ARROW_COL_CREATION_TIME,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(new Field(ARROW_COL_IS_DIRECTORY,
          FieldType.nullable(new ArrowType.Bool()), null));

      Map<String, String> metadata = new HashMap<>();
      if (nextMarker != null) {
        metadata.put(ARROW_METADATA_NEXT_MARKER, nextMarker);
      }
      Schema schema = new Schema(fields, metadata);

      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
              Channels.newChannel(out))) {
        VarCharVector name = (VarCharVector) root.getVector(ARROW_COL_NAME);
        VarCharVector etag = (VarCharVector) root.getVector(ARROW_COL_ETAG);
        BigIntVector len = (BigIntVector) root.getVector(ARROW_COL_CONTENT_LENGTH);
        VarCharVector lm = (VarCharVector) root.getVector(ARROW_COL_LAST_MODIFIED);
        VarCharVector ct = (VarCharVector) root.getVector(ARROW_COL_CREATION_TIME);
        BitVector dir = (BitVector) root.getVector(ARROW_COL_IS_DIRECTORY);

        int count = rows.size();
        name.allocateNew(count);
        etag.allocateNew(count);
        len.allocateNew(count);
        lm.allocateNew(count);
        ct.allocateNew(count);
        dir.allocateNew(count);

        for (int i = 0; i < count; i++) {
          Object[] r = rows.get(i);
          name.setSafe(i, ((String) r[0]).getBytes(StandardCharsets.UTF_8));
          etag.setSafe(i, ((String) r[1]).getBytes(StandardCharsets.UTF_8));
          len.setSafe(i, (Long) r[2]);
          setVarCharOrNull(lm, i, (String) r[3]);
          setVarCharOrNull(ct, i, (String) r[4]);
          dir.setSafe(i, ((Boolean) r[5]) ? 1 : 0);
        }
        root.setRowCount(count);

        writer.start();
        if (count > 0) {
          writer.writeBatch();
        }
        writer.end();
        return out.toByteArray();
      }
    }

    private static void setVarCharOrNull(VarCharVector vector, int index,
        String value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Build an Arrow IPC stream where every provided column is an all-string
   * (VarChar) column. Useful for testing unknown/missing columns.
   *
   * @param rows list of column-name to value maps (one map per row). All rows
   *             must share the same key set.
   * @param nextMarker optional continuation token (may be {@code null}).
   */
  private static byte[] buildStringColumnStream(List<Map<String, String>> rows,
      String nextMarker) throws IOException {
    List<String> columns = new ArrayList<>(rows.get(0).keySet());
    List<Field> fields = new ArrayList<>();
    for (String column : columns) {
      fields.add(new Field(column, FieldType.nullable(new ArrowType.Utf8()),
          null));
    }
    Map<String, String> metadata = new HashMap<>();
    if (nextMarker != null) {
      metadata.put(ARROW_METADATA_NEXT_MARKER, nextMarker);
    }
    Schema schema = new Schema(fields, metadata);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      int count = rows.size();
      for (String column : columns) {
        ((VarCharVector) root.getVector(column)).allocateNew(count);
      }
      for (int i = 0; i < count; i++) {
        for (String column : columns) {
          String value = rows.get(i).get(column);
          VarCharVector vector = (VarCharVector) root.getVector(column);
          if (value == null) {
            vector.setNull(i);
          } else {
            vector.setSafe(i, new Text(value));
          }
        }
      }
      root.setRowCount(count);
      writer.start();
      if (count > 0) {
        writer.writeBatch();
      }
      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Builds an Arrow IPC stream that mirrors the real Blob-endpoint ListBlobs
   * response shape: {@code Content-Length} as an unsigned 64-bit integer
   * ({@code UInt8}), {@code Creation-Time}/{@code Last-Modified} as native
   * {@code TimeStampSec} vectors, and blob user metadata (including the
   * {@code hdi_isfolder} directory marker) inside a single {@code Metadata}
   * map column of {@code struct<key, value>} entries.
   */
  private static final class BlobEndpointStreamBuilder {
    private final List<Object[]> rows = new ArrayList<>();

    BlobEndpointStreamBuilder addRow(String name, String etag,
        long contentLength, String lastModifiedIso, String creationTimeIso,
        String resourceType, Map<String, String> metadata) {
      rows.add(new Object[]{name, etag, contentLength, lastModifiedIso,
          creationTimeIso, resourceType, metadata});
      return this;
    }

    byte[] build() throws IOException {
      Field keyField = new Field(MapVector.KEY_NAME,
          FieldType.notNullable(new ArrowType.Utf8()), null);
      Field valueField = new Field(MapVector.VALUE_NAME,
          FieldType.nullable(new ArrowType.Utf8()), null);
      Field entriesField = new Field(MapVector.DATA_VECTOR_NAME,
          FieldType.notNullable(new ArrowType.Struct()),
          Arrays.asList(keyField, valueField));
      Field metadataField = new Field(ARROW_COL_METADATA,
          new FieldType(true, new ArrowType.Map(false), null),
          Collections.singletonList(entriesField));

      List<Field> fields = new ArrayList<>();
      fields.add(new Field(ARROW_COL_NAME,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(new Field(ARROW_COL_ETAG,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(new Field(ARROW_COL_CONTENT_LENGTH,
          FieldType.nullable(new ArrowType.Int(ARROW_INT_BIT_WIDTH, false)), null));
      fields.add(new Field(ARROW_COL_LAST_MODIFIED,
          FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)),
          null));
      fields.add(new Field(ARROW_COL_CREATION_TIME,
          FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)),
          null));
      fields.add(new Field(ARROW_COL_RESOURCE_TYPE,
          FieldType.nullable(new ArrowType.Utf8()), null));
      fields.add(metadataField);
      Schema schema = new Schema(fields);

      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
              Channels.newChannel(out))) {
        VarCharVector name = (VarCharVector) root.getVector(ARROW_COL_NAME);
        VarCharVector etag = (VarCharVector) root.getVector(ARROW_COL_ETAG);
        UInt8Vector len =
            (UInt8Vector) root.getVector(ARROW_COL_CONTENT_LENGTH);
        TimeStampSecVector lm =
            (TimeStampSecVector) root.getVector(ARROW_COL_LAST_MODIFIED);
        TimeStampSecVector ct =
            (TimeStampSecVector) root.getVector(ARROW_COL_CREATION_TIME);
        VarCharVector rt =
            (VarCharVector) root.getVector(ARROW_COL_RESOURCE_TYPE);
        MapVector metadata = (MapVector) root.getVector(ARROW_COL_METADATA);
        UnionMapWriter mapWriter = metadata.getWriter();

        int count = rows.size();
        for (int i = 0; i < count; i++) {
          Object[] r = rows.get(i);
          name.setSafe(i, ((String) r[0]).getBytes(StandardCharsets.UTF_8));
          etag.setSafe(i, ((String) r[1]).getBytes(StandardCharsets.UTF_8));
          len.setSafe(i, (Long) r[2]);
          lm.setSafe(i, toEpochSeconds((String) r[3]));
          ct.setSafe(i, toEpochSeconds((String) r[4]));
          rt.setSafe(i, ((String) r[5]).getBytes(StandardCharsets.UTF_8));

          @SuppressWarnings("unchecked")
          Map<String, String> entries = (Map<String, String>) r[6];
          mapWriter.setPosition(i);
          mapWriter.startMap();
          for (Map.Entry<String, String> e : entries.entrySet()) {
            mapWriter.startEntry();
            writeVarChar(allocator, mapWriter.key().varChar(), e.getKey());
            // The value field is nullable; leave it unwritten (null) when the
            // test supplies a null value so the null-metadata path is exercised.
            if (e.getValue() != null) {
              writeVarChar(allocator, mapWriter.value().varChar(), e.getValue());
            }
            mapWriter.endEntry();
          }
          mapWriter.endMap();
        }
        mapWriter.setValueCount(count);
        root.setRowCount(count);

        writer.start();
        if (count > 0) {
          writer.writeBatch();
        }
        writer.end();
        return out.toByteArray();
      }
    }

    private static long toEpochSeconds(String iso) {
      return LocalDateTime.parse(iso).toEpochSecond(ZoneOffset.UTC);
    }

    private static void writeVarChar(BufferAllocator allocator,
        org.apache.arrow.vector.complex.writer.VarCharWriter writer,
        String value) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      try (ArrowBuf buffer = allocator.buffer(bytes.length)) {
        buffer.setBytes(0, bytes);
        writer.writeVarChar(0, bytes.length, buffer);
      }
    }
  }

  /**
   * Build a single-row Arrow stream whose {@code Last-Modified} column is a
   * timezone-aware millisecond timestamp vector, whose {@code getObject()}
   * returns a raw epoch-millis {@link Long} rather than a local date-time.
   */
  private static byte[] buildTimeZoneTimestampStream(String name,
      long epochMillis) throws IOException {
    Field nameField = new Field(ARROW_COL_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Field lmField = new Field(ARROW_COL_LAST_MODIFIED,
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
        null);
    Schema schema = new Schema(Arrays.asList(nameField, lmField));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      VarCharVector nameVector =
          (VarCharVector) root.getVector(ARROW_COL_NAME);
      TimeStampMilliTZVector lmVector =
          (TimeStampMilliTZVector) root.getVector(ARROW_COL_LAST_MODIFIED);
      nameVector.allocateNew(1);
      lmVector.allocateNew(1);
      nameVector.setSafe(0, name.getBytes(StandardCharsets.UTF_8));
      lmVector.setSafe(0, epochMillis);
      root.setRowCount(1);
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Build a single Arrow stream containing two record batches (2 rows then 1
   * row) sharing one {@code Name} column, reusing the same
   * {@link VectorSchemaRoot} across batches as {@code ArrowStreamReader} does on
   * read.
   */
  private static byte[] buildTwoBatchNameStream() throws IOException {
    Field nameField = new Field(ARROW_COL_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(Collections.singletonList(nameField));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      VarCharVector name = (VarCharVector) root.getVector(ARROW_COL_NAME);
      writer.start();

      name.allocateNew(2);
      name.setSafe(0, "batch1-a".getBytes(StandardCharsets.UTF_8));
      name.setSafe(1, "batch1-b".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(2);
      writer.writeBatch();

      name.reset();
      name.allocateNew(1);
      name.setSafe(0, "batch2-a".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(1);
      writer.writeBatch();

      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Build a schema-only stream that nonetheless emits a record batch of zero
   * rows (the shape the service returns for an empty container), as opposed to a
   * stream with no batch at all.
   */
  private static byte[] buildEmptyBatchStream() throws IOException {
    Field nameField = new Field(ARROW_COL_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(Collections.singletonList(nameField));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      VarCharVector name = (VarCharVector) root.getVector(ARROW_COL_NAME);
      name.allocateNew(0);
      root.setRowCount(0);
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Build a zero-row stream whose schema carries no {@code Name} column at all -
   * the shape the service can send for an empty listing, where there is no data
   * and hence no populated columns. The parser must return an empty result
   * rather than failing the "missing Name column" guard, since that guard is
   * meant to catch a malformed response that actually carries rows.
   */
  private static byte[] buildEmptyStreamWithoutNameColumn() throws IOException {
    Field etagField = new Field(ARROW_COL_ETAG,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(Collections.singletonList(etagField));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      VarCharVector etag = (VarCharVector) root.getVector(ARROW_COL_ETAG);
      etag.allocateNew(0);
      root.setRowCount(0);
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Build a single-row stream whose {@code Copy-Completion-Time} column is a
   * native {@code TimeStampSec} vector (the shape the real Blob endpoint sends),
   * rather than a VarChar RFC 1123 string.
   */
  private static byte[] buildCopyCompletionTimestampStream(String name,
      String completionIso) throws IOException {
    Field nameField = new Field(ARROW_COL_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Field copyCompletionField = new Field(ARROW_COL_COPY_COMPLETION_TIME,
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)), null);
    Schema schema = new Schema(Arrays.asList(nameField, copyCompletionField));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      VarCharVector nameVector =
          (VarCharVector) root.getVector(ARROW_COL_NAME);
      TimeStampSecVector copyCompletion =
          (TimeStampSecVector) root.getVector(ARROW_COL_COPY_COMPLETION_TIME);
      nameVector.allocateNew(1);
      copyCompletion.allocateNew(1);
      nameVector.setSafe(0, name.getBytes(StandardCharsets.UTF_8));
      copyCompletion.setSafe(0,
          LocalDateTime.parse(completionIso).toEpochSecond(ZoneOffset.UTC));
      root.setRowCount(1);
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Build a single stream carrying two record batches, each with a {@code Name}
   * column, a native {@code TimeStampSec} {@code Last-Modified} column and a
   * {@code Metadata} map column. Reuses one {@link VectorSchemaRoot} across
   * batches as {@code ArrowStreamReader} does on read, so it exercises the
   * value-count guards on the metadata and timestamp vectors across batches.
   */
  private static byte[] buildTwoBatchMetadataTimestampStream()
      throws IOException {
    Field keyField = new Field(MapVector.KEY_NAME,
        FieldType.notNullable(new ArrowType.Utf8()), null);
    Field valueField = new Field(MapVector.VALUE_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Field entriesField = new Field(MapVector.DATA_VECTOR_NAME,
        FieldType.notNullable(new ArrowType.Struct()),
        Arrays.asList(keyField, valueField));
    Field metadataField = new Field(ARROW_COL_METADATA,
        new FieldType(true, new ArrowType.Map(false), null),
        Collections.singletonList(entriesField));
    Field nameField = new Field(ARROW_COL_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Field lmField = new Field(ARROW_COL_LAST_MODIFIED,
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)), null);
    Schema schema = new Schema(Arrays.asList(nameField, lmField, metadataField));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
            Channels.newChannel(out))) {
      writer.start();
      Map<String, String> marker = Collections.singletonMap(
          XML_TAG_HDI_ISFOLDER, "true");

      writeMetadataTimestampBatch(allocator, root, Arrays.asList(
          new Object[]{"dirA", "2026-07-13T07:06:45", marker},
          new Object[]{"fileB.txt", "2026-07-13T07:06:46",
              Collections.<String, String>emptyMap()}));
      writer.writeBatch();

      writeMetadataTimestampBatch(allocator, root, Collections.singletonList(
          new Object[]{"dirC", "2026-07-14T08:09:10", marker}));
      writer.writeBatch();

      writer.end();
      return out.toByteArray();
    }
  }

  /**
   * Populate the {@code Name}, {@code Last-Modified} (TimeStampSec) and
   * {@code Metadata} map vectors of {@code root} for one record batch, resetting
   * them first so the same root can be reused across batches.
   */
  private static void writeMetadataTimestampBatch(BufferAllocator allocator,
      VectorSchemaRoot root, List<Object[]> rows) {
    VarCharVector name = (VarCharVector) root.getVector(ARROW_COL_NAME);
    TimeStampSecVector lm =
        (TimeStampSecVector) root.getVector(ARROW_COL_LAST_MODIFIED);
    MapVector metadata = (MapVector) root.getVector(ARROW_COL_METADATA);
    name.reset();
    lm.reset();
    metadata.reset();

    int count = rows.size();
    name.allocateNew(count);
    lm.allocateNew(count);
    UnionMapWriter mapWriter = metadata.getWriter();
    for (int i = 0; i < count; i++) {
      Object[] r = rows.get(i);
      name.setSafe(i, ((String) r[0]).getBytes(StandardCharsets.UTF_8));
      lm.setSafe(i, LocalDateTime.parse((String) r[1])
          .toEpochSecond(ZoneOffset.UTC));
      @SuppressWarnings("unchecked")
      Map<String, String> entries = (Map<String, String>) r[2];
      mapWriter.setPosition(i);
      mapWriter.startMap();
      for (Map.Entry<String, String> e : entries.entrySet()) {
        mapWriter.startEntry();
        writeVarChar(allocator, mapWriter.key().varChar(), e.getKey());
        if (e.getValue() != null) {
          writeVarChar(allocator, mapWriter.value().varChar(), e.getValue());
        }
        mapWriter.endEntry();
      }
      mapWriter.endMap();
    }
    mapWriter.setValueCount(count);
    root.setRowCount(count);
  }

  /**
   * Write a UTF-8 value into a map key/value {@code VarCharWriter}, staging the
   * bytes in a temporary {@link ArrowBuf}.
   */
  private static void writeVarChar(BufferAllocator allocator,
      org.apache.arrow.vector.complex.writer.VarCharWriter writer,
      String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    try (ArrowBuf buffer = allocator.buffer(bytes.length)) {
      buffer.setBytes(0, bytes);
      writer.writeVarChar(0, bytes.length, buffer);
    }
  }
}
