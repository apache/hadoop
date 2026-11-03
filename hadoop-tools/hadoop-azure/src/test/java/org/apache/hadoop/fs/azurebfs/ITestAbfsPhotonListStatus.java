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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_FALLBACK_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_LISTING_LATENCY;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_PARSE_FAILURE_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_REQUEST_COUNT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.PHOTON_RESPONSE_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_PHOTON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Live integration tests for the Photon (Apache Arrow based ListBlob) listing
 * path against a real Blob-endpoint account. These cover the integration
 * scenarios from the design document that are not verifiable with unit tests:
 * <ul>
 *   <li>XML-vs-Photon {@link FileStatus} parity (identical results regardless of
 *   the wire format).</li>
 *   <li>Graceful XML fallback when Arrow is requested but the account returns
 *   XML (asserted via the Photon telemetry classification).</li>
 *   <li>Pagination across multiple Photon responses.</li>
 *   <li>Photon telemetry counters and listing-latency tracker are emitted.</li>
 * </ul>
 *
 * <p>Photon is only offered on the Blob endpoint, so every test asserts a Blob
 * service type before running.</p>
 */
public class ITestAbfsPhotonListStatus extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsPhotonListStatus.class);

  private static final String[] CHILD_FILES = {
      "file-a.txt",
      "file-b.txt",
      "name with space.txt",
      "\u6587\u4ef6-unicode.txt",
  };

  private static final String CHILD_DIR = "subdir";

  public ITestAbfsPhotonListStatus() throws Exception {
  }

  /**
   * Create a filesystem instance with Photon explicitly toggled and, optionally,
   * a reduced ListBlobs page size to force multi-page pagination.
   */
  private AzureBlobFileSystem newFileSystem(final boolean photonEnabled,
      final int listMaxResults) throws IOException {
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(FS_AZURE_ENABLE_PHOTON, photonEnabled);
    if (listMaxResults > 0) {
      conf.setInt(AZURE_LIST_MAX_RESULTS, listMaxResults);
    }
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  /**
   * Populate a directory with a mix of files (including special-character and
   * Unicode names) and a subdirectory, so a listing exercises files and
   * directories together.
   */
  private void createTree(final AzureBlobFileSystem fs, final Path baseDir)
      throws IOException {
    fs.mkdirs(baseDir);
    for (int i = 0; i < CHILD_FILES.length; i++) {
      try (FSDataOutputStream out = fs.create(new Path(baseDir, CHILD_FILES[i]))) {
        // Give the files distinct, non-zero sizes to make parity meaningful.
        out.write(new byte[i + 1]);
      }
    }
    fs.mkdirs(new Path(baseDir, CHILD_DIR));
  }

  private static List<FileStatus> sortedByName(final FileStatus[] statuses) {
    return Arrays.stream(statuses)
        .sorted(Comparator.comparing(s -> s.getPath().getName()))
        .collect(Collectors.toList());
  }

  /**
   * Verify that listing the same directory over XML (Photon disabled) and over
   * Arrow (Photon enabled) yields identical {@link FileStatus} results, covering
   * the doc's XML/Photon parity and identical-FileStatus scenarios.
   */
  @Test
  public void testPhotonAndXmlListingParity() throws Exception {
    assumeBlobServiceType();
    Path baseDir = path("photonParity-" + getMethodName());
    createTree(getFileSystem(), baseDir);

    try (AzureBlobFileSystem xmlFs = newFileSystem(false, -1);
         AzureBlobFileSystem photonFs = newFileSystem(true, -1)) {

      List<FileStatus> xmlStatuses = sortedByName(xmlFs.listStatus(baseDir));
      List<FileStatus> photonStatuses = sortedByName(photonFs.listStatus(baseDir));

      assertThat(photonStatuses)
          .as("Photon and XML listings must return the same number of entries")
          .hasSameSizeAs(xmlStatuses);

      for (int i = 0; i < xmlStatuses.size(); i++) {
        FileStatus xml = xmlStatuses.get(i);
        FileStatus photon = photonStatuses.get(i);
        assertThat(photon.getPath())
            .as("path parity for entry %d", i)
            .isEqualTo(xml.getPath());
        assertThat(photon.isDirectory())
            .as("isDirectory parity for %s", xml.getPath())
            .isEqualTo(xml.isDirectory());
        assertThat(photon.getLen())
            .as("length parity for %s", xml.getPath())
            .isEqualTo(xml.getLen());
        // Modification-time parity is the only end-to-end guard on the
        // hand-written fastIsoUtcToRfc1123() / Sakamoto weekday math. Allow a
        // second of tolerance in case the XML and Arrow paths differ in
        // sub-second precision.
        assertThat(photon.getModificationTime())
            .as("modification time parity for %s", xml.getPath())
            .isCloseTo(xml.getModificationTime(), within(1000L));
      }
    }
  }

  /**
   * Verify that a recursive {@code listFiles} over an empty directory (one whose
   * only child is an empty sub-directory marker blob) returns no files on both
   * the XML and Photon (Arrow) paths. This is the exact scenario behind
   * {@code testListFilesEmptyDirectoryRecursive}: the Arrow parser must classify
   * the {@code hdi_isfolder=true} marker blob as a directory - not a file - so
   * the recursive listing yields zero files, matching XML.
   */
  @Test
  public void testPhotonRecursiveListFilesEmptyDirectoryParity() throws Exception {
    assumeBlobServiceType();
    Path baseDir = path("photonEmptyRecursive-" + getMethodName());
    FileSystem fs = getFileSystem();
    fs.mkdirs(baseDir);
    // The only entry under baseDir is an empty sub-directory marker blob.
    fs.mkdirs(new Path(baseDir, CHILD_DIR));

    try (AzureBlobFileSystem xmlFs = newFileSystem(false, -1);
         AzureBlobFileSystem photonFs = newFileSystem(true, -1)) {

      List<Path> xmlFiles = recursiveFilePaths(xmlFs, baseDir);
      List<Path> photonFiles = recursiveFilePaths(photonFs, baseDir);

      assertThat(xmlFiles)
          .as("XML recursive listFiles over an empty directory must find no files")
          .isEmpty();
      assertThat(photonFiles)
          .as("Photon recursive listFiles must match XML and find no files")
          .isEqualTo(xmlFiles);
    }
  }

  /**
   * Collect the paths returned by a recursive {@code listFiles}, sorted for a
   * stable comparison.
   */
  private static List<Path> recursiveFilePaths(final FileSystem fs,
      final Path dir) throws IOException {
    List<Path> paths = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, true);
    while (it.hasNext()) {
      paths.add(it.next().getPath());
    }
    paths.sort(Comparator.naturalOrder());
    return paths;
  }

  /**
   * Verify that a Photon-enabled listing emits the telemetry counters and the
   * listing-latency tracker, that every request is classified as exactly one of
   * Arrow response or XML fallback (covering graceful fallback), that pagination
   * across multiple Photon responses works, and that no parse failures occur.
   */
  @Test
  public void testPhotonListingMetricsEmitted() throws Exception {
    assumeBlobServiceType();
    Path baseDir = path("photonMetrics-" + getMethodName());
    createTree(getFileSystem(), baseDir);

    // Force small pages so the listing spans multiple Photon responses. With
    // CHILD_FILES.length files plus one subdirectory listed PAGE_SIZE at a time,
    // the service must return exactly ceil(entries / PAGE_SIZE) pages.
    final int pageSize = 2;
    final int totalEntries = CHILD_FILES.length + 1;
    final int expectedPages = (totalEntries + pageSize - 1) / pageSize;
    try (AzureBlobFileSystem photonFs = newFileSystem(true, pageSize)) {
      FileStatus[] statuses = photonFs.listStatus(baseDir);
      assertThat(statuses)
          .as("listing must return every child")
          .hasSize(totalEntries);

      Map<String, Long> metrics = photonFs.getInstrumentationMap();
      assertThat(metrics.keySet())
          .as("Photon counters must be registered")
          .contains(PHOTON_REQUEST_COUNT.getStatName(),
              PHOTON_RESPONSE_COUNT.getStatName(),
              PHOTON_FALLBACK_COUNT.getStatName(),
              PHOTON_PARSE_FAILURE_COUNT.getStatName());
      long requests = metrics.getOrDefault(PHOTON_REQUEST_COUNT.getStatName(), -1L);
      long responses = metrics.getOrDefault(PHOTON_RESPONSE_COUNT.getStatName(), -1L);
      long fallbacks = metrics.getOrDefault(PHOTON_FALLBACK_COUNT.getStatName(), -1L);
      long parseFailures =
          metrics.getOrDefault(PHOTON_PARSE_FAILURE_COUNT.getStatName(), -1L);

      assertThat(requests)
          .as("Photon must be requested once per page")
          .isEqualTo(expectedPages);
      assertThat(responses + fallbacks)
          .as("every Photon request must be classified as Arrow or XML fallback")
          .isEqualTo(requests);
      assertThat(parseFailures)
          .as("no Arrow parse failures expected on a healthy listing")
          .isZero();
      assertThat(metrics.keySet())
          .as("Photon listing-latency tracker must be emitted")
          .anyMatch(key -> key.startsWith(PHOTON_LISTING_LATENCY.getStatName()));

      if (fallbacks > 0) {
        LOG.info("Account returned XML fallback for {} of {} Photon requests; "
            + "parity is still guaranteed by testPhotonAndXmlListingParity.",
            fallbacks, requests);
      }
    }
  }
}
