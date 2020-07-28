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

package org.apache.hadoop.fs.s3a.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.impl.DirMarkerTracker;
import org.apache.hadoop.fs.s3a.impl.OperationCallbacks;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.OperationDuration;

import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * Handle directory-related command-line options in the
 * s3guard tool.
 * <pre>
 *   scan: scan for markers
 *   clean: clean up marker entries.
 * </pre>
 * This tool does not go anywhere near S3Guard; its scan bypasses any
 * metastore as we are explicitly looking for marker objects.
 *
 */
public final class MarkerTool extends S3GuardTool {

  private static final Logger LOG =
      LoggerFactory.getLogger(MarkerTool.class);

  public static final String NAME = "markers";

  public static final String PURPOSE =
      "view and manipulate S3 directory markers";

  private static final String USAGE = NAME
      + " [-verbose] [-expected <count>]"
      + " (audit || report || clean)"
//      + " [-out <path>]"
      + " [-" + VERBOSE + "]"
      + " <PATH>\n"
      + "\t" + PURPOSE + "\n\n";

  public static final String OPT_EXPECTED = "expected";

  public static final String AUDIT = "audit";

  public static final String CLEAN = "clean";

  public static final String REPORT = "report";

  public static final String OPT_OUTPUT = "output";

  public static final String OPT_VERBOSE = "verbose";

  static final String TOO_FEW_ARGUMENTS = "Too few arguments";

  /** Will be overridden in run(), but during tests needs to avoid NPEs. */
  private PrintStream out = System.out;

  private boolean verbose;

  private boolean purge;

  private int expected;

  private OperationCallbacks operationCallbacks;

  private StoreContext storeContext;

  public MarkerTool(final Configuration conf) {
    super(conf,
        OPT_VERBOSE
    );
    getCommandFormat().addOptionWithValue(OPT_EXPECTED);
//    getCommandFormat().addOptionWithValue(OPT_OUTPUT);
  }

  @Override
  public String getUsage() {
    return USAGE;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void resetBindings() {
    super.resetBindings();
    storeContext = null;
    operationCallbacks = null;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public int run(final String[] args, final PrintStream stream)
      throws ExitUtil.ExitException, Exception {
    this.out = stream;
    final List<String> parsedArgs;
    try {
      parsedArgs = parseArgs(args);
    } catch (CommandFormat.UnknownOptionException e) {
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE, e.getMessage(), e);
    }
    if (parsedArgs.size() < 2) {
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE, TOO_FEW_ARGUMENTS);
    }
    // read arguments
    CommandFormat commandFormat = getCommandFormat();
    verbose = commandFormat.getOpt(VERBOSE);

    expected = 0;
    // argument 0 is the action
    String action = parsedArgs.get(0);
    switch (action) {
    case AUDIT:
      purge = false;
      expected = 0;
      break;
    case CLEAN:
      purge = true;
      expected = -1;
      break;
    case REPORT:
      purge = false;
      expected = -1;
      break;
    default:
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE,
          "Unknown action: " + action);
    }

    final String file = parsedArgs.get(1);
    final Path path = new Path(file);
    ScanResult result = execute(
        path.getFileSystem(getConf()),
        path,
        purge,
        expected);
    return result.exitCode;
  }

  /**
   * Execute the scan/purge.
   * @param sourceFS source FS; must be or wrap an S3A FS.
   * @param path path to scan.
   * @param doPurge purge?
   * @param expectedMarkerCount expected marker count
   * @return scan+purge result.
   * @throws IOException failure
   */
  @VisibleForTesting
  ScanResult execute(
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount)
      throws IOException {
    S3AFileSystem fs = bindFilesystem(sourceFS);
    storeContext = fs.createStoreContext();
    operationCallbacks = fs.getOperationCallbacks();

    ScanResult result = once("action", path.toString(),
        () -> scan(path, doPurge, expectedMarkerCount));
    if (verbose) {
      dumpFileSystemStatistics(fs);
    }
    return result;
  }

  /**
   * Result of the scan operation.
   */
  static final class ScanResult {

    private int exitCode;

    private DirMarkerTracker tracker;

    private MarkerPurgeSummary purgeSummary;

    @Override
    public String toString() {
      return "ScanResult{" +
          "exitCode=" + exitCode +
          ", tracker=" + tracker +
          ", purgeSummary=" + purgeSummary +
          '}';
    }

    /** Exit code to report. */
    public int getExitCode() {
      return exitCode;
    }

    /** Tracker which did the scan. */
    public DirMarkerTracker getTracker() {
      return tracker;
    }

    /** Summary of purge. Null if none took place. */
    public MarkerPurgeSummary getPurgeSummary() {
      return purgeSummary;
    }
  }

  /**
   * Do the scan.
   * @param path path to scan.
   * @param doPurge purge?
   * @param expectedMarkerCount expected marker count
   * @return scan+purge result.
   * @throws IOException
   * @throws ExitUtil.ExitException
   */
  private ScanResult scan(
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount)
      throws IOException, ExitUtil.ExitException {
    ScanResult result = new ScanResult();

    DirMarkerTracker tracker = new DirMarkerTracker();
    result.tracker = tracker;
    try (DurationInfo ignored =
             new DurationInfo(LOG, "marker scan %s", path)) {
      scanDirectoryTree(path, tracker);
    }
    // scan done. what have we got?
    Map<Path, DirMarkerTracker.Marker> surplusMarkers
        = tracker.getSurplusMarkers();
    Map<Path, DirMarkerTracker.Marker> leafMarkers
        = tracker.getLeafMarkers();
    int size = surplusMarkers.size();
    if (size == 0) {
      println(out, "No surplus directory markers were found under %s", path);
    } else {
      println(out, "Found %d surplus directory marker%s under %s",
          size,
          suffix(size),
          path);

      for (Path markers : surplusMarkers.keySet()) {
        println(out, "    %s", markers);
      }

    }
    if (verbose && !leafMarkers.isEmpty()) {
      println(out, "Found %d empty directory 'leaf' marker%s under %s",
          leafMarkers.size(),
          suffix(leafMarkers.size()),
          path);
      for (Path markers : leafMarkers.keySet()) {
        println(out, "    %s", markers);
      }
      println(out, "These are required to indicate empty directories");
    }
    if (size > expectedMarkerCount) {
      // failure
      println(out, "Expected %d marker%s", expectedMarkerCount, suffix(size));
      result.exitCode = EXIT_NOT_ACCEPTABLE;
      return result;
    }

    if (doPurge) {
      int deletePageSize = storeContext.getConfiguration()
          .getInt(Constants.BULK_DELETE_PAGE_SIZE,
              Constants.BULK_DELETE_PAGE_SIZE_DEFAULT);
      result.purgeSummary = purgeMarkers(tracker,
          deletePageSize);
    }
    result.exitCode = EXIT_SUCCESS;
    return result;
  }

  /**
   * Suffix for plurals.
   * @param size size to generate a suffix for
   * @return "" or "s", depending on size
   */
  private String suffix(final int size) {
    return size == 1 ? "" : "s";
  }

  public static Logger getLOG() {
    return LOG;
  }

  /**
   * Scan a directory tree.
   * @param path path to scan
   * @param tracker tracker to update
   * @throws IOException
   */
  private void scanDirectoryTree(final Path path,
      final DirMarkerTracker tracker) throws IOException {
    RemoteIterator<S3AFileStatus> listing = operationCallbacks
        .listObjects(path, storeContext.pathToKey(path));
    while (listing.hasNext()) {
      S3AFileStatus status = listing.next();
      Path p = status.getPath();
      S3ALocatedFileStatus lfs = new S3ALocatedFileStatus(
          status, null);
      String key = storeContext.pathToKey(p);
      if (status.isDirectory()) {
        if (verbose) {
          println(out, "Directory Marker %s", key);
        }
        LOG.debug("{}", key);
        tracker.markerFound(p,
            key + "/",
            lfs);
      } else {
        tracker.fileFound(p,
            key,
            lfs);
      }
    }

  }


  /**
   * Result of a call of {@link #purgeMarkers(DirMarkerTracker, int)};
   * included in {@link ScanResult} so must share visibility.
   */
  static final class MarkerPurgeSummary {

    /** Number of markers deleted. */
    private int markersDeleted;

    /** Number of delete requests issued. */
    private int deleteRequests;

    /**
     * Total duration of delete requests.
     * If this is ever parallelized, this will
     * be greater than the elapsed time of the
     * operation.
     */
    private long totalDeleteRequestDuration;

    @Override
    public String toString() {
      return "MarkerPurgeSummary{" +
          "markersDeleted=" + markersDeleted +
          ", deleteRequests=" + deleteRequests +
          ", totalDeleteRequestDuration=" + totalDeleteRequestDuration +
          '}';
    }


    int getMarkersDeleted() {
      return markersDeleted;
    }

    int getDeleteRequests() {
      return deleteRequests;
    }

    long getTotalDeleteRequestDuration() {
      return totalDeleteRequestDuration;
    }
  }

  /**
   * Purge the markers.
   * @param tracker tracker with the details
   * @param deletePageSize page size of deletes
   * @return summary
   * @throws MultiObjectDeleteException
   * @throws AmazonClientException
   * @throws IOException
   */
  private MarkerPurgeSummary purgeMarkers(DirMarkerTracker tracker,
      int deletePageSize)
      throws MultiObjectDeleteException, AmazonClientException, IOException {

    MarkerPurgeSummary summary = new MarkerPurgeSummary();
    // we get a map of surplus markers to delete.
    Map<Path, DirMarkerTracker.Marker> markers
        = tracker.getSurplusMarkers();
    int size = markers.size();
    // build a list from the strings in the map
    List<DeleteObjectsRequest.KeyVersion> collect =
        markers.values().stream()
            .map(p -> new DeleteObjectsRequest.KeyVersion(p.getKey()))
            .collect(Collectors.toList());
    // as an array list so .sublist is straightforward
    List<DeleteObjectsRequest.KeyVersion> markerKeys = new ArrayList<>(
        collect);

    // now randomize. Why so? if the list spans multiple S3 partitions,
    // it should reduce the IO load on each part.
    Collections.shuffle(markerKeys);
    int pages = size / deletePageSize;
    if (size % deletePageSize > 0) {
      pages += 1;
    }
    if (verbose) {
      println(out, "%d markers to delete in %d pages of %d keys/page",
          size, pages, deletePageSize);
    }
    int start = 0;
    while (start < size) {
      // end is one past the end of the page
      int end = Math.min(start + deletePageSize, size);
      List<DeleteObjectsRequest.KeyVersion> page = markerKeys.subList(start,
          end);
      List<Path> undeleted = new ArrayList<>();
      // currently no attempt at doing this in pages.
      OperationDuration duration = new OperationDuration();
      operationCallbacks.removeKeys(page, true, undeleted, null, false);
      duration.finished();
      summary.deleteRequests++;
      summary.totalDeleteRequestDuration += duration.value();
      // and move to the start of the next page
      start = end;
    }
    summary.markersDeleted = size;
    return summary;
  }

  /**
   * Dump the filesystem Storage Statistics.
   * @param fs filesystem; can be null
   */
  private void dumpFileSystemStatistics(FileSystem fs) {
    if (fs == null) {
      return;
    }
    println(out, "Storage Statistics");
    StorageStatistics st = fs.getStorageStatistics();
    Iterator<StorageStatistics.LongStatistic> it
        = st.getLongStatistics();
    while (it.hasNext()) {
      StorageStatistics.LongStatistic next = it.next();
      println(out, "%s\t%s", next.getName(), next.getValue());
    }
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(final boolean verbose) {
    this.verbose = verbose;
  }
}
