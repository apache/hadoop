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

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.UnknownStoreException;
import org.apache.hadoop.fs.s3a.impl.DirMarkerTracker;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicy;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_INTERRUPTED;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * Audit and S3 bucket for directory markers.
 * <p></p>
 * This tool does not go anywhere near S3Guard; its scan bypasses any
 * metastore as we are explicitly looking for marker objects.
 */
public final class MarkerTool extends S3GuardTool {

  private static final Logger LOG = LoggerFactory.getLogger(MarkerTool.class);

  /**
   * Name of this tool: {@value}.
   */
  public static final String NAME = "markers";

  /**
   * Purpose of this tool: {@value}.
   */
  public static final String PURPOSE =
      "View and manipulate S3 directory markers";

  /**
   * Audit sub-command: {@value}.
   */
  public static final String OPT_AUDIT = "audit";

  /**
   * Clean Sub-command: {@value}.
   */
  public static final String OPT_CLEAN = "clean";

  /**
   * Audit sub-command: {@value}.
   */
  public static final String AUDIT = "-" + OPT_AUDIT;

  /**
   * Clean Sub-command: {@value}.
   */
  public static final String CLEAN = "-" + OPT_CLEAN;

  /**
   * Expected number of markers to find: {@value}.
   */
  public static final String OPT_EXPECTED = "expected";

  /**
   * Name of a file to save the list of markers to: {@value}.
   */
  public static final String OPT_OUT = "out";

  /**
   * Limit of objects to scan: {@value}.
   */
  public static final String OPT_LIMIT = "limit";

  /**
   * Only consider markers found in non-authoritative paths
   * as failures: {@value}.
   */
  public static final String OPT_NONAUTH = "nonauth";

  /**
   * Error text when too few arguments are found.
   */
  @VisibleForTesting
  static final String E_ARGUMENTS = "Wrong number of arguments: %d";

  /**
   * Constant to use when there is no limit on the number of
   * objects listed: {@value}.
   * <p></p>
   * The value is 0 and not -1 because it allows for the limit to be
   * set on the command line {@code -limit 0}.
   * The command line parser rejects {@code -limit -1} as the -1
   * is interpreted as the (unknown) option "-1".
   */
  public static final int UNLIMITED_LISTING = 0;


  /**
   * Usage string: {@value}.
   */
  private static final String USAGE = NAME
      + " (-" + OPT_AUDIT
      + " | -" + OPT_CLEAN + ")"
      + " [-" + OPT_EXPECTED + " <count>]"
      + " [-" + OPT_OUT + " <filename>]"
      + " [-" + OPT_LIMIT + " <limit>]"
      + " [-" + OPT_NONAUTH + "]"
      + " [-" + VERBOSE + "]"

      + " <PATH>\n"
      + "\t" + PURPOSE + "\n\n";

  /** Will be overridden in run(), but during tests needs to avoid NPEs. */
  private PrintStream out = System.out;

  /**
   * Verbosity flag.
   */
  private boolean verbose;

  /**
   * Store context.
   */
  private StoreContext storeContext;

  /**
   * Operations during the scan.
   */
  private MarkerToolOperations operations;

  /**
   * Constructor.
   * @param conf configuration
   */
  public MarkerTool(final Configuration conf) {
    super(conf,
        OPT_AUDIT,
        OPT_CLEAN,
        VERBOSE,
        OPT_NONAUTH);
    CommandFormat format = getCommandFormat();
    format.addOptionWithValue(OPT_EXPECTED);
    format.addOptionWithValue(OPT_LIMIT);
    format.addOptionWithValue(OPT_OUT);
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
    operations = null;
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
    if (parsedArgs.size() != 1) {
      errorln(getUsage());
      println(out, "Supplied arguments: ["
          + parsedArgs.stream()
          .collect(Collectors.joining(", "))
          + "]");
      throw new ExitUtil.ExitException(EXIT_USAGE,
          String.format(E_ARGUMENTS, parsedArgs.size()));
    }
    // read arguments
    CommandFormat command = getCommandFormat();
    verbose = command.getOpt(VERBOSE);

    // How many markers are expected?
    int expected = 0;
    String value = command.getOptValue(OPT_EXPECTED);
    if (value != null && !value.isEmpty()) {
      expected = Integer.parseInt(value);
    }

    // determine the action
    boolean audit = command.getOpt(OPT_AUDIT);
    boolean clean = command.getOpt(OPT_CLEAN);
    if (audit == clean) {
      // either both are set or neither are set
      // this is equivalent to (not audit xor clean)
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE,
          "Exactly one of " + AUDIT + " and " + CLEAN);
    }
    int limit = UNLIMITED_LISTING;
    value = command.getOptValue(OPT_LIMIT);
    if (value != null && !value.isEmpty()) {
      limit = Integer.parseInt(value);
    }
    final String dir = parsedArgs.get(0);
    Path path = new Path(dir);
    URI uri = path.toUri();
    if (uri.getPath().isEmpty()) {
      // fix up empty URI for better CLI experience
      path = new Path(path, "/");
    }
    FileSystem fs = path.getFileSystem(getConf());
    ScanResult result = execute(
        fs,
        path,
        clean,
        expected,
        limit,
        command.getOpt(OPT_NONAUTH));
    if (verbose) {
      dumpFileSystemStatistics(out);
    }

    // and finally see if the output should be saved to a file
    String saveFile = command.getOptValue(OPT_OUT);
    if (saveFile != null && !saveFile.isEmpty()) {
      println(out, "Saving result to %s", saveFile);
      try (Writer writer = new FileWriter(saveFile)) {
        final List<String> surplus = result.getTracker()
            .getSurplusMarkers()
            .keySet()
            .stream()
            .map(p-> p.toString() + "/")
            .sorted()
            .collect(Collectors.toList());
        IOUtils.writeLines(surplus, "\n", writer);
      }
    }
    return result.exitCode;
  }

  /**
   * Execute the scan/purge.
   * @param sourceFS source FS; must be or wrap an S3A FS.
   * @param path path to scan.
   * @param doPurge purge?
   * @param expectedMarkerCount expected marker count
   * @param limit limit of files to scan; -1 for 'unlimited'
   * @param nonAuth consider only markers in nonauth paths as errors
   * @return scan+purge result.
   * @throws IOException failure
   */
  @VisibleForTesting
  ScanResult execute(
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount,
      final int limit,
      final boolean nonAuth)
      throws IOException {
    S3AFileSystem fs = bindFilesystem(sourceFS);

    // extract the callbacks needed for the rest of the work
    storeContext = fs.createStoreContext();
    operations = fs.createMarkerToolOperations();
    // filesystem policy.
    // if the -nonauth option is set, this is used to filter
    // out surplus markers from the results.
    DirectoryPolicy activePolicy = fs.getDirectoryMarkerPolicy();
    DirectoryPolicy.MarkerPolicy policy = activePolicy
        .getMarkerPolicy();
    println(out, "The directory marker policy of %s is \"%s\"",
        storeContext.getFsURI(),
        policy);
    String authPath = storeContext.getConfiguration()
        .getTrimmed(AUTHORITATIVE_PATH, "");
    if (policy == DirectoryPolicy.MarkerPolicy.Authoritative) {
      // in auth mode, note the auth paths.
      println(out, "Authoritative path list is \"%s\"", authPath);
    }
    // qualify the path
    Path target = path.makeQualified(fs.getUri(),new Path("/"));
    // initial safety check: does the path exist?
    try {
      getFilesystem().getFileStatus(target);
    } catch (UnknownStoreException ex) {
      // bucket doesn't exist.
      // replace the stack trace with an error code.
      throw new ExitUtil.ExitException(EXIT_NOT_FOUND,
          ex.toString(), ex);

    } catch (FileNotFoundException ex) {
      throw new ExitUtil.ExitException(EXIT_NOT_FOUND,
          "Not found: " + target, ex);
    }

    // the default filter policy is that all entries should be deleted
    DirectoryPolicy filterPolicy = nonAuth
        ? activePolicy
        : null;
    ScanResult result = scan(target, doPurge, expectedMarkerCount, limit,
        filterPolicy);
    return result;
  }

  /**
   * Result of the scan operation.
   */
  public static final class ScanResult {

    /**
     * Exit code to return if an exception was not raised.
     */
    private int exitCode;

    /**
     * The tracker.
     */
    private DirMarkerTracker tracker;

    /**
     * Scan summary.
     */
    private MarkerPurgeSummary purgeSummary;

    private ScanResult() {
    }

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
   * Do the scan/purge.
   * @param path path to scan.
   * @param clean purge?
   * @param expectedMarkerCount expected marker count
   * @param limit limit of files to scan; 0 for 'unlimited'
   * @param filterPolicy filter policy on a nonauth scan; may be null
   * @return result.
   * @throws IOException IO failure
   * @throws ExitUtil.ExitException explicitly raised failure
   */
  @Retries.RetryTranslated
  private ScanResult scan(
      final Path path,
      final boolean clean,
      final int expectedMarkerCount,
      final int limit,
      final DirectoryPolicy filterPolicy)
      throws IOException, ExitUtil.ExitException {

    ScanResult result = new ScanResult();

    // Mission Accomplished
    result.exitCode = EXIT_SUCCESS;
    // Now do the work.
    DirMarkerTracker tracker = new DirMarkerTracker(path, true);
    result.tracker = tracker;
    boolean completed;
    try (DurationInfo ignored =
             new DurationInfo(LOG, "marker scan %s", path)) {
      completed = scanDirectoryTree(path, tracker, limit);
    }
    int objectsFound = tracker.getObjectsFound();
    println(out, "Listed %d object%s under %s%n",
        objectsFound,
        suffix(objectsFound),
        path);
    // scan done. what have we got?
    Map<Path, DirMarkerTracker.Marker> surplusMarkers
        = tracker.getSurplusMarkers();
    Map<Path, DirMarkerTracker.Marker> leafMarkers
        = tracker.getLeafMarkers();
    int surplus = surplusMarkers.size();
    if (surplus == 0) {
      println(out, "No surplus directory markers were found under %s", path);
    } else {
      println(out, "Found %d surplus directory marker%s under %s",
          surplus,
          suffix(surplus),
          path);

      for (Path markers : surplusMarkers.keySet()) {
        println(out, "    %s/", markers);
      }
    }
    if (!leafMarkers.isEmpty()) {
      println(out, "Found %d empty directory 'leaf' marker%s under %s",
          leafMarkers.size(),
          suffix(leafMarkers.size()),
          path);
      for (Path markers : leafMarkers.keySet()) {
        println(out, "    %s/", markers);
      }
      println(out, "These are required to indicate empty directories");
    }

    if (clean) {
      // clean: remove the markers, do not worry about their
      // presence when reporting success/failiure
      int deletePageSize = storeContext.getConfiguration()
          .getInt(BULK_DELETE_PAGE_SIZE,
              BULK_DELETE_PAGE_SIZE_DEFAULT);
      result.purgeSummary = purgeMarkers(tracker, deletePageSize);
    } else {
      // this is an audit, so validate the marker count

      if (filterPolicy != null) {
        // if a filter policy is supplied, filter out all markers
        // under the auth path
        List<Path> allowed = tracker.removeAllowedMarkers(filterPolicy);
        int allowedMarkers =  allowed.size();
        println(out, "%nIgnoring %d marker%s in authoritative paths",
            allowedMarkers, suffix(allowedMarkers));
        if (verbose) {
          allowed.forEach(p -> println(out, p.toString()));
        }
        // recalculate the marker size
        surplus = surplusMarkers.size();
      }
      if (surplus > expectedMarkerCount) {
        // failure
        if (expectedMarkerCount > 0) {
          println(out, "Expected %d marker%s", expectedMarkerCount,
              suffix(surplus));
        }
        println(out, "Surplus markers were found -failing audit");

        result.exitCode = EXIT_NOT_ACCEPTABLE;
      }
    }


    // now one little check for whether a limit was reached.
    if (!completed) {
      println(out, "Listing limit reached before completing the scan");
      result.exitCode = EXIT_INTERRUPTED;
    }
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

  /**
   * Scan a directory tree.
   * @param path path to scan
   * @param tracker tracker to update
   * @param limit limit of files to scan; -1 for 'unlimited'
   * @return true if the scan completedly scanned the entire tree
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private boolean scanDirectoryTree(
      final Path path,
      final DirMarkerTracker tracker,
      final int limit) throws IOException {

    int count = 0;
    RemoteIterator<S3AFileStatus> listing = operations
        .listObjects(path, storeContext.pathToKey(path));
    while (listing.hasNext()) {
      count++;
      S3AFileStatus status = listing.next();
      Path statusPath = status.getPath();
      S3ALocatedFileStatus locatedStatus = new S3ALocatedFileStatus(
          status, null);
      String key = storeContext.pathToKey(statusPath);
      if (status.isDirectory()) {
        if (verbose) {
          println(out, "  Directory Marker %s/", key);
        }
        LOG.debug("{}", key);
        tracker.markerFound(statusPath,
            key + "/",
            locatedStatus);
      } else {
        tracker.fileFound(statusPath,
            key,
            locatedStatus);
      }
      if ((count % 1000) == 0) {
        println(out, "Scanned %,d objects", count);
      }
      if (limit > 0 && count >= limit) {
        println(out, "Limit of scan reached - %,d object%s", limit, suffix(limit));
        return false;
      }
    }
    return true;
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
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private MarkerPurgeSummary purgeMarkers(
      final DirMarkerTracker tracker,
      final int deletePageSize)
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
    // build an array list for ease of creating the lists of
    // keys in each page through the subList() method.
    List<DeleteObjectsRequest.KeyVersion> markerKeys =
        new ArrayList<>(collect);

    // now randomize. Why so? if the list spans multiple S3 partitions,
    // it should reduce the IO load on each part.
    Collections.shuffle(markerKeys);
    int pages = size / deletePageSize;
    if (size % deletePageSize > 0) {
      pages += 1;
    }
    if (verbose) {
      println(out, "%n%d marker%s to delete in %d page%s of %d keys/page",
          size, suffix(size),
          pages, suffix(pages),
          deletePageSize);
    }
    DurationInfo durationInfo = new DurationInfo(LOG, "Deleting markers");
    int start = 0;
    while (start < size) {
      // end is one past the end of the page
      int end = Math.min(start + deletePageSize, size);
      List<DeleteObjectsRequest.KeyVersion> page = markerKeys.subList(start,
          end);
      List<Path> undeleted = new ArrayList<>();
      once("Remove S3 Keys",
          tracker.getBasePath().toString(), () ->
              operations.removeKeys(page, true, undeleted, null, false));
      summary.deleteRequests++;
      // and move to the start of the next page
      start = end;
    }
    durationInfo.close();
    summary.totalDeleteRequestDuration = durationInfo.value();
    summary.markersDeleted = size;
    return summary;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(final boolean verbose) {
    this.verbose = verbose;
  }

  /**
   * Execute the marker tool, with no checks on return codes.
   *
   * @param sourceFS filesystem to use
   * @param path path to scan
   * @param doPurge should markers be purged
   * @param expectedMarkers number of markers expected
   * @param limit limit of files to scan; -1 for 'unlimited'
   * @param nonAuth only use nonauth path count for failure rules
   * @return the result
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public static MarkerTool.ScanResult execMarkerTool(
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkers,
      final int limit, boolean nonAuth) throws IOException {
    MarkerTool tool = new MarkerTool(sourceFS.getConf());
    tool.setVerbose(LOG.isDebugEnabled());

    return tool.execute(sourceFS, path, doPurge,
        expectedMarkers, limit, nonAuth);
  }
}
