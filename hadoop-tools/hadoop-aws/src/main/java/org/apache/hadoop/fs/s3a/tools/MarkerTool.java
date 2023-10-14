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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.s3a.AWSBadRequestException;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicyImpl;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;


import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_INTERRUPTED;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * Audit an S3 bucket for directory markers.
 */
@InterfaceAudience.LimitedPrivate("management tools")
@InterfaceStability.Unstable
public final class MarkerTool extends S3GuardTool {

  private static final Logger LOG = LoggerFactory.getLogger(MarkerTool.class);

  /**
   * Name of this tool: {@value}.
   */
  public static final String MARKERS = "markers";

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
   * Min number of markers to find: {@value}.
   */
  public static final String OPT_MIN = "min";

  /**
   * Max number of markers to find: {@value}.
   */
  public static final String OPT_MAX = "max";

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
   * <p>
   * The value is 0 and not -1 because it allows for the limit to be
   * set on the command line {@code -limit 0}.
   * The command line parser rejects {@code -limit -1} as the -1
   * is interpreted as the (unknown) option "-1".
   */
  public static final int UNLIMITED_LISTING = 0;


  /**
   * Constant to use when there is no minimum number of
   * markers: {@value}.
   */
  public static final int UNLIMITED_MIN_MARKERS = -1;


  /**
   * Usage string: {@value}.
   */
  private static final String USAGE = MARKERS
      + " (-" + OPT_AUDIT
      + " | -" + OPT_CLEAN + ")"
      + " [-" + OPT_MIN + " <count>]"
      + " [-" + OPT_MAX + " <count>]"
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
    format.addOptionWithValue(OPT_MIN);
    format.addOptionWithValue(OPT_MAX);
    format.addOptionWithValue(OPT_LIMIT);
    format.addOptionWithValue(OPT_OUT);
  }

  @Override
  public String getUsage() {
    return USAGE;
  }

  @Override
  public String getName() {
    return MARKERS;
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
          + String.join(", ", parsedArgs)
          + "]");
      throw new ExitUtil.ExitException(EXIT_USAGE,
          String.format(E_ARGUMENTS, parsedArgs.size()));
    }
    // read arguments
    CommandFormat command = getCommandFormat();
    verbose = command.getOpt(VERBOSE);

    // minimum number of markers expected
    int expectedMin = getOptValue(OPT_MIN, 0);
    // max number of markers allowed
    int expectedMax = getOptValue(OPT_MAX, 0);


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
    int limit = getOptValue(OPT_LIMIT, UNLIMITED_LISTING);
    final String dir = parsedArgs.get(0);
    Path path = new Path(dir);
    URI uri = path.toUri();
    if (uri.getPath().isEmpty()) {
      // fix up empty URI for better CLI experience
      path = new Path(path, "/");
    }
    FileSystem fs = path.getFileSystem(getConf());
    boolean nonAuth = command.getOpt(OPT_NONAUTH);
    ScanResult result;
    try {
      result = execute(
              new ScanArgsBuilder()
                      .withSourceFS(fs)
                      .withPath(path)
                      .withDoPurge(clean)
                      .withMinMarkerCount(expectedMin)
                      .withMaxMarkerCount(expectedMax)
                      .withLimit(limit)
                      .withNonAuth(nonAuth)
                      .build());
    } catch (UnknownStoreException ex) {
      // bucket doesn't exist.
      // replace the stack trace with an error code.
      throw new ExitUtil.ExitException(EXIT_NOT_FOUND,
              ex.toString(), ex);
    }
    if (verbose) {
      dumpFileSystemStatistics(out);
    }

    // and finally see if the output should be saved to a file
    String saveFile = command.getOptValue(OPT_OUT);
    if (saveFile != null && !saveFile.isEmpty()) {
      println(out, "Saving result to %s", saveFile);
      try (Writer writer =
               new OutputStreamWriter(
                   new FileOutputStream(saveFile),
                   StandardCharsets.UTF_8)) {
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

    return result.finish();
  }

  /**
   * Get the value of an option, or the default if the option
   * is unset/empty.
   * @param option option key
   * @param defVal default
   * @return the value to use
   */
  private int getOptValue(String option, int defVal) {
    CommandFormat command = getCommandFormat();
    String value = command.getOptValue(option);
    if (value != null && !value.isEmpty()) {
      try {
        return  Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new ExitUtil.ExitException(EXIT_USAGE,
            String.format("Argument for %s is not a number: %s",
                option, value));
      }
    } else {
      return defVal;
    }
  }

  /**
   * Execute the scan/purge.
   *
   * @param scanArgs@return scan+purge result.
   * @throws IOException failure
   */
  @VisibleForTesting
  ScanResult execute(final ScanArgs scanArgs)
      throws IOException {
    S3AFileSystem fs = bindFilesystem(scanArgs.getSourceFS());

    // extract the callbacks needed for the rest of the work
    storeContext = fs.createStoreContext();
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
    Path path = scanArgs.getPath();
    Path target = path.makeQualified(fs.getUri(), new Path("/"));
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
    DirectoryPolicy filterPolicy;
    if (scanArgs.isNonAuth()) {
      filterPolicy = new DirectoryPolicyImpl(
          DirectoryPolicy.MarkerPolicy.Authoritative,
          fs::allowAuthoritative);
    } else {
      filterPolicy = null;
    }
    int minMarkerCount = scanArgs.getMinMarkerCount();
    int maxMarkerCount = scanArgs.getMaxMarkerCount();
    if (minMarkerCount > maxMarkerCount) {
      // swap min and max if they are wrong.
      // this is to ensure any test scripts written to work around
      // HADOOP-17332 and min/max swapping continue to work.
      println(out, "Swapping -min (%d) and -max (%d) values",
          minMarkerCount, maxMarkerCount);
      int m = minMarkerCount;
      minMarkerCount = maxMarkerCount;
      maxMarkerCount = m;
    }
    // extract the callbacks needed for the rest of the work
    operations = fs.createMarkerToolOperations(
        target.toString());
    return scan(target,
        scanArgs.isDoPurge(),
        minMarkerCount,
        maxMarkerCount,
        scanArgs.getLimit(),
        filterPolicy);
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
     * Text to include if raising an exception.
     */
    private String exitText = "";

    /**
     * Count of all markers found.
     */
    private int totalMarkerCount;

    /**
     * Count of all markers found after excluding
     * any from a [-nonauth] qualification.
     */
    private int filteredMarkerCount;

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
          ", exitText=" + exitText +
          ", totalMarkerCount=" + totalMarkerCount +
          ", filteredMarkerCount=" + filteredMarkerCount +
          ", tracker=" + tracker +
          ", purgeSummary=" + purgeSummary +
          '}';
    }

    /**
     * @return Exit code to report.
     */
    public int getExitCode() {
      return exitCode;
    }

    /**
     * @return Tracker which did the scan.
     */
    public DirMarkerTracker getTracker() {
      return tracker;
    }

    /**
     * @return Summary of purge. Null if none took place.
     */
    public MarkerPurgeSummary getPurgeSummary() {
      return purgeSummary;
    }

    public int getTotalMarkerCount() {
      return totalMarkerCount;
    }

    public int getFilteredMarkerCount() {
      return filteredMarkerCount;
    }

    /**
     * Throw an exception if the exit code is non-zero.
     * @return 0 if everything is good.
     * @throws ExitUtil.ExitException if code != 0
     */
    public int finish() throws ExitUtil.ExitException {
      if (exitCode != 0) {
        throw new ExitUtil.ExitException(exitCode, exitText);
      }
      return 0;
    }
  }

  /**
   * Do the scan/purge.
   * @param path path to scan.
   * @param doPurge purge rather than just scan/audit?
   * @param minMarkerCount min marker count (ignored on purge)
   * @param maxMarkerCount max marker count (ignored on purge)
   * @param limit limit of files to scan; 0 for 'unlimited'
   * @param filterPolicy filter policy on a nonauth scan; may be null
   * @return result.
   * @throws IOException IO failure
   * @throws ExitUtil.ExitException explicitly raised failure
   */
  @Retries.RetryTranslated
  private ScanResult scan(
      final Path path,
      final boolean doPurge,
      final int minMarkerCount,
      final int maxMarkerCount,
      final int limit,
      final DirectoryPolicy filterPolicy)
      throws IOException, ExitUtil.ExitException {

    // safety check: min and max are correctly ordered at this point.
    Preconditions.checkArgument(minMarkerCount <= maxMarkerCount,
        "The min marker count of %d is greater than the max value of %d",
        minMarkerCount, maxMarkerCount);

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
    // determine marker count
    int markerCount = surplusMarkers.size();
    result.totalMarkerCount = markerCount;
    result.filteredMarkerCount = markerCount;
    if (markerCount == 0) {
      println(out, "No surplus directory markers were found under %s", path);
    } else {
      println(out, "Found %d surplus directory marker%s under %s",
          markerCount,
          suffix(markerCount),
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

    if (doPurge) {
      // clean: remove the markers, do not worry about their
      // presence when reporting success/failure
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
        markerCount = surplusMarkers.size();
        result.filteredMarkerCount = markerCount;
      }
      if (markerCount < minMarkerCount || markerCount > maxMarkerCount) {
        // failure
        return failScan(result, EXIT_NOT_ACCEPTABLE,
            "Marker count %d out of range "
                  + "[%d - %d]",
              markerCount, minMarkerCount, maxMarkerCount);
      }
    }

    // now one little check for whether a limit was reached.
    if (!completed) {
      failScan(result, EXIT_INTERRUPTED,
          "Listing limit (%d) reached before completing the scan", limit);
    }
    return result;
  }

  /**
   * Fail the scan; print the formatted error and update the result.
   * @param result result to update
   * @param code Exit code
   * @param message Error message
   * @param args arguments for the error message
   * @return scan result
   */
  private ScanResult failScan(
      ScanResult result,
      int code,
      String message,
      Object...args) {
    String text = String.format(message, args);
    result.exitCode = code;
    result.exitText = text;
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
   * @return true if the scan completely scanned the entire tree
   * @throws IOException IO failure
   */
  @Retries.RetryTranslated
  private boolean scanDirectoryTree(
      final Path path,
      final DirMarkerTracker tracker,
      final int limit) throws IOException {

    int count = 0;
    boolean result = true;

    // the path/key stuff loses any trailing / passed in.
    // but this may actually be needed.
    RemoteIterator<S3AFileStatus> listing = null;
    String listkey = storeContext.pathToKey(path);
    if (listkey.isEmpty()) {
      // root. always give it a path to keep ranger happy.
      listkey = "/";
    }

    try {
      listing = operations.listObjects(path, listkey);
    } catch (AWSBadRequestException e) {
      // endpoint was unhappy. this is generally unrecoverable, but some
      // third party stores do insist on a / here.
      LOG.debug("Failed to list \"{}\"", listkey, e);
      // now retry with a trailing / in case that works
      if (listkey.endsWith("/")) {
        // already has a trailing /, so fail
        throw e;
      }
      // try again.
      listing = operations.listObjects(path, listkey + "/");
    }
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
        println(out, "Limit of scan reached - %,d object%s",
            limit, suffix(limit));
        result = false;
        break;
      }
    }
    LOG.debug("Listing summary {}", listing);
    if (verbose) {
      println(out, "%nListing statistics:%n  %s%n",
          ioStatisticsSourceToString(listing));
    }
    return result;
  }

  /**
   * Result of a call of {@link #purgeMarkers(DirMarkerTracker, int)};
   * included in {@link ScanResult} so must share visibility.
   */
  public static final class MarkerPurgeSummary {

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


    /**
     * Count of markers deleted.
     * @return a number, zero when prune==false.
     */
    int getMarkersDeleted() {
      return markersDeleted;
    }

    /**
     * Count of bulk delete requests issued.
     * @return count of calls made to S3.
     */
    int getDeleteRequests() {
      return deleteRequests;
    }

    /**
     * Total duration of delete requests.
     * @return a time interval in millis.
     */
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
      throws MultiObjectDeleteException, AwsServiceException, IOException {

    MarkerPurgeSummary summary = new MarkerPurgeSummary();
    // we get a map of surplus markers to delete.
    Map<Path, DirMarkerTracker.Marker> markers
        = tracker.getSurplusMarkers();
    int size = markers.size();
    // build a list from the strings in the map
    List<ObjectIdentifier> collect =
        markers.values().stream()
            .map(p -> ObjectIdentifier.builder().key(p.getKey()).build())
            .collect(Collectors.toList());
    // build an array list for ease of creating the lists of
    // keys in each page through the subList() method.
    List<ObjectIdentifier> markerKeys =
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
      List<ObjectIdentifier> page = markerKeys.subList(start,
          end);
      once("Remove S3 Keys",
          tracker.getBasePath().toString(), () ->
              operations.removeKeys(page, true));
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
   * @param scanArgs set of args for the scanner.
   * @throws IOException IO failure
   * @return the result
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public static MarkerTool.ScanResult execMarkerTool(
      ScanArgs scanArgs) throws IOException {
    MarkerTool tool = new MarkerTool(scanArgs.getSourceFS().getConf());
    tool.setVerbose(LOG.isDebugEnabled());

    return tool.execute(scanArgs);
  }

  /**
   * Arguments for the scan.
   * <p>
   * Uses a builder/argument object because too many arguments were
   * being created, and it was making maintenance harder.
   */
  public static final class ScanArgs {

    /** Source FS; must be or wrap an S3A FS. */
    private final FileSystem sourceFS;

    /** Path to scan. */
    private final Path path;

    /** Purge? */
    private final boolean doPurge;

    /** Min marker count (ignored on purge). */
    private final int minMarkerCount;

    /** Max marker count (ignored on purge). */
    private final int maxMarkerCount;

    /** Limit of files to scan; 0 for 'unlimited'. */
    private final int limit;

    /** Consider only markers in nonauth paths as errors. */
    private final boolean nonAuth;

    /**
     * @param sourceFS source FS; must be or wrap an S3A FS.
     * @param path path to scan.
     * @param doPurge purge?
     * @param minMarkerCount min marker count (ignored on purge)
     * @param maxMarkerCount max marker count (ignored on purge)
     * @param limit limit of files to scan; 0 for 'unlimited'
     * @param nonAuth consider only markers in nonauth paths as errors
     */
    private ScanArgs(final FileSystem sourceFS,
        final Path path,
        final boolean doPurge,
        final int minMarkerCount,
        final int maxMarkerCount,
        final int limit,
        final boolean nonAuth) {
      this.sourceFS = sourceFS;
      this.path = path;
      this.doPurge = doPurge;
      this.minMarkerCount = minMarkerCount;
      this.maxMarkerCount = maxMarkerCount;
      this.limit = limit;
      this.nonAuth = nonAuth;
    }

    FileSystem getSourceFS() {
      return sourceFS;
    }

    Path getPath() {
      return path;
    }

    boolean isDoPurge() {
      return doPurge;
    }

    int getMinMarkerCount() {
      return minMarkerCount;
    }

    int getMaxMarkerCount() {
      return maxMarkerCount;
    }

    int getLimit() {
      return limit;
    }

    boolean isNonAuth() {
      return nonAuth;
    }
  }

  /**
   * Builder of the scan arguments.
   */
  public static final class ScanArgsBuilder {

    /** Source FS; must be or wrap an S3A FS. */
    private FileSystem sourceFS;

    /** Path to scan. */
    private Path path;

    /** Purge? */
    private boolean doPurge = false;

    /** Min marker count (ignored on purge). */
    private int minMarkerCount = 0;

    /** Max marker count (ignored on purge). */
    private int maxMarkerCount = 0;

    /** Limit of files to scan; 0 for 'unlimited'. */
    private int limit = UNLIMITED_LISTING;

    /** Consider only markers in nonauth paths as errors. */
    private boolean nonAuth = false;

    /**
     * Source FS; must be or wrap an S3A FS.
     * @param source Source FileSystem
     * @return the builder class after scanning source FS
     */
    public ScanArgsBuilder withSourceFS(final FileSystem source) {
      this.sourceFS = source;
      return this;
    }

    /**
     * Path to scan.
     * @param p path to scan
     * @return builder class for method chaining
     */
    public ScanArgsBuilder withPath(final Path p) {
      this.path = p;
      return this;
    }

    /**
     * Should the markers be purged? This is also enabled when using the clean flag on the CLI.
     * @param d set to purge if true
     * @return builder class for method chaining
     */
    public ScanArgsBuilder withDoPurge(final boolean d) {
      this.doPurge = d;
      return this;
    }

    /**
     * Min marker count an audit must find (ignored on purge).
     * @param min Minimum Marker Count (default 0)
     * @return builder class for method chaining
     */
    public ScanArgsBuilder withMinMarkerCount(final int min) {
      this.minMarkerCount = min;
      return this;
    }

    /**
     * Max marker count an audit must find (ignored on purge).
     * @param max Maximum Marker Count (default 0)
     * @return builder class for method chaining
     */
    public ScanArgsBuilder withMaxMarkerCount(final int max) {
      this.maxMarkerCount = max;
      return this;
    }

    /**
     * Limit of files to scan; 0 for 'unlimited'.
     * @param l Limit of files to scan
     * @return builder class for method chaining
     */
    public ScanArgsBuilder withLimit(final int l) {
      this.limit = l;
      return this;
    }

    /**
     * Consider only markers in non-authoritative paths as errors.
     * @param b True if tool should only consider markers in non-authoritative paths
     * @return builder class for method chaining
     */
    public ScanArgsBuilder withNonAuth(final boolean b) {
      this.nonAuth = b;
      return this;
    }

    /**
     * Build the actual argument instance.
     * @return the arguments to pass in
     */
    public ScanArgs build() {
      return new ScanArgs(sourceFS,
          path,
          doPurge,
          minMarkerCount,
          maxMarkerCount,
          limit,
          nonAuth);
    }
  }
}
