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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
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
      + " [OPTIONS]"
      + " (audit || report || clean)"
//      + " [-out <path>]"
      + " [-" + VERBOSE + "]"
      + " <PATH>\n"
      + "\t" + PURPOSE + "\n\n";

  public static final String OPT_EXPECTED = "expected";

  public static final String OPT_AUDIT = "audit";

  public static final String OPT_CLEAN = "clean";

  public static final String OPT_REPORT = "report";

  public static final String OPT_OUTPUT = "output";

  public static final String OPT_VERBOSE = "verbose";

  static final String TOO_FEW_ARGUMENTS = "Too few arguments";

  private PrintStream out;

  public MarkerTool(final Configuration conf) {
    super(conf, OPT_VERBOSE);
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
    boolean verbose = commandFormat.getOpt(VERBOSE);

    boolean purge = false;
    int expected = 0;
    String action = parsedArgs.get(0);
    switch (action) {
    case OPT_AUDIT:
      purge = false;
      expected = 0;
      break;
    case OPT_CLEAN:
      purge = true;
      expected = -1;
      break;
    case OPT_REPORT:
      purge = false;
      expected = -1;
      break;
    default:
      errorln(getUsage());
      throw new ExitUtil.ExitException(EXIT_USAGE, "Unknown action: " + action);
    }

    final String file = parsedArgs.get(1);
    final Path path = new Path(file);
    S3AFileSystem fs = bindFilesystem(path.getFileSystem(getConf()));
    final StoreContext context = fs.createStoreContext();
    final OperationCallbacks operations = fs.getOperationCallbacks();


    boolean finalPurge = purge;
    int finalExpected = expected;
    int result = once("action", path.toString(),
        () -> scan(path, finalPurge, finalExpected, verbose, context,
            operations));
    if (verbose) {
      dumpFileSystemStatistics(fs);
    }
    return result;
  }

  private int scan(final Path path,
      final boolean purge,
      final int expected,
      final boolean verbose,
      final StoreContext context,
      final OperationCallbacks operations)
      throws IOException, ExitUtil.ExitException {
    DirMarkerTracker tracker = new DirMarkerTracker();
    try (DurationInfo ignored =
             new DurationInfo(LOG, "marker scan %s", path)) {
      scanDirectoryTree(path, expected, context, operations, tracker);
    }
    // scan done. what have we got?
    Map<Path, Pair<String, S3ALocatedFileStatus>> surplusMarkers
        = tracker.getSurplusMarkers();
    Map<Path, Pair<String, S3ALocatedFileStatus>> leafMarkers
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

      if (size > expected) {
        // failure
        println(out, "Expected %d marker%s", expected, suffix(size));
        return EXIT_NOT_ACCEPTABLE;
      }

    }
    return EXIT_SUCCESS;
  }

  private String suffix(final int size) {
    return size == 1 ? "" : "s";
  }

  private void scanDirectoryTree(final Path path,
      final int expected,
      final StoreContext context,
      final OperationCallbacks operations,
      final DirMarkerTracker tracker) throws IOException {
    RemoteIterator<S3AFileStatus> listing = operations
        .listObjects(path,
            context.pathToKey(path));
    while (listing.hasNext()) {
      S3AFileStatus status = listing.next();
      Path p = status.getPath();
      S3ALocatedFileStatus lfs = new S3ALocatedFileStatus(
          status, null);
      String key = context.pathToKey(p);
      if (status.isDirectory()) {
        LOG.info("{}", key);
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
}
