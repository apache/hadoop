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

package org.apache.hadoop.fs.s3a.s3guard;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.List;

import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Listing;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;
import org.apache.hadoop.service.launcher.ServiceLauncher;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.ACCEPT_ALL;

/**
 * This is a low-level diagnostics entry point which does a CVE/TSV dump of
 * the DDB state.
 * As it also lists the filesystem, it actually changes the state of the store
 * during the operation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DumpS3GuardDynamoTable extends AbstractS3GuardDynamoDBDiagnostic {

  private static final Logger LOG =
      LoggerFactory.getLogger(DumpS3GuardDynamoTable.class);

  /**
   * Application name.
   */
  public static final String NAME = "DumpS3GuardDynamoTable";

  /**
   * Usage.
   */
  private static final String USAGE_MESSAGE = NAME
      + " <filesystem> <dest-file>";

  /**
   * Suffix for the flat list: {@value}.
   */
  public static final String FLAT_CSV = "-flat.csv";

  /**
   * Suffix for the raw S3 dump: {@value}.
   */
  public static final String RAW_CSV = "-s3.csv";

  /**
   * Suffix for the DDB scan: {@value}.
   */
  public static final String SCAN_CSV = "-scan.csv";

  /**
   * Suffix for the second DDB scan: : {@value}.
   */
  public static final String SCAN2_CSV = "-scan-2.csv";

  /**
   * Suffix for the treewalk scan of the S3A Filesystem: {@value}.
   */
  public static final String TREE_CSV = "-tree.csv";

  /**
   * Suffix for a recursive treewalk through the metastore: {@value}.
   */
  public static final String STORE_CSV = "-store.csv";

  /**
   * Path in the local filesystem to save the data.
   */
  private String destPath;

  private Pair<Long, Long> scanEntryResult;

  private Pair<Long, Long> secondScanResult;

  private long rawObjectStoreCount;

  private long listStatusCount;

  private long treewalkCount;

  /**
   * Instantiate.
   * @param name application name.
   */
  public DumpS3GuardDynamoTable(final String name) {
    super(name);
  }

  /**
   * Instantiate with default name.
   */
  public DumpS3GuardDynamoTable() {
    this(NAME);
  }

  /**
   * Bind to a specific FS + store.
   * @param fs filesystem
   * @param store metastore to use
   * @param destFile the base filename for output
   * @param uri URI of store -only needed if FS is null.
   */
  public DumpS3GuardDynamoTable(
      final S3AFileSystem fs,
      final DynamoDBMetadataStore store,
      final File destFile,
      final URI uri) {
    super(NAME, fs, store, uri);
    this.destPath = destFile.getAbsolutePath();
  }

  /**
   * Bind to the argument list, including validating the CLI.
   * @throws Exception failure.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (getStore() == null) {
      List<String> arg = getArgumentList(2, 2, USAGE_MESSAGE);
      bindFromCLI(arg.get(0));
      destPath = arg.get(1);
    }
  }

  /**
   * Dump the filesystem and the metastore.
   * @return the exit code.
   * @throws ServiceLaunchException on failure.
   * @throws IOException IO failure.
   */
  @Override
  public int execute() throws ServiceLaunchException, IOException {

    try {
      final File scanFile = new File(
          destPath + SCAN_CSV).getCanonicalFile();
      File parentDir = scanFile.getParentFile();
      if (!parentDir.mkdirs() && !parentDir.isDirectory()) {
        throw new PathIOException(parentDir.toString(),
            "Could not create destination directory");
      }

      try (CsvFile csv = new CsvFile(scanFile);
           DurationInfo ignored = new DurationInfo(LOG,
               "scanFile dump to %s", scanFile)) {
        scanEntryResult = scanMetastore(csv);
      }

      if (getFilesystem() != null) {

        Path basePath = getFilesystem().qualify(new Path(getUri()));

        final File destFile = new File(destPath + STORE_CSV)
            .getCanonicalFile();
        LOG.info("Writing Store details to {}", destFile);
        try (CsvFile csv = new CsvFile(destFile);
             DurationInfo ignored = new DurationInfo(LOG, "List metastore")) {

          LOG.info("Base path: {}", basePath);
          dumpMetastore(csv, basePath);
        }

        // these operations all update the metastore as they list,
        // that is: they are side-effecting.
        final File treewalkFile = new File(destPath + TREE_CSV)
            .getCanonicalFile();

        try (CsvFile csv = new CsvFile(treewalkFile);
             DurationInfo ignored = new DurationInfo(LOG,
                 "Treewalk to %s", treewalkFile)) {
          treewalkCount = treewalkFilesystem(csv, basePath);
        }
        final File flatlistFile = new File(
            destPath + FLAT_CSV).getCanonicalFile();

        try (CsvFile csv = new CsvFile(flatlistFile);
             DurationInfo ignored = new DurationInfo(LOG,
                 "Flat list to %s", flatlistFile)) {
          listStatusCount = listStatusFilesystem(csv, basePath);
        }
        final File rawFile = new File(
            destPath + RAW_CSV).getCanonicalFile();

        try (CsvFile csv = new CsvFile(rawFile);
             DurationInfo ignored = new DurationInfo(LOG,
                 "Raw dump to %s", rawFile)) {
          rawObjectStoreCount = dumpRawS3ObjectStore(csv);
        }
        final File scanFile2 = new File(
            destPath + SCAN2_CSV).getCanonicalFile();

        try (CsvFile csv = new CsvFile(scanFile);
             DurationInfo ignored = new DurationInfo(LOG,
                 "scanFile dump to %s", scanFile2)) {
          secondScanResult = scanMetastore(csv);
        }
      }

      return LauncherExitCodes.EXIT_SUCCESS;
    } catch (IOException | RuntimeException e) {
      LOG.error("failure", e);
      throw e;
    }
  }

  /**
   * Push all elements of a list to a queue, such that the first entry
   * on the list becomes the head of the queue.
   * @param queue queue to update
   * @param entries list of entries to add.
   * @param <T> type of queue
   */
  private <T> void pushAll(Deque<T> queue, List<T> entries) {
    List<T> reversed = Lists.reverse(entries);
    for (T t : reversed) {
      queue.push(t);
    }
  }

  /**
   * Dump the filesystem via a treewalk.
   * If metastore entries mark directories as deleted, this
   * walk will not explore them.
   * @param csv destination.
   * @param base base path.
   * @return number of entries found.
   * @throws IOException IO failure.
   */
  protected long treewalkFilesystem(
      final CsvFile csv,
      final Path base) throws IOException {
    ArrayDeque<Path> queue = new ArrayDeque<>();
    queue.add(base);
    long count = 0;
    while (!queue.isEmpty()) {
      Path path = queue.pop();
      count++;
      FileStatus[] fileStatuses;
      try {
        fileStatuses = getFilesystem().listStatus(path);
      } catch (FileNotFoundException e) {
        LOG.warn("File {} was not found", path);
        continue;
      }
      // entries
      for (FileStatus fileStatus : fileStatuses) {
        csv.entry((S3AFileStatus) fileStatus);
      }
      // scan through the list, building up a reverse list of all directories
      // found.
      List<Path> dirs = new ArrayList<>(fileStatuses.length);
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()
            && !(fileStatus.getPath().equals(path))) {
          // directory: add to the end of the queue.
          dirs.add(fileStatus.getPath());
        } else {
          // file: just increment the count
          count++;
        }
        // now push the dirs list in reverse
        // so that they have been added in the sort order as returned.
        pushAll(queue, dirs);
      }
    }
    return count;
  }

  /**
   * Dump the filesystem via a recursive listStatus call.
   * @param csv destination.
   * @return number of entries found.
   * @throws IOException IO failure.
   */
  protected long listStatusFilesystem(
      final CsvFile csv,
      final Path path) throws IOException {
    long count = 0;
    RemoteIterator<S3ALocatedFileStatus> iterator = getFilesystem()
        .listFilesAndEmptyDirectories(path, true);
    while (iterator.hasNext()) {
      S3ALocatedFileStatus status = iterator.next();
      csv.entry(status.toS3AFileStatus());
    }
    return count;
  }

  /**
   * Dump the raw S3 Object Store.
   * @param csv destination.
   * @return number of entries found.
   * @throws IOException IO failure.
   */
  protected long dumpRawS3ObjectStore(
      final CsvFile csv) throws IOException {
    S3AFileSystem fs = getFilesystem();
    Path rootPath = fs.qualify(new Path("/"));
    Listing listing = fs.getListing();
    S3ListRequest request = listing.createListObjectsRequest("", null);
    long count = 0;
    RemoteIterator<S3AFileStatus> st =
        listing.createFileStatusListingIterator(rootPath, request,
            ACCEPT_ALL,
            new Listing.AcceptAllButSelfAndS3nDirs(rootPath));
    while (st.hasNext()) {
      count++;
      S3AFileStatus next = st.next();
      LOG.debug("[{}] {}", count, next);
      csv.entry(next);
    }
    LOG.info("entry count: {}", count);
    return count;
  }

  /**
   * list children under the metastore from a base path, through
   * a recursive query + walk strategy.
   * @param csv dest
   * @param basePath base path
   * @throws IOException failure.
   */
  protected void dumpMetastore(final CsvFile csv,
      final Path basePath) throws IOException {
    dumpStoreEntries(csv, getStore().listChildren(basePath));
  }

  /**
   * Recursive Store Dump.
   * @param csv open CSV file.
   * @param dir directory listing
   * @return (directories, files)
   * @throws IOException failure
   */
  private Pair<Long, Long> dumpStoreEntries(
      CsvFile csv,
      DirListingMetadata dir) throws IOException {
    ArrayDeque<DirListingMetadata> queue = new ArrayDeque<>();
    queue.add(dir);
    long files = 0, dirs = 1;
    while (!queue.isEmpty()) {
      DirListingMetadata next = queue.pop();
      List<DDBPathMetadata> childDirs = new ArrayList<>();
      Collection<PathMetadata> listing = next.getListing();
      // sort by name
      List<PathMetadata> sorted = new ArrayList<>(listing);
      sorted.sort(new PathOrderComparators.PathMetadataComparator(
          (l, r) -> l.compareTo(r)));

      for (PathMetadata pmd : sorted) {
        DDBPathMetadata ddbMd = (DDBPathMetadata) pmd;
        dumpEntry(csv, ddbMd);
        if (ddbMd.getFileStatus().isDirectory()) {
          childDirs.add(ddbMd);
        } else {
          files++;
        }
      }
      List<DirListingMetadata> childMD = new ArrayList<>(childDirs.size());
      for (DDBPathMetadata childDir : childDirs) {
        childMD.add(getStore().listChildren(
            childDir.getFileStatus().getPath()));
      }
      pushAll(queue, childMD);
    }

    return Pair.of(dirs, files);
  }


  /**
   * Dump a single entry, and log it.
   * @param csv CSV output file.
   * @param md metadata to log.
   */
  private void dumpEntry(CsvFile csv, DDBPathMetadata md) {
    LOG.debug("{}", md.prettyPrint());
    csv.entry(md);
  }

  /**
   * Scan the metastore for all entries and dump them.
   * There's no attempt to sort the output.
   * @param csv file
   * @return tuple of (live entries, tombstones).
   */
  private Pair<Long, Long> scanMetastore(CsvFile csv) {
    S3GuardTableAccess tableAccess = new S3GuardTableAccess(getStore());
    ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
    Iterable<DDBPathMetadata> results =
        getStore().wrapWithRetries(tableAccess.scanMetadata(builder));
    long live = 0;
    long tombstone = 0;
    for (DDBPathMetadata md : results) {
      if (!(md instanceof S3GuardTableAccess.VersionMarker)) {
        // print it
        csv.entry(md);
        if (md.isDeleted()) {
          tombstone++;
        } else {
          live++;
        }

      }
    }
    return Pair.of(live, tombstone);
  }

  public Pair<Long, Long> getScanEntryResult() {
    return scanEntryResult;
  }

  public Pair<Long, Long> getSecondScanResult() {
    return secondScanResult;
  }

  public long getRawObjectStoreCount() {
    return rawObjectStoreCount;
  }

  public long getListStatusCount() {
    return listStatusCount;
  }

  public long getTreewalkCount() {
    return treewalkCount;
  }

  /**
   * Convert a timestamp in milliseconds to a human string.
   * @param millis epoch time in millis
   * @return a string for the CSV file.
   */
  private static String stringify(long millis) {
    return new Date(millis).toString();
  }

  /**
   * This is the JVM entry point for the service launcher.
   *
   * Converts the arguments to a list, then invokes
   * {@link #serviceMain(List, AbstractS3GuardDynamoDBDiagnostic)}.
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    try {
      serviceMain(Arrays.asList(args), new DumpS3GuardDynamoTable());
    } catch (ExitUtil.ExitException e) {
      ExitUtil.terminate(e);
    }
  }

  /**
   * The real main function, which takes the arguments as a list.
   * Argument 0 MUST be the service classname
   * @param argsList the list of arguments
   * @param service service to launch.
   */
  static void serviceMain(
      final List<String> argsList,
      final AbstractS3GuardDynamoDBDiagnostic service) {
    ServiceLauncher<Service> serviceLauncher =
        new ServiceLauncher<>(service.getName());

    ExitUtil.ExitException ex = serviceLauncher.launchService(
        new Configuration(),
        service,
        argsList,
        false,
        true);
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Entry point to dump the metastore and s3 store world views
   * <p>
   * Both the FS and the store will be dumped: the store is scanned
   * before and after the sequence to show what changes were made to
   * the store during the list operation.
   * @param fs fs to dump. If null a store must be provided.
   * @param store store to dump (fallback to FS)
   * @param conf configuration to use (fallback to fs)
   * @param destFile base name of the output files.
   * @param uri URI of store -only needed if FS is null.
   * @throws ExitUtil.ExitException failure.
   * @return the store
   */
  public static DumpS3GuardDynamoTable dumpStore(
      @Nullable final S3AFileSystem fs,
      @Nullable DynamoDBMetadataStore store,
      @Nullable Configuration conf,
      final File destFile,
      @Nullable URI uri) throws ExitUtil.ExitException {
    ServiceLauncher<Service> serviceLauncher =
        new ServiceLauncher<>(NAME);

    if (conf == null) {
      conf = checkNotNull(fs, "No filesystem").getConf();
    }
    if (store == null) {
      store = (DynamoDBMetadataStore) checkNotNull(fs, "No filesystem")
          .getMetadataStore();
    }
    DumpS3GuardDynamoTable dump = new DumpS3GuardDynamoTable(fs,
        store,
        destFile,
        uri);
    ExitUtil.ExitException ex = serviceLauncher.launchService(
        conf,
        dump,
        Collections.emptyList(),
        false,
        true);
    if (ex != null && ex.getExitCode() != 0) {
      throw ex;
    }
    LOG.info("Results:");
    Pair<Long, Long> r = dump.getScanEntryResult();
    LOG.info("Metastore entries: {}", r);
    LOG.info("Metastore scan total {}, entries {}, tombstones {}",
        r.getLeft() + r.getRight(),
        r.getLeft(),
        r.getRight());
    LOG.info("S3 count {}", dump.getRawObjectStoreCount());
    LOG.info("Treewalk Count {}", dump.getTreewalkCount());
    LOG.info("List Status Count {}", dump.getListStatusCount());
    r = dump.getSecondScanResult();
    if (r != null) {
      LOG.info("Second metastore scan total {}, entries {}, tombstones {}",
          r.getLeft() + r.getRight(),
          r.getLeft(),
          r.getRight());
    }
    return dump;
  }

  /**
   * Writer for generating test CSV files.
   *
   * Quotes are manged by passing in a long whose specific bits control
   * whether or not a row is quoted, bit 0 for column 0, etc.
   *
   * There is no escaping of values here.
   */
  private static final class CsvFile implements Closeable {


    /** constant to quote all columns. */
    public static final long ALL_QUOTES = 0x7fffffff;

    /** least significant bit is used for first column; 1  mean 'quote'. */
    public static final int ROW_QUOTE_MAP = 0b1110_1001_1111;

    /** quote nothing: {@value}. */
    public static final long NO_QUOTES = 0;

    private final Path path;

    private final PrintWriter out;

    private final String separator;

    private final String eol;

    private final String quote;

    /**
     * Create.
     * @param path filesystem path.
     * @param out output write.
     * @param separator separator of entries.
     * @param eol EOL marker.
     * @param quote quote marker.
     * @throws IOException failure.
     */
    private CsvFile(
        final Path path,
        final PrintWriter out,
        final String separator,
        final String eol,
        final String quote) throws IOException {
      this.separator = checkNotNull(separator);
      this.eol = checkNotNull(eol);
      this.quote = checkNotNull(quote);
      this.path = path;
      this.out = checkNotNull(out);
      header();
    }

    /**
     * Create to a file, with UTF-8 output and the standard
     * options of the TSV file.
     * @param file destination file.
     * @throws IOException failure.
     */
    private CsvFile(File file) throws IOException {
      this(null,
          new PrintWriter(file, "UTF-8"), "\t", "\n", "\"");
    }

    /**
     * Close the file, if not already done.
     * @throws IOException on a failure.
     */
    @Override
    public synchronized void close() throws IOException {
      if (out != null) {
        out.close();
      }
    }

    public Path getPath() {
      return path;
    }

    public String getSeparator() {
      return separator;
    }

    public String getEol() {
      return eol;
    }

    /**
     * Write a row.
     * Entries are quoted if the bit for that column is true.
     * @param quotes quote policy: every bit defines the rule for that element
     * @param columns columns to write
     * @return self for ease of chaining.
     */
    public CsvFile row(long quotes, Object... columns) {
      checkNotNull(out);
      for (int i = 0; i < columns.length; i++) {
        if (i != 0) {
          out.write(separator);
        }
        boolean toQuote = (quotes & 1) == 1;
        // unsigned right shift to make next column flag @ position 0
        quotes = quotes >>> 1;
        if (toQuote) {
          out.write(quote);
        }
        Object column = columns[i];
        out.write(column != null ? column.toString() : "");
        if (toQuote) {
          out.write(quote);
        }
      }
      out.write(eol);
      return this;
    }

    /**
     * Write a line.
     * @param line line to print
     * @return self for ease of chaining.
     */
    public CsvFile line(String line) {
      out.write(line);
      out.write(eol);
      return this;
    }

    /**
     * Get the output stream.
     * @return the stream.
     */
    public PrintWriter getOut() {
      return out;
    }

    /**
     * Print the header.
     */
    void header() {
      row(CsvFile.ALL_QUOTES,
          "type",
          "deleted",
          "path",
          "is_auth_dir",
          "is_empty_dir",
          "len",
          "updated",
          "updated_s",
          "last_modified",
          "last_modified_s",
          "etag",
          "version");
    }

    /**
     * Add a metadata entry.
     * @param md metadata.
     */
    void entry(DDBPathMetadata md) {
      S3AFileStatus fileStatus = md.getFileStatus();
      row(ROW_QUOTE_MAP,
          fileStatus.isDirectory() ? "dir" : "file",
          md.isDeleted(),
          fileStatus.getPath().toString(),
          md.isAuthoritativeDir(),
          md.isEmptyDirectory().name(),
          fileStatus.getLen(),
          md.getLastUpdated(),
          stringify(md.getLastUpdated()),
          fileStatus.getModificationTime(),
          stringify(fileStatus.getModificationTime()),
          fileStatus.getETag(),
          fileStatus.getVersionId());
    }

    /**
     * filesystem entry: no metadata.
     * @param fileStatus file status
     */
    void entry(S3AFileStatus fileStatus) {
      row(ROW_QUOTE_MAP,
          fileStatus.isDirectory() ? "dir" : "file",
          "false",
          fileStatus.getPath().toString(),
          "",
          fileStatus.isEmptyDirectory().name(),
          fileStatus.getLen(),
          "",
          "",
          fileStatus.getModificationTime(),
          stringify(fileStatus.getModificationTime()),
          fileStatus.getETag(),
          fileStatus.getVersionId());
    }
  }

}
