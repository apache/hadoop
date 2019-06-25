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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.launcher.AbstractLaunchableService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;
import org.apache.hadoop.service.launcher.ServiceLauncher;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * This is a new diagnostics entry point which does a TSV dump of
 * the DDB treewalk.
 */
public class DumpS3GuardTable extends AbstractLaunchableService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DumpS3GuardTable.class);

  public static final String NAME = "DumpS3GuardTable";

  private static String USAGE_MESSAGE = NAME + " <filesystem> <dest-file>";

  private List<String> arguments;

  private DynamoDBMetadataStore store;

  public DumpS3GuardTable(final String name) {
    super(name);
  }

  public DumpS3GuardTable() {
    this("DumpS3GuardTable");
  }

  @Override
  public Configuration bindArgs(final Configuration config,
      final List<String> args)
      throws Exception {
    this.arguments = args;
    return super.bindArgs(config, args);
  }

  public List<String> getArguments() {
    return arguments;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int execute() throws ServiceLaunchException, IOException {

    Preconditions.checkNotNull(arguments, "No arguments");
    Preconditions.checkState(arguments.size() == 2,
        "Wrong number of arguments: %s", arguments.size());

    String fsURI = arguments.get(0);
    String destFilename = arguments.get(1);
    Configuration conf = getConfig();
    URI uri = null;
    try {
      uri = new URI(fsURI);
    } catch (URISyntaxException e) {
      throw fail("Bad URI " + fsURI, e);
    }
    try {
      FileSystem fileSystem = FileSystem.get(uri, conf);
      require(fileSystem instanceof S3AFileSystem,
          "Not an S3A Filesystem:  " + fsURI);
      S3AFileSystem fs = (S3AFileSystem) fileSystem;
      require(fs.hasMetadataStore(),
          "Filesystem has no metadata store:  " + fsURI);
      MetadataStore ms = fs.getMetadataStore();
      require(ms instanceof DynamoDBMetadataStore,
          "Filesystem " + fsURI
              + "does not have a DynamoDB metadata store:  " + ms);
      store = (DynamoDBMetadataStore) ms;
//    Table table = store.getTable();

      File destFile = new File(destFilename).getCanonicalFile();
      LOG.info("Writing Store details to {}", destFile);
      try (CsvFile csv = new CsvFile(null,
          new PrintWriter(destFile),
          "\t", "\n", "\"")) {
        csv.row(CsvFile.ALL_QUOTES,
            "path", "type", "is_auth_dir", "deleted",
            "is_empty_dir", "len", "updated", "updated_s",
            "last_modified", "last_modified_s", "etag", "version");

        Path basePath = fs.qualify(new Path(uri));
        LOG.info("Base path: {}", basePath);
        DirListingMetadata children = store.listChildren(basePath);
        dumpRecursively(csv, children);
      }
      return LauncherExitCodes.EXIT_SUCCESS;
    } catch (Exception e) {
      LOG.error("failure", e);
      throw e;
    }
  }


  private Pair<Integer, Integer> dumpRecursively(
      CsvFile csv, DirListingMetadata dir) throws IOException {
    int files = 0, dirs = 1;
    List<DDBPathMetadata> childDirs = new ArrayList<>();
    Collection<PathMetadata> listing = dir.getListing();
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
    for (DDBPathMetadata childDir : childDirs) {
      DirListingMetadata children = store.listChildren(
          childDir.getFileStatus().getPath());
      Pair<Integer, Integer> pair = dumpRecursively(csv,
          children);
      dirs += pair.getLeft();
      files += pair.getRight();
    }

    return Pair.of(dirs, files);
  }

  /** least significant bit is used for first column; 1  mean 'quote'. */
  public static final int QUOTE_MAP = 0b1110_1001_1111;

  private void dumpEntry(CsvFile csv, DDBPathMetadata md) {
    S3AFileStatus fileStatus = md.getFileStatus();
    LOG.info("{}", md.prettyPrint());
    csv.row(QUOTE_MAP,
        fileStatus.getPath().toString(),
        fileStatus.isDirectory() ? "dir" : "file",
        md.isAuthoritativeDir(),
        md.isDeleted(),
        md.isEmptyDirectory().name(),
        fileStatus.getLen(),
        md.getLastUpdated(),
        stringify(md.getLastUpdated()),
        fileStatus.getModificationTime(),
        stringify(fileStatus.getModificationTime()),
        fileStatus.getETag(),
        fileStatus.getVersionId());
  }

  private String stringify(long millis) {
    return new Date(millis).toString();
  }

  private void require(boolean condition, String error) {
    if (!condition) {
      throw fail(error);
    }
  }

  private ServiceLaunchException fail(String message, Throwable ex) {
    return new ServiceLaunchException(LauncherExitCodes.EXIT_FAIL, message, ex);
  }

  private ServiceLaunchException fail(String message) {
    return new ServiceLaunchException(LauncherExitCodes.EXIT_FAIL, message);
  }

  /**
   * This is the JVM entry point for the service launcher.
   *
   * Converts the arguments to a list, then invokes {@link #serviceMain(List)}
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    try {
      LinkedList<String> argsList = new LinkedList<>(Arrays.asList(args));
      serviceMain(argsList);
    } catch (ExitUtil.ExitException e) {
      ExitUtil.terminate(e);
    }
  }

  /**
   * The real main function, which takes the arguments as a list.
   * Argument 0 MUST be the service classname
   * @param argsList the list of arguments
   */
  public static void serviceMain(List<String> argsList) {
    if (argsList.size() != 2) {
      // no arguments: usage message
      ExitUtil.terminate(new ServiceLaunchException(EXIT_USAGE, USAGE_MESSAGE));

    } else {
      ServiceLauncher<Service> serviceLauncher =
          new ServiceLauncher<>(NAME);

      ExitUtil.ExitException ex = serviceLauncher.launchService(
          new Configuration(),
          new DumpS3GuardTable(),
          argsList,
          false,
          true);
      if (ex != null) {
        throw ex;
      }
    }
  }

  /**
   *
   * @param filesystem filesystem path
   * @param conf configuration to instantiate with.
   * @param destFile output file.
   * @throws ExitUtil.ExitException failure.
   */
  public static void dumpS3GuardStore(String filesystem,
      Configuration conf,
      File destFile) throws ExitUtil.ExitException {
    ServiceLauncher<Service> serviceLauncher =
        new ServiceLauncher<>("");
    if (!filesystem.endsWith("/")) {
      filesystem += "/";
    }

    ExitUtil.ExitException ex = serviceLauncher.launchService(
        conf,
        new DumpS3GuardTable(),
        Lists.newArrayList(filesystem, destFile.getAbsolutePath()),
        false,
        true);
    if (ex != null && ex.getExitCode() != 0) {
      throw ex;
    }
  }

  /**
   * Writer for generating test CSV files.
   *
   * Quotes are manged by passing in a long whose specific bits control
   * whether or not a row is quoted, bit 0 for column 0, etc.
   */
  private static class CsvFile implements Closeable {


    /** constant to quote all columns. */
    public static final long ALL_QUOTES = 0x7fffffff;

    /** quote nothing: {@value}. */
    public static final long NO_QUOTES = 0;

    private final Path path;

    private final PrintWriter out;

    private final String separator;

    private final String eol;

    private final String quote;

    CsvFile(final FileSystem fs,
        final Path path,
        final boolean overwrite,
        final String separator,
        final String eol,
        final String quote) throws IOException {
      this(path,
          new PrintWriter(fs.create(path, overwrite)),
          separator, eol, quote);
    }

    public CsvFile(
        final Path path,
        final PrintWriter out,
        final String separator,
        final String eol,
        final String quote) throws IOException {
      this.separator = Preconditions.checkNotNull(separator);
      this.eol = Preconditions.checkNotNull(eol);
      this.quote = Preconditions.checkNotNull(quote);
      this.path = path;
      this.out = Preconditions.checkNotNull(out);
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
      Preconditions.checkNotNull(out);
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
  }
}
