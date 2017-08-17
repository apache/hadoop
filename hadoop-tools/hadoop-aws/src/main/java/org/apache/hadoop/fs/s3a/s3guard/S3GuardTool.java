/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * CLI to manage S3Guard Metadata Store.
 */
public abstract class S3GuardTool extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(S3GuardTool.class);

  private static final String NAME = "s3guard";
  private static final String COMMON_USAGE =
      "When possible and not overridden by more specific options, metadata\n" +
      "repository information will be inferred from the S3A URL (if provided)" +
      "\n\n" +
      "Generic options supported are:\n" +
      "  -conf <config file> - specify an application configuration file\n" +
      "  -D <property=value> - define a value for a given property\n";

  private static final String USAGE = NAME +
      " [command] [OPTIONS] [s3a://BUCKET]\n\n" +
      "Commands: \n" +
      "\t" + Init.NAME + " - " + Init.PURPOSE + "\n" +
      "\t" + Destroy.NAME + " - " + Destroy.PURPOSE + "\n" +
      "\t" + Import.NAME + " - " + Import.PURPOSE + "\n" +
      "\t" + Diff.NAME + " - " + Diff.PURPOSE + "\n" +
      "\t" + Prune.NAME + " - " + Prune.PURPOSE + "\n";
  private static final String DATA_IN_S3_IS_PRESERVED
      = "(all data in S3 is preserved";

  abstract public String getUsage();

  // Exit codes
  static final int SUCCESS = 0;
  static final int INVALID_ARGUMENT = 1;
  static final int ERROR = 99;

  private S3AFileSystem filesystem;
  private MetadataStore store;
  private final CommandFormat commandFormat;

  private static final String META_FLAG = "meta";
  private static final String DAYS_FLAG = "days";
  private static final String HOURS_FLAG = "hours";
  private static final String MINUTES_FLAG = "minutes";
  private static final String SECONDS_FLAG = "seconds";

  private static final String REGION_FLAG = "region";
  private static final String READ_FLAG = "read";
  private static final String WRITE_FLAG = "write";

  /**
   * Constructor a S3Guard tool with HDFS configuration.
   * @param conf Configuration.
   */
  protected S3GuardTool(Configuration conf) {
    super(conf);

    commandFormat = new CommandFormat(0, Integer.MAX_VALUE);
    // For metadata store URI
    commandFormat.addOptionWithValue(META_FLAG);
    // DDB region.
    commandFormat.addOptionWithValue(REGION_FLAG);
  }

  /**
   * Return sub-command name.
   */
  abstract String getName();

  /**
   * Parse DynamoDB region from either -m option or a S3 path.
   *
   * This function should only be called from {@link Init} or
   * {@link Destroy}.
   *
   * @param paths remaining parameters from CLI.
   * @return false for invalid parameters.
   * @throws IOException on I/O errors.
   */
  boolean parseDynamoDBRegion(List<String> paths) throws IOException {
    Configuration conf = getConf();
    String fromCli = getCommandFormat().getOptValue(REGION_FLAG);
    String fromConf = conf.get(S3GUARD_DDB_REGION_KEY);
    boolean hasS3Path = !paths.isEmpty();

    if (fromCli != null) {
      if (fromCli.isEmpty()) {
        System.err.println("No region provided with -" + REGION_FLAG + " flag");
        return false;
      }
      if (hasS3Path) {
        System.err.println("Providing both an S3 path and the -" + REGION_FLAG
            + " flag is not supported. If you need to specify a different "
            + "region than the S3 bucket, configure " + S3GUARD_DDB_REGION_KEY);
        return false;
      }
      conf.set(S3GUARD_DDB_REGION_KEY, fromCli);
      return true;
    }

    if (fromConf != null) {
      if (fromConf.isEmpty()) {
        System.err.printf("No region provided with config %s, %n",
            S3GUARD_DDB_REGION_KEY);
        return false;
      }
      return true;
    }

    if (hasS3Path) {
      String s3Path = paths.get(0);
      initS3AFileSystem(s3Path);
      return true;
    }

    System.err.println("No region found from -" + REGION_FLAG + " flag, " +
        "config, or S3 bucket");
    return false;
  }

  /**
   * Parse metadata store from command line option or HDFS configuration.
   *
   * @param forceCreate override the auto-creation setting to true.
   * @return a initialized metadata store.
   */
  MetadataStore initMetadataStore(boolean forceCreate) throws IOException {
    if (getStore() != null) {
      return getStore();
    }
    Configuration conf;
    if (filesystem == null) {
      conf = getConf();
    } else {
      conf = filesystem.getConf();
    }
    String metaURI = getCommandFormat().getOptValue(META_FLAG);
    if (metaURI != null && !metaURI.isEmpty()) {
      URI uri = URI.create(metaURI);
      LOG.info("create metadata store: {}", uri + " scheme: "
          + uri.getScheme());
      switch (uri.getScheme().toLowerCase(Locale.ENGLISH)) {
      case "local":
        setStore(new LocalMetadataStore());
        break;
      case "dynamodb":
        setStore(new DynamoDBMetadataStore());
        conf.set(S3GUARD_DDB_TABLE_NAME_KEY, uri.getAuthority());
        if (forceCreate) {
          conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, true);
        }
        break;
      default:
        throw new IOException(
            String.format("Metadata store %s is not supported", uri));
      }
    } else {
      // CLI does not specify metadata store URI, it uses default metadata store
      // DynamoDB instead.
      setStore(new DynamoDBMetadataStore());
      if (forceCreate) {
        conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, true);
      }
    }

    if (filesystem == null) {
      getStore().initialize(conf);
    } else {
      getStore().initialize(filesystem);
    }
    LOG.info("Metadata store {} is initialized.", getStore());
    return getStore();
  }

  /**
   * Initialize S3A FileSystem instance.
   *
   * @param path s3a URI
   * @throws IOException
   */
  void initS3AFileSystem(String path) throws IOException {
    URI uri;
    try {
      uri = new URI(path);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    // Make sure that S3AFileSystem does not hold an actual MetadataStore
    // implementation.
    Configuration conf = getConf();
    conf.setClass(S3_METADATA_STORE_IMPL, NullMetadataStore.class,
        MetadataStore.class);
    FileSystem fs = FileSystem.get(uri, getConf());
    if (!(fs instanceof S3AFileSystem)) {
      throw new IOException(
          String.format("URI %s is not a S3A file system: %s", uri,
              fs.getClass().getName()));
    }
    filesystem = (S3AFileSystem) fs;
  }

  /**
   * Parse CLI arguments and returns the position arguments.
   * The options are stored in {@link #commandFormat}
   *
   * @param args command line arguments.
   * @return the position arguments from CLI.
   */
  List<String> parseArgs(String[] args) {
    return getCommandFormat().parse(args, 1);
  }

  protected S3AFileSystem getFilesystem() {
    return filesystem;
  }

  protected void setFilesystem(S3AFileSystem filesystem) {
    this.filesystem = filesystem;
  }

  @VisibleForTesting
  public MetadataStore getStore() {
    return store;
  }

  @VisibleForTesting
  protected void setStore(MetadataStore store) {
    Preconditions.checkNotNull(store);
    this.store = store;
  }

  protected CommandFormat getCommandFormat() {
    return commandFormat;
  }

  /**
   * Create the metadata store.
   */
  static class Init extends S3GuardTool {
    private static final String NAME = "init";
    public static final String PURPOSE = "initialize metadata repository";
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
          "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "  -" + READ_FLAG + " UNIT - Provisioned read throughput units\n" +
        "  -" + WRITE_FLAG + " UNIT - Provisioned write through put units\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    Init(Configuration conf) {
      super(conf);
      // read capacity.
      getCommandFormat().addOptionWithValue(READ_FLAG);
      // write capacity.
      getCommandFormat().addOptionWithValue(WRITE_FLAG);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public int run(String[] args) throws IOException {
      List<String> paths = parseArgs(args);

      String readCap = getCommandFormat().getOptValue(READ_FLAG);
      if (readCap != null && !readCap.isEmpty()) {
        int readCapacity = Integer.parseInt(readCap);
        getConf().setInt(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY, readCapacity);
      }
      String writeCap = getCommandFormat().getOptValue(WRITE_FLAG);
      if (writeCap != null && !writeCap.isEmpty()) {
        int writeCapacity = Integer.parseInt(writeCap);
        getConf().setInt(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY, writeCapacity);
      }

      // Validate parameters.
      if (!parseDynamoDBRegion(paths)) {
        System.err.println(USAGE);
        return INVALID_ARGUMENT;
      }
      initMetadataStore(true);
      return SUCCESS;
    }
  }

  /**
   * Destroy a metadata store.
   */
  static class Destroy extends S3GuardTool {
    private static final String NAME = "destroy";
    public static final String PURPOSE = "destroy Metadata Store data "
        + DATA_IN_S3_IS_PRESERVED;
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
          "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    Destroy(Configuration conf) {
      super(conf);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    public int run(String[] args) throws IOException {
      List<String> paths = parseArgs(args);
      if (!parseDynamoDBRegion(paths)) {
        System.err.println(USAGE);
        return INVALID_ARGUMENT;
      }

      try {
        initMetadataStore(false);
      } catch (FileNotFoundException e) {
        // indication that the table was not found
        LOG.debug("Failed to bind to store to be destroyed", e);
        LOG.info("Metadata Store does not exist.");
        return SUCCESS;
      }

      Preconditions.checkState(getStore() != null,
          "Metadata Store is not initialized");

      getStore().destroy();
      LOG.info("Metadata store is deleted.");
      return SUCCESS;
    }
  }

  /**
   * Import s3 metadata to the metadata store.
   */
  static class Import extends S3GuardTool {
    private static final String NAME = "import";
    public static final String PURPOSE = "import metadata from existing S3 " +
        "data";
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
        "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    private final Set<Path> dirCache = new HashSet<>();

    Import(Configuration conf) {
      super(conf);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    /**
     * Put parents into MS and cache if the parents are not presented.
     *
     * @param f the file or an empty directory.
     * @throws IOException on I/O errors.
     */
    private void putParentsIfNotPresent(FileStatus f) throws IOException {
      Preconditions.checkNotNull(f);
      Path parent = f.getPath().getParent();
      while (parent != null) {
        if (dirCache.contains(parent)) {
          return;
        }
        FileStatus dir = DynamoDBMetadataStore.makeDirStatus(parent,
            f.getOwner());
        getStore().put(new PathMetadata(dir));
        dirCache.add(parent);
        parent = parent.getParent();
      }
    }

    /**
     * Recursively import every path under path.
     * @return number of items inserted into MetadataStore
     * @throws IOException on I/O errors.
     */
    private long importDir(FileStatus status) throws IOException {
      Preconditions.checkArgument(status.isDirectory());
      RemoteIterator<LocatedFileStatus> it = getFilesystem()
          .listFilesAndEmptyDirectories(status.getPath(), true);
      long items = 0;

      while (it.hasNext()) {
        LocatedFileStatus located = it.next();
        FileStatus child;
        if (located.isDirectory()) {
          child = DynamoDBMetadataStore.makeDirStatus(located.getPath(),
              located.getOwner());
          dirCache.add(child.getPath());
        } else {
          child = new S3AFileStatus(located.getLen(),
              located.getModificationTime(),
              located.getPath(),
              located.getBlockSize(),
              located.getOwner());
        }
        putParentsIfNotPresent(child);
        getStore().put(new PathMetadata(child));
        items++;
      }
      return items;
    }

    @Override
    public int run(String[] args) throws IOException {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        System.err.println(getUsage());
        return INVALID_ARGUMENT;
      }
      String s3Path = paths.get(0);
      initS3AFileSystem(s3Path);

      URI uri;
      try {
        uri = new URI(s3Path);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      String filePath = uri.getPath();
      if (filePath.isEmpty()) {
        // If they specify a naked S3 URI (e.g. s3a://bucket), we'll consider
        // root to be the path
        filePath = "/";
      }
      Path path = new Path(filePath);
      FileStatus status = getFilesystem().getFileStatus(path);

      initMetadataStore(false);

      long items = 1;
      if (status.isFile()) {
        PathMetadata meta = new PathMetadata(status);
        getStore().put(meta);
      } else {
        items = importDir(status);
      }

      System.out.printf("Inserted %d items into Metadata Store%n", items);

      return SUCCESS;
    }
  }

  /**
   * Show diffs between the s3 and metadata store.
   */
  static class Diff extends S3GuardTool {
    private static final String NAME = "diff";
    public static final String PURPOSE = "report on delta between S3 and " +
        "repository";
    private static final String USAGE = NAME + " [OPTIONS] s3a://BUCKET\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
        "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    private static final String SEP = "\t";
    static final String S3_PREFIX = "S3";
    static final String MS_PREFIX = "MS";

    Diff(Configuration conf) {
      super(conf);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    /**
     * Formats the output of printing a FileStatus in S3guard diff tool.
     * @param status the status to print.
     * @return the string of output.
     */
    private static String formatFileStatus(FileStatus status) {
      return String.format("%s%s%d%s%s",
          status.isDirectory() ? "D" : "F",
          SEP,
          status.getLen(),
          SEP,
          status.getPath().toString());
    }

    /**
     * Compares metadata from 2 S3 FileStatus's to see if they differ.
     * @param thisOne
     * @param thatOne
     * @return true if the metadata is not identical
     */
    private static boolean differ(FileStatus thisOne, FileStatus thatOne) {
      Preconditions.checkArgument(!(thisOne == null && thatOne == null));
      return (thisOne == null || thatOne == null) ||
          (thisOne.getLen() != thatOne.getLen()) ||
          (thisOne.isDirectory() != thatOne.isDirectory()) ||
          (!thisOne.isDirectory() &&
              thisOne.getModificationTime() != thatOne.getModificationTime());
    }

    /**
     * Print difference, if any, between two file statuses to the output stream.
     *
     * @param msStatus file status from metadata store.
     * @param s3Status file status from S3.
     * @param out output stream.
     */
    private static void printDiff(FileStatus msStatus,
                                  FileStatus s3Status,
                                  PrintStream out) {
      Preconditions.checkArgument(!(msStatus == null && s3Status == null));
      if (msStatus != null && s3Status != null) {
        Preconditions.checkArgument(
            msStatus.getPath().equals(s3Status.getPath()),
            String.format("The path from metadata store and s3 are different:" +
            " ms=%s s3=%s", msStatus.getPath(), s3Status.getPath()));
      }

      if (differ(msStatus, s3Status)) {
        if (s3Status != null) {
          out.printf("%s%s%s%n", S3_PREFIX, SEP, formatFileStatus(s3Status));
        }
        if (msStatus != null) {
          out.printf("%s%s%s%n", MS_PREFIX, SEP, formatFileStatus(msStatus));
        }
      }
    }

    /**
     * Compare the metadata of the directory with the same path, on S3 and
     * the metadata store, respectively. If one of them is null, consider the
     * metadata of the directory and all its subdirectories are missing from
     * the source.
     *
     * Pass the FileStatus obtained from s3 and metadata store to avoid one
     * round trip to fetch the same metadata twice, because the FileStatus
     * hve already been obtained from listStatus() / listChildren operations.
     *
     * @param msDir the directory FileStatus obtained from the metadata store.
     * @param s3Dir the directory FileStatus obtained from S3.
     * @param out the output stream to generate diff results.
     * @throws IOException on I/O errors.
     */
    private void compareDir(FileStatus msDir, FileStatus s3Dir,
                            PrintStream out) throws IOException {
      Preconditions.checkArgument(!(msDir == null && s3Dir == null));
      if (msDir != null && s3Dir != null) {
        Preconditions.checkArgument(msDir.getPath().equals(s3Dir.getPath()),
            String.format("The path from metadata store and s3 are different:" +
             " ms=%s s3=%s", msDir.getPath(), s3Dir.getPath()));
      }

      Map<Path, FileStatus> s3Children = new HashMap<>();
      if (s3Dir != null && s3Dir.isDirectory()) {
        for (FileStatus status : getFilesystem().listStatus(s3Dir.getPath())) {
          s3Children.put(status.getPath(), status);
        }
      }

      Map<Path, FileStatus> msChildren = new HashMap<>();
      if (msDir != null && msDir.isDirectory()) {
        DirListingMetadata dirMeta =
            getStore().listChildren(msDir.getPath());

        if (dirMeta != null) {
          for (PathMetadata meta : dirMeta.getListing()) {
            FileStatus status = meta.getFileStatus();
            msChildren.put(status.getPath(), status);
          }
        }
      }

      Set<Path> allPaths = new HashSet<>(s3Children.keySet());
      allPaths.addAll(msChildren.keySet());

      for (Path path : allPaths) {
        FileStatus s3Status = s3Children.get(path);
        FileStatus msStatus = msChildren.get(path);
        printDiff(msStatus, s3Status, out);
        if ((s3Status != null && s3Status.isDirectory()) ||
            (msStatus != null && msStatus.isDirectory())) {
          compareDir(msStatus, s3Status, out);
        }
      }
      out.flush();
    }

    /**
     * Compare both metadata store and S3 on the same path.
     *
     * @param path the path to be compared.
     * @param out  the output stream to display results.
     * @throws IOException on I/O errors.
     */
    private void compareRoot(Path path, PrintStream out) throws IOException {
      Path qualified = getFilesystem().qualify(path);
      FileStatus s3Status = null;
      try {
        s3Status = getFilesystem().getFileStatus(qualified);
      } catch (FileNotFoundException e) {
      }
      PathMetadata meta = getStore().get(qualified);
      FileStatus msStatus = (meta != null && !meta.isDeleted()) ?
          meta.getFileStatus() : null;
      compareDir(msStatus, s3Status, out);
    }

    @VisibleForTesting
    public int run(String[] args, PrintStream out) throws IOException {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        out.println(USAGE);
        return INVALID_ARGUMENT;
      }
      String s3Path = paths.get(0);
      initS3AFileSystem(s3Path);
      initMetadataStore(true);

      URI uri;
      try {
        uri = new URI(s3Path);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      Path root;
      if (uri.getPath().isEmpty()) {
        root = new Path("/");
      } else {
        root = new Path(uri.getPath());
      }
      root = getFilesystem().qualify(root);
      compareRoot(root, out);
      out.flush();
      return SUCCESS;
    }

    @Override
    public int run(String[] args) throws IOException {
      return run(args, System.out);
    }
  }

  /**
   * Prune metadata that has not been modified recently.
   */
  static class Prune extends S3GuardTool {
    private static final String NAME = "prune";
    public static final String PURPOSE = "truncate older metadata from " +
        "repository "
        + DATA_IN_S3_IS_PRESERVED;;
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
        "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    Prune(Configuration conf) {
      super(conf);

      CommandFormat format = getCommandFormat();
      format.addOptionWithValue(DAYS_FLAG);
      format.addOptionWithValue(HOURS_FLAG);
      format.addOptionWithValue(MINUTES_FLAG);
      format.addOptionWithValue(SECONDS_FLAG);
    }

    @VisibleForTesting
    void setMetadataStore(MetadataStore ms) {
      Preconditions.checkNotNull(ms);
      this.setStore(ms);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    private long getDeltaComponent(TimeUnit unit, String arg) {
      String raw = getCommandFormat().getOptValue(arg);
      if (raw == null || raw.isEmpty()) {
        return 0;
      }
      Long parsed = Long.parseLong(raw);
      return unit.toMillis(parsed);
    }

    @VisibleForTesting
    public int run(String[] args, PrintStream out) throws
        InterruptedException, IOException {
      List<String> paths = parseArgs(args);
      if (!parseDynamoDBRegion(paths)) {
        System.err.println(USAGE);
        return INVALID_ARGUMENT;
      }
      initMetadataStore(false);

      Configuration conf = getConf();
      long confDelta = conf.getLong(Constants.S3GUARD_CLI_PRUNE_AGE, 0);

      long cliDelta = 0;
      cliDelta += getDeltaComponent(TimeUnit.DAYS, "days");
      cliDelta += getDeltaComponent(TimeUnit.HOURS, "hours");
      cliDelta += getDeltaComponent(TimeUnit.MINUTES, "minutes");
      cliDelta += getDeltaComponent(TimeUnit.SECONDS, "seconds");

      if (confDelta <= 0 && cliDelta <= 0) {
        System.err.println(
            "You must specify a positive age for metadata to prune.");
      }

      // A delta provided on the CLI overrides if one is configured
      long delta = confDelta;
      if (cliDelta > 0) {
        delta = cliDelta;
      }

      long now = System.currentTimeMillis();
      long divide = now - delta;

      getStore().prune(divide);

      out.flush();
      return SUCCESS;
    }

    @Override
    public int run(String[] args) throws InterruptedException, IOException {
      return run(args, System.out);
    }
  }

  private static S3GuardTool command;

  private static void printHelp() {
    if (command == null) {
      System.err.println("Usage: hadoop " + USAGE);
      System.err.println("\tperform S3Guard metadata store " +
          "administrative commands.");
    } else {
      System.err.println("Usage: hadoop " + command.getUsage());
    }
    System.err.println();
    System.err.println(COMMON_USAGE);
  }

  /**
   * Execute the command with the given arguments.
   *
   * @param args command specific arguments.
   * @param conf Hadoop configuration.
   * @return exit code.
   * @throws Exception on I/O errors.
   */
  public static int run(String[] args, Configuration conf) throws
      Exception {
    /* ToolRunner.run does this too, but we must do it before looking at
    subCommand or instantiating the cmd object below */
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length == 0) {
      printHelp();
      return INVALID_ARGUMENT;
    }
    final String subCommand = otherArgs[0];
    switch (subCommand) {
    case Init.NAME:
      command = new Init(conf);
      break;
    case Destroy.NAME:
      command = new Destroy(conf);
      break;
    case Import.NAME:
      command = new Import(conf);
      break;
    case Diff.NAME:
      command = new Diff(conf);
      break;
    case Prune.NAME:
      command = new Prune(conf);
      break;
    default:
      printHelp();
      return INVALID_ARGUMENT;
    }
    return ToolRunner.run(conf, command, otherArgs);
  }

  /**
   * Main entry point. Calls {@code System.exit()} on all execution paths.
   * @param args argument list
   */
  public static void main(String[] args) {
    try {
      int ret = run(args, new Configuration());
      System.exit(ret);
    } catch (CommandFormat.UnknownOptionException e) {
      System.err.println(e.getMessage());
      printHelp();
      System.exit(INVALID_ARGUMENT);
    } catch (Throwable e) {
      e.printStackTrace(System.err);
      System.exit(ERROR);
    }
  }
}
