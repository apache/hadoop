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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.MultipartUpload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.MultipartUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.LOG_EVENT;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.*;

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
      "\t" + BucketInfo.NAME + " - " + BucketInfo.PURPOSE + "\n" +
      "\t" + Uploads.NAME + " - " + Uploads.PURPOSE + "\n" +
      "\t" + Diff.NAME + " - " + Diff.PURPOSE + "\n" +
      "\t" + Prune.NAME + " - " + Prune.PURPOSE + "\n" +
      "\t" + SetCapacity.NAME + " - " +SetCapacity.PURPOSE + "\n";
  private static final String DATA_IN_S3_IS_PRESERVED
      = "(all data in S3 is preserved)";

  abstract public String getUsage();

  // Exit codes
  static final int SUCCESS = EXIT_SUCCESS;
  static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;
  static final int E_USAGE = EXIT_USAGE;
  static final int ERROR = EXIT_FAIL;
  static final int E_BAD_STATE = EXIT_NOT_ACCEPTABLE;
  static final int E_NOT_FOUND = EXIT_NOT_FOUND;

  private S3AFileSystem filesystem;
  private MetadataStore store;
  private final CommandFormat commandFormat;

  public static final String META_FLAG = "meta";

  // These are common to prune, upload subcommands.
  public static final String DAYS_FLAG = "days";
  public static final String HOURS_FLAG = "hours";
  public static final String MINUTES_FLAG = "minutes";
  public static final String SECONDS_FLAG = "seconds";
  public static final String AGE_OPTIONS_USAGE = "[-days <days>] "
      + "[-hours <hours>] [-minutes <minutes>] [-seconds <seconds>]";

  public static final String REGION_FLAG = "region";
  public static final String READ_FLAG = "read";
  public static final String WRITE_FLAG = "write";
  public static final String TAG_FLAG = "tag";

  /**
   * Constructor a S3Guard tool with HDFS configuration.
   * @param conf Configuration.
   * @param opts any boolean options to support
   */
  protected S3GuardTool(Configuration conf, String...opts) {
    super(conf);

    commandFormat = new CommandFormat(0, Integer.MAX_VALUE, opts);
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
   * @throws IOException on I/O errors.
   * @throws ExitUtil.ExitException on validation errors
   */
  void parseDynamoDBRegion(List<String> paths) throws IOException {
    Configuration conf = getConf();
    String fromCli = getCommandFormat().getOptValue(REGION_FLAG);
    String fromConf = conf.get(S3GUARD_DDB_REGION_KEY);
    boolean hasS3Path = !paths.isEmpty();

    if (fromCli != null) {
      if (fromCli.isEmpty()) {
        throw invalidArgs("No region provided with -" + REGION_FLAG + " flag");
      }
      if (hasS3Path) {
        throw invalidArgs("Providing both an S3 path and the"
            + " -" + REGION_FLAG
            + " flag is not supported. If you need to specify a different "
            + "region than the S3 bucket, configure " + S3GUARD_DDB_REGION_KEY);
      }
      conf.set(S3GUARD_DDB_REGION_KEY, fromCli);
      return;
    }

    if (fromConf != null) {
      if (fromConf.isEmpty()) {
        throw invalidArgs("No region provided with config %s",
            S3GUARD_DDB_REGION_KEY);
      }
      return;
    }

    if (hasS3Path) {
      String s3Path = paths.get(0);
      initS3AFileSystem(s3Path);
      return;
    }

    throw invalidArgs("No region found from -" + REGION_FLAG + " flag, " +
        "config, or S3 bucket");
  }

  private long getDeltaComponent(TimeUnit unit, String arg) {
    String raw = getCommandFormat().getOptValue(arg);
    if (raw == null || raw.isEmpty()) {
      return 0;
    }
    Long parsed = Long.parseLong(raw);
    return unit.toMillis(parsed);
  }

  /**
   * Convert all age options supplied to total milliseconds of time.
   * @return Sum of all age options, or zero if none were given.
   */
  long ageOptionsToMsec() {
    long cliDelta = 0;
    cliDelta += getDeltaComponent(TimeUnit.DAYS, DAYS_FLAG);
    cliDelta += getDeltaComponent(TimeUnit.HOURS, HOURS_FLAG);
    cliDelta += getDeltaComponent(TimeUnit.MINUTES, MINUTES_FLAG);
    cliDelta += getDeltaComponent(TimeUnit.SECONDS, SECONDS_FLAG);
    return cliDelta;
  }

  protected void addAgeOptions() {
    CommandFormat format = getCommandFormat();
    format.addOptionWithValue(DAYS_FLAG);
    format.addOptionWithValue(HOURS_FLAG);
    format.addOptionWithValue(MINUTES_FLAG);
    format.addOptionWithValue(SECONDS_FLAG);
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
      LOG.info("Create metadata store: {}", uri + " scheme: "
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
   * Create and initialize a new S3A FileSystem instance.
   * This instance is always created without S3Guard, so allowing
   * a previously created metastore to be patched in.
   *
   * Note: this is a bit convoluted as it needs to also handle the situation
   * of a per-bucket option in core-site.xml, which isn't easily overridden.
   * The new config and the setting of the values before any
   * {@code Configuration.get()} calls are critical.
   *
   * @param path s3a URI
   * @throws IOException failure to init filesystem
   * @throws ExitUtil.ExitException if the FS is not an S3A FS
   */
  void initS3AFileSystem(String path) throws IOException {
    URI uri = toUri(path);
    // Make sure that S3AFileSystem does not hold an actual MetadataStore
    // implementation.
    Configuration conf = new Configuration(getConf());
    String nullStore = NullMetadataStore.class.getName();
    conf.set(S3_METADATA_STORE_IMPL, nullStore);
    String bucket = uri.getHost();
    S3AUtils.setBucketOption(conf,
        bucket,
        S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);
    String updatedBucketOption = S3AUtils.getBucketOption(conf, bucket,
        S3_METADATA_STORE_IMPL);
    LOG.debug("updated bucket store option {}", updatedBucketOption);
    Preconditions.checkState(S3GUARD_METASTORE_NULL.equals(updatedBucketOption),
        "Expected bucket option to be %s but was %s",
        S3GUARD_METASTORE_NULL, updatedBucketOption);

    FileSystem fs = FileSystem.newInstance(uri, conf);
    if (!(fs instanceof S3AFileSystem)) {
      throw invalidArgs("URI %s is not a S3A file system: %s",
          uri, fs.getClass().getName());
    }
    filesystem = (S3AFileSystem) fs;
  }

  /**
   * Parse CLI arguments and returns the position arguments.
   * The options are stored in {@link #commandFormat}.
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

  @Override
  public final int run(String[] args) throws Exception {
    return run(args, System.out);
  }

  /**
   * Run the tool, capturing the output (if the tool supports that).
   *
   * As well as returning an exit code, the implementations can choose to
   * throw an instance of {@link ExitUtil.ExitException} with their exit
   * code set to the desired exit value. The exit code of auch an exception
   * is used for the tool's exit code, and the stack trace only logged at
   * debug.
   * @param args argument list
   * @param out output stream
   * @return the exit code to return.
   * @throws Exception on any failure
   * @throws ExitUtil.ExitException for an alternative clean exit
   */
  public abstract int run(String[] args, PrintStream out) throws Exception;

  /**
   * Create the metadata store.
   */
  static class Init extends S3GuardTool {
    public static final String NAME = "init";
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
        "  -" + TAG_FLAG + " key=value; list of tags to tag dynamo table\n" +
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
      // tag
      getCommandFormat().addOptionWithValue(TAG_FLAG);
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
    public int run(String[] args, PrintStream out) throws Exception {
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

      String tags = getCommandFormat().getOptValue(TAG_FLAG);
      if (tags != null && !tags.isEmpty()) {
        String[] stringList = tags.split(";");
        Map<String, String> tagsKV = new HashMap<>();
        for(String kv : stringList) {
          if(kv.isEmpty() || !kv.contains("=")){
            continue;
          }
          String[] kvSplit = kv.split("=");
          tagsKV.put(kvSplit[0], kvSplit[1]);
        }

        for (Map.Entry<String, String> kv : tagsKV.entrySet()) {
          getConf().set(S3GUARD_DDB_TABLE_TAG + kv.getKey(), kv.getValue());
        }
      }

      // Validate parameters.
      try {
        parseDynamoDBRegion(paths);
      } catch (ExitUtil.ExitException e) {
        errorln(USAGE);
        throw e;
      }
      MetadataStore store = initMetadataStore(true);
      printStoreDiagnostics(out, store);
      return SUCCESS;
    }
  }

  /**
   * Change the capacity of the metadata store.
   */
  static class SetCapacity extends S3GuardTool {
    public static final String NAME = "set-capacity";
    public static final String PURPOSE = "Alter metadata store IO capacity";
    public static final String READ_CAP_INVALID = "Read capacity must have "
        + "value greater than or equal to 1.";
    public static final String WRITE_CAP_INVALID = "Write capacity must have "
        + "value greater than or equal to 1.";
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
          "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + READ_FLAG + " UNIT - Provisioned read throughput units\n" +
        "  -" + WRITE_FLAG + " UNIT - Provisioned write through put units\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    SetCapacity(Configuration conf) {
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
    public int run(String[] args, PrintStream out) throws Exception {
      List<String> paths = parseArgs(args);
      Map<String, String> options = new HashMap<>();
      String s3Path = paths.get(0);

      // Check if DynamoDB url is set from arguments.
      String metadataStoreUri = getCommandFormat().getOptValue(META_FLAG);
      if(metadataStoreUri == null || metadataStoreUri.isEmpty()) {
        // If not set, check if filesystem is guarded by creating an
        // S3AFileSystem and check if hasMetadataStore is true
        try (S3AFileSystem s3AFileSystem = (S3AFileSystem)
            S3AFileSystem.newInstance(toUri(s3Path), getConf())){
          Preconditions.checkState(s3AFileSystem.hasMetadataStore(),
              "The S3 bucket is unguarded. " + getName()
                  + " can not be used on an unguarded bucket.");
        }
      }

      String readCap = getCommandFormat().getOptValue(READ_FLAG);
      if (StringUtils.isNotEmpty(readCap)) {
        Preconditions.checkArgument(Integer.parseInt(readCap) > 0,
            READ_CAP_INVALID);

        S3GuardTool.println(out, "Read capacity set to %s", readCap);
        options.put(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY, readCap);
      }
      String writeCap = getCommandFormat().getOptValue(WRITE_FLAG);
      if (StringUtils.isNotEmpty(writeCap)) {
        Preconditions.checkArgument(Integer.parseInt(writeCap) > 0,
            WRITE_CAP_INVALID);

        S3GuardTool.println(out, "Write capacity set to %s", writeCap);
        options.put(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY, writeCap);
      }

      // Validate parameters.
      try {
        parseDynamoDBRegion(paths);
      } catch (ExitUtil.ExitException e) {
        errorln(USAGE);
        throw e;
      }
      MetadataStore store = initMetadataStore(false);
      store.updateParameters(options);
      printStoreDiagnostics(out, store);
      return SUCCESS;
    }
  }


  /**
   * Destroy a metadata store.
   */
  static class Destroy extends S3GuardTool {
    public static final String NAME = "destroy";
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

    public int run(String[] args, PrintStream out) throws Exception {
      List<String> paths = parseArgs(args);
      try {
        parseDynamoDBRegion(paths);
      } catch (ExitUtil.ExitException e) {
        errorln(USAGE);
        throw e;
      }

      try {
        initMetadataStore(false);
      } catch (FileNotFoundException e) {
        // indication that the table was not found
        println(out, "Metadata Store does not exist.");
        LOG.debug("Failed to bind to store to be destroyed", e);
        return SUCCESS;
      }

      Preconditions.checkState(getStore() != null,
          "Metadata Store is not initialized");

      getStore().destroy();
      println(out, "Metadata store is deleted.");
      return SUCCESS;
    }
  }

  /**
   * Import s3 metadata to the metadata store.
   */
  static class Import extends S3GuardTool {
    public static final String NAME = "import";
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
    public int run(String[] args, PrintStream out) throws Exception {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        errorln(getUsage());
        throw invalidArgs("no arguments");
      }
      String s3Path = paths.get(0);
      initS3AFileSystem(s3Path);

      URI uri = toUri(s3Path);
      String filePath = uri.getPath();
      if (filePath.isEmpty()) {
        // If they specify a naked S3 URI (e.g. s3a://bucket), we'll consider
        // root to be the path
        filePath = "/";
      }
      Path path = new Path(filePath);
      FileStatus status = getFilesystem().getFileStatus(path);

      try {
        initMetadataStore(false);
      } catch (FileNotFoundException e) {
        throw storeNotFound(e);
      }

      long items = 1;
      if (status.isFile()) {
        PathMetadata meta = new PathMetadata(status);
        getStore().put(meta);
      } else {
        items = importDir(status);
      }

      println(out, "Inserted %d items into Metadata Store", items);

      return SUCCESS;
    }

  }

  /**
   * Show diffs between the s3 and metadata store.
   */
  static class Diff extends S3GuardTool {
    public static final String NAME = "diff";
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
          println(out, "%s%s%s", S3_PREFIX, SEP, formatFileStatus(s3Status));
        }
        if (msStatus != null) {
          println(out, "%s%s%s", MS_PREFIX, SEP, formatFileStatus(msStatus));
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
      Preconditions.checkArgument(!(msDir == null && s3Dir == null),
          "The path does not exist in metadata store and on s3.");

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
        /* ignored */
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
        throw invalidArgs("no arguments");
      }
      String s3Path = paths.get(0);
      initS3AFileSystem(s3Path);
      initMetadataStore(false);

      URI uri = toUri(s3Path);
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

  }

  /**
   * Prune metadata that has not been modified recently.
   */
  static class Prune extends S3GuardTool {
    public static final String NAME = "prune";
    public static final String PURPOSE = "truncate older metadata from " +
        "repository "
        + DATA_IN_S3_IS_PRESERVED;;
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
        "(implementation-specific)\n" +
        "Age options. Any combination of these integer-valued options:\n" +
        AGE_OPTIONS_USAGE + "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    Prune(Configuration conf) {
      super(conf);
      addAgeOptions();
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

    public int run(String[] args, PrintStream out) throws
        InterruptedException, IOException {
      List<String> paths = parseArgs(args);
      try {
        parseDynamoDBRegion(paths);
      } catch (ExitUtil.ExitException e) {
        errorln(USAGE);
        throw e;
      }
      initMetadataStore(false);

      Configuration conf = getConf();
      long confDelta = conf.getLong(S3GUARD_CLI_PRUNE_AGE, 0);

      long cliDelta = ageOptionsToMsec();

      if (confDelta <= 0 && cliDelta <= 0) {
        errorln("You must specify a positive age for metadata to prune.");
      }

      // A delta provided on the CLI overrides if one is configured
      long delta = confDelta;
      if (cliDelta > 0) {
        delta = cliDelta;
      }

      long now = System.currentTimeMillis();
      long divide = now - delta;

      // remove the protocol from path string to get keyPrefix
      // by default the keyPrefix is "/" - unless the s3 URL is provided
      String keyPrefix = "/";
      if(paths.size() > 0) {
        Path path = new Path(paths.get(0));
        keyPrefix = PathMetadataDynamoDBTranslation.pathToParentKey(path);
      }

      try {
        getStore().prune(divide, keyPrefix);
      } catch (UnsupportedOperationException e){
        errorln("Prune operation not supported in metadata store.");
      }

      out.flush();
      return SUCCESS;
    }

  }

  /**
   * Get info about a bucket and its S3Guard integration status.
   */
  static class BucketInfo extends S3GuardTool {
    public static final String NAME = "bucket-info";
    public static final String GUARDED_FLAG = "guarded";
    public static final String UNGUARDED_FLAG = "unguarded";
    public static final String AUTH_FLAG = "auth";
    public static final String NONAUTH_FLAG = "nonauth";
    public static final String ENCRYPTION_FLAG = "encryption";
    public static final String MAGIC_FLAG = "magic";

    public static final String PURPOSE = "provide/check S3Guard information"
        + " about a specific bucket";
    private static final String USAGE = NAME + " [OPTIONS] s3a://BUCKET\n"
        + "\t" + PURPOSE + "\n\n"
        + "Common options:\n"
        + "  -" + GUARDED_FLAG + " - Require S3Guard\n"
        + "  -" + UNGUARDED_FLAG + " - Require S3Guard to be disabled\n"
        + "  -" + AUTH_FLAG + " - Require the S3Guard mode to be \"authoritative\"\n"
        + "  -" + NONAUTH_FLAG + " - Require the S3Guard mode to be \"non-authoritative\"\n"
        + "  -" + MAGIC_FLAG + " - Require the S3 filesystem to be support the \"magic\" committer\n"
        + "  -" + ENCRYPTION_FLAG
        + " -require {none, sse-s3, sse-kms} - Require encryption policy";

    BucketInfo(Configuration conf) {
      super(conf, GUARDED_FLAG, UNGUARDED_FLAG, AUTH_FLAG, NONAUTH_FLAG, MAGIC_FLAG);
      CommandFormat format = getCommandFormat();
      format.addOptionWithValue(ENCRYPTION_FLAG);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    public int run(String[] args, PrintStream out)
        throws InterruptedException, IOException {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        errorln(getUsage());
        throw invalidArgs("No bucket specified");
      }
      String s3Path = paths.get(0);
      S3AFileSystem fs = (S3AFileSystem) FileSystem.newInstance(
          toUri(s3Path), getConf());
      setFilesystem(fs);
      Configuration conf = fs.getConf();
      URI fsUri = fs.getUri();
      MetadataStore store = fs.getMetadataStore();
      println(out, "Filesystem %s", fsUri);
      println(out, "Location: %s", fs.getBucketLocation());
      boolean usingS3Guard = !(store instanceof NullMetadataStore);
      boolean authMode = false;
      if (usingS3Guard) {
        out.printf("Filesystem %s is using S3Guard with store %s%n",
            fsUri, store.toString());
        printOption(out, "Authoritative S3Guard",
            METADATASTORE_AUTHORITATIVE, "false");
        authMode = conf.getBoolean(METADATASTORE_AUTHORITATIVE, false);
        printStoreDiagnostics(out, store);
      } else {
        println(out, "Filesystem %s is not using S3Guard", fsUri);
      }
      boolean magic = fs.hasCapability(
          CommitConstants.STORE_CAPABILITY_MAGIC_COMMITTER);
      println(out, "The \"magic\" committer %s supported",
          magic ? "is" : "is not");

      println(out, "%nS3A Client");

      String endpoint = conf.getTrimmed(ENDPOINT, "");
      println(out, "\tEndpoint: %s=%s",
          ENDPOINT,
          StringUtils.isNotEmpty(endpoint) ? endpoint : "(unset)");
      String encryption =
          printOption(out, "\tEncryption", SERVER_SIDE_ENCRYPTION_ALGORITHM,
              "none");
      printOption(out, "\tInput seek policy", INPUT_FADVISE, INPUT_FADV_NORMAL);

      CommandFormat commands = getCommandFormat();
      if (usingS3Guard) {
        if (commands.getOpt(UNGUARDED_FLAG)) {
          throw badState("S3Guard is enabled for %s", fsUri);
        }
        if (commands.getOpt(AUTH_FLAG) && !authMode) {
          throw badState("S3Guard is enabled for %s,"
              + " but not in authoritative mode", fsUri);
        }
        if (commands.getOpt(NONAUTH_FLAG) && authMode) {
          throw badState("S3Guard is enabled in authoritative mode for %s",
              fsUri);
        }
      } else {
        if (commands.getOpt(GUARDED_FLAG)) {
          throw badState("S3Guard is not enabled for %s", fsUri);
        }
      }
      if (commands.getOpt(MAGIC_FLAG) && !magic) {
        throw badState("The magic committer is not enabled for %s", fsUri);
      }

      String desiredEncryption = getCommandFormat()
          .getOptValue(ENCRYPTION_FLAG);
      if (StringUtils.isNotEmpty(desiredEncryption)
          && !desiredEncryption.equalsIgnoreCase(encryption)) {
        throw badState("Bucket %s: required encryption is %s"
                    + " but actual encryption is %s",
                fsUri, desiredEncryption, encryption);
      }

      out.flush();
      return SUCCESS;
    }

    private String printOption(PrintStream out,
        String description, String key, String defVal) {
      String t = getFilesystem().getConf().getTrimmed(key, defVal);
      println(out, "%s: %s=%s", description, key, t);
      return t;
    }

  }

  /**
   * Command to list / abort pending multipart uploads.
   */
  static class Uploads extends S3GuardTool {
    public static final String NAME = "uploads";
    public static final String ABORT = "abort";
    public static final String LIST = "list";
    public static final String EXPECT = "expect";
    public static final String VERBOSE = "verbose";
    public static final String FORCE = "force";

    public static final String PURPOSE = "list or abort pending " +
        "multipart uploads";
    private static final String USAGE = NAME + " [OPTIONS] " +
        "s3a://BUCKET[/path]\n"
        + "\t" + PURPOSE + "\n\n"
        + "Common options:\n"
        + " (-" + LIST + " | -" + EXPECT +" <num-uploads> | -" + ABORT
        + ") [-" + VERBOSE +"] "
        + "[<age-options>] [-force]\n"
        + "\t - Under given path, list or delete all uploads," +
        " or only those \n"
        + "older than specified by <age-options>\n"
        + "<age-options> are any combination of the integer-valued options:\n"
        + "\t" + AGE_OPTIONS_USAGE + "\n"
        + "-" + EXPECT + " is similar to list, except no output is printed,\n"
        + "\tbut the exit code will be an error if the provided number\n"
        + "\tis different that the number of uploads found by the command.\n"
        + "-" + FORCE + " option prevents the \"Are you sure\" prompt when\n"
        + "\tusing -" + ABORT;

    /** Constant used for output and parsed by tests. */
    public static final String TOTAL = "Total";

    /** Runs in one of three modes. */
    private enum Mode { LIST, EXPECT, ABORT };
    private Mode mode = null;

    /** For Mode == EXPECT, expected listing size. */
    private int expectedCount;

    /** List/abort uploads older than this many milliseconds. */
    private long ageMsec = 0;

    /** Verbose output flag. */
    private boolean verbose = false;

    /** Whether to delete with out "are you sure" prompt. */
    private boolean force = false;

    /** Path prefix to use when searching multipart uploads. */
    private String prefix;

    Uploads(Configuration conf) {
      super(conf, ABORT, LIST, VERBOSE, FORCE);
      addAgeOptions();
      getCommandFormat().addOptionWithValue(EXPECT);
    }

    @Override
    String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    public int run(String[] args, PrintStream out)
        throws InterruptedException, IOException {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        errorln(getUsage());
        throw invalidArgs("No options specified");
      }
      processArgs(paths, out);
      promptBeforeAbort(out);
      processUploads(out);

      out.flush();
      return SUCCESS;
    }

    private void promptBeforeAbort(PrintStream out) throws IOException {
      if (mode != Mode.ABORT || force) {
        return;
      }
      Scanner scanner = new Scanner(System.in, "UTF-8");
      out.println("Are you sure you want to delete any pending " +
          "uploads? (yes/no) >");
      String response = scanner.nextLine();
      if (!"yes".equalsIgnoreCase(response)) {
        throw S3GuardTool.userAborted("User did not answer yes, quitting.");
      }
    }

    private void processUploads(PrintStream out) throws IOException {
      MultipartUtils.UploadIterator uploads;
      uploads = getFilesystem().listUploads(prefix);

      int count = 0;
      while (uploads.hasNext()) {
        MultipartUpload upload = uploads.next();
        if (!olderThan(upload, ageMsec)) {
          continue;
        }
        count++;
        if (mode == Mode.ABORT || mode == Mode.LIST || verbose) {
          println(out, "%s%s %s", mode == Mode.ABORT ? "Deleting: " : "",
              upload.getKey(), upload.getUploadId());
        }
        if (mode == Mode.ABORT) {
          getFilesystem().getWriteOperationHelper()
              .abortMultipartUpload(upload.getKey(), upload.getUploadId(),
                  LOG_EVENT);
        }
      }
      if (mode != Mode.EXPECT || verbose) {
        println(out, "%s %d uploads %s.", TOTAL, count,
            mode == Mode.ABORT ? "deleted" : "found");
      }
      if (mode == Mode.EXPECT) {
        if (count != expectedCount) {
          throw badState("Expected %d uploads, found %d", expectedCount, count);
        }
      }
    }

    /**
     * Check if upload is at least as old as given age.
     * @param u upload to check
     * @param msec age in milliseconds
     * @return true iff u was created at least age milliseconds ago.
     */
    private boolean olderThan(MultipartUpload u, long msec) {
      Date ageDate = new Date(System.currentTimeMillis() - msec);
      return ageDate.compareTo(u.getInitiated()) >= 0;
    }

    private void processArgs(List<String> args, PrintStream out)
        throws IOException {
      CommandFormat commands = getCommandFormat();
      String err = "Can only specify one of -" + LIST + ", " +
          " -" + ABORT + ", and " + EXPECT;

      // Three mutually-exclusive options
      if (commands.getOpt(LIST)) {
        mode = Mode.LIST;
      }
      if (commands.getOpt(ABORT)) {
        if (mode != null) {
          throw invalidArgs(err);
        }
        mode = Mode.ABORT;
      }

      String expectVal = commands.getOptValue(EXPECT);
      if (expectVal != null) {
        if (mode != null) {
          throw invalidArgs(err);
        }
        mode = Mode.EXPECT;
        expectedCount = Integer.parseInt(expectVal);
      }

      // Default to list
      if (mode == null) {
        vprintln(out, "No mode specified, defaulting to -" + LIST);
        mode = Mode.LIST;
      }

      // Other flags
      if (commands.getOpt(VERBOSE)) {
        verbose = true;
      }
      if (commands.getOpt(FORCE)) {
        force = true;
      }
      ageMsec = ageOptionsToMsec();

      String s3Path = args.get(0);
      URI uri = S3GuardTool.toUri(s3Path);
      prefix = uri.getPath();
      if (prefix.length() > 0) {
        prefix = prefix.substring(1);
      }
      vprintln(out, "Command: %s, age %d msec, path %s (prefix \"%s\")",
          mode.name(), ageMsec, s3Path, prefix);

      initS3AFileSystem(s3Path);
    }

    /**
     * If verbose flag is set, print a formatted string followed by a newline
     * to the output stream.
     * @param out destination
     * @param format format string
     * @param args optional arguments
     */
    private void vprintln(PrintStream out, String format, Object...
        args) {
      if (verbose) {
        out.println(String.format(format, args));
      }
    }
  }

  private static S3GuardTool command;

  /**
   * Convert a path to a URI, catching any {@code URISyntaxException}
   * and converting to an invalid args exception.
   * @param s3Path path to convert to a URI
   * @return a URI of the path
   * @throws ExitUtil.ExitException INVALID_ARGUMENT if the URI is invalid
   */
  protected static URI toUri(String s3Path) {
    URI uri;
    try {
      uri = new URI(s3Path);
    } catch (URISyntaxException e) {
      throw invalidArgs("Not a valid fileystem path: %s", s3Path);
    }
    return uri;
  }

  private static void printHelp() {
    if (command == null) {
      errorln("Usage: hadoop " + USAGE);
      errorln("\tperform S3Guard metadata store " +
          "administrative commands.");
    } else {
      errorln("Usage: hadoop " + command.getUsage());
    }
    errorln();
    errorln(COMMON_USAGE);
  }

  private static void errorln() {
    System.err.println();
  }

  private static void errorln(String x) {
    System.err.println(x);
  }

  /**
   * Print a formatted string followed by a newline to the output stream.
   * @param out destination
   * @param format format string
   * @param args optional arguments
   */
  private static void println(PrintStream out, String format, Object... args) {
    out.println(String.format(format, args));
  }

  /**
   * Retrieve and Print store diagnostics.
   * @param out output stream
   * @param store store
   * @throws IOException Failure to retrieve the data.
   */
  protected static void printStoreDiagnostics(PrintStream out,
      MetadataStore store)
      throws IOException {
    Map<String, String> diagnostics = store.getDiagnostics();
    out.println("Metadata Store Diagnostics:");
    for (Map.Entry<String, String> entry : diagnostics.entrySet()) {
      println(out, "\t%s=%s", entry.getKey(), entry.getValue());
    }
  }


  /**
   * Handle store not found by converting to an exit exception
   * with specific error code.
   * @param e exception
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException storeNotFound(
      FileNotFoundException e) {
    return new ExitUtil.ExitException(
        E_NOT_FOUND, e.toString(), e);
  }

  /**
   * Build the exception to raise on invalid arguments.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException invalidArgs(
      String format, Object...args) {
    return new ExitUtil.ExitException(INVALID_ARGUMENT,
        String.format(format, args));
  }

  /**
   * Build the exception to raise on a bad store/bucket state.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException badState(
      String format, Object...args) {
    return new ExitUtil.ExitException(E_BAD_STATE,
        String.format(format, args));
  }

  /**
   * Build the exception to raise on user-aborted action.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException userAborted(
      String format, Object...args) {
    return new ExitUtil.ExitException(ERROR, String.format(format, args));
  }

  /**
   * Execute the command with the given arguments.
   *
   * @param conf Hadoop configuration.
   * @param args command specific arguments.
   * @return exit code.
   * @throws Exception on I/O errors.
   */
  public static int run(Configuration conf, String...args) throws
      Exception {
    /* ToolRunner.run does this too, but we must do it before looking at
    subCommand or instantiating the cmd object below */
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length == 0) {
      printHelp();
      throw new ExitUtil.ExitException(E_USAGE, "No arguments provided");
    }
    final String subCommand = otherArgs[0];
    LOG.debug("Executing command {}", subCommand);
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
    case BucketInfo.NAME:
      command = new BucketInfo(conf);
      break;
    case Diff.NAME:
      command = new Diff(conf);
      break;
    case Prune.NAME:
      command = new Prune(conf);
      break;
    case SetCapacity.NAME:
      command = new SetCapacity(conf);
      break;
    case Uploads.NAME:
      command = new Uploads(conf);
      break;
    default:
      printHelp();
      throw new ExitUtil.ExitException(E_USAGE,
          "Unknown command " + subCommand);
    }
    return ToolRunner.run(conf, command, otherArgs);
  }

  /**
   * Main entry point. Calls {@code System.exit()} on all execution paths.
   * @param args argument list
   */
  public static void main(String[] args) {
    try {
      int ret = run(new Configuration(), args);
      exit(ret, "");
    } catch (CommandFormat.UnknownOptionException e) {
      errorln(e.getMessage());
      printHelp();
      exit(E_USAGE, e.getMessage());
    } catch (ExitUtil.ExitException e) {
      // explicitly raised exit code
      exit(e.getExitCode(), e.toString());
    } catch (Throwable e) {
      e.printStackTrace(System.err);
      exit(ERROR, e.toString());
    }
  }

  protected static void exit(int status, String text) {
    ExitUtil.terminate(status, text);
  }
}
