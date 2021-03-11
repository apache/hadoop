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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.MultipartUpload;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.s3a.MultipartUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.RolePolicies;
import org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicy;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicyImpl;
import org.apache.hadoop.fs.s3a.select.SelectTool;
import org.apache.hadoop.fs.s3a.tools.MarkerTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.LOG_EVENT;
import static org.apache.hadoop.fs.s3a.S3AUtils.clearBucketOption;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.FILESYSTEM_TEMP_PATH;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStoreTableManager.SSE_DEFAULT_MASTER_KEY;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.*;

/**
 * CLI to manage S3Guard Metadata Store.
 * <p></p>
 * Some management tools invoke this class directly.
 */
@InterfaceAudience.LimitedPrivate("management tools")
@InterfaceStability.Evolving
public abstract class S3GuardTool extends Configured implements Tool,
    Closeable {
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
      "\t" + Authoritative.NAME + " - " + Authoritative.PURPOSE + "\n" +
      "\t" + BucketInfo.NAME + " - " + BucketInfo.PURPOSE + "\n" +
      "\t" + Diff.NAME + " - " + Diff.PURPOSE + "\n" +
      "\t" + Fsck.NAME + " - " + Fsck.PURPOSE + "\n" +
      "\t" + Import.NAME + " - " + Import.PURPOSE + "\n" +
      "\t" + MarkerTool.MARKERS + " - " + MarkerTool.PURPOSE + "\n" +
      "\t" + Prune.NAME + " - " + Prune.PURPOSE + "\n" +
      "\t" + SetCapacity.NAME + " - " + SetCapacity.PURPOSE + "\n" +
      "\t" + SelectTool.NAME + " - " + SelectTool.PURPOSE + "\n" +
      "\t" + Uploads.NAME + " - " + Uploads.PURPOSE + "\n";

  private static final String DATA_IN_S3_IS_PRESERVED
      = "(all data in S3 is preserved)";

  public abstract String getUsage();

  // Exit codes
  static final int SUCCESS = EXIT_SUCCESS;
  static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;
  static final int E_USAGE = EXIT_USAGE;

  static final int ERROR = EXIT_FAIL;
  static final int E_BAD_STATE = EXIT_NOT_ACCEPTABLE;
  static final int E_NOT_FOUND = EXIT_NOT_FOUND;

  /** Error String when the wrong FS is used for binding: {@value}. **/
  @VisibleForTesting
  public static final String WRONG_FILESYSTEM = "Wrong filesystem for ";

  /**
   * The FS we close when we are closed.
   */
  private FileSystem baseFS;
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
  public static final String SSE_FLAG = "sse";
  public static final String CMK_FLAG = "cmk";
  public static final String TAG_FLAG = "tag";

  public static final String VERBOSE = "verbose";

  /**
   * Constructor a S3Guard tool with HDFS configuration.
   * @param conf Configuration.
   * @param opts any boolean options to support
   */
  protected S3GuardTool(Configuration conf, String...opts) {
    super(conf);

    // Set s3guard is off warn level to silent, as the fs is often instantiated
    // without s3guard on purpose.
    conf.set(S3GUARD_DISABLED_WARN_LEVEL,
        S3Guard.DisabledWarnLevel.SILENT.toString());

    commandFormat = new CommandFormat(0, Integer.MAX_VALUE, opts);
    // For metadata store URI
    commandFormat.addOptionWithValue(META_FLAG);
    // DDB region.
    commandFormat.addOptionWithValue(REGION_FLAG);
  }

  /**
   * Return sub-command name.
   * @return sub-command name.
   */
  public abstract String getName();

  /**
   * Close the FS and metastore.
   * @throws IOException on failure.
   */
  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG,
        baseFS, store);
    baseFS = null;
    filesystem = null;
    store = null;
  }

  /**
   * Parse DynamoDB region from either -m option or a S3 path.
   *
   * Note that as a side effect, if the paths included an S3 path,
   * and there is no region set on the CLI, then the S3A FS is
   * initialized, after which {@link #filesystem} will no longer
   * be null.
   *
   * @param paths remaining parameters from CLI.
   * @throws IOException on I/O errors.
   * @throws ExitUtil.ExitException on validation errors
   */
  protected void parseDynamoDBRegion(List<String> paths) throws IOException {
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

  protected void checkIfS3BucketIsGuarded(List<String> paths)
      throws IOException {
    // be sure that path is provided in params, so there's no IOoBE
    String s3Path = "";
    if(!paths.isEmpty()) {
      s3Path = paths.get(0);
    }

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
  }

  /**
   * Check if bucket or DDB table name is set.
   * @param paths position arguments in which S3 path is provided.
   */
  protected void checkBucketNameOrDDBTableNameProvided(List<String> paths) {
    String s3Path = null;
    if(!paths.isEmpty()) {
      s3Path = paths.get(0);
    }

    String metadataStoreUri = getCommandFormat().getOptValue(META_FLAG);

    if(metadataStoreUri == null && s3Path == null) {
      throw invalidArgs("S3 bucket url or DDB table name have to be provided "
          + "explicitly to use " + getName() + " command.");
    }
  }

  /**
   * Parse metadata store from command line option or HDFS configuration.
   *
   * @param forceCreate override the auto-creation setting to true.
   * @return a initialized metadata store.
   * @throws IOException on unsupported metadata store.
   */
  protected MetadataStore initMetadataStore(boolean forceCreate)
      throws IOException {
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
      getStore().initialize(conf, new S3Guard.TtlTimeProvider(conf));
    } else {
      getStore().initialize(filesystem, new S3Guard.TtlTimeProvider(conf));
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
  protected void initS3AFileSystem(String path) throws IOException {
    LOG.debug("Initializing S3A FS to {}", path);
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

    bindFilesystem(FileSystem.newInstance(uri, conf));
  }

  /**
   * Initialize the filesystem if there is none bonded to already and
   * the command line path list is not empty.
   * @param paths path list.
   * @return true if at the end of the call, getFilesystem() is not null
   * @throws IOException failure to instantiate.
   */
  @VisibleForTesting
  boolean maybeInitFilesystem(final List<String> paths)
      throws IOException {
    // is there an S3 FS to create?
    if (getFilesystem() == null) {
      // none yet -create one
      if (!paths.isEmpty()) {
        initS3AFileSystem(paths.get(0));
      } else {
        LOG.debug("No path on command line, so not instantiating FS");
      }
    }
    return getFilesystem() != null;
  }

  /**
   * Parse CLI arguments and returns the position arguments.
   * The options are stored in {@link #commandFormat}.
   *
   * @param args command line arguments.
   * @return the position arguments from CLI.
   */
  protected List<String> parseArgs(String[] args) {
    return getCommandFormat().parse(args, 1);
  }

  protected S3AFileSystem getFilesystem() {
    return filesystem;
  }

  /**
   * Sets the filesystem; it must be an S3A FS instance, or a FilterFS
   * around an S3A Filesystem.
   * @param bindingFS filesystem to bind to
   * @return the bound FS.
   * @throws ExitUtil.ExitException if the FS is not an S3 FS
   */
  protected S3AFileSystem bindFilesystem(FileSystem bindingFS) {
    FileSystem fs = bindingFS;
    baseFS = bindingFS;
    while (fs instanceof FilterFileSystem) {
      fs = ((FilterFileSystem) fs).getRawFileSystem();
    }
    if (!(fs instanceof S3AFileSystem)) {
      throw new ExitUtil.ExitException(EXIT_SERVICE_UNAVAILABLE,
          WRONG_FILESYSTEM + "URI " + fs.getUri() + " : "
          + fs.getClass().getName());
    }
    filesystem = (S3AFileSystem) fs;
    return filesystem;
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

  /**
   * Reset the store and filesystem bindings.
   */
  protected void resetBindings() {
    store = null;
    filesystem = null;
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
   * throw an instance of {@code ExitUtil.ExitException} with their exit
   * code set to the desired exit value. The exit code of such an exception
   * is used for the tool's exit code, and the stack trace only logged at
   * debug.
   * @param args argument list
   * @param out output stream
   * @return the exit code to return.
   * @throws Exception on any failure
   */
  public abstract int run(String[] args, PrintStream out) throws Exception,
      ExitUtil.ExitException;

  /**
   * Dump the filesystem Storage Statistics if the FS is not null.
   * Only non-zero statistics are printed.
   * @param stream output stream
   */
  protected void dumpFileSystemStatistics(PrintStream stream) {
    FileSystem fs = getFilesystem();
    if (fs == null) {
      return;
    }
    println(stream, "%nStorage Statistics for %s%n", fs.getUri());
    StorageStatistics st = fs.getStorageStatistics();
    Iterator<StorageStatistics.LongStatistic> it
        = st.getLongStatistics();
    while (it.hasNext()) {
      StorageStatistics.LongStatistic next = it.next();
      long value = next.getValue();
      if (value != 0) {
        println(stream, "%s\t%s", next.getName(), value);
      }
    }
    println(stream, "");
  }

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
        "  -" + SSE_FLAG + " - Enable server side encryption\n" +
        "  -" + CMK_FLAG + " KEY - Customer managed CMK\n" +
        "  -" + TAG_FLAG + " key=value; list of tags to tag dynamo table\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.\n"
        + "To create a table with per-request billing, set the read and write\n"
        + "capacities to 0";

    Init(Configuration conf) {
      super(conf, SSE_FLAG);
      // read capacity.
      getCommandFormat().addOptionWithValue(READ_FLAG);
      // write capacity.
      getCommandFormat().addOptionWithValue(WRITE_FLAG);
      // customer managed customer master key (CMK) for server side encryption
      getCommandFormat().addOptionWithValue(CMK_FLAG);
      // tag
      getCommandFormat().addOptionWithValue(TAG_FLAG);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public int run(String[] args, PrintStream out) throws Exception {
      List<String> paths = parseArgs(args);
      try {
        checkBucketNameOrDDBTableNameProvided(paths);
      } catch (ExitUtil.ExitException e) {
        errorln(USAGE);
        throw e;
      }
      CommandFormat commands = getCommandFormat();
      String readCap = commands.getOptValue(READ_FLAG);
      if (readCap != null && !readCap.isEmpty()) {
        int readCapacity = Integer.parseInt(readCap);
        getConf().setInt(S3GUARD_DDB_TABLE_CAPACITY_READ_KEY, readCapacity);
      }
      String writeCap = commands.getOptValue(WRITE_FLAG);
      if (writeCap != null && !writeCap.isEmpty()) {
        int writeCapacity = Integer.parseInt(writeCap);
        getConf().setInt(S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY, writeCapacity);
      }
      if (!paths.isEmpty()) {
        String s3path = paths.get(0);
        URI fsURI = new URI(s3path);
        Configuration bucketConf = propagateBucketOptions(getConf(),
            fsURI.getHost());
        setConf(bucketConf);
      }

      String cmk = commands.getOptValue(CMK_FLAG);
      if (commands.getOpt(SSE_FLAG)) {
        getConf().setBoolean(S3GUARD_DDB_TABLE_SSE_ENABLED, true);
        LOG.debug("SSE flag is passed to command {}", this.getName());
        if (!StringUtils.isEmpty(cmk)) {
          if (SSE_DEFAULT_MASTER_KEY.equals(cmk)) {
            LOG.warn("Ignoring default DynamoDB table KMS Master Key " +
                "alias/aws/dynamodb in configuration");
          } else {
            LOG.debug("Setting customer managed CMK {}", cmk);
            getConf().set(S3GUARD_DDB_TABLE_SSE_CMK, cmk);
          }
        }
      } else if (!StringUtils.isEmpty(cmk)) {
        throw invalidArgs("Option %s can only be used with option %s",
            CMK_FLAG, SSE_FLAG);
      }

      String tags = commands.getOptValue(TAG_FLAG);
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
    public String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public int run(String[] args, PrintStream out) throws Exception {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        errorln(getUsage());
        throw invalidArgs("no arguments");
      }
      Map<String, String> options = new HashMap<>();
      checkIfS3BucketIsGuarded(paths);

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
        maybeInitFilesystem(paths);
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
    public static final String PURPOSE = "destroy the Metadata Store including its"
        + " contents" + DATA_IN_S3_IS_PRESERVED;
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
    public String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    public int run(String[] args, PrintStream out) throws Exception {
      List<String> paths = parseArgs(args);
      try {
        checkBucketNameOrDDBTableNameProvided(paths);
        checkIfS3BucketIsGuarded(paths);
        parseDynamoDBRegion(paths);
        maybeInitFilesystem(paths);
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

      try {
        getStore().destroy();
      } catch (TableDeleteTimeoutException e) {
        LOG.warn("The table is been deleted but it is still (briefly)"
            + " listed as present in AWS");
        LOG.debug("Timeout waiting for table disappearing", e);
      }
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
    public static final String AUTH_FLAG = "authoritative";
    private static final String USAGE = NAME + " [OPTIONS] [s3a://PATH]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + AUTH_FLAG + " - Mark imported directory data as authoritative.\n" +
        "  -" + VERBOSE + " - Verbose Output.\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
        "(implementation-specific)\n" +
        "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    Import(Configuration conf) {
      super(conf, AUTH_FLAG, VERBOSE);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
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
      S3AFileStatus status = (S3AFileStatus) getFilesystem()
          .getFileStatus(path);

      try {
        initMetadataStore(false);
      } catch (FileNotFoundException e) {
        throw storeNotFound(e);
      }

      final CommandFormat commandFormat = getCommandFormat();

      final ImportOperation importer = new ImportOperation(
          getFilesystem(),
          getStore(),
          status,
          commandFormat.getOpt(AUTH_FLAG),
          commandFormat.getOpt(VERBOSE));
      long items = importer.execute();
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
    public String getName() {
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
      Path qualified = getFilesystem().makeQualified(path);
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
      root = getFilesystem().makeQualified(root);
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

    public static final String TOMBSTONE = "tombstone";

    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + META_FLAG + " URL - Metadata repository details " +
        "(implementation-specific)\n" +
        "[-" + TOMBSTONE + "]\n" +
        "Age options. Any combination of these integer-valued options:\n" +
        AGE_OPTIONS_USAGE + "\n" +
        "Amazon DynamoDB-specific options:\n" +
        "  -" + REGION_FLAG + " REGION - Service region for connections\n" +
        "\n" +
        "  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.\n" +
        "  Specifying both the -" + REGION_FLAG + " option and an S3A path\n" +
        "  is not supported.";

    Prune(Configuration conf) {
      super(conf, TOMBSTONE);
      addAgeOptions();
    }

    @VisibleForTesting
    void setMetadataStore(MetadataStore ms) {
      Preconditions.checkNotNull(ms);
      this.setStore(ms);
    }

    @Override
    public String getName() {
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
        checkBucketNameOrDDBTableNameProvided(paths);
        parseDynamoDBRegion(paths);
      } catch (ExitUtil.ExitException e) {
        errorln(USAGE);
        throw e;
      }
      maybeInitFilesystem(paths);
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

      MetadataStore.PruneMode mode
          = MetadataStore.PruneMode.ALL_BY_MODTIME;
      if (getCommandFormat().getOpt(TOMBSTONE)) {
        mode = MetadataStore.PruneMode.TOMBSTONES_BY_LASTUPDATED;
      }
      try {
        getStore().prune(mode, divide,
            keyPrefix);
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
  public static class BucketInfo extends S3GuardTool {
    public static final String BUCKET_INFO = "bucket-info";
    public static final String NAME = BUCKET_INFO;
    public static final String GUARDED_FLAG = "guarded";
    public static final String UNGUARDED_FLAG = "unguarded";
    public static final String AUTH_FLAG = "auth";
    public static final String NONAUTH_FLAG = "nonauth";
    public static final String ENCRYPTION_FLAG = "encryption";
    public static final String MAGIC_FLAG = "magic";
    public static final String MARKERS_FLAG = "markers";
    public static final String MARKERS_AWARE = "aware";

    public static final String PURPOSE = "provide/check S3Guard information"
        + " about a specific bucket";

    private static final String USAGE = NAME + " [OPTIONS] s3a://BUCKET\n"
        + "\t" + PURPOSE + "\n\n"
        + "Common options:\n"
        + "  -" + GUARDED_FLAG + " - Require S3Guard\n"
        + "  -" + UNGUARDED_FLAG + " - Force S3Guard to be disabled\n"
        + "  -" + AUTH_FLAG + " - Require the S3Guard mode to be \"authoritative\"\n"
        + "  -" + NONAUTH_FLAG + " - Require the S3Guard mode to be \"non-authoritative\"\n"
        + "  -" + MAGIC_FLAG + " - Require the S3 filesystem to be support the \"magic\" committer\n"
        + "  -" + ENCRYPTION_FLAG
        + " (none, sse-s3, sse-kms) - Require encryption policy\n"
        + "  -" + MARKERS_FLAG
        + " (aware, keep, delete, authoritative) - directory markers policy\n";

    /**
     * Output when the client cannot get the location of a bucket.
     */
    @VisibleForTesting
    public static final String LOCATION_UNKNOWN =
        "Location unknown -caller lacks "
            + RolePolicies.S3_GET_BUCKET_LOCATION + " permission";


    @VisibleForTesting
    public static final String IS_MARKER_AWARE =
        "\tThe S3A connector is compatible with buckets where"
            + " directory markers are not deleted";

    public BucketInfo(Configuration conf) {
      super(conf, GUARDED_FLAG, UNGUARDED_FLAG, AUTH_FLAG, NONAUTH_FLAG, MAGIC_FLAG);
      CommandFormat format = getCommandFormat();
      format.addOptionWithValue(ENCRYPTION_FLAG);
      format.addOptionWithValue(MARKERS_FLAG);
    }

    @Override
    public String getName() {
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
      CommandFormat commands = getCommandFormat();
      URI fsURI = toUri(s3Path);

      // check if UNGUARDED_FLAG is passed and use NullMetadataStore in
      // config to avoid side effects like creating the table if not exists
      Configuration unguardedConf = getConf();
      if (commands.getOpt(UNGUARDED_FLAG)) {
        LOG.debug("Unguarded flag is passed to command :" + this.getName());
        clearBucketOption(unguardedConf, fsURI.getHost(), S3_METADATA_STORE_IMPL);
        unguardedConf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);
      }

      S3AFileSystem fs = bindFilesystem(
          FileSystem.newInstance(fsURI, unguardedConf));
      Configuration conf = fs.getConf();
      URI fsUri = fs.getUri();
      MetadataStore store = fs.getMetadataStore();
      println(out, "Filesystem %s", fsUri);
      try {
        println(out, "Location: %s", fs.getBucketLocation());
      } catch (AccessDeniedException e) {
        // Caller cannot get the location of this bucket due to permissions
        // in their role or the bucket itself.
        // Note and continue.
        LOG.debug("failed to get bucket location", e);
        println(out, LOCATION_UNKNOWN);
      }
      boolean usingS3Guard = !(store instanceof NullMetadataStore);
      boolean authMode = false;
      if (usingS3Guard) {
        out.printf("Filesystem %s is using S3Guard with store %s%n",
            fsUri, store.toString());
        printOption(out, "Authoritative Metadata Store",
            METADATASTORE_AUTHORITATIVE, "false");
        printOption(out, "Authoritative Path",
              AUTHORITATIVE_PATH, "");
        final Collection<String> authoritativePaths
            = S3Guard.getAuthoritativePaths(fs);
        if (!authoritativePaths.isEmpty()) {
          println(out, "Qualified Authoritative Paths:");
          for (String path : authoritativePaths) {
            println(out, "\t%s", path);
          }
          println(out, "");
        }
        authMode = conf.getBoolean(METADATASTORE_AUTHORITATIVE, false);
        final long ttl = conf.getTimeDuration(METADATASTORE_METADATA_TTL,
            DEFAULT_METADATASTORE_METADATA_TTL, TimeUnit.MILLISECONDS);
        println(out, "\tMetadata time to live: (set in %s) = %s",
            METADATASTORE_METADATA_TTL,
            DurationFormatUtils.formatDurationHMS(ttl));
        printStoreDiagnostics(out, store);
      } else {
        println(out, "Filesystem %s is not using S3Guard", fsUri);
      }

      println(out, "%nS3A Client");
      printOption(out, "\tSigning Algorithm", SIGNING_ALGORITHM, "(unset)");
      String endpoint = conf.getTrimmed(ENDPOINT, "");
      println(out, "\tEndpoint: %s=%s",
          ENDPOINT,
          StringUtils.isNotEmpty(endpoint) ? endpoint : "(unset)");
      String encryption =
          printOption(out, "\tEncryption", SERVER_SIDE_ENCRYPTION_ALGORITHM,
              "none");
      printOption(out, "\tInput seek policy", INPUT_FADVISE, INPUT_FADV_NORMAL);
      printOption(out, "\tChange Detection Source", CHANGE_DETECT_SOURCE,
          CHANGE_DETECT_SOURCE_DEFAULT);
      printOption(out, "\tChange Detection Mode", CHANGE_DETECT_MODE,
          CHANGE_DETECT_MODE_DEFAULT);
      // committers
      println(out, "%nS3A Committers");
      boolean magic = fs.hasPathCapability(
          new Path(s3Path),
          CommitConstants.STORE_CAPABILITY_MAGIC_COMMITTER);
      println(out, "\tThe \"magic\" committer %s supported in the filesystem",
          magic ? "is" : "is not");

      printOption(out, "\tS3A Committer factory class",
          S3A_COMMITTER_FACTORY_KEY, "");
      String committer = conf.getTrimmed(FS_S3A_COMMITTER_NAME,
          COMMITTER_NAME_FILE);
      printOption(out, "\tS3A Committer name",
          FS_S3A_COMMITTER_NAME, COMMITTER_NAME_FILE);
      switch (committer) {
      case COMMITTER_NAME_FILE:
        println(out, "The original 'file' commmitter is active"
            + " -this is slow and potentially unsafe");
        break;
      case InternalCommitterConstants.COMMITTER_NAME_STAGING:
        println(out, "The 'staging' committer is used "
            + "-prefer the 'directory' committer");
        // fall through
      case COMMITTER_NAME_DIRECTORY:
        // fall through
      case COMMITTER_NAME_PARTITIONED:
        // print all the staging options.
        printOption(out, "\tCluster filesystem staging directory",
            FS_S3A_COMMITTER_STAGING_TMP_PATH, FILESYSTEM_TEMP_PATH);
        printOption(out, "\tLocal filesystem buffer directory",
            BUFFER_DIR, "");
        printOption(out, "\tFile conflict resolution",
            FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, DEFAULT_CONFLICT_MODE);
        break;
      case COMMITTER_NAME_MAGIC:
        printOption(out, "\tStore magic committer integration",
            MAGIC_COMMITTER_ENABLED,
            Boolean.toString(DEFAULT_MAGIC_COMMITTER_ENABLED));
        if (!magic) {
          println(out, "Warning: although the magic committer is enabled, "
              + "the store does not support it");
        }
        break;
      default:
        println(out, "\tWarning: committer '%s' is unknown", committer);
      }

      // look at delegation token support
      println(out, "%nSecurity");
      if (fs.getDelegationTokens().isPresent()) {
        // DT is enabled
        S3ADelegationTokens dtIntegration = fs.getDelegationTokens().get();
        println(out, "\tDelegation Support enabled: token kind = %s",
            dtIntegration.getTokenKind());
        UserGroupInformation.AuthenticationMethod authenticationMethod
            = UserGroupInformation.getCurrentUser().getAuthenticationMethod();
        println(out, "\tHadoop security mode: %s", authenticationMethod);
        if (UserGroupInformation.isSecurityEnabled()) {
          println(out,
              "\tWarning: security is disabled; tokens will not be collected");
        }
      } else {
        println(out, "\tDelegation token support is disabled");
      }

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

      // directory markers
      processMarkerOption(out, fs,
          getCommandFormat().getOptValue(MARKERS_FLAG));

      // and finally flush the output and report a success.
      out.flush();
      return SUCCESS;
    }

    /**
     * Validate the marker options.
     * @param out output stream
     * @param fs filesystem
     * @param path test path
     * @param marker desired marker option -may be null.
     */
    private void processMarkerOption(final PrintStream out,
        final S3AFileSystem fs,
        final String marker) {
      println(out, "%nSecurity");
      DirectoryPolicy markerPolicy = fs.getDirectoryMarkerPolicy();
      String desc = markerPolicy.describe();
      println(out, "\tThe directory marker policy is \"%s\"", desc);

      String pols = DirectoryPolicyImpl.availablePolicies()
          .stream()
          .map(DirectoryPolicy.MarkerPolicy::getOptionName)
          .collect(Collectors.joining(", "));
      println(out, "\tAvailable Policies: %s", pols);
      printOption(out, "\tAuthoritative paths",
          AUTHORITATIVE_PATH, "");
      DirectoryPolicy.MarkerPolicy mp = markerPolicy.getMarkerPolicy();

      String desiredMarker = marker == null
          ? ""
          : marker.trim();
      final String optionName = mp.getOptionName();
      if (!desiredMarker.isEmpty()) {
        if (MARKERS_AWARE.equalsIgnoreCase(desiredMarker)) {
          // simple awareness test -provides a way to validate compatibility
          // on the command line
          println(out, IS_MARKER_AWARE);
        } else {
          // compare with current policy
          if (!optionName.equalsIgnoreCase(desiredMarker)) {
            throw badState("Bucket %s: required marker policy is \"%s\""
                    + " but actual policy is \"%s\"",
                fs.getUri(), desiredMarker, optionName);
          }
        }
      }
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
    public String getName() {
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
                  true, LOG_EVENT);
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

  /**
   * Fsck - check for consistency between S3 and the metadatastore.
   */
  static class Fsck extends S3GuardTool {
    public static final String CHECK_FLAG = "check";
    public static final String DDB_MS_CONSISTENCY_FLAG = "internal";
    public static final String FIX_FLAG = "fix";

    public static final String NAME = "fsck";
    public static final String PURPOSE = "Compares S3 with MetadataStore, and "
        + "returns a failure status if any rules or invariants are violated. "
        + "Only works with DynamoDB metadata stores.";
    private static final String USAGE = NAME + " [OPTIONS] [s3a://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "  -" + CHECK_FLAG + " Check the metadata store for errors, but do "
        + "not fix any issues.\n" +
        "  -" + DDB_MS_CONSISTENCY_FLAG + " Check the dynamodb metadata store "
        + "for internal consistency.\n" +
        "  -" + FIX_FLAG + " Fix the errors found in the metadata store. Can " +
        "be used with " + CHECK_FLAG + " or " + DDB_MS_CONSISTENCY_FLAG + " flags. "
        + "\n\t\tFixes: \n" +
        "\t\t\t- Remove orphan entries from DDB." +
        "\n";

    Fsck(Configuration conf) {
      super(conf, CHECK_FLAG, DDB_MS_CONSISTENCY_FLAG, FIX_FLAG);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    public int run(String[] args, PrintStream out) throws
        InterruptedException, IOException {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        out.println(USAGE);
        throw invalidArgs("no arguments");
      }
      int exitValue = EXIT_SUCCESS;

      final CommandFormat commandFormat = getCommandFormat();

      // check if there's more than one arguments
      // from CHECK and INTERNAL CONSISTENCY
      int flags = countTrue(commandFormat.getOpt(CHECK_FLAG),
          commandFormat.getOpt(DDB_MS_CONSISTENCY_FLAG));
      if (flags > 1) {
        out.println(USAGE);
        throw invalidArgs("There should be only one parameter used for checking.");
      }
      if (flags == 0 && commandFormat.getOpt(FIX_FLAG)) {
        errorln(FIX_FLAG + " flag can be used with either " + CHECK_FLAG + " or " +
            DDB_MS_CONSISTENCY_FLAG + " flag, but not alone.");
        errorln(USAGE);
        return ERROR;
      }

      String s3Path = paths.get(0);
      try {
        initS3AFileSystem(s3Path);
      } catch (Exception e) {
        errorln("Failed to initialize S3AFileSystem from path: " + s3Path);
        throw e;
      }

      URI uri = toUri(s3Path);
      Path root;
      if (uri.getPath().isEmpty()) {
        root = new Path("/");
      } else {
        root = new Path(uri.getPath());
      }

      final S3AFileSystem fs = getFilesystem();
      initMetadataStore(false);
      final MetadataStore ms = getStore();

      if (ms == null ||
          !(ms instanceof DynamoDBMetadataStore)) {
        errorln(s3Path + " path uses metadata store: " + ms);
        errorln(NAME + " can be only used with a DynamoDB backed s3a bucket.");
        errorln(USAGE);
        return ERROR;
      }

      List<S3GuardFsck.ComparePair> violations;

      if (commandFormat.getOpt(CHECK_FLAG)) {
        // do the check
        S3GuardFsck s3GuardFsck = new S3GuardFsck(fs, ms);
        try {
          violations = s3GuardFsck.compareS3ToMs(fs.qualify(root));
        } catch (IOException e) {
          throw e;
        }
      } else if (commandFormat.getOpt(DDB_MS_CONSISTENCY_FLAG)) {
        S3GuardFsck s3GuardFsck = new S3GuardFsck(fs, ms);
        violations = s3GuardFsck.checkDdbInternalConsistency(fs.qualify(root));
      } else {
        errorln("No supported operation is selected.");
        errorln(USAGE);
        return ERROR;
      }

      if (commandFormat.getOpt(FIX_FLAG)) {
        S3GuardFsck s3GuardFsck = new S3GuardFsck(fs, ms);
        s3GuardFsck.fixViolations(violations);
      }

      out.flush();

      // We fail if there were compare pairs, as the returned compare pairs
      // contain issues.
      if (violations == null || violations.size() > 0) {
        exitValue = EXIT_FAIL;
      }
      return exitValue;
    }

    int countTrue(Boolean... bools) {
      return (int) Arrays.stream(bools).filter(p -> p).count();
    }
  }
  /**
   * Audits a DynamoDB S3Guard repository for all the entries being
   * 'authoritative'.
   * Checks bucket settings if {@link #CHECK_FLAG} is set, then
   * treewalk.
   */
  static class Authoritative extends S3GuardTool {

    public static final String NAME = "authoritative";

    public static final String CHECK_FLAG = "check-config";
    public static final String REQUIRE_AUTH = "required";

    public static final String PURPOSE = "Audits a DynamoDB S3Guard "
        + "repository for all the entries being 'authoritative'";

    private static final String USAGE = NAME + " [OPTIONS] [s3a://PATH]\n"
        + "\t" + PURPOSE + "\n\n"
        + "Options:\n"
        + "  -" + REQUIRE_AUTH + " - Require directories under the path to"
        + " be authoritative.\n"
        + "  -" + CHECK_FLAG + " - Check the configuration for the path to"
        + " be authoritative\n"
        + "  -" + VERBOSE + " - Verbose Output.\n";

    Authoritative(Configuration conf) {
      super(conf, CHECK_FLAG, REQUIRE_AUTH, VERBOSE);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    public int run(String[] args, PrintStream out) throws
        InterruptedException, IOException {
      List<String> paths = parseArgs(args);
      if (paths.isEmpty()) {
        out.println(USAGE);
        throw invalidArgs("no arguments");
      }
      maybeInitFilesystem(paths);
      initMetadataStore(false);
      String s3Path = paths.get(0);

      URI uri = toUri(s3Path);
      Path auditPath;
      if (uri.getPath().isEmpty()) {
        auditPath = new Path("/");
      } else {
        auditPath = new Path(uri.getPath());
      }

      final S3AFileSystem fs = getFilesystem();
      final MetadataStore ms = getStore();

      if (!(ms instanceof DynamoDBMetadataStore)) {
        errorln(s3Path + " path uses MS: " + ms);
        errorln(NAME + " can be only used with a DynamoDB-backed S3Guard table.");
        errorln(USAGE);
        return ERROR;
      }

      final CommandFormat commandFormat = getCommandFormat();
      if (commandFormat.getOpt(CHECK_FLAG)) {
        // check that the path is auth
        if (!fs.allowAuthoritative(auditPath)) {
          // path isn't considered auth in the S3A bucket info
          errorln("Path " + auditPath
              + " is not configured to be authoritative");
          return AuthoritativeAuditOperation.ERROR_PATH_NOT_AUTH_IN_FS;
        }
      }

      final AuthoritativeAuditOperation audit = new AuthoritativeAuditOperation(
          fs.createStoreContext(),
          (DynamoDBMetadataStore) ms,
          commandFormat.getOpt(REQUIRE_AUTH),
          commandFormat.getOpt(VERBOSE));
      audit.audit(fs.qualify(auditPath));

      out.flush();
      return EXIT_SUCCESS;
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

  protected static void errorln() {
    System.err.println();
  }

  protected static void errorln(String x) {
    System.err.println(x);
  }

  /**
   * Print a formatted string followed by a newline to the output stream.
   * @param out destination
   * @param format format string
   * @param args optional arguments
   */
  protected static void println(PrintStream out,
      String format,
      Object... args) {
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
    return exitException(INVALID_ARGUMENT, format, args);
  }

  /**
   * Build the exception to raise on a bad store/bucket state.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException badState(
      String format, Object...args) {
    int exitCode = E_BAD_STATE;
    return exitException(exitCode, format, args);
  }

  /**
   * Build the exception to raise on user-aborted action.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException userAborted(
      String format, Object...args) {
    return exitException(ERROR, format, args);
  }

  /**
   * Build a exception to throw with a formatted message.
   * @param exitCode exit code to use
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException exitException(
      final int exitCode,
      final String format,
      final Object... args) {
    return new ExitUtil.ExitException(exitCode,
        String.format(format, args));
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
    case MarkerTool.MARKERS:
      command = new MarkerTool(conf);
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
    case SelectTool.NAME:
      // the select tool is not technically a S3Guard tool, but it's on the CLI
      // because this is the defacto S3 CLI.
      command = new SelectTool(conf);
      break;
    case Fsck.NAME:
      command = new Fsck(conf);
      break;
    case Authoritative.NAME:
      command = new Authoritative(conf);
      break;
    default:
      printHelp();
      throw new ExitUtil.ExitException(E_USAGE,
          "Unknown command " + subCommand);
    }
    try {
      return ToolRunner.run(conf, command, otherArgs);
    } finally {
      IOUtils.cleanupWithLogger(LOG, command);
    }
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
      LOG.debug("Exception raised", e);
      exit(e.getExitCode(), e.toString());
    } catch (FileNotFoundException e) {
      // Bucket doesn't exist or similar - return code of 44, "404".
      errorln(e.toString());
      LOG.debug("Not found:", e);
      exit(EXIT_NOT_FOUND, e.toString());
    } catch (Throwable e) {
      if (e instanceof ExitCodeProvider) {
        // this exception provides its own exit code
        final ExitCodeProvider ec = (ExitCodeProvider) e;
        LOG.debug("Exception raised", e);
        exit(ec.getExitCode(), e.toString());
      } else {
        e.printStackTrace(System.err);
        exit(ERROR, e.toString());
      }
    }
  }

  protected static void exit(int status, String text) {
    ExitUtil.terminate(status, text);
  }
}
