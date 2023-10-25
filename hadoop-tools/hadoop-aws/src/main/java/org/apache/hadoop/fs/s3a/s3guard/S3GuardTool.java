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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.s3.model.MultipartUpload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.MultipartUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.auth.RolePolicies;
import org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicy;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicyImpl;
import org.apache.hadoop.fs.s3a.select.SelectTool;
import org.apache.hadoop.fs.s3a.tools.MarkerTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Invoker.LOG_EVENT;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.FILESYSTEM_TEMP_PATH;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.*;

/**
 * CLI to manage S3Guard Metadata Store.
 * <p>
 * Some management tools invoke this class directly.
 */
@InterfaceAudience.LimitedPrivate("management tools")
@InterfaceStability.Evolving
public abstract class S3GuardTool extends Configured implements Tool,
    Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(S3GuardTool.class);
  private static final String ENTRY_POINT = "s3guard";

  private static final String NAME = ENTRY_POINT;
  private static final String COMMON_USAGE =
      "When possible and not overridden by more specific options, metadata\n" +
          "repository information will be inferred from the S3A URL (if provided)" +
          "\n\n" +
          "Generic options supported are:\n" +
          "  -conf <config file> - specify an application configuration file\n" +
          "  -D <property=value> - define a value for a given property\n";

  static final List<String> UNSUPPORTED_COMMANDS = Arrays.asList(
      "init",
      "destroy",
      "authoritative",
      "diff",
      "fsck",
      "import",
      "prune",
      "set-capacity");

  /**
   * Usage includes supported commands, but not the excluded ones.
   */
  private static final String USAGE = ENTRY_POINT +
      " [command] [OPTIONS] [s3a://BUCKET]\n\n" +
      "Commands: \n" +
      "\t" + BucketInfo.NAME + " - " + BucketInfo.PURPOSE + "\n" +
      "\t" + MarkerTool.MARKERS + " - " + MarkerTool.PURPOSE + "\n" +
      "\t" + SelectTool.NAME + " - " + SelectTool.PURPOSE + "\n" +
      "\t" + Uploads.NAME + " - " + Uploads.PURPOSE + "\n";

  private static final String E_UNSUPPORTED = "This command is no longer supported";

  public abstract String getUsage();

  // Exit codes
  static final int SUCCESS = EXIT_SUCCESS;
  static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;
  static final int E_USAGE = EXIT_USAGE;

  static final int ERROR = EXIT_FAIL;
  static final int E_BAD_STATE = EXIT_NOT_ACCEPTABLE;
  static final int E_NOT_FOUND = EXIT_NOT_FOUND;
  static final int E_S3GUARD_UNSUPPORTED = ERROR;

  /** Error String when the wrong FS is used for binding: {@value}. **/
  @VisibleForTesting
  public static final String WRONG_FILESYSTEM = "Wrong filesystem for ";

  /**
   * The FS we close when we are closed.
   */
  private FileSystem baseFS;
  private S3AFileSystem filesystem;
  private final CommandFormat commandFormat;

  public static final String META_FLAG = "meta";

  // These are common options
  public static final String DAYS_FLAG = "days";
  public static final String HOURS_FLAG = "hours";
  public static final String MINUTES_FLAG = "minutes";
  public static final String SECONDS_FLAG = "seconds";
  public static final String AGE_OPTIONS_USAGE = "[-days <days>] "
      + "[-hours <hours>] [-minutes <minutes>] [-seconds <seconds>]";

  public static final String VERBOSE = "verbose";

  /**
   * Constructor a S3Guard tool with HDFS configuration.
   * @param conf Configuration.
   * @param opts any boolean options to support
   */
  protected S3GuardTool(Configuration conf, String... opts) {
    super(conf);
    commandFormat = new CommandFormat(0, Integer.MAX_VALUE, opts);
  }

  /**
   * Return sub-command name.
   * @return sub-command name.
   */
  public abstract String getName();

  /**
   * Close the FS.
   * @throws IOException on failure.
   */
  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG,
        baseFS);
    baseFS = null;
    filesystem = null;
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
   * Create and initialize a new S3A FileSystem instance.
   *
   * @param path s3a URI
   * @throws IOException failure to init filesystem
   * @throws ExitUtil.ExitException if the FS is not an S3A FS
   */
  protected void initS3AFileSystem(String path) throws IOException {
    LOG.debug("Initializing S3A FS to {}", path);
    URI uri = toUri(path);
    bindFilesystem(FileSystem.newInstance(uri, getConf()));
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

  /**
   * Reset the store and filesystem bindings.
   */
  protected void resetBindings() {
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
    println(stream, "%nIO Statistics for %s%n", fs.getUri());
    final IOStatistics iostats = retrieveIOStatistics(fs);
    if (iostats != null) {
      println(stream, ioStatisticsToPrettyString(iostats));

    } else {
      println(stream, "FileSystem does not provide IOStatistics");
    }
    println(stream, "");
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

    public static final String PURPOSE = "provide/check information"
        + " about a specific bucket";

    private static final String USAGE = NAME + " [OPTIONS] s3a://BUCKET\n"
        + "\t" + PURPOSE + "\n\n"
        + "Common options:\n"
        + "  -" + AUTH_FLAG + " - Require the S3Guard mode to be \"authoritative\"\n"
        + "  -" + NONAUTH_FLAG + " - Require the S3Guard mode to be \"non-authoritative\"\n"
        + "  -" + MAGIC_FLAG +
        " - Require the S3 filesystem to be support the \"magic\" committer\n"
        + "  -" + ENCRYPTION_FLAG
        + " (none, sse-s3, sse-kms) - Require encryption policy\n"
        + "  -" + MARKERS_FLAG
        + " (aware, keep, delete, authoritative) - directory markers policy\n"
        + "  -" + GUARDED_FLAG + " - Require S3Guard. Will always fail.\n"
        + "  -" + UNGUARDED_FLAG + " - Force S3Guard to be disabled (always true)\n";

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

      S3AFileSystem fs = bindFilesystem(
          FileSystem.newInstance(fsURI, getConf()));
      Configuration conf = fs.getConf();
      URI fsUri = fs.getUri();
      println(out, "Filesystem %s", fsUri);
      try {
        println(out, "Location: %s", fs.getBucketLocation());
      } catch (IOException e) {
        // Caller cannot get the location of this bucket due to permissions
        // in their role or the bucket itself, or it is not an operation
        // supported by this store.
        // Note and continue.
        LOG.debug("failed to get bucket location", e);
        println(out, LOCATION_UNKNOWN);

        // it may be the bucket is not found; we can't differentiate
        // that and handle third party store issues where the API may
        // not work.
        // Fallback to looking for bucket root attributes.
        println(out, "Probing for bucket existence");
        fs.listXAttrs(new Path("/"));
      }

      // print any auth paths for directory marker info
      final Collection<String> authoritativePaths
          = S3Guard.getAuthoritativePaths(fs);
      if (!authoritativePaths.isEmpty()) {
        println(out, "Qualified Authoritative Paths:");
        for (String path : authoritativePaths) {
          println(out, "\t%s", path);
        }
        println(out, "");
      }
      println(out, "%nS3A Client");
      printOption(out, "\tSigning Algorithm", SIGNING_ALGORITHM, "(unset)");
      String endpoint = conf.getTrimmed(ENDPOINT, "");
      println(out, "\tEndpoint: %s=%s",
          ENDPOINT,
          StringUtils.isNotEmpty(endpoint) ? endpoint : "(unset)");
      String encryption =
          printOption(out, "\tEncryption", Constants.S3_ENCRYPTION_ALGORITHM,
              "none");
      printOption(out, "\tInput seek policy", INPUT_FADVISE,
          Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_DEFAULT);
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
      if (commands.getOpt(GUARDED_FLAG)) {
        throw badState("S3Guard is not supported");
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
     * @param marker desired marker option -may be null.
     */
    private void processMarkerOption(final PrintStream out,
        final S3AFileSystem fs,
        final String marker) {
      println(out, "%nDirectory Markers");
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
        + " (-" + LIST + " | -" + EXPECT + " <num-uploads> | -" + ABORT
        + ") [-" + VERBOSE + "] "
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
    private enum Mode {LIST, EXPECT, ABORT}

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
      println(out, "Listing uploads under path \"%s\"", prefix);
      promptBeforeAbort(out);
      processUploads(out);
      if (verbose) {
        dumpFileSystemStatistics(out);
      }
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
      final S3AFileSystem fs = getFilesystem();
      MultipartUtils.UploadIterator uploads = fs.listUploads(prefix);
      // create a span so that the write operation helper
      // is within one
      AuditSpan span =
          fs.createSpan(MULTIPART_UPLOAD_ABORTED,
              prefix, null);
      final WriteOperationHelper writeOperationHelper
          = fs.getWriteOperationHelper();

      int count = 0;
      while (uploads.hasNext()) {
        MultipartUpload upload = uploads.next();
        if (!olderThan(upload, ageMsec)) {
          continue;
        }
        count++;
        if (mode == Mode.ABORT || mode == Mode.LIST || verbose) {
          println(out, "%s%s %s", mode == Mode.ABORT ? "Deleting: " : "",
              upload.key(), upload.uploadId());
        }
        if (mode == Mode.ABORT) {
          writeOperationHelper
              .abortMultipartUpload(upload.key(), upload.uploadId(),
                  true, LOG_EVENT);
        }
      }
      span.deactivate();
      if (mode != Mode.EXPECT || verbose) {
        println(out, "%s %d uploads %s.", TOTAL, count,
            mode == Mode.ABORT ? "deleted" : "found");
      }
      if (mode == Mode.EXPECT) {
        if (count != expectedCount) {
          throw badState("Expected upload count under %s: %d, found %d",
              prefix, expectedCount, count);
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
      if (msec == 0) {
        return true;
      }
      Date ageDate = new Date(System.currentTimeMillis() - msec);
      return ageDate.compareTo(Date.from(u.initiated())) >= 0;
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
      throw invalidArgs("Not a valid filesystem path: %s", s3Path);
    }
    return uri;
  }

  private static void printHelp() {
    if (command == null) {
      errorln("Usage: hadoop " + USAGE);
      errorln("\tperform S3A connector administrative commands.");
    } else {
      errorln("Usage: hadoop " + ENTRY_POINT + command.getUsage());
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
   * Handle FileNotFoundException by converting to an exit exception
   * with specific error code.
   * @param e exception
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException notFound(
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
      String format, Object... args) {
    return exitException(INVALID_ARGUMENT, format, args);
  }

  /**
   * Build the exception to raise on a bad store/bucket state.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException badState(
      String format, Object... args) {
    return exitException(E_BAD_STATE, format, args);
  }

  /**
   * Crate an exception declaring S3Guard is unsupported.
   * @return an exception raise.
   */
  protected static ExitUtil.ExitException s3guardUnsupported() {
    throw exitException(E_S3GUARD_UNSUPPORTED, E_UNSUPPORTED);
  }

  /**
   * Build the exception to raise on user-aborted action.
   * @param format string format
   * @param args optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException userAborted(
      String format, Object... args) {
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
  public static int run(Configuration conf, String... args) throws
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
    // if it is no longer supported: raise an exception
    if (UNSUPPORTED_COMMANDS.contains(subCommand)) {
      throw s3guardUnsupported();
    }
    switch (subCommand) {
    case BucketInfo.NAME:
      command = new BucketInfo(conf);
      break;
    case MarkerTool.MARKERS:
      command = new MarkerTool(conf);
      break;
    case Uploads.NAME:
      command = new Uploads(conf);
      break;
    case SelectTool.NAME:
      // the select tool is not technically a S3Guard tool, but it's on the CLI
      // because this is the defacto S3 CLI.
      command = new SelectTool(conf);
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
