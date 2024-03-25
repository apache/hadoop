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
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketType;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DataRedundancy;
import software.amazon.awssdk.services.s3.model.LocationInfo;
import software.amazon.awssdk.services.s3.model.LocationType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.PRODUCT_NAME;
import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.STORE_CAPABILITY_S3_EXPRESS_STORAGE;
import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS;
import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.isAwsEndpoint;
import static org.apache.hadoop.fs.s3a.impl.S3ExpressStorage.hasS3ExpressSuffix;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_BAD_CONFIGURATION;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * Bucket operations, e.g. create/delete/probe.
 */
public final class BucketTool extends S3GuardTool {

  private static final Logger LOG = LoggerFactory.getLogger(BucketTool.class);

  /**
   * Name of this tool: {@value}.
   */
  public static final String NAME = "bucket";

  /**
   * Purpose of this tool: {@value}.
   */
  public static final String PURPOSE =
      "View and manipulate S3 buckets";

  /**
   * create command.
   */
  public static final String CREATE = "create";

  /**
   * region {@value}.
   */
  public static final String OPT_REGION = "region";

  /**
   * endpoint {@value}.
   */
  public static final String OPT_ENDPOINT = "endpoint";

  /**
   * Zone for a store.
   */
  public static final String OPT_ZONE = "zone";

  /**
   * Error message if -zone is set but the name doesn't match.
   * Value {@value}.
   */
  static final String UNSUPPORTED_ZONE_ARG =
      "The -zone option is only supported for " + PRODUCT_NAME;

  /**
   * Error message if the bucket is S3 Express but -zone wasn't set.
   * Value {@value}.
   */
  static final String NO_ZONE_SUPPLIED = "Required option -zone missing for "
      + PRODUCT_NAME + " bucket";

  /**
   * Error Message logged/thrown when the tool could not start as
   * the bucket probe was not disabled and the probe (inevitably)
   * failed.
   */
  public static final String PROBE_FAILURE =
      "Initialization failed because the bucket existence probe"
          + S3A_BUCKET_PROBE + " was not disabled. Check core-site settings.";

  public BucketTool(final Configuration conf) {
    super(conf, 1, 1,
        CREATE);
    CommandFormat format = getCommandFormat();
    format.addOptionWithValue(OPT_REGION);
    format.addOptionWithValue(OPT_ENDPOINT);
    format.addOptionWithValue(OPT_ZONE);
  }

  public String getUsage() {
    return "bucket "
        + "-" + CREATE + " "
        + "[-" + OPT_ENDPOINT + " <endpoint>] "
        + "[-" + OPT_REGION + " <region>] "
        + "[-" + OPT_ZONE + " <zone>] "
        + " <s3a-URL>";
  }

  public String getName() {
    return NAME;
  }

  private Optional<String> getOptionalString(String key) {
    String value = getCommandFormat().getOptValue(key);
    return isNotEmpty(value) ? Optional.of(value) : Optional.empty();
  }

  @VisibleForTesting
  int exec(final String...args) throws Exception {
    return run(args, System.out);
  }

  @Override
  public int run(final String[] args, final PrintStream out)
      throws Exception, ExitUtil.ExitException {

    LOG.debug("Supplied arguments: {}", String.join(", ", args));
    final List<String> parsedArgs = parseArgsWithErrorReporting(args);

    CommandFormat command = getCommandFormat();
    boolean create = command.getOpt(CREATE);

    Optional<String> endpoint = getOptionalString(OPT_ENDPOINT);
    Optional<String> region = getOptionalString(OPT_REGION);
    Optional<String> zone = getOptionalString(OPT_ZONE);


    final String bucketPath = parsedArgs.get(0);
    final Path source = new Path(bucketPath);
    URI fsURI = source.toUri();
    String bucket = fsURI.getHost();

    println(out, "Filesystem %s", fsURI);
    if (!"s3a".equals(fsURI.getScheme())) {
      throw new ExitUtil.ExitException(EXIT_USAGE, "Filesystem is not S3A URL: " + fsURI);
    }
    println(out, "Options region=%s endpoint=%s zone=%s s3a://%s",
        region.orElse("(unset)"),
        endpoint.orElse("(unset)"),
        zone.orElse("(unset)"),
        bucket);
    if (!create) {
      errorln(getUsage());
      println(out, "Supplied arguments: ["
          + String.join(", ", parsedArgs)
          + "]");
      throw new ExitUtil.ExitException(EXIT_USAGE,
          "required option not found: -create");
    }

    final Configuration conf = getConf();

    removeBucketOverrides(bucket, conf,
        S3A_BUCKET_PROBE,
        REJECT_OUT_OF_SPAN_OPERATIONS,
        AWS_REGION,
        ENDPOINT);
    // stop any S3 calls taking place against a bucket which
    // may not exist
    String bucketPrefix = "fs.s3a.bucket." + bucket + '.';
    conf.setInt(bucketPrefix + S3A_BUCKET_PROBE, 0);
    conf.setBoolean(bucketPrefix + REJECT_OUT_OF_SPAN_OPERATIONS, false);
    // propagate an option
    BiFunction<String, Optional<String>, Boolean> propagate = (key, opt) ->
        opt.map(v -> {
          conf.set(key, v);
          LOG.info("{} = {}", key, v);
          return true;
        }).orElse(false);


    propagate.apply(AWS_REGION, region);
    propagate.apply(ENDPOINT, endpoint);

    // fail fast on third party store
    if (hasS3ExpressSuffix(bucket) && !isAwsEndpoint(endpoint.orElse(""))) {
      throw new ExitUtil.ExitException(EXIT_NOT_ACCEPTABLE, UNSUPPORTED_ZONE_ARG);
    }
    S3AFileSystem fs;
    try {
      fs = (S3AFileSystem) FileSystem.newInstance(fsURI, conf);
    } catch (FileNotFoundException e) {
      // this happens if somehow the probe wasn't disabled.
      errorln(PROBE_FAILURE);
      throw new ExitUtil.ExitException(EXIT_BAD_CONFIGURATION, PROBE_FAILURE);
    }

    try {

      // now build the configuration
      final CreateBucketConfiguration.Builder builder = CreateBucketConfiguration.builder();

      if (fs.hasPathCapability(new Path("/"), STORE_CAPABILITY_S3_EXPRESS_STORAGE)) {
        //  S3 Express store requires a zone and some other other settings
        final String az = zone.orElseThrow(() ->
            new ExitUtil.ExitException(EXIT_USAGE, NO_ZONE_SUPPLIED + bucket));
        builder.location(LocationInfo.builder()
                .type(LocationType.AVAILABILITY_ZONE).name(az).build())
            .bucket(software.amazon.awssdk.services.s3.model.BucketInfo.builder()
                .type(BucketType.DIRECTORY)
                .dataRedundancy(DataRedundancy.SINGLE_AVAILABILITY_ZONE).build());

      } else {
        if (zone.isPresent()) {
          throw new ExitUtil.ExitException(EXIT_USAGE, UNSUPPORTED_ZONE_ARG + " not " + bucket);
        }
        region.ifPresent(builder::locationConstraint);
      }

      final CreateBucketRequest request = CreateBucketRequest.builder()
          .bucket(bucket)
          .createBucketConfiguration(builder.build())
          .build();

      println(out, "Creating bucket %s", bucket);

      final S3Client s3Client = fs.getS3AInternals().getAmazonS3Client(NAME);
      try (DurationInfo ignored = new DurationInfo(LOG,
          "Create %sbucket %s in region %s",
          (fs.hasPathCapability(new Path("/"),
              STORE_CAPABILITY_S3_EXPRESS_STORAGE) ? (PRODUCT_NAME + " "): ""),
          bucket, region.orElse("(unset)"))) {
        once("create", source.toString(), () ->
            s3Client.createBucket(request));
      }
    } finally {
      IOUtils.closeStream(fs);
    }

    return 0;
  }

  /**
   * Remove any values from a bucket.
   * @param bucket bucket whose overrides are to be removed. Can be null/empty
   * @param conf config
   * @param options list of fs.s3a options to remove
   */
  public static void removeBucketOverrides(final String bucket,
      final Configuration conf,
      final String... options) {

    if (StringUtils.isEmpty(bucket)) {
      return;
    }
    final String bucketPrefix = "fs.s3a.bucket." + bucket + '.';
    for (String option : options) {
      final String stripped = option.substring("fs.s3a.".length());
      String target = bucketPrefix + stripped;
      String v = conf.get(target);
      if (v != null) {
        conf.unset(target);
      }
      String extended = bucketPrefix + option;
      if (conf.get(extended) != null) {
        conf.unset(extended);
      }
    }
  }
}
