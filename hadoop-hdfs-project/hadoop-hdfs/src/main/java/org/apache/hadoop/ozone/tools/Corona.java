/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.tools;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.lang.Math.min;

/**
 * Corona - A tool to populate ozone with data for testing.<br>
 * This is not a map-reduce program and this is not for benchmarking
 * Ozone write throughput.<br>
 * It supports both online and offline modes. Default mode is offline,
 * <i>-mode</i> can be used to change the mode.
 * <p>
 * In online mode, active internet connection is required,
 * common crawl data from AWS will be used.<br>
 * Default source is:<br>
 * https://commoncrawl.s3.amazonaws.com/crawl-data/
 * CC-MAIN-2017-17/warc.paths.gz<br>
 * (it contains the path to actual data segment)<br>
 * User can override this using <i>-source</i>.
 * The following values are derived from URL of Common Crawl data
 * <ul>
 * <li>Domain will be used as Volume</li>
 * <li>URL will be used as Bucket</li>
 * <li>FileName will be used as Key</li>
 * </ul></p>
 * In offline mode, the data will be random bytes and
 * size of data will be 10 KB.<br>
 * <ul>
 * <li>Default number of Volumes 10, <i>-numOfVolumes</i>
 * can be used to override</li>
 * <li>Default number of Buckets per Volume 1000, <i>-numOfBuckets</i>
 * can be used to override</li>
 * <li>Default number of Keys per Bucket 500000, <i>-numOfKeys</i>
 * can be used to override</li>
 * </ul>
 */
public final class Corona extends Configured implements Tool {

  enum CoronaOps {
    VOLUME_CREATE,
    BUCKET_CREATE,
    KEY_CREATE,
    KEY_WRITE
  }

  private static final String HELP = "help";
  private static final String MODE = "mode";
  private static final String SOURCE = "source";
  private static final String VALIDATE_WRITE = "validateWrites";
  private static final String JSON_WRITE_DIRECTORY = "jsonDir";
  private static final String NUM_OF_THREADS = "numOfThreads";
  private static final String NUM_OF_VOLUMES = "numOfVolumes";
  private static final String NUM_OF_BUCKETS = "numOfBuckets";
  private static final String NUM_OF_KEYS = "numOfKeys";
  private static final String KEY_SIZE = "keySize";
  private static final String RATIS = "ratis";

  private static final String MODE_DEFAULT = "offline";
  private static final String SOURCE_DEFAULT =
      "https://commoncrawl.s3.amazonaws.com/" +
          "crawl-data/CC-MAIN-2017-17/warc.paths.gz";
  private static final String NUM_OF_THREADS_DEFAULT = "10";
  private static final String NUM_OF_VOLUMES_DEFAULT = "10";
  private static final String NUM_OF_BUCKETS_DEFAULT = "1000";
  private static final String NUM_OF_KEYS_DEFAULT = "500000";
  private static final String DURATION_FORMAT = "HH:mm:ss,SSS";

  private static final int KEY_SIZE_DEFAULT = 10240;
  private static final int QUANTILES = 10;

  private static final Logger LOG =
      LoggerFactory.getLogger(Corona.class);

  private boolean printUsage = false;
  private boolean completed = false;
  private boolean exception = false;

  private String mode;
  private String source;
  private String numOfThreads;
  private String numOfVolumes;
  private String numOfBuckets;
  private String numOfKeys;
  private String jsonDir;
  private boolean useRatis;
  private ReplicationType type;
  private ReplicationFactor factor;

  private int threadPoolSize;
  private int keySize;
  private byte[] keyValue = null;

  private boolean validateWrites;

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private ExecutorService processor;

  private long startTime;
  private long jobStartTime;

  private AtomicLong volumeCreationTime;
  private AtomicLong bucketCreationTime;
  private AtomicLong keyCreationTime;
  private AtomicLong keyWriteTime;

  private AtomicLong totalBytesWritten;

  private AtomicInteger numberOfVolumesCreated;
  private AtomicInteger numberOfBucketsCreated;
  private AtomicLong numberOfKeysAdded;

  private Long totalWritesValidated;
  private Long writeValidationSuccessCount;
  private Long writeValidationFailureCount;

  private BlockingQueue<KeyValue> validationQueue;
  private ArrayList<Histogram> histograms = new ArrayList<>();

  @VisibleForTesting
  Corona(Configuration conf) throws IOException {
    startTime = System.nanoTime();
    jobStartTime = System.currentTimeMillis();
    volumeCreationTime = new AtomicLong();
    bucketCreationTime = new AtomicLong();
    keyCreationTime = new AtomicLong();
    keyWriteTime = new AtomicLong();
    totalBytesWritten = new AtomicLong();
    numberOfVolumesCreated = new AtomicInteger();
    numberOfBucketsCreated = new AtomicInteger();
    numberOfKeysAdded = new AtomicLong();
    OzoneClientFactory.setConfiguration(conf);
    ozoneClient = OzoneClientFactory.getClient();
    objectStore = ozoneClient.getObjectStore();
    for (CoronaOps ops : CoronaOps.values()) {
      histograms.add(ops.ordinal(), new Histogram(new UniformReservoir()));
    }
  }

  /**
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new OzoneConfiguration();
    int res = ToolRunner.run(conf, new Corona(conf), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    GenericOptionsParser parser = new GenericOptionsParser(getConf(),
        getOptions(), args);
    parseOptions(parser.getCommandLine());
    if (printUsage) {
      usage();
      return 0;
    }

    keyValue =
        DFSUtil.string2Bytes(RandomStringUtils.randomAscii(keySize - 36));

    LOG.info("Number of Threads: " + numOfThreads);
    threadPoolSize =
        min(Integer.parseInt(numOfVolumes), Integer.parseInt(numOfThreads));
    processor = Executors.newFixedThreadPool(threadPoolSize);
    addShutdownHook();
    if (mode.equals("online")) {
      LOG.info("Mode: online");
      throw new UnsupportedOperationException("Not yet implemented.");
    } else {
      LOG.info("Mode: offline");
      LOG.info("Number of Volumes: {}.", numOfVolumes);
      LOG.info("Number of Buckets per Volume: {}.", numOfBuckets);
      LOG.info("Number of Keys per Bucket: {}.", numOfKeys);
      LOG.info("Key size: {} bytes", keySize);
      for (int i = 0; i < Integer.parseInt(numOfVolumes); i++) {
        String volume = "vol-" + i + "-" +
            RandomStringUtils.randomNumeric(5);
        processor.submit(new OfflineProcessor(volume));
      }
    }
    Thread validator = null;
    if (validateWrites) {
      totalWritesValidated = 0L;
      writeValidationSuccessCount = 0L;
      writeValidationFailureCount = 0L;

      validationQueue =
          new ArrayBlockingQueue<>(Integer.parseInt(numOfThreads));
      validator = new Thread(new Validator());
      validator.start();
      LOG.info("Data validation is enabled.");
    }
    Thread progressbar = getProgressBarThread();
    LOG.info("Starting progress bar Thread.");
    progressbar.start();
    processor.shutdown();
    processor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    completed = true;
    progressbar.join();
    if (validateWrites) {
      validator.join();
    }
    ozoneClient.close();
    return 0;
  }

  private Options getOptions() {
    Options options = new Options();

    OptionBuilder.withDescription("prints usage.");
    Option optHelp = OptionBuilder.create(HELP);

    OptionBuilder.withArgName("online | offline");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("specifies the mode of " +
        "Corona run.");
    Option optMode = OptionBuilder.create(MODE);

    OptionBuilder.withArgName("source url");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("specifies the URL of s3 " +
        "commoncrawl warc file to be used when the mode is online.");
    Option optSource = OptionBuilder.create(SOURCE);

    OptionBuilder.withDescription("do random validation of " +
        "data written into ozone, only subset of data is validated.");
    Option optValidateWrite = OptionBuilder.create(VALIDATE_WRITE);


    OptionBuilder.withDescription("directory where json is created");
    OptionBuilder.hasArg();
    Option optJsonDir = OptionBuilder.create(JSON_WRITE_DIRECTORY);

    OptionBuilder.withArgName("value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("number of threads to be launched " +
        "for the run");
    Option optNumOfThreads = OptionBuilder.create(NUM_OF_THREADS);

    OptionBuilder.withArgName("value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("specifies number of Volumes to be " +
        "created in offline mode");
    Option optNumOfVolumes = OptionBuilder.create(NUM_OF_VOLUMES);

    OptionBuilder.withArgName("value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("specifies number of Buckets to be " +
        "created per Volume in offline mode");
    Option optNumOfBuckets = OptionBuilder.create(NUM_OF_BUCKETS);

    OptionBuilder.withArgName("value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("specifies number of Keys to be " +
        "created per Bucket in offline mode");
    Option optNumOfKeys = OptionBuilder.create(NUM_OF_KEYS);

    OptionBuilder.withArgName("value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("specifies the size of Key in bytes to be " +
        "created in offline mode");
    Option optKeySize = OptionBuilder.create(KEY_SIZE);

    OptionBuilder.withArgName(RATIS);
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Use Ratis as the default replication " +
        "strategy");
    Option optRatis = OptionBuilder.create(RATIS);

    options.addOption(optHelp);
    options.addOption(optMode);
    options.addOption(optSource);
    options.addOption(optValidateWrite);
    options.addOption(optJsonDir);
    options.addOption(optNumOfThreads);
    options.addOption(optNumOfVolumes);
    options.addOption(optNumOfBuckets);
    options.addOption(optNumOfKeys);
    options.addOption(optKeySize);
    options.addOption(optRatis);
    return options;
  }

  private void parseOptions(CommandLine cmdLine) {
    printUsage = cmdLine.hasOption(HELP);

    mode = cmdLine.getOptionValue(MODE, MODE_DEFAULT);

    source = cmdLine.getOptionValue(SOURCE, SOURCE_DEFAULT);

    numOfThreads =
        cmdLine.getOptionValue(NUM_OF_THREADS, NUM_OF_THREADS_DEFAULT);

    validateWrites = cmdLine.hasOption(VALIDATE_WRITE);

    jsonDir = cmdLine.getOptionValue(JSON_WRITE_DIRECTORY);

    numOfVolumes =
        cmdLine.getOptionValue(NUM_OF_VOLUMES, NUM_OF_VOLUMES_DEFAULT);

    numOfBuckets =
        cmdLine.getOptionValue(NUM_OF_BUCKETS, NUM_OF_BUCKETS_DEFAULT);

    numOfKeys = cmdLine.getOptionValue(NUM_OF_KEYS, NUM_OF_KEYS_DEFAULT);

    keySize = cmdLine.hasOption(KEY_SIZE) ?
        Integer.parseInt(cmdLine.getOptionValue(KEY_SIZE)) : KEY_SIZE_DEFAULT;
    if (keySize < 1024) {
      throw new IllegalArgumentException(
          "keySize can not be less than 1024 bytes");
    }

    useRatis = cmdLine.hasOption(RATIS);

    type = ReplicationType.STAND_ALONE;
    factor = ReplicationFactor.ONE;

    if (useRatis) {
      type = ReplicationType.RATIS;
      int replicationFactor = Integer.parseInt(cmdLine.getOptionValue(RATIS));
      switch (replicationFactor) {
      case 1:
        factor = ReplicationFactor.ONE;
        break;
      case 3:
        factor = ReplicationFactor.THREE;
        break;
      default:
        throw new IllegalArgumentException("Illegal replication factor:"
            + replicationFactor);
      }
    }
  }

  private void usage() {
    System.out.println("Options supported are:");
    System.out.println("-numOfThreads <value>           "
        + "number of threads to be launched for the run.");
    System.out.println("-validateWrites                 "
        + "do random validation of data written into ozone, " +
        "only subset of data is validated.");
    System.out.println("-jsonDir                        "
        + "directory where json is created.");
    System.out.println("-mode [online | offline]        "
        + "specifies the mode in which Corona should run.");
    System.out.println("-source <url>                   "
        + "specifies the URL of s3 commoncrawl warc file to " +
        "be used when the mode is online.");
    System.out.println("-numOfVolumes <value>           "
        + "specifies number of Volumes to be created in offline mode");
    System.out.println("-numOfBuckets <value>           "
        + "specifies number of Buckets to be created per Volume " +
        "in offline mode");
    System.out.println("-numOfKeys <value>              "
        + "specifies number of Keys to be created per Bucket " +
        "in offline mode");
    System.out.println("-keySize <value>                "
        + "specifies the size of Key in bytes to be created in offline mode");
    System.out.println("-help                           "
        + "prints usage.");
    System.out.println();
  }

  /**
   * Adds ShutdownHook to print statistics.
   */
  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> printStats(System.out)));
  }

  private Thread getProgressBarThread() {
    Supplier<Long> currentValue;
    long maxValue;

    if (mode.equals("online")) {
      throw new UnsupportedOperationException("Not yet implemented.");
    } else {
      currentValue = () -> numberOfKeysAdded.get();
      maxValue = Long.parseLong(numOfVolumes) *
          Long.parseLong(numOfBuckets) *
          Long.parseLong(numOfKeys);
    }
    Thread progressBarThread = new Thread(
        new ProgressBar(System.out, currentValue, maxValue));
    progressBarThread.setName("ProgressBar");
    return progressBarThread;
  }

  /**
   * Prints stats of {@link Corona} run to the PrintStream.
   *
   * @param out PrintStream
   */
  private void printStats(PrintStream out) {
    long endTime = System.nanoTime() - startTime;
    String execTime = DurationFormatUtils
        .formatDuration(TimeUnit.NANOSECONDS.toMillis(endTime),
            DURATION_FORMAT);

    long volumeTime = TimeUnit.NANOSECONDS.toMillis(volumeCreationTime.get())
        / threadPoolSize;
    String prettyAverageVolumeTime =
        DurationFormatUtils.formatDuration(volumeTime, DURATION_FORMAT);

    long bucketTime = TimeUnit.NANOSECONDS.toMillis(bucketCreationTime.get())
        / threadPoolSize;
    String prettyAverageBucketTime =
        DurationFormatUtils.formatDuration(bucketTime, DURATION_FORMAT);

    long averageKeyCreationTime =
        TimeUnit.NANOSECONDS.toMillis(keyCreationTime.get())
            / threadPoolSize;
    String prettyAverageKeyCreationTime = DurationFormatUtils
        .formatDuration(averageKeyCreationTime, DURATION_FORMAT);

    long averageKeyWriteTime =
        TimeUnit.NANOSECONDS.toMillis(keyWriteTime.get()) / threadPoolSize;
    String prettyAverageKeyWriteTime = DurationFormatUtils
        .formatDuration(averageKeyWriteTime, DURATION_FORMAT);

    out.println();
    out.println("***************************************************");
    out.println("Status: " + (exception ? "Failed" : "Success"));
    out.println("Git Base Revision: " + VersionInfo.getRevision());
    out.println("Number of Volumes created: " + numberOfVolumesCreated);
    out.println("Number of Buckets created: " + numberOfBucketsCreated);
    out.println("Number of Keys added: " + numberOfKeysAdded);
    out.println("Ratis replication factor: " + factor.name());
    out.println("Ratis replication type: " + type.name());
    out.println(
        "Average Time spent in volume creation: " + prettyAverageVolumeTime);
    out.println(
        "Average Time spent in bucket creation: " + prettyAverageBucketTime);
    out.println(
        "Average Time spent in key creation: " + prettyAverageKeyCreationTime);
    out.println(
        "Average Time spent in key write: " + prettyAverageKeyWriteTime);
    out.println("Total bytes written: " + totalBytesWritten);
    if (validateWrites) {
      out.println("Total number of writes validated: " +
          totalWritesValidated);
      out.println("Writes validated: " +
          (100.0 * totalWritesValidated / numberOfKeysAdded.get())
          + " %");
      out.println("Successful validation: " +
          writeValidationSuccessCount);
      out.println("Unsuccessful validation: " +
          writeValidationFailureCount);
    }
    out.println("Total Execution time: " + execTime);
    out.println("***************************************************");

    if (jsonDir != null) {

      String[][] quantileTime =
          new String[CoronaOps.values().length][QUANTILES + 1];
      String[] deviations = new String[CoronaOps.values().length];
      String[] means = new String[CoronaOps.values().length];
      for (CoronaOps ops : CoronaOps.values()) {
        Snapshot snapshot = histograms.get(ops.ordinal()).getSnapshot();
        for (int i = 0; i <= QUANTILES; i++) {
          quantileTime[ops.ordinal()][i] = DurationFormatUtils.formatDuration(
              TimeUnit.NANOSECONDS
                  .toMillis((long) snapshot.getValue((1.0 / QUANTILES) * i)),
              DURATION_FORMAT);
        }
        deviations[ops.ordinal()] = DurationFormatUtils.formatDuration(
            TimeUnit.NANOSECONDS.toMillis((long) snapshot.getStdDev()),
            DURATION_FORMAT);
        means[ops.ordinal()] = DurationFormatUtils.formatDuration(
            TimeUnit.NANOSECONDS.toMillis((long) snapshot.getMean()),
            DURATION_FORMAT);
      }

      CoronaJobInfo jobInfo = new CoronaJobInfo().setExecTime(execTime)
          .setGitBaseRevision(VersionInfo.getRevision())
          .setMeanVolumeCreateTime(means[CoronaOps.VOLUME_CREATE.ordinal()])
          .setDeviationVolumeCreateTime(
              deviations[CoronaOps.VOLUME_CREATE.ordinal()])
          .setTenQuantileVolumeCreateTime(
              quantileTime[CoronaOps.VOLUME_CREATE.ordinal()])
          .setMeanBucketCreateTime(means[CoronaOps.BUCKET_CREATE.ordinal()])
          .setDeviationBucketCreateTime(
              deviations[CoronaOps.BUCKET_CREATE.ordinal()])
          .setTenQuantileBucketCreateTime(
              quantileTime[CoronaOps.BUCKET_CREATE.ordinal()])
          .setMeanKeyCreateTime(means[CoronaOps.KEY_CREATE.ordinal()])
          .setDeviationKeyCreateTime(deviations[CoronaOps.KEY_CREATE.ordinal()])
          .setTenQuantileKeyCreateTime(
              quantileTime[CoronaOps.KEY_CREATE.ordinal()])
          .setMeanKeyWriteTime(means[CoronaOps.KEY_WRITE.ordinal()])
          .setDeviationKeyWriteTime(deviations[CoronaOps.KEY_WRITE.ordinal()])
          .setTenQuantileKeyWriteTime(
              quantileTime[CoronaOps.KEY_WRITE.ordinal()]);
      String jsonName =
          new SimpleDateFormat("yyyyMMddHHmmss").format(Time.now()) + ".json";
      String jsonPath = jsonDir + "/" + jsonName;
      FileOutputStream os = null;
      try {
        os = new FileOutputStream(jsonPath);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD,
            JsonAutoDetect.Visibility.ANY);
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        writer.writeValue(os, jobInfo);
      } catch (FileNotFoundException e) {
        out.println("Json File could not be created for the path: " + jsonPath);
        out.println(e);
      } catch (IOException e) {
        out.println("Json object could not be created");
        out.println(e);
      } finally {
        try {
          if (os != null) {
            os.close();
          }
        } catch (IOException e) {
          LOG.warn("Could not close the output stream for json", e);
        }
      }
    }
  }

  /**
   * Returns the number of volumes created.
   * @return volume count.
   */
  @VisibleForTesting
  int getNumberOfVolumesCreated() {
    return numberOfVolumesCreated.get();
  }

  /**
   * Returns the number of buckets created.
   * @return bucket count.
   */
  @VisibleForTesting
  int getNumberOfBucketsCreated() {
    return numberOfBucketsCreated.get();
  }

  /**
   * Returns the number of keys added.
   * @return keys count.
   */
  @VisibleForTesting
  long getNumberOfKeysAdded() {
    return numberOfKeysAdded.get();
  }

  /**
   * Returns true if random validation of write is enabled.
   * @return validateWrites
   */
  @VisibleForTesting
  boolean getValidateWrites() {
    return validateWrites;
  }

  /**
   * Returns the number of keys validated.
   * @return validated key count.
   */
  @VisibleForTesting
  long getTotalKeysValidated() {
    return totalWritesValidated;
  }

  /**
   * Returns the number of successful validation.
   * @return successful validation count.
   */
  @VisibleForTesting
  long getSuccessfulValidationCount() {
    return writeValidationSuccessCount;
  }

  /**
   * Returns the number of unsuccessful validation.
   * @return unsuccessful validation count.
   */
  @VisibleForTesting
  long getUnsuccessfulValidationCount() {
    return writeValidationFailureCount;
  }

  /**
   * Returns the length of the common key value initialized.
   * @return key value length initialized.
   */
  @VisibleForTesting
  long getKeyValueLength(){
    return keyValue.length;
  }

  /**
   * Wrapper to hold ozone key-value pair.
   */
  private static class KeyValue {

    /**
     * Bucket name associated with the key-value.
     */
    private OzoneBucket bucket;
    /**
     * Key name associated with the key-value.
     */
    private String key;
    /**
     * Value associated with the key-value.
     */
    private byte[] value;

    /**
     * Constructs a new ozone key-value pair.
     *
     * @param key   key part
     * @param value value part
     */
    KeyValue(OzoneBucket bucket, String key, byte[] value) {
      this.bucket = bucket;
      this.key = key;
      this.value = value;
    }
  }

  private class OfflineProcessor implements Runnable {

    private int totalBuckets;
    private int totalKeys;
    private String volumeName;

    OfflineProcessor(String volumeName) {
      this.totalBuckets = Integer.parseInt(numOfBuckets);
      this.totalKeys = Integer.parseInt(numOfKeys);
      this.volumeName = volumeName;
    }

    @Override
    public void run() {
      LOG.trace("Creating volume: {}", volumeName);
      long start = System.nanoTime();
      OzoneVolume volume;
      try {
        objectStore.createVolume(volumeName);
        long volumeCreationDuration = System.nanoTime() - start;
        volumeCreationTime.getAndAdd(volumeCreationDuration);
        histograms.get(CoronaOps.VOLUME_CREATE.ordinal())
            .update(volumeCreationDuration);
        numberOfVolumesCreated.getAndIncrement();
        volume = objectStore.getVolume(volumeName);
      } catch (IOException e) {
        exception = true;
        LOG.error("Could not create volume", e);
        return;
      }

      Long threadKeyWriteTime = 0L;
      for (int j = 0; j < totalBuckets; j++) {
        String bucketName = "bucket-" + j + "-" +
            RandomStringUtils.randomNumeric(5);
        try {
          LOG.trace("Creating bucket: {} in volume: {}",
              bucketName, volume.getName());
          start = System.nanoTime();
          volume.createBucket(bucketName);
          long bucketCreationDuration = System.nanoTime() - start;
          histograms.get(CoronaOps.BUCKET_CREATE.ordinal())
              .update(bucketCreationDuration);
          bucketCreationTime.getAndAdd(bucketCreationDuration);
          numberOfBucketsCreated.getAndIncrement();
          OzoneBucket bucket = volume.getBucket(bucketName);
          for (int k = 0; k < totalKeys; k++) {
            String key = "key-" + k + "-" +
                RandomStringUtils.randomNumeric(5);
            byte[] randomValue =
                DFSUtil.string2Bytes(UUID.randomUUID().toString());
            try {
              LOG.trace("Adding key: {} in bucket: {} of volume: {}",
                  key, bucket, volume);
              long keyCreateStart = System.nanoTime();
              OzoneOutputStream os =
                  bucket.createKey(key, keySize, type, factor);
              long keyCreationDuration = System.nanoTime() - keyCreateStart;
              histograms.get(CoronaOps.KEY_CREATE.ordinal())
                  .update(keyCreationDuration);
              keyCreationTime.getAndAdd(keyCreationDuration);
              long keyWriteStart = System.nanoTime();
              os.write(keyValue);
              os.write(randomValue);
              os.close();
              long keyWriteDuration = System.nanoTime() - keyWriteStart;
              threadKeyWriteTime += keyWriteDuration;
              histograms.get(CoronaOps.KEY_WRITE.ordinal())
                  .update(keyWriteDuration);
              totalBytesWritten.getAndAdd(keySize);
              numberOfKeysAdded.getAndIncrement();
              if (validateWrites) {
                byte[] value = ArrayUtils.addAll(keyValue, randomValue);
                boolean validate = validationQueue.offer(
                    new KeyValue(bucket, key, value));
                if (validate) {
                  LOG.trace("Key {}, is queued for validation.", key);
                }
              }
            } catch (Exception e) {
              exception = true;
              LOG.error("Exception while adding key: {} in bucket: {}" +
                  " of volume: {}.", key, bucket, volume, e);
            }
          }
        } catch (Exception e) {
          exception = true;
          LOG.error("Exception while creating bucket: {}" +
              " in volume: {}.", bucketName, volume, e);
        }
      }

      keyWriteTime.getAndAdd(threadKeyWriteTime);
    }

  }

  private final class CoronaJobInfo {

    private String status;
    private String gitBaseRevision;
    private String jobStartTime;
    private String numOfVolumes;
    private String numOfBuckets;
    private String numOfKeys;
    private String numOfThreads;
    private String mode;
    private String dataWritten;
    private String execTime;
    private String replicationFactor;
    private String replicationType;

    private int keySize;

    private String totalThroughputPerSecond;

    private String meanVolumeCreateTime;
    private String deviationVolumeCreateTime;
    private String[] tenQuantileVolumeCreateTime;

    private String meanBucketCreateTime;
    private String deviationBucketCreateTime;
    private String[] tenQuantileBucketCreateTime;

    private String meanKeyCreateTime;
    private String deviationKeyCreateTime;
    private String[] tenQuantileKeyCreateTime;

    private String meanKeyWriteTime;
    private String deviationKeyWriteTime;
    private String[] tenQuantileKeyWriteTime;

    private CoronaJobInfo() {
      this.status = exception ? "Failed" : "Success";
      this.numOfVolumes = Corona.this.numOfVolumes;
      this.numOfBuckets = Corona.this.numOfBuckets;
      this.numOfKeys = Corona.this.numOfKeys;
      this.numOfThreads = Corona.this.numOfThreads;
      this.keySize = Corona.this.keySize;
      this.mode = Corona.this.mode;
      this.jobStartTime = Time.formatTime(Corona.this.jobStartTime);
      this.replicationFactor = Corona.this.factor.name();
      this.replicationType = Corona.this.type.name();

      long totalBytes =
          Long.parseLong(numOfVolumes) * Long.parseLong(numOfBuckets) * Long
              .parseLong(numOfKeys) * keySize;
      this.dataWritten = getInStorageUnits((double) totalBytes);
      this.totalThroughputPerSecond = getInStorageUnits(
          (totalBytes * 1.0) / TimeUnit.NANOSECONDS
              .toSeconds(Corona.this.keyWriteTime.get() / threadPoolSize));
    }

    private String getInStorageUnits(Double value) {
      double size;
      OzoneQuota.Units unit;
      if ((long) (value / OzoneConsts.TB) != 0) {
        size = value / OzoneConsts.TB;
        unit = OzoneQuota.Units.TB;
      } else if ((long) (value / OzoneConsts.GB) != 0) {
        size = value / OzoneConsts.GB;
        unit = OzoneQuota.Units.GB;
      } else if ((long) (value / OzoneConsts.MB) != 0) {
        size = value / OzoneConsts.MB;
        unit = OzoneQuota.Units.MB;
      } else if ((long) (value / OzoneConsts.KB) != 0) {
        size = value / OzoneConsts.KB;
        unit = OzoneQuota.Units.KB;
      } else {
        size = value;
        unit = OzoneQuota.Units.BYTES;
      }
      return size + " " + unit;
    }

    public CoronaJobInfo setGitBaseRevision(String gitBaseRevisionVal) {
      gitBaseRevision = gitBaseRevisionVal;
      return this;
    }

    public CoronaJobInfo setExecTime(String execTimeVal) {
      execTime = execTimeVal;
      return this;
    }

    public CoronaJobInfo setMeanKeyWriteTime(String deviationKeyWriteTimeVal) {
      this.meanKeyWriteTime = deviationKeyWriteTimeVal;
      return this;
    }

    public CoronaJobInfo setDeviationKeyWriteTime(
        String deviationKeyWriteTimeVal) {
      this.deviationKeyWriteTime = deviationKeyWriteTimeVal;
      return this;
    }

    public CoronaJobInfo setTenQuantileKeyWriteTime(
        String[] tenQuantileKeyWriteTimeVal) {
      this.tenQuantileKeyWriteTime = tenQuantileKeyWriteTimeVal;
      return this;
    }

    public CoronaJobInfo setMeanKeyCreateTime(String deviationKeyWriteTimeVal) {
      this.meanKeyCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public CoronaJobInfo setDeviationKeyCreateTime(
        String deviationKeyCreateTimeVal) {
      this.deviationKeyCreateTime = deviationKeyCreateTimeVal;
      return this;
    }

    public CoronaJobInfo setTenQuantileKeyCreateTime(
        String[] tenQuantileKeyCreateTimeVal) {
      this.tenQuantileKeyCreateTime = tenQuantileKeyCreateTimeVal;
      return this;
    }

    public CoronaJobInfo setMeanBucketCreateTime(
        String deviationKeyWriteTimeVal) {
      this.meanBucketCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public CoronaJobInfo setDeviationBucketCreateTime(
        String deviationBucketCreateTimeVal) {
      this.deviationBucketCreateTime = deviationBucketCreateTimeVal;
      return this;
    }

    public CoronaJobInfo setTenQuantileBucketCreateTime(
        String[] tenQuantileBucketCreateTimeVal) {
      this.tenQuantileBucketCreateTime = tenQuantileBucketCreateTimeVal;
      return this;
    }

    public CoronaJobInfo setMeanVolumeCreateTime(
        String deviationKeyWriteTimeVal) {
      this.meanVolumeCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public CoronaJobInfo setDeviationVolumeCreateTime(
        String deviationVolumeCreateTimeVal) {
      this.deviationVolumeCreateTime = deviationVolumeCreateTimeVal;
      return this;
    }

    public CoronaJobInfo setTenQuantileVolumeCreateTime(
        String[] tenQuantileVolumeCreateTimeVal) {
      this.tenQuantileVolumeCreateTime = tenQuantileVolumeCreateTimeVal;
      return this;
    }

    public String getJobStartTime() {
      return jobStartTime;
    }

    public String getNumOfVolumes() {
      return numOfVolumes;
    }

    public String getNumOfBuckets() {
      return numOfBuckets;
    }

    public String getNumOfKeys() {
      return numOfKeys;
    }

    public String getNumOfThreads() {
      return numOfThreads;
    }

    public String getMode() {
      return mode;
    }

    public String getExecTime() {
      return execTime;
    }

    public String getReplicationFactor() {
      return replicationFactor;
    }

    public String getReplicationType() {
      return replicationType;
    }

    public String getStatus() {
      return status;
    }

    public int getKeySize() {
      return keySize;
    }

    public String getGitBaseRevision() {
      return gitBaseRevision;
    }

    public String getDataWritten() {
      return dataWritten;
    }

    public String getTotalThroughputPerSecond() {
      return totalThroughputPerSecond;
    }

    public String getMeanVolumeCreateTime() {
      return meanVolumeCreateTime;
    }

    public String getDeviationVolumeCreateTime() {
      return deviationVolumeCreateTime;
    }

    public String[] getTenQuantileVolumeCreateTime() {
      return tenQuantileVolumeCreateTime;
    }

    public String getMeanBucketCreateTime() {
      return meanBucketCreateTime;
    }

    public String getDeviationBucketCreateTime() {
      return deviationBucketCreateTime;
    }

    public String[] getTenQuantileBucketCreateTime() {
      return tenQuantileBucketCreateTime;
    }

    public String getMeanKeyCreateTime() {
      return meanKeyCreateTime;
    }

    public String getDeviationKeyCreateTime() {
      return deviationKeyCreateTime;
    }

    public String[] getTenQuantileKeyCreateTime() {
      return tenQuantileKeyCreateTime;
    }

    public String getMeanKeyWriteTime() {
      return meanKeyWriteTime;
    }

    public String getDeviationKeyWriteTime() {
      return deviationKeyWriteTime;
    }

    public String[] getTenQuantileKeyWriteTime() {
      return tenQuantileKeyWriteTime;
    }
  }

  private class ProgressBar implements Runnable {

    private static final long REFRESH_INTERVAL = 1000L;

    private PrintStream stream;
    private Supplier<Long> currentValue;
    private long maxValue;

    ProgressBar(PrintStream stream, Supplier<Long> currentValue,
        long maxValue) {
      this.stream = stream;
      this.currentValue = currentValue;
      this.maxValue = maxValue;
    }

    @Override
    public void run() {
      try {
        stream.println();
        long value;
        while ((value = currentValue.get()) < maxValue) {
          print(value);
          if (completed) {
            break;
          }
          Thread.sleep(REFRESH_INTERVAL);
        }
        if (exception) {
          stream.println();
          stream.println("Incomplete termination, " +
              "check log for exception.");
        } else {
          print(maxValue);
        }
        stream.println();
      } catch (InterruptedException e) {
      }
    }

    /**
     * Given current value prints the progress bar.
     *
     * @param value
     */
    private void print(long value) {
      stream.print('\r');
      double percent = 100.0 * value / maxValue;
      StringBuilder sb = new StringBuilder();
      sb.append(" " + String.format("%.2f", percent) + "% |");

      for (int i = 0; i <= percent; i++) {
        sb.append('█');
      }
      for (int j = 0; j < 100 - percent; j++) {
        sb.append(' ');
      }
      sb.append("|  ");
      sb.append(value + "/" + maxValue);
      long timeInSec = TimeUnit.SECONDS.convert(
          System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
      String timeToPrint = String.format("%d:%02d:%02d", timeInSec / 3600,
          (timeInSec % 3600) / 60, timeInSec % 60);
      sb.append(" Time: " + timeToPrint);
      stream.print(sb);
    }
  }

  /**
   * Validates the write done in ozone cluster.
   */
  private class Validator implements Runnable {

    @Override
    public void run() {
      while (!completed) {
        try {
          KeyValue kv = validationQueue.poll(5, TimeUnit.SECONDS);
          if (kv != null) {

            OzoneInputStream is = kv.bucket.readKey(kv.key);
            byte[] value = new byte[kv.value.length];
            int length = is.read(value);
            totalWritesValidated++;
            if (length == kv.value.length && Arrays.equals(value, kv.value)) {
              writeValidationSuccessCount++;
            } else {
              writeValidationFailureCount++;
              LOG.warn("Data validation error for key {}/{}/{}",
                  kv.bucket.getVolumeName(), kv.bucket, kv.key);
              LOG.warn("Expected: {}, Actual: {}",
                  DFSUtil.bytes2String(kv.value),
                  DFSUtil.bytes2String(value));
            }
          }
        } catch (IOException | InterruptedException ex) {
          LOG.error("Exception while validating write: " + ex.getMessage());
        }
      }
    }
  }
}
