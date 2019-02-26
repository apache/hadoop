
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

package org.apache.hadoop.ozone.freon;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import static java.lang.Math.min;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Data generator tool to generate as much keys as possible.
 */
@Command(name = "randomkeys",
    aliases = "rk",
    description = "Generate volumes/buckets and put generated keys.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public final class RandomKeyGenerator implements Callable<Void> {

  @ParentCommand
  private Freon freon;

  enum FreonOps {
    VOLUME_CREATE,
    BUCKET_CREATE,
    KEY_CREATE,
    KEY_WRITE
  }

  private static final String RATIS = "ratis";

  private static final String DURATION_FORMAT = "HH:mm:ss,SSS";

  private static final int QUANTILES = 10;

  private static final Logger LOG =
      LoggerFactory.getLogger(RandomKeyGenerator.class);

  private boolean completed = false;
  private boolean exception = false;

  @Option(names = "--numOfThreads",
      description = "number of threads to be launched for the run",
      defaultValue = "10")
  private int numOfThreads = 10;

  @Option(names = "--numOfVolumes",
      description = "specifies number of Volumes to be created in offline mode",
      defaultValue = "10")
  private int numOfVolumes = 10;

  @Option(names = "--numOfBuckets",
      description = "specifies number of Buckets to be created per Volume",
      defaultValue = "1000")
  private int numOfBuckets = 1000;

  @Option(
      names = "--numOfKeys",
      description = "specifies number of Keys to be created per Bucket",
      defaultValue = "500000"
  )
  private int numOfKeys = 500000;

  @Option(
      names = "--keySize",
      description = "Specifies the size of Key in bytes to be created",
      defaultValue = "10240"
  )
  private int keySize = 10240;

  @Option(
      names = "--json",
      description = "directory where json is created."
  )
  private String jsonDir;

  @Option(
      names = "--replicationType",
      description = "Replication type (STAND_ALONE, RATIS)",
      defaultValue = "STAND_ALONE"
  )
  private ReplicationType type = ReplicationType.STAND_ALONE;

  @Option(
      names = "--factor",
      description = "Replication factor (ONE, THREE)",
      defaultValue = "ONE"
  )
  private ReplicationFactor factor = ReplicationFactor.ONE;

  private int threadPoolSize;
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

  private OzoneConfiguration ozoneConfiguration;
  private ProgressBar progressbar;

  RandomKeyGenerator() {
  }

  @VisibleForTesting
  RandomKeyGenerator(OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
  }

  public void init(OzoneConfiguration configuration) throws IOException {
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
    ozoneClient = OzoneClientFactory.getClient(configuration);
    objectStore = ozoneClient.getObjectStore();
    for (FreonOps ops : FreonOps.values()) {
      histograms.add(ops.ordinal(), new Histogram(new UniformReservoir()));
    }
    if (freon != null) {
      freon.startHttpServer();
    }
  }

  @Override
  public Void call() throws Exception {
    if (ozoneConfiguration != null) {
      init(ozoneConfiguration);
    } else {
      init(freon.createOzoneConfiguration());
    }

    keyValue =
        DFSUtil.string2Bytes(RandomStringUtils.randomAscii(keySize - 36));

    LOG.info("Number of Threads: " + numOfThreads);
    threadPoolSize =
        min(numOfVolumes, numOfThreads);
    processor = Executors.newFixedThreadPool(threadPoolSize);
    addShutdownHook();

    LOG.info("Number of Volumes: {}.", numOfVolumes);
    LOG.info("Number of Buckets per Volume: {}.", numOfBuckets);
    LOG.info("Number of Keys per Bucket: {}.", numOfKeys);
    LOG.info("Key size: {} bytes", keySize);
    for (int i = 0; i < numOfVolumes; i++) {
      String volume = "vol-" + i + "-" +
          RandomStringUtils.randomNumeric(5);
      processor.submit(new OfflineProcessor(volume));
    }

    Thread validator = null;
    if (validateWrites) {
      totalWritesValidated = 0L;
      writeValidationSuccessCount = 0L;
      writeValidationFailureCount = 0L;

      validationQueue =
          new ArrayBlockingQueue<>(numOfThreads);
      validator = new Thread(new Validator());
      validator.start();
      LOG.info("Data validation is enabled.");
    }

    Supplier<Long> currentValue;
    long maxValue;

    currentValue = () -> numberOfKeysAdded.get();
    maxValue = numOfVolumes *
            numOfBuckets *
            numOfKeys;

    progressbar = new ProgressBar(System.out, maxValue, currentValue);

    LOG.info("Starting progress bar Thread.");

    progressbar.start();

    processor.shutdown();
    processor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    completed = true;

    if (exception) {
      progressbar.terminate();
    } else {
      progressbar.shutdown();
    }

    if (validator != null) {
      validator.join();
    }
    ozoneClient.close();
    return null;
  }

  /**
   * Adds ShutdownHook to print statistics.
   */
  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          printStats(System.out);
          if (freon != null) {
            freon.stopHttpServer();
          }
        }));
  }
  /**
   * Prints stats of {@link Freon} run to the PrintStream.
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
          new String[FreonOps.values().length][QUANTILES + 1];
      String[] deviations = new String[FreonOps.values().length];
      String[] means = new String[FreonOps.values().length];
      for (FreonOps ops : FreonOps.values()) {
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

      FreonJobInfo jobInfo = new FreonJobInfo().setExecTime(execTime)
          .setGitBaseRevision(VersionInfo.getRevision())
          .setMeanVolumeCreateTime(means[FreonOps.VOLUME_CREATE.ordinal()])
          .setDeviationVolumeCreateTime(
              deviations[FreonOps.VOLUME_CREATE.ordinal()])
          .setTenQuantileVolumeCreateTime(
              quantileTime[FreonOps.VOLUME_CREATE.ordinal()])
          .setMeanBucketCreateTime(means[FreonOps.BUCKET_CREATE.ordinal()])
          .setDeviationBucketCreateTime(
              deviations[FreonOps.BUCKET_CREATE.ordinal()])
          .setTenQuantileBucketCreateTime(
              quantileTime[FreonOps.BUCKET_CREATE.ordinal()])
          .setMeanKeyCreateTime(means[FreonOps.KEY_CREATE.ordinal()])
          .setDeviationKeyCreateTime(deviations[FreonOps.KEY_CREATE.ordinal()])
          .setTenQuantileKeyCreateTime(
              quantileTime[FreonOps.KEY_CREATE.ordinal()])
          .setMeanKeyWriteTime(means[FreonOps.KEY_WRITE.ordinal()])
          .setDeviationKeyWriteTime(deviations[FreonOps.KEY_WRITE.ordinal()])
          .setTenQuantileKeyWriteTime(
              quantileTime[FreonOps.KEY_WRITE.ordinal()]);
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
   *
   * @return volume count.
   */
  @VisibleForTesting
  int getNumberOfVolumesCreated() {
    return numberOfVolumesCreated.get();
  }

  /**
   * Returns the number of buckets created.
   *
   * @return bucket count.
   */
  @VisibleForTesting
  int getNumberOfBucketsCreated() {
    return numberOfBucketsCreated.get();
  }

  /**
   * Returns the number of keys added.
   *
   * @return keys count.
   */
  @VisibleForTesting
  long getNumberOfKeysAdded() {
    return numberOfKeysAdded.get();
  }

  /**
   * Returns true if random validation of write is enabled.
   *
   * @return validateWrites
   */
  @VisibleForTesting
  boolean getValidateWrites() {
    return validateWrites;
  }

  /**
   * Returns the number of keys validated.
   *
   * @return validated key count.
   */
  @VisibleForTesting
  long getTotalKeysValidated() {
    return totalWritesValidated;
  }

  /**
   * Returns the number of successful validation.
   *
   * @return successful validation count.
   */
  @VisibleForTesting
  long getSuccessfulValidationCount() {
    return writeValidationSuccessCount;
  }

  /**
   * Returns the number of unsuccessful validation.
   *
   * @return unsuccessful validation count.
   */
  @VisibleForTesting
  long getUnsuccessfulValidationCount() {
    return writeValidationFailureCount;
  }

  /**
   * Returns the length of the common key value initialized.
   *
   * @return key value length initialized.
   */
  @VisibleForTesting
  long getKeyValueLength() {
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
      this.totalBuckets = numOfBuckets;
      this.totalKeys = numOfKeys;
      this.volumeName = volumeName;
    }

    @Override
    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    public void run() {
      LOG.trace("Creating volume: {}", volumeName);
      long start = System.nanoTime();
      OzoneVolume volume;
      try (Scope scope = GlobalTracer.get().buildSpan("createVolume")
          .startActive(true)) {
        objectStore.createVolume(volumeName);
        long volumeCreationDuration = System.nanoTime() - start;
        volumeCreationTime.getAndAdd(volumeCreationDuration);
        histograms.get(FreonOps.VOLUME_CREATE.ordinal())
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
          try (Scope scope = GlobalTracer.get().buildSpan("createBucket")
              .startActive(true)) {
            volume.createBucket(bucketName);
            long bucketCreationDuration = System.nanoTime() - start;
            histograms.get(FreonOps.BUCKET_CREATE.ordinal())
                .update(bucketCreationDuration);
            bucketCreationTime.getAndAdd(bucketCreationDuration);
            numberOfBucketsCreated.getAndIncrement();
          }
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
              try (Scope scope = GlobalTracer.get().buildSpan("createKey")
                  .startActive(true)) {
                OzoneOutputStream os =
                    bucket
                        .createKey(key, keySize, type, factor, new HashMap<>());
                long keyCreationDuration = System.nanoTime() - keyCreateStart;
                histograms.get(FreonOps.KEY_CREATE.ordinal())
                    .update(keyCreationDuration);
                keyCreationTime.getAndAdd(keyCreationDuration);
                long keyWriteStart = System.nanoTime();
                try (Scope writeScope = GlobalTracer.get()
                    .buildSpan("writeKeyData")
                    .startActive(true)) {
                  os.write(keyValue);
                  os.write(randomValue);
                  os.close();
                }

                long keyWriteDuration = System.nanoTime() - keyWriteStart;

                threadKeyWriteTime += keyWriteDuration;
                histograms.get(FreonOps.KEY_WRITE.ordinal())
                    .update(keyWriteDuration);
                totalBytesWritten.getAndAdd(keySize);
                numberOfKeysAdded.getAndIncrement();
              }
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

  private final class FreonJobInfo {

    private String status;
    private String gitBaseRevision;
    private String jobStartTime;
    private int numOfVolumes;
    private int numOfBuckets;
    private int numOfKeys;
    private int numOfThreads;
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

    private FreonJobInfo() {
      this.status = exception ? "Failed" : "Success";
      this.numOfVolumes = RandomKeyGenerator.this.numOfVolumes;
      this.numOfBuckets = RandomKeyGenerator.this.numOfBuckets;
      this.numOfKeys = RandomKeyGenerator.this.numOfKeys;
      this.numOfThreads = RandomKeyGenerator.this.numOfThreads;
      this.keySize = RandomKeyGenerator.this.keySize;
      this.jobStartTime = Time.formatTime(RandomKeyGenerator.this.jobStartTime);
      this.replicationFactor = RandomKeyGenerator.this.factor.name();
      this.replicationType = RandomKeyGenerator.this.type.name();

      long totalBytes =
          (long) numOfVolumes * numOfBuckets * numOfKeys * keySize;
      this.dataWritten = getInStorageUnits((double) totalBytes);
      this.totalThroughputPerSecond = getInStorageUnits(
          (totalBytes * 1.0) / TimeUnit.NANOSECONDS
              .toSeconds(
                  RandomKeyGenerator.this.keyWriteTime.get() / threadPoolSize));
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

    public FreonJobInfo setGitBaseRevision(String gitBaseRevisionVal) {
      gitBaseRevision = gitBaseRevisionVal;
      return this;
    }

    public FreonJobInfo setExecTime(String execTimeVal) {
      execTime = execTimeVal;
      return this;
    }

    public FreonJobInfo setMeanKeyWriteTime(String deviationKeyWriteTimeVal) {
      this.meanKeyWriteTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationKeyWriteTime(
        String deviationKeyWriteTimeVal) {
      this.deviationKeyWriteTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileKeyWriteTime(
        String[] tenQuantileKeyWriteTimeVal) {
      this.tenQuantileKeyWriteTime = tenQuantileKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setMeanKeyCreateTime(String deviationKeyWriteTimeVal) {
      this.meanKeyCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationKeyCreateTime(
        String deviationKeyCreateTimeVal) {
      this.deviationKeyCreateTime = deviationKeyCreateTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileKeyCreateTime(
        String[] tenQuantileKeyCreateTimeVal) {
      this.tenQuantileKeyCreateTime = tenQuantileKeyCreateTimeVal;
      return this;
    }

    public FreonJobInfo setMeanBucketCreateTime(
        String deviationKeyWriteTimeVal) {
      this.meanBucketCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationBucketCreateTime(
        String deviationBucketCreateTimeVal) {
      this.deviationBucketCreateTime = deviationBucketCreateTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileBucketCreateTime(
        String[] tenQuantileBucketCreateTimeVal) {
      this.tenQuantileBucketCreateTime = tenQuantileBucketCreateTimeVal;
      return this;
    }

    public FreonJobInfo setMeanVolumeCreateTime(
        String deviationKeyWriteTimeVal) {
      this.meanVolumeCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationVolumeCreateTime(
        String deviationVolumeCreateTimeVal) {
      this.deviationVolumeCreateTime = deviationVolumeCreateTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileVolumeCreateTime(
        String[] tenQuantileVolumeCreateTimeVal) {
      this.tenQuantileVolumeCreateTime = tenQuantileVolumeCreateTimeVal;
      return this;
    }

    public String getJobStartTime() {
      return jobStartTime;
    }

    public int getNumOfVolumes() {
      return numOfVolumes;
    }

    public int getNumOfBuckets() {
      return numOfBuckets;
    }

    public int getNumOfKeys() {
      return numOfKeys;
    }

    public int getNumOfThreads() {
      return numOfThreads;
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
              LOG.warn("Expected checksum: {}, Actual checksum: {}",
                  DigestUtils.md5Hex(kv.value),
                  DigestUtils.md5Hex(value));
            }
          }
        } catch (IOException | InterruptedException ex) {
          LOG.error("Exception while validating write: " + ex.getMessage());
        }
      }
    }
  }

  @VisibleForTesting
  public void setNumOfVolumes(int numOfVolumes) {
    this.numOfVolumes = numOfVolumes;
  }

  @VisibleForTesting
  public void setNumOfBuckets(int numOfBuckets) {
    this.numOfBuckets = numOfBuckets;
  }

  @VisibleForTesting
  public void setNumOfKeys(int numOfKeys) {
    this.numOfKeys = numOfKeys;
  }

  @VisibleForTesting
  public void setNumOfThreads(int numOfThreads) {
    this.numOfThreads = numOfThreads;
  }

  @VisibleForTesting
  public void setKeySize(int keySize) {
    this.keySize = keySize;
  }

  @VisibleForTesting
  public void setType(ReplicationType type) {
    this.type = type;
  }

  @VisibleForTesting
  public void setFactor(ReplicationFactor factor) {
    this.factor = factor;
  }

  @VisibleForTesting
  public void setValidateWrites(boolean validateWrites) {
    this.validateWrites = validateWrites;
  }
}
