/**
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

package org.apache.hadoop.ozone;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ozone.io.OzoneOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

  private static final String HELP = "help";
  private static final String MODE = "mode";
  private static final String SOURCE = "source";
  private static final String NUM_OF_VOLUMES = "numOfVolumes";
  private static final String NUM_OF_BUCKETS = "numOfBuckets";
  private static final String NUM_OF_KEYS = "numOfKeys";

  private static final String MODE_DEFAULT = "offline";
  private static final String SOURCE_DEFAULT =
      "https://commoncrawl.s3.amazonaws.com/" +
          "crawl-data/CC-MAIN-2017-17/warc.paths.gz";
  private static final String NUM_OF_VOLUMES_DEFAULT = "10";
  private static final String NUM_OF_BUCKETS_DEFAULT = "1000";
  private static final String NUM_OF_KEYS_DEFAULT = "500000";

  private static final int NUM_OF_THREADS_DEFAULT = 10;

  private static final Logger LOG =
      LoggerFactory.getLogger(Corona.class);

  private boolean printUsage = false;

  private String mode;
  private String source;
  private String numOfVolumes;
  private String numOfBuckets;
  private String numOfKeys;

  private OzoneClient ozoneClient;
  private ExecutorService processor;

  private long startTime;

  private AtomicInteger numberOfVolumesCreated;
  private AtomicInteger numberOfBucketsCreated;
  private AtomicLong numberOfKeysAdded;

  private Corona(Configuration conf) throws IOException {
    startTime = System.nanoTime();
    numberOfVolumesCreated = new AtomicInteger();
    numberOfBucketsCreated = new AtomicInteger();
    numberOfKeysAdded = new AtomicLong();
    OzoneClientFactory.setConfiguration(conf);
    ozoneClient = OzoneClientFactory.getRpcClient();
    processor = Executors.newFixedThreadPool(NUM_OF_THREADS_DEFAULT);
  }

  @Override
  public int run(String[] args) throws Exception {
    GenericOptionsParser parser = new GenericOptionsParser(getConf(),
        getOzonePetaGenOptions(), args);
    parseOzonePetaGenOptions(parser.getCommandLine());
    if(printUsage) {
      usage();
      System.exit(0);
    }
    addShutdownHook();
    if(mode.equals("online")) {
      LOG.info("Mode: online");
      throw new UnsupportedOperationException("Not yet implemented.");
    } else {
      LOG.info("Mode: offline");
      LOG.info("Number of Volumes: {}.", numOfBuckets);
      LOG.info("Number of Buckets per Volume: {}.", numOfBuckets);
      LOG.info("Number of Keys per Bucket: {}.", numOfKeys);
      for(int i = 0; i < Integer.parseInt(numOfVolumes); i++) {
        String volume = "vol-" + i + "-" +
            RandomStringUtils.randomNumeric(5);
        processor.submit(new OfflineProcessor(volume));
      }
      processor.shutdown();
      processor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      return 0;
    }
  }

  private Options getOzonePetaGenOptions() {
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

    options.addOption(optHelp);
    options.addOption(optMode);
    options.addOption(optSource);
    options.addOption(optNumOfVolumes);
    options.addOption(optNumOfBuckets);
    options.addOption(optNumOfKeys);
    return options;
  }

  private void parseOzonePetaGenOptions(CommandLine cmdLine) {
    printUsage = cmdLine.hasOption(HELP);

    mode = cmdLine.hasOption(MODE) ?
        cmdLine.getOptionValue(MODE) : MODE_DEFAULT;

    source = cmdLine.hasOption(SOURCE) ?
        cmdLine.getOptionValue(SOURCE) : SOURCE_DEFAULT;

    numOfVolumes = cmdLine.hasOption(NUM_OF_VOLUMES) ?
        cmdLine.getOptionValue(NUM_OF_VOLUMES) : NUM_OF_VOLUMES_DEFAULT;

    numOfBuckets = cmdLine.hasOption(NUM_OF_BUCKETS) ?
        cmdLine.getOptionValue(NUM_OF_BUCKETS) : NUM_OF_BUCKETS_DEFAULT;

    numOfKeys = cmdLine.hasOption(NUM_OF_KEYS) ?
        cmdLine.getOptionValue(NUM_OF_KEYS) : NUM_OF_KEYS_DEFAULT;
  }

  private void usage() {
    System.out.println("Options supported are:");
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
    System.out.println("-help                           "
        + "prints usage.");
    System.out.println();
  }

  private class OfflineProcessor implements Runnable {

    private int totalBuckets;
    private int totalKeys;
    private String volume;

    OfflineProcessor(String volume) throws Exception {
      this.totalBuckets = Integer.parseInt(numOfBuckets);
      this.totalKeys = Integer.parseInt(numOfKeys);
      this.volume = volume;
      LOG.trace("Creating volume: {}", volume);
      ozoneClient.createVolume(this.volume);
      numberOfVolumesCreated.getAndIncrement();
    }

    @Override
    public void run() {
      for (int j = 0; j < totalBuckets; j++) {
        String bucket = "bucket-" + j + "-" +
            RandomStringUtils.randomNumeric(5);
        try {
          LOG.trace("Creating bucket: {} in volume: {}", bucket, volume);
          ozoneClient.createBucket(volume, bucket);
          numberOfBucketsCreated.getAndIncrement();
          for (int k = 0; k < totalKeys; k++) {
            String key = "key-" + k + "-" +
                RandomStringUtils.randomNumeric(5);
            byte[] value = RandomStringUtils.randomAscii(10240).getBytes();
            try {
              LOG.trace("Adding key: {} in bucket: {} of volume: {}",
                  key, bucket, volume);
              OzoneOutputStream os = ozoneClient.createKey(
                  volume, bucket, key, value.length);
              os.write(value);
              os.close();
              numberOfKeysAdded.getAndIncrement();
            } catch (Exception e) {
              LOG.error("Exception while adding key: {} in bucket: {}" +
                  " of volume: {}.", key, bucket, volume, e);
            }
          }
        } catch (Exception e) {
          LOG.error("Exception while creating bucket: {}" +
              " in volume: {}.", bucket, volume, e);
        }
      }
    }
  }

  /**
   * Adds ShutdownHook to print statistics.
   */
  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        printStats(System.out);
      }
    });
  }

  /**
   * Prints stats of {@link Corona} run to the PrintStream.
   *
   * @param out PrintStream
   */
  private void printStats(PrintStream out) {
    long timeInSec = TimeUnit.SECONDS.convert(
        System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    String timeToPrint = timeInSec < 60 ? timeInSec + " seconds" :
        TimeUnit.MINUTES.convert(timeInSec, TimeUnit.SECONDS) + " minuites";
    out.println("***************************************************");
    out.println("Number of Volumes created: " + numberOfVolumesCreated);
    out.println("Number of Buckets created: " + numberOfBucketsCreated);
    out.println("Number of Keys added: " + numberOfKeysAdded);
    out.println("Execution time: " + timeToPrint);
    out.println("***************************************************");
  }

  /**
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new OzoneConfiguration();
    int res = ToolRunner.run(conf, new Corona(conf), args);
    System.exit(res);
  }
}
