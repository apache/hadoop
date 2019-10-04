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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Base class for simplified performance tests.
 */
public class BaseFreonGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseFreonGenerator.class);

  private static final int CHECK_INTERVAL_MILLIS = 1000;

  private static final String DIGEST_ALGORITHM = "MD5";

  private static final Pattern ENV_VARIABLE_IN_PATTERN =
      Pattern.compile("__(.+?)__");

  @ParentCommand
  private Freon freonCommand;

  @Option(names = {"-n", "--number-of-tests"},
      description = "Number of the generated objects.",
      defaultValue = "1000")
  private long testNo = 1000;

  @Option(names = {"-t", "--threads", "--thread"},
      description = "Number of threads used to execute",
      defaultValue = "10")
  private int threadNo;

  @Option(names = {"-f", "--fail-at-end"},
      description = "If turned on, all the tasks will be executed even if "
          + "there are failures.")
  private boolean failAtEnd;

  @Option(names = {"-p", "--prefix"},
      description = "Unique identifier of the test execution. Usually used as"
          + " a prefix of the generated object names. If empty, a random name"
          + " will be generated",
      defaultValue = "")
  private String prefix = "";

  private MetricRegistry metrics = new MetricRegistry();

  private ExecutorService executor;

  private AtomicLong successCounter;

  private AtomicLong failureCounter;

  private long startTime;

  private PathSchema pathSchema;

  /**
   * The main logic to execute a test generator.
   *
   * @param provider creates the new steps to execute.
   */
  public void runTests(TaskProvider provider) {

    executor = Executors.newFixedThreadPool(threadNo);

    ProgressBar progressBar =
        new ProgressBar(System.out, testNo, successCounter::get);
    progressBar.start();

    startTime = System.currentTimeMillis();
    //schedule the execution of all the tasks.

    for (long i = 0; i < testNo; i++) {

      final long counter = i;

      executor.execute(() -> {
        try {

          //in case of an other failed test, we shouldn't execute more tasks.
          if (!failAtEnd && failureCounter.get() > 0) {
            return;
          }

          provider.executeNextTask(counter);
          successCounter.incrementAndGet();
        } catch (Exception e) {
          failureCounter.incrementAndGet();
          LOG.error("Error on executing task", e);
        }
      });
    }

    // wait until all tasks are executed

    while (successCounter.get() + failureCounter.get() < testNo && (
        failureCounter.get() == 0 || failAtEnd)) {
      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    //shutdown everything
    if (failureCounter.get() > 0 && !failAtEnd) {
      progressBar.terminate();
    } else {
      progressBar.shutdown();
    }
    executor.shutdown();
    try {
      executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    if (failureCounter.get() > 0) {
      throw new RuntimeException("One ore more freon test is failed.");
    }
  }

  /**
   * Initialize internal counters, and variables. Call it before runTests.
   */
  public void init() {

    successCounter = new AtomicLong(0);
    failureCounter = new AtomicLong(0);

    if (prefix.length() == 0) {
      prefix = RandomStringUtils.randomAlphanumeric(10);
    } else {
      //replace environment variables to support multi-node execution
      prefix = resolvePrefix(prefix);
    }
    LOG.info("Executing test with prefix {}", prefix);

    pathSchema = new PathSchema(prefix);

    Runtime.getRuntime().addShutdownHook(
        new Thread(this::printReport));
  }

  /**
   * Resolve environment variables in the prefixes.
   */
  public String resolvePrefix(String inputPrefix) {
    Matcher m = ENV_VARIABLE_IN_PATTERN.matcher(inputPrefix);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String environment = System.getenv(m.group(1));
      m.appendReplacement(sb, environment != null ? environment : "");
    }
    m.appendTail(sb);
    return sb.toString();
  }

  /**
   * Print out reports from the executed tests.
   */
  public void printReport() {
    ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();
    reporter.report();
    System.out.println("Total execution time (sec): " + Math
        .round((System.currentTimeMillis() - startTime) / 1000.0));
    System.out.println("Failures: " + failureCounter.get());
    System.out.println("Successful executions: " + successCounter.get());
  }

  /**
   * Create the OM RPC client to use it for testing.
   */
  public OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      OzoneConfiguration conf) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    long omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    InetSocketAddress omAddress = OmUtils.getOmAddressForClients(conf);
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();
    return new OzoneManagerProtocolClientSideTranslatorPB(
        RPC.getProxy(OzoneManagerProtocolPB.class, omVersion, omAddress,
            ugi, conf, NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf)), clientId);
  }

  /**
   * Generate a key/file name based on the prefix and counter.
   */
  public String generateObjectName(long counter) {
    return pathSchema.getPath(counter);
  }

  /**
   * Create missing target volume/bucket.
   */
  public void ensureVolumeAndBucketExist(OzoneConfiguration ozoneConfiguration,
      String volumeName, String bucketName) throws IOException {

    try (OzoneClient rpcClient = OzoneClientFactory
        .getRpcClient(ozoneConfiguration)) {

      OzoneVolume volume = null;
      try {
        volume = rpcClient.getObjectStore().getVolume(volumeName);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
          rpcClient.getObjectStore().createVolume(volumeName);
          volume = rpcClient.getObjectStore().getVolume(volumeName);
        } else {
          throw ex;
        }
      }

      try {
        volume.getBucket(bucketName);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
          volume.createBucket(bucketName);
        } else {
          throw ex;
        }
      }
    }
  }

  /**
   * Create missing target volume.
   */
  public void ensureVolumeExists(
      OzoneConfiguration ozoneConfiguration,
      String volumeName) throws IOException {
    try (OzoneClient rpcClient = OzoneClientFactory
        .getRpcClient(ozoneConfiguration)) {

      try {
        rpcClient.getObjectStore().getVolume(volumeName);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
          rpcClient.getObjectStore().createVolume(volumeName);
        }
      }

    }
  }

  /**
   * Calculate checksum of a byte array.
   */
  public byte[] getDigest(byte[] content) throws IOException {
    DigestUtils dig = new DigestUtils(DIGEST_ALGORITHM);
    dig.getMessageDigest().reset();
    return dig.digest(content);
  }

  /**
   * Calculate checksum of an Input stream.
   */
  public byte[] getDigest(InputStream stream) throws IOException {
    DigestUtils dig = new DigestUtils(DIGEST_ALGORITHM);
    dig.getMessageDigest().reset();
    return dig.digest(stream);
  }

  public String getPrefix() {
    return prefix;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public OzoneConfiguration createOzoneConfiguration() {
    return freonCommand.createOzoneConfiguration();
  }
  /**
   * Simple contract to execute a new step during a freon test.
   */
  @FunctionalInterface
  public interface TaskProvider {
    void executeNextTask(long step) throws Exception;
  }

}
