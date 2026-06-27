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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.KMSUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main class for a series of KMS benchmarks.
 *
 * Each benchmark measures throughput and average execution time
 * of a specific kms operation, e.g. encrypt or decrypt of
 * Data Encryption Keys.
 *
 * The benchmark does not involve any other hadoop components
 * except for kms operations. Each operation is executed
 * by calling directly the respective kms operation.
 *
 * For usage, please see <a href="http://hadoop.apache.org/docs/current/
 * hadoop-project-dist/hadoop-common/Benchmarking.html#KMSBenchmark">
 * the documentation</a>.
 * Meanwhile, if you change the usage of this program, please also update the
 * documentation accordingly.
 */
public class KMSBenchmark implements Tool {
  private static final Logger LOG =
          LoggerFactory.getLogger(KMSBenchmark.class);

  private static final String GENERAL_OPTIONS_USAGE = "[-threads int] |" +
          " [-numops int] | [{-warmup (true|false)}]";

  private static Configuration config;

  private KeyProviderCryptoExtension kp;
  private KeyProviderCryptoExtension.EncryptedKeyVersion eek = null;
  private String encryptionKeyName = "systest";
  private boolean createEncryptionKey = false;
  private boolean warmupKey = false;
  private List<String> keys = new ArrayList<String>();

  KMSBenchmark(Configuration conf, String[] args)
          throws IOException {
    config = conf;
    kp = createKeyProviderCryptoExtension(config);
    try {
      eek = kp.generateEncryptedKey(encryptionKeyName);
    } catch (GeneralSecurityException e) {
      LOG.warn("failed to generate key", e);
    }
    // create key and/or warm up
    for (int i = 2; i < args.length; i++) {
      if (args[i].equals("-warmup")) {
        warmupKey = Boolean.parseBoolean(args[++i]);
      } else if (args[i].equals("-createkey")) {
        encryptionKeyName = args[++i];
      }
    }
    try {
      if (createEncryptionKey) {
        keys = kp.getKeys();
        if (!keys.contains(encryptionKeyName)) {
          kp.createKey(encryptionKeyName, KeyProvider.options(conf));
        } else {
          LOG.warn("encryption key already exists: {}",
                  encryptionKeyName);
        }
      }
      if (warmupKey) {
        kp.warmUpEncryptedKeys(encryptionKeyName);
      }
    } catch (GeneralSecurityException e) {
      LOG.warn(" failed to create or warmup encryption key", e);
    }
  }

  /**
   * Base class for collecting operation statistics.
   *
   * Overload this class in order to run statistics for a
   * specific kms operation.
   */
  abstract class OperationStatsBase {
    protected static final String OP_ALL_NAME = "all";
    protected static final String OP_ALL_USAGE =
            "-op all <other ops options>";

    // number of threads
    private int  numThreads = 0;

    // number of operations requested
    private int  numOpsRequired = 0;

    // number of operations executed
    private int  numOpsExecuted = 0;

    // sum of times for each op
    private long cumulativeTime = 0;

    // time from start to finish
    private long elapsedTime = 0;

    private List<StatsDaemon> daemons;

    /**
     * Operation name.
     */
    abstract String getOpName();

    /**
     * Parse command line arguments.
     *
     * @param args arguments
     * @throws IOException
     */
    abstract void parseArguments(List<String> args) throws IOException;

    /**
     * This corresponds to the arg1 argument of
     * {@link #executeOp(int, int, String)}, which can have
     * different meanings depending on the operation performed.
     *
     * @param daemonId id of the daemon calling this method
     * @return the argument
     */
    abstract String getExecutionArgument(int daemonId);

    /**
     * Execute kms operation.
     *
     * @param daemonId id of the daemon calling this method.
     * @param inputIdx serial index of the operation called by the deamon.
     * @param arg1 operation specific argument.
     * @return time of the individual kms call.
     * @throws IOException
     */
    abstract long executeOp(int daemonId, int inputIdx, String arg1)
            throws IOException;

    /**
     * Print the results of the benchmarking.
     */
    abstract void printResults();

    OperationStatsBase() {
      numOpsRequired = 10000;
      numThreads = 3;
    }

    void benchmark() throws IOException {
      daemons = new ArrayList<StatsDaemon>();
      long start = 0;
      try {
        numOpsExecuted = 0;
        cumulativeTime = 0;
        if (numThreads < 1) {
          return;
        }
        // thread index < nrThreads
        int tIdx = 0;
        int[] opsPerThread = new int[numThreads];
        for (int opsScheduled = 0; opsScheduled < numOpsRequired;
             opsScheduled += opsPerThread[tIdx++]) {
          // execute  in a separate thread
          opsPerThread[tIdx] =
                  (numOpsRequired-opsScheduled)/(numThreads-tIdx);
          if (opsPerThread[tIdx] == 0) {
            opsPerThread[tIdx] = 1;
          }
        }
        // if numThreads > numOpsRequired then the remaining threads
        // will do nothing
        for (; tIdx < numThreads; tIdx++) {
          opsPerThread[tIdx] = 0;
        }
        for (tIdx=0; tIdx < numThreads; tIdx++) {
          daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));
        }
        start = Time.now();
        LOG.info("Starting "+numOpsRequired+" "+getOpName()+"(s).");
        for (StatsDaemon d : daemons) {
          d.start();
        }
      } finally {
        while(isInProgress()) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {}
        }
        elapsedTime = Time.now() - start;
        for (StatsDaemon d : daemons) {
          incrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
          System.out.println(d.toString() + ": ops Exec = " +
                  d.localNumOpsExecuted);
        }
      }
    }

    private boolean isInProgress() {
      for (StatsDaemon d : daemons) {
        if (d.isInProgress()) {
          return true;
        }
      }
      return false;
    }

    void cleanUp() throws IOException {
    }

    int getNumOpsExecuted() {
      return numOpsExecuted;
    }

    long getCumulativeTime() {
      return cumulativeTime;
    }

    long getElapsedTime() {
      return elapsedTime;
    }

    long getAverageTime() {
      LOG.info("getAverageTime, cumulativeTime = " + cumulativeTime);
      LOG.info("getAverageTime, numOpsExecuted = " + numOpsExecuted);
      return numOpsExecuted == 0? 0 : cumulativeTime/numOpsExecuted;
    }

    double getOpsPerSecond() {
      return elapsedTime == 0?
              0 : 1000*(double)numOpsExecuted / elapsedTime;
    }

    String getClientName(int idx) {
      return getOpName() + "-client-" + idx;
    }

    void incrementStats(int ops, long time) {
      numOpsExecuted += ops;
      cumulativeTime += time;
    }

    int getNumThreads() {
      return numThreads;
    }

    void setNumThreads(int num) {
      numThreads = num;
    }

    int getNumOpsRequired() {
      return numOpsRequired;
    }

    void setNumOpsRequired(int num) {
      numOpsRequired = num;
    }

    /**
     * Parse first 2 arguments, corresponding to the "-op" option.
     *
     * @param args argument list
     * @return true if operation is all, which means that options not
     * related to this operation should be ignored, or false
     * otherwise, meaning that usage should be printed when an
     * unrelated option is encountered.
     */
    protected boolean verifyOpArgument(List<String> args) {
      if (args.size() < 2 || !args.get(0).startsWith("-op")) {
        printUsage();
      }

      // process common options
      String type = args.get(1);
      if (OP_ALL_NAME.equals(type)) {
        type = getOpName();
        return true;
      }
      if (!getOpName().equals(type)) {
        printUsage();
      }
      return false;
    }

    void printStats() {
      LOG.info("--- " + getOpName() + " stats  ---");
      LOG.info("# operations: " + getNumOpsExecuted());
      LOG.info("Elapsed Time: " + getElapsedTime());
      LOG.info(" Ops per sec: " + getOpsPerSecond());
      LOG.info("Average Time: " + getAverageTime());
    }
  }

  /**
   * One of the threads that perform stats operations.
   */
  private class StatsDaemon extends Thread {
    private final int daemonId;
    private int opsPerThread;
    private String arg1;      // argument passed to executeOp()
    private volatile int  localNumOpsExecuted = 0;
    private volatile long localCumulativeTime = 0;
    private final OperationStatsBase statsOp;

    StatsDaemon(int daemonId, int nOps, OperationStatsBase op) {
      this.daemonId = daemonId;
      this.opsPerThread = nOps;
      this.statsOp = op;
      setName(toString());
    }

    @Override
    public void run() {
      localNumOpsExecuted = 0;
      localCumulativeTime = 0;
      arg1 = statsOp.getExecutionArgument(daemonId);
      try {
        benchmarkOne();
      } catch(IOException ex) {
        LOG.error("StatsDaemon " + daemonId + " failed: \n"
            + StringUtils.stringifyException(ex));
      }
    }

    @Override
    public String toString() {
      return "StatsDaemon-" + daemonId;
    }

    void benchmarkOne() throws IOException {
      for (int idx = 0; idx < opsPerThread; idx++) {
        long stat = statsOp.executeOp(daemonId, idx, arg1);
        localNumOpsExecuted++;
        localCumulativeTime += stat;
      }
    }

    boolean isInProgress() {
      return localNumOpsExecuted < opsPerThread;
    }

    /**
     * Schedule to stop this daemon.
     */
    void terminate() {
      opsPerThread = localNumOpsExecuted;
    }
  }

  /**
   * Encrypt key statistics.
   *
   * Each thread encrypts the key.
   */
  class EncryptKeyStats extends OperationStatsBase {
    // Operation types
    static final String OP_ENCRYPT_KEY = "encrypt";
    static final String OP_ENCRYPT_USAGE =
            "-op encrypt [-threads T -numops N -warmup F]";

    EncryptKeyStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_ENCRYPT_KEY;
    }

    @Override
    void parseArguments(List<String> args) {
      verifyOpArgument(args);
      // parse command line
      for (int i = 2; i < args.size(); i++) {
        if (args.get(i).equals("-threads")) {
          if (i+1 == args.size()) {
            printUsage();
          }
          setNumThreads(Integer.parseInt(args.get(++i)));
        } else if (args.get(i).equals("-numops")) {
          setNumOpsRequired(Integer.parseInt(args.get(++i)));
        }
      }
    }

    /**
     * Returns client name.
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Execute key encryption.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName)
            throws IOException {
      long start = Time.now();
      try {
        eek = kp.generateEncryptedKey(encryptionKeyName);
      } catch (GeneralSecurityException e) {
        LOG.warn("failed to generate encrypted key", e);
      }

      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nOps = " + getNumOpsRequired());
      LOG.info("nThreads = " + getNumThreads());
      printStats();
    }
  }

  /**
   * Decrypt key statistics.
   *
   * Each thread decrypts the key.
   */
  class DecryptKeyStats extends OperationStatsBase {
    // Operation types
    static final String OP_DECRYPT_KEY = "decrypt";
    static final String OP_DECRYPT_USAGE =
            "-op decrypt [-threads T -numops N -warmup F]";

    DecryptKeyStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_DECRYPT_KEY;
    }

    @Override
    void parseArguments(List<String> args) {
      verifyOpArgument(args);
      // parse command line
      for (int i = 2; i < args.size(); i++) {
        if (args.get(i).equals("-threads")) {
          if (i+1 == args.size()) {
            printUsage();
          }
          setNumThreads(Integer.parseInt(args.get(++i)));
        } else if (args.get(i).equals("-numops")) {
          setNumOpsRequired(Integer.parseInt(args.get(++i)));
        }
      }
    }

    /**
     * returns client name.
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Execute key decryption.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName)
        throws IOException {
      long start = Time.now();
      try {
        kp.decryptEncryptedKey(eek);
      } catch (GeneralSecurityException e) {
        LOG.warn("failed to generate and/or decrypt key", e);
      }
      long end = Time.now();
      return end - start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrOps = " + getNumOpsRequired());
      LOG.info("nrThreads = " + getNumThreads());
      printStats();
    }
  }

  static void printUsage() {
    System.err.println("Usage: KMSBenchmark"
        + "\n\t"    + OperationStatsBase.OP_ALL_USAGE
        + " | \n\t" + EncryptKeyStats.OP_ENCRYPT_USAGE
        + " | \n\t" + DecryptKeyStats.OP_DECRYPT_USAGE
        + " | \n\t" + GENERAL_OPTIONS_USAGE
    );
    System.err.println();
    GenericOptionsParser.printGenericCommandUsage(System.err);
    ExitUtil.terminate(-1);
  }

  public static KeyProviderCryptoExtension createKeyProviderCryptoExtension(
          final Configuration conf) throws IOException {

    KeyProvider keyProvider = KMSUtil.createKeyProvider(conf,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
    if (keyProvider == null) {
      throw new IOException("Key provider was not configured.");
    }
    return KeyProviderCryptoExtension.
            createKeyProviderCryptoExtension(keyProvider);
  }

  public static void runBenchmark(Configuration conf, String[] args)
      throws Exception {
    KMSBenchmark bench = null;
    try {
      bench = new KMSBenchmark(conf, args);
      ToolRunner.run(bench, args);
    } finally {
      LOG.info("runBenchmark finished.");
    }
  }

  /**
   * Main method of the benchmark.
   * @param aArgs command line parameters
   */
  @Override // Tool
  public int run(String[] aArgs) throws Exception {
    List<String> args = new ArrayList<String>(Arrays.asList(aArgs));
    if (args.size() < 2 || !args.get(0).startsWith("-op")) {
      printUsage();
    }

    String type = args.get(1);
    boolean runAll = OperationStatsBase.OP_ALL_NAME.equals(type);

    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
    try {
      if (runAll || EncryptKeyStats.OP_ENCRYPT_KEY.equals(type)) {
        opStat = new EncryptKeyStats(args);
        ops.add(opStat);
      }
      if (runAll || DecryptKeyStats.OP_DECRYPT_KEY.equals(type)) {
        opStat = new DecryptKeyStats(args);
        ops.add(opStat);
      }
      if (ops.isEmpty()) {
        printUsage();
      }

      // run each benchmark
      for (OperationStatsBase op : ops) {
        LOG.info("Starting benchmark: " + op.getOpName());
        op.benchmark();
        op.cleanUp();
      }
      // print statistics
      for (OperationStatsBase op : ops) {
        LOG.info("");
        op.printResults();
      }
    } catch(Exception e) {
      LOG.error("failed to run benchmarks", e);
      throw e;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    runBenchmark(new Configuration(), args);
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override // Configurable
  public Configuration getConf() {
    return config;
  }
}