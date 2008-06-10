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
package org.apache.hadoop.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.*;
import org.apache.log4j.Level;

/**
 * Main class for a series of name-node benchmarks.
 * 
 * Each benchmark measures throughput and average execution time 
 * of a specific name-node operation, e.g. file creation or block reports.
 * 
 * The benchmark does not involve any other hadoop components
 * except for the name-node. Each operation is executed
 * by calling directly the respective name-node method.
 * The name-node here is real all other components are simulated.
 * 
 * Command line arguments for the benchmark include:<br>
 * 1) total number of operations to be performed,<br>
 * 2) number of threads to run these operations,<br>
 * 3) followed by operation specific input parameters.
 * 
 * Then the benchmark generates inputs for each thread so that the
 * input generation overhead does not effect the resulting statistics.
 * The number of operations performed by threads practically is the same. 
 * Precisely, the difference between the number of operations 
 * performed by any two threads does not exceed 1.
 * 
 * Then the benchmark executes the specified number of operations using 
 * the specified number of threads and outputs the resulting stats.
 */
public class NNThroughputBenchmark implements FSConstants {
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NNThroughputBenchmark");
  private static final int BLOCK_SIZE = 16;

  static Configuration config;
  static NameNode nameNode;

  private final UserGroupInformation ugi;

  NNThroughputBenchmark(Configuration conf) throws IOException, LoginException {
    config = conf;
    ugi = UnixUserGroupInformation.login(config);
    UserGroupInformation.setCurrentUGI(ugi);

    // We do not need many handlers, since each thread simulates a handler
    // by calling name-node methods directly
    config.setInt("dfs.namenode.handler.count", 1);
    // set exclude file
    config.set("dfs.hosts.exclude", "${hadoop.tmp.dir}/dfs/hosts/exclude");
    File excludeFile = new File(config.get("dfs.hosts.exclude", "exclude"));
    if(! excludeFile.exists()) {
      if(!excludeFile.getParentFile().mkdirs())
        throw new IOException("NNThroughputBenchmark: cannot mkdir " + excludeFile);
    }
    new FileOutputStream(excludeFile).close();
    // Start the NameNode
    String[] args = new String[] {};
    nameNode = NameNode.createNameNode(args, config);
  }

  void close() throws IOException {
    nameNode.stop();
  }

  static void turnOffNameNodeLogging() {
    // change log level to ERROR: NameNode.LOG & NameNode.stateChangeLog
    ((Log4JLogger)NameNode.LOG).getLogger().setLevel(Level.ERROR);
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ERROR);
    ((Log4JLogger)NetworkTopology.LOG).getLogger().setLevel(Level.ERROR);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ERROR);
  }

  /**
   * Base class for collecting operation statistics.
   * 
   * Overload this class in order to run statistics for a 
   * specific name-node operation.
   */
  abstract class OperationStatsBase {
    protected static final String BASE_DIR_NAME = "/nnThroughputBenchmark";
    protected static final String OP_ALL_NAME = "all";
    protected static final String OP_ALL_USAGE = "-op all <other ops options>";

    protected String baseDir;
    protected short replication;
    protected int  numThreads = 0;        // number of threads
    protected int  numOpsRequired = 0;    // number of operations requested
    protected int  numOpsExecuted = 0;    // number of operations executed
    protected long cumulativeTime = 0;    // sum of times for each op
    protected long elapsedTime = 0;       // time from start to finish

    protected List<StatsDaemon> daemons;

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
    abstract void parseArguments(String[] args) throws IOException;

    /**
     * Generate inputs for each daemon thread.
     * 
     * @param opsPerThread number of inputs for each thread.
     * @throws IOException
     */
    abstract void generateInputs(int[] opsPerThread) throws IOException;

    /**
     * This corresponds to the arg1 argument of 
     * {@link #executeOp(int, int, String)}, which can have different meanings
     * depending on the operation performed.
     * 
     * @param daemonId
     * @return the argument
     */
    abstract String getExecutionArgument(int daemonId);

    /**
     * Execute name-node operation.
     * 
     * @param daemonId id of the daemon calling this method.
     * @param inputIdx serial index of the operation called by the deamon.
     * @param arg1 operation specific argument.
     * @return time of the individual name-node call.
     * @throws IOException
     */
    abstract long executeOp(int daemonId, int inputIdx, String arg1) throws IOException;

    /**
     * Print the results of the benchmarking.
     */
    abstract void printResults();

    OperationStatsBase() {
      baseDir = BASE_DIR_NAME + "/" + getOpName();
      replication = (short) config.getInt("dfs.replication", 3);
      numOpsRequired = 10;
      numThreads = 3;
    }

    void benchmark() throws IOException {
      daemons = new ArrayList<StatsDaemon>();
      long start = 0;
      try {
        numOpsExecuted = 0;
        cumulativeTime = 0;
        if(numThreads < 1)
          return;
        int tIdx = 0; // thread index < nrThreads
        int opsPerThread[] = new int[numThreads];
        for(int opsScheduled = 0; opsScheduled < numOpsRequired; 
                                  opsScheduled += opsPerThread[tIdx++]) {
          // execute  in a separate thread
          opsPerThread[tIdx] = (numOpsRequired-opsScheduled)/(numThreads-tIdx);
          if(opsPerThread[tIdx] == 0)
            opsPerThread[tIdx] = 1;
        }
        // if numThreads > numOpsRequired then the remaining threads will do nothing
        for(; tIdx < numThreads; tIdx++)
          opsPerThread[tIdx] = 0;
        turnOffNameNodeLogging();
        generateInputs(opsPerThread);
        for(tIdx=0; tIdx < numThreads; tIdx++)
          daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));
        start = System.currentTimeMillis();
        LOG.info("Starting " + numOpsRequired + " " + getOpName() + "(s).");
        for(StatsDaemon d : daemons)
          d.start();
      } finally {
        while(isInPorgress()) {
          // try {Thread.sleep(500);} catch (InterruptedException e) {}
        }
        elapsedTime = System.currentTimeMillis() - start;
        for(StatsDaemon d : daemons) {
          incrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
          // System.out.println(d.toString() + ": ops Exec = " + d.localNumOpsExecuted);
        }
      }
    }

    private boolean isInPorgress() {
      for(StatsDaemon d : daemons)
        if(d.isInProgress())
          return true;
      return false;
    }

    void cleanUp() throws IOException {
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      nameNode.delete(getBaseDir(), true);
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
      return numOpsExecuted == 0 ? 0 : cumulativeTime / numOpsExecuted;
    }

    double getOpsPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numOpsExecuted / elapsedTime;
    }

    String getBaseDir() {
      return baseDir;
    }

    String getClientName(int idx) {
      return getOpName() + "-client-" + idx;
    }

    void incrementStats(int ops, long time) {
      numOpsExecuted += ops;
      cumulativeTime += time;
    }

    /**
     * Parse first 2 arguments, corresponding to the "-op" option.
     * 
     * @param args
     * @return true if operation is all, which means that options not related
     * to this operation should be ignored, or false otherwise, meaning
     * that usage should be printed when an unrelated option is encountered.
     * @throws IOException
     */
    protected boolean verifyOpArgument(String[] args) {
      if(args.length < 2 || ! args[0].startsWith("-op"))
        printUsage();
      String type = args[1];
      if(OP_ALL_NAME.equals(type)) {
        type = getOpName();
        return true;
      }
      if(!getOpName().equals(type))
        printUsage();
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
    private int daemonId;
    private int opsPerThread;
    private String arg1;      // argument passed to executeOp()
    private volatile int  localNumOpsExecuted = 0;
    private volatile long localCumulativeTime = 0;
    private OperationStatsBase statsOp;

    StatsDaemon(int daemonId, int nrOps, OperationStatsBase op) {
      this.daemonId = daemonId;
      this.opsPerThread = nrOps;
      this.statsOp = op;
      setName(toString());
    }

    public void run() {
      UserGroupInformation.setCurrentUGI(ugi);
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

    public String toString() {
      return "StatsDaemon-" + daemonId;
    }

    void benchmarkOne() throws IOException {
      for(int idx = 0; idx < opsPerThread; idx++) {
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
   * File name generator.
   * 
   * Each directory contains not more than a fixed number (filesPerDir) 
   * of files and directories.
   * When the number of files in one directory reaches the maximum,
   * the generator creates a new directory and proceeds generating files in it.
   * The generated namespace tree is balanced that is any path to a leaf
   * file is not less than the height of the tree minus one.
   */
  private static class FileGenerator {
    private static final int DEFAULT_FILES_PER_DIRECTORY = 32;
    // Average file name size is 16.5 bytes
    private static final String FILE_NAME_PREFFIX ="ThrouputBenchfile"; // 17 bytes
    private static final String DIR_NAME_PREFFIX = "ThrouputBenchDir";  // 16 bytes
    // private static final int NUM_CLIENTS = 100;

    private int[] pathIndecies = new int[20]; // this will support up to 32**20 = 2**100 = 10**30 files
    private String baseDir;
    private String currentDir;
    private int filesPerDirectory = DEFAULT_FILES_PER_DIRECTORY;
    private long fileCount;

    FileGenerator(String baseDir, int filesPerDir) {
      this.baseDir = baseDir;
      this.filesPerDirectory = filesPerDir;
      reset();
    }

    String getNextDirName() {
      int depth = 0;
      while(pathIndecies[depth] >= 0)
        depth++;
      int level;
      for(level = depth-1; 
          level >= 0 && pathIndecies[level] == filesPerDirectory-1; level--)
        pathIndecies[level] = 0;
      if(level < 0)
        pathIndecies[depth] = 0;
      else
        pathIndecies[level]++;
      level = 0;
      String next = baseDir;
      while(pathIndecies[level] >= 0)
        next = next + "/" + DIR_NAME_PREFFIX + pathIndecies[level++];
      return next; 
    }

    synchronized String getNextFileName() {
      long fNum = fileCount % filesPerDirectory;
      if(fNum == 0) {
        currentDir = getNextDirName();
      }
      String fn = currentDir + "/" + FILE_NAME_PREFFIX + fileCount;
      fileCount++;
      return fn;
    }

    private synchronized void reset() {
      Arrays.fill(pathIndecies, -1);
      fileCount = 0L;
      currentDir = "";
    }
  }

  /**
   * File creation statistics.
   * 
   * Each thread creates the same (+ or -1) number of files.
   * File names are pre-generated during initialization.
   * The created files do not have blocks.
   */
  class CreateFileStats extends OperationStatsBase {
    // Operation types
    static final String OP_CREATE_NAME = "create";
    static final String OP_CREATE_USAGE = 
      "-op create [-threads T] [-files N] [-filesPerDir P]";

    protected FileGenerator nameGenerator;
    protected String[][] fileNames;

    CreateFileStats(String[] args) {
      super();
      parseArguments(args);
    }

    String getOpName() {
      return OP_CREATE_NAME;
    }

    void parseArguments(String[] args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrFilesPerDir = 4;
      for (int i = 2; i < args.length; i++) {       // parse command line
        if(args[i].equals("-files")) {
          if(i+1 == args.length)  printUsage();
          numOpsRequired = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-threads")) {
          if(i+1 == args.length)  printUsage();
          numThreads = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-filesPerDir")) {
          if(i+1 == args.length)  printUsage();
          nrFilesPerDir = Integer.parseInt(args[++i]);
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileGenerator(getBaseDir(), nrFilesPerDir);
    }

    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length"; 
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      // int generatedFileIdx = 0;
      LOG.info("Generate " + numOpsRequired + " intputs for " + getOpName());
      fileNames = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        fileNames[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          fileNames[idx][jdx] = nameGenerator.getNextFileName();
      }
    }

    void dummyActionNoSynch(int daemonId, int fileIdx) {
      for(int i=0; i < 2000; i++)
        fileNames[daemonId][fileIdx].contains(""+i);
    }

    /**
     * returns client name
     */
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do file create.
     */
    long executeOp(int daemonId, int inputIdx, String clientName) 
    throws IOException {
      long start = System.currentTimeMillis();
      // dummyActionNoSynch(fileIdx);
      nameNode.create(fileNames[daemonId][inputIdx], FsPermission.getDefault(),
                      clientName, true, replication, BLOCK_SIZE);
      long end = System.currentTimeMillis();
      return end-start;
    }

    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrFiles = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrFilesPerDir = " + nameGenerator.filesPerDirectory);
      printStats();
    }
  }

  /**
   * Open file statistics.
   * 
   * Each thread creates the same (+ or -1) number of files.
   * File names are pre-generated during initialization.
   * The created files do not have blocks.
   */
  class OpenFileStats extends CreateFileStats {
    // Operation types
    static final String OP_OPEN_NAME = "open";
    static final String OP_OPEN_USAGE = 
      "-op open [-threads T] [-files N] [-filesPerDir P]";

    OpenFileStats(String[] args) {
      super(args);
    }

    String getOpName() {
      return OP_OPEN_NAME;
    }

    void generateInputs(int[] opsPerThread) throws IOException {
      // create files using opsPerThread
      String[] createArgs = new String[] {
              "-op", "create", 
              "-threads", String.valueOf(this.numThreads), 
              "-files", String.valueOf(numOpsRequired),
              "-filesPerDir", String.valueOf(nameGenerator.filesPerDirectory)};
      CreateFileStats opCreate =  new CreateFileStats(createArgs);
      opCreate.benchmark();
      LOG.info("Created " + numOpsRequired + " files.");
      nameNode.rename(opCreate.getBaseDir(), getBaseDir());
      // use the same files for open
      super.generateInputs(opsPerThread);
    }

    /**
     * Do file open.
     */
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = System.currentTimeMillis();
      nameNode.getBlockLocations(fileNames[daemonId][inputIdx], 0L, BLOCK_SIZE);
      long end = System.currentTimeMillis();
      return end-start;
    }
  }

  /**
   * Minimal datanode simulator.
   */
  private static class TinyDatanode implements Comparable<String> {
    private static final long DF_CAPACITY = 100*1024*1024;
    private static final long DF_USED = 0;
    
    NamespaceInfo nsInfo;
    DatanodeRegistration dnRegistration;
    Block[] blocks;
    int nrBlocks; // actual number of blocks

    /**
     * Get data-node in the form 
     * <host name> : <port>
     * where port is a 6 digit integer.
     * This is necessary in order to provide lexocographic ordering.
     * Host names are all the same, the ordering goes by port numbers.
     */
    private static String getNodeName(int port) throws IOException {
      String machineName = DNS.getDefaultHost("default", "default");
      String sPort = String.valueOf(100000 + port);
      if(sPort.length() > 6)
        throw new IOException("Too many data-nodes.");
      return machineName + ":" + sPort;
    }

    TinyDatanode(int dnIdx, int blockCapacity) throws IOException {
      dnRegistration = new DatanodeRegistration(getNodeName(dnIdx));
      this.blocks = new Block[blockCapacity];
      this.nrBlocks = 0;
    }

    String getName() {
      return dnRegistration.getName();
    }

    void register() throws IOException {
      // get versions from the namenode
      nsInfo = nameNode.versionRequest();
      dnRegistration.setStorageInfo(new DataStorage(nsInfo, ""));
      DataNode.setNewStorageID(dnRegistration);
      // register datanode
      dnRegistration = nameNode.register(dnRegistration);
    }

    /**
     * Send a heartbeat to the name-node.
     * Ignore reply commands.
     */
    void sendHeartbeat() throws IOException {
      // register datanode
      DatanodeCommand cmd = nameNode.sendHeartbeat(
          dnRegistration, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, 0, 0);
      if(cmd != null)
        LOG.debug("sendHeartbeat Name-node reply: " + cmd.getAction());
    }

    boolean addBlock(Block blk) {
      if(nrBlocks == blocks.length) {
        LOG.debug("Cannot add block: datanode capacity = " + blocks.length);
        return false;
      }
      blocks[nrBlocks] = blk;
      nrBlocks++;
      return true;
    }

    void formBlockReport() {
      // fill remaining slots with blocks that do not exist
      for(int idx = blocks.length-1; idx >= nrBlocks; idx--)
        blocks[idx] = new Block(blocks.length - idx, 0, 0);
    }

    public int compareTo(String name) {
      return getName().compareTo(name);
    }

    /**
     * Send a heartbeat to the name-node and replicate blocks if requested.
     */
    int replicateBlocks() throws IOException {
      // register datanode
      DatanodeCommand cmd = nameNode.sendHeartbeat(
          dnRegistration, DF_CAPACITY, DF_USED, DF_CAPACITY - DF_USED, 0, 0);
      if(cmd == null || cmd.getAction() != DatanodeProtocol.DNA_TRANSFER)
        return 0;
      // Send a copy of a block to another datanode
      BlockCommand bcmd = (BlockCommand)cmd;
      return transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
    }

    /**
     * Transfer blocks to another data-node.
     * Just report on behalf of the other data-node
     * that the blocks have been received.
     */
    private int transferBlocks( Block blocks[], 
                                DatanodeInfo xferTargets[][] 
                              ) throws IOException {
      for(int i = 0; i < blocks.length; i++) {
        DatanodeInfo blockTargets[] = xferTargets[i];
        for(int t = 0; t < blockTargets.length; t++) {
          DatanodeInfo dnInfo = blockTargets[t];
          DatanodeRegistration receivedDNReg;
          receivedDNReg = new DatanodeRegistration(dnInfo.getName());
          receivedDNReg.setStorageInfo(
                          new DataStorage(nsInfo, dnInfo.getStorageID()));
          receivedDNReg.setInfoPort(dnInfo.getInfoPort());
          nameNode.blockReceived( receivedDNReg, 
                                  new Block[] {blocks[i]},
                                  new String[] {DataNode.EMPTY_DEL_HINT});
        }
      }
      return blocks.length;
    }
  }

  /**
   * Block report statistics.
   * 
   * Each thread here represents its own data-node.
   * Data-nodes send the same block report each time.
   * The block report may contain missing or non-existing blocks.
   */
  class BlockReportStats extends OperationStatsBase {
    static final String OP_BLOCK_REPORT_NAME = "blockReport";
    static final String OP_BLOCK_REPORT_USAGE = 
      "-op blockReport [-datanodes T] [-reports N] " +
      "[-blocksPerReport B] [-blocksPerFile F]";

    private int blocksPerReport;
    private int blocksPerFile;
    private TinyDatanode[] datanodes; // array of data-nodes sorted by name

    BlockReportStats(String[] args) {
      super();
      this.blocksPerReport = 100;
      this.blocksPerFile = 10;
      // set heartbeat interval to 3 min, so that expiration were 40 min
      config.setLong("dfs.heartbeat.interval", 3 * 60);
      parseArguments(args);
      // adjust replication to the number of data-nodes
      this.replication = (short)Math.min((int)replication, getNumDatanodes());
    }

    /**
     * Each thread pretends its a data-node here.
     */
    private int getNumDatanodes() {
      return numThreads;
    }

    String getOpName() {
      return OP_BLOCK_REPORT_NAME;
    }

    void parseArguments(String[] args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      for (int i = 2; i < args.length; i++) {       // parse command line
        if(args[i].equals("-reports")) {
          if(i+1 == args.length)  printUsage();
          numOpsRequired = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-datanodes")) {
          if(i+1 == args.length)  printUsage();
          numThreads = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-blocksPerReport")) {
          if(i+1 == args.length)  printUsage();
          blocksPerReport = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-blocksPerFile")) {
          if(i+1 == args.length)  printUsage();
          blocksPerFile = Integer.parseInt(args[++i]);
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
    }

    void generateInputs(int[] ignore) throws IOException {
      int nrDatanodes = getNumDatanodes();
      int nrBlocks = (int)Math.ceil((double)blocksPerReport * nrDatanodes 
                                    / replication);
      int nrFiles = (int)Math.ceil((double)nrBlocks / blocksPerFile);
      datanodes = new TinyDatanode[nrDatanodes];
      // create data-nodes
      String prevDNName = "";
      for(int idx=0; idx < nrDatanodes; idx++) {
        datanodes[idx] = new TinyDatanode(idx, blocksPerReport);
        datanodes[idx].register();
        assert datanodes[idx].getName().compareTo(prevDNName) > 0
          : "Data-nodes must be sorted lexicographically.";
        datanodes[idx].sendHeartbeat();
        prevDNName = datanodes[idx].getName();
      }
      int numResolved = 0;
      DatanodeInfo[] dnInfos = nameNode.getDatanodeReport(DatanodeReportType.ALL);
      do {
        numResolved = 0;
        for (DatanodeInfo info : dnInfos) {
          if (!info.getNetworkLocation().equals(NetworkTopology.UNRESOLVED)) {
            numResolved++;
          } else {
            try {
              Thread.sleep(2);
            } catch (Exception e) {
            }
            dnInfos = nameNode.getDatanodeReport(DatanodeReportType.LIVE);
            break;
          }
        }
      } while (numResolved != nrDatanodes);

      // create files 
      LOG.info("Creating " + nrFiles + " with " + blocksPerFile + " blocks each.");
      FileGenerator nameGenerator;
      nameGenerator = new FileGenerator(getBaseDir(), 100);
      String clientName = getClientName(007);
      nameNode.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);
      for(int idx=0; idx < nrFiles; idx++) {
        String fileName = nameGenerator.getNextFileName();
        nameNode.create(fileName, FsPermission.getDefault(),
                        clientName, true, replication, BLOCK_SIZE);
        addBlocks(fileName, clientName);
        nameNode.complete(fileName, clientName);
      }
      // prepare block reports
      for(int idx=0; idx < nrDatanodes; idx++) {
        datanodes[idx].formBlockReport();
      }
    }

    private void addBlocks(String fileName, String clientName) throws IOException {
      for(int jdx = 0; jdx < blocksPerFile; jdx++) {
        LocatedBlock loc = nameNode.addBlock(fileName, clientName);
        for(DatanodeInfo dnInfo : loc.getLocations()) {
          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getName());
          datanodes[dnIdx].addBlock(loc.getBlock());
          nameNode.blockReceived(
              datanodes[dnIdx].dnRegistration, 
              new Block[] {loc.getBlock()},
              new String[] {""});
        }
      }
    }

    /**
     * Does not require the argument
     */
    String getExecutionArgument(int daemonId) {
      return null;
    }

    long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
      assert daemonId < numThreads : "Wrong daemonId.";
      TinyDatanode dn = datanodes[daemonId];
      long start = System.currentTimeMillis();
      nameNode.blockReport(dn.dnRegistration,
          BlockListAsLongs.convertToArrayLongs(dn.blocks));
      long end = System.currentTimeMillis();
      return end-start;
    }

    void printResults() {
      String blockDistribution = "";
      String delim = "(";
      for(int idx=0; idx < getNumDatanodes(); idx++) {
        blockDistribution += delim + datanodes[idx].nrBlocks;
        delim = ", ";
      }
      blockDistribution += ")";
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("reports = " + numOpsRequired);
      LOG.info("datanodes = " + numThreads + " " + blockDistribution);
      LOG.info("blocksPerReport = " + blocksPerReport);
      LOG.info("blocksPerFile = " + blocksPerFile);
      printStats();
    }
  }   // end BlockReportStats

  /**
   * Measures how fast replication monitor can compute data-node work.
   * 
   * It runs only one thread until no more work can be scheduled.
   */
  class ReplicationStats extends OperationStatsBase {
    static final String OP_REPLICATION_NAME = "replication";
    static final String OP_REPLICATION_USAGE = 
      "-op replication [-datanodes T] [-nodesToDecommission D] " +
      "[-nodeReplicationLimit C] [-totalBlocks B] [-replication R]";

    private BlockReportStats blockReportObject;
    private int numDatanodes;
    private int nodesToDecommission;
    private int nodeReplicationLimit;
    private int totalBlocks;
    private int numDecommissionedBlocks;
    private int numPendingBlocks;

    ReplicationStats(String[] args) {
      super();
      numThreads = 1;
      numDatanodes = 3;
      nodesToDecommission = 1;
      nodeReplicationLimit = 100;
      totalBlocks = 100;
      parseArguments(args);
      // number of operations is 4 times the number of decommissioned
      // blocks divided by the number of needed replications scanned 
      // by the replication monitor in one iteration
      numOpsRequired = (totalBlocks*replication*nodesToDecommission*2)
            / (numDatanodes*numDatanodes);

      String[] blkReportArgs = {
        "-op", "blockReport",
        "-datanodes", String.valueOf(numDatanodes),
        "-blocksPerReport", String.valueOf(totalBlocks*replication/numDatanodes),
        "-blocksPerFile", String.valueOf(numDatanodes)};
      blockReportObject = new BlockReportStats(blkReportArgs);
      numDecommissionedBlocks = 0;
      numPendingBlocks = 0;
    }

    String getOpName() {
      return OP_REPLICATION_NAME;
    }

    void parseArguments(String[] args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      for (int i = 2; i < args.length; i++) {       // parse command line
        if(args[i].equals("-datanodes")) {
          if(i+1 == args.length)  printUsage();
          numDatanodes = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-nodesToDecommission")) {
          if(i+1 == args.length)  printUsage();
          nodesToDecommission = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-nodeReplicationLimit")) {
          if(i+1 == args.length)  printUsage();
          nodeReplicationLimit = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-totalBlocks")) {
          if(i+1 == args.length)  printUsage();
          totalBlocks = Integer.parseInt(args[++i]);
        } else if(args[i].equals("-replication")) {
          if(i+1 == args.length)  printUsage();
          replication = Short.parseShort(args[++i]);
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
    }

    void generateInputs(int[] ignore) throws IOException {
      // start data-nodes; create a bunch of files; generate block reports.
      blockReportObject.generateInputs(ignore);
      // stop replication monitor
      nameNode.namesystem.replthread.interrupt();
      try {
        nameNode.namesystem.replthread.join();
      } catch(InterruptedException ei) {
        return;
      }
      // report blocks once
      int nrDatanodes = blockReportObject.getNumDatanodes();
      for(int idx=0; idx < nrDatanodes; idx++) {
        blockReportObject.executeOp(idx, 0, null);
      }
      // decommission data-nodes
      decommissionNodes();
      // set node replication limit
      nameNode.namesystem.setNodeReplicationLimit(nodeReplicationLimit);
    }

    private void decommissionNodes() throws IOException {
      String excludeFN = config.get("dfs.hosts.exclude", "exclude");
      FileOutputStream excludeFile = new FileOutputStream(excludeFN);
      excludeFile.getChannel().truncate(0L);
      int nrDatanodes = blockReportObject.getNumDatanodes();
      numDecommissionedBlocks = 0;
      for(int i=0; i < nodesToDecommission; i++) {
        TinyDatanode dn = blockReportObject.datanodes[nrDatanodes-1-i];
        numDecommissionedBlocks += dn.nrBlocks;
        excludeFile.write(dn.getName().getBytes());
        excludeFile.write('\n');
        LOG.info("Datanode " + dn.getName() + " is decommissioned.");
      }
      excludeFile.close();
      nameNode.refreshNodes();
    }

    /**
     * Does not require the argument
     */
    String getExecutionArgument(int daemonId) {
      return null;
    }

    long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
      assert daemonId < numThreads : "Wrong daemonId.";
      long start = System.currentTimeMillis();
      // compute datanode work
      int work = nameNode.namesystem.computeDatanodeWork();
      long end = System.currentTimeMillis();
      numPendingBlocks += work;
      if(work == 0)
        daemons.get(daemonId).terminate();
      return end-start;
    }

    void printResults() {
      String blockDistribution = "";
      String delim = "(";
      int totalReplicas = 0;
      for(int idx=0; idx < blockReportObject.getNumDatanodes(); idx++) {
        totalReplicas += blockReportObject.datanodes[idx].nrBlocks;
        blockDistribution += delim + blockReportObject.datanodes[idx].nrBlocks;
        delim = ", ";
      }
      blockDistribution += ")";
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("numOpsRequired = " + numOpsRequired);
      LOG.info("datanodes = " + numDatanodes + " " + blockDistribution);
      LOG.info("decommissioned datanodes = " + nodesToDecommission);
      LOG.info("datanode replication limit = " + nodeReplicationLimit);
      LOG.info("total blocks = " + totalBlocks);
      printStats();
      LOG.info("decommissioned blocks = " + numDecommissionedBlocks);
      LOG.info("pending replications = " + numPendingBlocks);
      LOG.info("replications per sec: " + getBlocksPerSecond());
    }

    private double getBlocksPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numPendingBlocks / elapsedTime;
    }

  }   // end ReplicationStats

  static void printUsage() {
    System.err.println("Usage: NNThroughputBenchmark"
        + "\n\t"    + OperationStatsBase.OP_ALL_USAGE
        + " | \n\t" + CreateFileStats.OP_CREATE_USAGE
        + " | \n\t" + OpenFileStats.OP_OPEN_USAGE
        + " | \n\t" + BlockReportStats.OP_BLOCK_REPORT_USAGE
        + " | \n\t" + ReplicationStats.OP_REPLICATION_USAGE
    );
    System.exit(-1);
  }

  /**
   * Main method of the benchmark.
   * @param args command line parameters
   */
  public static void runBenchmark(Configuration conf, String[] args) throws Exception {
    if(args.length < 2 || ! args[0].startsWith("-op"))
      printUsage();

    String type = args[1];
    boolean runAll = OperationStatsBase.OP_ALL_NAME.equals(type);

    NNThroughputBenchmark bench = null;
    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
    try {
      bench = new NNThroughputBenchmark(conf);
      if(runAll || CreateFileStats.OP_CREATE_NAME.equals(type)) {
        opStat = bench.new CreateFileStats(args);
        ops.add(opStat);
      }
      if(runAll || OpenFileStats.OP_OPEN_NAME.equals(type)) {
        opStat = bench.new OpenFileStats(args);
        ops.add(opStat);
      }
      if(runAll || BlockReportStats.OP_BLOCK_REPORT_NAME.equals(type)) {
        opStat = bench.new BlockReportStats(args);
        ops.add(opStat);
      }
      if(runAll || ReplicationStats.OP_REPLICATION_NAME.equals(type)) {
        opStat = bench.new ReplicationStats(args);
        ops.add(opStat);
      }
      if(ops.size() == 0)
        printUsage();
      // run each bencmark
      for(OperationStatsBase op : ops) {
        LOG.info("Starting benchmark: " + op.getOpName());
        op.benchmark();
        op.cleanUp();
      }
      // print statistics
      for(OperationStatsBase op : ops) {
        LOG.info("");
        op.printResults();
      }
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  public static void main(String[] args) throws Exception {
    runBenchmark(new Configuration(), args);
  }
}
