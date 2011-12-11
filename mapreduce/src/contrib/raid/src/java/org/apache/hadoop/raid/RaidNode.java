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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Thread;
import java.net.InetSocketAddress;
import java.net.URI;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.PolicyList;
import org.apache.hadoop.raid.protocol.RaidProtocol;
import org.apache.hadoop.raid.protocol.PolicyInfo.ErasureCodeType;

/**
 * A base class that implements {@link RaidProtocol}.
 *
 * use raid.classname to specify which implementation to use
 */
public abstract class RaidNode implements RaidProtocol {

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.raid.RaidNode");
  public static final long SLEEP_TIME = 10000L; // 10 seconds
  public static final int DEFAULT_PORT = 60000;
  // Default stripe length = 5, parity length for RS code = 3
  public static final int DEFAULT_STRIPE_LENGTH = 5;
  public static final int RS_PARITY_LENGTH_DEFAULT = 3;

  public static final String RS_PARITY_LENGTH_KEY = "hdfs.raidrs.paritylength";
  public static final String STRIPE_LENGTH_KEY = "hdfs.raid.stripeLength";

  public static final String DEFAULT_RAID_LOCATION = "/raid";
  public static final String RAID_LOCATION_KEY = "hdfs.raid.locations";
  public static final String DEFAULT_RAID_TMP_LOCATION = "/tmp/raid";
  public static final String RAID_TMP_LOCATION_KEY = "fs.raid.tmpdir";
  public static final String DEFAULT_RAID_HAR_TMP_LOCATION = "/tmp/raid_har";
  public static final String RAID_HAR_TMP_LOCATION_KEY = "fs.raid.hartmpdir";

  public static final String DEFAULT_RAIDRS_LOCATION = "/raidrs";
  public static final String RAIDRS_LOCATION_KEY = "hdfs.raidrs.locations";
  public static final String DEFAULT_RAIDRS_TMP_LOCATION = "/tmp/raidrs";
  public static final String RAIDRS_TMP_LOCATION_KEY = "fs.raidrs.tmpdir";
  public static final String DEFAULT_RAIDRS_HAR_TMP_LOCATION = "/tmp/raidrs_har";
  public static final String RAIDRS_HAR_TMP_LOCATION_KEY = "fs.raidrs.hartmpdir";

  public static final String HAR_SUFFIX = "_raid.har";
  public static final Pattern PARITY_HAR_PARTFILE_PATTERN =
    Pattern.compile(".*" + HAR_SUFFIX + "/part-.*");

  public static final String RAIDNODE_CLASSNAME_KEY = "raid.classname";

  /** RPC server */
  private Server server;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** only used for testing purposes  */
  protected boolean stopRequested = false;

  /** Configuration Manager */
  private ConfigManager configMgr;

  /** hadoop configuration */
  protected Configuration conf;

  protected boolean initialized;  // Are we initialized?
  protected volatile boolean running; // Are we running?

  /** Deamon thread to trigger policies */
  Daemon triggerThread = null;

  /** Deamon thread to delete obsolete parity files */
  PurgeMonitor purgeMonitor = null;
  Daemon purgeThread = null;
  
  /** Deamon thread to har raid directories */
  Daemon harThread = null;

  /** Daemon thread to fix corrupt files */
  BlockFixer blockFixer = null;
  Daemon blockFixerThread = null;

  // statistics about RAW hdfs blocks. This counts all replicas of a block.
  public static class Statistics {
    long numProcessedBlocks; // total blocks encountered in namespace
    long processedSize;   // disk space occupied by all blocks
    long remainingSize;      // total disk space post RAID
    
    long numMetaBlocks;      // total blocks in metafile
    long metaSize;           // total disk space for meta files

    public void clear() {
      numProcessedBlocks = 0;
      processedSize = 0;
      remainingSize = 0;
      numMetaBlocks = 0;
      metaSize = 0;
    }
    public String toString() {
      long save = processedSize - (remainingSize + metaSize);
      long savep = 0;
      if (processedSize > 0) {
        savep = (save * 100)/processedSize;
      }
      String msg = " numProcessedBlocks = " + numProcessedBlocks +
                   " processedSize = " + processedSize +
                   " postRaidSize = " + remainingSize +
                   " numMetaBlocks = " + numMetaBlocks +
                   " metaSize = " + metaSize +
                   " %save in raw disk space = " + savep;
      return msg;
    }
  }

  // Startup options
  static public enum StartupOption{
    TEST ("-test"),
    REGULAR ("-regular");

    private String name = null;
    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
  }
  
  /**
   * Start RaidNode.
   * <p>
   * The raid-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal raid node startup</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>fs.raidnode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the RaidNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */

  RaidNode(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      this.stop();
      throw e;
    } catch (Exception e) {
      this.stop();
      throw new IOException(e);
    }
  }

  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(RaidProtocol.class.getName())) {
      return RaidProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      if (server != null) server.join();
      if (triggerThread != null) triggerThread.join();
      if (blockFixerThread != null) blockFixerThread.join();
      if (purgeThread != null) purgeThread.join();
    } catch (InterruptedException ie) {
      // do nothing
    }
  }
  
  /**
   * Stop all RaidNode threads and wait for all to finish.
   */
  public void stop() {
    if (stopRequested) {
      return;
    }
    stopRequested = true;
    running = false;
    if (server != null) server.stop();
    if (triggerThread != null) triggerThread.interrupt();
    if (blockFixer != null) blockFixer.running = false;
    if (blockFixerThread != null) blockFixerThread.interrupt();
    if (purgeThread != null) purgeThread.interrupt();
  }

  private static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String nodeport = conf.get("raid.server.address");
    if (nodeport == null) {
      nodeport = "localhost:" + DEFAULT_PORT;
    }
    return getAddress(nodeport);
  }

  public InetSocketAddress getListenerAddress() {
    return server.getListenerAddress();
  }
  
  private void initialize(Configuration conf) 
    throws IOException, SAXException, InterruptedException, RaidConfigurationException,
           ClassNotFoundException, ParserConfigurationException {
    this.conf = conf;
    InetSocketAddress socAddr = RaidNode.getAddress(conf);
    int handlerCount = conf.getInt("fs.raidnode.handler.count", 10);

    // read in the configuration
    configMgr = new ConfigManager(conf);

    // create rpc server 
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf);

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.serverAddress = this.server.getListenerAddress();
    LOG.info("RaidNode up at: " + this.serverAddress);

    initialized = true;
    running = true;
    this.server.start(); // start RPC server

    this.blockFixer = new BlockFixer(conf);
    this.blockFixerThread = new Daemon(this.blockFixer);
    this.blockFixerThread.start();

    // start the deamon thread to fire polcies appropriately
    this.triggerThread = new Daemon(new TriggerMonitor());
    this.triggerThread.start();

    // start the thread that deletes obsolete parity files
    this.purgeMonitor = new PurgeMonitor();
    this.purgeThread = new Daemon(purgeMonitor);
    this.purgeThread.start();

    // start the thread that creates HAR files
    this.harThread = new Daemon(new HarMonitor());
    this.harThread.start();
  }

  
  /**
   * Implement RaidProtocol methods
   */

  /** {@inheritDoc} */
  public PolicyList[] getAllPolicies() throws IOException {
    Collection<PolicyList> list = configMgr.getAllPolicies();
    return list.toArray(new PolicyList[list.size()]);
  }

  /** {@inheritDoc} */
  public String recoverFile(String inStr, long corruptOffset) throws IOException {

    LOG.info("Recover File for " + inStr + " for corrupt offset " + corruptOffset);
    Path inputPath = new Path(inStr);
    Path srcPath = inputPath.makeQualified(inputPath.getFileSystem(conf));
    // find stripe length from config
    int stripeLength = getStripeLength(conf);

    // first try decode using XOR code
    Path destPref = xorDestinationPath(conf);
    Decoder decoder = new XORDecoder(conf, RaidNode.getStripeLength(conf));
    Path unraided = unRaid(conf, srcPath, destPref, decoder,
        stripeLength, corruptOffset);
    if (unraided != null) {
      return unraided.toString();
    }

    // try decode using ReedSolomon code
    destPref = rsDestinationPath(conf);
    decoder = new ReedSolomonDecoder(conf, RaidNode.getStripeLength(conf),
        RaidNode.rsParityLength(conf));
    unraided = unRaid(conf, srcPath, destPref, decoder,
        stripeLength, corruptOffset);
    if (unraided != null) {
      return unraided.toString();
    }
    return null;
  }

  /**
   * returns the number of raid jobs running for a particular policy
   */
  abstract int getRunningJobsForPolicy(String policyName);

  /**
   * Periodically checks to see which policies should be fired.
   */
  class TriggerMonitor implements Runnable {
    class ScanState {
      long fullScanStartTime;
      DirectoryTraversal pendingTraversal;
      RaidFilter.Statistics stats;
      ScanState() {
        fullScanStartTime = 0;
        pendingTraversal = null;
        stats = new RaidFilter.Statistics();
      }
    }

    private Map<String, ScanState> scanStateMap =
      new HashMap<String, ScanState>();

    /**
     */
    public void run() {
      while (running) {
        try {
          doProcess();
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        } finally {
          LOG.info("Trigger thread continuing to run...");
        }
      }
    }

    /**
     * Should we select more files for a policy.
     */
    private boolean shouldSelectFiles(PolicyInfo info) {
      String policyName = info.getName();
      ScanState scanState = scanStateMap.get(policyName);
      int runningJobsCount = getRunningJobsForPolicy(policyName);
      // Is there a scan in progress for this policy?
      if (scanState.pendingTraversal != null) {
        int maxJobsPerPolicy = configMgr.getMaxJobsPerPolicy();

        // If there is a scan in progress for this policy, we can have
        // upto maxJobsPerPolicy running jobs.
        return (runningJobsCount < maxJobsPerPolicy);
      } else {
        // If there isn't a scan in progress for this policy, we don't
        // want to start a fresh scan if there is even one running job.
        if (runningJobsCount >= 1) {
          return false;
        }
        // Check the time of the last full traversal before starting a fresh
        // traversal.
        long lastScan = scanState.fullScanStartTime;
        return (now() > lastScan + configMgr.getPeriodicity());
      }
    }

   /**
    * Returns a list of pathnames that needs raiding.
    * The list of paths could be obtained by resuming a previously suspended
    * traversal.
    * The number of paths returned is limited by raid.distraid.max.jobs.
    */
    private List<FileStatus> selectFiles(
      PolicyInfo info, ArrayList<PolicyInfo> allPolicies) throws IOException {
      Path destPrefix = getDestinationPath(info.getErasureCode(), conf);
      String policyName = info.getName();
      Path srcPath = info.getSrcPath();
      long modTimePeriod = Long.parseLong(info.getProperty("modTimePeriod"));

      // Max number of files returned.
      int selectLimit = configMgr.getMaxFilesPerJob();
      int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));

      long selectStartTime = System.currentTimeMillis();

      ScanState scanState = scanStateMap.get(policyName);
      // If we have a pending traversal, resume it.
      if (scanState.pendingTraversal != null) {
        DirectoryTraversal dt = scanState.pendingTraversal;
        LOG.info("Resuming traversal for policy " + policyName);
        DirectoryTraversal.FileFilter filter =
          filterForPolicy(selectStartTime, info, allPolicies, scanState.stats);
        List<FileStatus> returnSet = dt.getFilteredFiles(filter, selectLimit);
        if (dt.doneTraversal()) {
          scanState.pendingTraversal = null;
        }
        return returnSet;
      }

      // Expand destination prefix path.
      String destpstr = destPrefix.toString();
      if (!destpstr.endsWith(Path.SEPARATOR)) {
        destpstr += Path.SEPARATOR;
      }

      List<FileStatus> returnSet = new LinkedList<FileStatus>();

      FileSystem fs = srcPath.getFileSystem(conf);
      FileStatus[] gpaths = fs.globStatus(srcPath);
      if (gpaths != null) {
        List<FileStatus> selectedPaths = new LinkedList<FileStatus>();
        for (FileStatus onepath: gpaths) {
          String pathstr = onepath.getPath().makeQualified(fs).toString();
          if (!pathstr.endsWith(Path.SEPARATOR)) {
            pathstr += Path.SEPARATOR;
          }
          if (pathstr.startsWith(destpstr) || destpstr.startsWith(pathstr)) {
            LOG.info("Skipping source " + pathstr +
                     " because it conflicts with raid directory " + destpstr);
          } else {
            selectedPaths.add(onepath);
          }
        }

        // Set the time for a new traversal.
        scanState.fullScanStartTime = now();
        DirectoryTraversal dt = new DirectoryTraversal(fs, selectedPaths,
          conf.getInt("raid.directorytraversal.threads", 4));
        DirectoryTraversal.FileFilter filter =
          filterForPolicy(selectStartTime, info, allPolicies, scanState.stats);
        returnSet = dt.getFilteredFiles(filter, selectLimit);
        if (!dt.doneTraversal()) {
          scanState.pendingTraversal = dt;
        }
      }
      return returnSet;
    }

    /**
     * Keep processing policies.
     * If the config file has changed, then reload config file and start afresh.
     */
    private void doProcess() throws IOException, InterruptedException {
      ArrayList<PolicyInfo> allPolicies = new ArrayList<PolicyInfo>();
      for (PolicyList category : configMgr.getAllPolicies()) {
        for (PolicyInfo info: category.getAll()) {
          allPolicies.add(info);
        }
      }
      while (running) {
        Thread.sleep(SLEEP_TIME);

        boolean reloaded = configMgr.reloadConfigsIfNecessary();
        if (reloaded) {
          allPolicies.clear();
          for (PolicyList category : configMgr.getAllPolicies()) {
            for (PolicyInfo info: category.getAll()) {
              allPolicies.add(info);
            }
          }
        }
        for (PolicyInfo info: allPolicies) {
          if (!scanStateMap.containsKey(info.getName())) {
            scanStateMap.put(info.getName(), new ScanState());
          }

          if (!shouldSelectFiles(info)) {
            continue;
          }

          LOG.info("Triggering Policy Filter " + info.getName() +
                   " " + info.getSrcPath());
          List<FileStatus> filteredPaths = null;
          try {
            filteredPaths = selectFiles(info, allPolicies);
          } catch (Exception e) {
            LOG.info("Exception while invoking filter on policy " + info.getName() +
                     " srcPath " + info.getSrcPath() + 
                     " exception " + StringUtils.stringifyException(e));
            continue;
          }

          if (filteredPaths == null || filteredPaths.size() == 0) {
            LOG.info("No filtered paths for policy " + info.getName());
             continue;
          }

          // Apply the action on accepted paths
          LOG.info("Triggering Policy Action " + info.getName() +
                   " " + info.getSrcPath());
          try {
            raidFiles(info, filteredPaths);
          } catch (Exception e) {
            LOG.info("Exception while invoking action on policy " + info.getName() +
                     " srcPath " + info.getSrcPath() + 
                     " exception " + StringUtils.stringifyException(e));
            continue;
          }
        }
      }
    }

    DirectoryTraversal.FileFilter filterForPolicy(
      long startTime, PolicyInfo info, List<PolicyInfo> allPolicies,
      RaidFilter.Statistics stats)
      throws IOException {
      switch (info.getErasureCode()) {
      case XOR:
        // Return a preference-based filter that prefers RS parity files
        // over XOR parity files.
        return new RaidFilter.PreferenceFilter(
          conf, rsDestinationPath(conf), xorDestinationPath(conf),
          info, allPolicies, startTime, stats);
      case RS:
        return new RaidFilter.TimeBasedFilter(conf, rsDestinationPath(conf),
          info, allPolicies, startTime, stats);
      default:
        return null;
      }
    }
  }

  /**
   * raid a list of files, this will be overridden by subclasses of RaidNode
   */
  abstract void raidFiles(PolicyInfo info, List<FileStatus> paths) 
    throws IOException;

  static private Path getOriginalParityFile(Path destPathPrefix, Path srcPath) {
    return new Path(destPathPrefix, makeRelative(srcPath));
  }
  
  static class ParityFilePair {
    private Path path;
    private FileSystem fs;
    
    public ParityFilePair( Path path, FileSystem fs) {
      this.path = path;
      this.fs = fs;
    }
    
    public Path getPath() {
      return this.path;
    }
    
    public FileSystem getFileSystem() {
      return this.fs;
    }
    
  }
  
  
  /**
   * Returns the Path to the parity file of a given file
   * 
   * @param destPathPrefix Destination prefix defined by some policy
   * @param srcPath Path to the original source file
   * @param create Boolean value telling whether a new parity file should be created
   * @return Path object representing the parity file of the source
   * @throws IOException
   */
  static ParityFilePair getParityFile(Path destPathPrefix, Path srcPath, Configuration conf) throws IOException {
    Path srcParent = srcPath.getParent();

    FileSystem fsDest = destPathPrefix.getFileSystem(conf);
    FileSystem fsSrc = srcPath.getFileSystem(conf);
    
    FileStatus srcStatus = null;
    try {
      srcStatus = fsSrc.getFileStatus(srcPath);
    } catch (java.io.FileNotFoundException e) {
      return null;
    }
    
    Path outDir = destPathPrefix;
    if (srcParent != null) {
      if (srcParent.getParent() == null) {
        outDir = destPathPrefix;
      } else {
        outDir = new Path(destPathPrefix, makeRelative(srcParent));
      }
    }

    
    //CASE 1: CHECK HAR - Must be checked first because har is created after
    // parity file and returning the parity file could result in error while
    // reading it.
    Path outPath =  getOriginalParityFile(destPathPrefix, srcPath);
    String harDirName = srcParent.getName() + HAR_SUFFIX; 
    Path HarPath = new Path(outDir,harDirName);
    if (fsDest.exists(HarPath)) {  
      URI HarPathUri = HarPath.toUri();
      Path inHarPath = new Path("har://",HarPathUri.getPath()+"/"+outPath.toUri().getPath());
      FileSystem fsHar = new HarFileSystem(fsDest);
      fsHar.initialize(inHarPath.toUri(), conf);
      if (fsHar.exists(inHarPath)) {
        FileStatus inHar = fsHar.getFileStatus(inHarPath);
        if (inHar.getModificationTime() == srcStatus.getModificationTime()) {
          return new ParityFilePair(inHarPath,fsHar);
        }
      }
    }
    
    //CASE 2: CHECK PARITY
    try {
      FileStatus outHar = fsDest.getFileStatus(outPath);
      if (outHar.getModificationTime() == srcStatus.getModificationTime()) {
        return new ParityFilePair(outPath,fsDest);
      }
    } catch (java.io.FileNotFoundException e) {
    }

    return null; // NULL if no parity file
  }

  static ParityFilePair xorParityForSource(Path srcPath, Configuration conf)
      throws IOException {
    try {
      Path destPath = xorDestinationPath(conf);
      return getParityFile(destPath, srcPath, conf);
    } catch (FileNotFoundException e) {
    }
    return null;
  }

  static ParityFilePair rsParityForSource(Path srcPath, Configuration conf)
      throws IOException {
    try {
      Path destPath = rsDestinationPath(conf);
      return getParityFile(destPath, srcPath, conf);
    } catch (FileNotFoundException e) {
    }
    return null;
  }

  private ParityFilePair getParityFile(Path destPathPrefix, Path srcPath)
      throws IOException {
    return getParityFile(destPathPrefix, srcPath, conf);
  }

  /**
   * RAID a list of files.
   */
  void doRaid(Configuration conf, PolicyInfo info, List<FileStatus> paths)
      throws IOException {
    int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
    int metaRepl = Integer.parseInt(info.getProperty("metaReplication"));
    int stripeLength = getStripeLength(conf);
    Path destPref = getDestinationPath(info.getErasureCode(), conf);
    String simulate = info.getProperty("simulate");
    boolean doSimulate = simulate == null ? false : Boolean
        .parseBoolean(simulate);

    Statistics statistics = new Statistics();
    int count = 0;

    for (FileStatus s : paths) {
      doRaid(conf, s, destPref, info.getErasureCode(), statistics,
        new RaidUtils.DummyProgressable(), doSimulate, targetRepl, metaRepl,
        stripeLength);
      if (count % 1000 == 0) {
        LOG.info("RAID statistics " + statistics.toString());
      }
      count++;
    }
    LOG.info("RAID statistics " + statistics.toString());
  }

  
  /**
   * RAID an individual file
   */

  static public void doRaid(Configuration conf, PolicyInfo info,
      FileStatus src, Statistics statistics, Progressable reporter)
      throws IOException {
    int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
    int metaRepl = Integer.parseInt(info.getProperty("metaReplication"));
    int stripeLength = getStripeLength(conf);

    Path destPref = getDestinationPath(info.getErasureCode(), conf);
    String simulate = info.getProperty("simulate");
    boolean doSimulate = simulate == null ? false : Boolean
        .parseBoolean(simulate);

    doRaid(conf, src, destPref, info.getErasureCode(), statistics,
                  reporter, doSimulate, targetRepl, metaRepl, stripeLength);
  }

  /**
   * RAID an individual file
   */
  static private void doRaid(Configuration conf, FileStatus stat, Path destPath,
                      PolicyInfo.ErasureCodeType code, Statistics statistics,
                      Progressable reporter, boolean doSimulate,
                      int targetRepl, int metaRepl, int stripeLength)
    throws IOException {
    Path p = stat.getPath();
    FileSystem srcFs = p.getFileSystem(conf);

    // extract block locations from File system
    BlockLocation[] locations = srcFs.getFileBlockLocations(stat, 0, stat.getLen());
    
    // if the file has fewer than 2 blocks, then nothing to do
    if (locations.length <= 2) {
      return;
    }

    // add up the raw disk space occupied by this file
    long diskSpace = 0;
    for (BlockLocation l: locations) {
      diskSpace += (l.getLength() * stat.getReplication());
    }
    statistics.numProcessedBlocks += locations.length;
    statistics.processedSize += diskSpace;

    // generate parity file
    generateParityFile(conf, stat, reporter, srcFs, destPath, code,
        locations, metaRepl, stripeLength);

    // reduce the replication factor of the source file
    if (!doSimulate) {
      if (srcFs.setReplication(p, (short)targetRepl) == false) {
        LOG.info("Error in reducing relication factor of file " + p + " to " + targetRepl);
        statistics.remainingSize += diskSpace;  // no change in disk space usage
        return;
      }
    }

    diskSpace = 0;
    for (BlockLocation l: locations) {
      diskSpace += (l.getLength() * targetRepl);
    }
    statistics.remainingSize += diskSpace;

    // the metafile will have this many number of blocks
    int numMeta = locations.length / stripeLength;
    if (locations.length % stripeLength != 0) {
      numMeta++;
    }

    // we create numMeta for every file. This metablock has metaRepl # replicas.
    // the last block of the metafile might not be completely filled up, but we
    // ignore that for now.
    statistics.numMetaBlocks += (numMeta * metaRepl);
    statistics.metaSize += (numMeta * metaRepl * stat.getBlockSize());
  }

  /**
   * Create the parity file.
   */
  static private void generateParityFile(Configuration conf, FileStatus stat,
                                  Progressable reporter,
                                  FileSystem inFs,
                                  Path destPathPrefix,
                                  ErasureCodeType code,
                                  BlockLocation[] locations,
                                  int metaRepl, int stripeLength) throws IOException {

    Path inpath = stat.getPath();
    Path outpath =  getOriginalParityFile(destPathPrefix, inpath);
    FileSystem outFs = outpath.getFileSystem(conf);

    // If the parity file is already upto-date, then nothing to do
    try {
      FileStatus stmp = outFs.getFileStatus(outpath);
      if (stmp.getModificationTime() == stat.getModificationTime()) {
        LOG.info("Parity file for " + inpath + "(" + locations.length +
              ") is " + outpath + " already upto-date. Nothing more to do.");
        return;
      }
    } catch (IOException e) {
      // ignore errors because the raid file might not exist yet.
    }

    Encoder encoder = encoderForCode(conf, code);
    encoder.encodeFile(inFs, inpath, outFs, outpath, (short)metaRepl, reporter);

    // set the modification time of the RAID file. This is done so that the modTime of the
    // RAID file reflects that contents of the source file that it has RAIDed. This should
    // also work for files that are being appended to. This is necessary because the time on
    // on the destination namenode may not be synchronised with the timestamp of the 
    // source namenode.
    outFs.setTimes(outpath, stat.getModificationTime(), -1);
    inFs.setTimes(inpath, stat.getModificationTime(), stat.getAccessTime());

    FileStatus outstat = outFs.getFileStatus(outpath);
    FileStatus inStat = inFs.getFileStatus(inpath);
    LOG.info("Source file " + inpath + " of size " + inStat.getLen() +
             " Parity file " + outpath + " of size " + outstat.getLen() +
             " src mtime " + stat.getModificationTime()  +
             " parity mtime " + outstat.getModificationTime());
  }

  /**
   * Extract a good block from the parity block. This assumes that the
   * corruption is in the main file and the parity file is always good.
   */
  public static Path unRaid(Configuration conf, Path srcPath,
      Path destPathPrefix, Decoder decoder, int stripeLength,
      long corruptOffset) throws IOException {

    // Test if parity file exists
    ParityFilePair ppair = getParityFile(destPathPrefix, srcPath, conf);
    if (ppair == null) {
      return null;
    }

    final Path recoveryDestination = new Path(
      RaidNode.unraidTmpDirectory(conf));
    FileSystem destFs = recoveryDestination.getFileSystem(conf);
    final Path recoveredPrefix = 
      destFs.makeQualified(new Path(recoveryDestination, makeRelative(srcPath)));
    final Path recoveredPath = 
      new Path(recoveredPrefix + "." + new Random().nextLong() + ".recovered");
    LOG.info("Creating recovered file " + recoveredPath);

    FileSystem srcFs = srcPath.getFileSystem(conf);
    decoder.decodeFile(srcFs, srcPath, ppair.getFileSystem(),
        ppair.getPath(), corruptOffset, recoveredPath);

    return recoveredPath;
  }

  /**
   * Periodically delete orphaned parity files.
   */
  class PurgeMonitor implements Runnable {
    /**
     */
    public void run() {
      while (running) {
        try {
          doPurge();
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        } finally {
          LOG.info("Purge parity files thread continuing to run...");
        }
      }
    }

    /**
     * Traverse the parity destination directory, removing directories that
     * no longer existing in the source.
     * @throws IOException
     */
    private void purgeDirectories(FileSystem fs, Path root) throws IOException {
      String prefix = root.toUri().getPath();
      List<FileStatus> startPaths = new LinkedList<FileStatus>();
      try {
        startPaths.add(fs.getFileStatus(root));
      } catch (FileNotFoundException e) {
        return;
      }
      DirectoryTraversal dt = new DirectoryTraversal(fs, startPaths);
      FileStatus dir = dt.getNextDirectory();
      for (; dir != null; dir = dt.getNextDirectory()) {
        Path dirPath = dir.getPath();
        if (dirPath.toUri().getPath().endsWith(HAR_SUFFIX)) {
          continue;
        }
        String dirStr = dirPath.toUri().getPath();
        if (!dirStr.startsWith(prefix)) {
          continue;
        }
        String src = dirStr.replaceFirst(prefix, "");
        if (src.length() == 0) continue;
        Path srcPath = new Path(src);
        if (!fs.exists(srcPath)) {
          LOG.info("Purging directory " + dirPath);
          boolean done = fs.delete(dirPath, true);
          if (!done) {
            LOG.error("Could not purge " + dirPath);
          }
        }
      }
    }

    /**
     * Delete orphaned files. The reason this is done by a separate thread 
     * is to not burden the TriggerMonitor with scanning the 
     * destination directories.
     */
    private void doPurge() throws IOException, InterruptedException {
      long prevExec = 0;
      while (running) {

        // The config may be reloaded by the TriggerMonitor. 
        // This thread uses whatever config is currently active.
        while(now() < prevExec + configMgr.getPeriodicity()){
          Thread.sleep(SLEEP_TIME);
        }

        LOG.info("Started purge scan");
        prevExec = now();

        // expand destination prefix path
        Path destPref = xorDestinationPath(conf);
        FileSystem destFs = destPref.getFileSystem(conf);
        purgeDirectories(destFs, destPref);

        destPref = rsDestinationPath(conf);
        destFs = destPref.getFileSystem(conf);
        purgeDirectories(destFs, destPref);

        // fetch all categories
        for (PolicyList category : configMgr.getAllPolicies()) {
          for (PolicyInfo info: category.getAll()) {

            try {
              // expand destination prefix path
              destPref = getDestinationPath(info.getErasureCode(), conf);
              destFs = destPref.getFileSystem(conf);

              //get srcPaths
              Path[] srcPaths = info.getSrcPathExpanded();

              if (srcPaths != null) {
                for (Path srcPath: srcPaths) {
                  // expand destination prefix
                  Path destPath = getOriginalParityFile(destPref, srcPath);

                  FileSystem srcFs = info.getSrcPath().getFileSystem(conf);
                  FileStatus stat = null;
                  try {
                    stat = destFs.getFileStatus(destPath);
                  } catch (FileNotFoundException e) {
                    // do nothing, leave stat = null;
                  }
                  if (stat != null) {
                    LOG.info("Purging obsolete parity files for policy " + 
                              info.getName() + " " + destPath);
                    recursePurge(info.getErasureCode(), srcFs, destFs,
                              destPref.toUri().getPath(), stat);
                  }

                }
              }
            } catch (Exception e) {
              LOG.warn("Ignoring Exception while processing policy " + 
                       info.getName() + " " + 
                       StringUtils.stringifyException(e));
            }
          }
        }
      }
    }

    /**
     * The destPrefix is the absolute pathname of the destinationPath
     * specified in the policy (without the host:port)
     */ 
    void recursePurge(ErasureCodeType code,
                              FileSystem srcFs, FileSystem destFs,
                              String destPrefix, FileStatus dest) 
      throws IOException {

      Path destPath = dest.getPath(); // pathname, no host:port
      String destStr = destPath.toUri().getPath();
      LOG.debug("Checking " + destPath + " prefix " + destPrefix);

      // Verify if it is a har file
      if (dest.isDirectory() && destStr.endsWith(HAR_SUFFIX)) {
        try {
          int harUsedPercent =
            usefulHar(code, srcFs, destFs, destPath, destPrefix, conf);
          LOG.info("Useful percentage of " + destStr + " " + harUsedPercent);
          // Delete the har if its usefulness reaches a threshold.
          if (harUsedPercent <= conf.getInt("raid.har.usage.threshold", 0)) {
            LOG.info("Purging " + destStr + " at usage " + harUsedPercent);
            boolean done = destFs.delete(destPath, true);
            if (!done) {
              LOG.error("Could not purge " + destPath);
            }
          }
        } catch (IOException e) {
          LOG.warn("Error during purging " + destStr + " " +
              StringUtils.stringifyException(e));
        }
        return;
      }
      
      // Verify the destPrefix is a prefix of the destPath
      if (!destStr.startsWith(destPrefix)) {
        LOG.error("Destination path " + destStr + " should have " + 
                  destPrefix + " as its prefix.");
        return;
      }
      
      if (dest.isDirectory()) {
        FileStatus[] files = null;
        files = destFs.listStatus(destPath);
        if (files == null || files.length == 0){
          boolean done = destFs.delete(destPath,true); // ideal is false, but
                  // DFSClient only deletes directories if it is recursive
          if (done) {
            LOG.info("Purged directory " + destPath );
          }
          else {
            LOG.info("Unable to purge directory " + destPath);
          }
        }
        if (files != null) {
          for (FileStatus one:files) {
            recursePurge(code, srcFs, destFs, destPrefix, one);
          }
        }
        // If the directory is empty now, it will be purged the next time this
        // thread runs.
        return; // the code below does the file checking
      }
      
      String src = destStr.replaceFirst(destPrefix, "");
      
      Path srcPath = new Path(src);
      boolean shouldDelete = false;

      if (!srcFs.exists(srcPath)) {
        shouldDelete = true;
      } else {
        try {
          // If there is a RS parity file, the XOR parity can be deleted.
          if (code == ErasureCodeType.XOR) {
            ParityFilePair ppair = getParityFile(
               getDestinationPath(ErasureCodeType.RS, conf), srcPath, conf);
            if (ppair != null) {
              shouldDelete = true;
            }
          }
          if (!shouldDelete) {
            Path dstPath = (new Path(destPrefix.trim())).makeQualified(destFs);
            ParityFilePair ppair = getParityFile(dstPath,srcPath);
            // If the parity file is not the appropriate one for the source or
            // the parityFs is not the same as this file's filesystem
            // (it is a HAR), this file can be deleted.
            if ( ppair == null ||
                 !destFs.equals(ppair.getFileSystem()) ||
                 !destPath.equals(ppair.getPath())) {
              shouldDelete = true;
            }
          }
        } catch (IOException e) {
          LOG.warn("Error during purging " + src + " " +
                   StringUtils.stringifyException(e));
        }
      }

      if (shouldDelete) {
        boolean done = destFs.delete(destPath, false);
        if (done) {
          LOG.info("Purged file " + destPath );
        } else {
          LOG.info("Unable to purge file " + destPath );
        }
      }
    } 
  }

  //
  // Returns the number of up-to-date files in the har as a percentage of the
  // total number of files in the har.
  //
  protected static int usefulHar(
    ErasureCodeType code,
    FileSystem srcFs, FileSystem destFs,
    Path harPath, String destPrefix, Configuration conf) throws IOException {

    FileSystem fsHar = new HarFileSystem(destFs);
    String harURIPath = harPath.toUri().getPath();
    Path qualifiedPath = new Path("har://", harURIPath +
      Path.SEPARATOR + harPath.getParent().toUri().getPath());
    fsHar.initialize(qualifiedPath.toUri(), conf);
    FileStatus[] filesInHar = fsHar.listStatus(qualifiedPath);
    if (filesInHar.length == 0) {
      return 0;
    }
    int numUseless = 0;
    for (FileStatus one: filesInHar) {
      Path parityPath = one.getPath();
      String parityStr = parityPath.toUri().getPath();
      if (parityStr.startsWith("har:/")) {
        LOG.error("Unexpected prefix har:/ for " + parityStr);
        continue;
      }
      String prefixToReplace = harURIPath + destPrefix;
      if (!parityStr.startsWith(prefixToReplace)) {
        continue;
      }
      String src = parityStr.substring(prefixToReplace.length());
      if (code == ErasureCodeType.XOR) {
        ParityFilePair ppair = getParityFile(
          getDestinationPath(ErasureCodeType.RS, conf), new Path(src), conf);
        if (ppair != null) {
          // There is a valid RS parity file, so the XOR one is useless.
          numUseless++;
          continue;
        }
      }
      try {
        FileStatus srcStatus = srcFs.getFileStatus(new Path(src));
        if (srcStatus == null) {
          numUseless++;
        } else if (one.getModificationTime() !=
                  srcStatus.getModificationTime()) {
          numUseless++;
        }
      } catch (FileNotFoundException e) {
        LOG.info("File not found: " + e);
        numUseless++;
      }
    }
    int uselessPercent = numUseless * 100 / filesInHar.length;
    return 100 - uselessPercent;
  }

  private void doHar() throws IOException, InterruptedException {
    long prevExec = 0;
    while (running) {

      // The config may be reloaded by the TriggerMonitor. 
      // This thread uses whatever config is currently active.
      while(now() < prevExec + configMgr.getPeriodicity()){
        Thread.sleep(SLEEP_TIME);
      }

      LOG.info("Started archive scan");
      prevExec = now();

      // fetch all categories
      for (PolicyList category : configMgr.getAllPolicies()) {
        for (PolicyInfo info: category.getAll()) {
          String tmpHarPath = tmpHarPathForCode(conf, info.getErasureCode());
          String str = info.getProperty("time_before_har");
          if (str != null) {
            try {
              long cutoff = now() - ( Long.parseLong(str) * 24L * 3600000L );

              Path destPref = getDestinationPath(info.getErasureCode(), conf);
              FileSystem destFs = destPref.getFileSystem(conf); 

              //get srcPaths
              Path[] srcPaths = info.getSrcPathExpanded();
              
              if ( srcPaths != null ){
                for (Path srcPath: srcPaths) {
                  // expand destination prefix
                  Path destPath = getOriginalParityFile(destPref, srcPath);

                  FileStatus stat = null;
                  try {
                    stat = destFs.getFileStatus(destPath);
                  } catch (FileNotFoundException e) {
                    // do nothing, leave stat = null;
                  }
                  if (stat != null) {
                    LOG.info("Haring parity files for policy " + 
                        info.getName() + " " + destPath);
                    recurseHar(info, destFs, stat, destPref.toUri().getPath(),
                        srcPath.getFileSystem(conf), cutoff, tmpHarPath);
                  }
                }
              }
            } catch (Exception e) {
              LOG.warn("Ignoring Exception while processing policy " + 
                  info.getName() + " " + 
                  StringUtils.stringifyException(e));
            }
          }
        }
      }
    }
    return;
  }
  
  void recurseHar(PolicyInfo info, FileSystem destFs, FileStatus dest,
    String destPrefix, FileSystem srcFs, long cutoff, String tmpHarPath)
    throws IOException {

    if (dest.isFile()) {
      return;
    }
    
    Path destPath = dest.getPath(); // pathname, no host:port
    String destStr = destPath.toUri().getPath();

    // Verify if it already contains a HAR directory
    if ( destFs.exists(new Path(destPath, destPath.getName()+HAR_SUFFIX)) ) {
      return;
    }

    FileStatus[] files = null;
    files = destFs.listStatus(destPath);
    boolean shouldHar = false;
    if (files != null) {
      shouldHar = files.length > 0;
      for (FileStatus one: files) {
        if (one.isDirectory()){
          recurseHar(info, destFs, one, destPrefix, srcFs, cutoff, tmpHarPath);
          shouldHar = false;
        } else if (one.getModificationTime() > cutoff ) {
          if (shouldHar) {
            LOG.info("Cannot archive " + destPath + 
                   " because " + one.getPath() + " was modified after cutoff");
            shouldHar = false;
          }
        }
      }

      if (shouldHar) {
        String src = destStr.replaceFirst(destPrefix, "");
        Path srcPath = new Path(src);
        FileStatus[] statuses = srcFs.listStatus(srcPath);
        Path destPathPrefix = new Path(destPrefix).makeQualified(destFs);
        if (statuses != null) {
          for (FileStatus status : statuses) {
            if (getParityFile(destPathPrefix, 
                              status.getPath().makeQualified(srcFs)) == null ) {
              LOG.info("Cannot archive " + destPath + 
                  " because it doesn't contain parity file for " +
                  status.getPath().makeQualified(srcFs) + " on destination " +
                  destPathPrefix);
              shouldHar = false;
              break;
            }
          }
        }
      }
    }

    if ( shouldHar ) {
      LOG.info("Archiving " + dest.getPath() + " to " + tmpHarPath );
      singleHar(info, destFs, dest, tmpHarPath);
    }
  } 

  
  private void singleHar(PolicyInfo info, FileSystem destFs, FileStatus dest,
    String tmpHarPath) throws IOException {

    Random rand = new Random();
    Path root = new Path("/");
    Path qualifiedPath = dest.getPath().makeQualified(destFs);
    String harFileDst = qualifiedPath.getName() + HAR_SUFFIX;
    String harFileSrc = qualifiedPath.getName() + "-" + 
                                rand.nextLong() + "-" + HAR_SUFFIX;
    short metaReplication =
      (short) Integer.parseInt(info.getProperty("metaReplication"));
    // HadoopArchives.HAR_PARTFILE_LABEL is private, so hard-coding the label.
    conf.setLong("har.partfile.size", configMgr.getHarPartfileSize());
    HadoopArchives har = new HadoopArchives(conf);
    String[] args = new String[7];
    args[0] = "-Ddfs.replication=" + metaReplication;
    args[1] = "-archiveName";
    args[2] = harFileSrc;
    args[3] = "-p"; 
    args[4] = root.makeQualified(destFs).toString();
    args[5] = qualifiedPath.toUri().getPath().substring(1);
    args[6] = tmpHarPath.toString();
    int ret = 0;
    try {
      ret = ToolRunner.run(har, args);
      if (ret == 0 && !destFs.rename(new Path(tmpHarPath+"/"+harFileSrc), 
                                     new Path(qualifiedPath, harFileDst))) {
        LOG.info("HAR rename didn't succeed from " + tmpHarPath+"/"+harFileSrc +
            " to " + qualifiedPath + "/" + harFileDst);
        ret = -2;
      }
    } catch (Exception exc) {
      throw new IOException("Error while creating archive " + ret, exc);
    }
    
    if (ret != 0){
      throw new IOException("Error while creating archive " + ret);
    }
    return;
  }
  
  /**
   * Periodically generates HAR files
   */
  class HarMonitor implements Runnable {

    public void run() {
      while (running) {
        try {
          doHar();
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        } finally {
          LOG.info("Har parity files thread continuing to run...");
        }
      }
      LOG.info("Leaving Har thread.");
    }
    
  }

  /**
   * Return the temp path for XOR parity files
   */
  public static String unraidTmpDirectory(Configuration conf) {
    return conf.get(RAID_TMP_LOCATION_KEY, DEFAULT_RAID_TMP_LOCATION);
  }

  /**
   * Return the temp path for ReedSolomonEncoder parity files
   */
  public static String rsTempPrefix(Configuration conf) {
    return conf.get(RAIDRS_TMP_LOCATION_KEY, DEFAULT_RAIDRS_TMP_LOCATION);
  }

  /**
   * Return the temp path for XOR parity files
   */
  public static String xorHarTempPrefix(Configuration conf) {
    return conf.get(RAID_HAR_TMP_LOCATION_KEY, DEFAULT_RAID_HAR_TMP_LOCATION);
  }

  /**
   * Return the temp path for ReedSolomonEncoder parity files
   */
  public static String rsHarTempPrefix(Configuration conf) {
    return conf.get(RAIDRS_HAR_TMP_LOCATION_KEY,
        DEFAULT_RAIDRS_HAR_TMP_LOCATION);
  }

  /**
   * Return the destination path for ReedSolomon parity files
   */
  public static Path rsDestinationPath(Configuration conf, FileSystem fs) {
    String loc = conf.get(RAIDRS_LOCATION_KEY, DEFAULT_RAIDRS_LOCATION);
    Path p = new Path(loc.trim());
    p = p.makeQualified(fs);
    return p;
  }

  /**
   * Return the destination path for ReedSolomon parity files
   */
  public static Path rsDestinationPath(Configuration conf)
    throws IOException {
    String loc = conf.get(RAIDRS_LOCATION_KEY, DEFAULT_RAIDRS_LOCATION);
    Path p = new Path(loc.trim());
    FileSystem fs = FileSystem.get(p.toUri(), conf);
    p = p.makeQualified(fs);
    return p;
  }

  /**
   * Return the destination path for XOR parity files
   */
  public static Path xorDestinationPath(Configuration conf, FileSystem fs) {
    String loc = conf.get(RAID_LOCATION_KEY, DEFAULT_RAID_LOCATION);
    Path p = new Path(loc.trim());
    p = p.makeQualified(fs);
    return p;
  }

  /**
   * Return the destination path for XOR parity files
   */
  public static Path xorDestinationPath(Configuration conf)
    throws IOException {
    String loc = conf.get(RAID_LOCATION_KEY, DEFAULT_RAID_LOCATION);
    Path p = new Path(loc.trim());
    FileSystem fs = FileSystem.get(p.toUri(), conf);
    p = p.makeQualified(fs);
    return p;
  }

  /**
   * Return the path prefix that stores the parity files
   */
  static Path getDestinationPath(ErasureCodeType code, Configuration conf)
    throws IOException {
    switch (code) {
      case XOR:
        return xorDestinationPath(conf);
      case RS:
        return rsDestinationPath(conf);
      default:
        return null;
    }
  }

  static Encoder encoderForCode(Configuration conf, ErasureCodeType code) {
    int stripeLength = getStripeLength(conf);
    switch (code) {
      case XOR:
        return new XOREncoder(conf, stripeLength);
      case RS:
        return new ReedSolomonEncoder(conf, stripeLength, rsParityLength(conf));
      default:
        return null;
    }
  }

  static String tmpHarPathForCode(Configuration conf, ErasureCodeType code) {
    switch (code) {
      case XOR:
        return xorHarTempPrefix(conf);
      case RS:
        return rsHarTempPrefix(conf);
      default:
        return null;
    }
  }

  /**
   * Obtain stripe length from configuration
   */
  public static int getStripeLength(Configuration conf) {
    return conf.getInt(STRIPE_LENGTH_KEY, DEFAULT_STRIPE_LENGTH);
  }

  /**
   * Obtain stripe length from configuration
   */
  public static int rsParityLength(Configuration conf) {
    return conf.getInt(RS_PARITY_LENGTH_KEY, RS_PARITY_LENGTH_DEFAULT);
  }

  static boolean isParityHarPartFile(Path p) {
    Matcher m = PARITY_HAR_PARTFILE_PATTERN.matcher(p.toUri().getPath());
    return m.matches();
  }

  /**
   * Returns current time.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  /**                       
   * Make an absolute path relative by stripping the leading /
   */   
  static private Path makeRelative(Path path) {
    if (!path.isAbsolute()) {
      return path;
    }          
    String p = path.toUri().getPath();
    String relative = p.substring(1, p.length());
    return new Path(relative);
  } 

  private static void printUsage() {
    System.err.println("Usage: java RaidNode ");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i]; // We have to parse command line args in future.
    }
    return startOpt;
  }


  /**
   * Convert command line options to configuration parameters
   */
  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("fs.raidnode.startup", opt.toString());
  }

  /**
   * Create an instance of the appropriate subclass of RaidNode 
   */
  public static RaidNode createRaidNode(Configuration conf)
    throws ClassNotFoundException {
    try {
      // default to distributed raid node
      Class<?> raidNodeClass =
        conf.getClass(RAIDNODE_CLASSNAME_KEY, DistRaidNode.class);
      if (!RaidNode.class.isAssignableFrom(raidNodeClass)) {
        throw new ClassNotFoundException("not an implementation of RaidNode");
      }
      Constructor<?> constructor =
        raidNodeClass.getConstructor(new Class[] {Configuration.class} );
      return (RaidNode) constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (InstantiationException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (IllegalAccessException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (InvocationTargetException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    }
  }

  /**
   * Create an instance of the RaidNode 
   */
  public static RaidNode createRaidNode(String argv[], Configuration conf)
    throws IOException, ClassNotFoundException {
    if (conf == null) {
      conf = new Configuration();
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);
    RaidNode node = createRaidNode(conf);
    return node;
  }


  /**
   */
  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(RaidNode.class, argv, LOG);
      RaidNode raid = createRaidNode(argv, null);
      if (raid != null) {
        raid.join();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  

}
