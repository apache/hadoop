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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.util.VersionInfo;

import javax.management.ObjectName;

/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the NamenodeProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
@InterfaceAudience.Private
public class SecondaryNameNode implements Runnable,
        SecondaryNameNodeInfoMXBean {
    
  static{
    HdfsConfiguration.init();
  }
  public static final Log LOG = 
    LogFactory.getLog(SecondaryNameNode.class.getName());

  private final long starttime = Time.now();
  private volatile long lastCheckpointTime = 0;
  private volatile long lastCheckpointWallclockTime = 0;

  private URL fsName;
  private CheckpointStorage checkpointImage;

  private NamenodeProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private volatile boolean shouldRun;
  private HttpServer2 infoServer;

  private Collection<URI> checkpointDirs;
  private List<URI> checkpointEditsDirs;

  private CheckpointConf checkpointConf;
  private FSNamesystem namesystem;

  private Thread checkpointThread;
  private ObjectName nameNodeStatusBeanName;
  private String legacyOivImageDir;

  @Override
  public String toString() {
    return getClass().getSimpleName() + " Status" 
      + "\nName Node Address      : " + nameNodeAddr
      + "\nStart Time             : " + new Date(starttime)
      + "\nLast Checkpoint        : " + (lastCheckpointTime == 0? "--":
        new Date(lastCheckpointWallclockTime))
      + " (" + ((Time.monotonicNow() - lastCheckpointTime) / 1000)
      + " seconds ago)"
      + "\nCheckpoint Period      : " + checkpointConf.getPeriod() + " seconds"
      + "\nCheckpoint Transactions: " + checkpointConf.getTxnCount()
      + "\nCheckpoint Dirs        : " + checkpointDirs
      + "\nCheckpoint Edits Dirs  : " + checkpointEditsDirs;
  }

  @VisibleForTesting
  FSImage getFSImage() {
    return checkpointImage;
  }

  @VisibleForTesting
  int getMergeErrorCount() {
    return checkpointImage.getMergeErrorCount();
  }

  @VisibleForTesting
  public FSNamesystem getFSNamesystem() {
    return namesystem;
  }
  
  @VisibleForTesting
  void setFSImage(CheckpointStorage image) {
    this.checkpointImage = image;
  }
  
  @VisibleForTesting
  NamenodeProtocol getNameNode() {
    return namenode;
  }
  
  @VisibleForTesting
  void setNameNode(NamenodeProtocol namenode) {
    this.namenode = namenode;
  }

  /**
   * Create a connection to the primary namenode.
   */
  public SecondaryNameNode(Configuration conf)  throws IOException {
    this(conf, new CommandLineOpts());
  }
  
  public SecondaryNameNode(Configuration conf,
      CommandLineOpts commandLineOpts) throws IOException {
    try {
      String nsId = DFSUtil.getSecondaryNameServiceId(conf);
      if (HAUtil.isHAEnabled(conf, nsId)) {
        throw new IOException(
            "Cannot use SecondaryNameNode in an HA cluster." +
            " The Standby Namenode will perform checkpointing.");
      }
      NameNode.initializeGenericKeys(conf, nsId, null);
      initialize(conf, commandLineOpts);
    } catch (IOException e) {
      shutdown();
      throw e;
    } catch (HadoopIllegalArgumentException e) {
      shutdown();
      throw e;
    }
  }
  
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return NetUtils.createSocketAddr(conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT));
  }
  
  /**
   * Initialize SecondaryNameNode.
   */
  private void initialize(final Configuration conf,
      CommandLineOpts commandLineOpts) throws IOException {
    final InetSocketAddress infoSocAddr = getHttpAddress(conf);
    final String infoBindAddress = infoSocAddr.getHostName();
    UserGroupInformation.setConfiguration(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      SecurityUtil.login(conf,
          DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY, infoBindAddress);
    }
    // initiate Java VM metrics
    DefaultMetricsSystem.initialize("SecondaryNameNode");
    JvmMetrics.create("SecondaryNameNode",
        conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
        DefaultMetricsSystem.instance());

    // Create connection to the namenode.
    shouldRun = true;
    nameNodeAddr = NameNode.getServiceAddress(conf, true);

    this.conf = conf;
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nameNodeAddr, 
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(),
        true).getProxy();

    // initialize checkpoint directories
    fsName = getInfoServer();
    checkpointDirs = FSImage.getCheckpointDirs(conf,
                                  "/tmp/hadoop/dfs/namesecondary");
    checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, 
                                  "/tmp/hadoop/dfs/namesecondary");    
    checkpointImage = new CheckpointStorage(conf, checkpointDirs, checkpointEditsDirs);
    checkpointImage.recoverCreate(commandLineOpts.shouldFormat());
    checkpointImage.deleteTempEdits();
    
    namesystem = new FSNamesystem(conf, checkpointImage, true);

    // Disable quota checks
    namesystem.dir.disableQuotaChecks();

    // Initialize other scheduling parameters from the configuration
    checkpointConf = new CheckpointConf(conf);
    nameNodeStatusBeanName = MBeans.register("SecondaryNameNode",
            "SecondaryNameNodeInfo", this);

    legacyOivImageDir = conf.get(
        DFSConfigKeys.DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY);

    LOG.info("Checkpoint Period   :" + checkpointConf.getPeriod() + " secs "
        + "(" + checkpointConf.getPeriod() / 60 + " min)");
    LOG.info("Log Size Trigger    :" + checkpointConf.getTxnCount() + " txns");
  }

  /**
   * Wait for the service to finish.
   * (Normally, it runs forever.)
   */
  private void join() {
    try {
      infoServer.join();
    } catch (InterruptedException ie) {
      LOG.debug("Exception ", ie);
    }
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   */
  public void shutdown() {
    shouldRun = false;
    if (checkpointThread != null) {
      checkpointThread.interrupt();
      try {
        checkpointThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Interrupted waiting to join on checkpointer thread");
        Thread.currentThread().interrupt(); // maintain status
      }
    }
    try {
      if (infoServer != null) {
        infoServer.stop();
        infoServer = null;
      }
    } catch (Exception e) {
      LOG.warn("Exception shutting down SecondaryNameNode", e);
    }
    if (nameNodeStatusBeanName != null) {
      MBeans.unregister(nameNodeStatusBeanName);
      nameNodeStatusBeanName = null;
    }
    try {
      if (checkpointImage != null) {
        checkpointImage.close();
        checkpointImage = null;
      }
    } catch(IOException e) {
      LOG.warn("Exception while closing CheckpointStorage", e);
    }
    if (namesystem != null) {
      namesystem.shutdown();
      namesystem = null;
    }
  }

  @Override
  public void run() {
    SecurityUtil.doAsLoginUserOrFatal(
        new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          doWork();
          return null;
        }
      });
  }
  //
  // The main work loop
  //
  public void doWork() {
    //
    // Poll the Namenode (once every checkpointCheckPeriod seconds) to find the
    // number of transactions in the edit log that haven't yet been checkpointed.
    //
    long period = checkpointConf.getCheckPeriod();
    int maxRetries = checkpointConf.getMaxRetriesOnMergeError();

    while (shouldRun) {
      try {
        Thread.sleep(1000 * period);
      } catch (InterruptedException ie) {
        // do nothing
      }
      if (!shouldRun) {
        break;
      }
      try {
        // We may have lost our ticket since last checkpoint, log in again, just in case
        if(UserGroupInformation.isSecurityEnabled())
          UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
        
        final long monotonicNow = Time.monotonicNow();
        final long now = Time.now();

        if (shouldCheckpointBasedOnCount() ||
            monotonicNow >= lastCheckpointTime + 1000 * checkpointConf.getPeriod()) {
          doCheckpoint();
          lastCheckpointTime = monotonicNow;
          lastCheckpointWallclockTime = now;
        }
      } catch (IOException e) {
        LOG.error("Exception in doCheckpoint", e);
        e.printStackTrace();
        // Prevent a huge number of edits from being created due to
        // unrecoverable conditions and endless retries.
        if (checkpointImage.getMergeErrorCount() > maxRetries) {
          LOG.fatal("Merging failed " + 
              checkpointImage.getMergeErrorCount() + " times.");
          terminate(1);
        }
      } catch (Throwable e) {
        LOG.fatal("Throwable Exception in doCheckpoint", e);
        e.printStackTrace();
        terminate(1, e);
      }
    }
  }

  /**
   * Download <code>fsimage</code> and <code>edits</code>
   * files from the name-node.
   * @return true if a new image has been downloaded and needs to be loaded
   * @throws IOException
   */
  static boolean downloadCheckpointFiles(
      final URL nnHostPort,
      final FSImage dstImage,
      final CheckpointSignature sig,
      final RemoteEditLogManifest manifest
  ) throws IOException {
    
    // Sanity check manifest - these could happen if, eg, someone on the
    // NN side accidentally rmed the storage directories
    if (manifest.getLogs().isEmpty()) {
      throw new IOException("Found no edit logs to download on NN since txid " 
          + sig.mostRecentCheckpointTxId);
    }
    
    long expectedTxId = sig.mostRecentCheckpointTxId + 1;
    if (manifest.getLogs().get(0).getStartTxId() != expectedTxId) {
      throw new IOException("Bad edit log manifest (expected txid = " +
          expectedTxId + ": " + manifest);
    }

    try {
        Boolean b = UserGroupInformation.getCurrentUser().doAs(
            new PrivilegedExceptionAction<Boolean>() {
  
          @Override
          public Boolean run() throws Exception {
            dstImage.getStorage().cTime = sig.cTime;

            // get fsimage
            if (sig.mostRecentCheckpointTxId ==
                dstImage.getStorage().getMostRecentCheckpointTxId()) {
              LOG.info("Image has not changed. Will not download image.");
            } else {
              LOG.info("Image has changed. Downloading updated image from NN.");
              MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
                  nnHostPort, sig.mostRecentCheckpointTxId,
                  dstImage.getStorage(), true, false);
              dstImage.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE,
                  sig.mostRecentCheckpointTxId, downloadedHash);
            }
        
            // get edits file
            for (RemoteEditLog log : manifest.getLogs()) {
              TransferFsImage.downloadEditsToStorage(
                  nnHostPort, log, dstImage.getStorage());
            }
        
            // true if we haven't loaded all the transactions represented by the
            // downloaded fsimage.
            return dstImage.getLastAppliedTxId() < sig.mostRecentCheckpointTxId;
          }
        });
        return b.booleanValue();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
  }
  
  InetSocketAddress getNameNodeAddress() {
    return nameNodeAddr;
  }

  /**
   * Returns the Jetty server that the Namenode is listening on.
   */
  private URL getInfoServer() throws IOException {
    URI fsName = FileSystem.getDefaultUri(conf);
    if (!HdfsConstants.HDFS_URI_SCHEME.equalsIgnoreCase(fsName.getScheme())) {
      throw new IOException("This is not a DFS");
    }

    final String scheme = DFSUtil.getHttpClientScheme(conf);
    URI address = DFSUtil.getInfoServerWithDefaultHost(fsName.getHost(), conf,
        scheme);
    LOG.debug("Will connect to NameNode at " + address);
    return address.toURL();
  }

  /**
   * Start the web server.
   */
  @VisibleForTesting
  public void startInfoServer() throws IOException {
    final InetSocketAddress httpAddr = getHttpAddress(conf);
    final String httpsAddrString = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf,
        httpAddr, httpsAddr, "secondary", DFSConfigKeys.
            DFS_SECONDARY_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY);

    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    infoServer = builder.build();
    infoServer.setAttribute("secondary.name.node", this);
    infoServer.setAttribute("name.system.image", checkpointImage);
    infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    infoServer.addInternalServlet("imagetransfer", ImageServlet.PATH_SPEC,
        ImageServlet.class, true);
    infoServer.start();

    LOG.info("Web server init done");

    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      InetSocketAddress httpAddress =
          infoServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      InetSocketAddress httpsAddress =
          infoServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
    }
  }

  /**
   * Create a new checkpoint
   * @return if the image is fetched from primary or not
   */
  @VisibleForTesting
  @SuppressWarnings("deprecated")
  public boolean doCheckpoint() throws IOException {
    checkpointImage.ensureCurrentDirExists();
    NNStorage dstStorage = checkpointImage.getStorage();
    
    // Tell the namenode to start logging transactions in a new edit file
    // Returns a token that would be used to upload the merged image.
    CheckpointSignature sig = namenode.rollEditLog();
    
    boolean loadImage = false;
    boolean isFreshCheckpointer = (checkpointImage.getNamespaceID() == 0);
    boolean isSameCluster =
        (dstStorage.versionSupportsFederation(NameNodeLayoutVersion.FEATURES)
            && sig.isSameCluster(checkpointImage)) ||
        (!dstStorage.versionSupportsFederation(NameNodeLayoutVersion.FEATURES)
            && sig.namespaceIdMatches(checkpointImage));
    if (isFreshCheckpointer ||
        (isSameCluster &&
         !sig.storageVersionMatches(checkpointImage.getStorage()))) {
      // if we're a fresh 2NN, or if we're on the same cluster and our storage
      // needs an upgrade, just take the storage info from the server.
      dstStorage.setStorageInfo(sig);
      dstStorage.setClusterID(sig.getClusterID());
      dstStorage.setBlockPoolID(sig.getBlockpoolID());
      loadImage = true;
    }
    sig.validateStorageInfo(checkpointImage);

    // error simulation code for junit test
    CheckpointFaultInjector.getInstance().afterSecondaryCallsRollEditLog();

    RemoteEditLogManifest manifest =
      namenode.getEditLogManifest(sig.mostRecentCheckpointTxId + 1);

    // Fetch fsimage and edits. Reload the image if previous merge failed.
    loadImage |= downloadCheckpointFiles(
        fsName, checkpointImage, sig, manifest) |
        checkpointImage.hasMergeError();
    try {
      doMerge(sig, manifest, loadImage, checkpointImage, namesystem);
    } catch (IOException ioe) {
      // A merge error occurred. The in-memory file system state may be
      // inconsistent, so the image and edits need to be reloaded.
      checkpointImage.setMergeError();
      throw ioe;
    }
    // Clear any error since merge was successful.
    checkpointImage.clearMergeError();

    
    //
    // Upload the new image into the NameNode. Then tell the Namenode
    // to make this new uploaded image as the most current image.
    //
    long txid = checkpointImage.getLastAppliedTxId();
    TransferFsImage.uploadImageFromStorage(fsName, conf, dstStorage,
        NameNodeFile.IMAGE, txid);

    // error simulation code for junit test
    CheckpointFaultInjector.getInstance().afterSecondaryUploadsNewImage();

    LOG.warn("Checkpoint done. New Image Size: " 
             + dstStorage.getFsImageName(txid).length());

    if (legacyOivImageDir != null && !legacyOivImageDir.isEmpty()) {
      try {
        checkpointImage.saveLegacyOIVImage(namesystem, legacyOivImageDir,
            new Canceler());
      } catch (IOException e) {
        LOG.warn("Failed to write legacy OIV image: ", e);
      }
    }
    return loadImage;
  }

  /**
   * @param opts The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  private int processStartupCommand(CommandLineOpts opts) throws Exception {
    if (opts.getCommand() == null) {
      return 0;
    }
    
    String cmd = StringUtils.toLowerCase(opts.getCommand().toString());
    
    int exitCode = 0;
    try {
      switch (opts.getCommand()) {
      case CHECKPOINT:
        long count = countUncheckpointedTxns();
        if (count > checkpointConf.getTxnCount() ||
            opts.shouldForceCheckpoint()) {
          doCheckpoint();
        } else {
          System.err.println("EditLog size " + count + " transactions is " +
                             "smaller than configured checkpoint " +
                             "interval " + checkpointConf.getTxnCount() + " transactions.");
          System.err.println("Skipping checkpoint.");
        }
        break;
      case GETEDITSIZE:
        long uncheckpointed = countUncheckpointedTxns();
        System.out.println("NameNode has " + uncheckpointed +
            " uncheckpointed transactions");
        break;
      default:
        throw new AssertionError("bad command enum: " + opts.getCommand());
      }
      
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = 1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        LOG.error(cmd + ": " + content[0]);
      } catch (Exception ex) {
        LOG.error(cmd + ": " + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = 1;
      LOG.error(cmd + ": " + e.getLocalizedMessage());
    } finally {
      // Does the RPC connection need to be closed?
    }
    return exitCode;
  }

  private long countUncheckpointedTxns() throws IOException {
    long curTxId = namenode.getTransactionID();
    long uncheckpointedTxns = curTxId -
      checkpointImage.getStorage().getMostRecentCheckpointTxId();
    assert uncheckpointedTxns >= 0;
    return uncheckpointedTxns;
  }

  boolean shouldCheckpointBasedOnCount() throws IOException {
    return countUncheckpointedTxns() >= checkpointConf.getTxnCount();
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    CommandLineOpts opts = SecondaryNameNode.parseArgs(argv);
    if (opts == null) {
      LOG.fatal("Failed to parse options");
      terminate(1);
    } else if (opts.shouldPrintHelp()) {
      opts.usage();
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
      Configuration tconf = new HdfsConfiguration();
      SecondaryNameNode secondary = null;
      secondary = new SecondaryNameNode(tconf, opts);

      // SecondaryNameNode can be started in 2 modes:
      // 1. run a command (i.e. checkpoint or geteditsize) then terminate
      // 2. run as a daemon when {@link #parseArgs} yields no commands
      if (opts != null && opts.getCommand() != null) {
        // mode 1
        int ret = secondary.processStartupCommand(opts);
        terminate(ret);
      } else {
        // mode 2
        secondary.startInfoServer();
        secondary.startCheckpointThread();
        secondary.join();
      }
    } catch (Throwable e) {
      LOG.fatal("Failed to start secondary namenode", e);
      terminate(1);
    }
  }

  public void startCheckpointThread() {
    Preconditions.checkState(checkpointThread == null,
        "Should not already have a thread");
    Preconditions.checkState(shouldRun, "shouldRun should be true");
    
    checkpointThread = new Daemon(this);
    checkpointThread.start();
  }

  @Override // SecondaryNameNodeInfoMXBean
  public String getHostAndPort() {
    return NetUtils.getHostPortString(nameNodeAddr);
  }

  @Override // SecondaryNameNodeInfoMXBean
  public long getStartTime() {
    return starttime;
  }

  @Override // SecondaryNameNodeInfoMXBean
  public long getLastCheckpointTime() {
    return lastCheckpointWallclockTime;
  }

  @Override // SecondaryNameNodeInfoMXBean
  public long getLastCheckpointDeltaMs() {
    if (lastCheckpointTime == 0) {
      return -1;
    } else {
      return (Time.monotonicNow() - lastCheckpointTime);
    }
  }

  @Override // SecondaryNameNodeInfoMXBean
  public String[] getCheckpointDirectories() {
    ArrayList<String> r = Lists.newArrayListWithCapacity(checkpointDirs.size());
    for (URI d : checkpointDirs) {
      r.add(d.toString());
    }
    return r.toArray(new String[r.size()]);
  }

  @Override // SecondaryNameNodeInfoMXBean
  public String[] getCheckpointEditlogDirectories() {
    ArrayList<String> r = Lists.newArrayListWithCapacity(checkpointEditsDirs.size());
    for (URI d : checkpointEditsDirs) {
      r.add(d.toString());
    }
    return r.toArray(new String[r.size()]);
  }

  @Override // VersionInfoMXBean
  public String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() +
            " from " + VersionInfo.getBranch();
  }

  @Override // VersionInfoMXBean
  public String getSoftwareVersion() {
    return VersionInfo.getVersion();
  }


  /**
   * Container for parsed command-line options.
   */
  @SuppressWarnings("static-access")
  static class CommandLineOpts {
    private final Options options = new Options();
    
    private final Option geteditsizeOpt;
    private final Option checkpointOpt;
    private final Option formatOpt;
    private final Option helpOpt;


    Command cmd;
    enum Command {
      GETEDITSIZE,
      CHECKPOINT;
    }
    
    private boolean shouldForce;
    private boolean shouldFormat;
    private boolean shouldPrintHelp;

    CommandLineOpts() {
      geteditsizeOpt = new Option("geteditsize",
        "return the number of uncheckpointed transactions on the NameNode");
      checkpointOpt = OptionBuilder.withArgName("force")
        .hasOptionalArg().withDescription("checkpoint on startup").create("checkpoint");;
      formatOpt = new Option("format", "format the local storage during startup");
      helpOpt = new Option("h", "help", false, "get help information");
      
      options.addOption(geteditsizeOpt);
      options.addOption(checkpointOpt);
      options.addOption(formatOpt);
      options.addOption(helpOpt);
    }
    
    public boolean shouldFormat() {
      return shouldFormat;
    }

    public boolean shouldPrintHelp() {
      return shouldPrintHelp;
    }
    
    public void parse(String ... argv) throws ParseException {
      CommandLineParser parser = new PosixParser();
      CommandLine cmdLine = parser.parse(options, argv);
      
      if (cmdLine.hasOption(helpOpt.getOpt())
          || cmdLine.hasOption(helpOpt.getLongOpt())) {
        shouldPrintHelp = true;
        return;
      }
      
      boolean hasGetEdit = cmdLine.hasOption(geteditsizeOpt.getOpt());
      boolean hasCheckpoint = cmdLine.hasOption(checkpointOpt.getOpt()); 
      if (hasGetEdit && hasCheckpoint) {
        throw new ParseException("May not pass both "
            + geteditsizeOpt.getOpt() + " and "
            + checkpointOpt.getOpt());
      }
      
      if (hasGetEdit) {
        cmd = Command.GETEDITSIZE;
      } else if (hasCheckpoint) {
        cmd = Command.CHECKPOINT;
        
        String arg = cmdLine.getOptionValue(checkpointOpt.getOpt());
        if ("force".equals(arg)) {
          shouldForce = true;
        } else if (arg != null) {
          throw new ParseException("-checkpoint may only take 'force' as an "
              + "argument");
        }
      }
      
      if (cmdLine.hasOption(formatOpt.getOpt())) {
        shouldFormat = true;
      }
    }
    
    public Command getCommand() {
      return cmd;
    }
    
    public boolean shouldForceCheckpoint() {
      return shouldForce;
    }
    
    void usage() {
      String header = "The Secondary NameNode is a helper "
          + "to the primary NameNode. The Secondary is responsible "
          + "for supporting periodic checkpoints of the HDFS metadata. "
          + "The current design allows only one Secondary NameNode "
          + "per HDFS cluster.";
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("secondarynamenode", header, options, "", false);
    }
  }

  private static CommandLineOpts parseArgs(String[] argv) {
    CommandLineOpts opts = new CommandLineOpts();
    try {
      opts.parse(argv);
    } catch (ParseException pe) {
      LOG.error(pe.getMessage());
      opts.usage();
      return null;
    }
    return opts;
  }
  
  static class CheckpointStorage extends FSImage {
    
    private int mergeErrorCount;
    private static class CheckpointLogPurger implements LogsPurgeable {
      
      private final NNStorage storage;
      private final StoragePurger purger
          = new NNStorageRetentionManager.DeletionStoragePurger();
      
      public CheckpointLogPurger(NNStorage storage) {
        this.storage = storage;
      }

      @Override
      public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
        Iterator<StorageDirectory> iter = storage.dirIterator();
        while (iter.hasNext()) {
          StorageDirectory dir = iter.next();
          List<EditLogFile> editFiles = FileJournalManager.matchEditLogs(
              dir.getCurrentDir());
          for (EditLogFile f : editFiles) {
            if (f.getLastTxId() < minTxIdToKeep) {
              purger.purgeLog(f);
            }
          }
        }
      }

      @Override
      public void selectInputStreams(Collection<EditLogInputStream> streams,
          long fromTxId, boolean inProgressOk, boolean onlyDurableTxns) {
        Iterator<StorageDirectory> iter = storage.dirIterator();
        while (iter.hasNext()) {
          StorageDirectory dir = iter.next();
          List<EditLogFile> editFiles;
          try {
            editFiles = FileJournalManager.matchEditLogs(
                dir.getCurrentDir());
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          FileJournalManager.addStreamsToCollectionFromFiles(editFiles, streams,
              fromTxId, Long.MAX_VALUE, inProgressOk);
        }
      }
      
    }
    
    /**
     * Construct a checkpoint image.
     * @param conf Node configuration.
     * @param imageDirs URIs of storage for image.
     * @param editsDirs URIs of storage for edit logs.
     * @throws IOException If storage cannot be access.
     */
    CheckpointStorage(Configuration conf, 
                      Collection<URI> imageDirs,
                      List<URI> editsDirs) throws IOException {
      super(conf, imageDirs, editsDirs);

      // the 2NN never writes edits -- it only downloads them. So
      // we shouldn't have any editLog instance. Setting to null
      // makes sure we don't accidentally depend on it.
      editLog = null;
      mergeErrorCount = 0;
      
      // Replace the archival manager with one that can actually work on the
      // 2NN's edits storage.
      this.archivalManager = new NNStorageRetentionManager(conf, storage,
          new CheckpointLogPurger(storage));
    }

    /**
     * Analyze checkpoint directories.
     * Create directories if they do not exist.
     * Recover from an unsuccessful checkpoint if necessary.
     *
     * @throws IOException
     */
    void recoverCreate(boolean format) throws IOException {
      storage.attemptRestoreRemovedStorage();
      storage.unlockAll();

      for (Iterator<StorageDirectory> it = 
                   storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        boolean isAccessible = true;
        try { // create directories if don't exist yet
          if(!sd.getRoot().mkdirs()) {
            // do nothing, directory is already created
          }
        } catch(SecurityException se) {
          isAccessible = false;
        }
        if(!isAccessible)
          throw new InconsistentFSStateException(sd.getRoot(),
              "cannot access checkpoint directory.");
        
        if (format) {
          // Don't confirm, since this is just the secondary namenode.
          LOG.info("Formatting storage directory " + sd);
          sd.clearDirectory();
        }
        
        StorageState curState;
        try {
          curState = sd.analyzeStorage(HdfsServerConstants.StartupOption.REGULAR, storage);
          // sd is locked but not opened
          switch(curState) {
          case NON_EXISTENT:
            // fail if any of the configured checkpoint dirs are inaccessible 
            throw new InconsistentFSStateException(sd.getRoot(),
                  "checkpoint directory does not exist or is not accessible.");
          case NOT_FORMATTED:
            break;  // it's ok since initially there is no current and VERSION
          case NORMAL:
            // Read the VERSION file. This verifies that:
            // (a) the VERSION file for each of the directories is the same,
            // and (b) when we connect to a NN, we can verify that the remote
            // node matches the same namespace that we ran on previously.
            storage.readProperties(sd);
            break;
          default:  // recovery is possible
            sd.doRecover(curState);
          }
        } catch (IOException ioe) {
          sd.unlock();
          throw ioe;
        }
      }
    }


    boolean hasMergeError() {
      return (mergeErrorCount > 0);
    }

    int getMergeErrorCount() {
      return mergeErrorCount;
    }

    void setMergeError() {
      mergeErrorCount++;
    }

    void clearMergeError() {
      mergeErrorCount = 0;
    }
 
    /**
     * Ensure that the current/ directory exists in all storage
     * directories
     */
    void ensureCurrentDirExists() throws IOException {
      for (Iterator<StorageDirectory> it
             = storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        File curDir = sd.getCurrentDir();
        if (!curDir.exists() && !curDir.mkdirs()) {
          throw new IOException("Could not create directory " + curDir);
        }
      }
    }

    void deleteTempEdits() throws IOException {
      FilenameFilter filter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.matches(NameNodeFile.EDITS_TMP.getName()
              + "_(\\d+)-(\\d+)_(\\d+)");
        }
      };
      Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.EDITS);
      for (;it.hasNext();) {
        StorageDirectory dir = it.next();
        File[] tempEdits = dir.getCurrentDir().listFiles(filter);
        if (tempEdits != null) {
          for (File t : tempEdits) {
            boolean success = t.delete();
            if (!success) {
              LOG.warn("Failed to delete temporary edits file: "
                  + t.getAbsolutePath());
            }
          }
        }
      }
    }

  }
    
  void doMerge(
      CheckpointSignature sig, RemoteEditLogManifest manifest,
      boolean loadImage, FSImage dstImage, FSNamesystem dstNamesystem)
      throws IOException {   
    NNStorage dstStorage = dstImage.getStorage();
    
    dstStorage.setStorageInfo(sig);
    if (loadImage) {
      File file = dstStorage.findImageFile(NameNodeFile.IMAGE,
          sig.mostRecentCheckpointTxId);
      if (file == null) {
        throw new IOException("Couldn't find image file at txid " + 
            sig.mostRecentCheckpointTxId + " even though it should have " +
            "just been downloaded");
      }
      dstNamesystem.writeLock();
      try {
        dstImage.reloadFromImageFile(file, dstNamesystem);
      } finally {
        dstNamesystem.writeUnlock();
      }
      dstNamesystem.imageLoadComplete();
    }
    // error simulation code for junit test
    CheckpointFaultInjector.getInstance().duringMerge();   

    Checkpointer.rollForwardByApplyingLogs(manifest, dstImage, dstNamesystem);
    // The following has the side effect of purging old fsimages/edit logs.
    dstImage.saveFSImageInAllDirs(dstNamesystem, dstImage.getLastAppliedTxId());
    if (!namenode.isRollingUpgrade()) {
      dstImage.updateStorageVersion();
    }
  }
}
