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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ErrorSimulator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;

import static org.apache.hadoop.util.ExitUtil.terminate;

import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Krb5AndCertsSslSocketConnector;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the ClientProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
@InterfaceAudience.Private
public class SecondaryNameNode implements Runnable {
    
  static{
    HdfsConfiguration.init();
  }
  public static final Log LOG = 
    LogFactory.getLog(SecondaryNameNode.class.getName());

  private final long starttime = System.currentTimeMillis();
  private volatile long lastCheckpointTime = 0;

  private String fsName;
  private CheckpointStorage checkpointImage;

  private NamenodeProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private volatile boolean shouldRun;
  private HttpServer infoServer;
  private int infoPort;
  private int imagePort;
  private String infoBindAddress;

  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;
  
  /** How often to checkpoint regardless of number of txns */
  private long checkpointPeriod;    // in seconds
  
  /** How often to poll the NN to check checkpointTxnCount */
  private long checkpointCheckPeriod; // in seconds
  
  /** checkpoint once every this many transactions, regardless of time */
  private long checkpointTxnCount;

  /** maxium number of retries when merge errors occur */
  private int maxRetries;


  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + " Status" 
      + "\nName Node Address    : " + nameNodeAddr   
      + "\nStart Time           : " + new Date(starttime)
      + "\nLast Checkpoint Time : " + (lastCheckpointTime == 0? "--": new Date(lastCheckpointTime))
      + "\nCheckpoint Period    : " + checkpointPeriod + " seconds"
      + "\nCheckpoint Size      : " + StringUtils.byteDesc(checkpointTxnCount)
                                    + " (= " + checkpointTxnCount + " bytes)" 
      + "\nCheckpoint Dirs      : " + checkpointDirs
      + "\nCheckpoint Edits Dirs: " + checkpointEditsDirs;
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

  @VisibleForTesting
  List<URI> getCheckpointDirs() {
    return ImmutableList.copyOf(checkpointDirs);
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
      NameNode.initializeGenericKeys(conf,
          DFSUtil.getSecondaryNameServiceId(conf));
      initialize(conf, commandLineOpts);
    } catch(IOException e) {
      shutdown();
      LOG.fatal("Failed to start secondary namenode. ", e);
      throw e;
    } catch(HadoopIllegalArgumentException e) {
      shutdown();
      LOG.fatal("Failed to start secondary namenode. ", e);
      throw e;
    }
  }
  
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return NetUtils.createSocketAddr(conf.get(
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT));
  }
  
  /**
   * Initialize SecondaryNameNode.
   * @param commandLineOpts 
   */
  private void initialize(final Configuration conf,
      CommandLineOpts commandLineOpts) throws IOException {
    final InetSocketAddress infoSocAddr = getHttpAddress(conf);
    infoBindAddress = infoSocAddr.getHostName();
    UserGroupInformation.setConfiguration(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      SecurityUtil.login(conf, DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
          DFS_SECONDARY_NAMENODE_USER_NAME_KEY, infoBindAddress);
    }
    // initiate Java VM metrics
    JvmMetrics.create("SecondaryNameNode",
        conf.get(DFS_METRICS_SESSION_ID_KEY), DefaultMetricsSystem.instance());
    
    // Create connection to the namenode.
    shouldRun = true;
    nameNodeAddr = NameNode.getServiceAddress(conf, true);

    this.conf = conf;
    this.namenode =
        (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID, nameNodeAddr, conf);

    // initialize checkpoint directories
    fsName = getInfoServer();
    checkpointDirs = FSImage.getCheckpointDirs(conf,
                                  "/tmp/hadoop/dfs/namesecondary");
    checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, 
                                  "/tmp/hadoop/dfs/namesecondary");    
    checkpointImage = new CheckpointStorage(conf, checkpointDirs, checkpointEditsDirs);
    checkpointImage.recoverCreate(commandLineOpts.shouldFormat());

    // Initialize other scheduling parameters from the configuration
    checkpointCheckPeriod = conf.getLong(
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT);
        
    checkpointPeriod = conf.getLong(DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
                                    DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    checkpointTxnCount = conf.getLong(DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
                                  DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);

    maxRetries = conf.getInt(DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY,
                                  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT);
    warnForDeprecatedConfigs(conf);

    // initialize the webserver for uploading files.
    // Kerberized SSL servers must be run from the host principal...
    UserGroupInformation httpUGI = 
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          SecurityUtil.getServerPrincipal(conf
              .get(DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
              infoBindAddress),
          conf.get(DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY));
    try {
      infoServer = httpUGI.doAs(new PrivilegedExceptionAction<HttpServer>() {
        @Override
        public HttpServer run() throws IOException, InterruptedException {
          LOG.info("Starting web server as: " +
              UserGroupInformation.getCurrentUser().getUserName());

          int tmpInfoPort = infoSocAddr.getPort();
          infoServer = new HttpServer("secondary", infoBindAddress, tmpInfoPort,
              tmpInfoPort == 0, conf, 
              new AccessControlList(conf.get(DFS_ADMIN, " ")));
          
          if(UserGroupInformation.isSecurityEnabled()) {
            System.setProperty("https.cipherSuites", 
                Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.get(0));
            InetSocketAddress secInfoSocAddr = 
              NetUtils.createSocketAddr(infoBindAddress + ":"+ conf.getInt(
                DFS_NAMENODE_SECONDARY_HTTPS_PORT_KEY,
                DFS_NAMENODE_SECONDARY_HTTPS_PORT_DEFAULT));
            imagePort = secInfoSocAddr.getPort();
            infoServer.addSslListener(secInfoSocAddr, conf, false, true);
          }
          
          infoServer.setAttribute("secondary.name.node", SecondaryNameNode.this);
          infoServer.setAttribute("name.system.image", checkpointImage);
          infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
          infoServer.addInternalServlet("getimage", "/getimage",
              GetImageServlet.class, true);
          infoServer.start();
          return infoServer;
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } 
    
    LOG.info("Web server init done");

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    if (!UserGroupInformation.isSecurityEnabled()) {
      imagePort = infoPort;
    }
    
    conf.set(DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, infoBindAddress + ":" +infoPort); 
    LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" +infoPort);
    LOG.info("Secondary image servlet up at: " + infoBindAddress + ":" + imagePort);
    LOG.info("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.info("Log Size Trigger    :" + checkpointTxnCount + " txns");
  }

  static void warnForDeprecatedConfigs(Configuration conf) {
    for (String key : ImmutableList.of(
          "fs.checkpoint.size",
          "dfs.namenode.checkpoint.size")) {
      if (conf.get(key) != null) {
        LOG.warn("Configuration key " + key + " is deprecated! Ignoring..." +
            " Instead please specify a value for " +
            DFS_NAMENODE_CHECKPOINT_TXNS_KEY);
      }
    }
  }

  /**
   * Wait for the service to finish.
   * (Normally, it runs forever.)
   */
  private void join() {
    try {
      infoServer.join();
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   */
  public void shutdown() {
    shouldRun = false;
    try {
      if (infoServer != null) infoServer.stop();
    } catch (Exception e) {
      LOG.warn("Exception shutting down SecondaryNameNode", e);
    }
    try {
      if (checkpointImage != null) checkpointImage.close();
    } catch(IOException e) {
      LOG.warn("Exception while closing CheckpointStorage", e);
    }
  }

  public void run() {
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation ugi = null;
      try { 
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        LOG.error("Exception while getting login user", e);
        e.printStackTrace();
        terminate(-1);
      }
      ugi.doAs(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          doWork();
          return null;
        }
      });
    } else {
      doWork();
    }
  }
  //
  // The main work loop
  //
  public void doWork() {

    //
    // Poll the Namenode (once every checkpointCheckPeriod seconds) to find the
    // number of transactions in the edit log that haven't yet been checkpointed.
    //
    long period = Math.min(checkpointCheckPeriod, checkpointPeriod);

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
          UserGroupInformation.getCurrentUser().reloginFromKeytab();
        
        long now = System.currentTimeMillis();

        if (shouldCheckpointBasedOnCount() ||
            now >= lastCheckpointTime + 1000 * checkpointPeriod) {
          doCheckpoint();
          lastCheckpointTime = now;
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
        terminate(1);
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
      final String nnHostPort,
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
            boolean downloadImage = true;
            if (sig.mostRecentCheckpointTxId ==
                dstImage.getStorage().getMostRecentCheckpointTxId()) {
              downloadImage = false;
              LOG.info("Image has not changed. Will not download image.");
            } else {
              LOG.info("Image has changed. Downloading updated image from NN.");
              MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
                  nnHostPort, sig.mostRecentCheckpointTxId, dstImage.getStorage(), true);
              dstImage.saveDigestAndRenameCheckpointImage(
                  sig.mostRecentCheckpointTxId, downloadedHash);
            }
        
            // get edits file
            for (RemoteEditLog log : manifest.getLogs()) {
              TransferFsImage.downloadEditsToStorage(
                  nnHostPort, log, dstImage.getStorage());
            }
        
            return Boolean.valueOf(downloadImage);
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
  private String getInfoServer() throws IOException {
    URI fsName = FileSystem.getDefaultUri(conf);
    if (!HdfsConstants.HDFS_URI_SCHEME.equalsIgnoreCase(fsName.getScheme())) {
      throw new IOException("This is not a DFS");
    }

    String configuredAddress = DFSUtil.getInfoServer(null, conf, true);
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    if (sockAddr.getAddress().isAnyLocalAddress()) {
      if(UserGroupInformation.isSecurityEnabled()) {
        throw new IOException("Cannot use a wildcard address with security. " +
                              "Must explicitly set bind address for Kerberos");
      }
      return fsName.getHost() + ":" + sockAddr.getPort();
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("configuredAddress = " + configuredAddress);
      }
      return configuredAddress;
    }
  }
  
  /**
   * Return the host:port of where this SecondaryNameNode is listening
   * for image transfers
   */
  private InetSocketAddress getImageListenAddress() {
    return new InetSocketAddress(infoBindAddress, imagePort);
  }

  /**
   * Create a new checkpoint
   * @return if the image is fetched from primary or not
   */
  boolean doCheckpoint() throws IOException {
    checkpointImage.ensureCurrentDirExists();
    NNStorage dstStorage = checkpointImage.getStorage();
    
    // Tell the namenode to start logging transactions in a new edit file
    // Returns a token that would be used to upload the merged image.
    CheckpointSignature sig = namenode.rollEditLog();
    
    boolean loadImage = false;
    boolean isFreshCheckpointer = (checkpointImage.getNamespaceID() == 0);
    boolean isSameCluster =
        (dstStorage.versionSupportsFederation() && sig.isSameCluster(checkpointImage)) ||
        (!dstStorage.versionSupportsFederation() && sig.namespaceIdMatches(checkpointImage));
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
    if (ErrorSimulator.getErrorSimulation(0)) {
      throw new IOException("Simulating error0 " +
                            "after creating edits.new");
    }

    RemoteEditLogManifest manifest =
      namenode.getEditLogManifest(sig.mostRecentCheckpointTxId + 1);

    // Fetch fsimage and edits. Reload the image if previous merge failed.
    loadImage |= downloadCheckpointFiles(
        fsName, checkpointImage, sig, manifest) |
        checkpointImage.hasMergeError();
    try {
      doMerge(sig, manifest, loadImage, checkpointImage);
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
    TransferFsImage.uploadImageFromStorage(fsName, getImageListenAddress(),
        dstStorage, txid);

    // error simulation code for junit test
    if (ErrorSimulator.getErrorSimulation(1)) {
      throw new IOException("Simulating error1 " +
                            "after uploading new image to NameNode");
    }

    LOG.warn("Checkpoint done. New Image Size: " 
             + dstStorage.getFsImageName(txid).length());
    
    // Since we've successfully checkpointed, we can remove some old
    // image files
    checkpointImage.purgeOldStorage();

    return loadImage;
  }
  
  
  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  private int processStartupCommand(CommandLineOpts opts) throws Exception {
    if (opts.getCommand() == null) {
      return 0;
    }
    
    String cmd = opts.getCommand().toString().toLowerCase();
    
    int exitCode = 0;
    try {
      switch (opts.getCommand()) {
      case CHECKPOINT:
        long count = countUncheckpointedTxns();
        if (count > checkpointTxnCount ||
            opts.shouldForceCheckpoint()) {
          doCheckpoint();
        } else {
          System.err.println("EditLog size " + count + " transactions is " +
                             "smaller than configured checkpoint " +
                             "interval " + checkpointTxnCount + " transactions.");
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
    return countUncheckpointedTxns() >= checkpointTxnCount;
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
    }
    
    StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
    Configuration tconf = new HdfsConfiguration();
    SecondaryNameNode secondary = new SecondaryNameNode(tconf, opts);

    if (opts.getCommand() != null) {
      int ret = secondary.processStartupCommand(opts);
      terminate(ret);
    }

    Daemon checkpointThread = new Daemon(secondary);
    checkpointThread.start();

    if (secondary != null) {
      secondary.join();
    }
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


    Command cmd;
    enum Command {
      GETEDITSIZE,
      CHECKPOINT;
    }
    
    private boolean shouldForce;
    private boolean shouldFormat;

    CommandLineOpts() {
      geteditsizeOpt = new Option("geteditsize",
        "return the number of uncheckpointed transactions on the NameNode");
      checkpointOpt = OptionBuilder.withArgName("force")
        .hasOptionalArg().withDescription("checkpoint on startup").create("checkpoint");;
      formatOpt = new Option("format", "format the local storage during startup");
      
      options.addOption(geteditsizeOpt);
      options.addOption(checkpointOpt);
      options.addOption(formatOpt);
    }
    
    public boolean shouldFormat() {
      return shouldFormat;
    }

    public void parse(String ... argv) throws ParseException {
      CommandLineParser parser = new PosixParser();
      CommandLine cmdLine = parser.parse(options, argv);
      
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
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("secondarynamenode", options);
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
    /**
     * Construct a checkpoint image.
     * @param conf Node configuration.
     * @param imageDirs URIs of storage for image.
     * @param editDirs URIs of storage for edit logs.
     * @throws IOException If storage cannot be access.
     */
    CheckpointStorage(Configuration conf, 
                      Collection<URI> imageDirs,
                      Collection<URI> editsDirs) throws IOException {
      super(conf, (FSNamesystem)null, imageDirs, editsDirs);
      setFSNamesystem(new FSNamesystem(this, conf));
      
      // the 2NN never writes edits -- it only downloads them. So
      // we shouldn't have any editLog instance. Setting to null
      // makes sure we don't accidentally depend on it.
      editLog = null;
      mergeErrorCount = 0;
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
  }
    
  static void doMerge(
      CheckpointSignature sig, RemoteEditLogManifest manifest,
      boolean loadImage, FSImage dstImage) throws IOException {   
    NNStorage dstStorage = dstImage.getStorage();
    
    dstStorage.setStorageInfo(sig);
    if (loadImage) {
      File file = dstStorage.findImageFile(sig.mostRecentCheckpointTxId);
      if (file == null) {
        throw new IOException("Couldn't find image file at txid " + 
            sig.mostRecentCheckpointTxId + " even though it should have " +
            "just been downloaded");
      }
      dstImage.reloadFromImageFile(file);
      dstImage.getFSNamesystem().dir.imageLoadComplete();
    }
    Checkpointer.rollForwardByApplyingLogs(manifest, dstImage);

    // error simulation for junit tests
    if (ErrorSimulator.getErrorSimulation(5)) {
      throw new IOException("Simulating error5 " +
                            "after uploading new image to NameNode");
    }

    dstImage.saveFSImageInAllDirs(dstImage.getLastAppliedTxId());
    dstStorage.writeAll();
  }
}
