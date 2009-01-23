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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.metrics.jvm.JvmMetrics;

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
public class SecondaryNameNode implements Runnable {
    
  public static final Log LOG = 
    LogFactory.getLog(SecondaryNameNode.class.getName());

  private String fsName;
  private CheckpointStorage checkpointImage;

  private NamenodeProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private volatile boolean shouldRun;
  private HttpServer infoServer;
  private int infoPort;
  private String infoBindAddress;

  private Collection<File> checkpointDirs;
  private Collection<File> checkpointEditsDirs;
  private long checkpointPeriod;	// in seconds
  private long checkpointSize;    // size (in MB) of current Edit Log

  /**
   * Utility class to facilitate junit test error simulation.
   */
  static class ErrorSimulator {
    private static boolean[] simulation = null; // error simulation events
    static void initializeErrorSimulationEvent(int numberOfEvents) {
      simulation = new boolean[numberOfEvents]; 
      for (int i = 0; i < numberOfEvents; i++) {
        simulation[i] = false;
      }
    }
    
    static boolean getErrorSimulation(int index) {
      if(simulation == null)
        return false;
      assert(index < simulation.length);
      return simulation[index];
    }
    
    static void setErrorSimulation(int index) {
      assert(index < simulation.length);
      simulation[index] = true;
    }
    
    static void clearErrorSimulation(int index) {
      assert(index < simulation.length);
      simulation[index] = false;
    }
  }

  FSImage getFSImage() {
    return checkpointImage;
  }

  /**
   * Create a connection to the primary namenode.
   */
  public SecondaryNameNode(Configuration conf)  throws IOException {
    try {
      initialize(conf);
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /**
   * Initialize SecondaryNameNode.
   */
  private void initialize(Configuration conf) throws IOException {
    // initiate Java VM metrics
    JvmMetrics.init("SecondaryNameNode", conf.get("session.id"));
    
    // Create connection to the namenode.
    shouldRun = true;
    nameNodeAddr = NameNode.getAddress(conf);

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
    checkpointImage = new CheckpointStorage();
    checkpointImage.recoverCreate(checkpointDirs, checkpointEditsDirs);

    // Initialize other scheduling parameters from the configuration
    checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    checkpointSize = conf.getLong("fs.checkpoint.size", 4194304);

    // initialize the webserver for uploading files.
    String infoAddr = 
      NetUtils.getServerAddress(conf, 
                                "dfs.secondary.info.bindAddress",
                                "dfs.secondary.info.port",
                                "dfs.secondary.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new HttpServer("secondary", infoBindAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("name.system.image", checkpointImage);
    this.infoServer.setAttribute("name.conf", conf);
    infoServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    infoServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    conf.set("dfs.secondary.http.address", infoBindAddress + ":" +infoPort); 
    LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" +infoPort);
    LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.warn("Log Size Trigger    :" + checkpointSize + " bytes " +
             "(" + checkpointSize/1024 + " KB)");
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
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  //
  // The main work loop
  //
  public void run() {

    //
    // Poll the Namenode (once every 5 minutes) to find the size of the
    // pending edit log.
    //
    long period = 5 * 60;              // 5 minutes
    long lastCheckpointTime = 0;
    if (checkpointPeriod < period) {
      period = checkpointPeriod;
    }

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
        long now = System.currentTimeMillis();

        long size = namenode.getEditLogSize();
        if (size >= checkpointSize || 
            now >= lastCheckpointTime + 1000 * checkpointPeriod) {
          doCheckpoint();
          lastCheckpointTime = now;
        }
      } catch (IOException e) {
        LOG.error("Exception in doCheckpoint: ");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
      } catch (Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  /**
   * Download <code>fsimage</code> and <code>edits</code>
   * files from the name-node.
   * @throws IOException
   */
  private void downloadCheckpointFiles(CheckpointSignature sig
                                      ) throws IOException {
    
    checkpointImage.cTime = sig.cTime;
    checkpointImage.checkpointTime = sig.checkpointTime;

    // get fsimage
    String fileid = "getimage=1";
    File[] srcNames = checkpointImage.getImageFiles();
    assert srcNames.length > 0 : "No checkpoint targets.";
    TransferFsImage.getFileClient(fsName, fileid, srcNames);
    LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
             srcNames[0].length() + " bytes.");

    // get edits file
    fileid = "getedit=1";
    srcNames = checkpointImage.getEditsFiles();
    assert srcNames.length > 0 : "No checkpoint targets.";
    TransferFsImage.getFileClient(fsName, fileid, srcNames);
    LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
        srcNames[0].length() + " bytes.");

    checkpointImage.checkpointUploadDone();
  }

  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(CheckpointSignature sig) throws IOException {
    String fileid = "putimage=1&port=" + infoPort +
      "&machine=" +
      InetAddress.getLocalHost().getHostAddress() +
      "&token=" + sig.toString();
    LOG.info("Posted URL " + fsName + fileid);
    TransferFsImage.getFileClient(fsName, fileid, (File[])null);
  }

  /**
   * Returns the Jetty server that the Namenode is listening on.
   */
  private String getInfoServer() throws IOException {
    URI fsName = FileSystem.getDefaultUri(conf);
    if (!"hdfs".equals(fsName.getScheme())) {
      throw new IOException("This is not a DFS");
    }
    return NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                     "dfs.info.port", "dfs.http.address");
  }

  /**
   * Create a new checkpoint
   */
  void doCheckpoint() throws IOException {

    // Do the required initialization of the merge work area.
    startCheckpoint();

    // Tell the namenode to start logging transactions in a new edit file
    // Retuns a token that would be used to upload the merged image.
    CheckpointSignature sig = (CheckpointSignature)namenode.rollEditLog();

    // error simulation code for junit test
    if (ErrorSimulator.getErrorSimulation(0)) {
      throw new IOException("Simulating error0 " +
                            "after creating edits.new");
    }

    downloadCheckpointFiles(sig);   // Fetch fsimage and edits
    doMerge(sig);                   // Do the merge
  
    //
    // Upload the new image into the NameNode. Then tell the Namenode
    // to make this new uploaded image as the most current image.
    //
    putFSImage(sig);

    // error simulation code for junit test
    if (ErrorSimulator.getErrorSimulation(1)) {
      throw new IOException("Simulating error1 " +
                            "after uploading new image to NameNode");
    }

    namenode.rollFsImage();
    checkpointImage.endCheckpoint();

    LOG.warn("Checkpoint done. New Image Size: " 
              + checkpointImage.getFsImageName().length());
  }

  private void startCheckpoint() throws IOException {
    checkpointImage.unlockAll();
    checkpointImage.getEditLog().close();
    checkpointImage.recoverCreate(checkpointDirs, checkpointEditsDirs);
    checkpointImage.startCheckpoint();
  }

  /**
   * Merge downloaded image and edits and write the new image into
   * current storage directory.
   */
  private void doMerge(CheckpointSignature sig) throws IOException {
    FSNamesystem namesystem = 
            new FSNamesystem(checkpointImage, conf);
    assert namesystem.dir.fsImage == checkpointImage;
    checkpointImage.doMerge(sig);
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  private int processArgs(String[] argv) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-geteditsize".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-checkpoint".equals(cmd)) {
      if (argv.length != 1 && argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && !"force".equals(argv[i])) {
        printUsage(cmd);
        return exitCode;
      }
    }

    exitCode = 0;
    try {
      if ("-checkpoint".equals(cmd)) {
        long size = namenode.getEditLogSize();
        if (size >= checkpointSize || 
            argv.length == 2 && "force".equals(argv[i])) {
          doCheckpoint();
        } else {
          System.err.println("EditLog size " + size + " bytes is " +
                             "smaller than configured checkpoint " +
                             "size " + checkpointSize + " bytes.");
          System.err.println("Skipping checkpoint.");
        }
      } else if ("-geteditsize".equals(cmd)) {
        long size = namenode.getEditLogSize();
        System.out.println("EditLog size is " + size + " bytes");
      } else {
        exitCode = -1;
        LOG.error(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        LOG.error(cmd.substring(1) + ": "
                  + content[0]);
      } catch (Exception ex) {
        LOG.error(cmd.substring(1) + ": "
                  + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = -1;
      LOG.error(cmd.substring(1) + ": "
                + e.getLocalizedMessage());
    } finally {
      // Does the RPC connection need to be closed?
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private void printUsage(String cmd) {
    if ("-geteditsize".equals(cmd)) {
      System.err.println("Usage: java SecondaryNameNode"
                         + " [-geteditsize]");
    } else if ("-checkpoint".equals(cmd)) {
      System.err.println("Usage: java SecondaryNameNode"
                         + " [-checkpoint [force]]");
    } else {
      System.err.println("Usage: java SecondaryNameNode " +
                         "[-checkpoint [force]] " +
                         "[-geteditsize] ");
    }
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
    Configuration tconf = new Configuration();
    if (argv.length >= 1) {
      SecondaryNameNode secondary = new SecondaryNameNode(tconf);
      int ret = secondary.processArgs(argv);
      System.exit(ret);
    }

    // Create a never ending deamon
    Daemon checkpointThread = new Daemon(new SecondaryNameNode(tconf)); 
    checkpointThread.start();
  }

  static class CheckpointStorage extends FSImage {
    /**
     */
    CheckpointStorage() throws IOException {
      super();
    }

    @Override
    public
    boolean isConversionNeeded(StorageDirectory sd) {
      return false;
    }

    /**
     * Analyze checkpoint directories.
     * Create directories if they do not exist.
     * Recover from an unsuccessful checkpoint is necessary. 
     * 
     * @param dataDirs
     * @param editsDirs
     * @throws IOException
     */
    void recoverCreate(Collection<File> dataDirs,
                       Collection<File> editsDirs) throws IOException {
      Collection<File> tempDataDirs = new ArrayList<File>(dataDirs);
      Collection<File> tempEditsDirs = new ArrayList<File>(editsDirs);
      this.storageDirs = new ArrayList<StorageDirectory>();
      setStorageDirectories(tempDataDirs, tempEditsDirs);
      for (Iterator<StorageDirectory> it = 
                   dirIterator(); it.hasNext();) {
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
        StorageState curState;
        try {
          curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
          // sd is locked but not opened
          switch(curState) {
          case NON_EXISTENT:
            // fail if any of the configured checkpoint dirs are inaccessible 
            throw new InconsistentFSStateException(sd.getRoot(),
                  "checkpoint directory does not exist or is not accessible.");
          case NOT_FORMATTED:
            break;  // it's ok since initially there is no current and VERSION
          case NORMAL:
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

    /**
     * Prepare directories for a new checkpoint.
     * <p>
     * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
     * and recreate <code>current</code>.
     * @throws IOException
     */
    void startCheckpoint() throws IOException {
      for(StorageDirectory sd : storageDirs) {
        File curDir = sd.getCurrentDir();
        File tmpCkptDir = sd.getLastCheckpointTmp();
        assert !tmpCkptDir.exists() : 
          tmpCkptDir.getName() + " directory must not exist.";
        if(curDir.exists()) {
          // rename current to tmp
          rename(curDir, tmpCkptDir);
        }
        if (!curDir.mkdir())
          throw new IOException("Cannot create directory " + curDir);
      }
    }

    void endCheckpoint() throws IOException {
      for(StorageDirectory sd : storageDirs) {
        File tmpCkptDir = sd.getLastCheckpointTmp();
        File prevCkptDir = sd.getPreviousCheckpoint();
        // delete previous dir
        if (prevCkptDir.exists())
          deleteDir(prevCkptDir);
        // rename tmp to previous
        if (tmpCkptDir.exists())
          rename(tmpCkptDir, prevCkptDir);
      }
    }

    /**
     * Merge image and edits, and verify consistency with the signature.
     */
    private void doMerge(CheckpointSignature sig) throws IOException {
      getEditLog().open();
      StorageDirectory sdName = null;
      StorageDirectory sdEdits = null;
      Iterator<StorageDirectory> it = null;
      it = dirIterator(NameNodeDirType.IMAGE);
      if (it.hasNext())
        sdName = it.next();
      it = dirIterator(NameNodeDirType.EDITS);
      if (it.hasNext())
        sdEdits = it.next();
      if ((sdName == null) || (sdEdits == null))
        throw new IOException("Could not locate checkpoint directories");
      loadFSImage(FSImage.getImageFile(sdName, NameNodeFile.IMAGE));
      loadFSEdits(sdEdits);
      sig.validateStorageInfo(this);
      saveFSImage();
    }
  }
}
