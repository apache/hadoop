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

import org.apache.commons.logging.*;

import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.apache.hadoop.net.NetUtils;

import java.io.*;
import java.net.*;
import java.util.Map;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
public class SecondaryNameNode implements FSConstants, Runnable {
    
  public static final Log LOG = LogFactory.getLog(
                                                  "org.apache.hadoop.dfs.NameNode.Secondary");
  private static final String SRC_FS_IMAGE = "srcimage.tmp";
  private static final String FS_EDITS = "edits.tmp";
  private static final String DEST_FS_IMAGE = "destimage.tmp";

  private ClientProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private boolean shouldRun;
  private StatusHttpServer infoServer;
  private int infoPort;
  private String infoBindAddress;

  private File checkpointDir;
  private long checkpointPeriod;	// in seconds
  private long checkpointSize;    // size (in MB) of current Edit Log
  private File srcImage;
  private File destImage;
  private File editFile;

  private boolean[] simulation = null; // error simulation events

  /**
   * Create a connection to the primary namenode.
   */
  public SecondaryNameNode(Configuration conf)  throws IOException {

    // initiate Java VM metrics
    JvmMetrics.init("SecondaryNameNode", conf.get("session.id"));
    
    //
    // initialize error simulation code for junit test
    //
    initializeErrorSimulationEvent(2);

    //
    // Create connection to the namenode.
    //
    shouldRun = true;
    nameNodeAddr = NetUtils.createSocketAddr(
                              conf.get("fs.default.name", "local"));
    this.conf = conf;
    this.namenode =
        (ClientProtocol) RPC.waitForProxy(ClientProtocol.class,
            ClientProtocol.versionID, nameNodeAddr, conf);

    //
    // initialize the webserver for uploading files.
    //
    String infoAddr = conf.get("dfs.secondary.http.bindAddress", 
                                "0.0.0.0:50090");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new StatusHttpServer("dfs", infoBindAddress, tmpInfoPort, 
                                      tmpInfoPort == 0);
    infoServer.setAttribute("name.secondary", this);
    this.infoServer.setAttribute("name.conf", conf);
    infoServer.addServlet("getimage", "/getimage", GetImageServlet.class);
    infoServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    conf.set("dfs.secondary.http.bindAddress", infoBindAddress + ":" +infoPort); 
    LOG.info("Secondary Web-server up at: " 
              + conf.get("dfs.secondary.http.bindAddress"));

    //
    // Initialize other scheduling parameters from the configuration
    //
    String[] dirName = conf.getStrings("fs.checkpoint.dir");
    checkpointDir = new File(dirName[0]);
    checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    checkpointSize = conf.getLong("fs.checkpoint.size", 4194304);
    doSetup();

    LOG.warn("Checkpoint Directory:" + checkpointDir);
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
      infoServer.stop();
    } catch (Exception e) {
    }
  }

  private void doSetup() throws IOException {
    //
    // Create the checkpoint directory if needed. 
    //
    checkpointDir.mkdirs();
    srcImage = new File(checkpointDir, SRC_FS_IMAGE);
    destImage = new File(checkpointDir, DEST_FS_IMAGE);
    editFile = new File(checkpointDir, FS_EDITS);
    srcImage.delete();
    destImage.delete();
    editFile.delete();
  }

  File getNewImage() {
    return destImage;
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
        LOG.error("Exception in doCheckpoint:");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
      } catch (Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint:");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  /**
   * get the current fsimage from Namenode.
   */
  private void getFSImage() throws IOException {
    String fsName = getInfoServer();
    String fileid = "getimage=1";
    TransferFsImage.getFileClient(fsName, fileid, srcImage);
    LOG.info("Downloaded file " + srcImage + " size " +
             srcImage.length() + " bytes.");
  }

  /**
   * get the old edits file from the NameNode
   */
  private void getFSEdits() throws IOException {
    String fsName = getInfoServer();
    String fileid = "getedit=1";
    TransferFsImage.getFileClient(fsName, fileid, editFile);
    LOG.info("Downloaded file " + editFile + " size " +
             editFile.length() + " bytes.");
  }

  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(long token) throws IOException {
    String fsName = getInfoServer();
    String fileid = "putimage=1&port=" + infoPort +
      "&machine=" +
      InetAddress.getLocalHost().getHostAddress() +
      "&token=" + token;
    LOG.info("Posted URL " + fsName + fileid);
    TransferFsImage.getFileClient(fsName, fileid, (File[])null);
  }

  /*
   * Returns the Jetty server that the Namenode is listening on.
   */
  private String getInfoServer() throws IOException {
    String fsName = conf.get("fs.default.name", "local");
    if (fsName.equals("local")) {
      throw new IOException("This is not a DFS");
    }
    return conf.get("dfs.http.bindAddress", "0.0.0.0:50070");
  }

  /*
   * Create a new checkpoint
   */
  void doCheckpoint() throws IOException {

    //
    // Do the required initialization of the merge work area.
    //
    doSetup();

    //
    // Tell the namenode to start logging transactions in a new edit file
    // Retuns a token that would be used to upload the merged image.
    //
    long token = namenode.rollEditLog();

    //
    // error simulation code for junit test
    //
    if (simulation != null && simulation[0]) {
      throw new IOException("Simulating error0 " +
                            "after creating edits.new");
    }

    getFSImage();                // Fetch fsimage
    getFSEdits();                // Fetch edist
    doMerge();                   // Do the merge
  
    //
    // Upload the new image into the NameNode. Then tell the Namenode
    // to make this new uploaded image as the most current image.
    //
    putFSImage(token);

    //
    // error simulation code for junit test
    //
    if (simulation != null && simulation[1]) {
      throw new IOException("Simulating error1 " +
                            "after uploading new image to NameNode");
    }

    namenode.rollFsImage();

    LOG.warn("Checkpoint done. Image Size:" + srcImage.length() +
             " Edit Size:" + editFile.length() +
             " New Image Size:" + destImage.length());
  }

  /**
   * merges SRC_FS_IMAGE with FS_EDITS and writes the output into
   * DEST_FS_IMAGE
   */
  private void doMerge() throws IOException {
    FSNamesystem namesystem = 
            new FSNamesystem(new FSImage(checkpointDir), conf);
    FSImage fsImage = namesystem.dir.fsImage;
    fsImage.loadFSImage(srcImage);
    fsImage.getEditLog().loadFSEdits(editFile);
    fsImage.saveFSImage(destImage);
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

  //
  // utility method to facilitate junit test error simulation
  //
  void initializeErrorSimulationEvent(int numberOfEvents) {
    simulation = new boolean[numberOfEvents]; 
    for (int i = 0; i < numberOfEvents; i++) {
      simulation[i] = false;
    }
  }

  void setErrorSimulation(int index) {
    assert(index < simulation.length);
    simulation[index] = true;
  }

  void clearErrorSimulation(int index) {
    assert(index < simulation.length);
    simulation[index] = false;
  }

  /**
   * This class is used in Namesystem's jetty to retrieve a file.
   * Typically used by the Secondary NameNode to retrieve image and
   * edit file for periodic checkpointing.
   */
  public static class GetImageServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      Map<String,String[]> pmap = request.getParameterMap();
      try {
        ServletContext context = getServletContext();
        SecondaryNameNode nn = (SecondaryNameNode) 
          context.getAttribute("name.secondary");
        TransferFsImage ff = new TransferFsImage(pmap, request, response);
        if (ff.getImage()) {
          TransferFsImage.getFileServer(response.getOutputStream(),
                                        nn.getNewImage());
        }
        LOG.info("New Image " + nn.getNewImage() + " retrieved by Namenode.");
      } catch (IOException ie) {
        StringUtils.stringifyException(ie);
        LOG.error(ie);
        String errMsg = "GetImage failed.";
        response.sendError(HttpServletResponse.SC_GONE, errMsg);
        throw ie;

      }
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
}
