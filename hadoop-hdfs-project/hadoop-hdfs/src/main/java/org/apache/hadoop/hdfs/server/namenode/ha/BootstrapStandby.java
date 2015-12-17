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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNUpgradeUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Tool which allows the standby node's storage directories to be bootstrapped
 * by copying the latest namespace snapshot from the active namenode. This is
 * used when first configuring an HA cluster.
 */
@InterfaceAudience.Private
public class BootstrapStandby implements Tool, Configurable {
  private static final Log LOG = LogFactory.getLog(BootstrapStandby.class);
  private String nsId;
  private String nnId;
  private String otherNNId;

  private URL otherHttpAddr;
  private InetSocketAddress otherIpcAddr;
  private Collection<URI> dirsToFormat;
  private List<URI> editUrisToFormat;
  private List<URI> sharedEditsUris;
  private Configuration conf;
  
  private boolean force = false;
  private boolean interactive = true;
  private boolean skipSharedEditsCheck = false;

  // Exit/return codes.
  static final int ERR_CODE_FAILED_CONNECT = 2;
  static final int ERR_CODE_INVALID_VERSION = 3;
  // Skip 4 - was used in previous versions, but no longer returned.
  static final int ERR_CODE_ALREADY_FORMATTED = 5;
  static final int ERR_CODE_LOGS_UNAVAILABLE = 6; 

  @Override
  public int run(String[] args) throws Exception {
    parseArgs(args);
    parseConfAndFindOtherNN();
    NameNode.checkAllowFormat(conf);

    InetSocketAddress myAddr = NameNode.getAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, myAddr.getHostName());

    return SecurityUtil.doAsLoginUserOrFatal(new PrivilegedAction<Integer>() {
      @Override
      public Integer run() {
        try {
          return doRun();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
  
  private void parseArgs(String[] args) {
    for (String arg : args) {
      if ("-force".equals(arg)) {
        force = true;
      } else if ("-nonInteractive".equals(arg)) {
        interactive = false;
      } else if ("-skipSharedEditsCheck".equals(arg)) {
        skipSharedEditsCheck = true;
      } else {
        printUsage();
        throw new HadoopIllegalArgumentException(
            "Illegal argument: " + arg);
      }
    }
  }

  private void printUsage() {
    System.err.println("Usage: " + this.getClass().getSimpleName() +
        " [-force] [-nonInteractive] [-skipSharedEditsCheck]");
  }
  
  private NamenodeProtocol createNNProtocolProxy()
      throws IOException {
    return NameNodeProxies.createNonHAProxy(getConf(),
        otherIpcAddr, NamenodeProtocol.class,
        UserGroupInformation.getLoginUser(), true)
        .getProxy();
  }
  
  private int doRun() throws IOException {
    NamenodeProtocol proxy = createNNProtocolProxy();
    NamespaceInfo nsInfo;
    boolean isUpgradeFinalized;
    try {
      nsInfo = proxy.versionRequest();
      isUpgradeFinalized = proxy.isUpgradeFinalized();
    } catch (IOException ioe) {
      LOG.fatal("Unable to fetch namespace information from active NN at " +
          otherIpcAddr + ": " + ioe.getMessage());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Full exception trace", ioe);
      }
      return ERR_CODE_FAILED_CONNECT;
    }

    if (!checkLayoutVersion(nsInfo)) {
      LOG.fatal("Layout version on remote node (" + nsInfo.getLayoutVersion()
          + ") does not match " + "this node's layout version ("
          + HdfsConstants.NAMENODE_LAYOUT_VERSION + ")");
      return ERR_CODE_INVALID_VERSION;
    }

    System.out.println(
        "=====================================================\n" +
        "About to bootstrap Standby ID " + nnId + " from:\n" +
        "           Nameservice ID: " + nsId + "\n" +
        "        Other Namenode ID: " + otherNNId + "\n" +
        "  Other NN's HTTP address: " + otherHttpAddr + "\n" +
        "  Other NN's IPC  address: " + otherIpcAddr + "\n" +
        "             Namespace ID: " + nsInfo.getNamespaceID() + "\n" +
        "            Block pool ID: " + nsInfo.getBlockPoolID() + "\n" +
        "               Cluster ID: " + nsInfo.getClusterID() + "\n" +
        "           Layout version: " + nsInfo.getLayoutVersion() + "\n" +
        "       isUpgradeFinalized: " + isUpgradeFinalized + "\n" +
        "=====================================================");
    
    NNStorage storage = new NNStorage(conf, dirsToFormat, editUrisToFormat);

    if (!isUpgradeFinalized) {
      // the remote NameNode is in upgrade state, this NameNode should also
      // create the previous directory. First prepare the upgrade and rename
      // the current dir to previous.tmp.
      LOG.info("The active NameNode is in Upgrade. " +
          "Prepare the upgrade for the standby NameNode as well.");
      if (!doPreUpgrade(storage, nsInfo)) {
        return ERR_CODE_ALREADY_FORMATTED;
      }
    } else if (!format(storage, nsInfo)) { // prompt the user to format storage
      return ERR_CODE_ALREADY_FORMATTED;
    }

    // download the fsimage from active namenode
    int download = downloadImage(storage, proxy);
    if (download != 0) {
      return download;
    }

    // finish the upgrade: rename previous.tmp to previous
    if (!isUpgradeFinalized) {
      doUpgrade(storage);
    }
    return 0;
  }

  /**
   * Iterate over all the storage directories, checking if it should be
   * formatted. Format the storage if necessary and allowed by the user.
   * @return True if formatting is processed
   */
  private boolean format(NNStorage storage, NamespaceInfo nsInfo)
      throws IOException {
    // Check with the user before blowing away data.
    if (!Storage.confirmFormat(storage.dirIterable(null), force, interactive)) {
      storage.close();
      return false;
    } else {
      // Format the storage (writes VERSION file)
      storage.format(nsInfo);
      return true;
    }
  }

  /**
   * This is called when using bootstrapStandby for HA upgrade. The SBN should
   * also create previous directory so that later when it starts, it understands
   * that the cluster is in the upgrade state. This function renames the old
   * current directory to previous.tmp.
   */
  private boolean doPreUpgrade(NNStorage storage, NamespaceInfo nsInfo)
      throws IOException {
    boolean isFormatted = false;
    Map<StorageDirectory, StorageState> dataDirStates =
        new HashMap<>();
    try {
      isFormatted = FSImage.recoverStorageDirs(StartupOption.UPGRADE, storage,
          dataDirStates);
      if (dataDirStates.values().contains(StorageState.NOT_FORMATTED)) {
        // recoverStorageDirs returns true if there is a formatted directory
        isFormatted = false;
        System.err.println("The original storage directory is not formatted.");
      }
    } catch (InconsistentFSStateException e) {
      // if the storage is in a bad state,
      LOG.warn("The storage directory is in an inconsistent state", e);
    } finally {
      storage.unlockAll();
    }

    // if there is InconsistentFSStateException or the storage is not formatted,
    // format the storage. Although this format is done through the new
    // software, since in HA setup the SBN is rolled back through
    // "-bootstrapStandby", we should still be fine.
    if (!isFormatted && !format(storage, nsInfo)) {
      return false;
    }

    // make sure there is no previous directory
    FSImage.checkUpgrade(storage);
    // Do preUpgrade for each directory
    for (Iterator<StorageDirectory> it = storage.dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        NNUpgradeUtil.renameCurToTmp(sd);
      } catch (IOException e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        throw e;
      }
    }
    storage.setStorageInfo(nsInfo);
    storage.setBlockPoolID(nsInfo.getBlockPoolID());
    return true;
  }

  private void doUpgrade(NNStorage storage) throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      NNUpgradeUtil.doUpgrade(sd, storage);
    }
  }

  private int downloadImage(NNStorage storage, NamenodeProtocol proxy)
      throws IOException {
    // Load the newly formatted image, using all of the directories
    // (including shared edits)
    final long imageTxId = proxy.getMostRecentCheckpointTxId();
    final long curTxId = proxy.getTransactionID();
    FSImage image = new FSImage(conf);
    try {
      image.getStorage().setStorageInfo(storage);
      image.initEditLog(StartupOption.REGULAR);
      assert image.getEditLog().isOpenForRead() :
          "Expected edit log to be open for read";

      // Ensure that we have enough edits already in the shared directory to
      // start up from the last checkpoint on the active.
      if (!skipSharedEditsCheck &&
          !checkLogsAvailableForRead(image, imageTxId, curTxId)) {
        return ERR_CODE_LOGS_UNAVAILABLE;
      }

      // Download that checkpoint into our storage directories.
      MD5Hash hash = TransferFsImage.downloadImageToStorage(
          otherHttpAddr, imageTxId, storage, true);
      image.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE, imageTxId,
          hash);

      // Write seen_txid to the formatted image directories.
      storage.writeTransactionIdFileToStorage(imageTxId, NameNodeDirType.IMAGE);
    } catch (IOException ioe) {
      image.close();
      throw ioe;
    }
    return 0;
  }

  private boolean checkLogsAvailableForRead(FSImage image, long imageTxId,
      long curTxIdOnOtherNode) {

    if (imageTxId == curTxIdOnOtherNode) {
      // The other node hasn't written any logs since the last checkpoint.
      // This can be the case if the NN was freshly formatted as HA, and
      // then started in standby mode, so it has no edit logs at all.
      return true;
    }
    long firstTxIdInLogs = imageTxId + 1;
    
    assert curTxIdOnOtherNode >= firstTxIdInLogs :
      "first=" + firstTxIdInLogs + " onOtherNode=" + curTxIdOnOtherNode;
    
    try {
      Collection<EditLogInputStream> streams =
        image.getEditLog().selectInputStreams(
          firstTxIdInLogs, curTxIdOnOtherNode, null, true);
      for (EditLogInputStream stream : streams) {
        IOUtils.closeStream(stream);
      }
      return true;
    } catch (IOException e) {
      String msg = "Unable to read transaction ids " +
          firstTxIdInLogs + "-" + curTxIdOnOtherNode +
          " from the configured shared edits storage " +
          Joiner.on(",").join(sharedEditsUris) + ". " +
          "Please copy these logs into the shared edits storage " + 
          "or call saveNamespace on the active node.\n" +
          "Error: " + e.getLocalizedMessage();
      if (LOG.isDebugEnabled()) {
        LOG.fatal(msg, e);
      } else {
        LOG.fatal(msg);
      }
      return false;
    }
  }

  private boolean checkLayoutVersion(NamespaceInfo nsInfo) throws IOException {
    return (nsInfo.getLayoutVersion() == HdfsConstants.NAMENODE_LAYOUT_VERSION);
  }
  
  private void parseConfAndFindOtherNN() throws IOException {
    Configuration conf = getConf();
    nsId = DFSUtil.getNamenodeNameServiceId(conf);

    if (!HAUtil.isHAEnabled(conf, nsId)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this namenode.");
    }
    nnId = HAUtil.getNameNodeId(conf, nsId);
    NameNode.initializeGenericKeys(conf, nsId, nnId);

    if (!HAUtil.usesSharedEditsDir(conf)) {
      throw new HadoopIllegalArgumentException(
        "Shared edits storage is not enabled for this namenode.");
    }
    
    Configuration otherNode = HAUtil.getConfForOtherNode(conf);
    otherNNId = HAUtil.getNameNodeId(otherNode, nsId);
    otherIpcAddr = NameNode.getServiceAddress(otherNode, true);
    Preconditions.checkArgument(otherIpcAddr.getPort() != 0 &&
        !otherIpcAddr.getAddress().isAnyLocalAddress(),
        "Could not determine valid IPC address for other NameNode (%s)" +
        ", got: %s", otherNNId, otherIpcAddr);

    final String scheme = DFSUtil.getHttpClientScheme(conf);
    otherHttpAddr = DFSUtil.getInfoServerWithDefaultHost(
        otherIpcAddr.getHostName(), otherNode, scheme).toURL();

    dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    editUrisToFormat = FSNamesystem.getNamespaceEditsDirs(
        conf, false);
    sharedEditsUris = FSNamesystem.getSharedEditsDirs(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = DFSHAAdmin.addSecurityConfiguration(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public static int run(String[] argv, Configuration conf) throws IOException {
    BootstrapStandby bs = new BootstrapStandby();
    bs.setConf(conf);
    try {
      return ToolRunner.run(bs, argv);
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException)e;
      } else {
        throw new IOException(e);
      }
    }
  }
}
