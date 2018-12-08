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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNUpgradeUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
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
  private static final Logger LOG =
      LoggerFactory.getLogger(BootstrapStandby.class);
  private String nsId;
  private String nnId;
  private List<RemoteNameNodeInfo> remoteNNs;

  private Collection<URI> dirsToFormat;
  private List<URI> editUrisToFormat;
  private List<URI> sharedEditsUris;
  private Configuration conf;
  
  private boolean force = false;
  private boolean interactive = true;
  private boolean skipSharedEditsCheck = false;

  private boolean inMemoryAliasMapEnabled;
  private String aliasMapPath;

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

    InetSocketAddress myAddr = DFSUtilClient.getNNAddress(conf);
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
    System.out.println("Usage: " + this.getClass().getSimpleName() +
        " [-force] [-nonInteractive] [-skipSharedEditsCheck]\n"
        + "\t-force: formats if the name directory exists.\n"
        + "\t-nonInteractive: formats aborts if the name directory exists,\n"
        + "\tunless -force option is specified.\n"
        + "\t-skipSharedEditsCheck: skips edits check which ensures that\n"
        + "\twe have enough edits already in the shared directory to start\n"
        + "\tup from the last checkpoint on the active.");
  }

  private NamenodeProtocol createNNProtocolProxy(InetSocketAddress otherIpcAddr)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(getConf(),
        otherIpcAddr, NamenodeProtocol.class,
        UserGroupInformation.getLoginUser(), true)
        .getProxy();
  }
  
  private int doRun() throws IOException {
    // find the active NN
    NamenodeProtocol proxy = null;
    NamespaceInfo nsInfo = null;
    boolean isUpgradeFinalized = false;
    RemoteNameNodeInfo proxyInfo = null;
    for (int i = 0; i < remoteNNs.size(); i++) {
      proxyInfo = remoteNNs.get(i);
      InetSocketAddress otherIpcAddress = proxyInfo.getIpcAddress();
      proxy = createNNProtocolProxy(otherIpcAddress);
      try {
        // Get the namespace from any active NN. If you just formatted the primary NN and are
        // bootstrapping the other NNs from that layout, it will only contact the single NN.
        // However, if there cluster is already running and you are adding a NN later (e.g.
        // replacing a failed NN), then this will bootstrap from any node in the cluster.
        nsInfo = proxy.versionRequest();
        isUpgradeFinalized = proxy.isUpgradeFinalized();
        break;
      } catch (IOException ioe) {
        LOG.warn("Unable to fetch namespace information from remote NN at " + otherIpcAddress
            + ": " + ioe.getMessage());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Full exception trace", ioe);
        }
      }
    }

    if (nsInfo == null) {
      LOG.error(
          "Unable to fetch namespace information from any remote NN. Possible NameNodes: "
              + remoteNNs);
      return ERR_CODE_FAILED_CONNECT;
    }

    if (!checkLayoutVersion(nsInfo)) {
      LOG.error("Layout version on remote node (" + nsInfo.getLayoutVersion()
          + ") does not match " + "this node's layout version ("
          + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + ")");
      return ERR_CODE_INVALID_VERSION;
    }

    System.out.println(
        "=====================================================\n" +
        "About to bootstrap Standby ID " + nnId + " from:\n" +
        "           Nameservice ID: " + nsId + "\n" +
        "        Other Namenode ID: " + proxyInfo.getNameNodeID() + "\n" +
        "  Other NN's HTTP address: " + proxyInfo.getHttpAddress() + "\n" +
        "  Other NN's IPC  address: " + proxyInfo.getIpcAddress() + "\n" +
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
    int download = downloadImage(storage, proxy, proxyInfo);
    if (download != 0) {
      return download;
    }

    // finish the upgrade: rename previous.tmp to previous
    if (!isUpgradeFinalized) {
      doUpgrade(storage);
    }

    if (inMemoryAliasMapEnabled) {
      return formatAndDownloadAliasMap(aliasMapPath, proxyInfo);
    } else {
      LOG.info("Skipping InMemoryAliasMap bootstrap as it was not configured");
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

  private int downloadImage(NNStorage storage, NamenodeProtocol proxy, RemoteNameNodeInfo proxyInfo)
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
        proxyInfo.getHttpAddress(), imageTxId, storage, true, true);
      image.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE, imageTxId,
          hash);

      // Write seen_txid to the formatted image directories.
      storage.writeTransactionIdFileToStorage(imageTxId, NameNodeDirType.IMAGE);
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      image.close();
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
      LOG.error(msg, e);

      return false;
    }
  }

  private boolean checkLayoutVersion(NamespaceInfo nsInfo) throws IOException {
    return (nsInfo.getLayoutVersion() == HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
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


    remoteNNs = RemoteNameNodeInfo.getRemoteNameNodes(conf, nsId);
    // validate the configured NNs
    List<RemoteNameNodeInfo> remove = new ArrayList<RemoteNameNodeInfo>(remoteNNs.size());
    for (RemoteNameNodeInfo info : remoteNNs) {
      InetSocketAddress address = info.getIpcAddress();
      LOG.info("Found nn: " + info.getNameNodeID() + ", ipc: " + info.getIpcAddress());
      if (address.getPort() == 0 || address.getAddress().isAnyLocalAddress()) {
        LOG.error("Could not determine valid IPC address for other NameNode ("
            + info.getNameNodeID() + ") , got: " + address);
        remove.add(info);
      }
    }

    // remove any invalid nns
    remoteNNs.removeAll(remove);

    // make sure we have at least one left to read
    Preconditions.checkArgument(!remoteNNs.isEmpty(), "Could not find any valid namenodes!");

    dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    editUrisToFormat = FSNamesystem.getNamespaceEditsDirs(
        conf, false);
    sharedEditsUris = FSNamesystem.getSharedEditsDirs(conf);

    parseProvidedConfigurations(conf);
  }

  private void parseProvidedConfigurations(Configuration configuration)
      throws IOException {
    // if provided and in-memory aliasmap are enabled,
    // get the aliasmap location.
    boolean providedEnabled = configuration.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED,
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT);
    boolean inmemoryAliasmapConfigured = configuration.getBoolean(
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED,
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED_DEFAULT);
    if (providedEnabled && inmemoryAliasmapConfigured) {
      inMemoryAliasMapEnabled = true;
      aliasMapPath = configuration.get(
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR);
    } else {
      inMemoryAliasMapEnabled = false;
      aliasMapPath = null;
    }
  }

  /**
   * A storage directory for aliasmaps. This is primarily used for the
   * StorageDirectory#hasSomeData for formatting aliasmap directories.
   */
  private static class AliasMapStorageDirectory extends StorageDirectory {

    AliasMapStorageDirectory(File aliasMapDir) {
      super(aliasMapDir);
    }

    @Override
    public String toString() {
      return "AliasMap directory = " + this.getRoot();
    }
  }

  /**
   * Format, if needed, and download the aliasmap.
   * @param pathAliasMap the path where the aliasmap should be downloaded.
   * @param proxyInfo remote namenode to get the aliasmap from.
   * @return 0 on a successful transfer, and error code otherwise.
   * @throws IOException
   */
  private int formatAndDownloadAliasMap(String pathAliasMap,
      RemoteNameNodeInfo proxyInfo) throws IOException {
    LOG.info("Bootstrapping the InMemoryAliasMap from "
        + proxyInfo.getHttpAddress());
    if (pathAliasMap == null) {
      throw new IOException("InMemoryAliasMap enabled with null location");
    }
    File aliasMapFile = new File(pathAliasMap);
    if (aliasMapFile.exists()) {
      AliasMapStorageDirectory aliasMapSD =
          new AliasMapStorageDirectory(aliasMapFile);
      if (!Storage.confirmFormat(
          Arrays.asList(aliasMapSD), force, interactive)) {
        return ERR_CODE_ALREADY_FORMATTED;
      } else {
        if (!FileUtil.fullyDelete(aliasMapFile)) {
          throw new IOException(
              "Cannot remove current alias map: " + aliasMapFile);
        }
      }
    }

    // create the aliasmap location.
    if (!aliasMapFile.mkdirs()) {
      throw new IOException("Cannot create directory " + aliasMapFile);
    }
    TransferFsImage.downloadAliasMap(proxyInfo.getHttpAddress(), aliasMapFile,
        true);
    return 0;
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
