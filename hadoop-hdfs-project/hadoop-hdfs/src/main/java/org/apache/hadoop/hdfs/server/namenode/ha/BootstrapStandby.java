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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

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

  private String otherHttpAddr;
  private InetSocketAddress otherIpcAddr;
  private Collection<URI> dirsToFormat;
  private List<URI> editUrisToFormat;
  private List<URI> sharedEditsUris;
  private Configuration conf;
  
  private boolean force = false;
  private boolean interactive = true;
  

  public int run(String[] args) throws Exception {
    SecurityUtil.initKrb5CipherSuites();
    parseArgs(args);
    parseConfAndFindOtherNN();
    NameNode.checkAllowFormat(conf);

    InetSocketAddress myAddr = NameNode.getAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_USER_NAME_KEY, myAddr.getHostName());

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
      } else {
        printUsage();
        throw new HadoopIllegalArgumentException(
            "Illegal argument: " + arg);
      }
    }
  }

  private void printUsage() {
    System.err.println("Usage: " + this.getClass().getSimpleName() +
        "[-force] [-nonInteractive]");
  }

  private int doRun() throws IOException {
    ProxyAndInfo<NamenodeProtocol> proxyAndInfo = NameNodeProxies.createNonHAProxy(getConf(),
      otherIpcAddr, NamenodeProtocol.class,
      UserGroupInformation.getLoginUser(), true);
    NamenodeProtocol proxy = proxyAndInfo.getProxy();
    NamespaceInfo nsInfo;
    try {
      nsInfo = proxy.versionRequest();
      checkLayoutVersion(nsInfo);
    } catch (IOException ioe) {
      LOG.fatal("Unable to fetch namespace information from active NN at " +
          otherIpcAddr + ": " + ioe.getMessage());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Full exception trace", ioe);
      }
      return 1;
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
        "=====================================================");

    // Check with the user before blowing away data.
    if (!NameNode.confirmFormat(
            Sets.union(Sets.newHashSet(dirsToFormat),
                Sets.newHashSet(editUrisToFormat)),
            force, interactive)) {
      return 1;
    }

    // Force the active to roll its log
    CheckpointSignature csig = proxy.rollEditLog();
    long imageTxId = csig.getMostRecentCheckpointTxId();
    long rollTxId = csig.getCurSegmentTxId();


    // Format the storage (writes VERSION file)
    NNStorage storage = new NNStorage(conf, dirsToFormat, editUrisToFormat);
    storage.format(nsInfo);

    // Load the newly formatted image, using all of the directories (including shared
    // edits)
    FSImage image = new FSImage(conf);
    assert image.getEditLog().isOpenForRead() :
        "Expected edit log to be open for read";
    
    // Ensure that we have enough edits already in the shared directory to
    // start up from the last checkpoint on the active.
    if (!checkLogsAvailableForRead(image, imageTxId, rollTxId)) {
      return 1;
    }
    
    image.getStorage().writeTransactionIdFileToStorage(rollTxId);

    // Download that checkpoint into our storage directories.
    MD5Hash hash = TransferFsImage.downloadImageToStorage(
        otherHttpAddr.toString(), imageTxId,
        storage, true);
    image.saveDigestAndRenameCheckpointImage(imageTxId, hash);
    return 0;
  }

  private boolean checkLogsAvailableForRead(FSImage image, long imageTxId,
      long rollTxId) {
    
    long firstTxIdInLogs = imageTxId + 1;
    long lastTxIdInLogs = rollTxId - 1;
    assert lastTxIdInLogs >= firstTxIdInLogs;
    
    try {
      Collection<EditLogInputStream> streams =
        image.getEditLog().selectInputStreams(
          firstTxIdInLogs, lastTxIdInLogs, false);
      for (EditLogInputStream stream : streams) {
        IOUtils.closeStream(stream);
      }
      return true;
    } catch (IOException e) {
      String msg = "Unable to read transaction ids " +
          firstTxIdInLogs + "-" + lastTxIdInLogs +
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

  private void checkLayoutVersion(NamespaceInfo nsInfo) throws IOException {
    if (nsInfo.getLayoutVersion() != HdfsConstants.LAYOUT_VERSION) {
      throw new IOException("Layout version on remote node (" +
          nsInfo.getLayoutVersion() + ") does not match " +
          "this node's layout version (" + HdfsConstants.LAYOUT_VERSION + ")");
    }
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

    otherHttpAddr = DFSUtil.getInfoServer(null, otherNode, true);
    otherHttpAddr = DFSUtil.substituteForWildcardAddress(otherHttpAddr,
        otherIpcAddr.getHostName());
    
    
    dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    editUrisToFormat = FSNamesystem.getNamespaceEditsDirs(
        conf, false);
    sharedEditsUris = FSNamesystem.getSharedEditsDirs(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
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
