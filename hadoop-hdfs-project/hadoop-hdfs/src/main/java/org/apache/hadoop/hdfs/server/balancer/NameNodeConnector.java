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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities.StreamCapability;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.annotations.VisibleForTesting;

/**
 * The class provides utilities for accessing a NameNode.
 */
@InterfaceAudience.Private
public class NameNodeConnector implements Closeable {
  private static final Log LOG = LogFactory.getLog(NameNodeConnector.class);

  public static final int DEFAULT_MAX_IDLE_ITERATIONS = 5;
  private static boolean write2IdFile = true;
  
  /** Create {@link NameNodeConnector} for the given namenodes. */
  public static List<NameNodeConnector> newNameNodeConnectors(
      Collection<URI> namenodes, String name, Path idPath, Configuration conf,
      int maxIdleIterations) throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    for (URI uri : namenodes) {
      NameNodeConnector nnc = new NameNodeConnector(name, uri, idPath,
          null, conf, maxIdleIterations);
      nnc.getKeyManager().startBlockKeyUpdater();
      connectors.add(nnc);
    }
    return connectors;
  }

  public static List<NameNodeConnector> newNameNodeConnectors(
      Map<URI, List<Path>> namenodes, String name, Path idPath,
      Configuration conf, int maxIdleIterations) throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    for (Map.Entry<URI, List<Path>> entry : namenodes.entrySet()) {
      NameNodeConnector nnc = new NameNodeConnector(name, entry.getKey(),
          idPath, entry.getValue(), conf, maxIdleIterations);
      nnc.getKeyManager().startBlockKeyUpdater();
      connectors.add(nnc);
    }
    return connectors;
  }

  @VisibleForTesting
  public static void setWrite2IdFile(boolean write2IdFile) {
    NameNodeConnector.write2IdFile = write2IdFile;
  }

  private final URI nameNodeUri;
  private final String blockpoolID;

  private final NamenodeProtocol namenode;
  private final ClientProtocol client;
  private final KeyManager keyManager;
  final AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);

  private final DistributedFileSystem fs;
  private final Path idPath;
  private final OutputStream out;
  private final List<Path> targetPaths;
  private final AtomicLong bytesMoved = new AtomicLong();

  private final int maxNotChangedIterations;
  private int notChangedIterations = 0;

  public NameNodeConnector(String name, URI nameNodeUri, Path idPath,
                           List<Path> targetPaths, Configuration conf,
                           int maxNotChangedIterations)
      throws IOException {
    this.nameNodeUri = nameNodeUri;
    this.idPath = idPath;
    this.targetPaths = targetPaths == null || targetPaths.isEmpty() ? Arrays
        .asList(new Path("/")) : targetPaths;
    this.maxNotChangedIterations = maxNotChangedIterations;

    this.namenode = NameNodeProxies.createProxy(conf, nameNodeUri,
        NamenodeProtocol.class).getProxy();
    this.client = NameNodeProxies.createProxy(conf, nameNodeUri,
        ClientProtocol.class, fallbackToSimpleAuth).getProxy();
    this.fs = (DistributedFileSystem)FileSystem.get(nameNodeUri, conf);

    final NamespaceInfo namespaceinfo = namenode.versionRequest();
    this.blockpoolID = namespaceinfo.getBlockPoolID();

    final FsServerDefaults defaults = fs.getServerDefaults(new Path("/"));
    this.keyManager = new KeyManager(blockpoolID, namenode,
        defaults.getEncryptDataTransfer(), conf);
    // if it is for test, we do not create the id file
    out = checkAndMarkRunning();
    if (out == null) {
      // Exit if there is another one running.
      throw new IOException("Another " + name + " is running.");
    }
  }

  public DistributedFileSystem getDistributedFileSystem() {
    return fs;
  }

  /** @return the block pool ID */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  AtomicLong getBytesMoved() {
    return bytesMoved;
  }

  /** @return blocks with locations. */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize)
      throws IOException {
    return namenode.getBlocks(datanode, size, minBlockSize);
  }

  /**
   * @return true if an upgrade is in progress, false if not.
   * @throws IOException
   */
  public boolean isUpgrading() throws IOException {
    // fsimage upgrade
    final boolean isUpgrade = !namenode.isUpgradeFinalized();
    // rolling upgrade
    RollingUpgradeInfo info = fs.rollingUpgrade(
        HdfsConstants.RollingUpgradeAction.QUERY);
    final boolean isRollingUpgrade = (info != null && !info.isFinalized());
    return (isUpgrade || isRollingUpgrade);
  }

  /** @return live datanode storage reports. */
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    return client.getDatanodeStorageReport(DatanodeReportType.LIVE);
  }

  /** @return the key manager */
  public KeyManager getKeyManager() {
    return keyManager;
  }

  /** @return the list of paths to scan/migrate */
  public List<Path> getTargetPaths() {
    return targetPaths;
  }

  /** Should the instance continue running? */
  public boolean shouldContinue(long dispatchBlockMoveBytes) {
    if (dispatchBlockMoveBytes > 0) {
      notChangedIterations = 0;
    } else {
      notChangedIterations++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("No block has been moved for " +
            notChangedIterations + " iterations, " +
            "maximum notChangedIterations before exit is: " +
            ((maxNotChangedIterations >= 0) ? maxNotChangedIterations : "Infinite"));
      }
      if ((maxNotChangedIterations >= 0) &&
          (notChangedIterations >= maxNotChangedIterations)) {
        System.out.println("No block has been moved for "
            + notChangedIterations + " iterations. Exiting...");
        return false;
      }
    }
    return true;
  }
  

  /**
   * The idea for making sure that there is no more than one instance
   * running in an HDFS is to create a file in the HDFS, writes the hostname
   * of the machine on which the instance is running to the file, but did not
   * close the file until it exits. 
   * 
   * This prevents the second instance from running because it can not
   * creates the file while the first one is running.
   * 
   * This method checks if there is any running instance. If no, mark yes.
   * Note that this is an atomic operation.
   * 
   * @return null if there is a running instance;
   *         otherwise, the output stream to the newly created file.
   */
  private OutputStream checkAndMarkRunning() throws IOException {
    try {
      if (fs.exists(idPath)) {
        // try appending to it so that it will fail fast if another balancer is
        // running.
        IOUtils.closeStream(fs.append(idPath));
        fs.delete(idPath, true);
      }

      final FSDataOutputStream fsout = fs.createFile(idPath)
          .replicate().recursive().build();

      Preconditions.checkState(
          fsout.hasCapability(StreamCapability.HFLUSH.getValue())
          && fsout.hasCapability(StreamCapability.HSYNC.getValue()),
          "Id lock file should support hflush and hsync");

      // mark balancer idPath to be deleted during filesystem closure
      fs.deleteOnExit(idPath);
      if (write2IdFile) {
        fsout.writeBytes(InetAddress.getLocalHost().getHostName());
        fsout.hflush();
      }
      return fsout;
    } catch(RemoteException e) {
      if(AlreadyBeingCreatedException.class.getName().equals(e.getClassName())){
        return null;
      } else {
        throw e;
      }
    }
  }

  @Override
  public void close() {
    keyManager.close();

    // close the output file
    IOUtils.closeStream(out); 
    if (fs != null) {
      try {
        fs.delete(idPath, true);
      } catch(IOException ioe) {
        LOG.warn("Failed to delete " + idPath, ioe);
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[namenodeUri=" + nameNodeUri
        + ", bpid=" + blockpoolID + "]";
  }
}
