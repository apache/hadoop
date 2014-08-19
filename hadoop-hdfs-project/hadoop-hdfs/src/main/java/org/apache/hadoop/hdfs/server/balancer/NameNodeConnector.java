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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;

/**
 * The class provides utilities for accessing a NameNode.
 */
@InterfaceAudience.Private
public class NameNodeConnector implements Closeable {
  private static final Log LOG = LogFactory.getLog(NameNodeConnector.class);

  private static final int MAX_NOT_CHANGED_ITERATIONS = 5;

  private final URI nameNodeUri;
  private final String blockpoolID;

  private final NamenodeProtocol namenode;
  private final ClientProtocol client;
  private final KeyManager keyManager;

  private final FileSystem fs;
  private final Path idPath;
  private final OutputStream out;

  private int notChangedIterations = 0;

  public NameNodeConnector(String name, URI nameNodeUri, Path idPath,
      Configuration conf) throws IOException {
    this.nameNodeUri = nameNodeUri;
    this.idPath = idPath;
    
    this.namenode = NameNodeProxies.createProxy(conf, nameNodeUri,
        NamenodeProtocol.class).getProxy();
    this.client = NameNodeProxies.createProxy(conf, nameNodeUri,
        ClientProtocol.class).getProxy();
    this.fs = FileSystem.get(nameNodeUri, conf);

    final NamespaceInfo namespaceinfo = namenode.versionRequest();
    this.blockpoolID = namespaceinfo.getBlockPoolID();

    final FsServerDefaults defaults = fs.getServerDefaults(new Path("/"));
    this.keyManager = new KeyManager(blockpoolID, namenode,
        defaults.getEncryptDataTransfer(), conf);
    // Exit if there is another one running.
    out = checkAndMarkRunning(); 
    if (out == null) {
      throw new IOException("Another " + name + " is running.");
    }
  }

  /** @return the block pool ID */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  /** @return blocks with locations. */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException {
    return namenode.getBlocks(datanode, size);
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

  /** Should the instance continue running? */
  public boolean shouldContinue(long dispatchBlockMoveBytes) {
    if (dispatchBlockMoveBytes > 0) {
      notChangedIterations = 0;
    } else {
      notChangedIterations++;
      if (notChangedIterations >= MAX_NOT_CHANGED_ITERATIONS) {
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
      final DataOutputStream out = fs.create(idPath);
      out.writeBytes(InetAddress.getLocalHost().getHostName());
      out.flush();
      return out;
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
