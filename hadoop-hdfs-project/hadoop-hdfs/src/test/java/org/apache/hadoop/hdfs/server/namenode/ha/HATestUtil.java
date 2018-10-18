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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;

import com.google.common.base.Supplier;

/**
 * Static utility functions useful for testing HA.
 */
public abstract class HATestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HATestUtil.class);
  
  private static final String LOGICAL_HOSTNAME = "ha-nn-uri-%d";
  
  /**
   * Trigger an edits log roll on the active and then wait for the standby to
   * catch up to all the edits done by the active. This method will check
   * repeatedly for up to NN_LAG_TIMEOUT milliseconds, and then fail throwing
   * {@link CouldNotCatchUpException}
   * 
   * @param active active NN
   * @param standby standby NN which should catch up to active
   * @throws IOException if an error occurs rolling the edit log
   * @throws CouldNotCatchUpException if the standby doesn't catch up to the
   *         active in NN_LAG_TIMEOUT milliseconds
   */
  public static void waitForStandbyToCatchUp(NameNode active,
      NameNode standby) throws InterruptedException, IOException, CouldNotCatchUpException {
    long activeTxId = active.getNamesystem().getFSImage().getEditLog()
      .getLastWrittenTxId();

    active.getRpcServer().rollEditLog();

    long start = Time.now();
    while (Time.now() - start < TestEditLogTailer.NN_LAG_TIMEOUT) {
      long nn2HighestTxId = standby.getNamesystem().getFSImage()
        .getLastAppliedTxId();
      if (nn2HighestTxId >= activeTxId) {
        return;
      }
      Thread.sleep(TestEditLogTailer.SLEEP_TIME);
    }
    throw new CouldNotCatchUpException("Standby did not catch up to txid " +
        activeTxId + " (currently at " +
        standby.getNamesystem().getFSImage().getLastAppliedTxId() + ")");
  }

  /**
   * Wait for the datanodes in the cluster to process any block
   * deletions that have already been asynchronously queued.
   */
  public static void waitForDNDeletions(final MiniDFSCluster cluster)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        for (DataNode dn : cluster.getDataNodes()) {
          if (cluster.getFsDatasetTestUtils(dn).getPendingAsyncDeletions() > 0) {
            return false;
          }
        }
        return true;
      }
    }, 1000, 10000);
    
  }

  /**
   * Wait for the NameNode to issue any deletions that are already
   * pending (i.e. for the pendingDeletionBlocksCount to go to 0)
   */
  public static void waitForNNToIssueDeletions(final NameNode nn)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("Waiting for NN to issue block deletions to DNs");
        return nn.getNamesystem().getBlockManager().getPendingDeletionBlocksCount() == 0;
      }
    }, 250, 10000);
  }

  public static class CouldNotCatchUpException extends IOException {
    private static final long serialVersionUID = 1L;

    public CouldNotCatchUpException(String message) {
      super(message);
    }
  }
  
  /** Gets the filesystem instance by setting the failover configurations */
  public static DistributedFileSystem configureFailoverFs(
      MiniDFSCluster cluster, Configuration conf)
      throws IOException, URISyntaxException {
    return configureFailoverFs(cluster, conf, 0);
  }

  /** 
   * Gets the filesystem instance by setting the failover configurations
   * @param cluster the single process DFS cluster
   * @param conf cluster configuration
   * @param nsIndex namespace index starting with zero
   * @throws IOException if an error occurs rolling the edit log
   */
  public static DistributedFileSystem configureFailoverFs(
      MiniDFSCluster cluster, Configuration conf,
      int nsIndex) throws IOException, URISyntaxException {
    conf = new Configuration(conf);
    String logicalName = getLogicalHostname(cluster);
    setFailoverConfigurations(cluster, conf, logicalName, nsIndex);
    FileSystem fs = FileSystem.get(new URI("hdfs://" + logicalName), conf);
    return (DistributedFileSystem)fs;
  }
  
  public static void setFailoverConfigurations(MiniDFSCluster cluster,
      Configuration conf) {
    setFailoverConfigurations(cluster, conf, getLogicalHostname(cluster));
  }
  
  /** Sets the required configurations for performing failover of default namespace. */
  public static void setFailoverConfigurations(MiniDFSCluster cluster,
      Configuration conf, String logicalName) {
    setFailoverConfigurations(cluster, conf, logicalName, 0);
  }
  
  /** Sets the required configurations for performing failover.  */
  public static void setFailoverConfigurations(MiniDFSCluster cluster,
      Configuration conf, String logicalName, int nsIndex) {
    MiniDFSCluster.NameNodeInfo[] nns = cluster.getNameNodeInfos(nsIndex);
    List<InetSocketAddress> nnAddresses = new ArrayList<InetSocketAddress>(3);
    for (MiniDFSCluster.NameNodeInfo nn : nns) {
      nnAddresses.add(nn.nameNode.getNameNodeAddress());
    }
    setFailoverConfigurations(conf, logicalName, nnAddresses);
  }

  public static void setFailoverConfigurations(Configuration conf, String logicalName,
      InetSocketAddress ... nnAddresses){
    setFailoverConfigurations(conf, logicalName, Arrays.asList(nnAddresses));
  }

  /**
   * Sets the required configurations for performing failover
   */
  public static void setFailoverConfigurations(Configuration conf,
      String logicalName, List<InetSocketAddress> nnAddresses) {
    setFailoverConfigurations(conf, logicalName,
        Iterables.transform(nnAddresses, new Function<InetSocketAddress, String>() {

          // transform the inet address to a simple string
          @Override
          public String apply(InetSocketAddress addr) {
            return "hdfs://" + addr.getHostName() + ":" + addr.getPort();
          }
        }));
  }

  public static void setFailoverConfigurations(Configuration conf, String logicalName,
      Iterable<String> nnAddresses) {
    List<String> nnids = new ArrayList<String>();
    int i = 0;
    for (String address : nnAddresses) {
      String nnId = "nn" + (i + 1);
      nnids.add(nnId);
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, logicalName, nnId), address);
      i++;
    }
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, logicalName);
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, logicalName),
        Joiner.on(',').join(nnids));
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalName,
        ConfiguredFailoverProxyProvider.class.getName());
    conf.set("fs.defaultFS", "hdfs://" + logicalName);
  }

  public static String getLogicalHostname(MiniDFSCluster cluster) {
    return String.format(LOGICAL_HOSTNAME, cluster.getInstanceId());
  }
  
  public static URI getLogicalUri(MiniDFSCluster cluster)
      throws URISyntaxException {
    return new URI(HdfsConstants.HDFS_URI_SCHEME + "://" +
        getLogicalHostname(cluster));
  }
  
  public static void waitForCheckpoint(MiniDFSCluster cluster, int nnIdx,
      List<Integer> txids) throws InterruptedException {
    long start = Time.now();
    while (true) {
      try {
        FSImageTestUtil.assertNNHasCheckpoints(cluster, nnIdx, txids);
        return;
      } catch (AssertionError err) {
        if (Time.now() - start > 10000) {
          throw err;
        } else {
          Thread.sleep(300);
        }
      }
    }
  }
}
