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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.HdfsVolumeId;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class BlockStorageLocationUtil {
  
  private static final Log LOG = LogFactory
      .getLog(BlockStorageLocationUtil.class);
  
  /**
   * Create a list of {@link VolumeBlockLocationCallable} corresponding to a set
   * of datanodes and blocks.
   * 
   * @param datanodeBlocks
   *          Map of datanodes to block replicas at each datanode
   * @return callables Used to query each datanode for location information on
   *         the block replicas at the datanode
   */
  private static List<VolumeBlockLocationCallable> createVolumeBlockLocationCallables(
      Configuration conf, Map<DatanodeInfo, List<LocatedBlock>> datanodeBlocks,
      int timeout, boolean connectToDnViaHostname) {
    // Construct the callables, one per datanode
    List<VolumeBlockLocationCallable> callables = 
        new ArrayList<VolumeBlockLocationCallable>();
    for (Map.Entry<DatanodeInfo, List<LocatedBlock>> entry : datanodeBlocks
        .entrySet()) {
      // Construct RPC parameters
      DatanodeInfo datanode = entry.getKey();
      List<LocatedBlock> locatedBlocks = entry.getValue();
      List<ExtendedBlock> extendedBlocks = 
          new ArrayList<ExtendedBlock>(locatedBlocks.size());
      List<Token<BlockTokenIdentifier>> dnTokens = 
          new ArrayList<Token<BlockTokenIdentifier>>(
          locatedBlocks.size());
      for (LocatedBlock b : locatedBlocks) {
        extendedBlocks.add(b.getBlock());
        dnTokens.add(b.getBlockToken());
      }
      VolumeBlockLocationCallable callable = new VolumeBlockLocationCallable(
          conf, datanode, extendedBlocks, dnTokens, timeout, 
          connectToDnViaHostname);
      callables.add(callable);
    }
    return callables;
  }
  
  /**
   * Queries datanodes for the blocks specified in <code>datanodeBlocks</code>,
   * making one RPC to each datanode. These RPCs are made in parallel using a
   * threadpool.
   * 
   * @param datanodeBlocks
   *          Map of datanodes to the blocks present on the DN
   * @return metadatas List of block metadata for each datanode, specifying
   *         volume locations for each block
   * @throws InvalidBlockTokenException
   *           if client does not have read access on a requested block
   */
  static List<HdfsBlocksMetadata> queryDatanodesForHdfsBlocksMetadata(
      Configuration conf, Map<DatanodeInfo, List<LocatedBlock>> datanodeBlocks,
      int poolsize, int timeout, boolean connectToDnViaHostname)
      throws InvalidBlockTokenException {

    List<VolumeBlockLocationCallable> callables = 
        createVolumeBlockLocationCallables(conf, datanodeBlocks, timeout, 
            connectToDnViaHostname);
    
    // Use a thread pool to execute the Callables in parallel
    List<Future<HdfsBlocksMetadata>> futures = 
        new ArrayList<Future<HdfsBlocksMetadata>>();
    ExecutorService executor = new ScheduledThreadPoolExecutor(poolsize);
    try {
      futures = executor.invokeAll(callables, timeout, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Swallow the exception here, because we can return partial results
    }
    executor.shutdown();
    
    // Initialize metadatas list with nulls
    // This is used to later indicate if we didn't get a response from a DN
    List<HdfsBlocksMetadata> metadatas = new ArrayList<HdfsBlocksMetadata>();
    for (int i = 0; i < futures.size(); i++) {
      metadatas.add(null);
    }
    // Fill in metadatas with results from DN RPCs, where possible
    for (int i = 0; i < futures.size(); i++) {
      Future<HdfsBlocksMetadata> future = futures.get(i);
      try {
        HdfsBlocksMetadata metadata = future.get();
        metadatas.set(i, metadata);
      } catch (ExecutionException e) {
        VolumeBlockLocationCallable callable = callables.get(i);
        DatanodeInfo datanode = callable.getDatanodeInfo();
        Throwable t = e.getCause();
        if (t instanceof InvalidBlockTokenException) {
          LOG.warn("Invalid access token when trying to retrieve "
              + "information from datanode " + datanode.getIpcAddr(false));
          throw (InvalidBlockTokenException) t;
        }
        else if (t instanceof UnsupportedOperationException) {
          LOG.info("Datanode " + datanode.getIpcAddr(false) + " does not support"
              + " required #getHdfsBlocksMetadata() API");
          throw (UnsupportedOperationException) t;
        } else {
          LOG.info("Failed to connect to datanode " +
              datanode.getIpcAddr(false));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not fetch information from datanode", t);
        }
      } catch (InterruptedException e) {
        // Shouldn't happen, because invokeAll waits for all Futures to be ready
        LOG.info("Interrupted while fetching HdfsBlocksMetadata");
      }
    }
    
    return metadatas;
  }
  
  /**
   * Group the per-replica {@link VolumeId} info returned from
   * {@link DFSClient#queryDatanodesForHdfsBlocksMetadata(Map)} to be associated
   * with the corresponding {@link LocatedBlock}.
   * 
   * @param blocks
   *          Original LocatedBlock array
   * @param datanodeBlocks
   *          Mapping from datanodes to the list of replicas on each datanode
   * @param metadatas
   *          VolumeId information for the replicas on each datanode
   * @return blockVolumeIds per-replica VolumeId information associated with the
   *         parent LocatedBlock
   */
  static Map<LocatedBlock, List<VolumeId>> associateVolumeIdsWithBlocks(
      List<LocatedBlock> blocks, Map<DatanodeInfo, 
      List<LocatedBlock>> datanodeBlocks, List<HdfsBlocksMetadata> metadatas) {
    
    // Initialize mapping of ExtendedBlock to LocatedBlock. 
    // Used to associate results from DN RPCs to the parent LocatedBlock
    Map<ExtendedBlock, LocatedBlock> extBlockToLocBlock = 
        new HashMap<ExtendedBlock, LocatedBlock>();
    for (LocatedBlock b : blocks) {
      extBlockToLocBlock.put(b.getBlock(), b);
    }
    
    // Initialize the mapping of blocks -> list of VolumeIds, one per replica
    // This is filled out with real values from the DN RPCs
    Map<LocatedBlock, List<VolumeId>> blockVolumeIds = 
        new HashMap<LocatedBlock, List<VolumeId>>();
    for (LocatedBlock b : blocks) {
      ArrayList<VolumeId> l = new ArrayList<VolumeId>(b.getLocations().length);
      // Start off all IDs as invalid, fill it in later with results from RPCs
      for (int i = 0; i < b.getLocations().length; i++) {
        l.add(VolumeId.INVALID_VOLUME_ID);
      }
      blockVolumeIds.put(b, l);
    }
    
    // Iterate through the list of metadatas (one per datanode). 
    // For each metadata, if it's valid, insert its volume location information 
    // into the Map returned to the caller 
    Iterator<HdfsBlocksMetadata> metadatasIter = metadatas.iterator();
    Iterator<DatanodeInfo> datanodeIter = datanodeBlocks.keySet().iterator();
    while (metadatasIter.hasNext()) {
      HdfsBlocksMetadata metadata = metadatasIter.next();
      DatanodeInfo datanode = datanodeIter.next();
      // Check if metadata is valid
      if (metadata == null) {
        continue;
      }
      ExtendedBlock[] metaBlocks = metadata.getBlocks();
      List<byte[]> metaVolumeIds = metadata.getVolumeIds();
      List<Integer> metaVolumeIndexes = metadata.getVolumeIndexes();
      // Add VolumeId for each replica in the HdfsBlocksMetadata
      for (int j = 0; j < metaBlocks.length; j++) {
        int volumeIndex = metaVolumeIndexes.get(j);
        ExtendedBlock extBlock = metaBlocks[j];
        // Skip if block wasn't found, or not a valid index into metaVolumeIds
        // Also skip if the DN responded with a block we didn't ask for
        if (volumeIndex == Integer.MAX_VALUE
            || volumeIndex >= metaVolumeIds.size()
            || !extBlockToLocBlock.containsKey(extBlock)) {
          continue;
        }
        // Get the VolumeId by indexing into the list of VolumeIds
        // provided by the datanode
        byte[] volumeId = metaVolumeIds.get(volumeIndex);
        HdfsVolumeId id = new HdfsVolumeId(volumeId);
        // Find out which index we are in the LocatedBlock's replicas
        LocatedBlock locBlock = extBlockToLocBlock.get(extBlock);
        DatanodeInfo[] dnInfos = locBlock.getLocations();
        int index = -1;
        for (int k = 0; k < dnInfos.length; k++) {
          if (dnInfos[k].equals(datanode)) {
            index = k;
            break;
          }
        }
        if (index < 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Datanode responded with a block volume id we did" +
                " not request, omitting.");
          }
          continue;
        }
        // Place VolumeId at the same index as the DN's index in the list of
        // replicas
        List<VolumeId> volumeIds = blockVolumeIds.get(locBlock);
        volumeIds.set(index, id);
      }
    }
    return blockVolumeIds;
  }

  /**
   * Helper method to combine a list of {@link LocatedBlock} with associated
   * {@link VolumeId} information to form a list of {@link BlockStorageLocation}
   * .
   */
  static BlockStorageLocation[] convertToVolumeBlockLocations(
      List<LocatedBlock> blocks, 
      Map<LocatedBlock, List<VolumeId>> blockVolumeIds) throws IOException {
    // Construct the final return value of VolumeBlockLocation[]
    BlockLocation[] locations = DFSUtil.locatedBlocks2Locations(blocks);
    List<BlockStorageLocation> volumeBlockLocs = 
        new ArrayList<BlockStorageLocation>(locations.length);
    for (int i = 0; i < locations.length; i++) {
      LocatedBlock locBlock = blocks.get(i);
      List<VolumeId> volumeIds = blockVolumeIds.get(locBlock);
      BlockStorageLocation bsLoc = new BlockStorageLocation(locations[i], 
          volumeIds.toArray(new VolumeId[0]));
      volumeBlockLocs.add(bsLoc);
    }
    return volumeBlockLocs.toArray(new BlockStorageLocation[] {});
  }
  
  /**
   * Callable that sets up an RPC proxy to a datanode and queries it for
   * volume location information for a list of ExtendedBlocks. 
   */
  private static class VolumeBlockLocationCallable implements 
    Callable<HdfsBlocksMetadata> {
    
    private Configuration configuration;
    private int timeout;
    private DatanodeInfo datanode;
    private List<ExtendedBlock> extendedBlocks;
    private List<Token<BlockTokenIdentifier>> dnTokens;
    private boolean connectToDnViaHostname;
    
    VolumeBlockLocationCallable(Configuration configuration,
        DatanodeInfo datanode, List<ExtendedBlock> extendedBlocks,
        List<Token<BlockTokenIdentifier>> dnTokens, int timeout, 
        boolean connectToDnViaHostname) {
      this.configuration = configuration;
      this.timeout = timeout;
      this.datanode = datanode;
      this.extendedBlocks = extendedBlocks;
      this.dnTokens = dnTokens;
      this.connectToDnViaHostname = connectToDnViaHostname;
    }
    
    public DatanodeInfo getDatanodeInfo() {
      return datanode;
    }

    @Override
    public HdfsBlocksMetadata call() throws Exception {
      HdfsBlocksMetadata metadata = null;
      // Create the RPC proxy and make the RPC
      ClientDatanodeProtocol cdp = null;
      try {
        cdp = DFSUtil.createClientDatanodeProtocolProxy(datanode, configuration,
            timeout, connectToDnViaHostname);
        metadata = cdp.getHdfsBlocksMetadata(extendedBlocks, dnTokens);
      } catch (IOException e) {
        // Bubble this up to the caller, handle with the Future
        throw e;
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp);
        }
      }
      return metadata;
    }
  }
}
