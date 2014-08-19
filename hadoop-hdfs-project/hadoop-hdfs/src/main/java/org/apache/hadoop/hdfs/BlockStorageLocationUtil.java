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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
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
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class BlockStorageLocationUtil {
  
  static final Log LOG = LogFactory
      .getLog(BlockStorageLocationUtil.class);
  
  /**
   * Create a list of {@link VolumeBlockLocationCallable} corresponding to a set
   * of datanodes and blocks. The blocks must all correspond to the same
   * block pool.
   * 
   * @param datanodeBlocks
   *          Map of datanodes to block replicas at each datanode
   * @return callables Used to query each datanode for location information on
   *         the block replicas at the datanode
   */
  private static List<VolumeBlockLocationCallable> createVolumeBlockLocationCallables(
      Configuration conf, Map<DatanodeInfo, List<LocatedBlock>> datanodeBlocks,
      int timeout, boolean connectToDnViaHostname) {
    
    if (datanodeBlocks.isEmpty()) {
      return Lists.newArrayList();
    }
    
    // Construct the callables, one per datanode
    List<VolumeBlockLocationCallable> callables = 
        new ArrayList<VolumeBlockLocationCallable>();
    for (Map.Entry<DatanodeInfo, List<LocatedBlock>> entry : datanodeBlocks
        .entrySet()) {
      // Construct RPC parameters
      DatanodeInfo datanode = entry.getKey();
      List<LocatedBlock> locatedBlocks = entry.getValue();
      if (locatedBlocks.isEmpty()) {
        continue;
      }
      
      // Ensure that the blocks all are from the same block pool.
      String poolId = locatedBlocks.get(0).getBlock().getBlockPoolId();
      for (LocatedBlock lb : locatedBlocks) {
        if (!poolId.equals(lb.getBlock().getBlockPoolId())) {
          throw new IllegalArgumentException(
              "All blocks to be queried must be in the same block pool: " +
              locatedBlocks.get(0).getBlock() + " and " + lb +
              " are from different pools.");
        }
      }
      
      long[] blockIds = new long[locatedBlocks.size()];
      int i = 0;
      List<Token<BlockTokenIdentifier>> dnTokens = 
          new ArrayList<Token<BlockTokenIdentifier>>(
          locatedBlocks.size());
      for (LocatedBlock b : locatedBlocks) {
        blockIds[i++] = b.getBlock().getBlockId();
        dnTokens.add(b.getBlockToken());
      }
      VolumeBlockLocationCallable callable = new VolumeBlockLocationCallable(
          conf, datanode, poolId, blockIds, dnTokens, timeout, 
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
   * @return metadatas Map of datanodes to block metadata of the DN
   * @throws InvalidBlockTokenException
   *           if client does not have read access on a requested block
   */
  static Map<DatanodeInfo, HdfsBlocksMetadata> queryDatanodesForHdfsBlocksMetadata(
      Configuration conf, Map<DatanodeInfo, List<LocatedBlock>> datanodeBlocks,
      int poolsize, int timeoutMs, boolean connectToDnViaHostname)
      throws InvalidBlockTokenException {

    List<VolumeBlockLocationCallable> callables = 
        createVolumeBlockLocationCallables(conf, datanodeBlocks, timeoutMs, 
            connectToDnViaHostname);
    
    // Use a thread pool to execute the Callables in parallel
    List<Future<HdfsBlocksMetadata>> futures = 
        new ArrayList<Future<HdfsBlocksMetadata>>();
    ExecutorService executor = new ScheduledThreadPoolExecutor(poolsize);
    try {
      futures = executor.invokeAll(callables, timeoutMs,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // Swallow the exception here, because we can return partial results
    }
    executor.shutdown();
    
    Map<DatanodeInfo, HdfsBlocksMetadata> metadatas =
        Maps.newHashMapWithExpectedSize(datanodeBlocks.size());
    // Fill in metadatas with results from DN RPCs, where possible
    for (int i = 0; i < futures.size(); i++) {
      VolumeBlockLocationCallable callable = callables.get(i);
      DatanodeInfo datanode = callable.getDatanodeInfo();
      Future<HdfsBlocksMetadata> future = futures.get(i);
      try {
        HdfsBlocksMetadata metadata = future.get();
        metadatas.put(callable.getDatanodeInfo(), metadata);
      } catch (CancellationException e) {
        LOG.info("Cancelled while waiting for datanode "
            + datanode.getIpcAddr(false) + ": " + e.toString());
      } catch (ExecutionException e) {
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
          LOG.info("Failed to query block locations on datanode " +
              datanode.getIpcAddr(false) + ": " + t);
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
   * {@link DFSClient#queryDatanodesForHdfsBlocksMetadata(Map)} to be
   * associated
   * with the corresponding {@link LocatedBlock}.
   * 
   * @param blocks
   *          Original LocatedBlock array
   * @param metadatas
   *          VolumeId information for the replicas on each datanode
   * @return blockVolumeIds per-replica VolumeId information associated with the
   *         parent LocatedBlock
   */
  static Map<LocatedBlock, List<VolumeId>> associateVolumeIdsWithBlocks(
      List<LocatedBlock> blocks,
      Map<DatanodeInfo, HdfsBlocksMetadata> metadatas) {
    
    // Initialize mapping of ExtendedBlock to LocatedBlock. 
    // Used to associate results from DN RPCs to the parent LocatedBlock
    Map<Long, LocatedBlock> blockIdToLocBlock = 
        new HashMap<Long, LocatedBlock>();
    for (LocatedBlock b : blocks) {
      blockIdToLocBlock.put(b.getBlock().getBlockId(), b);
    }
    
    // Initialize the mapping of blocks -> list of VolumeIds, one per replica
    // This is filled out with real values from the DN RPCs
    Map<LocatedBlock, List<VolumeId>> blockVolumeIds = 
        new HashMap<LocatedBlock, List<VolumeId>>();
    for (LocatedBlock b : blocks) {
      ArrayList<VolumeId> l = new ArrayList<VolumeId>(b.getLocations().length);
      for (int i = 0; i < b.getLocations().length; i++) {
        l.add(null);
      }
      blockVolumeIds.put(b, l);
    }
    
    // Iterate through the list of metadatas (one per datanode). 
    // For each metadata, if it's valid, insert its volume location information 
    // into the Map returned to the caller 
    for (Map.Entry<DatanodeInfo, HdfsBlocksMetadata> entry : metadatas.entrySet()) {
      DatanodeInfo datanode = entry.getKey();
      HdfsBlocksMetadata metadata = entry.getValue();
      // Check if metadata is valid
      if (metadata == null) {
        continue;
      }
      long[] metaBlockIds = metadata.getBlockIds();
      List<byte[]> metaVolumeIds = metadata.getVolumeIds();
      List<Integer> metaVolumeIndexes = metadata.getVolumeIndexes();
      // Add VolumeId for each replica in the HdfsBlocksMetadata
      for (int j = 0; j < metaBlockIds.length; j++) {
        int volumeIndex = metaVolumeIndexes.get(j);
        long blockId = metaBlockIds[j];
        // Skip if block wasn't found, or not a valid index into metaVolumeIds
        // Also skip if the DN responded with a block we didn't ask for
        if (volumeIndex == Integer.MAX_VALUE
            || volumeIndex >= metaVolumeIds.size()
            || !blockIdToLocBlock.containsKey(blockId)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No data for block " + blockId);
          }
          continue;
        }
        // Get the VolumeId by indexing into the list of VolumeIds
        // provided by the datanode
        byte[] volumeId = metaVolumeIds.get(volumeIndex);
        HdfsVolumeId id = new HdfsVolumeId(volumeId);
        // Find out which index we are in the LocatedBlock's replicas
        LocatedBlock locBlock = blockIdToLocBlock.get(blockId);
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
    
    private final Configuration configuration;
    private final int timeout;
    private final DatanodeInfo datanode;
    private final String poolId;
    private final long[] blockIds;
    private final List<Token<BlockTokenIdentifier>> dnTokens;
    private final boolean connectToDnViaHostname;
    
    VolumeBlockLocationCallable(Configuration configuration,
        DatanodeInfo datanode, String poolId, long []blockIds,
        List<Token<BlockTokenIdentifier>> dnTokens, int timeout, 
        boolean connectToDnViaHostname) {
      this.configuration = configuration;
      this.timeout = timeout;
      this.datanode = datanode;
      this.poolId = poolId;
      this.blockIds = blockIds;
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
        metadata = cdp.getHdfsBlocksMetadata(poolId, blockIds, dnTokens);
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
