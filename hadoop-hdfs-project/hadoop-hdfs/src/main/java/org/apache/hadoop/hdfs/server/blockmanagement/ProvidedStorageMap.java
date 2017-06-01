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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * This class allows us to manage and multiplex between storages local to
 * datanodes, and provided storage.
 */
public class ProvidedStorageMap {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProvidedStorageMap.class);

  // limit to a single provider for now
  private final BlockProvider blockProvider;
  private final String storageId;
  private final ProvidedDescriptor providedDescriptor;
  private final DatanodeStorageInfo providedStorageInfo;
  private boolean providedEnabled;

  ProvidedStorageMap(RwLock lock, BlockManager bm, Configuration conf)
      throws IOException {

    storageId = conf.get(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);

    providedEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED,
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT);

    if (!providedEnabled) {
      // disable mapping
      blockProvider = null;
      providedDescriptor = null;
      providedStorageInfo = null;
      return;
    }

    DatanodeStorage ds = new DatanodeStorage(
        storageId, State.NORMAL, StorageType.PROVIDED);
    providedDescriptor = new ProvidedDescriptor();
    providedStorageInfo = providedDescriptor.createProvidedStorage(ds);

    // load block reader into storage
    Class<? extends BlockProvider> fmt = conf.getClass(
        DFSConfigKeys.DFS_NAMENODE_BLOCK_PROVIDER_CLASS,
        BlockFormatProvider.class, BlockProvider.class);

    blockProvider = ReflectionUtils.newInstance(fmt, conf);
    blockProvider.init(lock, bm, providedStorageInfo);
    LOG.info("Loaded block provider class: " +
        blockProvider.getClass() + " storage: " + providedStorageInfo);
  }

  /**
   * @param dn datanode descriptor
   * @param s data node storage
   * @param context the block report context
   * @return the {@link DatanodeStorageInfo} for the specified datanode.
   * If {@code s} corresponds to a provided storage, the storage info
   * representing provided storage is returned.
   * @throws IOException
   */
  DatanodeStorageInfo getStorage(DatanodeDescriptor dn, DatanodeStorage s,
      BlockReportContext context) throws IOException {
    if (providedEnabled && storageId.equals(s.getStorageID())) {
      if (StorageType.PROVIDED.equals(s.getStorageType())) {
        // poll service, initiate
        blockProvider.start(context);
        dn.injectStorage(providedStorageInfo);
        return providedDescriptor.getProvidedStorage(dn, s);
      }
      LOG.warn("Reserved storage {} reported as non-provided from {}", s, dn);
    }
    return dn.getStorageInfo(s.getStorageID());
  }

  @VisibleForTesting
  public DatanodeStorageInfo getProvidedStorageInfo() {
    return providedStorageInfo;
  }

  public LocatedBlockBuilder newLocatedBlocks(int maxValue) {
    if (!providedEnabled) {
      return new LocatedBlockBuilder(maxValue);
    }
    return new ProvidedBlocksBuilder(maxValue);
  }

  public void removeDatanode(DatanodeDescriptor dnToRemove) {
    if (providedDescriptor != null) {
      int remainingDatanodes = providedDescriptor.remove(dnToRemove);
      if (remainingDatanodes == 0) {
        blockProvider.stop();
      }
    }
  }

  /**
   * Builder used for creating {@link LocatedBlocks} when a block is provided.
   */
  class ProvidedBlocksBuilder extends LocatedBlockBuilder {

    private ShadowDatanodeInfoWithStorage pending;
    private boolean hasProvidedLocations;

    ProvidedBlocksBuilder(int maxBlocks) {
      super(maxBlocks);
      pending = new ShadowDatanodeInfoWithStorage(
          providedDescriptor, storageId);
      hasProvidedLocations = false;
    }

    @Override
    LocatedBlock newLocatedBlock(ExtendedBlock eb,
        DatanodeStorageInfo[] storages, long pos, boolean isCorrupt) {

      DatanodeInfoWithStorage[] locs =
        new DatanodeInfoWithStorage[storages.length];
      String[] sids = new String[storages.length];
      StorageType[] types = new StorageType[storages.length];
      for (int i = 0; i < storages.length; ++i) {
        sids[i] = storages[i].getStorageID();
        types[i] = storages[i].getStorageType();
        if (StorageType.PROVIDED.equals(storages[i].getStorageType())) {
          locs[i] = pending;
          hasProvidedLocations = true;
        } else {
          locs[i] = new DatanodeInfoWithStorage(
              storages[i].getDatanodeDescriptor(), sids[i], types[i]);
        }
      }
      return new LocatedBlock(eb, locs, sids, types, pos, isCorrupt, null);
    }

    @Override
    LocatedBlocks build(DatanodeDescriptor client) {
      // TODO: to support multiple provided storages, need to pass/maintain map
      if (hasProvidedLocations) {
        // set all fields of pending DatanodeInfo
        List<String> excludedUUids = new ArrayList<String>();
        for (LocatedBlock b : blocks) {
          DatanodeInfo[] infos = b.getLocations();
          StorageType[] types = b.getStorageTypes();

          for (int i = 0; i < types.length; i++) {
            if (!StorageType.PROVIDED.equals(types[i])) {
              excludedUUids.add(infos[i].getDatanodeUuid());
            }
          }
        }

        DatanodeDescriptor dn =
                providedDescriptor.choose(client, excludedUUids);
        if (dn == null) {
          dn = providedDescriptor.choose(client);
        }
        pending.replaceInternal(dn);
      }

      return new LocatedBlocks(
          flen, isUC, blocks, last, lastComplete, feInfo, ecPolicy);
    }

    @Override
    LocatedBlocks build() {
      return build(providedDescriptor.chooseRandom());
    }
  }

  /**
   * An abstract {@link DatanodeInfoWithStorage} to represent provided storage.
   */
  static class ShadowDatanodeInfoWithStorage extends DatanodeInfoWithStorage {
    private String shadowUuid;

    ShadowDatanodeInfoWithStorage(DatanodeDescriptor d, String storageId) {
      super(d, storageId, StorageType.PROVIDED);
    }

    @Override
    public String getDatanodeUuid() {
      return shadowUuid;
    }

    public void setDatanodeUuid(String uuid) {
      shadowUuid = uuid;
    }

    void replaceInternal(DatanodeDescriptor dn) {
      updateRegInfo(dn); // overwrite DatanodeID (except UUID)
      setDatanodeUuid(dn.getDatanodeUuid());
      setCapacity(dn.getCapacity());
      setDfsUsed(dn.getDfsUsed());
      setRemaining(dn.getRemaining());
      setBlockPoolUsed(dn.getBlockPoolUsed());
      setCacheCapacity(dn.getCacheCapacity());
      setCacheUsed(dn.getCacheUsed());
      setLastUpdate(dn.getLastUpdate());
      setLastUpdateMonotonic(dn.getLastUpdateMonotonic());
      setXceiverCount(dn.getXceiverCount());
      setNetworkLocation(dn.getNetworkLocation());
      adminState = dn.getAdminState();
      setUpgradeDomain(dn.getUpgradeDomain());
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  /**
   * An abstract DatanodeDescriptor to track datanodes with provided storages.
   * NOTE: never resolved through registerDatanode, so not in the topology.
   */
  static class ProvidedDescriptor extends DatanodeDescriptor {

    private final NavigableMap<String, DatanodeDescriptor> dns =
        new ConcurrentSkipListMap<>();

    ProvidedDescriptor() {
      super(new DatanodeID(
            null,                         // String ipAddr,
            null,                         // String hostName,
            UUID.randomUUID().toString(), // String datanodeUuid,
            0,                            // int xferPort,
            0,                            // int infoPort,
            0,                            // int infoSecurePort,
            0));                          // int ipcPort
    }

    DatanodeStorageInfo getProvidedStorage(
        DatanodeDescriptor dn, DatanodeStorage s) {
      dns.put(dn.getDatanodeUuid(), dn);
      // TODO: maintain separate RPC ident per dn
      return storageMap.get(s.getStorageID());
    }

    DatanodeStorageInfo createProvidedStorage(DatanodeStorage ds) {
      assert null == storageMap.get(ds.getStorageID());
      DatanodeStorageInfo storage = new ProvidedDatanodeStorageInfo(this, ds);
      storage.setHeartbeatedSinceFailover(true);
      storageMap.put(storage.getStorageID(), storage);
      return storage;
    }

    DatanodeDescriptor choose(DatanodeDescriptor client) {
      // exact match for now
      DatanodeDescriptor dn = client != null ?
              dns.get(client.getDatanodeUuid()) : null;
      if (null == dn) {
        dn = chooseRandom();
      }
      return dn;
    }

    DatanodeDescriptor choose(DatanodeDescriptor client,
        List<String> excludedUUids) {
      // exact match for now
      DatanodeDescriptor dn = client != null ?
              dns.get(client.getDatanodeUuid()) : null;

      if (null == dn || excludedUUids.contains(client.getDatanodeUuid())) {
        dn = null;
        Set<String> exploredUUids = new HashSet<String>();

        while(exploredUUids.size() < dns.size()) {
          Map.Entry<String, DatanodeDescriptor> d =
                  dns.ceilingEntry(UUID.randomUUID().toString());
          if (null == d) {
            d = dns.firstEntry();
          }
          String uuid = d.getValue().getDatanodeUuid();
          //this node has already been explored, and was not selected earlier
          if (exploredUUids.contains(uuid)) {
            continue;
          }
          exploredUUids.add(uuid);
          //this node has been excluded
          if (excludedUUids.contains(uuid)) {
            continue;
          }
          return dns.get(uuid);
        }
      }

      return dn;
    }

    DatanodeDescriptor chooseRandom(DatanodeStorageInfo[] excludedStorages) {
      // TODO: Currently this is not uniformly random;
      // skewed toward sparse sections of the ids
      Set<DatanodeDescriptor> excludedNodes =
          new HashSet<DatanodeDescriptor>();
      if (excludedStorages != null) {
        for (int i= 0; i < excludedStorages.length; i++) {
          LOG.info("Excluded: " + excludedStorages[i].getDatanodeDescriptor());
          excludedNodes.add(excludedStorages[i].getDatanodeDescriptor());
        }
      }
      Set<DatanodeDescriptor> exploredNodes = new HashSet<DatanodeDescriptor>();

      while(exploredNodes.size() < dns.size()) {
        Map.Entry<String, DatanodeDescriptor> d =
            dns.ceilingEntry(UUID.randomUUID().toString());
        if (null == d) {
          d = dns.firstEntry();
        }
        DatanodeDescriptor node = d.getValue();
        //this node has already been explored, and was not selected earlier
        if (exploredNodes.contains(node)) {
          continue;
        }
        exploredNodes.add(node);
        //this node has been excluded
        if (excludedNodes.contains(node)) {
          continue;
        }
        return node;
      }
      return null;
    }

    DatanodeDescriptor chooseRandom() {
      return chooseRandom(null);
    }

    @Override
    void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
      // pick a random datanode, delegate to it
      DatanodeDescriptor node = chooseRandom(targets);
      if (node != null) {
        node.addBlockToBeReplicated(block, targets);
      } else {
        LOG.error("Cannot find a source node to replicate block: "
            + block + " from");
      }
    }

    int remove(DatanodeDescriptor dnToRemove) {
      // this operation happens under the FSNamesystem lock;
      // no additional synchronization required.
      if (dnToRemove != null) {
        DatanodeDescriptor storedDN = dns.get(dnToRemove.getDatanodeUuid());
        if (storedDN != null) {
          dns.remove(dnToRemove.getDatanodeUuid());
        }
      }
      return dns.size();
    }

    int activeProvidedDatanodes() {
      return dns.size();
    }

    @Override
    public boolean equals(Object obj) {
      return (this == obj) || super.equals(obj);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  /**
   * The DatanodeStorageInfo used for the provided storage.
   */
  static class ProvidedDatanodeStorageInfo extends DatanodeStorageInfo {

    ProvidedDatanodeStorageInfo(ProvidedDescriptor dn, DatanodeStorage ds) {
      super(dn, ds);
    }

    @Override
    boolean removeBlock(BlockInfo b) {
      ProvidedDescriptor dn = (ProvidedDescriptor) getDatanodeDescriptor();
      if (dn.activeProvidedDatanodes() == 0) {
        return super.removeBlock(b);
      } else {
        return false;
      }
    }
  }
  /**
   * Used to emulate block reports for provided blocks.
   */
  static class ProvidedBlockList extends BlockListAsLongs {

    private final Iterator<Block> inner;

    ProvidedBlockList(Iterator<Block> inner) {
      this.inner = inner;
    }

    @Override
    public Iterator<BlockReportReplica> iterator() {
      return new Iterator<BlockReportReplica>() {
        @Override
        public BlockReportReplica next() {
          return new BlockReportReplica(inner.next());
        }
        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumberOfBlocks() {
      // VERIFY: only printed for debugging
      return -1;
    }

    @Override
    public ByteString getBlocksBuffer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getBlockListAsLongs() {
      // should only be used for backwards compat, DN.ver > NN.ver
      throw new UnsupportedOperationException();
    }
  }
}
